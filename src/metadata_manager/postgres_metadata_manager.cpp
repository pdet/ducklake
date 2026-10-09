#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/prepared_statement.hpp"
#include "duckdb/logging/logger.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_state.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "common/ducklake_types.hpp"

#include <chrono>
#include <tuple>

namespace duckdb {

static bool HasFourDigitDatePrefix(const string &value) {
	return value.size() >= 10 && StringUtil::CharacterIsDigit(value[0]) && StringUtil::CharacterIsDigit(value[1]) &&
	       StringUtil::CharacterIsDigit(value[2]) && StringUtil::CharacterIsDigit(value[3]) && value[4] == '-' &&
	       StringUtil::CharacterIsDigit(value[5]) && StringUtil::CharacterIsDigit(value[6]) && value[7] == '-' &&
	       StringUtil::CharacterIsDigit(value[8]) && StringUtil::CharacterIsDigit(value[9]);
}

static string WithPostgresBinaryCollation(const string &expression) {
	return "(" + expression + " COLLATE \"C\")";
}

static bool IsPostgresTemporalStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	default:
		return false;
	}
}

static string GetPostgresStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		return "NUMERIC";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMPTZ";
	case LogicalTypeId::DECIMAL:
		return type.ToString();
	default:
		return string();
	}
}

static bool CanCastPostgresStatsForValueComparison(const LogicalType &type) {
	return type.IsNumeric() || type.id() == LogicalTypeId::BOOLEAN || IsPostgresTemporalStatsType(type);
}

static bool CanCastPostgresTemporalValue(const Value &value, const LogicalType &type) {
	auto string_value = value.ToString();
	if (!HasFourDigitDatePrefix(string_value)) {
		return false;
	}
	return type.id() != LogicalTypeId::DATE || string_value.size() == 10;
}

string PostgresMetadataManager::CastValueToTarget(const Value &value, const LogicalType &type) {
	if (value.IsNull() || value.ToString().find('\0') != string::npos || type.id() == LogicalTypeId::BLOB) {
		return string();
	}
	if (RequiresValueComparison(type) && (!CanCastPostgresStatsForValueComparison(type) || !ValueIsFinite(value))) {
		return string();
	}
	if (!RequiresValueComparison(type) && type.id() != LogicalTypeId::VARCHAR) {
		return string();
	}
	if (IsPostgresTemporalStatsType(type) && !CanCastPostgresTemporalValue(value, type)) {
		return string();
	}
	if (type.IsNumeric()) {
		return value.ToString();
	}
	auto literal = SQLString::ToString(value.ToString());
	if (type.id() == LogicalTypeId::VARCHAR) {
		return WithPostgresBinaryCollation(literal);
	}
	if (IsPostgresTemporalStatsType(type)) {
		return literal + "::" + GetPostgresStatsType(type);
	}
	if (type.id() == LogicalTypeId::BOOLEAN) {
		return literal + "::BOOLEAN";
	}
	return string();
}

//! The largest year PostgreSQL accepts for the type
static idx_t PostgresMaxTemporalYear(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
		return 5874897;
	case LogicalTypeId::TIMESTAMP_TZ:
		// an offset can push past the last year
		return 294275;
	default:
		return 294276;
	}
}

//! commit_ordering also accepts infinities, (BC) and long years
static string PostgresSafeTemporalStatsCast(const string &stats, const LogicalType &type, bool commit_ordering) {
	string year_digits = commit_ordering ? "{4,7}" : "{4}";
	string date_regex = "'^[0-9]" + year_digits + "-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01])" +
	                    (commit_ordering ? "( \\(BC\\))?" : "");
	string regex;
	if (type.id() == LogicalTypeId::DATE) {
		regex = date_regex + "$'";
	} else if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
		regex = date_regex +
		        " ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,6})?"
		        "(Z|[+-](0[0-9]|1[0-5])(:[0-5][0-9])" +
		        (commit_ordering ? "{0,2}" : "?") + ")$'";
	} else {
		regex = date_regex + "( ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,6})?)?$'";
	}

	string year;
	string month;
	string day;
	string leap_year;
	if (commit_ordering) {
		year = StringUtil::Format("split_part(%s, '-', 1)::INTEGER", stats);
		month = StringUtil::Format("split_part(%s, '-', 2)::INTEGER", stats);
		day = StringUtil::Format("substr(split_part(%s, '-', 3), 1, 2)::INTEGER", stats);
		// N (BC) is astronomical year 1 - N
		leap_year = StringUtil::Format("(CASE WHEN strpos(%s, '(BC)') > 0 THEN 1 - %s ELSE %s END)", stats, year, year);
	} else {
		year = StringUtil::Format("substring(%s FROM 1 FOR 4)::INTEGER", stats);
		month = StringUtil::Format("substring(%s FROM 6 FOR 2)::INTEGER", stats);
		day = StringUtil::Format("substring(%s FROM 9 FOR 2)::INTEGER", stats);
		leap_year = year;
	}
	auto max_day = StringUtil::Format(
	    "(CASE WHEN %s = 2 THEN CASE WHEN mod(%s, 4) = 0 AND (mod(%s, 100) <> 0 OR mod(%s, 400) = 0) "
	    "THEN 29 ELSE 28 END WHEN %s IN (4, 6, 9, 11) THEN 30 ELSE 31 END)",
	    month, leap_year, leap_year, leap_year, month);
	auto valid_date = StringUtil::Format("%s > 0 AND %s <= %s", year, day, max_day);
	if (!commit_ordering) {
		return StringUtil::Format("(CASE WHEN %s ~ %s THEN CASE WHEN %s THEN %s::%s END END)", stats, regex, valid_date,
		                          stats, GetPostgresStatsType(type));
	}
	// PostgreSQL raises on out-of-range years
	auto in_range = StringUtil::Format("%s <= CASE WHEN strpos(%s, '(BC)') > 0 THEN 4713 ELSE %d END", year, stats,
	                                   PostgresMaxTemporalYear(type));
	return StringUtil::Format("(CASE WHEN %s IN ('infinity', '-infinity') THEN %s::%s WHEN %s ~ %s THEN CASE WHEN %s "
	                          "AND %s THEN %s::%s END END)",
	                          stats, stats, GetPostgresStatsType(type), stats, regex, valid_date, in_range, stats,
	                          GetPostgresStatsType(type));
}

string PostgresMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type,
                                                  StatsCastType cast_type) {
	if (IsPostgresTemporalStatsType(type)) {
		if (cast_type == StatsCastType::COMMIT_ORDERING) {
			return PostgresSafeTemporalStatsCast(stats, type, true);
		}
		auto cast = PostgresSafeTemporalStatsCast(stats, type, false);
		if (cast_type == StatsCastType::ORDERING) {
			return cast;
		}
		return BoundOrInfinity(cast, GetPostgresStatsType(type), cast_type);
	}
	if (CanCastPostgresStatsForValueComparison(type)) {
		return stats + "::" + GetPostgresStatsType(type);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		return WithPostgresBinaryCollation(stats);
	}
	return string();
}

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
	// If we knew that the Postgres installation has PostGIS installed, we could support GEOMETRY in the future.
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::SQLNULL:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

bool PostgresMetadataManager::InlinedDeletionTableExists(const string &table_name) {
	auto &catalog = transaction.GetCatalog();
	auto remote_query = StringUtil::Format(
	    "SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename = %s LIMIT 1",
	    SQLString::ToString(catalog.MetadataSchemaName().GetIdentifierName()), SQLString::ToString(table_name));
	auto query =
	    StringUtil::Format("SELECT 1 FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, %s, use_transaction = true)",
	                       SQLString::ToString(remote_query));
	auto result = DuckLakeMetadataManager::Query(query);
	result->ThrowIfError("Failed to probe for DuckLake inlined-deletion table: ");
	return result->Fetch() != nullptr;
}

void PostgresMetadataManager::MigrateInlinedDataTypes() {
	auto columns = DuckLakeMetadataManager::Query(GetInlinedTableColumnsSql());
	columns->ThrowIfError("Failed to read the columns of inlined-data tables while migrating: ");
	map<string, case_insensitive_map_t<string>> inlined_tables;
	for (auto &row : *columns) {
		inlined_tables[row.GetValue<string>(0)][row.GetValue<string>(1)] = row.GetValue<string>(2);
	}
	for (auto &inlined_table : inlined_tables) {
		auto &table_name = inlined_table.first;
		auto probe = DuckLakeMetadataManager::Query(
		    StringUtil::Format("SELECT * FROM {METADATA_CATALOG}.%s LIMIT 0", SQLIdentifier(table_name)));
		if (probe->HasError() || probe->GetNames().size() < 3) {
			continue;
		}
		// the metadata columns are always the first three columns, user columns follow
		auto &names = probe->GetNames();
		DuckLakeInlinedColNames col_names(false);
		col_names.row_id = names[0].GetIdentifierName();
		col_names.begin_snapshot = names[1].GetIdentifierName();
		col_names.end_snapshot = names[2].GetIdentifierName();
		vector<string> select_list;
		for (idx_t i = 0; i < 3; i++) {
			select_list.push_back(SQLIdentifier::ToString(names[i].GetIdentifierName()));
		}
		string column_defs;
		bool rewrite = false;
		for (idx_t i = 3; i < names.size(); i++) {
			auto name = names[i].GetIdentifierName();
			auto column_type = inlined_table.second.find(name);
			if (column_type == inlined_table.second.end()) {
				rewrite = false;
				break;
			}
			DuckLakeColumnInfo column;
			column.type = column_type->second;
			auto storage_type_name = GetColumnType(column);
			auto storage_type = UnboundType::TryParseAndDefaultBind(storage_type_name);
			auto type = DuckLakeTypes::FromString(column.type);
			auto native_type = type.HasAlias() ? LogicalType(type.id()) : type;
			// DuckLake 0.3 stored values with the native type of their DuckLake type, other columns are kept
			auto &stored_type = probe->GetTypes()[i];
			bool convert = stored_type != storage_type && stored_type == native_type;
			auto column_name = SQLIdentifier::ToString(name);
			column_defs += StringUtil::Format("%s%s %s", column_defs.empty() ? "" : ", ", column_name,
			                                  convert ? storage_type_name : stored_type.ToString());
			select_list.push_back(convert ? DuckLakeUtil::InlinedStorageExpression(*this, column_name, type)
			                              : column_name);
			rewrite = rewrite || convert;
		}
		if (!rewrite) {
			continue;
		}
		auto migrated_name = table_name + "_migrated";
		auto migrate_query = InlinedTableDdlSql(migrated_name, column_defs, col_names);
		migrate_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.%s SELECT %s FROM {METADATA_CATALOG}.%s;",
		                                    SQLIdentifier(migrated_name), StringUtil::Join(select_list, ", "),
		                                    SQLIdentifier(table_name));
		migrate_query += StringUtil::Format("DROP TABLE {METADATA_CATALOG}.%s;", SQLIdentifier(table_name));
		migrate_query += StringUtil::Format("ALTER TABLE {METADATA_CATALOG}.%s RENAME TO %s;",
		                                    SQLIdentifier(migrated_name), SQLIdentifier(table_name));
		auto result = DuckLakeMetadataManager::Execute(migrate_query);
		result->ThrowIfError(
		    StringUtil::Format("Failed to migrate the column types of inlined-data table \"%s\": ", table_name));
	}
}

void PostgresMetadataManager::SubstitutePostgresPlaceholders(string &query) const {
	SubstituteCatalogPlaceholders(query, SQLQuotedIdentifier::ToString(transaction.GetCatalog().MetadataSchemaName()));
}

string PostgresMetadataManager::WithPostgresPlaceholders(string query) const {
	SubstitutePostgresPlaceholders(query);
	return query;
}

string PostgresMetadataManager::ServerCallSql(const string &query) const {
	auto catalog_literal = SQLString::ToString(transaction.GetCatalog().MetadataDatabaseName());
	return StringUtil::Format("CALL postgres_execute(%s, %s, prepare=FALSE)", catalog_literal, SQLString(query));
}

string PostgresMetadataManager::ServerQuerySql(const string &query) const {
	auto catalog_literal = SQLString::ToString(transaction.GetCatalog().MetadataDatabaseName());
	return StringUtil::Format("SELECT * FROM postgres_query(%s, %s)", catalog_literal, SQLString(query));
}

string PostgresMetadataManager::ServerCommitCallSql(const string &query) const {
	auto catalog_literal = SQLString::ToString(transaction.GetCatalog().MetadataDatabaseName());
	// autocommit runs the function at READ COMMITTED
	return StringUtil::Format("SELECT status, committed_snapshot_id, committed_schema_version, "
	                          "committed_next_file_id, last_attempt, detail FROM postgres_query(%s, %s, "
	                          "use_transaction = false)",
	                          catalog_literal, SQLString(query));
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteOnServer(const string &query) {
	auto result = transaction.GetConnection().Query(ServerCallSql(query));
	return std::move(result);
}

unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	SubstituteTransactionPlaceholders(snapshot, query);
	SubstitutePostgresPlaceholders(query);
	return ExecuteOnServer(query);
}

void PostgresMetadataManager::ClearCache() {
	auto result = transaction.ExecuteRaw("CALL pg_clear_cache();");
	result->ThrowIfError("Failed to clear the PostgreSQL metadata cache: ");
}

static string ServerCommitDisabledReason(const PostgresServerCommitCapabilities &capabilities) {
	if (!capabilities.has_plpgsql) {
		return "PL/pgSQL is not available or not usable";
	}
	if (!capabilities.installed) {
		return "the commit functions are missing and could not be created";
	}
	if (!capabilities.executable) {
		return "the commit functions are not executable";
	}
	if (capabilities.default_isolation != "read committed") {
		return "default_transaction_isolation is " + capabilities.default_isolation;
	}
	return string();
}

string PostgresMetadataManager::ProbeServerCommit(PostgresServerCommitCapabilities &capabilities) {
	auto &catalog = transaction.GetCatalog();
	auto schema_literal = SQLString::ToString(catalog.MetadataSchemaName().GetIdentifierName());
	auto result = transaction.ExecuteRaw(ServerQuerySql(PostgresServerCommit::ProbeSql(schema_literal)));
	if (result->HasError()) {
		return result->GetError();
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return "the probe returned no rows";
	}
	capabilities.has_plpgsql = chunk->GetValue(0, 0).GetValue<bool>();
	capabilities.installed = chunk->GetValue(1, 0).GetValue<bool>();
	capabilities.executable = chunk->GetValue(2, 0).GetValue<bool>();
	capabilities.can_create = chunk->GetValue(3, 0).GetValue<bool>();
	capabilities.default_isolation = chunk->GetValue(4, 0).ToString();
	return string();
}

string PostgresMetadataManager::InstallServerCommit() {
	auto schema_literal = SQLString::ToString(transaction.GetCatalog().MetadataSchemaName().GetIdentifierName());
	auto install = PostgresServerCommit::InstallSql(schema_literal,
	                                                [&](const string &sql) { return WithPostgresPlaceholders(sql); });
	auto result = ExecuteOnServer(install);
	return result->HasError() ? result->GetError() : string();
}

//! Turns the server-side commit off with a warning
static void DisableServerCommit(DuckLakeCatalog &catalog, const string &reason) {
	catalog.SetServerCommitMode(DuckLakeServerCommitMode::NONE);
	DUCKDB_LOG_WARNING(catalog.GetDatabase(),
	                   StringUtil::Format("DuckLake server-side commit retries are disabled: %s", reason));
}

bool PostgresMetadataManager::ProbeServerCapabilities() {
	auto &catalog = transaction.GetCatalog();
	catalog.SetServerCommitMode(DuckLakeServerCommitMode::NONE);
	PostgresServerCommitCapabilities capabilities;
	auto error = ProbeServerCommit(capabilities);
	if (error.empty() && !capabilities.installed && capabilities.has_plpgsql && capabilities.can_create) {
		error = InstallServerCommit();
		if (error.empty()) {
			error = ProbeServerCommit(capabilities);
		}
	}
	auto reason = error.empty() ? ServerCommitDisabledReason(capabilities) : error;
	if (!reason.empty()) {
		DisableServerCommit(catalog, reason);
		return error.empty();
	}
	catalog.SetServerCommitMode(DuckLakeServerCommitMode::PG_FUNCTION);
	return true;
}

static int64_t SteadyClockMs() {
	return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

//! The key wait budget starts with the first wait
static void StartLockBudget(PostgresSequencedRetry &retry, int64_t now_ms) {
	if (retry.deadline_ms < 0) {
		retry.deadline_ms = now_ms + retry.lock_budget_ms;
	}
}

static int64_t RemainingLockBudgetMs(PostgresSequencedRetry &retry) {
	auto now_ms = SteadyClockMs();
	StartLockBudget(retry, now_ms);
	return MaxValue<int64_t>(retry.deadline_ms - now_ms, 0);
}

//! Errors that turn the server-side commit off
static bool DisablesServerCommit(const string &message) {
	return StringUtil::Contains(message, "does not exist") || StringUtil::Contains(message, "permission denied");
}

//! First line of the server error after the echoed sql
static string EchoedServerError(const string &message, const string &sent_sql) {
	// the scanner drops trailing semicolons
	auto sent_end = sent_sql.find_last_not_of("; \t\n\r\v\f");
	auto sent = sent_end == string::npos ? string::npos : message.find(sent_sql.c_str(), 0, sent_end + 1);
	if (sent == string::npos) {
		return string();
	}
	// libpq writes "<localized severity>:  <message>"
	auto start = message.find(":  ", sent + sent_end + 1);
	if (start == string::npos) {
		return string();
	}
	start += 3;
	return message.substr(start, message.find('\n', start) - start);
}

void PostgresMetadataManager::PrepareCommitLoop(DuckLakeCommitContext &context,
                                                const DuckLakeRetryConfig &retry_config) {
	if (transaction.GetCatalog().GetServerCommitMode() != DuckLakeServerCommitMode::PG_FUNCTION) {
		return;
	}
	auto retry = make_shared_ptr<PostgresSequencedRetry>();
	if (retry_config.debug_relative_commit) {
		auto debug_state = make_shared_ptr<PostgresRelativeDebugState>();
		auto get_table_stats = context.get_table_stats;
		context.execute_relative_commit = [this, debug_state, retry,
		                                   get_table_stats](const DuckLakeCommitAttempt &attempt) {
			return ExecuteRelativeCommit(*debug_state, *retry, attempt, get_table_stats);
		};
	}
	if (!retry_config.server_side_retries) {
		return;
	}
	retry->shared_key = true;
	retry->lock_budget_ms = PostgresServerCommit::SequencedLockBudgetMs(retry_config);
	context.acquire_sequenced_state = [this, retry](idx_t attempt, DuckLakeSnapshot transaction_snapshot,
	                                                SnapshotAndStats &state, SnapshotChangeInfo &changes) {
		return AcquireSequencedState(*retry, attempt, transaction_snapshot, state, changes);
	};
	context.retry_sleep_ms = [this, retry, retry_config](idx_t attempt, double random_multiplier) {
		if (RetryWaitsOnServer(*retry)) {
			return optional_idx(0);
		}
		return optional_idx(
		    static_cast<idx_t>(PostgresServerCommit::RetrySleepMs(retry_config, attempt) * random_multiplier));
	};
	auto execute_commit_batch = context.execute_commit_batch;
	context.execute_commit_batch = [this, retry, execute_commit_batch](DuckLakeSnapshot snapshot, string &query) {
		if (retry->sequenced_state) {
			PostgresServerCommit::SetSequencedSnapshotTime(query);
			return execute_commit_batch(snapshot, query);
		}
		auto shared_key = SharedKeySql(*retry);
		if (shared_key.empty()) {
			return execute_commit_batch(snapshot, query);
		}
		query.insert(0, shared_key);
		auto result = execute_commit_batch(snapshot, query);
		auto server_error =
		    result->HasError() ? EchoedServerError(result->GetErrorObject().RawMessage(), query) : string();
		if (StringUtil::Contains(server_error, "plpgsql")) {
			// the leading DO failed, so nothing ran
			DisableServerCommit(transaction.GetCatalog(), server_error);
			retry->shared_key_denied = true;
		}
		return result;
	};
	auto is_retryable_metadata_error = context.is_retryable_metadata_error;
	context.is_retryable_metadata_error = [retry, is_retryable_metadata_error](const string &message) {
		if (retry->shared_key_denied) {
			retry->shared_key_denied = false;
			return true;
		}
		return is_retryable_metadata_error(message);
	};
}

string PostgresMetadataManager::SharedKeySql(PostgresSequencedRetry &retry) const {
	if (!retry.shared_key || retry.sequenced_state ||
	    transaction.GetCatalog().GetServerCommitMode() != DuckLakeServerCommitMode::PG_FUNCTION) {
		return string();
	}
	return PostgresServerCommit::SharedKeySql(RemainingLockBudgetMs(retry),
	                                          [&](const string &sql) { return WithPostgresPlaceholders(sql); });
}

bool PostgresMetadataManager::RetryWaitsOnServer(PostgresSequencedRetry &retry) const {
	if (retry.unsequenced || retry.key_wait_timed_out ||
	    transaction.GetCatalog().GetServerCommitMode() != DuckLakeServerCommitMode::PG_FUNCTION) {
		return false;
	}
	auto now_ms = SteadyClockMs();
	StartLockBudget(retry, now_ms);
	return now_ms < retry.deadline_ms;
}

bool PostgresMetadataManager::AcquireSequencedState(PostgresSequencedRetry &retry, idx_t attempt,
                                                    DuckLakeSnapshot transaction_snapshot, SnapshotAndStats &state,
                                                    SnapshotChangeInfo &changes) {
	retry.sequenced_state = false;
	auto &catalog = transaction.GetCatalog();
	if (retry.unsequenced || catalog.GetServerCommitMode() != DuckLakeServerCommitMode::PG_FUNCTION) {
		return false;
	}
	auto lock_budget_ms = RemainingLockBudgetMs(retry);
	auto schema_literal = SQLString::ToString(catalog.MetadataSchemaName().GetIdentifierName());
	auto sequencer =
	    transaction.ExecuteRaw(ServerCallSql(PostgresServerCommit::SequencerSql(schema_literal, lock_budget_ms)));
	if (sequencer->HasError()) {
		auto &error = sequencer->GetErrorObject();
		bool disables = DisablesServerCommit(error.RawMessage());
		if (!disables && !StringUtil::Contains(error.RawMessage(), "must be called before any query")) {
			error.Throw("Failed to sequence the DuckLake commit: ");
		}
		if (disables) {
			catalog.SetServerCommitMode(DuckLakeServerCommitMode::NONE);
		}
		DUCKDB_LOG_WARNING(catalog.GetDatabase(), StringUtil::Format("DuckLake commit attempt %d runs unsequenced: %s",
		                                                             attempt, error.RawMessage()));
		// nothing was written yet
		retry.unsequenced = true;
		transaction.ResetCommitAttempt();
		return false;
	}

	auto state_executor = [&](string query) -> unique_ptr<QueryResult> {
		SubstituteSnapshotPlaceholders(transaction_snapshot, query);
		SubstitutePostgresPlaceholders(query);
		auto result = transaction.ExecuteRaw(ServerQuerySql(query));
		result->ThrowIfError("Failed to commit DuckLake transaction - failed to read the sequenced commit state: ");
		return result;
	};
	auto extra_columns = PostgresServerCommit::SequencedStateColumns();
	SnapshotAndStats sequenced_state;
	string catalog_version;
	string inlined_tables;
	string sequenced;
	string lock_wait_ms;
	auto changes_made = GetSnapshotAndStatsAndChanges(
	    sequenced_state, state_executor, catalog.SupportsV1_1Metadata(), extra_columns, [&](const QueryResultRow &row) {
		    auto first_column = row.GetChunk().ColumnCount() - extra_columns.size();
		    TryReadValue(row, first_column, catalog_version);
		    TryReadValue(row, first_column + 1, inlined_tables);
		    TryReadValue(row, first_column + 2, sequenced);
		    TryReadValue(row, first_column + 3, lock_wait_ms);
	    });
	CheckCatalogVersion(catalog_version);

	auto inlined_table_list = DuckLakeUtil::ParseQuotedList(inlined_tables);
	if (inlined_table_list.size() % 2 != 0) {
		throw InvalidInputException("Failed to parse the inlined tables of the sequenced commit state");
	}
	unordered_map<idx_t, string> inlined_table_names;
	for (idx_t i = 0; i < inlined_table_list.size(); i += 2) {
		inlined_table_names[Value(inlined_table_list[i]).GetValue<idx_t>()] = inlined_table_list[i + 1];
	}
	SetInlinedTableNames(std::move(inlined_table_names), true);
	transaction.SetSnapshot(sequenced_state.snapshot);
	retry.key_wait_timed_out = sequenced != "true";
	if (retry.key_wait_timed_out) {
		DUCKDB_LOG_INFO(catalog.GetDatabase(),
		                StringUtil::Format("DuckLake commit attempt %d did not get the commit keys within %s ms and "
		                                   "runs unsequenced",
		                                   attempt, lock_wait_ms));
	}
	retry.sequenced_state = true;
	state = std::move(sequenced_state);
	changes = std::move(changes_made);
	return true;
}

string PostgresMetadataManager::InlinedTablePreconditionSql(TableIndex table_id, const string &inlined_table_name) {
	return StringUtil::Format(
	    "SELECT {METADATA_CATALOG}.ducklake_commit_fallback_v1('stale inlined table %d') WHERE (SELECT i.table_name "
	    "FROM {METADATA_CATALOG}.ducklake_inlined_data_tables i WHERE i.table_id = %d ORDER BY i.schema_version DESC "
	    "LIMIT 1) IS DISTINCT FROM %s;\n",
	    table_id.index, table_id.index, SQLString(inlined_table_name));
}

string PostgresMetadataManager::RelativeGlobalTableStatsSql(TableIndex table_id, const DuckLakeNewGlobalStats &delta,
                                                            idx_t seed, const set<FieldIndex> &new_columns,
                                                            bool write_stats_exactness) {
	auto &stats = delta.stats;
	if (stats.record_count_unknown || stats.record_count < seed || stats.next_row_id < seed ||
	    stats.table_size_bytes < seed) {
		throw InternalException("DuckLake relative commit stats fell below their seed");
	}
	for (auto &entry : stats.column_stats) {
		auto &column = entry.second;
		if (column.extra_stats) {
			throw NotImplementedException("DuckLake relative commit cannot merge %s stats", column.type.ToString());
		}
		if (column.bounds_unknown || column.has_num_values) {
			throw NotImplementedException("DuckLake relative commit cannot merge the stats of column %d",
			                              entry.first.index);
		}
		if ((column.has_min && column.min.find('\0') != string::npos) ||
		    (column.has_max && column.max.find('\0') != string::npos)) {
			throw NotImplementedException("DuckLake relative commit cannot store a bound with a NUL byte");
		}
	}
	auto table = std::to_string(table_id.index);
	auto stats_missing = StringUtil::Format(
	    "NOT EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_table_stats s WHERE s.table_id = %s)", table);
	string result;
	// a missing stats row merges as empty-table stats
	if (!new_columns.empty()) {
		vector<string> column_ids;
		for (auto &column_id : new_columns) {
			column_ids.push_back(StringUtil::Format("(%d)", column_id.index));
		}
		result += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats (table_id, column_id, contains_null, "
		    "contains_nan, min_value, max_value, extra_stats%s) SELECT %s, v.column_id, false, false, NULL, NULL, "
		    "NULL%s FROM (VALUES %s) AS v(column_id) WHERE %s;\n",
		    write_stats_exactness ? ", min_is_exact, max_is_exact" : "", table,
		    write_stats_exactness ? ", NULL, NULL" : "", StringUtil::Join(column_ids, ", "), stats_missing);
	}
	result += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_stats (table_id, record_count, "
	                             "next_row_id, file_size_bytes) SELECT %s, 0, 0, 0 WHERE %s;\n",
	                             table, stats_missing);
	// one merge per comparison class
	map<string, pair<LogicalType, vector<string>>> classes;
	for (auto &column_stats : DuckLakeTransaction::ConvertNewGlobalStats(table_id, delta).column_stats) {
		auto &column = stats.column_stats.at(column_stats.column_id);
		auto sql = ColumnStatsSQL::FromColumnStats(column_stats);
		auto key = RequiresValueComparison(column.type) ? column.type.ToString() : string();
		auto &column_class = classes.emplace(key, make_pair(column.type, vector<string>())).first->second;
		column_class.second.push_back(StringUtil::Format(
		    "(%d, CAST(%s AS BOOLEAN), CAST(%s AS BOOLEAN), %s, CAST(%s AS TEXT), CAST(%s AS BOOLEAN), CAST(%s AS "
		    "TEXT), CAST(%s AS BOOLEAN))",
		    column_stats.column_id.index, sql.contains_null, sql.contains_nan,
		    DuckLakeUtil::BoolLiteral(column.AnyValid()), sql.min_val, sql.min_is_exact, sql.max_val,
		    sql.max_is_exact));
	}
	for (auto &column_class : classes) {
		result += RelativeColumnStatsUpdate(table, column_class.second.first, column_class.second.second,
		                                    write_stats_exactness);
	}
	// summing would lock ducklake_data_file, so fall back
	result += StringUtil::Format(
	    "SELECT {METADATA_CATALOG}.ducklake_commit_fallback_v1('missing size of table %s') FROM "
	    "{METADATA_CATALOG}.ducklake_table_stats s WHERE s.table_id = %s AND s.file_size_bytes IS NULL;\n"
	    "UPDATE {METADATA_CATALOG}.ducklake_table_stats SET record_count = record_count + %d, next_row_id = "
	    "next_row_id + %d, file_size_bytes = file_size_bytes + %d WHERE table_id = %s;\n",
	    table, table, stats.record_count - seed, stats.next_row_id - seed, stats.table_size_bytes - seed, table);
	return result;
}

string PostgresMetadataManager::RelativeColumnStatsUpdate(const string &table_id, const LogicalType &type,
                                                          const vector<string> &rows, bool write_stats_exactness) {
	auto value_class = RequiresValueComparison(type);
	auto order = [&](const string &bound) {
		if (!value_class) {
			return WithPostgresBinaryCollation(bound);
		}
		auto cast = CastStatsToTarget(bound, type, StatsCastType::COMMIT_ORDERING);
		if (cast.empty()) {
			throw NotImplementedException("DuckLake relative commit cannot order %s stats", type.ToString());
		}
		return cast;
	};
	// g stored, d delta, t tie, n dropped
	auto pick = [&](const string &bound, bool is_min) {
		auto delta = "d.d_" + bound;
		auto stored = "c." + bound + "_value";
		auto delta_order = order(delta);
		auto stored_order = order(stored);
		return StringUtil::Format(
		    "CASE WHEN NOT d.d_valid THEN 'g' "
		    "WHEN c.min_value IS NULL AND c.max_value IS NULL AND c.extra_stats IS NULL THEN "
		    "CASE WHEN ts.record_count IS NULL OR ts.record_count > 0 THEN 'g' ELSE 'd' END "
		    "WHEN %s IS NULL OR %s IS NULL THEN 'n' "
		    "WHEN %s %s %s THEN 'd' WHEN %s = %s THEN 't' WHEN %s %s %s THEN 'g' "
		    "ELSE CASE WHEN {METADATA_CATALOG}.ducklake_commit_fallback_v1('incomparable ' || CASE WHEN %s IS NULL "
		    "THEN 'new' ELSE 'stored' END || ' %s bound of table %s') THEN 'n' END END",
		    delta, stored, delta_order, is_min ? "<" : ">", stored_order, delta_order, stored_order, delta_order,
		    is_min ? ">" : "<", stored_order, delta_order, bound, table_id);
	};
	auto exactness = [&](const string &bound) {
		auto delta = "d.d_" + bound;
		auto stored = "c." + bound + "_value";
		auto delta_exact = "COALESCE(d.d_" + bound + "_exact, false)";
		auto stored_exact = "COALESCE(c." + bound + "_is_exact, false)";
		if (value_class) {
			delta_exact = "true";
			stored_exact = "true";
		}
		return StringUtil::Format("CASE p.%s_pick WHEN 'n' THEN NULL WHEN 'd' THEN CASE WHEN %s IS NULL THEN NULL "
		                          "ELSE %s END WHEN 't' THEN %s AND %s ELSE CASE WHEN %s IS NULL THEN NULL ELSE %s END "
		                          "END AS %s_is_exact",
		                          bound, delta, delta_exact, stored_exact, delta_exact, stored, stored_exact, bound);
	};
	string exactness_set;
	string exactness_columns;
	if (write_stats_exactness) {
		exactness_set = ", min_is_exact = r.min_is_exact, max_is_exact = r.max_is_exact";
		exactness_columns = ",\n  " + exactness("min") + ",\n  " + exactness("max");
	}
	return StringUtil::Format(
	    "UPDATE {METADATA_CATALOG}.ducklake_table_column_stats g SET contains_null = r.contains_null, contains_nan = "
	    "r.contains_nan, min_value = r.min_value, max_value = r.max_value, extra_stats = NULL%s\n"
	    "FROM (SELECT d.column_id,\n"
	    "  CASE WHEN c.contains_null IS NULL OR d.null_delta IS NULL THEN NULL ELSE c.contains_null OR d.null_delta "
	    "END AS contains_null,\n"
	    "  CASE WHEN c.contains_nan IS NULL OR d.nan_delta IS NULL THEN NULL ELSE c.contains_nan OR d.nan_delta END "
	    "AS contains_nan,\n"
	    "  CASE p.min_pick WHEN 'd' THEN d.d_min WHEN 'n' THEN NULL ELSE c.min_value END AS min_value,\n"
	    "  CASE p.max_pick WHEN 'd' THEN d.d_max WHEN 'n' THEN NULL ELSE c.max_value END AS max_value%s\n"
	    "FROM (VALUES %s) AS d(column_id, null_delta, nan_delta, d_valid, d_min, d_min_exact, d_max, d_max_exact)\n"
	    "JOIN {METADATA_CATALOG}.ducklake_table_column_stats c ON c.table_id = %s AND c.column_id = d.column_id\n"
	    "JOIN {METADATA_CATALOG}.ducklake_table_stats ts ON ts.table_id = %s\n"
	    "CROSS JOIN LATERAL (SELECT %s AS min_pick,\n  %s AS max_pick) p) r\n"
	    "WHERE g.table_id = %s AND g.column_id = r.column_id;\n",
	    exactness_set, exactness_columns, StringUtil::Join(rows, ",\n  "), table_id, table_id, pick("min", true),
	    pick("max", false), table_id);
}

static idx_t RelativeRowIdBase(TableIndex table_id, optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats,
                               const std::function<shared_ptr<DuckLakeTableStats>(TableIndex)> &get_table_stats) {
	if (!stats) {
		auto table_stats = get_table_stats(table_id);
		return table_stats ? table_stats->next_row_id : 0;
	}
	for (auto &table_stats : *stats) {
		if (table_stats.table_id == table_id) {
			return table_stats.next_row_id;
		}
	}
	return 0;
}

//! Throws when a seed changed the render
static void CheckRelativeCommitInvariance(DatabaseInstance &db, const DuckLakeRelativeCommit &relative,
                                          const DuckLakeRelativeCommit &shifted) {
	if (relative.body != shifted.body) {
		idx_t offset = 0;
		while (offset < relative.body.size() && offset < shifted.body.size() &&
		       relative.body[offset] == shifted.body[offset]) {
			offset++;
		}
		auto start = offset < 100 ? 0 : offset - 100;
		DUCKDB_LOG_WARNING(db, StringUtil::Format("DuckLake relative commit renders differ: \"%s\" vs \"%s\"",
		                                          relative.body.substr(start, 200), shifted.body.substr(start, 200)));
		throw InternalException("DuckLake relative commit render depends on its seed at byte %d", offset);
	}
	if (relative.file_id_count != shifted.file_id_count || relative.insert_tables != shifted.insert_tables ||
	    relative.inlined_insert_tables != shifted.inlined_insert_tables ||
	    relative.row_id_tables != shifted.row_id_tables) {
		throw InternalException("DuckLake relative commit summary depends on its seed");
	}
}

string PostgresMetadataManager::RelativeCommitTemplate(const DuckLakeRelativeCommit &relative) const {
	auto metadata_catalog = SQLQuotedIdentifier::ToString(transaction.GetCatalog().MetadataSchemaName());
	auto result = ReplacePlaceholdersOutsideLiterals(relative.body, CatalogPlaceholderReplacements(metadata_catalog));
	if (result.size() > PostgresServerCommit::MAX_TEMPLATE_BYTES) {
		throw NotImplementedException("DuckLake relative commit of %llu bytes is too large", result.size());
	}
	return result;
}

pair<DuckLakeRelativeCommit, string>
PostgresMetadataManager::RenderRelativeTemplate(DuckLakeSnapshot transaction_snapshot,
                                                const TransactionChangeInformation &changes, bool check_invariance) {
	auto relative = transaction.RenderRelativeCommit(transaction_snapshot, changes, 0);
	if (check_invariance) {
		auto shifted =
		    transaction.RenderRelativeCommit(transaction_snapshot, changes, PostgresServerCommit::INVARIANCE_SEED);
		CheckRelativeCommitInvariance(transaction.GetCatalog().GetDatabase(), relative, shifted);
	}
	auto body = RelativeCommitTemplate(relative);
	return make_pair(std::move(relative), std::move(body));
}

//! Whether a render error hands the commit back
static bool IsRenderFallback(const ErrorData &error, bool known_fallbacks_only) {
	auto type = error.Type();
	if (known_fallbacks_only) {
		return type == ExceptionType::NOT_IMPLEMENTED &&
		       StringUtil::StartsWith(error.RawMessage(), "DuckLake relative commit");
	}
	return !Exception::InvalidatesDatabase(type) && type != ExceptionType::INTERNAL && type != ExceptionType::INTERRUPT;
}

bool PostgresMetadataManager::RelativeCommitUsable(PostgresRelativeDebugState &debug_state) {
	if (debug_state.checked) {
		return debug_state.usable;
	}
	auto schema_literal = SQLString::ToString(transaction.GetCatalog().MetadataSchemaName().GetIdentifierName());
	auto result =
	    transaction.GetConnection().Query(ServerQuerySql(PostgresServerCommit::RelativeUsableSql(schema_literal)));
	result->ThrowIfError("Failed to check the DuckLake relative commit functions: ");
	auto chunk = result->Fetch();
	debug_state.checked = true;
	debug_state.usable = chunk && chunk->size() > 0 && chunk->GetValue(0, 0).GetValue<bool>();
	if (!debug_state.usable) {
		DUCKDB_LOG_WARNING(transaction.GetCatalog().GetDatabase(),
		                   "DuckLake relative commit fell back: PL/pgSQL or its fallback function is not usable");
	}
	return debug_state.usable;
}

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteRelativeCommit(
    PostgresRelativeDebugState &debug_state, PostgresSequencedRetry &retry, const DuckLakeCommitAttempt &attempt,
    const std::function<shared_ptr<DuckLakeTableStats>(TableIndex)> &get_table_stats) {
	if ((debug_state.checked && !debug_state.usable) || !transaction.IsAppendOnlyCommit(attempt.changes)) {
		return nullptr;
	}
	auto &db = transaction.GetCatalog().GetDatabase();
	DuckLakeRelativeCommit relative;
	string relative_body;
	try {
		std::tie(relative, relative_body) = RenderRelativeTemplate(attempt.transaction_snapshot, attempt.changes, true);
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (!IsRenderFallback(error, true)) {
			throw;
		}
		DUCKDB_LOG_WARNING(db, StringUtil::Format("DuckLake relative commit fell back: %s", error.RawMessage()));
		return nullptr;
	}
	auto &read_snapshot = attempt.read_snapshot;
	auto &commit_snapshot = attempt.commit_snapshot;
	if (commit_snapshot.next_file_id != read_snapshot.next_file_id + relative.file_id_count ||
	    commit_snapshot.schema_version != read_snapshot.schema_version ||
	    commit_snapshot.next_catalog_id != read_snapshot.next_catalog_id) {
		throw InternalException("DuckLake relative commit allocated other ids than its absolute batch");
	}
	auto insert_snapshot = DuckLakeMetadataManager::InsertSnapshotSql();
	if (!StringUtil::StartsWith(attempt.batch, insert_snapshot)) {
		throw InternalException("DuckLake commit batch does not start with its snapshot");
	}
	if (!RelativeCommitUsable(debug_state)) {
		return nullptr;
	}
	vector<pair<string, string>> bases {
	    {DuckLakeRelativeCommitBase::SnapshotId(), std::to_string(commit_snapshot.snapshot_id)},
	    {DuckLakeRelativeCommitBase::SchemaVersion(), std::to_string(commit_snapshot.schema_version)},
	    {DuckLakeRelativeCommitBase::NextCatalogId(), std::to_string(commit_snapshot.next_catalog_id)},
	    {DuckLakeRelativeCommitBase::FileIdBase(), std::to_string(read_snapshot.next_file_id)}};
	for (auto &table_id : relative.row_id_tables) {
		bases.emplace_back(DuckLakeRelativeCommitBase::RowIdBase(table_id),
		                   std::to_string(RelativeRowIdBase(table_id, attempt.stats, get_table_stats)));
	}
	auto claim = insert_snapshot;
	if (retry.sequenced_state) {
		PostgresServerCommit::SetSequencedSnapshotTime(claim);
	}
	SubstituteSnapshotPlaceholders(commit_snapshot, claim);
	SubstitutePostgresPlaceholders(claim);
	auto absolute_body = attempt.batch.substr(insert_snapshot.size());
	SubstituteTransactionPlaceholders(commit_snapshot, absolute_body);
	SubstitutePostgresPlaceholders(absolute_body);
	auto result =
	    ExecuteOnServer(WithPostgresPlaceholders(SharedKeySql(retry)) +
	                    PostgresServerCommit::RelativeExecutionSql(claim, bases, relative_body, absolute_body));
	// the fallback reason is only read when logged
	if (result->HasError() || !Logger::Get(db).ShouldLog(DefaultLogType::NAME, LogLevel::LOG_WARNING)) {
		return result;
	}
	auto fallback = transaction.GetConnection().Query(ServerQuerySql(
	    "SELECT pg_catalog.current_setting('" + PostgresServerCommit::DebugFallbackSetting() + "', true)"));
	if (fallback->HasError()) {
		DUCKDB_LOG_WARNING(
		    db, StringUtil::Format("DuckLake relative commit fallback reason is unknown: %s", fallback->GetError()));
		return result;
	}
	auto chunk = fallback->Fetch();
	// a reset custom setting reads as empty
	auto reason =
	    chunk && chunk->size() > 0 && !chunk->GetValue(0, 0).IsNull() ? chunk->GetValue(0, 0).ToString() : string();
	if (!reason.empty()) {
		DUCKDB_LOG_WARNING(db, StringUtil::Format("DuckLake relative commit fell back: %s", reason));
	}
	return result;
}

bool PostgresMetadataManager::SupportsServerSideCommit(const TransactionChangeInformation &changes) const {
	return transaction.GetCatalog().GetServerCommitMode() == DuckLakeServerCommitMode::PG_FUNCTION &&
	       transaction.IsAppendOnlyCommit(changes);
}

void PostgresMetadataManager::FlushChangesServerSide(DuckLakeTransaction &, DuckLakeSnapshot,
                                                     const TransactionChangeInformation &transaction_changes,
                                                     const DuckLakeRetryConfig &retry_config) {
	// attempt 0 is a client batch, so load the snapshot
	auto transaction_snapshot = transaction.GetSnapshot();
	auto state = make_shared_ptr<PostgresServerCommitState>();
	transaction.RunCommitLoop(
	    transaction_snapshot, transaction_changes, retry_config, [&](DuckLakeCommitContext &context) {
		    auto prepare_retry = context.prepare_retry;
		    context.prepare_retry = [this, state, prepare_retry]() {
			    if (!state->attempted) {
				    state->inlined_table_names = GetInlinedTableNames();
			    }
			    prepare_retry();
		    };
		    context.server_attempt = [this, state, transaction_snapshot, &transaction_changes, retry_config]() {
			    return ServerCommitAttempt(*state, transaction_snapshot, transaction_changes, retry_config);
		    };
	    });
}

//! EXECUTE is checked before the function body runs
static bool CommitCallNotPermitted(const string &message, const string &call_sql) {
	return StringUtil::StartsWith(EchoedServerError(message, call_sql),
	                              "permission denied for function ducklake_commit_v1");
}

DuckLakeServerAttempt PostgresMetadataManager::ServerCommitHandBack(const ErrorData &error, idx_t last_attempt) {
	DuckLakeServerAttempt result;
	DUCKDB_LOG_WARNING(
	    transaction.GetCatalog().GetDatabase(),
	    StringUtil::Format("DuckLake server-side commit handed back to the client: %s", error.RawMessage()));
	try {
		transaction.ResetCommitAttempt();
	} catch (std::exception &ex) {
		result.status = DuckLakeServerAttemptStatus::FAILED;
		result.error = ErrorData(ex);
		return result;
	}
	result.last_attempt = last_attempt;
	return result;
}

DuckLakeServerAttempt PostgresMetadataManager::ServerCommitStatus(QueryResult &query_result,
                                                                  const DuckLakeRetryConfig &retry_config) {
	DuckLakeServerAttempt result;
	result.status = DuckLakeServerAttemptStatus::OUTCOME_UNKNOWN;
	vector<Value> row;
	idx_t row_count = 0;
	while (auto chunk = query_result.Fetch()) {
		if (chunk->size() == 0) {
			break;
		}
		for (idx_t column = 0; row_count == 0 && column < chunk->ColumnCount(); column++) {
			row.push_back(chunk->GetValue(column, 0));
		}
		row_count += chunk->size();
	}
	if (row_count != 1 || row.size() != 6 || row[0].IsNull() || row[4].IsNull()) {
		result.error = ErrorData(ExceptionType::INVALID_INPUT, "ducklake_commit_v1 returned no valid status row");
		return result;
	}
	auto status = row[0].ToString();
	auto detail = row[5].IsNull() ? string() : row[5].ToString();
	auto last_attempt = row[4].GetValue<int64_t>();
	// the function never counts past max_retry_count
	if (last_attempt < 0 || static_cast<idx_t>(last_attempt) > retry_config.max_retry_count) {
		result.error = ErrorData(ExceptionType::INVALID_INPUT,
		                         StringUtil::Format("ducklake_commit_v1 returned attempt %d", last_attempt));
		return result;
	}
	result.last_attempt = static_cast<idx_t>(last_attempt);
	if (status == "committed") {
		auto is_id = [](const Value &id) {
			return !id.IsNull() && id.GetValue<int64_t>() >= 0;
		};
		if (!is_id(row[1]) || !is_id(row[2]) || !is_id(row[3])) {
			result.error = ErrorData(ExceptionType::INVALID_INPUT, "ducklake_commit_v1 committed without its ids");
			return result;
		}
		result.status = DuckLakeServerAttemptStatus::COMMITTED;
		result.snapshot.snapshot_id = row[1].GetValue<idx_t>();
		result.snapshot.schema_version = row[2].GetValue<idx_t>();
		result.snapshot.next_file_id = row[3].GetValue<idx_t>();
		return result;
	}
	if (status == "conflict") {
		result.status = DuckLakeServerAttemptStatus::FAILED;
		result.finished_retrying = result.last_attempt >= retry_config.max_retry_count;
		// detail is "<table id>:<change kind>"
		auto separator = detail.find(':');
		const char *action = nullptr;
		idx_t table_id = 0;
		if (separator != string::npos) {
			auto table_text = detail.substr(0, separator);
			if (TryCast::Operation<string_t, idx_t>(string_t(table_text), table_id, true)) {
				action = DuckLakeTransactionState::InsertConflictAction(detail.substr(separator + 1));
			}
		}
		auto message = action ? DuckLakeTransactionState::ConflictMessage("insert into table", table_id, action)
		                      : "Transaction conflict - the server-side commit reported " + detail;
		result.error = ErrorData(ExceptionType::TRANSACTION, message);
		return result;
	}
	if (status == "exhausted" && result.last_attempt < retry_config.max_retry_count) {
		// stopped before the retry count; client continues
		result.status = DuckLakeServerAttemptStatus::CLIENT;
		result.last_attempt++;
		result.error = ErrorData(ExceptionType::TRANSACTION, status + ": " + detail);
		return result;
	}
	if (status == "exhausted") {
		result.status = DuckLakeServerAttemptStatus::FAILED;
		result.finished_retrying = true;
		result.error = ErrorData(
		    ExceptionType::TRANSACTION,
		    "Failed to flush changes into DuckLake: the server-side commit retries were exhausted: " + detail);
		return result;
	}
	if (status == "fallback" || status == "error" || status == "disabled") {
		if (status == "disabled") {
			// the server runs no READ COMMITTED autocommit
			transaction.GetCatalog().SetServerCommitMode(DuckLakeServerCommitMode::NONE);
			result.last_attempt = 0;
		}
		result.status = DuckLakeServerAttemptStatus::CLIENT;
		result.error = ErrorData(ExceptionType::TRANSACTION, status + ": " + detail);
		return result;
	}
	result.error = ErrorData(ExceptionType::INVALID_INPUT, "ducklake_commit_v1 returned status " + status);
	return result;
}

DuckLakeServerAttempt PostgresMetadataManager::ServerCommitAttempt(PostgresServerCommitState &state,
                                                                   DuckLakeSnapshot transaction_snapshot,
                                                                   const TransactionChangeInformation &changes,
                                                                   const DuckLakeRetryConfig &retry_config) {
	state.attempted = true;
	auto &catalog = transaction.GetCatalog();
	DuckLakeServerAttempt result;
	if (catalog.GetServerCommitMode() != DuckLakeServerCommitMode::PG_FUNCTION ||
	    !transaction.IsAppendOnlyCommit(changes)) {
		return result;
	}
	string remote_sql;
	string log_sql;
	try {
		// render from the first attempt's cached state
		transaction.SetSnapshot(transaction_snapshot);
		SetInlinedTableNames(state.inlined_table_names, false);
#if defined(DEBUG) || defined(DUCKDB_FORCE_ASSERT)
		bool check_invariance = true;
#else
		bool check_invariance = false;
#endif
		DuckLakeRelativeCommit relative;
		string body;
		std::tie(relative, body) = RenderRelativeTemplate(transaction_snapshot, changes, check_invariance);
		auto metadata_catalog = SQLQuotedIdentifier::ToString(catalog.MetadataSchemaName());
		auto catalog_version = DuckLakeVersionToString(catalog.GetDuckLakeVersion());
		remote_sql = PostgresServerCommit::CommitCallSql(metadata_catalog, transaction_snapshot.snapshot_id,
		                                                 catalog_version, relative, body, false, retry_config);
		log_sql = ServerCommitCallSql(PostgresServerCommit::CommitCallSql(
		    metadata_catalog, transaction_snapshot.snapshot_id, catalog_version, relative, body, true, retry_config));
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (!IsRenderFallback(error, false)) {
			result.status = DuckLakeServerAttemptStatus::FAILED;
			result.error = std::move(error);
			return result;
		}
		return ServerCommitHandBack(error, 0);
	}
	try {
		// the call must start its own transaction
		transaction.ResetCommitAttempt();
	} catch (std::exception &ex) {
		result.status = DuckLakeServerAttemptStatus::FAILED;
		result.error = ErrorData(ex);
		return result;
	}

	auto &connection = transaction.GetConnection();
	auto start = std::chrono::steady_clock::now();
	auto log_call = [&]() {
		// logging never changes the outcome
		try {
			transaction.LogMetadataQuery(log_sql, std::chrono::steady_clock::now() - start);
		} catch (std::exception &) { // NOLINT
		}
	};
	unique_ptr<PreparedStatement> prepared;
	ErrorData prepare_error;
	try {
		// Parse and Describe never run the function
		prepared = connection.Prepare(ServerCommitCallSql(remote_sql));
		if (prepared->HasError()) {
			prepare_error = prepared->GetErrorObject();
		}
	} catch (std::exception &ex) {
		prepare_error = ErrorData(ex);
	}
	if (prepare_error.HasError()) {
		log_call();
		ErrorData error(prepare_error.Type(),
		                PostgresServerCommit::RedactCommitCallError(prepare_error.RawMessage(), remote_sql));
		if (error.Type() == ExceptionType::INTERRUPT) {
			result.status = DuckLakeServerAttemptStatus::FAILED;
			result.error = std::move(error);
			return result;
		}
		if (DisablesServerCommit(error.RawMessage())) {
			catalog.SetServerCommitMode(DuckLakeServerCommitMode::NONE);
		}
		return ServerCommitHandBack(error, 0);
	}

	// from here on the function may have run
	ErrorData execute_error;
	try {
		auto query_result = prepared->Execute();
		if (query_result->HasError()) {
			execute_error = query_result->GetErrorObject();
		} else {
			result = ServerCommitStatus(*query_result, retry_config);
		}
	} catch (std::exception &ex) {
		execute_error = ErrorData(ex);
	}
	log_call();
	if (execute_error.HasError()) {
		ErrorData error(execute_error.Type(),
		                PostgresServerCommit::RedactCommitCallError(execute_error.RawMessage(), remote_sql));
		if (CommitCallNotPermitted(execute_error.RawMessage(), remote_sql)) {
			catalog.SetServerCommitMode(DuckLakeServerCommitMode::NONE);
			return ServerCommitHandBack(error, 0);
		}
		result.status = DuckLakeServerAttemptStatus::OUTCOME_UNKNOWN;
		result.error = std::move(error);
		return result;
	}
	if (result.status == DuckLakeServerAttemptStatus::COMMITTED &&
	    retry_config.debug_server_commit_fault == "ack_lost") {
		result.status = DuckLakeServerAttemptStatus::OUTCOME_UNKNOWN;
		result.error = ErrorData(ExceptionType::IO, "debug fault: the reply of the committed call was lost");
	}
	if (result.status != DuckLakeServerAttemptStatus::CLIENT) {
		return result;
	}
	return ServerCommitHandBack(
	    ErrorData(result.error.Type(), PostgresServerCommit::RedactCommitCallError(result.error.RawMessage(), "")),
	    result.last_attempt);
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id,
		 (SELECT MAX(value) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_metadata WHERE key = ''version'')
		 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		 );')
	)";
}

string PostgresMetadataManager::GenerateFileListQuery(DuckLakeTableEntry &table, const FilterPushdownInfo *filter_info,
                                                      const vector<DuckLakeFileListDynamicFilter> &dynamic_filters,
                                                      const vector<idx_t> &runtime_filter_stats_columns,
                                                      FileListType file_list_type, const string &) {
	auto remote_query = DuckLakeMetadataManager::GenerateFileListQuery(
	    table, filter_info, dynamic_filters, runtime_filter_stats_columns, file_list_type, "{METADATA_SCHEMA_ESCAPED}");

	return StringUtil::Format("SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, %s)",
	                          SQLString(remote_query));
}

} // namespace duckdb
