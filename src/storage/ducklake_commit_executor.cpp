#include "storage/ducklake_commit_executor.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/main/query_result.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_commit_retry.hpp"
#include "storage/ducklake_transaction.hpp"

#include <sstream>

namespace duckdb {

namespace {

string IntLit(int64_t v) {
	return std::to_string(v);
}

} // namespace

DuckLakeCommitExecutor::DuckLakeCommitExecutor(ClientContext &context_p, Catalog &catalog_p, string uuid_p)
    : context(context_p), catalog(catalog_p), transaction(DuckLakeTransaction::Get(context_p, catalog_p)),
      uuid(std::move(uuid_p)) {
}

string DuckLakeCommitExecutor::TempTableName(DuckLakeCommitKind kind) const {
	return DuckLakeCommitTempTables::TableName(uuid, kind);
}

bool DuckLakeCommitExecutor::IsPopulated(DuckLakeCommitKind kind) const {
	return populated_table_names.find(TempTableName(kind)) != populated_table_names.end();
}

void DuckLakeCommitExecutor::RefreshPopulatedKinds() {
	populated_table_names.clear();
	auto sanitized = DuckLakeCommitTempTables::SanitizeUUID(uuid);
	string prefix = "_dl_commit_" + sanitized + "_";
	string query = "SELECT table_name FROM information_schema.tables WHERE table_name LIKE '" + prefix + "%';";
	auto result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to probe ducklake commit temp tables: ");
	}
	for (auto &row : *result) {
		populated_table_names.insert(row.GetValue<string>(0));
	}
}

DuckLakeCommitExecutor::CommitMeta DuckLakeCommitExecutor::ReadMeta() {
	auto meta_table = TempTableName(DuckLakeCommitKind::META);
	string query =
	    "SELECT snapshot_id_was, schema_version_was, schema_changed, has_inlined_flush FROM " + meta_table + ";";
	auto result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to read ducklake commit meta: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw InvalidInputException("ducklake_commit: meta temp table %s must contain exactly one row", meta_table);
	}
	CommitMeta meta;
	meta.snapshot_id_was = chunk->GetValue(0, 0).GetValue<int64_t>();
	meta.schema_version_was = chunk->GetValue(1, 0).GetValue<int64_t>();
	meta.schema_changed = chunk->GetValue(2, 0).GetValue<bool>();
	meta.has_inlined_flush = chunk->GetValue(3, 0).GetValue<bool>();
	return meta;
}

DuckLakeCommitExecutor::LatestSnapshot DuckLakeCommitExecutor::ReadLatestSnapshot() {
	string query = "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id "
	               "FROM {METADATA_CATALOG}.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;";
	auto result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to read latest ducklake snapshot: ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() != 1) {
		throw InternalException("ducklake_commit: catalog has no snapshot rows");
	}
	LatestSnapshot snap;
	snap.snapshot_id = chunk->GetValue(0, 0).GetValue<int64_t>();
	snap.schema_version = chunk->GetValue(1, 0).GetValue<int64_t>();
	snap.next_catalog_id = chunk->GetValue(2, 0).GetValue<int64_t>();
	snap.next_file_id = chunk->GetValue(3, 0).GetValue<int64_t>();
	return snap;
}

bool DuckLakeCommitExecutor::CheckForConflictsFromTempTables(int64_t, int64_t) {
	// Step 8 fills this in.
	return false;
}

DuckLakeCommitResult DuckLakeCommitExecutor::Execute() {
	RefreshPopulatedKinds();
	auto meta = ReadMeta();

	auto retry_config = DuckLakeRetryConfig::LoadFromContext(context);
	const idx_t max_retry = retry_config.max_retry_count;

	auto &connection = transaction.GetConnection();

	for (idx_t i = 0; i < max_retry + 1; i++) {
		bool can_retry = false;
		try {
			if (i > 0) {
				(void)CheckForConflictsFromTempTables(meta.snapshot_id_was, meta.schema_version_was);
			}

			auto latest = ReadLatestSnapshot();
			int64_t new_snapshot_id = latest.snapshot_id + 1;
			int64_t new_schema_version = latest.schema_version + (meta.schema_changed ? 1 : 0);

			// TODO(step 5): bump catalog and file counters.
			int64_t next_catalog_id = latest.next_catalog_id;
			int64_t next_file_id = latest.next_file_id;

			can_retry = true;
			string batch = BuildCommitBatch(new_snapshot_id, new_schema_version);
			std::ostringstream snap_insert;
			snap_insert << "INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES (" << new_snapshot_id << ", NOW(), "
			            << new_schema_version << ", " << next_catalog_id << ", " << next_file_id << ");";
			batch += snap_insert.str();

			auto res = transaction.Query(batch);
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to flush changes into DuckLake: ");
			}

			if (connection.context->transaction.HasActiveTransaction()) {
				connection.Commit();
			}
			// Leave tx open for DuckLakeTransaction::Commit.
			connection.BeginTransaction();

			DuckLakeCommitResult result;
			result.snapshot_id = new_snapshot_id;
			result.schema_version = new_schema_version;
			result.retry_count = static_cast<int32_t>(i);
			return result;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (connection.context->transaction.HasActiveTransaction()) {
				connection.Rollback();
			}
			bool retry_on_error = duckdb::RetryOnError(error.Message());
			bool finished_retrying = i + 1 >= max_retry;
			if (!can_retry || !retry_on_error || finished_retrying) {
				std::ostringstream error_message;
				error_message << "Failed to commit DuckLake transaction." << '\n';
				if (finished_retrying) {
					error_message << "Exceeded the maximum retry count of " << max_retry
					              << " set by the ducklake_max_retry_count setting." << '\n'
					              << ". Consider increasing the value with: e.g., \"SET ducklake_max_retry_count = "
					              << max_retry * 10 << ";\"" << '\n';
				}
				error.Throw(error_message.str());
			}

			SleepBeforeRetry(i, retry_config);
			if (!connection.context->transaction.HasActiveTransaction()) {
				connection.BeginTransaction();
			}
		}
	}
	throw InternalException("DuckLakeCommitExecutor::Execute: unreachable");
}

string DuckLakeCommitExecutor::BuildCommitBatch(int64_t new_snapshot_id, int64_t new_schema_version) {
	string batch;

	batch += DropTablesSQL(new_snapshot_id, false);
	batch += DropTablesSQL(new_snapshot_id, true);
	batch += DropViewsSQL(new_snapshot_id, false);
	batch += DropViewsSQL(new_snapshot_id, true);
	batch += DropSchemasSQL(new_snapshot_id);
	batch += DropColumnsSQL(new_snapshot_id);

	batch += InsertSchemasSQL(new_snapshot_id);
	batch += InsertTablesSQL(new_snapshot_id);
	batch += InsertViewsSQL(new_snapshot_id);
	batch += InsertColumnsSQL(new_snapshot_id);
	batch += InsertPartitionInfoSQL(new_snapshot_id);
	batch += InsertPartitionColumnSQL();
	batch += InsertSortInfoSQL(new_snapshot_id);
	batch += InsertSortExpressionSQL();
	batch += ExpireAndInsertTagsSQL(new_snapshot_id);
	batch += ExpireAndInsertColumnTagsSQL(new_snapshot_id);
	batch += InsertColumnMappingSQL();
	batch += InsertNameMappingSQL();
	batch += InsertInlinedTablesSQL();
	batch += InsertDataFilesSQL(new_snapshot_id);
	batch += InsertFileColumnStatsSQL();
	batch += InsertDeleteFilesSQL(new_snapshot_id);
	batch += DropDataFilesSQL(new_snapshot_id);
	batch += InsertTableStatsSQL();

	batch += InsertSnapshotChangesSQL(new_snapshot_id);

	(void)new_schema_version;
	return batch;
}

string DuckLakeCommitExecutor::DropTablesSQL(int64_t snapshot_id, bool renamed) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_TABLES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_TABLES);
	string where_renamed = renamed ? "renamed = TRUE" : "renamed = FALSE";
	string sub = "(SELECT table_id FROM " + temp + " WHERE " + where_renamed + ")";

	string sql;
	sql += "UPDATE {METADATA_CATALOG}.ducklake_table SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND table_id IN " + sub + ";";
	if (!renamed) {
		// Cascade only when table truly gone.
		for (const char *child : {"ducklake_partition_info", "ducklake_column", "ducklake_column_tag",
		                          "ducklake_data_file", "ducklake_delete_file", "ducklake_tag", "ducklake_sort_info"}) {
			sql += "UPDATE {METADATA_CATALOG}.";
			sql += child;
			sql += " SET end_snapshot = " + IntLit(snapshot_id) + " WHERE end_snapshot IS NULL AND table_id IN " + sub +
			       ";";
		}
	}
	return sql;
}

string DuckLakeCommitExecutor::DropViewsSQL(int64_t snapshot_id, bool renamed) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_VIEWS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_VIEWS);
	string where_renamed = renamed ? "renamed = TRUE" : "renamed = FALSE";
	string sub = "(SELECT view_id FROM " + temp + " WHERE " + where_renamed + ")";

	string sql;
	sql += "UPDATE {METADATA_CATALOG}.ducklake_view SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND view_id IN " + sub + ";";
	if (!renamed) {
		sql += "UPDATE {METADATA_CATALOG}.ducklake_tag SET end_snapshot = " + IntLit(snapshot_id) +
		       " WHERE end_snapshot IS NULL AND object_id IN " + sub + ";";
	}
	return sql;
}

string DuckLakeCommitExecutor::DropSchemasSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_SCHEMAS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_SCHEMAS);
	return "UPDATE {METADATA_CATALOG}.ducklake_schema SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND schema_id IN (SELECT schema_id FROM " + temp + ");";
}

string DuckLakeCommitExecutor::DropColumnsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_COLUMNS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_COLUMNS);
	return "UPDATE {METADATA_CATALOG}.ducklake_column SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND (table_id, column_id) IN "
	       "(SELECT table_id, column_id FROM " +
	       temp + ");";
}

string DuckLakeCommitExecutor::DropDataFilesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_DATA_FILES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_DATA_FILES);
	return "UPDATE {METADATA_CATALOG}.ducklake_data_file SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND data_file_id IN (SELECT data_file_id FROM " + temp + ");";
}

string DuckLakeCommitExecutor::InsertSchemasSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::SCHEMAS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::SCHEMAS);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_schema "
	       "(schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) "
	       "SELECT schema_id, schema_uuid, " +
	       IntLit(snapshot_id) + ", NULL, schema_name, path, path_is_relative FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertTablesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::TABLES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::TABLES);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_table "
	       "(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) "
	       "SELECT table_id, table_uuid, " +
	       IntLit(snapshot_id) + ", NULL, schema_id, table_name, path, path_is_relative FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertViewsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::VIEWS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::VIEWS);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_view "
	       "(view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases) "
	       "SELECT view_id, view_uuid, " +
	       IntLit(snapshot_id) + ", NULL, schema_id, view_name, dialect, sql, column_aliases FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertColumnsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::COLUMNS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::COLUMNS);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_column "
	       "(column_id, begin_snapshot, end_snapshot, table_id, column_order, column_name, column_type, "
	       "initial_default, default_value, nulls_allowed, parent_column, default_value_type, "
	       "default_value_dialect) "
	       "SELECT column_id, " +
	       IntLit(snapshot_id) +
	       ", NULL, table_id, column_order, column_name, column_type, initial_default, default_value, "
	       "nulls_allowed, parent_column, default_value_type, default_value_dialect FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertPartitionInfoSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::PARTITION_INFO)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::PARTITION_INFO);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_partition_info "
	       "(partition_id, table_id, begin_snapshot, end_snapshot) "
	       "SELECT partition_id, table_id, " +
	       IntLit(snapshot_id) + ", NULL FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertPartitionColumnSQL() {
	if (!IsPopulated(DuckLakeCommitKind::PARTITION_COLUMN)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::PARTITION_COLUMN);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_partition_column "
	       "(partition_id, table_id, partition_key_index, column_id, transform) "
	       "SELECT partition_id, table_id, partition_key_index, column_id, transform FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertSortInfoSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::SORT_INFO)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::SORT_INFO);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_sort_info "
	       "(sort_id, table_id, begin_snapshot, end_snapshot) "
	       "SELECT sort_id, table_id, " +
	       IntLit(snapshot_id) + ", NULL FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertSortExpressionSQL() {
	if (!IsPopulated(DuckLakeCommitKind::SORT_EXPRESSION)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::SORT_EXPRESSION);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_sort_expression "
	       "(sort_id, table_id, sort_key_index, expression, dialect, sort_direction, null_order) "
	       "SELECT sort_id, table_id, sort_key_index, expression, dialect, sort_direction, null_order FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::ExpireAndInsertTagsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::TAGS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::TAGS);
	string sql;
	sql += "UPDATE {METADATA_CATALOG}.ducklake_tag SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND (object_id, key) IN "
	       "(SELECT object_id, key FROM " +
	       temp + ");";
	sql += "INSERT INTO {METADATA_CATALOG}.ducklake_tag "
	       "(object_id, begin_snapshot, end_snapshot, key, value) "
	       "SELECT object_id, " +
	       IntLit(snapshot_id) + ", NULL, key, value FROM " + temp + ";";
	return sql;
}

string DuckLakeCommitExecutor::ExpireAndInsertColumnTagsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::COLUMN_TAGS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::COLUMN_TAGS);
	string sql;
	sql += "UPDATE {METADATA_CATALOG}.ducklake_column_tag SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND (table_id, column_id, key) IN "
	       "(SELECT table_id, column_id, key FROM " +
	       temp + ");";
	sql += "INSERT INTO {METADATA_CATALOG}.ducklake_column_tag "
	       "(table_id, column_id, begin_snapshot, end_snapshot, key, value) "
	       "SELECT table_id, column_id, " +
	       IntLit(snapshot_id) + ", NULL, key, value FROM " + temp + ";";
	return sql;
}

string DuckLakeCommitExecutor::InsertColumnMappingSQL() {
	if (!IsPopulated(DuckLakeCommitKind::COLUMN_MAPPING)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::COLUMN_MAPPING);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_column_mapping "
	       "(mapping_id, table_id, type) "
	       "SELECT mapping_id, table_id, type FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertNameMappingSQL() {
	if (!IsPopulated(DuckLakeCommitKind::NAME_MAPPING)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::NAME_MAPPING);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_name_mapping "
	       "(mapping_id, column_id, source_name, target_field_id, parent_column, is_partition) "
	       "SELECT mapping_id, column_id, source_name, target_field_id, parent_column, is_partition FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertInlinedTablesSQL() {
	if (!IsPopulated(DuckLakeCommitKind::INLINED_TABLES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::INLINED_TABLES);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_inlined_data_tables "
	       "(table_id, table_name, schema_version) "
	       "SELECT table_id, table_name, schema_version FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertDataFilesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DATA_FILES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DATA_FILES);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_data_file "
	       "(data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path, path_is_relative, "
	       "file_format, record_count, file_size_bytes, footer_size, row_id_start, partition_id, "
	       "encryption_key, mapping_id, partial_max) "
	       "SELECT data_file_id, table_id, " +
	       IntLit(snapshot_id) +
	       ", NULL, file_order, path, path_is_relative, file_format, record_count, file_size_bytes, "
	       "footer_size, row_id_start, partition_id, encryption_key, mapping_id, partial_max FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertFileColumnStatsSQL() {
	if (!IsPopulated(DuckLakeCommitKind::FILE_COLUMN_STATS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::FILE_COLUMN_STATS);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_file_column_stats "
	       "(data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, "
	       "max_value, contains_nan, extra_stats) "
	       "SELECT data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, min_value, "
	       "max_value, contains_nan, extra_stats FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertDeleteFilesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DELETE_FILES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DELETE_FILES);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_delete_file "
	       "(delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, "
	       "format, delete_count, file_size_bytes, footer_size, encryption_key, partial_max) "
	       "SELECT delete_file_id, table_id, " +
	       IntLit(snapshot_id) +
	       ", NULL, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, "
	       "footer_size, encryption_key, partial_max FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertTableStatsSQL() {
	if (!IsPopulated(DuckLakeCommitKind::TABLE_STATS)) {
		return "";
	}
	// TODO(step 5): promote to UPSERT.
	auto temp = TempTableName(DuckLakeCommitKind::TABLE_STATS);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_table_stats "
	       "(table_id, record_count, next_row_id, file_size_bytes) "
	       "SELECT table_id, record_count, next_row_id, file_size_bytes FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertSnapshotChangesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::SNAPSHOT_CHANGES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::SNAPSHOT_CHANGES);
	return "INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes "
	       "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
	       "SELECT " +
	       IntLit(snapshot_id) + ", changes_made, author, commit_message, commit_extra_info FROM " + temp + ";";
}

} // namespace duckdb
