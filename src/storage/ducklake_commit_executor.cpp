#include "storage/ducklake_commit_executor.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_result.hpp"
#include "storage/ducklake_commit_retry.hpp"
#include "storage/ducklake_transaction_changes.hpp"

#include <sstream>

namespace duckdb {

namespace {

string IntLit(int64_t v) {
	return std::to_string(v);
}

} // namespace

DuckLakeCommitExecutor::DuckLakeCommitExecutor(ClientContext &context_p, string uuid_p)
    : context(context_p), connection(make_uniq<Connection>(*context_p.db)), uuid(std::move(uuid_p)) {
}

string DuckLakeCommitExecutor::TempTableName(DuckLakeCommitKind kind) const {
	return DuckLakeCommitTempTables::TableName(uuid, kind);
}

string DuckLakeCommitExecutor::BuildTrivialInsertSQL(DuckLakeCommitKind kind) {
	if (!IsPopulated(kind)) {
		return "";
	}
	auto &spec = GetCommitTableSpec(kind);
	return string("INSERT INTO ") + spec.real_table + " (" + spec.insert_columns + ") SELECT " + spec.insert_columns +
	       " FROM " + TempTableName(kind) + ";";
}

unique_ptr<QueryResult> DuckLakeCommitExecutor::RunQuery(const string &sql) {
	return connection->Query(sql);
}

bool DuckLakeCommitExecutor::IsPopulated(DuckLakeCommitKind kind) const {
	return populated_table_names.find(TempTableName(kind)) != populated_table_names.end();
}

DuckLakeCommitExecutor::LatestSnapshot DuckLakeCommitExecutor::ReadLatestSnapshot() {
	string query = "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id "
	               "FROM ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1;";
	auto result = RunQuery(query);
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

namespace {

template <class T>
void ConflictIntersect(const set<T> &lhs, const set<T> &rhs, const char *action, const char *conflict_action) {
	for (auto &idx : lhs) {
		if (rhs.find(idx) != rhs.end()) {
			throw TransactionException("Transaction conflict - attempting to %s with index \"%d\""
			                           " - but another transaction has %s",
			                           action, idx.index, conflict_action);
		}
	}
}

template <class V>
void ConflictIntersectKey(const case_insensitive_map_t<V> &lhs, const case_insensitive_map_t<V> &rhs,
                          const char *action, const char *conflict_action) {
	for (auto &entry : lhs) {
		if (rhs.find(entry.first) != rhs.end()) {
			throw TransactionException("Transaction conflict - attempting to %s with name \"%s\""
			                           " - but another transaction has %s",
			                           action, entry.first, conflict_action);
		}
	}
}

void RunSemanticConflictCheck(const SnapshotChangeInformation &ours, const SnapshotChangeInformation &other) {
	ConflictIntersect(ours.dropped_tables, other.dropped_tables, "drop table", "dropped it already");
	ConflictIntersect(ours.dropped_views, other.dropped_views, "drop view", "dropped it already");
	ConflictIntersect(ours.dropped_scalar_macros, other.dropped_scalar_macros, "drop macro", "dropped it already");
	ConflictIntersect(ours.dropped_table_macros, other.dropped_table_macros, "drop macro", "dropped it already");
	ConflictIntersect(ours.dropped_schemas, other.dropped_schemas, "drop schema", "dropped it already");

	for (auto &entry : ours.created_schemas) {
		if (other.created_schemas.find(entry) != other.created_schemas.end()) {
			throw TransactionException("Transaction conflict - attempting to create schema \"%s\""
			                           " - but another transaction has created a schema with this name already",
			                           entry);
		}
	}
	ConflictIntersectKey(ours.created_tables, other.created_tables, "create table", "created it already");
	ConflictIntersectKey(ours.created_scalar_macros, other.created_scalar_macros, "create macro", "created it already");
	ConflictIntersectKey(ours.created_table_macros, other.created_table_macros, "create macro", "created it already");

	ConflictIntersect(ours.inserted_tables, other.dropped_tables, "insert into table", "dropped it");
	ConflictIntersect(ours.inserted_tables, other.altered_tables, "insert into table", "altered it");
	ConflictIntersect(ours.inserted_tables, other.tables_deleted_from, "insert into table", "deleted from it");
	ConflictIntersect(ours.inserted_tables, other.tables_deleted_inlined, "insert into table",
	                  "deleted inlined data from it");

	ConflictIntersect(ours.tables_inserted_inlined, other.dropped_tables, "insert into table", "dropped it");
	ConflictIntersect(ours.tables_inserted_inlined, other.altered_tables, "insert into table", "altered it");
	ConflictIntersect(ours.tables_inserted_inlined, other.tables_deleted_from, "insert into table", "deleted from it");
	ConflictIntersect(ours.tables_inserted_inlined, other.tables_deleted_inlined, "insert into table",
	                  "deleted inlined data from it");

	ConflictIntersect(ours.tables_deleted_from, other.dropped_tables, "delete from table", "dropped it");
	ConflictIntersect(ours.tables_deleted_from, other.altered_tables, "delete from table", "altered it");
	ConflictIntersect(ours.tables_deleted_from, other.tables_merge_adjacent, "delete from table", "compacted it");
	ConflictIntersect(ours.tables_deleted_from, other.tables_rewrite_delete, "delete from table", "compacted it");
	ConflictIntersect(ours.tables_deleted_from, other.inserted_tables, "delete from table", "inserted into it");
	ConflictIntersect(ours.tables_deleted_from, other.tables_inserted_inlined, "delete from table", "inserted into it");

	ConflictIntersect(ours.tables_deleted_inlined, other.dropped_tables, "delete from table", "dropped it");
	ConflictIntersect(ours.tables_deleted_inlined, other.altered_tables, "delete from table", "altered it");
	ConflictIntersect(ours.tables_deleted_inlined, other.tables_deleted_inlined, "delete from table",
	                  "deleted from it");
	ConflictIntersect(ours.tables_deleted_inlined, other.tables_flushed_inlined, "delete from table",
	                  "flushed the inlined data");
	ConflictIntersect(ours.tables_deleted_inlined, other.inserted_tables, "delete from table", "inserted into it");
	ConflictIntersect(ours.tables_deleted_inlined, other.tables_inserted_inlined, "delete from table",
	                  "inserted into it");

	ConflictIntersect(ours.tables_flushed_inlined, other.dropped_tables, "flush inline data", "dropped it");
	ConflictIntersect(ours.tables_flushed_inlined, other.tables_deleted_inlined, "flush inline data",
	                  "deleted from it");
	ConflictIntersect(ours.tables_flushed_inlined, other.tables_flushed_inlined, "flush inline data", "flushed it");

	ConflictIntersect(ours.tables_merge_adjacent, other.dropped_tables, "compact table", "dropped it");
	ConflictIntersect(ours.tables_merge_adjacent, other.tables_deleted_from, "compact table", "deleted from it");
	ConflictIntersect(ours.tables_merge_adjacent, other.tables_merge_adjacent, "compact table", "compacted it");
	ConflictIntersect(ours.tables_merge_adjacent, other.tables_rewrite_delete, "compact table", "compacted it");

	ConflictIntersect(ours.tables_rewrite_delete, other.dropped_tables, "compact table", "dropped it");
	ConflictIntersect(ours.tables_rewrite_delete, other.tables_deleted_from, "compact table", "deleted from it");
	ConflictIntersect(ours.tables_rewrite_delete, other.tables_merge_adjacent, "compact table", "compacted it");
	ConflictIntersect(ours.tables_rewrite_delete, other.tables_rewrite_delete, "compact table", "compacted it");

	ConflictIntersect(ours.altered_tables, other.dropped_tables, "alter table", "dropped it");
	ConflictIntersect(ours.altered_tables, other.altered_tables, "alter table", "altered it");
	ConflictIntersect(ours.altered_views, other.altered_views, "alter view", "altered it");
}

} // namespace

void DuckLakeCommitExecutor::CheckForSemanticConflicts(int64_t snapshot_id_was) {
	if (!IsPopulated(DuckLakeCommitKind::SNAPSHOT_CHANGES)) {
		return;
	}
	auto temp = TempTableName(DuckLakeCommitKind::SNAPSHOT_CHANGES);
	auto our_result = RunQuery("SELECT changes_made FROM " + temp + ";");
	if (our_result->HasError()) {
		our_result->GetErrorObject().Throw("Failed to read staged snapshot changes for conflict check: ");
	}
	auto our_chunk = our_result->Fetch();
	if (!our_chunk || our_chunk->size() == 0) {
		return;
	}
	auto our_changes_val = our_chunk->GetValue(0, 0);
	if (our_changes_val.IsNull()) {
		return;
	}
	auto ours_str = our_changes_val.GetValue<string>();
	auto ours = SnapshotChangeInformation::ParseChangesMade(ours_str);

	bool another_committer_deleted = false;
	std::ostringstream others_sql;
	others_sql << "SELECT changes_made FROM ducklake_snapshot_changes "
	              "WHERE snapshot_id > "
	           << snapshot_id_was << " AND changes_made IS NOT NULL;";
	auto others_result = RunQuery(others_sql.str());
	if (others_result->HasError()) {
		others_result->GetErrorObject().Throw("Failed to read other snapshot changes for conflict check: ");
	}
	while (auto chunk = others_result->Fetch()) {
		for (idx_t i = 0; i < chunk->size(); i++) {
			auto val = chunk->GetValue(0, i);
			if (val.IsNull()) {
				continue;
			}
			auto other_str = val.GetValue<string>();
			auto other = SnapshotChangeInformation::ParseChangesMade(other_str);
			RunSemanticConflictCheck(ours, other);
			if (!other.tables_deleted_from.empty()) {
				another_committer_deleted = true;
			}
		}
	}

	if (!ours.tables_deleted_from.empty() && another_committer_deleted &&
	    IsPopulated(DuckLakeCommitKind::DELETE_FILES)) {
		auto temp = TempTableName(DuckLakeCommitKind::DELETE_FILES);
		std::ostringstream sql;
		sql << "SELECT t.data_file_id FROM " << temp << " t WHERE t.data_file_id IN ("
		    << "SELECT data_file_id FROM ducklake_delete_file WHERE begin_snapshot > " << snapshot_id_was
		    << " UNION ALL "
		    << "SELECT data_file_id FROM ducklake_data_file WHERE end_snapshot IS NOT NULL AND end_snapshot > "
		    << snapshot_id_was << ") LIMIT 1;";
		auto conflict_res = RunQuery(sql.str());
		if (conflict_res->HasError()) {
			conflict_res->GetErrorObject().Throw("Failed to check file-level delete conflicts: ");
		}
		auto conflict_chunk = conflict_res->Fetch();
		if (conflict_chunk && conflict_chunk->size() > 0) {
			auto conflicting_id = conflict_chunk->GetValue(0, 0).GetValue<int64_t>();
			throw TransactionException("Transaction conflict - attempting to delete from file with index \"%lld\""
			                           " - but another transaction has deleted from it",
			                           (long long)conflicting_id);
		}
	}
}

void DuckLakeCommitExecutor::CheckForConflictsFromTempTables(DuckLakeCommitMeta &meta, const LatestSnapshot &latest) {
	int64_t file_delta = latest.next_file_id - meta.next_file_id_baseline;
	if (file_delta > 0) {
		struct FileShift {
			DuckLakeCommitKind kind;
			const char *column;
		};
		const FileShift shifts[] = {
		    {DuckLakeCommitKind::DATA_FILES, "data_file_id"},
		    {DuckLakeCommitKind::FILE_COLUMN_STATS, "data_file_id"},
		    {DuckLakeCommitKind::DELETE_FILES, "delete_file_id"},
		    {DuckLakeCommitKind::DELETE_FILES, "data_file_id"},
		};
		for (auto &s : shifts) {
			if (!IsPopulated(s.kind)) {
				continue;
			}
			std::ostringstream sql;
			sql << "UPDATE " << TempTableName(s.kind) << " SET " << s.column << " = " << s.column << " + " << file_delta
			    << " WHERE " << s.column << " >= " << meta.next_file_id_baseline << ";";
			auto res = RunQuery(sql.str());
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to remap file ids during retry: ");
			}
		}
		meta.next_file_id += file_delta;
		meta.next_file_id_baseline = latest.next_file_id;
	}

	int64_t catalog_delta = latest.next_catalog_id - meta.next_catalog_id_baseline;
	if (catalog_delta > 0) {
		struct CatalogShift {
			DuckLakeCommitKind kind;
			const char *column;
		};
		const CatalogShift shifts[] = {
		    {DuckLakeCommitKind::SCHEMAS, "schema_id"},
		    {DuckLakeCommitKind::TABLES, "table_id"},
		    {DuckLakeCommitKind::VIEWS, "view_id"},
		    {DuckLakeCommitKind::COLUMNS, "column_id"},
		    {DuckLakeCommitKind::PARTITION_INFO, "partition_id"},
		    {DuckLakeCommitKind::PARTITION_COLUMN, "partition_id"},
		    {DuckLakeCommitKind::SORT_INFO, "sort_id"},
		    {DuckLakeCommitKind::SORT_EXPRESSION, "sort_id"},
		    {DuckLakeCommitKind::COLUMN_MAPPING, "mapping_id"},
		    {DuckLakeCommitKind::NAME_MAPPING, "mapping_id"},
		};
		for (auto &s : shifts) {
			if (!IsPopulated(s.kind)) {
				continue;
			}
			std::ostringstream sql;
			sql << "UPDATE " << TempTableName(s.kind) << " SET " << s.column << " = " << s.column << " + "
			    << catalog_delta << " WHERE " << s.column << " >= " << meta.next_catalog_id_baseline << ";";
			auto res = RunQuery(sql.str());
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to remap catalog ids during retry: ");
			}
		}
		meta.next_catalog_id += catalog_delta;
		meta.next_catalog_id_baseline = latest.next_catalog_id;
	}
}

DuckLakeCommitResult DuckLakeCommitExecutor::Execute(const string &staging_sql, const string &populated_kinds,
                                                     const DuckLakeCommitMeta *inline_meta,
                                                     const DuckLakeRetryConfig *inline_retry_config) {
	if (!staging_sql.empty()) {
		auto stage_res = RunQuery(staging_sql);
		if (stage_res->HasError()) {
			stage_res->GetErrorObject().Throw("ducklake_commit: failed to stage commit payload: ");
		}
	}
	if (populated_kinds.empty()) {
		throw InvalidInputException("ducklake_commit: populated_kinds must be provided");
	}
	populated_table_names.clear();
	size_t pos = 0;
	while (pos < populated_kinds.size()) {
		auto comma = populated_kinds.find(',', pos);
		auto raw = populated_kinds.substr(pos, comma == string::npos ? string::npos : comma - pos);
		auto dot = raw.find('.');
		auto name = dot == string::npos ? raw : raw.substr(dot + 1);
		if (!name.empty()) {
			populated_table_names.insert(std::move(name));
		}
		if (comma == string::npos) {
			break;
		}
		pos = comma + 1;
	}
	if (!inline_meta) {
		throw InvalidInputException("ducklake_commit: meta args must be provided");
	}
	DuckLakeCommitMeta meta = *inline_meta;

	auto retry_config = inline_retry_config ? *inline_retry_config : DuckLakeRetryConfig::LoadFromContext(context);
	const idx_t max_retry = retry_config.max_retry_count;

	DuckLakeCommitResult final_result;
	bool finished = false;
	std::exception_ptr pending_error;
	for (idx_t i = 0; i < max_retry + 1 && !finished; i++) {
		bool can_retry = false;
		try {
			auto latest = ReadLatestSnapshot();
			if (latest.snapshot_id > meta.snapshot_id_was) {
				CheckForSemanticConflicts(meta.snapshot_id_was);
			}
			if (i > 0) {
				CheckForConflictsFromTempTables(meta, latest);
			}

			int64_t new_snapshot_id = latest.snapshot_id + 1;
			int64_t new_schema_version = latest.schema_version + (meta.schema_changed ? 1 : 0);
			int64_t next_catalog_id = meta.next_catalog_id;
			int64_t next_file_id = meta.next_file_id;

			can_retry = true;
			string batch = "BEGIN TRANSACTION;";
			batch += BuildCommitBatch(new_snapshot_id, new_schema_version);
			std::ostringstream snap_insert;
			snap_insert << "INSERT INTO ducklake_snapshot VALUES (" << new_snapshot_id << ", NOW(), "
			            << new_schema_version << ", " << next_catalog_id << ", " << next_file_id << ");";
			batch += snap_insert.str();
			batch += "COMMIT;";
			for (auto &name : populated_table_names) {
				batch += "DELETE FROM " + name + ";";
			}

			auto res = RunQuery(batch);
			if (res->HasError()) {
				res->GetErrorObject().Throw("Failed to flush changes into DuckLake: ");
			}

			final_result.snapshot_id = new_snapshot_id;
			final_result.schema_version = new_schema_version;
			final_result.retry_count = static_cast<int32_t>(i);
			final_result.committed_next_catalog_id = next_catalog_id;
			final_result.committed_next_file_id = next_file_id;
			finished = true;
		} catch (std::exception &ex) {
			ErrorData error(ex);
			try {
				RunQuery("ROLLBACK;");
			} catch (...) {
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
				try {
					error.Throw(error_message.str());
				} catch (...) {
					pending_error = std::current_exception();
				}
				break;
			}
		}
	}
	if (!finished) {
		DropStagedTables();
	}
	if (pending_error) {
		std::rethrow_exception(pending_error);
	}
	if (!finished) {
		throw InternalException("DuckLakeCommitExecutor::Execute: unreachable");
	}
	return final_result;
}

void DuckLakeCommitExecutor::DropStagedTables() {
	if (populated_table_names.empty()) {
		return;
	}
	string clear_batch;
	for (auto &name : populated_table_names) {
		clear_batch += "DELETE FROM " + name + ";";
	}
	auto res = RunQuery(clear_batch);
	if (res && res->HasError()) {
		fprintf(stderr, "ducklake_commit: warning failed to clear staged tables: %s\n", res->GetError().c_str());
	}
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
	batch += InsertFileVariantStatsSQL();
	batch += InsertFilePartitionValuesSQL();
	batch += OverwriteDeleteFilesSQL();
	batch += InsertDeleteFilesSQL(new_snapshot_id);
	batch += DropDataFilesSQL(new_snapshot_id);
	batch += InsertTableStatsSQL();
	batch += InsertTableColumnStatsSQL();

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
	sql += "UPDATE ducklake_table SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND table_id IN " + sub + ";";
	if (!renamed) {
		for (const char *child : {"ducklake_partition_info", "ducklake_column", "ducklake_column_tag",
		                          "ducklake_data_file", "ducklake_delete_file", "ducklake_tag", "ducklake_sort_info"}) {
			sql += "UPDATE ";
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
	sql += "UPDATE ducklake_view SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND view_id IN " + sub + ";";
	if (!renamed) {
		sql += "UPDATE ducklake_tag SET end_snapshot = " + IntLit(snapshot_id) +
		       " WHERE end_snapshot IS NULL AND object_id IN " + sub + ";";
	}
	return sql;
}

string DuckLakeCommitExecutor::DropSchemasSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_SCHEMAS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_SCHEMAS);
	return "UPDATE ducklake_schema SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND schema_id IN (SELECT schema_id FROM " + temp + ");";
}

string DuckLakeCommitExecutor::DropColumnsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_COLUMNS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_COLUMNS);
	return "UPDATE ducklake_column SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND (table_id, column_id) IN "
	       "(SELECT table_id, column_id FROM " +
	       temp + ");";
}

string DuckLakeCommitExecutor::DropDataFilesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DROPPED_DATA_FILES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DROPPED_DATA_FILES);
	return "UPDATE ducklake_data_file SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND data_file_id IN (SELECT data_file_id FROM " + temp + ");";
}

string DuckLakeCommitExecutor::InsertSchemasSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::SCHEMAS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::SCHEMAS);
	return "INSERT INTO ducklake_schema "
	       "(schema_id, schema_uuid, begin_snapshot, end_snapshot, schema_name, path, path_is_relative) "
	       "SELECT schema_id, schema_uuid, " +
	       IntLit(snapshot_id) + ", NULL, schema_name, path, path_is_relative FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertTablesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::TABLES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::TABLES);
	return "INSERT INTO ducklake_table "
	       "(table_id, table_uuid, begin_snapshot, end_snapshot, schema_id, table_name, path, path_is_relative) "
	       "SELECT table_id, table_uuid, " +
	       IntLit(snapshot_id) + ", NULL, schema_id, table_name, path, path_is_relative FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertViewsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::VIEWS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::VIEWS);
	return "INSERT INTO ducklake_view "
	       "(view_id, view_uuid, begin_snapshot, end_snapshot, schema_id, view_name, dialect, sql, column_aliases) "
	       "SELECT view_id, view_uuid, " +
	       IntLit(snapshot_id) + ", NULL, schema_id, view_name, dialect, sql, column_aliases FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertColumnsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::COLUMNS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::COLUMNS);
	return "INSERT INTO ducklake_column "
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
	return "INSERT INTO ducklake_partition_info "
	       "(partition_id, table_id, begin_snapshot, end_snapshot) "
	       "SELECT partition_id, table_id, " +
	       IntLit(snapshot_id) + ", NULL FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertPartitionColumnSQL() {
	return BuildTrivialInsertSQL(DuckLakeCommitKind::PARTITION_COLUMN);
}

string DuckLakeCommitExecutor::InsertSortInfoSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::SORT_INFO)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::SORT_INFO);
	return "INSERT INTO ducklake_sort_info "
	       "(sort_id, table_id, begin_snapshot, end_snapshot) "
	       "SELECT sort_id, table_id, " +
	       IntLit(snapshot_id) + ", NULL FROM " + temp + ";";
}

string DuckLakeCommitExecutor::InsertSortExpressionSQL() {
	return BuildTrivialInsertSQL(DuckLakeCommitKind::SORT_EXPRESSION);
}

string DuckLakeCommitExecutor::ExpireAndInsertTagsSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::TAGS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::TAGS);
	string sql;
	sql += "UPDATE ducklake_tag SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND (object_id, key) IN "
	       "(SELECT object_id, key FROM " +
	       temp + ");";
	sql += "INSERT INTO ducklake_tag "
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
	sql += "UPDATE ducklake_column_tag SET end_snapshot = " + IntLit(snapshot_id) +
	       " WHERE end_snapshot IS NULL AND (table_id, column_id, key) IN "
	       "(SELECT table_id, column_id, key FROM " +
	       temp + ");";
	sql += "INSERT INTO ducklake_column_tag "
	       "(table_id, column_id, begin_snapshot, end_snapshot, key, value) "
	       "SELECT table_id, column_id, " +
	       IntLit(snapshot_id) + ", NULL, key, value FROM " + temp + ";";
	return sql;
}

string DuckLakeCommitExecutor::InsertColumnMappingSQL() {
	return BuildTrivialInsertSQL(DuckLakeCommitKind::COLUMN_MAPPING);
}

string DuckLakeCommitExecutor::InsertNameMappingSQL() {
	return BuildTrivialInsertSQL(DuckLakeCommitKind::NAME_MAPPING);
}

string DuckLakeCommitExecutor::InsertInlinedTablesSQL() {
	return BuildTrivialInsertSQL(DuckLakeCommitKind::INLINED_TABLES);
}

string DuckLakeCommitExecutor::InsertDataFilesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DATA_FILES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DATA_FILES);
	return "INSERT INTO ducklake_data_file "
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
	return BuildTrivialInsertSQL(DuckLakeCommitKind::FILE_COLUMN_STATS);
}

string DuckLakeCommitExecutor::InsertFileVariantStatsSQL() {
	return BuildTrivialInsertSQL(DuckLakeCommitKind::FILE_VARIANT_STATS);
}

string DuckLakeCommitExecutor::InsertDeleteFilesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::DELETE_FILES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::DELETE_FILES);
	return "INSERT INTO ducklake_delete_file "
	       "(delete_file_id, table_id, begin_snapshot, end_snapshot, data_file_id, path, path_is_relative, "
	       "format, delete_count, file_size_bytes, footer_size, encryption_key, partial_max) "
	       "SELECT delete_file_id, table_id, COALESCE(begin_snapshot_override, " +
	       IntLit(snapshot_id) +
	       "), NULL, data_file_id, path, path_is_relative, format, delete_count, file_size_bytes, "
	       "footer_size, encryption_key, partial_max FROM " +
	       temp + ";";
}

string DuckLakeCommitExecutor::InsertFilePartitionValuesSQL() {
	return BuildTrivialInsertSQL(DuckLakeCommitKind::FILE_PARTITION_VALUES);
}

string DuckLakeCommitExecutor::OverwriteDeleteFilesSQL() {
	if (!IsPopulated(DuckLakeCommitKind::OVERWRITTEN_DELETE_FILES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::OVERWRITTEN_DELETE_FILES);
	string sql;
	sql += "INSERT INTO ducklake_files_scheduled_for_deletion (data_file_id, path, path_is_relative, schedule_start) "
	       "SELECT delete_file_id, path, path_is_relative, NOW() FROM " +
	       temp + ";";
	sql += "DELETE FROM ducklake_delete_file WHERE delete_file_id IN (SELECT delete_file_id FROM " + temp + ");";
	return sql;
}

string DuckLakeCommitExecutor::InsertTableStatsSQL() {
	if (!IsPopulated(DuckLakeCommitKind::TABLE_STATS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::TABLE_STATS);
	string sql;
	sql += "DELETE FROM ducklake_table_stats WHERE table_id IN "
	       "(SELECT table_id FROM " +
	       temp + ");";
	sql += "INSERT INTO ducklake_table_stats "
	       "(table_id, record_count, next_row_id, file_size_bytes) "
	       "SELECT table_id, record_count, next_row_id, file_size_bytes FROM " +
	       temp + ";";
	return sql;
}

string DuckLakeCommitExecutor::InsertTableColumnStatsSQL() {
	if (!IsPopulated(DuckLakeCommitKind::TABLE_COLUMN_STATS)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::TABLE_COLUMN_STATS);
	string sql;
	sql += "DELETE FROM ducklake_table_column_stats WHERE (table_id, column_id) IN "
	       "(SELECT table_id, column_id FROM " +
	       temp + ");";
	sql += "INSERT INTO ducklake_table_column_stats "
	       "(table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats) "
	       "SELECT table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats FROM " +
	       temp + ";";
	return sql;
}

string DuckLakeCommitExecutor::InsertSnapshotChangesSQL(int64_t snapshot_id) {
	if (!IsPopulated(DuckLakeCommitKind::SNAPSHOT_CHANGES)) {
		return "";
	}
	auto temp = TempTableName(DuckLakeCommitKind::SNAPSHOT_CHANGES);
	return "INSERT INTO ducklake_snapshot_changes "
	       "(snapshot_id, changes_made, author, commit_message, commit_extra_info) "
	       "SELECT " +
	       IntLit(snapshot_id) + ", changes_made, author, commit_message, commit_extra_info FROM " + temp + ";";
}

} // namespace duckdb
