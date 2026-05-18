//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_commit_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_commit_retry.hpp"
#include "storage/ducklake_commit_temp_tables.hpp"

namespace duckdb {
class ClientContext;

//! The result of a commit
struct DuckLakeCommitResult {
	int64_t snapshot_id = 0;
	int64_t schema_version = 0;
	int32_t retry_count = 0;
	int64_t committed_next_catalog_id = 0;
	int64_t committed_next_file_id = 0;
};

struct DuckLakeCommitMeta {
	int64_t snapshot_id_was = 0;
	int64_t schema_version_was = 0;
	bool schema_changed = false;
	bool has_inlined_flush = false;
	int64_t next_catalog_id = 0;
	int64_t next_file_id = 0;
	int64_t next_catalog_id_baseline = 0;
	int64_t next_file_id_baseline = 0;
};

class DuckLakeCommitExecutor {
public:
	DuckLakeCommitExecutor(ClientContext &context, string uuid);

	DuckLakeCommitResult Execute(const string &staging_sql = "", const string &populated_kinds = "",
	                             const DuckLakeCommitMeta *inline_meta = nullptr,
	                             const DuckLakeRetryConfig *inline_retry_config = nullptr);

private:
	struct LatestSnapshot {
		int64_t snapshot_id = 0;
		int64_t schema_version = 0;
		int64_t next_catalog_id = 0;
		int64_t next_file_id = 0;
	};

	LatestSnapshot ReadLatestSnapshot();
	bool IsPopulated(DuckLakeCommitKind kind) const;
	void CheckForConflictsFromTempTables(DuckLakeCommitMeta &meta, const LatestSnapshot &latest);
	void CheckForSemanticConflicts(int64_t snapshot_id_was);

	string BuildCommitBatch(int64_t new_snapshot_id, int64_t new_schema_version);

	string DropTablesSQL(int64_t snapshot_id, bool renamed);
	string DropViewsSQL(int64_t snapshot_id, bool renamed);
	string DropSchemasSQL(int64_t snapshot_id);
	string DropColumnsSQL(int64_t snapshot_id);
	string DropDataFilesSQL(int64_t snapshot_id);
	string InsertSchemasSQL(int64_t snapshot_id);
	string InsertTablesSQL(int64_t snapshot_id);
	string InsertViewsSQL(int64_t snapshot_id);
	string InsertColumnsSQL(int64_t snapshot_id);
	string InsertPartitionInfoSQL(int64_t snapshot_id);
	string InsertPartitionColumnSQL();
	string InsertSortInfoSQL(int64_t snapshot_id);
	string InsertSortExpressionSQL();
	string ExpireAndInsertTagsSQL(int64_t snapshot_id);
	string ExpireAndInsertColumnTagsSQL(int64_t snapshot_id);
	string InsertColumnMappingSQL();
	string InsertNameMappingSQL();
	string InsertInlinedTablesSQL();
	string InsertDataFilesSQL(int64_t snapshot_id);
	string InsertFileColumnStatsSQL();
	string InsertFileVariantStatsSQL();
	string InsertFilePartitionValuesSQL();
	string InsertDeleteFilesSQL(int64_t snapshot_id);
	string OverwriteDeleteFilesSQL();
	string InsertTableStatsSQL();
	string InsertTableColumnStatsSQL();
	string InsertSnapshotChangesSQL(int64_t snapshot_id);

	string TempTableName(DuckLakeCommitKind kind) const;
	string BuildTrivialInsertSQL(DuckLakeCommitKind kind);
	unique_ptr<QueryResult> RunQuery(const string &sql);
	void DropStagedTables();

private:
	ClientContext &context;
	unique_ptr<Connection> connection;
	string uuid;
	unordered_set<string> populated_table_names;
};

} // namespace duckdb
