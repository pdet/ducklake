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
#include "storage/ducklake_commit_temp_tables.hpp"

namespace duckdb {
class Catalog;
class ClientContext;
class DuckLakeTransaction;

struct DuckLakeCommitResult {
	int64_t snapshot_id = 0;
	int64_t schema_version = 0;
	int32_t retry_count = 0;
};

//! Runs one DuckLake commit on the metadata side.
class DuckLakeCommitExecutor {
public:
	DuckLakeCommitExecutor(ClientContext &context, Catalog &catalog, string uuid);

	DuckLakeCommitResult Execute();

private:
	struct CommitMeta {
		int64_t snapshot_id_was = 0;
		int64_t schema_version_was = 0;
		bool schema_changed = false;
		bool has_inlined_flush = false;
	};

	struct LatestSnapshot {
		int64_t snapshot_id = 0;
		int64_t schema_version = 0;
		int64_t next_catalog_id = 0;
		int64_t next_file_id = 0;
	};

	CommitMeta ReadMeta();
	LatestSnapshot ReadLatestSnapshot();
	void RefreshPopulatedKinds();
	bool IsPopulated(DuckLakeCommitKind kind) const;
	//! Step 8 conflict resolution hook.
	bool CheckForConflictsFromTempTables(int64_t snapshot_id_was, int64_t schema_version_was);

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
	string InsertDeleteFilesSQL(int64_t snapshot_id);
	string InsertTableStatsSQL();
	string InsertSnapshotChangesSQL(int64_t snapshot_id);

	string TempTableName(DuckLakeCommitKind kind) const;

private:
	ClientContext &context;
	Catalog &catalog;
	DuckLakeTransaction &transaction;
	string uuid;
	unordered_set<string> populated_table_names;
};

} // namespace duckdb
