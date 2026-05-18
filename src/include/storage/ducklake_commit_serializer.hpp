//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_commit_serializer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "storage/ducklake_commit_executor.hpp"
#include "storage/ducklake_commit_state.hpp"
#include "storage/ducklake_commit_temp_tables.hpp"

namespace duckdb {
class DuckLakeMetadataManager;
class DuckLakeTransaction;

struct DuckLakeCommitSerializerResult {
	idx_t next_catalog_id = 0;
	idx_t next_file_id = 0;
	set<string> populated_table_names;
	string staging_sql;
	DuckLakeCommitMeta meta;
};

class DuckLakeCommitSerializer {
public:
	DuckLakeCommitSerializer(DuckLakeTransaction &transaction, DuckLakeMetadataManager &metadata_manager, string uuid);

	bool CanHandle() const;

	DuckLakeCommitSerializerResult Serialize(TransactionChangeInformation &txn_changes);

private:
	void StageDataFilesAndStats(DuckLakeCommitState &commit_state);
	void StageDeleteFiles(DuckLakeCommitState &commit_state);
	void StageSnapshotChanges(DuckLakeCommitState &commit_state, TransactionChangeInformation &txn_changes);
	void EnsureCreated(DuckLakeCommitKind kind);
	void Append(const string &sql);
	void Flush(const char *context);
	string TempTableName(DuckLakeCommitKind kind) const;

private:
	DuckLakeTransaction &transaction;
	DuckLakeMetadataManager &metadata_manager;
	string uuid;
	DuckLakeCommitSerializerResult result;
	set<DuckLakeCommitKind> created_kinds;
	string pending_sql;
};

} // namespace duckdb
