#pragma once

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_staged_commit.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_transaction_state.hpp"

#include <map>
#include <vector>

namespace duckdb {
class ClientContext;

struct DuckLakeServerSideCommitResult {
	int64_t committed_snapshot_id = 0;
	int64_t committed_schema_version = 0;
	int64_t next_catalog_id = 0;
	int64_t next_file_id = 0;
	bool had_flushes = false;
};

//! Executes a DuckLake commit server-side from staged tables.
class DuckLakeServerSideCommit {
public:
	DuckLakeServerSideCommit(ClientContext &context, string metadata_schema_name, string identifier_suffix,
	                         int64_t schema_version);

	//! Transform staged tables to Objects and commit.
	DuckLakeServerSideCommitResult Run();
	//! Override the retry configuration
	void SetRetryConfigOverride(const DuckLakeRetryConfig &retry_config);

private:
	struct ColumnKey {
		TableIndex table_id;
		FieldIndex column_id;
		bool operator<(const ColumnKey &other) const {
			if (table_id != other.table_id) {
				return table_id < other.table_id;
			}
			return column_id < other.column_id;
		}
	};

	//! Read commit metadata (author, message, snapshot ids).
	void ReadCommitHeader();
	//! Build the batched SQL for all hydration queries after the commit header.
	string BuildHydrationBatch();
	//! Advance to the next result in the chain; throw on error or missing.
	MaterializedQueryResult &AdvanceResult(QueryResult *&cursor, const char *what);

	//! Process column types from a batched result.
	void ProcessColumnTypes(MaterializedQueryResult &result);
	//! Process staged data files and their per-file column stats.
	void ProcessStagedDataFiles(MaterializedQueryResult &stats_result, MaterializedQueryResult &part_result,
	                            MaterializedQueryResult &attached_result, MaterializedQueryResult &files_result);
	//! Process staged inlined data rows, row ids, and column stats.
	void ProcessStagedInlinedData(MaterializedQueryResult &meta_result, MaterializedQueryResult &rows_result,
	                              MaterializedQueryResult &inlined_stats_result);
	//! Process staged inlined row deletes grouped by table.
	void ProcessStagedInlinedDeletes(MaterializedQueryResult &result);
	//! Process staged inlined file-level deletes grouped by table.
	void ProcessStagedInlinedFileDeletes(MaterializedQueryResult &result);
	//! Process staged delete files not attached to data files.
	void ProcessStagedDeleteFiles(MaterializedQueryResult &result);
	//! Process staged dropped file paths and tables deleted from.
	void ProcessStagedDroppedFiles(MaterializedQueryResult &dropped_result, MaterializedQueryResult &tables_result);
	//! Process staged flushed inlined table entries.
	void ProcessStagedFlushedInlinedTables(MaterializedQueryResult &result);
	//! Process staged compaction headers and their source files.
	void ProcessStagedCompactions(MaterializedQueryResult &header_result, MaterializedQueryResult &sources_result);
	//! Process staged name maps and rebuild entry trees.
	void ProcessStagedNameMaps(MaterializedQueryResult &entries_result, MaterializedQueryResult &header_result);
	//! Process current global table stats from a batched result.
	void ProcessExistingTableStats(MaterializedQueryResult &result);

	//! Query the metadata catalog for the latest snapshot.
	DuckLakeSnapshot ReadLatestSnapshot();
	//! Build a DuckLakeTableStats from parsed global stats.
	unique_ptr<DuckLakeTableStats> BuildTableStats(const DuckLakeGlobalStatsInfo &gs);
	//! Build a full DuckLakeStats map from global stats.
	unique_ptr<DuckLakeStats> BuildStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats);
	//! Assemble the DuckLakeCommitContext with all closures.
	DuckLakeCommitContext BuildContext(idx_t &committed_snapshot_id, idx_t &committed_schema_version,
	                                   idx_t &committed_next_catalog_id, idx_t &committed_next_file_id);
	//! Build INSERT SQL from staged inlined tuples.
	string BuildInlinedDataInserts(const vector<DuckLakeInlinedDataInfo> &new_data);
	//! Resolve and cache the latest inlined-data table name.
	const string &ResolveInlinedTableName(TableIndex table_id);
	//! Replace {METADATA_CATALOG}, {SNAPSHOT_ID}, etc. in SQL.
	string SubstitutePlaceholders(string sql, const DuckLakeSnapshot &snapshot) const;
	//! Execute a query on the fresh connection; throw on error.
	unique_ptr<MaterializedQueryResult> RunQuery(const string &query, const char *what);
	//! Fully-qualified name of a staging table for this commit.
	string Staged(DuckLakeStagedTableType kind) const;
	//! SELECT columns FROM staged_table [tail].
	string Select(const char *columns, DuckLakeStagedTableType kind, const char *tail = "") const;

private:
	ClientContext &context;
	const string metadata_schema_name;
	const string schema_id;
	const string identifier_suffix;
	const int64_t schema_version;
	Connection fresh_conn;
	DuckLakeRetryConfig retry_config;

	DuckLakeNameMapSet new_name_maps;
	unique_ptr<DuckLakeTransactionState> state;
	DuckLakeSnapshot transaction_snapshot;
	TransactionChangeInformation transaction_changes;
	map<ColumnKey, LogicalType> column_types;
	map<TableIndex, shared_ptr<DuckLakeTableStats>> existing_table_stats;

	//! Per-table SQL literal tuples for inlined-data inserts.
	map<TableIndex, vector<string>> staged_inlined_tuples;
	//! Parallel row_ids for update-inlining; empty if !HasPreservedRowIds.
	map<TableIndex, vector<int64_t>> staged_inlined_row_ids;
	//! Cache of inlined_table_name lookups across the commit retry loop.
	map<idx_t, string> inlined_table_name_cache;
	//! Compaction-output files indexed by compaction_id.
	map<idx_t, DuckLakeDataFile> compaction_output_files;
};

} // namespace duckdb
