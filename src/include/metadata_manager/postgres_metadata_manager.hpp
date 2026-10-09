//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"
#include "metadata_manager/postgres_server_commit.hpp"

namespace duckdb {
struct DuckLakeCommitAttempt;

//! Commit key state of one client commit loop
struct PostgresSequencedRetry {
	int64_t lock_budget_ms = 0;
	//! Steady clock ms; negative until the first wait
	int64_t deadline_ms = -1;
	bool key_wait_timed_out = false;
	//! Whether this attempt read its state sequenced
	bool sequenced_state = false;
	//! Set after a failed sequencer, for all attempts
	bool unsequenced = false;
};

//! Server-side commit state of one commit loop
struct PostgresServerCommitState {
	bool attempted = false;
	//! The inlined tables the first attempt used
	unordered_map<idx_t, string> inlined_table_names;
};

//! Relative-execution debug state of one commit loop
struct PostgresRelativeDebugState {
	bool checked = false;
	//! PL/pgSQL and the fallback function are usable
	bool usable = false;
};

class PostgresMetadataManager : public DuckLakeMetadataManager {
public:
	explicit PostgresMetadataManager(DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<PostgresMetadataManager>(transaction);
	}

	bool TypeIsNativelySupported(const LogicalType &type) override;
	bool SupportsAppender() const override {
		return false;
	}
	idx_t MaxIdentifierLength() const override {
		return 63;
	}

	string GetColumnTypeInternal(const LogicalType &type) override;
	bool InlinedDeletionTableExists(const string &table_name) override;
	void MigrateInlinedDataTypes() override;

	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;

	void ClearCache() override;
	bool ProbeServerCapabilities() override;
	bool SupportsServerSideCommit(const TransactionChangeInformation &changes) const override;
	void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                            const TransactionChangeInformation &transaction_changes,
	                            const DuckLakeRetryConfig &retry_config) override;
	void PrepareCommitLoop(DuckLakeCommitContext &context, const DuckLakeRetryConfig &retry_config) override;
	string InlinedTablePreconditionSql(TableIndex table_id, const string &inlined_table_name) override;
	string RelativeGlobalTableStatsSql(TableIndex table_id, const DuckLakeNewGlobalStats &delta, idx_t seed,
	                                   const set<FieldIndex> &new_columns, bool write_stats_exactness) override;
	//! Final template text of a relative commit
	string RelativeCommitTemplate(const DuckLakeRelativeCommit &relative) const;

protected:
	string GetLatestSnapshotQuery() const override;
	string GenerateFileListQuery(DuckLakeTableEntry &table, const FilterPushdownInfo *filter_info,
	                             const vector<DuckLakeFileListDynamicFilter> &dynamic_filters,
	                             const vector<idx_t> &runtime_filter_stats_columns, FileListType file_list_type,
	                             const string &metadata_table_prefix) override;
	string CastValueToTarget(const Value &value, const LogicalType &type) override;
	string CastStatsToTarget(const string &stats, const LogicalType &type, StatsCastType cast_type) override;

private:
	void SubstitutePostgresPlaceholders(string &query) const;
	string ServerCallSql(const string &query) const;
	string ServerQuerySql(const string &query) const;
	string ServerCommitCallSql(const string &query) const;
	unique_ptr<QueryResult> ExecuteOnServer(const string &query);
	//! Locks the commit key, then reads the state
	bool AcquireSequencedState(PostgresSequencedRetry &retry, idx_t attempt, DuckLakeSnapshot transaction_snapshot,
	                           SnapshotAndStats &state, SnapshotChangeInfo &changes);
	bool RetryWaitsOnServer(PostgresSequencedRetry &retry) const;
	//! Debug: commits an append through the template
	unique_ptr<QueryResult>
	ExecuteRelativeCommit(PostgresRelativeDebugState &debug_state, const PostgresSequencedRetry &retry,
	                      const DuckLakeCommitAttempt &attempt,
	                      const std::function<shared_ptr<DuckLakeTableStats>(TableIndex)> &get_table_stats);
	//! Whether the debug template can run now
	bool RelativeCommitUsable(PostgresRelativeDebugState &debug_state);
	//! The relative commit and its template text
	pair<DuckLakeRelativeCommit, string> RenderRelativeTemplate(DuckLakeSnapshot transaction_snapshot,
	                                                            const TransactionChangeInformation &changes,
	                                                            bool check_invariance);
	//! Commits through ducklake_commit_v1 once
	DuckLakeServerAttempt ServerCommitAttempt(PostgresServerCommitState &state, DuckLakeSnapshot transaction_snapshot,
	                                          const TransactionChangeInformation &changes,
	                                          const DuckLakeRetryConfig &retry_config);
	//! Classifies a ducklake_commit_v1 status row
	DuckLakeServerAttempt ServerCommitStatus(QueryResult &result, const DuckLakeRetryConfig &retry_config);
	//! Resets an uncommitted attempt for the client
	DuckLakeServerAttempt ServerCommitHandBack(const ErrorData &error, idx_t last_attempt);
	//! The merge of one comparison class of columns
	string RelativeColumnStatsUpdate(const string &table_id, const LogicalType &type, const vector<string> &rows,
	                                 bool write_stats_exactness);
	//! Empty on success, else the error
	string ProbeServerCommit(PostgresServerCommitCapabilities &capabilities);
	//! Empty on success, else the error
	string InstallServerCommit();
};

} // namespace duckdb
