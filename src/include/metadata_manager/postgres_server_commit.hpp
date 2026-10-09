//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/postgres_server_commit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/pair.hpp"

#include <functional>

namespace duckdb {
struct DuckLakeRetryConfig;
struct DuckLakeRelativeCommit;

//! What the server-side commit probe found
struct PostgresServerCommitCapabilities {
	bool has_plpgsql = false;
	bool installed = false;
	bool executable = false;
	bool can_create = false;
	string default_isolation;
};

//! PL/pgSQL functions behind Postgres server-side commits
class PostgresServerCommit {
public:
	//! Shared by every commit function version
	static constexpr int32_t COMMIT_LOCK_KEY = 1145848659;
	//! Serializes installs
	static constexpr int32_t INSTALL_LOCK_KEY = 1145848649;
	//! Locked after COMMIT_LOCK_KEY; optimistic batches share it
	static constexpr int32_t BATCH_LOCK_KEY = 1145848642;

	//! Capability probe query; never raises
	static string ProbeSql(const string &schema_literal);
	//! DO block creating missing functions; errors warn
	static string InstallSql(const string &schema_literal, const std::function<string(const string &)> &substitute);
	//! Lock budget shared with ducklake_commit_v1
	static int64_t SequencedLockBudgetMs(const DuckLakeRetryConfig &retry_config);
	//! Capped client backoff before an attempt, without jitter
	static double RetrySleepMs(const DuckLakeRetryConfig &retry_config, idx_t attempt);
	//! First statements of a sequenced commit attempt
	static string SequencerSql(const string &schema_literal, int64_t lock_budget_ms);
	//! Bounded shared batch key take, with placeholders
	static string SharedKeySql(int64_t lock_budget_ms, const std::function<string(const string &)> &substitute);
	//! Snapshot row columns of the sequenced state read
	static vector<string> SequencedStateColumns();
	//! Stamps the snapshot claim after the key wait
	static void SetSequencedSnapshotTime(string &batch);
	//! Whether PL/pgSQL and the fallback function are usable
	static string RelativeUsableSql(const string &schema_literal);
	//! Records the SQL-time fallback of the debug template
	static string DebugFallbackSetting();
	//! Claim, bases, relative body; fallback runs the absolute
	static string RelativeExecutionSql(const string &claim, const vector<pair<string, string>> &bases,
	                                   const string &relative_body, const string &absolute_body);
	//! The commit call; redact_body prints its size
	static string CommitCallSql(const string &metadata_catalog, idx_t transaction_snapshot_id,
	                            const string &catalog_version, const DuckLakeRelativeCommit &relative,
	                            const string &body, bool redact_body, const DuckLakeRetryConfig &retry_config);
	//! Shortens an error echoing the commit call
	static string RedactCommitCallError(const string &message, const string &call_sql);
	//! Second seed of the relative render invariance check
	static constexpr idx_t INVARIANCE_SEED = 1ULL << 40;
	//! Larger templates take the client path
	static constexpr idx_t MAX_TEMPLATE_BYTES = 16ULL * 1024ULL * 1024ULL;
};

} // namespace duckdb
