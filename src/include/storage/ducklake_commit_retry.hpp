//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_commit_retry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class ClientContext;

struct DuckLakeRetryConfig {
	idx_t max_retry_count = 10;
	idx_t retry_wait_ms = 100;
	double retry_backoff = 1.5;

	static DuckLakeRetryConfig LoadFromContext(ClientContext &context);
};

bool RetryOnError(const string &original_message);

void SleepBeforeRetry(idx_t attempt, const DuckLakeRetryConfig &config);

} // namespace duckdb
