#include "storage/ducklake_commit_retry.hpp"

#include "duckdb/common/random_engine.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"

#include <chrono>
#include <cmath>
#include <thread>

namespace duckdb {

DuckLakeRetryConfig DuckLakeRetryConfig::LoadFromContext(ClientContext &context) {
	DuckLakeRetryConfig config;
	Value setting_val;
	if (context.TryGetCurrentSetting("ducklake_max_retry_count", setting_val)) {
		config.max_retry_count = setting_val.GetValue<idx_t>();
	}
	if (context.TryGetCurrentSetting("ducklake_retry_wait_ms", setting_val)) {
		config.retry_wait_ms = setting_val.GetValue<idx_t>();
	}
	if (context.TryGetCurrentSetting("ducklake_retry_backoff", setting_val)) {
		config.retry_backoff = setting_val.GetValue<double>();
	}
	return config;
}

bool RetryOnError(const string &original_message) {
	auto message = StringUtil::Lower(original_message);
	// retry on primary key errors
	if (StringUtil::Contains(message, "primary key") || StringUtil::Contains(message, "unique")) {
		return true;
	}
	// retry on conflicts
	if (StringUtil::Contains(message, "conflict")) {
		return true;
	}
	// retry on concurrent access
	if (StringUtil::Contains(message, "concurrent")) {
		return true;
	}
	return false;
}

void SleepBeforeRetry(idx_t attempt, const DuckLakeRetryConfig &config) {
#ifndef DUCKDB_NO_THREADS
	RandomEngine random;
	// random multiplier between 0.5 - 1.0
	double random_multiplier = (random.NextRandom() + 1.0) / 2.0;
	uint64_t sleep_amount = (uint64_t)((double)config.retry_wait_ms * random_multiplier *
	                                   pow(config.retry_backoff, static_cast<double>(attempt)));
	std::this_thread::sleep_for(std::chrono::milliseconds(sleep_amount));
#endif
}

} // namespace duckdb
