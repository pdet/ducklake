//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_commit_session.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "storage/ducklake_commit_temp_tables.hpp"

namespace duckdb {

struct DuckLakeCommitSession {
	string GetUUID() {
		lock_guard<mutex> lk(lock);
		if (uuid.empty()) {
			uuid = UUID::ToString(UUID::GenerateRandomUUID());
		}
		return uuid;
	}
	bool IsKindCreated(DuckLakeCommitKind kind) {
		lock_guard<mutex> lk(lock);
		return created_kinds.find(kind) != created_kinds.end();
	}
	void MarkKindCreated(DuckLakeCommitKind kind) {
		lock_guard<mutex> lk(lock);
		created_kinds.insert(kind);
	}
	void Reset() {
		lock_guard<mutex> lk(lock);
		created_kinds.clear();
	}

private:
	mutex lock;
	string uuid;
	set<DuckLakeCommitKind> created_kinds;
};

} // namespace duckdb
