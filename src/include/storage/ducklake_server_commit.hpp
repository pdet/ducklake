//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_server_commit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/table_index.hpp"
#include "common/ducklake_snapshot.hpp"

namespace duckdb {

//! How the server runs the commit retry loop
enum class DuckLakeServerCommitMode : uint8_t {
	//! Retries run on the client
	NONE,
	//! The quack server runs a staged commit
	STAGED,
	//! A PL/pgSQL function runs the retries
	PG_FUNCTION
};

enum class DuckLakeServerAttemptStatus : uint8_t {
	COMMITTED,
	//! Not committed; continue on the client
	CLIENT,
	//! Not committed; the commit fails
	FAILED,
	//! May or may not be committed
	OUTCOME_UNKNOWN
};

//! The outcome of a server-side commit attempt
struct DuckLakeServerAttempt {
	DuckLakeServerAttemptStatus status = DuckLakeServerAttemptStatus::CLIENT;
	//! The committed snapshot
	DuckLakeSnapshot snapshot;
	ErrorData error;
	bool finished_retrying = false;
	//! The client attempt to continue with
	idx_t last_attempt = 0;
};

//! Prints the per-attempt ids of a commit batch
class DuckLakeCommitIdRenderer {
public:
	virtual ~DuckLakeCommitIdRenderer() = default;

	virtual string FileId(idx_t file_id) const {
		return std::to_string(file_id);
	}
	virtual string RowId(TableIndex, idx_t row_id) const {
		return std::to_string(row_id);
	}
};

//! Per-attempt bases of relative commits; ducklake_commit_v1 sets the same
struct DuckLakeRelativeCommitBase {
	static string SnapshotId() {
		return "snapshot_id";
	}
	static string SchemaVersion() {
		return "schema_version";
	}
	static string NextCatalogId() {
		return "next_catalog_id";
	}
	static string FileIdBase() {
		return "file_id_base";
	}
	static string RowIdBase(TableIndex table_id) {
		return "row_id_base_" + std::to_string(table_id.index);
	}
	static string SettingName(const string &base) {
		return "ducklake_commit." + base;
	}
	//! The template expression reading a base
	static string Expression(const string &base) {
		return "current_setting('" + SettingName(base) + "')::BIGINT";
	}
};

//! Prints ids as offsets from per-attempt server bases
class DuckLakeRelativeIdRenderer : public DuckLakeCommitIdRenderer {
public:
	explicit DuckLakeRelativeIdRenderer(idx_t seed) : seed(seed) {
	}

	string FileId(idx_t file_id) const override {
		return "(" + DuckLakeRelativeCommitBase::Expression(DuckLakeRelativeCommitBase::FileIdBase()) + " + " +
		       Offset(file_id) + ")";
	}
	string RowId(TableIndex table_id, idx_t row_id) const override {
		return "(" + DuckLakeRelativeCommitBase::Expression(DuckLakeRelativeCommitBase::RowIdBase(table_id)) + " + " +
		       Offset(row_id) + ")";
	}

private:
	string Offset(idx_t id) const {
		if (id < seed) {
			throw InternalException("DuckLake relative commit id %llu is below its seed %llu", id, seed);
		}
		return std::to_string(id - seed);
	}

	idx_t seed;
};

//! A commit rendered against per-attempt bases
struct DuckLakeRelativeCommit {
	string body;
	idx_t file_id_count = 0;
	vector<TableIndex> insert_tables;
	vector<TableIndex> inlined_insert_tables;
	vector<TableIndex> row_id_tables;
};

} // namespace duckdb
