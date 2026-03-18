//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_inlined_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "storage/ducklake_stats.hpp"
#include "common/index.hpp"

namespace duckdb {

struct DuckLakeInlinedData {
	unique_ptr<ColumnDataCollection> data;
	map<FieldIndex, DuckLakeColumnStats> column_stats;
	//! Explicit row IDs for update-inlined data (when non-empty, used instead of sequential assignment)
	vector<int64_t> explicit_row_ids;

	//! Returns the number of rows that use sequential (non-explicit) row IDs
	idx_t SequentialCount() const {
		return data->Count() - explicit_row_ids.size();
	}

	//! Returns the row ID for the given ordinal position within the inlined data.
	//! The first SequentialCount() rows use sequential IDs starting from sequential_start.
	//! The remaining rows use explicit_row_ids.
	int64_t GetRowId(idx_t ordinal, int64_t sequential_start = 0) const {
		auto seq_count = SequentialCount();
		if (ordinal < seq_count) {
			return NumericCast<int64_t>(ordinal) + sequential_start;
		}
		return explicit_row_ids[ordinal - seq_count];
	}
};

struct DuckLakeInlinedDataDeletes {
	set<idx_t> rows;
};

//! Stores inlined file deletions for a table
struct DuckLakeInlinedFileDeletes {
	map<idx_t, set<idx_t>> file_deletes;
};

} // namespace duckdb
