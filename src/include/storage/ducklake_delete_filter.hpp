//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_delete_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

struct DuckLakeDeleteData {
	vector<idx_t> deleted_rows;
	vector<idx_t> snapshot_ids;
	//! For delete scans with embedded snapshots: stores snapshot_id for each row_id being scanned
	//! scan_row_snapshot_ids[row_id] = snapshot_id, or 0 if not being scanned
	vector<idx_t> scan_row_snapshot_ids;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel,
	             optional_idx snapshot_filter = optional_idx()) const;

	bool HasEmbeddedSnapshots() const;
	bool HasScanSnapshotIds() const;
	optional_idx GetScanSnapshotId(idx_t row_id) const;
};

struct DeleteFileScanResult {
	vector<idx_t> deleted_rows;
	vector<idx_t> snapshot_ids;
};

class DuckLakeDeleteFilter : public DeleteFilter {
public:
	DuckLakeDeleteFilter();

	shared_ptr<DuckLakeDeleteData> delete_data;
	optional_idx max_row_count;

	optional_idx snapshot_filter;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;
	void Initialize(ClientContext &context, const DuckLakeFileData &delete_file);
	void Initialize(const DuckLakeInlinedDataDeletes &inlined_deletes);
	void Initialize(ClientContext &context, const DuckLakeDeleteScanEntry &delete_scan);
	void SetMaxRowCount(idx_t max_row_count);
	void SetSnapshotFilter(idx_t snapshot_filter);

private:
	static DeleteFileScanResult ScanDeleteFile(ClientContext &context, const DuckLakeFileData &delete_file,
	                                           optional_idx snapshot_min = optional_idx(),
	                                           optional_idx snapshot_max = optional_idx());
};

} // namespace duckdb
