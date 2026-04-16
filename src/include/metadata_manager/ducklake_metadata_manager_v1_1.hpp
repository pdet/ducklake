//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/ducklake_metadata_manager_v1_1.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

//! DDL for the ducklake_delete_vector metadata table introduced in v1.1
constexpr const char *DUCKLAKE_DELETE_VECTOR_TABLE_DDL =
    "{METADATA_CATALOG}.ducklake_delete_vector(delete_file_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, "
    "delete_vector_offset BIGINT, delete_vector_size BIGINT, delete_count BIGINT)";

template <typename Base>
class DuckLakeMetadataManagerV1_1 : public Base {
public:
	explicit DuckLakeMetadataManagerV1_1(DuckLakeTransaction &transaction) : Base(transaction) {
	}

	string GetCreateTableStatements() override;
	string GetVersionString() override;

	//! Delete vector read/write
	vector<DuckLakeDeleteVectorInfo> GetDeleteVectors(DataFileIndex delete_file_id) override;
	string WriteNewDeleteVectors(const vector<DuckLakeDeleteFileInfo> &new_delete_files) override;

protected:
	//! Delete vector query support
	string GetDeleteFileSelectList(const string &prefix) override;
	string GetDeleteFileWithVectorJoin(idx_t table_id_val, const string &snapshot_filter,
	                                   const string &dv_snapshot_filter) override;
	string GetDeleteFileLateralJoinSQL(idx_t table_id_val, const string &where_clause,
	                                   const string &dv_filter) override;
	string GetNullDeleteSentinel() const override;
	string GetNullDeleteFileColumns() const override;
	bool HasDeleteVectorColumns() const override;

	//! Delete vector cleanup hooks
	string DeleteVectorCleanupSQL(const string &delete_file_ids) override;
	string DeleteVectorCleanupForDataFilesSQL(const string &data_file_ids) override;
	string EndSnapshotDeleteVectorSQL(idx_t delete_file_id, idx_t snapshot) override;
	void CleanupDeleteVectorsForSnapshotDeletion(const string &deleted_delete_ids) override;
};

} // namespace duckdb
