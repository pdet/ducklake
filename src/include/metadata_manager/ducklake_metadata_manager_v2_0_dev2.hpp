//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/ducklake_metadata_manager_v2_0_dev2.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "metadata_manager/ducklake_metadata_manager_v2_0.hpp"

namespace duckdb {

template <typename Base>
class DuckLakeMetadataManagerV2_0Dev2 : public DuckLakeMetadataManagerV2_0<Base> {
public:
	explicit DuckLakeMetadataManagerV2_0Dev2(DuckLakeTransaction &transaction)
	    : DuckLakeMetadataManagerV2_0<Base>(transaction) {
	}

	string GetCreateTableStatements() override;
	string GetSnapshotTableStatement() override;
	string GetInitialDataStatements() override;
	string GetVersionString() override;
};

} // namespace duckdb
