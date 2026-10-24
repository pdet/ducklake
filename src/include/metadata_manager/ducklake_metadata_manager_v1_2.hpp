//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/ducklake_metadata_manager_v1_2.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"

namespace duckdb {

template <typename Base>
class DuckLakeMetadataManagerV1_2 : public DuckLakeMetadataManagerV1_1<Base> {
public:
	explicit DuckLakeMetadataManagerV1_2(DuckLakeTransaction &transaction)
	    : DuckLakeMetadataManagerV1_1<Base>(transaction) {
	}

	string GetCreateTableStatements() override;
	string GetVersionString() override;
};

} // namespace duckdb
