//===----------------------------------------------------------------------===//
//                         DuckDB
//
// functions/ducklake_epoch_transforms.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

struct DuckLakeEpochTransforms {
	static ScalarFunctionSet GetEpochYearFunction();
	static ScalarFunctionSet GetEpochMonthFunction();
	static ScalarFunctionSet GetEpochDayFunction();
	static ScalarFunctionSet GetEpochHourFunction();
};

} // namespace duckdb
