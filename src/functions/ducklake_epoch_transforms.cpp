#include "functions/ducklake_epoch_transforms.hpp"

#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// epoch_year: years since 1970
//===--------------------------------------------------------------------===//
template <class INPUT_TYPE>
static void EpochYearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::ExecuteWithNulls<INPUT_TYPE, int64_t>(input, result, args.size(),
	                                                     [&](INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
		                                                     auto year = Date::ExtractYear(input);
		                                                     return static_cast<int64_t>(year - 1970);
	                                                     });
}

static void EpochYearTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::ExecuteWithNulls<timestamp_t, int64_t>(
	    input, result, args.size(), [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
		    auto date = Timestamp::GetDate(input);
		    auto year = Date::ExtractYear(date);
		    return static_cast<int64_t>(year - 1970);
	    });
}

ScalarFunctionSet DuckLakeEpochTransforms::GetEpochYearFunction() {
	ScalarFunctionSet set("epoch_year");
	set.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, EpochYearFunction<date_t>));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, EpochYearTimestampFunction));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, EpochYearTimestampFunction));
	return set;
}

//===--------------------------------------------------------------------===//
// epoch_month: months since 1970-01
//===--------------------------------------------------------------------===//
template <class INPUT_TYPE>
static void EpochMonthFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::ExecuteWithNulls<INPUT_TYPE, int64_t>(input, result, args.size(),
	                                                     [&](INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
		                                                     int32_t year, month, day;
		                                                     Date::Convert(input, year, month, day);
		                                                     return static_cast<int64_t>((year - 1970) * 12 + month - 1);
	                                                     });
}

static void EpochMonthTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::ExecuteWithNulls<timestamp_t, int64_t>(
	    input, result, args.size(), [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
		    auto date = Timestamp::GetDate(input);
		    int32_t year, month, day;
		    Date::Convert(date, year, month, day);
		    return static_cast<int64_t>((year - 1970) * 12 + month - 1);
	    });
}

ScalarFunctionSet DuckLakeEpochTransforms::GetEpochMonthFunction() {
	ScalarFunctionSet set("epoch_month");
	set.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, EpochMonthFunction<date_t>));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, EpochMonthTimestampFunction));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, EpochMonthTimestampFunction));
	return set;
}

//===--------------------------------------------------------------------===//
// epoch_day: days since 1970-01-01
//===--------------------------------------------------------------------===//
template <class INPUT_TYPE>
static void EpochDayFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::ExecuteWithNulls<INPUT_TYPE, int64_t>(input, result, args.size(),
	                                                     [&](INPUT_TYPE input, ValidityMask &mask, idx_t idx) {
		                                                     return static_cast<int64_t>(Date::Epoch(input) / 86400);
	                                                     });
}

static void EpochDayTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::ExecuteWithNulls<timestamp_t, int64_t>(
	    input, result, args.size(), [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
		    auto date = Timestamp::GetDate(input);
		    return static_cast<int64_t>(Date::Epoch(date) / 86400);
	    });
}

ScalarFunctionSet DuckLakeEpochTransforms::GetEpochDayFunction() {
	ScalarFunctionSet set("epoch_day");
	set.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, EpochDayFunction<date_t>));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, EpochDayTimestampFunction));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, EpochDayTimestampFunction));
	return set;
}

//===--------------------------------------------------------------------===//
// epoch_hour: hours since 1970-01-01 00:00:00
//===--------------------------------------------------------------------===//
static void EpochHourTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &input = args.data[0];
	UnaryExecutor::ExecuteWithNulls<timestamp_t, int64_t>(
	    input, result, args.size(), [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
		    return Timestamp::GetEpochSeconds(input) / 3600;
	    });
}

ScalarFunctionSet DuckLakeEpochTransforms::GetEpochHourFunction() {
	ScalarFunctionSet set("epoch_hour");
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, EpochHourTimestampFunction));
	set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT, EpochHourTimestampFunction));
	return set;
}

} // namespace duckdb
