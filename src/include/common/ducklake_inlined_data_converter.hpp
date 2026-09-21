#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

//! Converts inlined data chunks between table types and metadata storage types in either direction
class DuckLakeInlinedDataConverter {
public:
	DuckLakeInlinedDataConverter(ClientContext &context, const vector<LogicalType> &source_types,
	                             const vector<LogicalType> &target_types);

	//! Returns the converted chunk, or the input itself when no conversion is needed
	DataChunk &Convert(DataChunk &chunk);

private:
	//! One cast or plain reference per column
	vector<unique_ptr<Expression>> expressions;
	ExpressionExecutor executor;
	DataChunk result;
};

} // namespace duckdb
