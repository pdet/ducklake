#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/function/cast/default_casts.hpp"

namespace duckdb {

class DuckLakeInlinedDataConverter {
public:
	DuckLakeInlinedDataConverter(ClientContext &context, const vector<LogicalType> &source_types,
	                             const vector<LogicalType> &target_types);

	DataChunk &Convert(DataChunk &chunk);

private:
	vector<BoundCastInfo> casts;
	vector<unique_ptr<FunctionLocalState>> states;
	DataChunk result;
};

} // namespace duckdb
