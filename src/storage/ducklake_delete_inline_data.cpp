#include "storage/ducklake_delete_inline_data.hpp"
#include "storage/ducklake_delete.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

DuckLakeDeleteInlineData::DuckLakeDeleteInlineData(PhysicalPlan &physical_plan, PhysicalOperator &child,
                                                   idx_t inline_row_limit)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, child.types, child.estimated_cardinality),
      inline_row_limit(inline_row_limit) {
	children.push_back(child);
}

class DeleteInlineDataState : public OperatorState {
public:
	explicit DeleteInlineDataState() {
	}
};

class DeleteInlineDataGlobalState : public GlobalOperatorState {
public:
	explicit DeleteInlineDataGlobalState(const DuckLakeDeleteInlineData &op) : op(op) {
	}

	const DuckLakeDeleteInlineData &op;
};

unique_ptr<OperatorState> DuckLakeDeleteInlineData::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<DeleteInlineDataState>();
}

unique_ptr<GlobalOperatorState> DuckLakeDeleteInlineData::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<DeleteInlineDataGlobalState>(*this);
}

OperatorResultType DuckLakeDeleteInlineData::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                     GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	// For now, pass through all rows to DuckLakeDelete
	// The inlining logic for file deletions will be handled in DuckLakeDelete::FlushDelete
	chunk.Reference(input);
	return OperatorResultType::NEED_MORE_INPUT;
}

OperatorFinalizeResultType DuckLakeDeleteInlineData::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                  GlobalOperatorState &gstate_p,
                                                                  OperatorState &state_p) const {
	return OperatorFinalizeResultType::FINISHED;
}

OperatorFinalResultType DuckLakeDeleteInlineData::OperatorFinalize(Pipeline &pipeline, Event &event,
                                                                   ClientContext &context,
                                                                   OperatorFinalizeInput &input) const {
	return OperatorFinalResultType::FINISHED;
}

string DuckLakeDeleteInlineData::GetName() const {
	return "DUCKLAKE_DELETE_INLINE_DATA";
}

} // namespace duckdb
