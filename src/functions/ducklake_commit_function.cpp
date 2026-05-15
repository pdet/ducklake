#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_commit_executor.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct DuckLakeCommitFunctionData final : public TableFunctionData {
	DuckLakeCommitFunctionData(Catalog &catalog, string uuid) : catalog(catalog), uuid(std::move(uuid)) {
	}
	Catalog &catalog;
	string uuid;
};

struct DuckLakeCommitFunctionState final : public GlobalTableFunctionState {
	DuckLakeCommitFunctionState() = default;
	bool done = false;
	DuckLakeCommitResult result;
};

static unique_ptr<FunctionData> DuckLakeCommitBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);

	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("schema_version");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("retry_count");

	if (input.inputs[1].IsNull()) {
		throw BinderException("ducklake_commit: commit_uuid must not be NULL");
	}
	return make_uniq<DuckLakeCommitFunctionData>(catalog, input.inputs[1].GetValue<string>());
}

static unique_ptr<GlobalTableFunctionState> DuckLakeCommitInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<DuckLakeCommitFunctionState>();
}

static void DuckLakeCommitExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeCommitFunctionState>();
	auto &bind_data = data_p.bind_data->Cast<DuckLakeCommitFunctionData>();
	if (state.done) {
		output.SetCardinality(0);
		return;
	}

	DuckLakeCommitExecutor executor(context, bind_data.catalog, bind_data.uuid);
	state.result = executor.Execute();
	state.done = true;

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(state.result.snapshot_id));
	output.SetValue(1, 0, Value::BIGINT(state.result.schema_version));
	output.SetValue(2, 0, Value::INTEGER(state.result.retry_count));
}

DuckLakeCommitFunction::DuckLakeCommitFunction()
    : TableFunction("ducklake_commit", {LogicalType::VARCHAR, LogicalType::VARCHAR}, DuckLakeCommitExecute,
                    DuckLakeCommitBind, DuckLakeCommitInit) {
}

} // namespace duckdb
