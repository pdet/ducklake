#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_commit_executor.hpp"

namespace duckdb {

struct DuckLakeCommitFunctionData final : public TableFunctionData {
	string uuid;
	string staging_sql;
	string populated_kinds;
	DuckLakeCommitMeta meta;
	bool meta_provided = false;
	DuckLakeRetryConfig retry_config;
	bool retry_config_provided = false;
};

struct DuckLakeCommitFunctionState final : public GlobalTableFunctionState {
	DuckLakeCommitFunctionState() = default;
	bool done = false;
	DuckLakeCommitResult result;
};

static unique_ptr<FunctionData> DuckLakeCommitBind(ClientContext &, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("schema_version");
	return_types.emplace_back(LogicalType::INTEGER);
	names.emplace_back("retry_count");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("next_catalog_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("next_file_id");

	if (input.inputs[0].IsNull()) {
		throw BinderException("ducklake_commit: commit_uuid must not be NULL");
	}
	auto data = make_uniq<DuckLakeCommitFunctionData>();
	data->uuid = input.inputs[0].GetValue<string>();
	if (input.inputs.size() > 1 && !input.inputs[1].IsNull()) {
		data->staging_sql = input.inputs[1].GetValue<string>();
	}
	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		data->populated_kinds = input.inputs[2].GetValue<string>();
	}
	if (input.inputs.size() >= 11) {
		data->meta_provided = true;
		data->meta.snapshot_id_was = input.inputs[3].GetValue<int64_t>();
		data->meta.schema_version_was = input.inputs[4].GetValue<int64_t>();
		data->meta.schema_changed = input.inputs[5].GetValue<bool>();
		data->meta.has_inlined_flush = input.inputs[6].GetValue<bool>();
		data->meta.next_catalog_id = input.inputs[7].GetValue<int64_t>();
		data->meta.next_file_id = input.inputs[8].GetValue<int64_t>();
		data->meta.next_catalog_id_baseline = input.inputs[9].GetValue<int64_t>();
		data->meta.next_file_id_baseline = input.inputs[10].GetValue<int64_t>();
	}
	if (input.inputs.size() >= 14) {
		data->retry_config_provided = true;
		data->retry_config.max_retry_count = static_cast<idx_t>(input.inputs[11].GetValue<int64_t>());
		data->retry_config.retry_wait_ms = static_cast<idx_t>(input.inputs[12].GetValue<int64_t>());
		data->retry_config.retry_backoff = input.inputs[13].GetValue<double>();
	}
	return std::move(data);
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

	DuckLakeCommitExecutor executor(context, bind_data.uuid);
	state.result = executor.Execute(bind_data.staging_sql, bind_data.populated_kinds,
	                                bind_data.meta_provided ? &bind_data.meta : nullptr,
	                                bind_data.retry_config_provided ? &bind_data.retry_config : nullptr);
	state.done = true;

	output.SetCardinality(1);
	output.SetValue(0, 0, Value::BIGINT(state.result.snapshot_id));
	output.SetValue(1, 0, Value::BIGINT(state.result.schema_version));
	output.SetValue(2, 0, Value::INTEGER(state.result.retry_count));
	output.SetValue(3, 0, Value::BIGINT(state.result.committed_next_catalog_id));
	output.SetValue(4, 0, Value::BIGINT(state.result.committed_next_file_id));
}

DuckLakeCommitFunction::DuckLakeCommitFunction()
    : TableFunction("ducklake_commit",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                     LogicalType::BIGINT, LogicalType::BOOLEAN, LogicalType::BOOLEAN, LogicalType::BIGINT,
                     LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                     LogicalType::BIGINT, LogicalType::DOUBLE},
                    DuckLakeCommitExecute, DuckLakeCommitBind, DuckLakeCommitInit) {
}

} // namespace duckdb
