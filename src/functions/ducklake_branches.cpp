#include "common/ducklake_util.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"

namespace duckdb {

struct DuckLakeBranchData : public TableFunctionData {
	DuckLakeBranchData(DuckLakeCatalog &catalog, string name) : catalog(catalog), name(std::move(name)) {
	}

	DuckLakeCatalog &catalog;
	string name;
	//! The snapshot ducklake_create_branch forks at
	idx_t fork_snapshot_id = DConstants::INVALID_INDEX;
};

static DuckLakeCatalog &GetBranchCatalog(ClientContext &context, TableFunctionBindInput &input,
                                         DatabaseModificationType modification) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	if (!catalog.SupportsBranching()) {
		ThrowUnsupportedByVersion(catalog.GetDuckLakeVersion(), "branches");
	}
	catalog.VerifyWritable();
	if (input.binder) {
		input.binder->GetStatementProperties().RegisterDBModify(catalog, context, modification);
	}
	return catalog;
}

static string BranchNameFromInput(const Value &input) {
	if (input.IsNull() || StringValue::Get(input).empty()) {
		throw BinderException("Branch name cannot be NULL or empty");
	}
	return StringValue::Get(input);
}

static void BranchReturnTypes(vector<LogicalType> &return_types, vector<Identifier> &names) {
	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
}

//! The snapshot of the version or timestamp option, or else the snapshot the transaction reads
static idx_t ForkSnapshotFromInput(ClientContext &context, TableFunctionBindInput &input, DuckLakeCatalog &catalog) {
	Value version;
	Value timestamp;
	bool has_version = DuckLakeTableFunctionUtil::TryGetNonNullOption(input, "version", "integer", version);
	bool has_timestamp = DuckLakeTableFunctionUtil::TryGetTimestampOption(input, "timestamp", timestamp);
	if (has_version && has_timestamp) {
		throw BinderException("ducklake_create_branch: specify either version or timestamp, not both");
	}
	unique_ptr<BoundAtClause> at_clause;
	if (has_version) {
		at_clause = make_uniq<BoundAtClause>(DuckLakeTableFunctionUtil::AtClauseFromValue(version));
	} else if (has_timestamp) {
		at_clause = make_uniq<BoundAtClause>(DuckLakeTableFunctionUtil::AtClauseFromValue(timestamp));
	}
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	return transaction.GetSnapshot(at_clause.get()).snapshot_id;
}

static unique_ptr<FunctionData> CreateBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = GetBranchCatalog(context, input, DatabaseModificationType::CREATE_CATALOG_ENTRY);
	auto result = make_uniq<DuckLakeBranchData>(catalog, BranchNameFromInput(input.inputs[1]));
	if (DuckLakeUtil::TryParseSnapshotId(input.inputs[1])) {
		throw BinderException("A branch cannot be named like a snapshot id, got \"%s\"", result->name);
	}
	BranchReturnTypes(return_types, names);
	result->fork_snapshot_id = ForkSnapshotFromInput(context, input, catalog);
	return std::move(result);
}

static void CreateBranchExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<DuckLakeBranchData>();
	auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
	transaction.CreateBranch(data.name, data.fork_snapshot_id);
}

static unique_ptr<FunctionData> DropBranchBind(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = GetBranchCatalog(context, input, DatabaseModificationType::DROP_CATALOG_ENTRY);
	auto result = make_uniq<DuckLakeBranchData>(catalog, BranchNameFromInput(input.inputs[1]));
	BranchReturnTypes(return_types, names);
	// the commit conflicts with branch changes after the snapshot the drop was validated against
	DuckLakeTransaction::Get(context, catalog).GetSnapshot();
	return std::move(result);
}

static void DropBranchExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.bind_data->Cast<DuckLakeBranchData>();
	auto &transaction = DuckLakeTransaction::Get(context, data.catalog);
	transaction.DropBranch(data.name);
}

static unique_ptr<FunctionData> BranchesBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	auto result = make_uniq<MetadataBindData>();
	names.emplace_back("branch_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("branch_name");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("fork_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("created_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	names.emplace_back("ended_snapshot_id");
	return_types.emplace_back(LogicalType::BIGINT);
	if (!catalog.SupportsBranching()) {
		return std::move(result);
	}
	auto bigint_or_null = [](optional_idx value) {
		return value.IsValid() ? Value::BIGINT(NumericCast<int64_t>(value.GetIndex())) : Value(LogicalType::BIGINT);
	};
	auto &transaction = DuckLakeTransaction::Get(context, catalog);
	for (auto &branch : transaction.GetBranches()) {
		vector<Value> row;
		if (branch.IsValid()) {
			row.push_back(Value::BIGINT(NumericCast<int64_t>(branch.branch_id)));
		} else {
			// a branch this transaction created has no id until its commit
			row.emplace_back(LogicalType::BIGINT);
		}
		row.emplace_back(branch.name);
		row.push_back(bigint_or_null(branch.fork_snapshot_id));
		row.push_back(bigint_or_null(branch.begin_snapshot));
		row.push_back(bigint_or_null(branch.end_snapshot));
		result->rows.push_back(std::move(row));
	}
	return std::move(result);
}

DuckLakeCreateBranchFunction::DuckLakeCreateBranchFunction()
    : TableFunction("ducklake_create_branch",
                    FunctionSignature()
                        .AddPositionalOnly("catalog", LogicalType::VARCHAR)
                        .AddPositionalOnly("branch_name", LogicalType::VARCHAR),
                    CreateBranchExecute, CreateBranchBind) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("version", LogicalType::BIGINT).Add("timestamp", LogicalType::TIMESTAMP_TZ);
	});
}

DuckLakeDropBranchFunction::DuckLakeDropBranchFunction()
    : TableFunction("ducklake_drop_branch",
                    FunctionSignature()
                        .AddPositionalOnly("catalog", LogicalType::VARCHAR)
                        .AddPositionalOnly("branch_name", LogicalType::VARCHAR),
                    DropBranchExecute, DropBranchBind) {
}

DuckLakeBranchesFunction::DuckLakeBranchesFunction() : DuckLakeBaseMetadataFunction("ducklake_branches", BranchesBind) {
}

} // namespace duckdb
