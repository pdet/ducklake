#include "common/ducklake_inlined_data_converter.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_transaction.hpp"
#include "duckdb/common/vector/struct_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/cast/cast_function_set.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/scalar/string_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

static unique_ptr<Expression> BindVariantFunction(ClientContext &context, const string &name, const LogicalType &type) {
	// Commit has no active client transaction
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &catalog = Catalog::GetSystemCatalog(db);
	auto transaction = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = catalog.GetSchema(transaction, Identifier::DefaultSchema());
	auto entry = schema.GetEntry(transaction, CatalogType::SCALAR_FUNCTION_ENTRY, Identifier(name));
	if (!entry) {
		throw MissingExtensionException("Inlining VARIANT data requires %s. Load the parquet extension", name);
	}
	vector<unique_ptr<Expression>> children;
	children.push_back(make_uniq<BoundReferenceExpression>(type, 0));
	ErrorData error;
	FunctionBinder binder(context);
	auto function = binder.BindScalarFunction(entry->Cast<ScalarFunctionCatalogEntry>(), std::move(children), error);
	if (!function) {
		error.Throw();
	}
	return function;
}

struct InlinedVariantCastState : public FunctionLocalState {
	InlinedVariantCastState(ClientContext &context, bool encode_p)
	    : encode(encode_p), executor(context), concat_executor(context) {
		auto source_type = encode ? LogicalType::VARIANT() : LogicalType::BLOB;
		function = BindVariantFunction(context, encode ? "variant_to_parquet_variant" : "variant_bytes_to_variant",
		                               source_type);
		executor.AddExpression(*function);
		input.InitializeEmpty({source_type});
		output.Initialize(context, {function->GetReturnType()});
		if (!encode) {
			return;
		}
		vector<unique_ptr<Expression>> children;
		children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BLOB, 1));
		children.push_back(make_uniq<BoundReferenceExpression>(LogicalType::BLOB, 2));
		FunctionBinder binder(context);
		auto concat = binder.BindScalarFunction(ConcatOperatorFun::GetFunction(), std::move(children));
		auto is_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NULL, LogicalType::BOOLEAN);
		is_null->GetChildrenMutable().push_back(make_uniq<BoundReferenceExpression>(source_type, 0));
		concat_function = make_uniq<BoundCaseExpression>(
		    std::move(is_null), make_uniq<BoundConstantExpression>(Value(LogicalType::BLOB)), std::move(concat));
		concat_executor.AddExpression(*concat_function);
		concat_input.InitializeEmpty({source_type, LogicalType::BLOB, LogicalType::BLOB});
	}

	bool encode;
	unique_ptr<Expression> function;
	unique_ptr<Expression> concat_function;
	ExpressionExecutor executor;
	ExpressionExecutor concat_executor;
	DataChunk input;
	DataChunk output;
	DataChunk concat_input;
};

template <bool ENCODE>
static unique_ptr<FunctionLocalState> InitVariantCast(CastLocalStateParameters &parameters) {
	return make_uniq<InlinedVariantCastState>(*parameters.context, ENCODE);
}

static bool CastVariant(Vector &source, Vector &result, idx_t count, CastParameters &parameters) {
	auto &state = parameters.local_state->Cast<InlinedVariantCastState>();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	// List children can exceed the executor batch size
	for (idx_t offset = 0; offset < count; offset += STANDARD_VECTOR_SIZE) {
		auto batch = MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - offset);
		Vector blobs_from_text(LogicalType::BLOB, count_t(batch));
		if (!state.encode && source.GetType().id() == LogicalTypeId::VARCHAR) {
			// Nested VARIANT values are stored as escaped blob text
			Vector text(source.GetType());
			text.Slice(source, offset, offset + batch);
			VectorOperations::DefaultCast(text, blobs_from_text, batch, true);
			state.input.data[0].Reference(blobs_from_text);
		} else {
			state.input.data[0].Slice(source, offset, offset + batch);
		}
		state.input.SetChildCardinality(batch);
		state.output.Reset();
		state.executor.Execute(state.input, state.output);
		if (state.encode) {
			auto &entries = StructVector::GetEntries(state.output.data[0]);
			state.concat_input.data[0].Reference(state.input.data[0]);
			state.concat_input.data[1].Reference(entries[0]);
			state.concat_input.data[2].Reference(entries[1]);
			state.concat_input.SetChildCardinality(batch);
			Vector blobs(LogicalType::BLOB, count_t(batch));
			state.concat_executor.ExecuteExpression(state.concat_input, blobs);
			VectorOperations::Copy(blobs, result, batch, 0, offset);
		} else {
			VectorOperations::Copy(state.output.data[0], result, batch, 0, offset);
		}
	}
	return true;
}

DuckLakeInlinedDataConverter::DuckLakeInlinedDataConverter(ClientContext &context,
                                                           const vector<LogicalType> &source_types,
                                                           const vector<LogicalType> &target_types) {
	if (source_types == target_types) {
		return;
	}
	if (source_types.size() != target_types.size()) {
		throw InvalidInputException("Expected %llu inlined columns but received %llu", target_types.size(),
		                            source_types.size());
	}
	CastFunctionSet functions;
	functions.RegisterCastFunction(LogicalType::VARIANT(), LogicalType::BLOB,
	                               BoundCastInfo(CastVariant, nullptr, InitVariantCast<true>));
	functions.RegisterCastFunction(LogicalType::BLOB, LogicalType::VARIANT(),
	                               BoundCastInfo(CastVariant, nullptr, InitVariantCast<false>));
	functions.RegisterCastFunction(LogicalType::VARCHAR, LogicalType::VARIANT(),
	                               BoundCastInfo(CastVariant, nullptr, InitVariantCast<false>));
	// Postgres stores strings as BYTEA to preserve null bytes
	GetCastFunctionInput input(context);
	for (idx_t i = 0; i < source_types.size(); i++) {
		if (source_types[i].id() == LogicalTypeId::BLOB && target_types[i].id() == LogicalTypeId::VARCHAR) {
			// Postgres stores VARCHAR columns (including aliased ones such as JSON) as BYTEA because they may hold
			// null bytes - read the bytes back as-is instead of escaping them
			casts.push_back(BoundCastInfo(DefaultCasts::ReinterpretCast));
			states.push_back(nullptr);
			continue;
		}
		auto cast = functions.GetCastFunction(source_types[i], target_types[i], input);
		unique_ptr<FunctionLocalState> state;
		if (cast.HasInitLocalState()) {
			CastLocalStateParameters parameters(context, cast.GetCastData());
			state = cast.InitLocalState(parameters);
		}
		casts.push_back(std::move(cast));
		states.push_back(std::move(state));
	}
	result.Initialize(context, target_types);
}

DataChunk &DuckLakeInlinedDataConverter::Convert(DataChunk &chunk) {
	if (casts.empty()) {
		return chunk;
	}
	result.Reset();
	for (idx_t i = 0; i < casts.size(); i++) {
		CastParameters parameters(casts[i].GetCastData(), false, nullptr, states[i]);
		casts[i].Cast(chunk.data[i], result.data[i], chunk.size(), parameters);
	}
	result.SetChildCardinality(chunk.size());
	return result;
}

} // namespace duckdb
