#include "storage/ducklake_update.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "storage/ducklake_delete.hpp"
#include "storage/ducklake_inline_data.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "common/ducklake_util.hpp"

namespace duckdb {

struct FileRowId {
	uint64_t file_index;
	int64_t row_number;

	bool operator==(const FileRowId &other) const {
		return file_index == other.file_index && row_number == other.row_number;
	}
};

struct FileRowIdHash {
	hash_t operator()(const FileRowId &id) const {
		return CombineHash(Hash(id.file_index), Hash(id.row_number));
	}
};

DuckLakeUpdate::DuckLakeUpdate(PhysicalPlan &physical_plan, DuckLakeTableEntry &table, vector<PhysicalIndex> columns_p,
                               PhysicalOperator &child, PhysicalOperator &copy_op, PhysicalOperator &delete_op,
                               PhysicalOperator &insert_op, vector<unique_ptr<Expression>> &expressions,
                               idx_t inline_row_limit)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BIGINT}, 1), table(table),
      columns(std::move(columns_p)), copy_op(copy_op), delete_op(delete_op), insert_op(insert_op),
      expressions(std::move(expressions)), inline_row_limit(inline_row_limit) {
	children.push_back(child);
	row_id_index = columns.size();
}

//===--------------------------------------------------------------------===//
// States
//===--------------------------------------------------------------------===//
class DuckLakeUpdateGlobalState : public GlobalSinkState {
public:
	DuckLakeUpdateGlobalState() : total_updated_count(0) {
	}

	atomic<idx_t> total_updated_count;

	//! Duplicate row detection (first-write-wins)
	mutex seen_rows_lock;
	unordered_set<FileRowId, FileRowIdHash> seen_rows;

	//! Inlining support
	atomic<idx_t> total_inlined_rows {0};
	bool inline_overflow = false;
	mutex inline_lock;
	unique_ptr<ColumnDataCollection> global_inlined_data;
	vector<int64_t> global_inlined_row_ids;
};

class DuckLakeUpdateLocalState : public LocalSinkState {
public:
	unique_ptr<LocalSinkState> copy_local_state;
	unique_ptr<LocalSinkState> delete_local_state;
	unique_ptr<ExpressionExecutor> expression_executor;
	//! Chunk where the updated expressions are executed.
	DataChunk update_expression_chunk;
	DataChunk insert_chunk;
	DataChunk delete_chunk;
	idx_t updated_count = 0;

	//! Inlining support
	unique_ptr<ColumnDataCollection> local_inlined_data;
	vector<int64_t> local_inlined_row_ids;
	bool local_overflow = false;
	DataChunk user_columns_chunk;
};

unique_ptr<GlobalSinkState> DuckLakeUpdate::GetGlobalSinkState(ClientContext &context) const {
	auto result = make_uniq<DuckLakeUpdateGlobalState>();
	copy_op.sink_state = copy_op.GetGlobalSinkState(context);
	delete_op.sink_state = delete_op.GetGlobalSinkState(context);
	return std::move(result);
}

unique_ptr<LocalSinkState> DuckLakeUpdate::GetLocalSinkState(ExecutionContext &context) const {
	auto result = make_uniq<DuckLakeUpdateLocalState>();
	result->copy_local_state = copy_op.GetLocalSinkState(context);
	result->delete_local_state = delete_op.GetLocalSinkState(context);

	vector<LogicalType> delete_types;
	delete_types.emplace_back(LogicalType::VARCHAR);
	delete_types.emplace_back(LogicalType::UBIGINT);
	delete_types.emplace_back(LogicalType::BIGINT);

	vector<LogicalType> expression_types;
	result->expression_executor = make_uniq<ExpressionExecutor>(context.client, expressions);
	for (auto &expr : result->expression_executor->expressions) {
		expression_types.push_back(expr->return_type);
	}

	for (auto &type : expression_types) {
		if (DuckLakeTypes::RequiresCast(type)) {
			type = DuckLakeTypes::GetCastedType(type);
		}
	}
	result->update_expression_chunk.Initialize(context.client, expression_types);
	// updates also write the row id to the file, so the final version needs the row_id placed
	// right after the physical columns (before computed partition columns).
	vector<LogicalType> insert_types = expression_types;
	auto physical_column_count = columns.size();
	D_ASSERT(physical_column_count <= insert_types.size());
	insert_types.insert(insert_types.begin() + physical_column_count, LogicalType::BIGINT);
	result->insert_chunk.Initialize(context.client, insert_types);

	result->delete_chunk.Initialize(context.client, delete_types);

	// initialize user_columns_chunk for inlining (physical columns only)
	if (inline_row_limit > 0) {
		vector<LogicalType> user_types;
		for (idx_t i = 0; i < physical_column_count; i++) {
			user_types.push_back(expression_types[i]);
		}
		result->user_columns_chunk.Initialize(context.client, user_types);
	}

	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType DuckLakeUpdate::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<DuckLakeUpdateGlobalState>();
	auto &lstate = input.local_state.Cast<DuckLakeUpdateLocalState>();

	// filter duplicate row IDs using deletion info (last 3 columns)
	idx_t delete_idx_start = chunk.ColumnCount() - DELETION_INFO_SIZE;
	auto &file_index_vec = chunk.data[delete_idx_start + 1];
	auto &row_number_vec = chunk.data[delete_idx_start + 2];

	UnifiedVectorFormat file_index_data, row_number_data;
	file_index_vec.ToUnifiedFormat(chunk.size(), file_index_data);
	row_number_vec.ToUnifiedFormat(chunk.size(), row_number_data);
	auto file_indices = UnifiedVectorFormat::GetData<uint64_t>(file_index_data);
	auto row_numbers = UnifiedVectorFormat::GetData<int64_t>(row_number_data);

	SelectionVector sel(chunk.size());
	idx_t sel_count = 0;
	{
		lock_guard<mutex> guard(gstate.seen_rows_lock);
		for (idx_t i = 0; i < chunk.size(); i++) {
			auto file_idx = file_index_data.sel->get_index(i);
			auto row_idx = row_number_data.sel->get_index(i);
			FileRowId key {file_indices[file_idx], row_numbers[row_idx]};
			if (gstate.seen_rows.insert(key).second) {
				sel.set_index(sel_count++, i);
			}
		}
	}

	if (sel_count == 0) {
		// all rows were duplicates
		return SinkResultType::NEED_MORE_INPUT;
	}

	// slice to non-duplicate rows only
	chunk.Slice(sel, sel_count);

	// push the to-be-inserted data into the copy
	auto &update_expression_chunk = lstate.update_expression_chunk;
	auto &insert_chunk = lstate.insert_chunk;

	update_expression_chunk.SetCardinality(chunk.size());
	insert_chunk.SetCardinality(chunk.size());
	lstate.expression_executor->Execute(chunk, update_expression_chunk);

	const idx_t physical_column_count = columns.size();
	const idx_t expression_column_count = update_expression_chunk.ColumnCount();
	D_ASSERT(expression_column_count >= physical_column_count);
	const idx_t partition_column_count = expression_column_count - physical_column_count;
	const idx_t insert_column_count = insert_chunk.ColumnCount();
	D_ASSERT(insert_column_count >= expression_column_count);
	// virtual columns (PlanUpdate sets WRITE_ROW_ID, so there is exactly one: the row id) must sit between the physical
	// and partition columns
	const idx_t virtual_column_count = insert_column_count - expression_column_count;
	D_ASSERT(virtual_column_count == 1);

	// copy the physical columns directly
	for (idx_t i = 0; i < physical_column_count; i++) {
		insert_chunk.data[i].Reference(update_expression_chunk.data[i]);
	}
	// reference the row id right after the physical columns
	insert_chunk.data[physical_column_count].Reference(chunk.data[row_id_index]);
	// place computed partition columns after the virtual columns
	for (idx_t part_idx = 0; part_idx < partition_column_count; part_idx++) {
		insert_chunk.data[physical_column_count + virtual_column_count + part_idx].Reference(
		    update_expression_chunk.data[physical_column_count + part_idx]);
	}

	bool inlined = false;
	if (inline_row_limit > 0 && !lstate.local_overflow) {
		idx_t new_total = gstate.total_inlined_rows.fetch_add(chunk.size()) + chunk.size();
		if (new_total > inline_row_limit) {
			// exceeded limit - set overflow and flush any buffered data
			gstate.inline_overflow = true;
			lstate.local_overflow = true;
			if (lstate.local_inlined_data) {
				// flush previously buffered data to copy_op
				FlushLocalInlinedDataToCopy(context, lstate, input);
			}
		} else {
			// buffer user columns + row_ids for inlining
			if (!lstate.local_inlined_data) {
				lstate.local_inlined_data = make_uniq<ColumnDataCollection>(context.client, lstate.user_columns_chunk.GetTypes());
			}
			lstate.user_columns_chunk.SetCardinality(chunk.size());
			for (idx_t i = 0; i < physical_column_count; i++) {
				lstate.user_columns_chunk.data[i].Reference(insert_chunk.data[i]);
			}
			lstate.local_inlined_data->Append(lstate.user_columns_chunk);
			// extract row_ids
			UnifiedVectorFormat row_id_format;
			chunk.data[row_id_index].ToUnifiedFormat(chunk.size(), row_id_format);
			auto row_id_data = UnifiedVectorFormat::GetData<int64_t>(row_id_format);
			for (idx_t r = 0; r < chunk.size(); r++) {
				auto idx = row_id_format.sel->get_index(r);
				lstate.local_inlined_row_ids.push_back(row_id_data[idx]);
			}
			inlined = true;
		}
	}

	if (!inlined) {
		OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_local_state, input.interrupt_state};
		copy_op.Sink(context, insert_chunk, copy_input);
	}

	// push the rowids into the delete (always, regardless of inlining)
	auto &delete_chunk = lstate.delete_chunk;
	delete_chunk.SetCardinality(chunk.size());
	for (idx_t i = 0; i < DELETION_INFO_SIZE; i++) {
		delete_chunk.data[i].Reference(chunk.data[delete_idx_start + i]);
	}

	OperatorSinkInput delete_input {*delete_op.sink_state, *lstate.delete_local_state, input.interrupt_state};
	delete_op.Sink(context, delete_chunk, delete_input);

	lstate.updated_count += chunk.size();
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Inlining helpers
//===--------------------------------------------------------------------===//
void DuckLakeUpdate::FlushLocalInlinedDataToCopy(ExecutionContext &context, DuckLakeUpdateLocalState &lstate,
                                                  OperatorSinkInput &input) const {
	if (!lstate.local_inlined_data) {
		return;
	}
	auto physical_column_count = columns.size();
	ColumnDataScanState scan_state;
	lstate.local_inlined_data->InitializeScan(scan_state);
	DataChunk scan_chunk;
	scan_chunk.Initialize(context.client, lstate.local_inlined_data->Types());

	DataChunk flush_chunk;
	flush_chunk.Initialize(context.client, lstate.insert_chunk.GetTypes());

	idx_t row_id_offset = 0;
	while (true) {
		lstate.local_inlined_data->Scan(scan_state, scan_chunk);
		if (scan_chunk.size() == 0) {
			break;
		}
		flush_chunk.SetCardinality(scan_chunk.size());
		// copy physical columns
		for (idx_t i = 0; i < physical_column_count; i++) {
			flush_chunk.data[i].Reference(scan_chunk.data[i]);
		}
		// use actual row_ids from the buffered local_inlined_row_ids
		auto row_id_out = FlatVector::GetData<int64_t>(flush_chunk.data[physical_column_count]);
		for (idx_t r = 0; r < scan_chunk.size(); r++) {
			row_id_out[r] = lstate.local_inlined_row_ids[row_id_offset + r];
		}
		row_id_offset += scan_chunk.size();

		OperatorSinkInput copy_input {*copy_op.sink_state, *lstate.copy_local_state, input.interrupt_state};
		copy_op.Sink(context, flush_chunk, copy_input);
	}
	lstate.local_inlined_data.reset();
	lstate.local_inlined_row_ids.clear();
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType DuckLakeUpdate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &global_state = input.global_state.Cast<DuckLakeUpdateGlobalState>();
	auto &local_state = input.local_state.Cast<DuckLakeUpdateLocalState>();

	// if another thread caused overflow but this thread still has buffered data, flush it BEFORE copy_op.Combine
	if (global_state.inline_overflow && !local_state.local_overflow && local_state.local_inlined_data) {
		local_state.local_overflow = true;
		OperatorSinkInput sink_input {*copy_op.sink_state, *local_state.copy_local_state, input.interrupt_state};
		FlushLocalInlinedDataToCopy(context, local_state, sink_input);
	}

	OperatorSinkCombineInput copy_combine_input {*copy_op.sink_state, *local_state.copy_local_state,
	                                             input.interrupt_state};
	auto result = copy_op.Combine(context, copy_combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("DuckLakeUpdate::Combine does not support async child operators");
	}
	OperatorSinkCombineInput del_combine_input {*delete_op.sink_state, *local_state.delete_local_state,
	                                            input.interrupt_state};
	result = delete_op.Combine(context, del_combine_input);
	if (result != SinkCombineResultType::FINISHED) {
		throw InternalException("DuckLakeUpdate::Combine does not support async child operators");
	}

	// merge per-thread inlined data into global (if not overflow)
	if (!global_state.inline_overflow && local_state.local_inlined_data) {
		lock_guard<mutex> guard(global_state.inline_lock);
		if (!global_state.global_inlined_data) {
			global_state.global_inlined_data = std::move(local_state.local_inlined_data);
		} else {
			ColumnDataAppendState append_state;
			global_state.global_inlined_data->InitializeAppend(append_state);
			for (auto &chunk : local_state.local_inlined_data->Chunks()) {
				global_state.global_inlined_data->Append(append_state, chunk);
			}
		}
		global_state.global_inlined_row_ids.insert(global_state.global_inlined_row_ids.end(),
		                                           local_state.local_inlined_row_ids.begin(),
		                                           local_state.local_inlined_row_ids.end());
	}

	global_state.total_updated_count += local_state.updated_count;
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType DuckLakeUpdate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                          OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<DuckLakeUpdateGlobalState>();

	// always finalize copy_op and delete_op
	OperatorSinkFinalizeInput copy_finalize_input {*copy_op.sink_state, input.interrupt_state};
	auto result = copy_op.Finalize(pipeline, event, context, copy_finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("DuckLakeUpdate::Finalize does not support async child operators");
	}
	OperatorSinkFinalizeInput del_finalize_input {*delete_op.sink_state, input.interrupt_state};
	result = delete_op.Finalize(pipeline, event, context, del_finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("DuckLakeUpdate::Finalize does not support async child operators");
	}

	if (!gstate.inline_overflow && gstate.global_inlined_data) {
		// data was inlined - compute stats and push to transaction
		auto inlined_result = DuckLakeInlineData::ComputeInlinedStats(*gstate.global_inlined_data, table);
		inlined_result->data = std::move(gstate.global_inlined_data);
		inlined_result->explicit_row_ids = std::move(gstate.global_inlined_row_ids);

		auto &transaction = DuckLakeTransaction::Get(context, table.ParentCatalog());
		transaction.AppendInlinedData(table.GetTableId(), std::move(inlined_result));
		return SinkFinalizeType::READY;
	}

	// normal path: scan the copy operator and sink into the insert operator
	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);
	auto global_source = copy_op.GetGlobalSourceState(context);
	auto local_source = copy_op.GetLocalSourceState(execution_context, *global_source);

	DataChunk copy_source_chunk;
	copy_source_chunk.Initialize(context, copy_op.types);

	auto global_sink = insert_op.GetGlobalSinkState(context);
	auto local_sink = insert_op.GetLocalSinkState(execution_context);

	OperatorSourceInput source_input {*global_source, *local_source, input.interrupt_state};
	OperatorSinkInput sink_input {*global_sink, *local_sink, input.interrupt_state};
	while (true) {
		auto source_result = copy_op.GetData(execution_context, copy_source_chunk, source_input);
		if (copy_source_chunk.size() == 0) {
			break;
		}
		if (source_result == SourceResultType::BLOCKED) {
			throw InternalException("DuckLakeUpdate::Finalize does not support async child operators");
		}

		auto sink_result = insert_op.Sink(execution_context, copy_source_chunk, sink_input);
		if (sink_result == SinkResultType::BLOCKED) {
			throw InternalException("DuckLakeUpdate::Finalize does not support async child operators");
		}
		if (source_result == SourceResultType::FINISHED) {
			break;
		}
	}

	OperatorSinkFinalizeInput insert_finalize_input {*global_sink, input.interrupt_state};
	result = insert_op.Finalize(pipeline, event, context, insert_finalize_input);
	if (result != SinkFinalizeType::READY) {
		throw InternalException("DuckLakeUpdate::Finalize does not support async child operators");
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// GetData
//===--------------------------------------------------------------------===//
SourceResultType DuckLakeUpdate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                 OperatorSourceInput &input) const {
	auto &global_state = sink_state->Cast<DuckLakeUpdateGlobalState>();
	auto value = Value::BIGINT(NumericCast<int64_t>(global_state.total_updated_count.load()));
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, value);
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//
string DuckLakeUpdate::GetName() const {
	return "DUCKLAKE_UPDATE";
}

InsertionOrderPreservingMap<string> DuckLakeUpdate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Table Name"] = table.name;
	return result;
}

static unique_ptr<Expression> GetFunction(ClientContext &context, unique_ptr<BoundReferenceExpression> column_reference,
                                          const string &function_name) {
	vector<unique_ptr<Expression>> children;
	children.emplace_back(std::move(column_reference));
	ErrorData error;
	FunctionBinder binder(context);
	auto function = binder.BindScalarFunction(DEFAULT_SCHEMA, function_name, std::move(children), error, false);
	if (!function) {
		error.Throw();
	}
	return function;
}

static unique_ptr<Expression> GetPartitionExpressionForUpdate(ClientContext &context,
                                                              unique_ptr<BoundReferenceExpression> column_reference,
                                                              const DuckLakePartitionField &field) {
	switch (field.transform.type) {
	case DuckLakeTransformType::IDENTITY:
		return std::move(column_reference);
	case DuckLakeTransformType::YEAR:
		return GetFunction(context, std::move(column_reference), "year");
	case DuckLakeTransformType::MONTH:
		return GetFunction(context, std::move(column_reference), "month");
	case DuckLakeTransformType::DAY:
		return GetFunction(context, std::move(column_reference), "day");
	case DuckLakeTransformType::HOUR:
		return GetFunction(context, std::move(column_reference), "hour");
	default:
		throw NotImplementedException("Unsupported partition transform type in GetPartitionExpressionForUpdate");
	}
}

PhysicalOperator &DuckLakeCatalog::PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
                                              PhysicalOperator &child_plan) {
	if (op.return_chunk) {
		throw BinderException("RETURNING clause not yet supported for updates of a DuckLake table");
	}
	for (auto &expr : op.expressions) {
		if (expr->type == ExpressionType::VALUE_DEFAULT) {
			throw BinderException("SET DEFAULT is not yet supported for updates of a DuckLake table");
		}
	}
	auto &table = op.table.Cast<DuckLakeTableEntry>();
	DuckLakeCopyInput copy_input(context, table);
	copy_input.virtual_columns = InsertVirtualColumns::WRITE_ROW_ID;
	auto &copy_op = DuckLakeInsert::PlanCopyForInsert(context, planner, copy_input, nullptr);
	// plan the delete
	vector<idx_t> row_id_indexes;
	for (idx_t i = 0; i < DuckLakeUpdate::DELETION_INFO_SIZE; i++) {
		row_id_indexes.push_back(i);
	}
	auto &delete_op = DuckLakeDelete::PlanDelete(context, planner, table, child_plan, std::move(row_id_indexes),
	                                             copy_input.encryption_key, false);
	// plan the actual insert
	auto &insert_op = DuckLakeInsert::PlanInsert(context, planner, table, copy_input.encryption_key);

	vector<unique_ptr<Expression>> expressions;
	unordered_map<idx_t, idx_t> expression_map;

	for (idx_t i = 0; i < op.columns.size(); i++) {
		expression_map[op.columns[i].index] = i;
	}
	for (idx_t i = 0; i < op.columns.size(); i++) {
		expressions.push_back(op.expressions[expression_map[i]]->Copy());
	}
	bool all_identity = true;
	if (copy_input.partition_data) {
		for (auto &field : copy_input.partition_data->fields) {
			if (field.transform.type != DuckLakeTransformType::IDENTITY) {
				all_identity = false;
				break;
			}
		}
		if (!all_identity) {
			for (auto &field : copy_input.partition_data->fields) {
				optional_idx col_idx;
				DuckLakeInsert::GetTopLevelColumn(copy_input, field.field_id, col_idx);
				D_ASSERT(col_idx.IsValid());
				auto &child_expression = expressions[col_idx.GetIndex()]->Cast<BoundReferenceExpression>();
				auto column_reference =
				    make_uniq<BoundReferenceExpression>(child_expression.return_type, child_expression.index);
				expressions.push_back(GetPartitionExpressionForUpdate(context, std::move(column_reference), field));
			}
		}
	}

	for (auto &expr : expressions) {
		if (DuckLakeTypes::RequiresCast(expr->return_type)) {
			auto target_type = DuckLakeTypes::GetCastedType(expr->return_type);
			expr = BoundCastExpression::AddCastToType(context, std::move(expr), std::move(target_type));
		}
	}

	// check if we can inline the update insertions
	idx_t update_inline_row_limit = 0;
	auto &ducklake_schema = table.ParentSchema().Cast<DuckLakeSchemaEntry>();
	auto &duck_transaction = DuckLakeTransaction::Get(context, *this);
	auto &metadata_manager = duck_transaction.GetMetadataManager();
	idx_t data_inlining_row_limit = DataInliningRowLimit(ducklake_schema.GetSchemaId(), table.GetTableId());
	// check all conditions for inlining:
	// 1) inlining must be enabled
	// 2) no column name conflicts with system columns
	// 3) all column types must support inlining
	// 4) no non-identity partition transforms (we can't reconstruct partition columns on overflow)
	// 5) auto-commit: within explicit transactions, the reader would show wrong row_ids
	//    for update-inlined data (TRANSACTION_LOCAL_ID_START + ordinal instead of original row_ids)
	bool has_non_identity_partitions = copy_input.partition_data && !all_identity;
	vector<LogicalType> user_column_types;
	for (idx_t i = 0; i < op.columns.size(); i++) {
		user_column_types.push_back(table.GetColumn(LogicalIndex(op.columns[i].index)).GetType());
	}
	bool is_auto_commit = context.transaction.IsAutoCommit();
	if (data_inlining_row_limit > 0 && !DuckLakeUtil::HasInlinedSystemColumnConflict(table.GetColumns()) &&
	    metadata_manager.SupportsInliningTypes(user_column_types) && !has_non_identity_partitions &&
	    is_auto_commit) {
		update_inline_row_limit = data_inlining_row_limit;
	}

	return planner.Make<DuckLakeUpdate>(table, op.columns, child_plan, copy_op, delete_op, insert_op, expressions,
	                                    update_inline_row_limit);
}

void DuckLakeTableEntry::BindUpdateConstraints(Binder &binder, LogicalGet &get, LogicalProjection &proj,
                                               LogicalUpdate &update, ClientContext &context) {
	// all updates in DuckLake are deletes + inserts
	update.update_is_del_and_insert = true;

	// push projections for all columns that are not projected yet
	// FIXME: this is almost a copy of LogicalUpdate::BindExtraColumns aside from the duplicate elimination
	// add that to main DuckDB
	auto &column_ids = get.GetColumnIds();
	for (auto &column : columns.Physical()) {
		auto physical_index = column.Physical();
		bool found = false;
		for (auto &col : update.columns) {
			if (col == physical_index) {
				found = true;
				break;
			}
		}
		if (found) {
			// already updated
			continue;
		}
		// check if the column is already projected
		optional_idx column_id_index;
		for (idx_t i = 0; i < column_ids.size(); i++) {
			if (column_ids[i].GetPrimaryIndex() == physical_index.index) {
				column_id_index = i;
				break;
			}
		}
		if (!column_id_index.IsValid()) {
			// not yet projected - add to a projection list
			column_id_index = column_ids.size();
			get.AddColumnId(physical_index.index);
		}
		// column is not projected yet: project it by adding the clause "i=i" to the set of updated columns
		update.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(proj.table_index, proj.expressions.size())));
		proj.expressions.push_back(make_uniq<BoundColumnRefExpression>(
		    column.Type(), ColumnBinding(get.table_index, column_id_index.GetIndex())));
		get.AddColumnId(physical_index.index);
		update.columns.push_back(physical_index);
	}
}

} // namespace duckdb
