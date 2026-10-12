#include "storage/ducklake_catalog.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/execution/operator/persistent/physical_merge_into.hpp"
#include "storage/ducklake_insert.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

PhysicalOperator &DuckLakeCatalog::PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner,
                                                 LogicalMergeInto &op, PhysicalOperator &plan) {
	// DuckLake writes a deletion file per data file, so it can apply at most one UPDATE/DELETE to a given row
	idx_t update_delete_count = 0;
	for (auto &entry : op.actions) {
		for (auto &action : entry.second) {
			if (action->action_type == MergeActionType::MERGE_UPDATE ||
			    action->action_type == MergeActionType::MERGE_DELETE) {
				update_delete_count++;
			}
		}
	}
	if (update_delete_count > 1) {
		throw NotImplementedException("MERGE INTO with DuckLake only supports a single UPDATE/DELETE action currently");
	}
	auto &table = op.table.Cast<DuckLakeTableEntry>();
	auto &transaction = DuckLakeTransaction::Get(context, *this);
	auto matched = op.actions.find(MergeActionCondition::WHEN_MATCHED);
	transaction.merge_update_without_row_ids.clear();
	if (matched != op.actions.end() && transaction.HasTransactionLocalInserts(table.GetTableId())) {
		// rows inserted in this transaction get their row ids at commit, so update them in an action of their own
		auto &matched_actions = matched->second;
		for (idx_t i = 0; i < matched_actions.size(); i++) {
			auto &action = *matched_actions[i];
			if (action.action_type != MergeActionType::MERGE_UPDATE) {
				continue;
			}
			auto local_rows = make_uniq<BoundMergeIntoAction>();
			local_rows->action_type = action.action_type;
			local_rows->columns = action.columns;
			local_rows->update_is_del_and_insert = action.update_is_del_and_insert;
			for (auto &expr : action.expressions) {
				local_rows->expressions.push_back(expr->Copy());
			}
			auto is_local_row = BoundComparisonExpression::Create(
			    ExpressionType::COMPARE_GREATERTHANOREQUALTO,
			    make_uniq<BoundReferenceExpression>(LogicalType::BIGINT, op.row_id_start),
			    make_uniq<BoundConstantExpression>(
			        Value::BIGINT(NumericCast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START))));
			if (action.condition) {
				local_rows->condition = make_uniq<BoundConjunctionExpression>(
				    ExpressionType::CONJUNCTION_AND, action.condition->Copy(), std::move(is_local_row));
			} else {
				local_rows->condition = std::move(is_local_row);
			}
			matched_actions.insert(matched_actions.begin() + NumericCast<int64_t>(i), std::move(local_rows));
			transaction.merge_update_without_row_ids = {true, false};
			break;
		}
	}
	auto &merge_into = Catalog::PlanMergeInto(context, planner, op, plan).Cast<PhysicalMergeInto>();
	transaction.merge_update_without_row_ids.clear();
	for (auto &action : merge_into.actions) {
		if (action->action_type == MergeActionType::MERGE_INSERT) {
			action->op->Cast<DuckLakeInsert>().reads_table = true;
		}
	}
	return merge_into;
}

} // namespace duckdb
