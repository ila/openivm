#include "rules/distinct.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

ModifiedPlan IvmDistinctRule::Rewrite(PlanWrapper pw) {
	auto *distinct_node = dynamic_cast<LogicalDistinct *>(pw.plan.get());
	OPENIVM_DEBUG_PRINT("[IvmDistinctRule] Rewriting DISTINCT node, %zu targets\n",
	                    distinct_node->distinct_targets.size());

	// 1. Recurse into child first (gets delta with multiplicity)
	auto child_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
	auto rewritten_child = std::move(child_mul.op);
	ColumnBinding input_mul_binding = child_mul.mul_binding;

	Binder &binder = pw.input.optimizer.binder;
	auto child_bindings = rewritten_child->GetColumnBindings();
	auto child_types = rewritten_child->types;

	// 2. Build a LOGICAL_AGGREGATE_AND_GROUP_BY that replaces the DISTINCT node.
	// Group-by keys = all child columns EXCEPT multiplicity
	// Aggregate = COUNT(*) as _ivm_distinct_count
	// Multiplicity is added as a group-by key (same as IvmAggregateRule)
	idx_t group_index = binder.GenerateTableIndex();
	idx_t aggregate_index = binder.GenerateTableIndex();

	// Create COUNT(*) aggregate expression
	auto count_star_func = CountStarFun::GetFunction();
	vector<unique_ptr<Expression>> count_args;
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_star_func), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = ivm::DISTINCT_COUNT_COL;

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(count_expr));

	auto agg_node = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));

	// Add all child columns as group-by keys (except multiplicity)
	GroupingSet grouping_set;
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		if (child_bindings[i] == input_mul_binding) {
			continue; // skip multiplicity — added separately below
		}
		auto group_expr = make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]);
		grouping_set.insert(agg_node->groups.size());
		agg_node->groups.push_back(std::move(group_expr));
		agg_node->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(child_types[i])));
	}

	// Add multiplicity as the last group-by key (same pattern as IvmAggregateRule)
	ColumnBinding mod_mul_binding;
	mod_mul_binding.column_index = agg_node->groups.size();
	mod_mul_binding.table_index = group_index;

	auto mul_expr = make_uniq<BoundColumnRefExpression>(ivm::MULTIPLICITY_COL, pw.mul_type, input_mul_binding);
	grouping_set.insert(agg_node->groups.size());
	agg_node->groups.push_back(std::move(mul_expr));
	agg_node->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(pw.mul_type)));

	agg_node->grouping_sets.push_back(std::move(grouping_set));

	agg_node->children.push_back(std::move(rewritten_child));
	agg_node->ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IvmDistinctRule] Done, replaced with AGGREGATE (%zu groups + 1 count), mul_binding: "
	                    "table=%lu col=%lu\n",
	                    agg_node->groups.size() - 1, (unsigned long)mod_mul_binding.table_index,
	                    (unsigned long)mod_mul_binding.column_index);

	return {std::move(agg_node), mod_mul_binding};
}

} // namespace duckdb
