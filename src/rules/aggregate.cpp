#include "rules/aggregate.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"

namespace duckdb {

ModifiedPlan IvmAggregateRule::Rewrite(PlanWrapper pw) {
	auto *agg_node = dynamic_cast<LogicalAggregate *>(pw.plan.get());
	OPENIVM_DEBUG_PRINT("[IvmAggregateRule] Rewriting AGGREGATE node, %zu groups, %zu aggregates\n",
	                    agg_node->groups.size(), agg_node->expressions.size());
	// Recurse into child first
	auto child_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
	pw.plan->children[0] = std::move(child_mul.op);
	ColumnBinding input_mul_binding = child_mul.mul_binding;

	auto modified_node = dynamic_cast<LogicalAggregate *>(pw.plan.get());

	// Add multiplicity as a new group-by key
	auto mult_group_by = make_uniq<BoundColumnRefExpression>(ivm::MULTIPLICITY_COL, pw.mul_type, input_mul_binding);
	ColumnBinding mod_mul_binding;
	mod_mul_binding.column_index = modified_node->groups.size();
	modified_node->groups.emplace_back(std::move(mult_group_by));

	auto mult_group_by_stats = make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(pw.mul_type));
	modified_node->group_stats.emplace_back(std::move(mult_group_by_stats));

	if (modified_node->grouping_sets.empty()) {
		modified_node->grouping_sets = {{0}};
	} else {
		idx_t gr = modified_node->grouping_sets[0].size();
		modified_node->grouping_sets[0].insert(gr);
	}

	mod_mul_binding.table_index = modified_node->group_index;
	pw.plan->Verify(pw.input.context);
	OPENIVM_DEBUG_PRINT("[IvmAggregateRule] Done, now %zu groups (added mul), mul_binding: table=%lu col=%lu\n",
	                    modified_node->groups.size(), (unsigned long)mod_mul_binding.table_index,
	                    (unsigned long)mod_mul_binding.column_index);
	return {std::move(pw.plan), mod_mul_binding};
}

} // namespace duckdb
