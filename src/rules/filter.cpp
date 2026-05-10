#include "rules/filter.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

ModifiedPlan IvmFilterRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("[IvmFilterRule] Rewriting FILTER node, %zu filter expressions\n", pw.plan->expressions.size());

	// HAVING clause: FILTER above AGGREGATE (possibly with intermediate PROJECTIONs).
	// Strip the filter from the delta plan — the delta only needs to identify affected
	// groups, not evaluate the HAVING condition. Group-recompute re-evaluates HAVING.
	if (!pw.plan->expressions.empty()) {
		auto *walk = pw.plan->children[0].get();
		while (walk->type == LogicalOperatorType::LOGICAL_PROJECTION && !walk->children.empty()) {
			walk = walk->children[0].get();
		}
		if (walk->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			OPENIVM_DEBUG_PRINT("[IvmFilterRule] HAVING filter above AGGREGATE — stripping from delta\n");
			auto child_mul_binding = RewriteLinearChild(pw);
			return {std::move(pw.plan->children[0]), child_mul_binding};
		}
	}

	auto child_mul_binding = RewriteLinearChild(pw);

	if (pw.plan->expressions.empty()) {
		OPENIVM_DEBUG_PRINT("[IvmFilterRule] Empty filter, passing through child directly\n");
		pw.plan->children[0]->Verify(pw.input.context);
		return {std::move(pw.plan->children[0]), child_mul_binding};
	}

	auto plan_as_filter = unique_ptr_cast<LogicalOperator, LogicalFilter>(std::move(pw.plan));
	plan_as_filter->ResolveOperatorTypes();
	if (!plan_as_filter->projection_map.empty()) {
		OPENIVM_DEBUG_PRINT("[IvmFilterRule] Filter has projection_map, adding mul column index\n");
		auto child_binds = plan_as_filter->children[0]->GetColumnBindings();
		idx_t mul_index = FindColumnBindingIndex(child_binds, child_mul_binding, "Filter child");
		plan_as_filter->projection_map.emplace_back(mul_index);
	}
	OPENIVM_DEBUG_PRINT("[IvmFilterRule] Done, mul_binding: table=%lu col=%lu\n",
	                    (unsigned long)child_mul_binding.table_index, (unsigned long)child_mul_binding.column_index);
	return {std::move(plan_as_filter), child_mul_binding};
}

} // namespace duckdb
