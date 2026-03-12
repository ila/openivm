#include "rules/ivm_filter_rule.hpp"
#include "core/openivm_debug.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

ModifiedPlan IvmFilterRule::Rewrite(PlanWrapper pw) {
	// Recurse into child first
	auto child_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
	pw.plan->children[0] = std::move(child_mul.op);
	ColumnBinding child_mul_binding = child_mul.mul_binding;

	if (pw.plan->expressions.empty()) {
		pw.plan->children[0]->Verify(pw.input.context);
		return {std::move(pw.plan->children[0]), child_mul_binding};
	}

	auto plan_as_filter = unique_ptr_cast<LogicalOperator, LogicalFilter>(std::move(pw.plan));
	plan_as_filter->ResolveOperatorTypes();
	if (!plan_as_filter->projection_map.empty()) {
		auto child_binds = plan_as_filter->children[0]->GetColumnBindings();
		idx_t mul_index = child_binds.size();
		bool mul_found = false;
		while (!mul_found && mul_index > 0) {
			--mul_index;
			if (child_binds[mul_index] == child_mul_binding) {
				mul_found = true;
			}
		}
		if (!mul_found) {
			throw InternalException("Filter's child does not have multiplicity column!");
		}
		plan_as_filter->projection_map.emplace_back(mul_index);
	}
	return {std::move(plan_as_filter), child_mul_binding};
}

} // namespace duckdb
