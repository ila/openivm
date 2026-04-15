#include "rules/window.hpp"
#include "core/openivm_debug.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

namespace duckdb {

ModifiedPlan IvmWindowRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("[IvmWindowRule] Rewriting WINDOW node, %zu expressions\n", pw.plan->expressions.size());

	// Passthrough: recurse into the child (which has scans/joins/aggregates),
	// then pass the WINDOW operator through unchanged. The multiplicity column
	// from the child propagates through — the window doesn't consume or produce it.
	auto child_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
	pw.plan->children[0] = std::move(child_mul.op);

	auto plan_as_window = unique_ptr_cast<LogicalOperator, LogicalWindow>(std::move(pw.plan));
	plan_as_window->ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IvmWindowRule] Done, mul_binding: table=%lu col=%lu\n",
	                    (unsigned long)child_mul.mul_binding.table_index,
	                    (unsigned long)child_mul.mul_binding.column_index);
	return {std::move(plan_as_window), child_mul.mul_binding};
}

} // namespace duckdb
