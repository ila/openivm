#include "rules/union.hpp"
#include "core/openivm_debug.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

ModifiedPlan IvmUnionRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("[IvmUnionRule] Rewriting UNION ALL node, %zu children\n", pw.plan->children.size());

	// Rewrite both children independently — delta(T1 UNION ALL T2) = delta(T1) UNION ALL delta(T2)
	auto left_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
	pw.plan->children[0] = std::move(left_mul.op);

	auto right_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[1], pw.view, pw.root);
	pw.plan->children[1] = std::move(right_mul.op);

	// Update the UNION's column count to match the rewritten children (which now include multiplicity).
	// Use GetColumnBindings().size() to ensure consistency with what LPTS reads.
	auto *set_op = dynamic_cast<LogicalSetOperation *>(pw.plan.get());
	set_op->column_count = pw.plan->children[0]->GetColumnBindings().size();
	pw.plan->ResolveOperatorTypes();

	// The multiplicity binding comes from the UNION's output bindings (last column)
	auto union_bindings = pw.plan->GetColumnBindings();
	ColumnBinding new_mul_binding = union_bindings.back();

	OPENIVM_DEBUG_PRINT("[IvmUnionRule] Done, mul_binding: table=%lu col=%lu\n",
	                    (unsigned long)new_mul_binding.table_index, (unsigned long)new_mul_binding.column_index);
	return {std::move(pw.plan), new_mul_binding};
}

} // namespace duckdb
