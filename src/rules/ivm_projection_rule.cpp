#include "rules/ivm_projection_rule.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

ModifiedPlan IvmProjectionRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("[IvmProjectionRule] Rewriting PROJECTION node, %zu expressions\n",
	                    pw.plan->expressions.size());
	// Recurse into child first
	auto child_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
	pw.plan->children[0] = std::move(child_mul.op);
	ColumnBinding child_mul_binding = child_mul.mul_binding;

	auto projection_node = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(pw.plan));
	auto mul_expression = make_uniq<BoundColumnRefExpression>(ivm::MULTIPLICITY_COL, pw.mul_type, child_mul_binding);
	projection_node->expressions.emplace_back(std::move(mul_expression));

	const auto new_bindings = projection_node->GetColumnBindings();
	auto new_mul_binding = new_bindings.back();
	projection_node->Verify(pw.input.context);
	OPENIVM_DEBUG_PRINT("[IvmProjectionRule] Done, %zu expressions (including mul), mul_binding: table=%lu col=%lu\n",
	                    projection_node->expressions.size(), (unsigned long)new_mul_binding.table_index,
	                    (unsigned long)new_mul_binding.column_index);
	return {std::move(projection_node), new_mul_binding};
}

} // namespace duckdb
