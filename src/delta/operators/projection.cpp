#include "delta/delta_operator.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

DeltaPlanFragment CompileProjectionDelta(DeltaOperatorInput input) {
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::PROJECTION_APPEND_MULTIPLICITY);
	OPENIVM_DEBUG_PRINT("[DeltaProjection] Rewriting PROJECTION node, %zu expressions\n",
	                    input.plan->expressions.size());
	// Recurse into child first
	auto child_mul = input.CompileChild(input.plan->children[0], input.root);
	input.plan->children[0] = std::move(child_mul.op);
	ColumnBinding child_mul_binding = child_mul.mul_binding;

	auto projection_node = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(input.plan));
	auto mul_expression =
	    make_uniq<BoundColumnRefExpression>(openivm::MULTIPLICITY_COL, input.mul_type, child_mul_binding);
	projection_node->expressions.emplace_back(std::move(mul_expression));
	projection_node->types.clear();
	for (auto &expr : projection_node->expressions) {
		projection_node->types.push_back(expr->return_type);
	}

	const auto new_bindings = projection_node->GetColumnBindings();
	auto new_mul_binding = new_bindings.back();
	projection_node->Verify(input.context.input.context);
	OPENIVM_DEBUG_PRINT("[DeltaProjection] Done, %zu expressions (including mul), mul_binding: table=%lu col=%lu\n",
	                    projection_node->expressions.size(), (unsigned long)new_mul_binding.table_index,
	                    (unsigned long)new_mul_binding.column_index);
	return {std::move(projection_node), new_mul_binding};
}

} // namespace duckdb
