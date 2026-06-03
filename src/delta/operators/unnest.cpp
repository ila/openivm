#include "delta/delta_operator.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

DeltaPlanFragment CompileUnnestDelta(DeltaOperatorInput input) {
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::UNNEST_LINEAR);
	if (input.plan->children.size() != 1) {
		throw InternalException("DeltaUnnest: expected exactly one child");
	}

	auto original_bindings = input.plan->GetColumnBindings();
	auto *original_unnest = input.plan.get();
	auto child_mul = input.CompileChild(input.plan->children[0], input.root);
	input.plan->children[0] = std::move(child_mul.op);
	input.plan->ResolveOperatorTypes();

	auto output_bindings = input.plan->GetColumnBindings();
	auto output_types = input.plan->types;
	vector<unique_ptr<Expression>> projection_exprs;
	auto projection_index = input.context.input.optimizer.binder.GenerateTableIndex();
	ColumnBindingReplacer replacer;
	idx_t visible_output_idx = 0;
	for (idx_t i = 0; i < output_bindings.size(); i++) {
		if (output_bindings[i] == child_mul.mul_binding) {
			continue;
		}
		projection_exprs.push_back(make_uniq<BoundColumnRefExpression>(output_types[i], output_bindings[i]));
		if (visible_output_idx < original_bindings.size()) {
			replacer.replacement_bindings.emplace_back(original_bindings[visible_output_idx],
			                                           ColumnBinding(projection_index, visible_output_idx));
		}
		visible_output_idx++;
	}
	projection_exprs.push_back(
	    make_uniq<BoundColumnRefExpression>(openivm::MULTIPLICITY_COL, input.mul_type, child_mul.mul_binding));

	replacer.stop_operator = original_unnest;
	replacer.VisitOperator(*input.root);

	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_exprs));
	projection->children.push_back(std::move(input.plan));
	projection->ResolveOperatorTypes();

	auto projection_bindings = projection->GetColumnBindings();
	auto mul_binding = projection_bindings.back();
	OPENIVM_DEBUG_PRINT("[DeltaUnnest] Done, mul_binding: table=%lu col=%lu\n", (unsigned long)mul_binding.table_index,
	                    (unsigned long)mul_binding.column_index);
	return {std::move(projection), mul_binding};
}

} // namespace duckdb
