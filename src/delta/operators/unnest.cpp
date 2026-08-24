#include "delta/delta_operator.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <algorithm>

namespace duckdb {

static void PreserveMultiplicityInProjectionMaps(LogicalOperator &op, const ColumnBinding &mul_binding) {
	for (auto &child : op.children) {
		PreserveMultiplicityInProjectionMaps(*child, mul_binding);
	}
	auto *join_ptr = dynamic_cast<LogicalJoin *>(&op);
	if (join_ptr) {
		auto &join = *join_ptr;
		const idx_t projected_child_count = std::min<idx_t>(join.children.size(), 2);
		// A join owns exactly two projection maps; disabling the loop or indexing child_count corrupts its plan.
		for (idx_t child_idx = 0; child_idx < projected_child_count; // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
		     child_idx++) {
			// DuckDB's only child indexes are 0=left and 1=right; swapping maps leaves invalid column indexes.
			auto &projection_map =
			    child_idx == 0 ? join.left_projection_map : join.right_projection_map; // mull-ignore: cxx_eq_to_ne
			if (projection_map.empty()) {
				continue;
			}
			auto child_bindings = join.children[child_idx]->GetColumnBindings();
			idx_t mul_idx = DConstants::INVALID_INDEX;
			// This exact bounded scan locates a planner-owned binding; >= disables it and <= indexes past the vector.
			for (idx_t binding_idx = 0; binding_idx < child_bindings.size(); // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
			     binding_idx++) {
				if (child_bindings[binding_idx] == mul_binding) {
					mul_idx = binding_idx;
					break;
				}
			}
			// INVALID_INDEX is a sentinel, never a projection index.
			if (mul_idx != DConstants::INVALID_INDEX && // mull-ignore: cxx_ne_to_eq
			    std::find(projection_map.begin(), projection_map.end(), mul_idx) == projection_map.end()) {
				projection_map.push_back(mul_idx);
				OPENIVM_DEBUG_PRINT("[DeltaUnnest] Added mul col %lu to %s projection map\n", (unsigned long)mul_idx,
				                    child_idx == 0 ? "left" : "right");
				join.ResolveOperatorTypes();
			}
		}
	}
}

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
	PreserveMultiplicityInProjectionMaps(*input.plan, child_mul.mul_binding);

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
		// Equality is already out of bounds for the original_bindings access below.
		if (visible_output_idx >= original_bindings.size()) { // mull-ignore: cxx_ge_to_gt
			throw InternalException("DeltaUnnest: rewritten plan exposed more than %llu visible output bindings",
			                        (idx_t)original_bindings.size());
		}
		projection_exprs.push_back(make_uniq<BoundColumnRefExpression>(output_types[i], output_bindings[i]));
		replacer.replacement_bindings.emplace_back(original_bindings[visible_output_idx],
		                                           ColumnBinding(projection_index, visible_output_idx));
		visible_output_idx++;
	}
	if (visible_output_idx != original_bindings.size()) {
		throw InternalException("DeltaUnnest: expected %llu visible output bindings, found %llu",
		                        (idx_t)original_bindings.size(), visible_output_idx);
	}
	projection_exprs.push_back(
	    make_uniq<BoundColumnRefExpression>(openivm::MULTIPLICITY_COL, input.mul_type, child_mul.mul_binding));

	replacer.stop_operator = original_unnest;
	replacer.VisitOperator(*input.root);

	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_exprs));
	projection->children.push_back(std::move(input.plan));
	projection->ResolveOperatorTypes();

	auto projection_bindings = projection->GetColumnBindings();
	if (projection_bindings.empty()) {
		throw InternalException("DeltaUnnest: rewritten projection has no multiplicity binding");
	}
	auto mul_binding = projection_bindings.back();
	OPENIVM_DEBUG_PRINT("[DeltaUnnest] Done, mul_binding: table=%lu col=%lu\n", (unsigned long)mul_binding.table_index,
	                    (unsigned long)mul_binding.column_index);
	return {std::move(projection), mul_binding};
}

} // namespace duckdb
