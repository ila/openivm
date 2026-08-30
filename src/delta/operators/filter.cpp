#include "delta/delta_operator.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"

namespace duckdb {

DeltaPlanFragment CompileFilterDelta(DeltaOperatorInput input) {
	OPENIVM_DEBUG_PRINT("[DeltaFilter] Rewriting FILTER node, %zu filter expressions\n",
	                    input.plan->expressions.size());

	// HAVING clause: FILTER above AGGREGATE (possibly with intermediate PROJECTIONs).
	// Strip the filter from the delta plan — the delta only needs to identify affected
	// groups, not evaluate the HAVING condition. Group-recompute re-evaluates HAVING.
	if (!input.plan->expressions.empty()) {
		auto *walk = input.plan->children[0].get();
		// CREATE-time normalization usually removes HAVING. This remains for copied/internal plans whose exact
		// PROJECTION chain is a planner-shape contract rather than independently observable SQL behavior.
		while (walk->type == LogicalOperatorType::LOGICAL_PROJECTION && // mull-ignore: cxx_eq_to_ne
		       !walk->children.empty()) {                               // mull-ignore: cxx_remove_negation
			walk = walk->children[0].get();
		}
		if (walk->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) { // mull-ignore: cxx_eq_to_ne
			LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::FILTER_HAVING_STRIP);
			OPENIVM_DEBUG_PRINT("[DeltaFilter] HAVING filter above AGGREGATE -- stripping from delta\n");
			auto child_mul = input.CompileChild(input.plan->children[0], input.root);
			return child_mul;
		}
	}

	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::FILTER_LINEAR);

	auto child_mul = input.CompileChild(input.plan->children[0], input.root);
	input.plan->children[0] = std::move(child_mul.op);
	ColumnBinding child_mul_binding = child_mul.mul_binding;

	if (input.plan->expressions.empty()) {
		OPENIVM_DEBUG_PRINT("[DeltaFilter] Empty filter, passing through child directly\n");
		input.plan->children[0]->Verify(input.context.input.context);
		return {std::move(input.plan->children[0]), child_mul_binding};
	}

	auto plan_as_filter = unique_ptr_cast<LogicalOperator, LogicalFilter>(std::move(input.plan));
	plan_as_filter->ResolveOperatorTypes();
	if (!plan_as_filter->projection_map.empty()) {
		OPENIVM_DEBUG_PRINT("[DeltaFilter] Filter has projection_map, adding mul column index\n");
		auto child_binds = plan_as_filter->children[0]->GetColumnBindings();
		idx_t mul_index = child_binds.size();
		bool mul_found = false;
		// CompileChild guarantees the binding exists. The zero boundary prevents unsigned underflow only when that
		// internal contract is broken; SQL cannot construct such a plan.
		while (!mul_found && mul_index > 0) { // mull-ignore: cxx_gt_to_ge,cxx_gt_to_le
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
	OPENIVM_DEBUG_PRINT("[DeltaFilter] Done, mul_binding: table=%lu col=%lu\n",
	                    (unsigned long)child_mul_binding.table_index, (unsigned long)child_mul_binding.column_index);
	return {std::move(plan_as_filter), child_mul_binding};
}

} // namespace duckdb
