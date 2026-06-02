#include "delta/delta_operator.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

DeltaPlanFragment CompileUnionDelta(DeltaOperatorInput input) {
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::UNION_ALL_LINEAR);
	OPENIVM_DEBUG_PRINT("[DeltaUnion] Rewriting UNION ALL node, %zu children\n", input.plan->children.size());

	// Rewrite both children independently — delta(T1 UNION ALL T2) = delta(T1) UNION ALL delta(T2)
	auto left_mul = input.CompileChild(input.plan->children[0], input.root);
	input.plan->children[0] = std::move(left_mul.op);

	auto right_mul = input.CompileChild(input.plan->children[1], input.root);
	input.plan->children[1] = std::move(right_mul.op);

	// Update the UNION's column count to match the rewritten children (which now include multiplicity).
	// Use GetColumnBindings().size() to ensure consistency with what LPTS reads.
	auto *set_op = dynamic_cast<LogicalSetOperation *>(input.plan.get());
	set_op->column_count = input.plan->children[0]->GetColumnBindings().size();
	input.plan->ResolveOperatorTypes();

	// The multiplicity binding comes from the UNION's output bindings (last column)
	auto union_bindings = input.plan->GetColumnBindings();
	ColumnBinding new_mul_binding = union_bindings.back();

	OPENIVM_DEBUG_PRINT("[DeltaUnion] Done, mul_binding: table=%lu col=%lu\n",
	                    (unsigned long)new_mul_binding.table_index, (unsigned long)new_mul_binding.column_index);
	return {std::move(input.plan), new_mul_binding};
}

} // namespace duckdb
