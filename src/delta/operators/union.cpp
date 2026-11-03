#include "delta/delta_operator.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

DeltaPlanFragment CompileUnionDelta(DeltaOperatorInput input) {
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::UNION_ALL_LINEAR);
	OPENIVM_DEBUG_PRINT("[DeltaUnion] Rewriting UNION ALL node, %zu children\n", input.plan->children.size());

	auto &set_op = input.plan->Cast<LogicalSetOperation>();
	if (!set_op.setop_all) {
		throw NotImplementedException("UNION without ALL is not supported for delta compilation");
	}
	if (input.plan->children.empty()) {
		throw InternalException("UNION delta compilation requires at least one child");
	}

	for (auto &child : input.plan->children) {
		auto child_delta = input.CompileChild(child, input.root);
		child = std::move(child_delta.op);
	}

	// Update the UNION's column count to match the rewritten children (which now include multiplicity).
	// Use GetColumnBindings().size() to preserve the UNION's own output bindings for parent operators.
	set_op.column_count = input.plan->children[0]->GetColumnBindings().size();
	input.plan->ResolveOperatorTypes();

	auto union_bindings = input.plan->GetColumnBindings();
	ColumnBinding new_mul_binding = union_bindings.back();

	OPENIVM_DEBUG_PRINT("[DeltaUnion] Done, mul_binding: table=%lu col=%lu\n",
	                    (unsigned long)new_mul_binding.table_index.index,
	                    (unsigned long)new_mul_binding.column_index.GetIndex());
	return {std::move(input.plan), new_mul_binding};
}

} // namespace duckdb
