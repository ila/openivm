#include "delta/delta_operator.hpp"
#include "core/openivm_debug.hpp"

namespace duckdb {

DeltaPlanFragment CompileScanDelta(DeltaOperatorInput input) {
	auto old_get = dynamic_cast<LogicalGet *>(input.plan.get());
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::SCAN_DELTA);
	OPENIVM_DEBUG_PRINT("[DeltaScan] Rewriting GET node (table_index=%lu) -> delta scan\n",
	                    (unsigned long)old_get->table_index);
	DeltaGetResult result = CreateDeltaGetNode(input.context.input.context, input.context.input.optimizer.binder,
	                                           old_get, input.context.view);
	result.node->Verify(input.context.input.context);
	OPENIVM_DEBUG_PRINT("[DeltaScan] Delta scan created, result types: %zu\n", result.node->types.size());
	return {std::move(result.node), result.mul_binding};
}

} // namespace duckdb
