#include "rules/ivm_scan_rule.hpp"
#include "core/openivm_debug.hpp"

namespace duckdb {

ModifiedPlan IvmScanRule::Rewrite(PlanWrapper pw) {
	auto old_get = dynamic_cast<LogicalGet *>(pw.plan.get());
	OPENIVM_DEBUG_PRINT("[IvmScanRule] Rewriting GET node (table_index=%lu) -> delta scan\n",
	                    (unsigned long)old_get->table_index);
	DeltaGetResult result = CreateDeltaGetNode(pw.input.context, pw.input.optimizer.binder, old_get, pw.view);
	result.node->Verify(pw.input.context);
	OPENIVM_DEBUG_PRINT("[IvmScanRule] Delta scan created, result types: %zu\n", result.node->types.size());
	return {std::move(result.node), result.mul_binding};
}

} // namespace duckdb
