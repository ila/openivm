#include "ivm_scan_rule.hpp"
#include "openivm_debug.hpp"

namespace duckdb {

ModifiedPlan IvmScanRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("Create replacement get node\n");
	auto old_get = dynamic_cast<LogicalGet *>(pw.plan.get());
	DeltaGetResult result = CreateDeltaGetNode(pw.input.context, old_get, pw.view);
	result.node->Verify(pw.input.context);
	return {std::move(result.node), result.mul_binding};
}

} // namespace duckdb
