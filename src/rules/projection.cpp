#include "rules/projection.hpp"
#include "core/openivm_debug.hpp"

namespace duckdb {

ModifiedPlan IvmProjectionRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("[IvmProjectionRule] Rewriting PROJECTION node, %zu expressions\n",
	                    pw.plan->expressions.size());
	auto result = RewriteLinearProjectionWithMultiplicity(std::move(pw));
	OPENIVM_DEBUG_PRINT("[IvmProjectionRule] Done, %zu expressions (including mul), mul_binding: table=%lu col=%lu\n",
	                    result.op->expressions.size(), (unsigned long)result.mul_binding.table_index,
	                    (unsigned long)result.mul_binding.column_index);
	return result;
}

} // namespace duckdb
