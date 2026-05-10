#include "rules/union.hpp"
#include "core/openivm_debug.hpp"

namespace duckdb {

ModifiedPlan IvmUnionRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("[IvmUnionRule] Rewriting UNION ALL node, %zu children\n", pw.plan->children.size());

	auto result = RewriteLinearUnion(std::move(pw));
	OPENIVM_DEBUG_PRINT("[IvmUnionRule] Done, mul_binding: table=%lu col=%lu\n",
	                    (unsigned long)result.mul_binding.table_index, (unsigned long)result.mul_binding.column_index);
	return result;
}

} // namespace duckdb
