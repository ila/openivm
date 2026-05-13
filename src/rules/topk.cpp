#include "rules/topk.hpp"
#include "core/openivm_debug.hpp"
#include "rules/incremental_rewrite_rule.hpp"

namespace duckdb {

ModifiedPlan IvmTopKRule::Rewrite(PlanWrapper pw) {
	OPENIVM_DEBUG_PRINT("[IvmTopKRule] Stripping %s — top-k is non-linear; delta is computed unbounded\n",
	                    LogicalOperatorToString(pw.plan->type).c_str());

	// LIMIT, TOP_N, and standalone ORDER BY are stripped from the delta plan:
	//   - LIMIT/TOP_N: ΔLIMIT(R) != LIMIT(ΔR). The maintained state stays
	//     unbounded, and the user-facing view applies ORDER BY/LIMIT at read time.
	//   - ORDER BY: a materialized view is a table; tables have no inherent order.
	//     The runtime ordering is meaningless for IVM purposes.
	// We recurse into the only child, take its rewritten plan + multiplicity binding,
	// and return them directly — the LIMIT/TOP_N/ORDER_BY node is discarded.
	auto child_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[0], pw.view, pw.root);
	return {std::move(child_mul.op), child_mul.mul_binding};
}

} // namespace duckdb
