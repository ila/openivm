#ifndef IVM_TOPK_RULE_HPP
#define IVM_TOPK_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IvmTopKRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;

	// LIMIT / TOP_N / ORDER BY are non-linear in Z-set algebra:
	//   Δ(LIMIT_k(R)) ≠ LIMIT_k(ΔR)
	// The delta rule strips the operator entirely so the inner plan delta flows
	// through unbounded. The user-facing view applies ORDER BY/LIMIT over the
	// incrementally maintained state.
	Linearity GetLinearity() const override {
		return Linearity::NON_LINEAR;
	}
};

} // namespace duckdb

#endif // IVM_TOPK_RULE_HPP
