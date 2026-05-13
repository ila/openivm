#ifndef OPENIVM_WINDOW_RULE_HPP
#define OPENIVM_WINDOW_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IncrementalWindowRule : public IncrementalRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
	// Window functions (ROW_NUMBER, RANK, LAG, etc.) depend on partition order
	// and accumulated state — a single insert/delete can cascade through the
	// partition. Falls back to per-partition recompute.
	Linearity GetLinearity() const override {
		return Linearity::NON_LINEAR;
	}
};

} // namespace duckdb

#endif
