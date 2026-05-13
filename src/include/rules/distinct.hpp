#ifndef OPENIVM_DISTINCT_RULE_HPP
#define OPENIVM_DISTINCT_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IncrementalDistinctRule : public IncrementalRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
	// DISTINCT (δ in DBSP) drops duplicates and negative weights — non-linear
	// even on positive Z-sets. Implemented via group-recompute + COUNT(*).
	Linearity GetLinearity() const override {
		return Linearity::NON_LINEAR;
	}
};

} // namespace duckdb

#endif
