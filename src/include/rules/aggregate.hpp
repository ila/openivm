#ifndef OPENIVM_AGGREGATE_RULE_HPP
#define OPENIVM_AGGREGATE_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IncrementalAggregateRule : public IncrementalRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
	// SUM/COUNT are linear in weights (DBSP §6); MIN/MAX/AVG/STDDEV are not —
	// those are detected during compile and routed to group-recompute. The rule
	// itself just propagates the multiplicity column through as a group key, so
	// it is structurally LINEAR; non-linearity is enforced at compile time.
	Linearity GetLinearity() const override {
		return Linearity::LINEAR;
	}
};

} // namespace duckdb

#endif
