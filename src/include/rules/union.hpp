#ifndef OPENIVM_UNION_RULE_HPP
#define OPENIVM_UNION_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IncrementalUnionRule : public IncrementalRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
	// UNION ALL is bag-union, which is Z-set addition — linear.
	Linearity GetLinearity() const override {
		return Linearity::LINEAR;
	}
};

} // namespace duckdb

#endif
