#ifndef OPENIVM_FILTER_RULE_HPP
#define OPENIVM_FILTER_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IncrementalFilterRule : public IncrementalRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
	Linearity GetLinearity() const override {
		return Linearity::LINEAR;
	}
};

} // namespace duckdb

#endif
