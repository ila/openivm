#ifndef OPENIVM_PROJECTION_RULE_HPP
#define OPENIVM_PROJECTION_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IncrementalProjectionRule : public IncrementalRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
	Linearity GetLinearity() const override {
		return Linearity::LINEAR;
	}
};

} // namespace duckdb

#endif
