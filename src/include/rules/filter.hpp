#ifndef IVM_FILTER_RULE_HPP
#define IVM_FILTER_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IvmFilterRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
