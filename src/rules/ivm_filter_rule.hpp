#ifndef IVM_FILTER_RULE_HPP
#define IVM_FILTER_RULE_HPP

#include "ivm_rule.hpp"

namespace duckdb {

class IvmFilterRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
