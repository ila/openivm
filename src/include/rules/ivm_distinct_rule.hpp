#ifndef IVM_DISTINCT_RULE_HPP
#define IVM_DISTINCT_RULE_HPP

#include "rules/ivm_rule.hpp"

namespace duckdb {

class IvmDistinctRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
