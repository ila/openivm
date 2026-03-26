#ifndef IVM_UNION_RULE_HPP
#define IVM_UNION_RULE_HPP

#include "rules/ivm_rule.hpp"

namespace duckdb {

class IvmUnionRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
