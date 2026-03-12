#ifndef IVM_PROJECTION_RULE_HPP
#define IVM_PROJECTION_RULE_HPP

#include "rules/ivm_rule.hpp"

namespace duckdb {

class IvmProjectionRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
