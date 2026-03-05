#ifndef IVM_AGGREGATE_RULE_HPP
#define IVM_AGGREGATE_RULE_HPP

#include "ivm_rule.hpp"

namespace duckdb {

class IvmAggregateRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
