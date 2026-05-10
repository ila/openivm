#ifndef IVM_DELIM_JOIN_RULE_HPP
#define IVM_DELIM_JOIN_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IvmDelimJoinRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
