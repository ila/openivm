#ifndef IVM_JOIN_RULE_HPP
#define IVM_JOIN_RULE_HPP

#include "rules/ivm_rule.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"

namespace duckdb {

class IvmJoinRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
