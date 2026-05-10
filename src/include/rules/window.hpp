#ifndef IVM_WINDOW_RULE_HPP
#define IVM_WINDOW_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IvmWindowRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
