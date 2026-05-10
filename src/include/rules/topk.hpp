#ifndef IVM_TOPK_RULE_HPP
#define IVM_TOPK_RULE_HPP

#include "rules/rule.hpp"

namespace duckdb {

class IvmTopKRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif // IVM_TOPK_RULE_HPP
