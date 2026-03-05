#ifndef IVM_SCAN_RULE_HPP
#define IVM_SCAN_RULE_HPP

#include "ivm_rule.hpp"

namespace duckdb {

class IvmScanRule : public IvmRule {
public:
	ModifiedPlan Rewrite(PlanWrapper pw) override;
};

} // namespace duckdb

#endif
