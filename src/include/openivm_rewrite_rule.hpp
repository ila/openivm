#ifndef OPENIVM_REWRITE_RULE_HPP
#define OPENIVM_REWRITE_RULE_HPP

#include "ivm_rule.hpp"

namespace duckdb {

class IVMRewriteRule : public OptimizerExtension {
public:
	IVMRewriteRule() {
		optimize_function = IVMRewriteRuleFunction;
	}

	static void AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, string &view_name,
	                          string &view_catalog_name, string &view_schema_name);

	/// Orchestrator: dispatches to the correct IvmRule based on operator type.
	static ModifiedPlan RewritePlan(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan, string &view,
	                                LogicalOperator *&root);

	static void IVMRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // OPENIVM_REWRITE_RULE_HPP
