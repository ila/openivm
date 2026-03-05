#ifndef OPENIVM_REWRITE_RULE_HPP
#define OPENIVM_REWRITE_RULE_HPP

#include "duckdb.hpp"

namespace duckdb {

struct PlanWrapper {
	PlanWrapper(OptimizerExtensionInput &input_, unique_ptr<LogicalOperator> &plan_, string &view_,
	            LogicalOperator *&root_)
	    : input(input_), plan(plan_), view(view_), root(root_) {
	}
	OptimizerExtensionInput &input;
	unique_ptr<LogicalOperator> &plan;
	string &view;
	LogicalOperator *&root;
	const LogicalType mul_type = LogicalType::BOOLEAN;
};

struct ModifiedPlan {
	ModifiedPlan(unique_ptr<LogicalOperator> op_, ColumnBinding mul_binding_)
	    : op(std::move(op_)), mul_binding(mul_binding_) {
	}

	unique_ptr<LogicalOperator> op;
	ColumnBinding mul_binding;
};

class IVMRewriteRule : public OptimizerExtension {
public:
	IVMRewriteRule() {
		optimize_function = IVMRewriteRuleFunction;
	}

	static void AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, string &view_name,
	                          string &view_catalog_name, string &view_schema_name);

	static ModifiedPlan ModifyPlan(PlanWrapper pw);

	static ModifiedPlan HandleJoinSubtree(PlanWrapper pw);

	static void IVMRewriteRuleFunction(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb

#endif // OPENIVM_REWRITE_RULE_HPP
