#ifndef IVM_RULE_HPP
#define IVM_RULE_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

//==============================================================================
// Shared types
//==============================================================================

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

struct DeltaGetResult {
	unique_ptr<LogicalOperator> node;
	ColumnBinding mul_binding;
};

//==============================================================================
// IvmRule — base class for all operator-specific IVM rewrite rules
//==============================================================================

class IvmRule {
public:
	virtual ~IvmRule() = default;
	virtual ModifiedPlan Rewrite(PlanWrapper pw) = 0;
};

//==============================================================================
// Shared helpers used by multiple rules
//==============================================================================

DeltaGetResult CreateDeltaGetNode(ClientContext &context, LogicalGet *old_get, const string &view_name);

} // namespace duckdb

#endif // IVM_RULE_HPP
