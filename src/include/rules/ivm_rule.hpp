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
	            LogicalOperator *&root_, const string &view_query_ = "")
	    : input(input_), plan(plan_), view(view_), root(root_), view_query(view_query_) {
	}
	OptimizerExtensionInput &input;
	unique_ptr<LogicalOperator> &plan;
	string &view;
	LogicalOperator *&root;
	const LogicalType mul_type = LogicalType::BOOLEAN;
	/// SQL text of the view query — used as fallback for Copy when serialization fails (e.g. DuckLake)
	string view_query;
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

struct SavedScanInfo {
	idx_t table_index;
	TableFunction saved_function;
	unique_ptr<FunctionData> saved_bind_data;
	shared_ptr<TableFunctionInfo> saved_function_info;
};
vector<SavedScanInfo> StubNonSerializableScans(LogicalOperator &op);
void RestoreScans(LogicalOperator &op, vector<SavedScanInfo> &saved);

} // namespace duckdb

#endif // IVM_RULE_HPP
