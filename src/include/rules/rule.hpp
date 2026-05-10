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
	// Integer-weighted Z-set multiplicity. Each delta row carries a signed weight:
	//   +k = inserted with multiplicity k (typically +1 for a single insert),
	//   -k = deleted with multiplicity k (typically -1 for a single delete).
	// Joins multiply weights (Z-set bilinear product); Möbius inclusion-exclusion
	// signs are applied in IvmJoinRule because base scans read R_now=R_old+ΔR.
	const LogicalType mul_type = LogicalType::INTEGER;
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

DeltaGetResult CreateDeltaGetNode(ClientContext &context, Binder &binder, LogicalGet *old_get, const string &view_name);
ColumnBinding RewriteLinearChild(PlanWrapper &pw, idx_t child_index = 0);
idx_t FindColumnBindingIndex(const vector<ColumnBinding> &bindings, ColumnBinding target, const string &context);
ModifiedPlan RewriteLinearProjectionWithMultiplicity(PlanWrapper pw);
ModifiedPlan RewriteLinearUnion(PlanWrapper pw);

} // namespace duckdb

#endif // IVM_RULE_HPP
