#ifndef OPENIVM_RULE_HPP
#define OPENIVM_RULE_HPP

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
	// signs are applied in IncrementalJoinRule because base scans read R_now=R_old+ΔR.
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
// IncrementalRule — base class for all operator-specific IVM rewrite rules
//==============================================================================

/// DBSP linearity classification of an operator's incremental form.
/// This is the algebraic taxonomy that determines how the delta-rule is derived:
///   - LINEAR     : Δ(Q(R)) = Q(ΔR). The rule applies the operator to the delta
///                  unchanged. Cost ∝ |delta|.
///   - BILINEAR   : Q is linear in each argument separately. Δ(R⋈S) expands by
///                  Z-set product times a Möbius inclusion-exclusion sign (in
///                  OpenIVM, because base scans read R_now = R_old + ΔR). Cost
///                  ∝ Σ_terms; pruned with FK pruning + empty-delta skipping.
///   - NON_LINEAR : Q is neither. DISTINCT, MIN/MAX, AVG, STDDEV, recursive
///                  fixpoints. The delta requires the accumulated state — there
///                  is no closed-form per-row rule, so OpenIVM falls back to
///                  group-recompute or full-refresh. Cost ∝ affected groups.
enum class Linearity { LINEAR, BILINEAR, NON_LINEAR };

class IncrementalRule {
public:
	virtual ~IncrementalRule() = default;
	virtual ModifiedPlan Rewrite(PlanWrapper pw) = 0;

	/// Linearity of this operator's delta rule. Used for documentation and
	/// dispatch-time assertions; does not affect runtime behaviour.
	virtual Linearity GetLinearity() const = 0;
};

//==============================================================================
// Shared helpers used by multiple rules
//==============================================================================

DeltaGetResult CreateDeltaGetNode(ClientContext &context, Binder &binder, LogicalGet *old_get, const string &view_name);

} // namespace duckdb

#endif // OPENIVM_RULE_HPP
