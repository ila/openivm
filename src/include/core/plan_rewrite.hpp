#ifndef PLAN_REWRITE_HPP
#define PLAN_REWRITE_HPP

#include "core/derived_aggregate_output.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <functional>

namespace duckdb {

struct CreateMVPlanFacts;
class BoundWindowExpression;
class LogicalComparisonJoin;
class LogicalAggregate;
class LogicalProjection;
bool IsRunningWindowCandidate(const BoundWindowExpression &window);

/// The single-child path from a plan root down to the first operator that is neither a projection nor accepted by
/// `pass_through` (a MATERIALIZED_CTE passes through to its consumer, children[1]).
struct OutputSpine {
	vector<LogicalProjection *> projections; // root first
	LogicalOperator *parent = nullptr;       // operator directly above `bottom`, nullptr when `bottom` is the root
	LogicalOperator *bottom = nullptr;
};
OutputSpine FindOutputSpine(LogicalOperator *root, const std::function<bool(const LogicalOperator &)> &pass_through);
/// The aggregate at the bottom of the spine, directly or below a HAVING filter.
LogicalAggregate *SpineAggregate(const OutputSpine &spine);

/// Keep the first usable projection refresh key whose lineage stays on
/// preserved inputs; search its preserved subtree when that key is unsafe.
LogicalComparisonJoin *FindProjectionKeyJoin(LogicalOperator &plan);

struct PlanRewriteNeeds {
	bool inline_cte_refs = false;
	bool fold_constant_scalar_subqueries = false;
	bool aggregate_filters = false;
	bool distinct = false;
	bool derived_aggregates = false;
	bool has_aggregate = false;
	bool outer_join_support = false;
	bool blocking_set_operation = false;
	bool semi_anti_subqueries = false;
	bool running_window_state = false;
};

/// Strip AGG(...) FILTER (WHERE p) by converting to AGG(CASE WHEN p THEN arg
/// END). Must be called on both the SELECT plan (for LPTS serialization) and
/// the full CREATE plan (for AnalyzePlan / find_group_cols) so the checker sees
/// no FILTER aggregates.
void RewriteAggregateFilters(ClientContext &context, unique_ptr<LogicalOperator> &plan);

/// Fold uncorrelated constant scalar subqueries (e.g. `(SELECT k FROM
/// constants_cte)`) to literals. Must be called on both the SELECT plan (so
/// LPTS emits the folded query) and the full CREATE plan (so AnalyzePlan
/// doesn't see the scalar-subquery guard's ungrouped `first()` aggregate and
/// degrade the view to FULL_REFRESH). Requires CTEs already inlined. Only folds
/// provably-constant single-row subtrees — never a subquery that scans base
/// tables.
void FoldConstantScalarSubqueries(ClientContext &context, unique_ptr<LogicalOperator> &plan);

/// Rewrite a materialized view's logical plan for IVM compatibility.
/// Modifies the plan in place:
/// - DISTINCT → AGGREGATE + COUNT(*) as openivm_distinct_count
/// - AVG(x) → SUM(x) as openivm_sum_<alias>, COUNT(x) as openivm_count_<alias>,
/// SUM/COUNT as <alias>
/// - LEFT/RIGHT JOIN → add projection with openivm_left_key column
/// planner_names: column names from Planner.names (user aliases). These are set
/// on aggregate expressions so LPTS can pick them up. Unaliased aggregates get
/// auto-generated names. needs is collected by InlineCtesIfPresent so passes
/// that cannot match the plan are skipped without another preflight traversal.
void PlanRewrite(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                 vector<string> &planner_names, const PlanRewriteNeeds &needs);

/// Expand materialized CTE references into independently renumbered plan copies.
/// Join delta compilation needs one leaf per CTE occurrence so it can generate
/// every inclusion-exclusion cross-term. This is also used after rebinding the
/// compact CTE graph emitted by LPTS for a stored materialized-view definition.
void InlineMaterializedCteRefs(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan);

bool RenderPlanOutputExpression(const Expression &expression, LogicalOperator &plan, const CreateMVPlanFacts &facts,
                                const vector<string> &output_names, string &sql);
string ResolvePlanOutputColumn(ColumnBinding binding, LogicalOperator &plan, const CreateMVPlanFacts &facts,
                               const vector<string> &output_names);
vector<string> ResolvePlanOutputColumns(ColumnBinding binding, LogicalOperator &plan, const CreateMVPlanFacts &facts,
                                        const vector<string> &output_names);

void InjectHiddenGroupKeys(unique_ptr<LogicalOperator> &plan);

/// Strip the HAVING filter (FILTER above AGGREGATE) from the plan.
/// Returns the HAVING predicate as SQL using output column aliases, or empty if
/// no HAVING. The plan is modified in place: the FILTER node is removed. If the
/// HAVING predicate references aggregates not in the SELECT list (e.g.
/// `SUM(COALESCE(x, 0))` when only `SUM(x)` is selected), those aggregates are
/// added to the PROJECTION as hidden columns (named `openivm_having_N`) and
/// `output_names` is extended accordingly — the predicate references the hidden
/// column names instead of re-rendering the aggregate expression.
string StripHavingFilter(unique_ptr<LogicalOperator> &plan, vector<string> &output_names);

/// Extract deterministic scalar outputs computed by the top projection from
/// aggregate outputs that are also projected by name. `complete` is false when
/// any user-visible expression cannot be rendered solely in terms of stored
/// output columns, so refresh compilation can fail closed.
DerivedAggregateOutputInfo ExtractDerivedAggregateOutputs(const LogicalOperator &plan, const CreateMVPlanFacts &facts,
                                                          const vector<string> &output_names);

} // namespace duckdb

#endif // PLAN_REWRITE_HPP
