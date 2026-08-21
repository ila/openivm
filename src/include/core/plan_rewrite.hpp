#ifndef PLAN_REWRITE_HPP
#define PLAN_REWRITE_HPP

#include "core/derived_aggregate_output.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

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
/// auto-generated names.
void PlanRewrite(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                 vector<string> &planner_names);

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
DerivedAggregateOutputInfo ExtractDerivedAggregateOutputs(const LogicalOperator &plan,
                                                          const vector<string> &output_names);

} // namespace duckdb

#endif // PLAN_REWRITE_HPP
