#ifndef IVM_PLAN_REWRITE_HPP
#define IVM_PLAN_REWRITE_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

/// Rewrite a materialized view's logical plan for IVM compatibility.
/// Modifies the plan in place:
/// - DISTINCT → AGGREGATE + COUNT(*) as _ivm_distinct_count
/// - AVG(x) → SUM(x) as _ivm_sum_<alias>, COUNT(x) as _ivm_count_<alias>, SUM/COUNT as <alias>
/// - LEFT/RIGHT JOIN → add projection with _ivm_left_key column
/// planner_names: column names from Planner.names (user aliases). These are set on
/// aggregate expressions so LPTS can pick them up. Unaliased aggregates get auto-generated names.
void IVMPlanRewrite(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                    vector<string> &planner_names);

/// Strip the HAVING filter (FILTER above AGGREGATE) from the plan.
/// Returns the HAVING predicate as SQL using output column aliases, or empty if no HAVING.
/// The plan is modified in place: the FILTER node is removed.
string StripHavingFilter(unique_ptr<LogicalOperator> &plan, const vector<string> &output_names);

} // namespace duckdb

#endif // IVM_PLAN_REWRITE_HPP
