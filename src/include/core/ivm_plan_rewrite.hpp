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
/// Returns the modified plan. The caller should use LPTS to convert to SQL.
void IVMPlanRewrite(ClientContext &context, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb

#endif // IVM_PLAN_REWRITE_HPP
