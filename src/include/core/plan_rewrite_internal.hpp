#ifndef OPENIVM_PLAN_REWRITE_INTERNAL_HPP
#define OPENIVM_PLAN_REWRITE_INTERNAL_HPP

#include "duckdb.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

AggregateFunction BindAggregateByName(ClientContext &context, const string &name, const vector<LogicalType> &arg_types);
LogicalOperator *FindProjectionAggregateInput(unique_ptr<LogicalOperator> &plan, bool allow_having_filter);
void RewriteDerivedAggregates(ClientContext &context, unique_ptr<LogicalOperator> &plan, Optimizer &opt,
                              bool is_top = true);
bool RewriteSafeSemiAntiDelimGets(ClientContext &context, unique_ptr<LogicalOperator> &plan);

} // namespace duckdb

#endif // OPENIVM_PLAN_REWRITE_INTERNAL_HPP
