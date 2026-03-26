#ifndef IVM_CHECKER_HPP
#define IVM_CHECKER_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

/// Walk the logical plan tree and throw NotImplementedException if any
/// operator, join type, or aggregate function is not supported by IVM.
void ValidateIVMPlan(LogicalOperator *plan);

} // namespace duckdb

#endif // IVM_CHECKER_HPP
