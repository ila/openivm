#ifndef IVM_CHECKER_HPP
#define IVM_CHECKER_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

/// Walk the logical plan tree and check if all operators, join types,
/// aggregate functions, and scalar functions are supported for IVM.
/// Returns true if the plan is fully IVM-compatible.
/// Returns false if any unsupported construct is found (caller should use full refresh).
bool ValidateIVMPlan(LogicalOperator *plan);

} // namespace duckdb

#endif // IVM_CHECKER_HPP
