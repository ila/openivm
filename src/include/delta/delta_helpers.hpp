#ifndef OPENIVM_DELTA_HELPERS_HPP
#define OPENIVM_DELTA_HELPERS_HPP

#include "delta/delta_plan_fragment.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

struct DeltaGetResult {
	unique_ptr<LogicalOperator> node;
	ColumnBinding mul_binding;
};

// Restore the SQL arguments that DuckLake plan serialization omits.
void PopulateDuckLakeChangeScanParameters(LogicalGet &get);

DeltaGetResult CreateDeltaGetNode(ClientContext &context, Connection &con, Binder &binder, LogicalGet *old_get,
                                  const string &view_name);

} // namespace duckdb

#endif // OPENIVM_DELTA_HELPERS_HPP
