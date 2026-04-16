#ifndef IVM_CHECKER_HPP
#define IVM_CHECKER_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

/// Result of a single-pass plan analysis: IVM compatibility check + metadata extraction.
struct PlanAnalysis {
	bool ivm_compatible = true;
	bool found_aggregation = false;
	bool found_projection = false;
	bool found_having = false;
	bool found_distinct = false;
	bool found_minmax = false;
	bool found_left_join = false;
	bool found_join = false;
	bool found_window = false;
	vector<string> aggregate_columns;
	vector<string> aggregate_types;          // per-column: "min", "max", "sum", "count_star", "count", "avg", "list"
	vector<string> window_partition_columns; // PARTITION BY columns from window functions
};

/// Walk the logical plan tree once, validating IVM compatibility AND extracting
/// metadata (aggregation type, join type, group-by columns, etc.).
/// Replaces the separate ValidateIVMPlan + parser stack walk.
PlanAnalysis AnalyzePlan(LogicalOperator *plan);

/// Thin wrapper for backward compatibility. Returns true if the plan is fully IVM-compatible.
bool ValidateIVMPlan(LogicalOperator *plan);

} // namespace duckdb

#endif // IVM_CHECKER_HPP
