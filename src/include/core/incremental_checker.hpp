#ifndef INCREMENTAL_CHECKER_HPP
#define INCREMENTAL_CHECKER_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

/// Result of a single-pass plan analysis: IVM compatibility check + metadata extraction.
struct PlanAnalysis {
	bool incremental_compatible = true;
	bool found_aggregation = false;
	bool found_projection = false;
	bool found_having = false;
	bool found_distinct = false;
	bool found_union_distinct = false;
	bool found_minmax = false;
	bool found_list = false;
	bool found_filtered_list = false; // LIST(...) FILTER needs group-recompute with original SQL
	bool found_left_join = false;
	bool found_full_outer = false;
	// An outer (LEFT/RIGHT/FULL) join sits beneath a UNION/EXCEPT/INTERSECT. Adding outer-join key /
	// match-count columns there would arity-mismatch the set-op branches, so such views are routed to
	// FULL_REFRESH. Collected during this walk to avoid a separate full-plan traversal at classification.
	bool found_outer_join_under_setop = false;
	bool found_semi_anti_join = false;
	bool found_join = false;
	bool found_delim_join = false;
	bool found_single_join = false;
	bool found_asof_join = false;
	bool found_positional_join = false;
	bool found_sample = false;
	bool found_window = false;
	bool found_top_k = false;
	bool found_count_distinct = false;   // COUNT(DISTINCT x) — handled via group-recompute
	bool found_grouping_sets = false;    // ROLLUP/CUBE/GROUPING SETS — handled via RECOMPUTE
	bool found_nested_aggregate = false; // outer aggregate over inner aggregate (CTE re-agg); COUNT(*) in outer is
	                                     // non-linear over source deltas → group-recompute
	// A non-linear / recompute-forcing construct lives inside a multi-reference materialized CTE
	// definition whose results feed a non-passthrough outer query. DuckDB keeps such CTEs materialized
	// (single-ref ones are inlined and seen directly), so the construct is otherwise invisible to the
	// top-level classification; this forces GROUP_RECOMPUTE for an outer aggregate. Set in
	// MergeCompatibilityAndNonLocalFacts.
	bool found_cte_nonlinear_construct = false;
	bool found_volatile_expression = false;
	bool found_non_foldable_unnest = false;
	bool found_unsupported_aggregate = false;
	bool found_unsupported_filtered_aggregate = false;
	bool found_unsupported_join_type = false;
	bool found_unsupported_order_by = false;
	bool found_unsupported_operator = false;
	idx_t top_k_limit = 0;                  // constant LIMIT value from LOGICAL_TOP_N
	idx_t top_k_offset = 0;                 // OFFSET value (0 if none)
	vector<string> top_k_order_columns;     // ORDER BY column names (in order) for the top-k
	vector<bool> top_k_order_desc;          // parallel to top_k_order_columns; true = DESC
	vector<string> top_k_partition_columns; // PARTITION BY cols for per-partition top-k via ROW_NUMBER ≤ k pattern;
	                                        // empty for global top-k
	vector<string> aggregate_columns;
	vector<string> aggregate_types;          // per-column: "min", "max", "sum", "count_star", "count", "avg", "list"
	vector<string> window_partition_columns; // PARTITION BY source columns from window functions
	vector<idx_t> window_partition_column_indexes; // Output-position hint for each PARTITION BY column
	size_t group_count = 0;                        // number of GROUP BY expressions
	idx_t group_index = DConstants::INVALID_INDEX; // aggregate's group_index for binding lookup
};

/// Walk the logical plan tree once, validating IVM compatibility AND extracting
/// metadata (aggregation type, join type, group-by columns, etc.).
/// Replaces the separate ValidateIncrementalPlan + parser stack walk.
PlanAnalysis AnalyzePlan(LogicalOperator *plan);

/// Thin wrapper for backward compatibility. Returns true if the plan is fully IVM-compatible.
bool ValidateIncrementalPlan(LogicalOperator *plan);

} // namespace duckdb

#endif // INCREMENTAL_CHECKER_HPP
