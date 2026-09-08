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
	bool found_semi_anti_join = false;
	bool found_join = false;
	bool found_delim_join = false;
	bool found_single_join = false;
	bool found_asof_join = false;
	bool found_positional_join = false;
	bool found_sample = false;
	bool found_window = false;
	bool found_count_distinct = false;   // COUNT(DISTINCT x) — handled via group-recompute
	bool found_grouping_sets = false;    // ROLLUP/CUBE/GROUPING SETS — handled via RECOMPUTE
	bool found_nested_aggregate = false; // outer aggregate over inner aggregate (CTE re-agg); COUNT(*) in outer is
	                                     // non-linear over source deltas → group-recompute
	bool found_volatile_expression = false;
	bool found_non_foldable_unnest = false;
	bool found_unsupported_aggregate = false;
	bool found_unsupported_filtered_aggregate = false;
	bool found_unsupported_join_type = false;
	bool found_unsupported_order_by = false;
	bool found_unsupported_operator = false;
	vector<string> aggregate_columns;
	vector<string> aggregate_types;          // per-column: "min", "max", "sum", "count_star", "count", "avg", "list"
	vector<string> window_partition_columns; // PARTITION BY source columns from window functions
	vector<string> window_order_columns;           // Common simple ORDER BY columns for all window expressions
	bool window_row_key_compatible = true;         // False when window expressions use different/non-column keys
	size_t group_count = 0;                        // number of GROUP BY expressions
	idx_t group_index = DConstants::INVALID_INDEX; // aggregate's group_index for binding lookup
};

/// Add one operator's compatibility and classification metadata to the plan analysis.
/// Tree traversal is owned by BuildCreateMVPlanFacts so analysis and lineage are collected together.
void AnalyzePlanOperator(LogicalOperator &op, PlanAnalysis &analysis);

/// Merge an already-analyzed materialized CTE body after its outer consumer has been analyzed.
/// The body supplies the query shape only for a pass-through consumer; otherwise only safety facts propagate.
void MergeMaterializedCteBodyAnalysis(PlanAnalysis &analysis, PlanAnalysis body_analysis);

} // namespace duckdb

#endif // INCREMENTAL_CHECKER_HPP
