#ifndef IVM_PLAN_ANALYSIS_HPP
#define IVM_PLAN_ANALYSIS_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

/// Result of CREATE-time plan analysis: IVM compatibility plus metadata used by classification and SQL compilation.
struct PlanAnalysis {
	bool ivm_compatible = true;
	bool found_aggregation = false;
	bool found_projection = false;
	bool found_having = false;
	bool found_distinct = false;
	bool found_minmax = false;
	bool found_list = false;
	bool found_filtered_list = false; // LIST(...) FILTER needs group-recompute with original SQL
	bool found_left_join = false;
	bool found_full_outer = false;
	bool found_semi_anti_join = false;
	bool found_join = false;
	bool found_delim_join = false;
	bool found_single_join = false;
	bool found_window = false;
	bool found_top_k = false;
	bool found_count_distinct = false;   // COUNT(DISTINCT x) — handled via group-recompute
	bool found_grouping_sets = false;    // ROLLUP/CUBE/GROUPING SETS — handled via RECOMPUTE
	bool found_nested_aggregate = false; // outer aggregate over inner aggregate (CTE re-agg); COUNT(*) in outer is
	                                     // non-linear over source deltas → group-recompute
	bool found_union_before_aggregate = false;         // UNION ancestor over aggregate output → group-recompute
	bool found_unsupported_set_operation = false;      // INTERSECT/EXCEPT are full-refresh only
	bool needs_original_sql_for_lpts = false;          // plan contains constructs LPTS cannot round-trip safely
	bool outer_join_aggregate_needs_recompute = false; // computed outer-join aggregate args need group-recompute
	idx_t top_k_limit = 0;                             // constant LIMIT value from LOGICAL_TOP_N
	idx_t top_k_offset = 0;                            // OFFSET value (0 if none)
	vector<string> top_k_order_columns;                // ORDER BY column names (in order) for the top-k
	vector<bool> top_k_order_desc;                     // parallel to top_k_order_columns; true = DESC
	vector<string> top_k_partition_columns; // PARTITION BY cols for per-partition top-k via ROW_NUMBER ≤ k pattern;
	                                        // empty for global top-k
	vector<string> aggregate_columns;
	vector<string> aggregate_types;          // per-column: "min", "max", "sum", "count_star", "count", "avg", "list"
	vector<string> window_partition_columns; // PARTITION BY source columns from window functions
	vector<idx_t> window_partition_column_indexes; // Output-position hint for each PARTITION BY column
	size_t group_count = 0;                        // number of GROUP BY expressions
	idx_t group_index = DConstants::INVALID_INDEX; // aggregate's group_index for binding lookup
};

} // namespace duckdb

#endif // IVM_PLAN_ANALYSIS_HPP
