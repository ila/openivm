#ifndef OPENIVM_CONSTANTS_HPP
#define OPENIVM_CONSTANTS_HPP

#include "duckdb.hpp"

namespace duckdb {
namespace openivm {

// System table names
constexpr const char *VIEWS_TABLE = "openivm_views";
constexpr const char *DELTA_TABLES_TABLE = "openivm_delta_tables";
constexpr const char *HISTORY_TABLE = "openivm_refresh_history";
constexpr const char *PROFILE_TABLE = "openivm_refresh_profile";

// View-matching system tables.
constexpr const char *MV_DEPS_TABLE = "openivm_mv_dependencies";
constexpr const char *CONSTRAINTS_CACHE_TABLE = "openivm_constraints_cache";
constexpr const char *MATCH_LOG_TABLE = "openivm_match_log";

// IVM metadata column names
constexpr const char *MULTIPLICITY_COL = "openivm_multiplicity";
constexpr const char *TIMESTAMP_COL = "openivm_timestamp";

// Prefixes
constexpr const char *DELTA_PREFIX = "openivm_delta_";
constexpr const char *DATA_TABLE_PREFIX = "openivm_data_";

// Internal column names (added by IVM plan rewrites, hidden from users via VIEW)
constexpr const char *LEFT_KEY_COL = "openivm_left_key";
constexpr const char *RIGHT_KEY_COL = "openivm_right_key";
constexpr const char *DISTINCT_COUNT_COL = "openivm_distinct_count";

// Internal column prefixes (for AVG and STDDEV/VARIANCE decomposition)
constexpr const char *SUM_COL_PREFIX = "openivm_sum_";
constexpr const char *SUM_SQ_COL_PREFIX = "openivm_sum_sq_";   // STDDEV: apply sqrt in upsert
constexpr const char *VAR_SQ_COL_PREFIX = "openivm_var_sq_";   // VARIANCE: no sqrt in upsert
constexpr const char *SUM_SQP_COL_PREFIX = "openivm_sum_sqp_"; // STDDEV_POP: sqrt + population denominator
constexpr const char *VAR_SQP_COL_PREFIX = "openivm_var_sqp_"; // VAR_POP: no sqrt + population denominator
constexpr const char *COUNT_COL_PREFIX = "openivm_count_";

// Hidden COUNT(*) injected into AGGREGATE_GROUP MVs that don't already have a
// count aggregate. Tracks per-group cardinality so the cleanup can delete rows
// whose group reaches 0 remaining tuples (without confusing legitimate SUM=0).
constexpr const char *COUNT_STAR_COL = "openivm_count_star";

// Match count columns for outer join incremental MERGE (Larson & Zhou / Zhang & Larson)
constexpr const char *MATCH_COUNT_COL = "openivm_match_count";
constexpr const char *RIGHT_MATCH_COUNT_COL = "openivm_right_match_count";

// Index suffix for GROUP BY unique index on MV data tables
constexpr const char *INDEX_SUFFIX = "openivm_index";

// Temporary table prefix for companion row snapshots
constexpr const char *TEMP_TABLE_PREFIX = "openivm_old_";

// Limits
static constexpr idx_t MAX_JOIN_TABLES = 16;

// Optimizer settings disabled during IVM rewrite (these interfere with the delta plan)
constexpr const char *DISABLED_OPTIMIZERS = "compressed_materialization, column_lifetime, statistics_propagation";

} // namespace openivm

enum class RefreshType : uint8_t {
	AGGREGATE_GROUP,
	SIMPLE_AGGREGATE,
	SIMPLE_PROJECTION,
	FULL_REFRESH,
	AGGREGATE_HAVING,
	WINDOW_PARTITION, // window functions — partition-level recompute
	GROUP_RECOMPUTE, // inner-DISTINCT-under-AGG fallback: DELETE+INSERT only the GROUP BY keys touched by source deltas
	TOP_K,           // Legacy enum value; current top-k support strips ORDER BY/LIMIT into the user-facing view
	DISTINCT_INCREMENTAL, // inner-DISTINCT-under-AGG with aux state (openivm_distinct_aux_state=true): DBSP-correct
	                      // distinct(R)=sgn(R[t]); per-tuple count table emits ±1 only on count transitions
	SEMI_ANTI_RECOMPUTE   // SEMI/ANTI join aux state: per-left-tuple match counts, transition-scoped MV updates
};

enum class GroupRecomputeAffectedMode : uint8_t { SOURCE_DELTA, SOURCE_DELTA_RELAX_AGGREGATE_FILTER, CURRENT_DIFF };

inline const char *RefreshTypeName(RefreshType type) {
	switch (type) {
	case RefreshType::AGGREGATE_HAVING:
		return "AGGREGATE_HAVING";
	case RefreshType::AGGREGATE_GROUP:
		return "AGGREGATE_GROUP";
	case RefreshType::SIMPLE_AGGREGATE:
		return "SIMPLE_AGGREGATE";
	case RefreshType::SIMPLE_PROJECTION:
		return "SIMPLE_PROJECTION";
	case RefreshType::WINDOW_PARTITION:
		return "WINDOW_PARTITION";
	case RefreshType::GROUP_RECOMPUTE:
		return "GROUP_RECOMPUTE";
	case RefreshType::DISTINCT_INCREMENTAL:
		return "DISTINCT_INCREMENTAL";
	case RefreshType::SEMI_ANTI_RECOMPUTE:
		return "SEMI_ANTI_RECOMPUTE";
	case RefreshType::TOP_K:
		return "TOP_K";
	case RefreshType::FULL_REFRESH:
		return "FULL_REFRESH";
	default:
		return "UNKNOWN";
	}
}

inline const char *GroupRecomputeAffectedModeName(GroupRecomputeAffectedMode mode) {
	switch (mode) {
	case GroupRecomputeAffectedMode::SOURCE_DELTA:
		return "source_delta";
	case GroupRecomputeAffectedMode::SOURCE_DELTA_RELAX_AGGREGATE_FILTER:
		return "source_delta_relax_aggregate_filter";
	case GroupRecomputeAffectedMode::CURRENT_DIFF:
		return "current_diff";
	default:
		return "current_diff";
	}
}

} // namespace duckdb

#endif // OPENIVM_CONSTANTS_HPP
