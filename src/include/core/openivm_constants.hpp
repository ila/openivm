#ifndef OPENIVM_CONSTANTS_HPP
#define OPENIVM_CONSTANTS_HPP

#include "duckdb.hpp"

namespace duckdb {
namespace ivm {

// System table names
constexpr const char *VIEWS_TABLE = "_duckdb_ivm_views";
constexpr const char *DELTA_TABLES_TABLE = "_duckdb_ivm_delta_tables";
constexpr const char *HISTORY_TABLE = "_duckdb_ivm_refresh_history";

// IVM metadata column names
constexpr const char *MULTIPLICITY_COL = "_duckdb_ivm_multiplicity";
constexpr const char *TIMESTAMP_COL = "_duckdb_ivm_timestamp";

// Prefixes
constexpr const char *DELTA_PREFIX = "delta_";
constexpr const char *DATA_TABLE_PREFIX = "_ivm_data_";

// Internal column names (added by IVM plan rewrites, hidden from users via VIEW)
constexpr const char *LEFT_KEY_COL = "_ivm_left_key";
constexpr const char *DISTINCT_COUNT_COL = "_ivm_distinct_count";

// Internal column prefixes (for AVG and STDDEV/VARIANCE decomposition)
constexpr const char *SUM_COL_PREFIX = "_ivm_sum_";
constexpr const char *SUM_SQ_COL_PREFIX = "_ivm_sum_sq_";   // STDDEV: apply sqrt in upsert
constexpr const char *VAR_SQ_COL_PREFIX = "_ivm_var_sq_";   // VARIANCE: no sqrt in upsert
constexpr const char *SUM_SQP_COL_PREFIX = "_ivm_sum_sqp_"; // STDDEV_POP: sqrt + population denominator
constexpr const char *VAR_SQP_COL_PREFIX = "_ivm_var_sqp_"; // VAR_POP: no sqrt + population denominator
constexpr const char *COUNT_COL_PREFIX = "_ivm_count_";

// Index suffix for GROUP BY unique index on MV data tables
constexpr const char *INDEX_SUFFIX = "_ivm_index";

// Temporary table prefix for companion row snapshots
constexpr const char *TEMP_TABLE_PREFIX = "_ivm_old_";

// Limits
static constexpr idx_t MAX_JOIN_TABLES = 16;

// Optimizer settings disabled during IVM rewrite (these interfere with the delta plan)
constexpr const char *DISABLED_OPTIMIZERS =
    "compressed_materialization, column_lifetime, statistics_propagation, expression_rewriter, filter_pushdown";

} // namespace ivm

enum class IVMType : uint8_t { AGGREGATE_GROUP, SIMPLE_AGGREGATE, SIMPLE_PROJECTION, FULL_REFRESH, AGGREGATE_HAVING };

} // namespace duckdb

#endif // OPENIVM_CONSTANTS_HPP
