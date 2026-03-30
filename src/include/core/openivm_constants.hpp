#ifndef OPENIVM_CONSTANTS_HPP
#define OPENIVM_CONSTANTS_HPP

namespace duckdb {
namespace ivm {

// System table names
constexpr const char *VIEWS_TABLE = "_duckdb_ivm_views";
constexpr const char *DELTA_TABLES_TABLE = "_duckdb_ivm_delta_tables";

// IVM metadata column names
constexpr const char *MULTIPLICITY_COL = "_duckdb_ivm_multiplicity";
constexpr const char *TIMESTAMP_COL = "_duckdb_ivm_timestamp";

// Prefixes
constexpr const char *DELTA_PREFIX = "delta_";

// Limits
static constexpr idx_t MAX_JOIN_TABLES = 16;

// Optimizer settings disabled during IVM rewrite (these interfere with the delta plan)
constexpr const char *DISABLED_OPTIMIZERS =
    "compressed_materialization, column_lifetime, statistics_propagation, expression_rewriter, filter_pushdown";

} // namespace ivm

enum class IVMType : uint8_t { AGGREGATE_GROUP, SIMPLE_AGGREGATE, SIMPLE_PROJECTION, FULL_REFRESH, AGGREGATE_HAVING };

} // namespace duckdb

#endif // OPENIVM_CONSTANTS_HPP
