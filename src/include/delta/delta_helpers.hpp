#ifndef OPENIVM_DELTA_HELPERS_HPP
#define OPENIVM_DELTA_HELPERS_HPP

#include "delta/delta_plan_fragment.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

struct DeltaGetResult {
	unique_ptr<LogicalOperator> node;
	ColumnBinding mul_binding;
};

// One source's delta metadata for the view being compiled, read once per compile.
struct DeltaSourceState {
	bool watermark_loaded = false;
	string watermark_error; // set when the metadata query failed
	Value last_update;      // NULL when no watermark is recorded
	string delta_key;       // the delta's metadata table_name
	optional_ptr<TableCatalogEntry> delta_entry;
	bool has_last_snapshot = false; // DuckLake: last applied snapshot
	int64_t last_snapshot = -1;
};

// Per-compile memo keyed by source table, so join terms do not re-read metadata for every term and leaf.
using DeltaSourceCache = unordered_map<const TableCatalogEntry *, DeltaSourceState>;

// The view's last_update watermark (and delta key) for source, memoized in cache.
DeltaSourceState &LoadDeltaWatermark(Connection &con, const string &view_name, TableCatalogEntry &source,
                                     DeltaSourceCache &cache);

// Restore the SQL arguments that DuckLake plan serialization omits.
void PopulateDuckLakeChangeScanParameters(LogicalGet &get);

DeltaGetResult CreateDeltaGetNode(ClientContext &context, Connection &con, Binder &binder, LogicalGet *old_get,
                                  const string &view_name, DeltaSourceCache *cache = nullptr,
                                  bool honor_source_projection = false);

} // namespace duckdb

#endif // OPENIVM_DELTA_HELPERS_HPP
