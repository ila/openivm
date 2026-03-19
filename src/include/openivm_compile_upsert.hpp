#ifndef OPENIVM_COMPILE_UPSERT_HPP
#define OPENIVM_COMPILE_UPSERT_HPP

#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

string CompileAggregateGroups(string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql = "",
                              bool has_minmax = false);
string CompileSimpleAggregates(string &view_name, const vector<string> &column_names,
                               const string &view_query_sql = "", bool has_minmax = false);
string CompileProjectionsFilters(string &view_name, const vector<string> &column_names);

} // namespace duckdb

#endif // OPENIVM_COMPILE_UPSERT_HPP
