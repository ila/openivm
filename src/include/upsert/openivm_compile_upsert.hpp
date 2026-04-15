#ifndef OPENIVM_COMPILE_UPSERT_HPP
#define OPENIVM_COMPILE_UPSERT_HPP

#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

string CompileAggregateGroups(const string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql = "", bool has_minmax = false,
                              bool list_mode = false, const string &delta_ts_filter = "",
                              const vector<string> &group_column_names = {}, const string &catalog_prefix = "",
                              bool insert_only = false, const vector<string> &aggregate_types = {});
string CompileSimpleAggregates(const string &view_name, const vector<string> &column_names,
                               const string &view_query_sql = "", bool has_minmax = false, bool list_mode = false,
                               const string &delta_ts_filter = "", const string &catalog_prefix = "",
                               bool insert_only = false);
string CompileProjectionsFilters(const string &view_name, const vector<string> &column_names,
                                 const string &delta_ts_filter = "", const string &catalog_prefix = "",
                                 bool insert_only = false);
string CompileWindowRecompute(const string &view_name, const string &view_query_sql, const string &delta_ts_filter = "",
                              const string &catalog_prefix = "", const vector<string> &partition_columns = {},
                              const vector<string> &delta_table_names = {});

} // namespace duckdb

#endif // OPENIVM_COMPILE_UPSERT_HPP
