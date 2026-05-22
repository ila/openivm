#ifndef REFRESH_COMPILER_HPP
#define REFRESH_COMPILER_HPP

#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

struct GroupRecomputeDeltaSpec {
	string base_table;
	string last_update;
	bool is_ducklake = false;
	string ducklake_catalog;
	string ducklake_schema;
	int64_t last_snapshot_id = -1;
	int64_t current_snapshot_id = -1;
};

struct WindowPartitionDeltaSpec {
	string delta_table;
	string delta_table_sql;
	string output_column;
	string source_column;
};

string CompileAggregateGroups(const string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql = "", bool has_minmax = false,
                              bool list_mode = false, const string &delta_ts_filter = "",
                              const vector<string> &group_column_names = {}, const string &catalog_prefix = "",
                              bool insert_only = false, const vector<string> &aggregate_types = {},
                              const vector<LogicalType> &column_types = {});
string CompileSimpleAggregates(const string &view_name, const vector<string> &column_names,
                               const string &view_query_sql = "", bool has_minmax = false, bool list_mode = false,
                               const string &delta_ts_filter = "", const string &catalog_prefix = "",
                               bool insert_only = false, const vector<LogicalType> &column_types = {});
string CompileProjectionsFilters(const string &view_name, const vector<string> &column_names,
                                 const string &delta_ts_filter = "", const string &catalog_prefix = "",
                                 bool insert_only = false);
string CompileWindowRecompute(const string &view_name, const string &view_query_sql, const string &delta_ts_filter = "",
                              const string &catalog_prefix = "", const vector<string> &partition_columns = {},
                              const vector<WindowPartitionDeltaSpec> &partition_delta_specs = {});
string CompileFullRecompute(const string &view_name, const string &view_query_sql, const string &catalog_prefix = "");

/// Group-level partial recompute, used by `RefreshType::GROUP_RECOMPUTE` (inner-DISTINCT under
/// aggregate). For each base table T_i with a non-empty delta, builds a "view query with T_i
/// restricted to its delta" variant by substituting `<catalog><schema>.<T_i>` in `view_query_sql`
/// with the delta-filtered subquery. The resulting affected-keys set drives a DELETE + INSERT
/// scoped to only the GROUP BY tuples touched by deltas — strictly cheaper than full RECOMPUTE
/// when the affected key set is small.
///
/// `delta_table_specs` carries one entry per source table, including whether the source is a
/// standard DuckDB delta table or a DuckLake snapshot-diff source.
/// `catalog_prefix` is the SQL prefix used for the MV's data table — empty for the default
/// catalog, "<cat>.<schema>." otherwise. `lpts_table_prefix` is the *always-fully-qualified*
/// prefix that LPTS used in `view_query_sql` to reference base tables ("<cat>.<schema>." even
/// when the catalog is default), so we can substitute the exact `cat.schema.tbl` pattern.
string CompileGroupRecompute(const string &view_name, const string &view_query_sql, const vector<string> &group_columns,
                             const vector<GroupRecomputeDeltaSpec> &delta_table_specs,
                             const string &catalog_prefix = "", const string &lpts_table_prefix = "");

/// Aux-state DBSP-correct DISTINCT pipeline. v0: single-source view, single SUM aggregate.
/// Generates a multi-statement SQL batch:
///   1. Materialise Δinput per `distinct_cols` from the source delta (timestamp-filtered,
///      WHERE-filter applied) into a temp table.
///   2. MERGE Δagg into the data table — Δdistinct (LEFT JOIN aux + CASE → ±1) drives a
///      per-`group_cols` SUM(<sum_arg> * dd) update, plus matching `openivm_count_star` deltas.
///   3. DELETE rows from the data table whose `openivm_count_star` fell to ≤ 0.
///   4. MERGE Δinput into the aux table (count = count + dmult); DELETE rows with count ≤ 0.
///
/// `aux_table`, `distinct_cols`, `delta_source` (the `delta_<source>` table name), and
/// `last_update` come from `openivm_views.distinct_aux_meta_json`. `filter_sql` is the
/// WHERE predicate of the DISTINCT body (empty if none) — applied to both Δinput (filters
/// delta rows that wouldn't have entered the DISTINCT) and the aux MERGE source.
/// `group_columns` is the parent aggregate's GROUP BY; `sum_arg`/`sum_out` are the single-SUM
/// argument and output column. `count_star_col` is the auto-injected `openivm_count_star`
/// column name on the data table (almost always literal `openivm_count_star`).
string CompileDistinctIncremental(const string &view_name, const string &aux_table, const vector<string> &distinct_cols,
                                  const vector<string> &source_exprs, const string &delta_source,
                                  const string &last_update, const string &filter_sql,
                                  const vector<string> &group_columns, const string &sum_arg, const string &sum_out,
                                  const string &count_star_col, const string &catalog_prefix = "");
string BuildDistinctAuxStateCreateSQL(const string &target_table, const vector<string> &distinct_cols,
                                      const vector<string> &source_exprs, const string &source_relation,
                                      const string &filter_sql, bool replace);

string CompileSemiAntiRecompute(const string &view_name, const string &aux_table, const string &join_type,
                                const string &left_table, const string &left_alias, const string &right_table,
                                const string &right_alias, const string &predicate, const string &post_filter,
                                const vector<string> &left_cols, const vector<string> &left_exprs,
                                const vector<string> &output_cols, const string &left_delta_source,
                                const string &right_delta_source, const string &left_last_update,
                                const string &right_last_update, const string &catalog_prefix = "");
string BuildSemiAntiAuxStateCreateSQL(const string &target_table, const string &left_source, const string &left_alias,
                                      const string &right_source, const string &right_alias, const string &predicate,
                                      const string &post_filter, const vector<string> &left_cols,
                                      const vector<string> &left_exprs, bool replace);

string CompileFilteredGroupCount(const string &view_name, const string &aux_table, const string &delta_source,
                                 const string &last_update, const string &group_col, const string &sum_col,
                                 const string &source_group_expr, const string &source_sum_expr,
                                 const string &output_col, const string &comparison_op, const string &threshold_sql,
                                 const string &catalog_prefix = "");
string BuildFilteredGroupCountAuxStateCreateSQL(const string &target_table, const string &source_table,
                                                const string &group_col, const string &sum_col,
                                                const string &source_group_expr, const string &source_sum_expr,
                                                bool replace);

} // namespace duckdb

#endif // REFRESH_COMPILER_HPP
