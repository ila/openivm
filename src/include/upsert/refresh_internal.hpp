#ifndef OPENIVM_REFRESH_INTERNAL_HPP
#define OPENIVM_REFRESH_INTERNAL_HPP

#include "core/refresh_metadata.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "upsert/refresh_compiler.hpp"

namespace duckdb {

constexpr const char *DUCKLAKE_SNAPSHOT_PLACEHOLDER = "__OPENIVM_DUCKLAKE_SNAPSHOT_ID__";

struct ViewLocation {
	string catalog_name;
	string schema_name;
	bool cross_system;
};

struct DuckLakeSourceLocation {
	string catalog_name;
	string schema_name;
	string table_name;
};

struct DeltaFastPathFlags {
	bool insert_only = false;
	bool skip_agg_delete = false;
	bool skip_proj_delete = false;
	bool minmax_incremental = false;
};

string BuildDeltaTimestampFilter(Connection &con, const string &view_name, bool has_ts_col);
bool IsEmptyDeltaPlan(LogicalOperator *op);
string BuildEmptyDeltaInsert(const string &view_name, const vector<string> &column_names,
                             const vector<LogicalType> &column_types);
string BuildCompactDeltaViewSQL(const string &view_name, const string &delta_view_name,
                                const vector<string> &column_names, const string &delta_ts_filter);
string BuildDeleteInsertRefreshSQL(const string &data_table, const string &view_query_sql,
                                   const string &recompute_alias, const string &delete_where,
                                   const string &insert_where, const string &statement_prefix = "");
string BuildAffectedKeyRefreshSQL(const string &data_table, const string &view_query_sql,
                                  const string &affected_subquery, const string &target_alias,
                                  const string &recompute_alias, const string &affected_alias,
                                  const string &target_match, const string &recompute_match,
                                  const string &affected_temp_table = "");
string BuildSignedMultisetDeltaInsertSQL(const string &delta_table, const string &old_source, const string &new_source,
                                         const string &statement_prefix = "");

string ResolveDuckLakeCatalogName(Connection &con, const string &view_catalog_name,
                                  const string &attached_db_catalog_name);
string BuildRecomputeQuery(RefreshMetadata &metadata, const string &view_name, const string &view_query_sql,
                           bool cross_system, const string &attached_catalog = "", const string &attached_schema = "",
                           const string &catalog_prefix = "", string *out_post_meta = nullptr);

string BuildFullOuterAffectedGroupRefresh(RefreshMetadata &metadata, const string &view_name,
                                          const vector<string> &delta_table_names, const vector<string> &group_cols,
                                          const string &data_table, const string &view_query_sql,
                                          const string &delta_ts_filter, const string &catalog_prefix,
                                          const string &recompute_alias);
string CompileProjectionRefresh(RefreshMetadata &metadata, const string &view_name, const vector<string> &column_names,
                                const vector<string> &delta_table_names, const string &data_table,
                                const string &view_query_sql, const string &delta_ts_filter,
                                const string &catalog_prefix, bool has_full_outer, bool has_left_join,
                                bool skip_proj_delete);
bool TryBuildDuckLakeProjectionKeyRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                          const vector<string> &delta_table_names, const string &data_table,
                                          const string &view_query_sql, const string &view_catalog_name,
                                          const string &view_schema_name, const string &attached_db_catalog_name,
                                          const string &attached_db_schema_name, string &upsert_query);
void AppendSimpleAggregateEmptySourceNulling(RefreshMetadata &metadata, string &upsert_query, const string &view_name,
                                             const vector<string> &column_names, const string &data_table,
                                             const string &view_catalog_name, const string &view_schema_name,
                                             const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name);

ViewLocation ResolveViewLocation(Connection &con, const string &view_name, const string &fallback_catalog,
                                 const string &fallback_schema);
DuckLakeSourceLocation ResolveDuckLakeSourceLocation(Connection &con, const string &view_name, const string &table_name,
                                                     const string &fallback_catalog, const string &fallback_schema,
                                                     const string &attached_catalog, const string &attached_schema);
vector<GroupRecomputeDeltaSpec> BuildGroupRecomputeDeltaSpecs(RefreshMetadata &metadata, const string &view_name,
                                                              Connection &con, const vector<string> &delta_table_names,
                                                              const string &ducklake_catalog,
                                                              const string &ducklake_schema);
string BuildDuckLakeSnapshotQuery(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                  const string &view_query_sql, const vector<string> &delta_table_names,
                                  const string &view_catalog_name, const string &view_schema_name,
                                  const string &attached_db_catalog_name, const string &attached_db_schema_name);
string QualifyViewQuerySources(RefreshMetadata &metadata, Connection &con, const string &view_name,
                               const string &view_query_sql, const vector<string> &delta_table_names,
                               const string &view_catalog_name, const string &view_schema_name,
                               const string &attached_db_catalog_name, const string &attached_db_schema_name);
string DuckLakeSnapshotPlaceholder(const string &catalog_name);

DeltaFastPathFlags ResolveDeltaFastPathFlags(ClientContext &context, RefreshMetadata &metadata, Connection &con,
                                             const string &view_name, const string &view_query_sql,
                                             const vector<string> &delta_table_names, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name, bool cross_system);

string BuildWindowPartitionRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const string &view_query_sql, const vector<string> &delta_table_names,
                                   const vector<string> &column_names, const string &data_table,
                                   const string &delta_ts_filter, const string &internal_catalog_prefix,
                                   const string &view_catalog_name, const string &view_schema_name,
                                   const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                   bool cross_system, bool emit_cascade_delta = false);

string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                          const string &view_name, bool cross_system, const string &attached_db_catalog_name,
                          const string &attached_db_schema_name, string *out_pre_meta = nullptr,
                          string *out_post_meta = nullptr);

} // namespace duckdb

#endif // OPENIVM_REFRESH_INTERNAL_HPP
