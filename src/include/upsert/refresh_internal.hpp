#ifndef OPENIVM_REFRESH_INTERNAL_HPP
#define OPENIVM_REFRESH_INTERNAL_HPP

#include "core/refresh_metadata.hpp"
#include "duckdb.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "upsert/refresh_compiler.hpp"

#include <chrono>

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
	vector<string> active_delta_table_names;
};

struct DuckLakeSnapshotAdvance {
	DuckLakeSnapshotAdvance() {
	}

	DuckLakeSnapshotAdvance(string table_name_p, int64_t snapshot_id_p)
	    : table_name(std::move(table_name_p)), snapshot_id(snapshot_id_p) {
	}

	string table_name;
	int64_t snapshot_id = -1;
};

struct DeltaActivityResult {
	bool has_join = false;
	idx_t tables_with_changes = 0;
	bool any_has_deletes = false;
	bool all_ducklake = true;
	bool requires_full_refresh = false;
	vector<string> active_delta_table_names;
	vector<DuckLakeSnapshotAdvance> ducklake_snapshot_advances;
	struct Source {
		string delta_table_name;
		string source_table_name;
		string catalog_name;
		string schema_name;
		bool ducklake = false;
		bool ok = false;
		bool has_changes = false;
		bool has_deletes = false;
		bool requires_full_refresh = false;
		int64_t pending_rows = -1;
		int64_t pending_deletes = -1;
		int64_t last_snapshot_id = -1;
		int64_t current_snapshot_id = -1;
	};
	vector<Source> sources;
};

struct DuckLakeTableActivity {
	bool ok = false;
	bool has_changes = false;
	bool has_deletes = false;
	bool requires_full_refresh = false;
};

class DeltaActivityProvider {
public:
	static DeltaActivityProvider Build(RefreshMetadata &metadata, Connection &con, const string &view_name,
	                                   const string &view_query_sql, const vector<string> &delta_table_names,
	                                   const string &view_catalog_name, const string &view_schema_name,
	                                   const string &attached_db_catalog_name, const string &attached_db_schema_name);

	const DeltaActivityResult &Summary() const {
		return summary;
	}

private:
	DeltaActivityResult summary;
};

struct RefreshCostEstimate;

struct RefreshCompileProfileStep {
	string step_name;
	int64_t duration_ms;
	string detail;
};

struct RefreshCompileProfile {
	vector<RefreshCompileProfileStep> steps;

	void AddStep(const string &step_name, std::chrono::steady_clock::time_point start,
	             const string &detail = string()) {
		auto duration_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
		steps.push_back({step_name, duration_ms, detail});
	}
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
bool IsSummableLogicalType(const LogicalType &type);
string NormalizeColumnNameForMatch(const string &name);
string BaseTableNameFromDeltaKey(const string &delta_key);
string BuildStandardDeltaRowsSQL(const string &delta_table_sql, const string &last_update,
                                 const string &extra_predicate = "");

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
                                             const string &attached_db_schema_name, bool cross_system,
                                             const DeltaActivityResult *precomputed_delta_activity = nullptr);
DeltaActivityResult BuildDeltaActivityResult(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                             const string &view_query_sql, const vector<string> &delta_table_names,
                                             const string &view_catalog_name, const string &view_schema_name,
                                             const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name);
DuckLakeTableActivity ProbeDuckLakeSnapshotActivity(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                                    const string &table_name, const string &catalog_name,
                                                    const string &schema_name, int64_t last_snapshot_id,
                                                    int64_t current_snapshot_id);

string BuildWindowPartitionRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const string &view_query_sql, const vector<string> &delta_table_names,
                                   const vector<string> &column_names, const string &data_table,
                                   const string &delta_ts_filter, const string &internal_catalog_prefix,
                                   const string &view_catalog_name, const string &view_schema_name,
                                   const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                   bool cross_system);
bool TryBuildGroupMeasureUpdateRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                       const string &view_query_sql, const vector<string> &active_delta_table_names,
                                       const vector<string> &column_names, const vector<LogicalType> &column_types,
                                       const string &data_table, const string &view_catalog_name,
                                       const string &view_schema_name, string &upsert_query);

string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                          const string &view_name, bool cross_system, const string &attached_db_catalog_name,
                          const string &attached_db_schema_name, string *out_pre_meta = nullptr,
                          string *out_post_meta = nullptr, RefreshCompileProfile *compile_profile = nullptr,
                          const DeltaActivityResult *precomputed_delta_activity = nullptr,
                          RefreshCostEstimate *out_adaptive_estimate = nullptr);

} // namespace duckdb

#endif // OPENIVM_REFRESH_INTERNAL_HPP
