#ifndef OPENIVM_REFRESH_INTERNAL_HPP
#define OPENIVM_REFRESH_INTERNAL_HPP

#include "compile_facts.hpp"
#include "core/refresh_metadata.hpp"
#include "core/time_travel_pins.hpp"
#include "duckdb.hpp"
#include "core/mv_profiler.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "upsert/refresh_compiler.hpp"

#include <chrono>

namespace duckdb {

constexpr const char *DUCKLAKE_SNAPSHOT_PLACEHOLDER = "__OPENIVM_DUCKLAKE_SNAPSHOT_ID__";
// Precedes the synthetic timestamp of DuckLake signed delta rows, so every row in the range counts.
constexpr const char *DUCKLAKE_SYNTHETIC_LAST_UPDATE = "1970-01-01 00:00:00";

// A maintenance compiler may retain its affected rows until publication finishes.
struct RefreshPublicationScope {
	vector<string> columns;
	string rows;
	string cleanup_sql;
	string signed_rows;
};

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

// Signed (+1 insert / -1 delete) rows of the snapshots after `last_snapshot_id` through the current one.
string BuildDuckLakeSignedDeltaRelation(const DuckLakeSourceLocation &loc, int64_t last_snapshot_id,
                                        int64_t current_snapshot_id);
RefreshMetadata::DeltaChangeStats GetDuckLakeDeltaChangeStats(RefreshMetadata &metadata,
                                                              const DuckLakeSourceLocation &loc,
                                                              int64_t last_snapshot_id, int64_t current_snapshot_id);

struct DuckLakeSourceSpec {
	string metadata_key;
	DuckLakeSourceLocation loc;
	int64_t old_snap = -1;
	int64_t current_snap = -1;
};

string StripOpenIVMDataPrefix(const string &name);
const DuckLakeSourceSpec *FindDuckLakeSourceSpec(const vector<DuckLakeSourceSpec> &specs, const string &table_name);
bool BuildDuckLakeSourceSpecs(RefreshMetadata &metadata, Connection &con, const string &view_name,
                              const vector<string> &delta_table_names, const string &view_catalog_name,
                              const string &view_schema_name, const string &attached_db_catalog_name,
                              const string &attached_db_schema_name, vector<DuckLakeSourceSpec> &specs);
string BuildDuckLakeChangedValuesSQL(const DuckLakeSourceSpec &spec, const string &source_col,
                                     const string &source_cast, const string &output_col);

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

struct RefreshCostEstimate;

struct RefreshCompileProfile {
	vector<MVProfileStep> steps;

	void AddStep(const string &step_name, std::chrono::steady_clock::time_point start,
	             const string &detail = string()) {
		auto duration_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
		steps.push_back({step_name, duration_ms, detail});
	}
};

// Native DuckLake refreshes can try an exhaustive tuple delete first and retry
// with ranked rowid deletion when the changed-row count proves that the target
// contained more copies than the delta removes.
struct ProjectionDeleteRetryPlan {
	string expected_count_statement;
	string delete_statement;
	//! The complete refresh program that runs the ranked deletion instead.
	string fallback_program;

	bool IsActive() const {
		return !expected_count_statement.empty() && !delete_statement.empty();
	}
};

// Mirrors the caller's session-scoped OpenIVM planning settings onto a fresh connection.
void PropagateRefreshPlanningSettings(ClientContext &from, ClientContext &to);
// Rewrites DuckDB-only casts, now() and IS NOT DISTINCT FROM into Spark-portable SQL.
string SparkPortableRefreshSQL(string sql);
string BuildDeltaTimestampFilter(Connection &con, const string &view_name, bool has_ts_col);
bool IsEmptyDeltaPlan(LogicalOperator *op);
string BuildEmptyDeltaInsert(const string &insert_target, const vector<string> &column_names,
                             const vector<LogicalType> &column_types, SqlDialect dialect = SqlDialect::DUCKDB);
string BuildCompactDeltaViewSQL(const string &view_name, const string &delta_view_name,
                                const vector<string> &column_names, const string &delta_ts_filter);
// When `upsert_keys` and `recompute_temp_table` are supplied, emits an INSERT OR REPLACE form that
// never deletes and re-inserts the same key inside one transaction. Required when the data table
// carries a UNIQUE index: DuckDB's on-disk unique index keeps deleted keys for constraint checking
// within the same transaction, so DELETE-then-INSERT of a surviving group raises a spurious
// "Duplicate key ... violates unique constraint". Only valid when such an index exists (INSERT OR
// REPLACE requires a UNIQUE/PK constraint); pass them empty to keep the plain DELETE+INSERT form.
string BuildAffectedKeyRefreshSQL(const string &data_table, const string &view_query_sql,
                                  const string &affected_subquery, const string &target_alias,
                                  const string &recompute_alias, const string &affected_alias,
                                  const string &target_match, const string &recompute_match,
                                  const string &affected_temp_table = "", const vector<string> &upsert_keys = {},
                                  const string &recompute_temp_table = "",
                                  RefreshPublicationScope *publication_scope = nullptr);
string BuildSignedMultisetDeltaInsertSQL(const string &delta_table, const string &old_source, const string &new_source,
                                         const string &statement_prefix = "");
// Insert column list plus the select lists that project a row as a retraction (-1) or an insertion (+1).
struct SignedRowLists {
	string columns;
	string old_rows;
	string new_rows;
};
SignedRowLists BuildSignedRowLists(const vector<string> &columns, const string &old_alias = "",
                                   const string &new_alias = "", SqlDialect dialect = SqlDialect::DUCKDB);
// Clears the current refresh's rows from a view delta before it is rewritten.
string BuildDeltaResetSQL(const string &delta_table, const string &delta_ts_filter);
// `snapshot` keeps the affected groups' rows before an in-place update; `publish` then replaces the
// view delta with their exact retraction and the updated rows.
struct AffectedGroupSignedDelta {
	string snapshot;
	string publish;
};
AffectedGroupSignedDelta BuildAffectedGroupSignedDelta(const string &data_table, const string &delta_table,
                                                       const vector<string> &keys, const vector<string> &columns,
                                                       const string &delta_ts_filter, const string &scratch_name,
                                                       bool persistent_scratch);
string BuildSnapshotDeltaRefreshSQL(const string &data_table, const string &query, const string &delta_table,
                                    const string &old_table, const string &new_table, const string &filter,
                                    bool use_data_modifying_ctes = false);
bool IsSummableLogicalType(const LogicalType &type);
// Resolve a logical source to its metadata key using catalog and schema as well as its name.
string ResolveDeltaMetadataKey(RefreshMetadata &metadata, const string &view_name, const string &table_name,
                               const vector<string> &delta_table_names);
string BaseTableNameFromDeltaKey(const string &delta_key);
string BuildGroupRecomputeDeltaRowsSQL(const GroupRecomputeDeltaSpec &spec);
string BuildFullOuterUnmatchedGroups(RefreshMetadata &metadata, const string &view_name,
                                     const vector<string> &delta_table_names, const vector<string> &group_columns,
                                     const string &view_query_sql, const vector<GroupRecomputeDeltaSpec> &delta_specs);

string BuildRecomputeQuery(RefreshMetadata &metadata, const string &view_name, const string &view_query_sql,
                           bool cross_system, const string &attached_catalog = "", const string &attached_schema = "",
                           const string &catalog_prefix = "", const string &metadata_prefix = "",
                           string *out_post_meta = nullptr, vector<string> *deferred_cleanup = nullptr,
                           const vector<string> &delta_metadata_tables = {}, const string &data_catalog = "");

string BuildFullOuterAffectedGroupRefresh(RefreshMetadata &metadata, const string &view_name,
                                          const vector<string> &delta_table_names, const vector<string> &group_cols,
                                          const string &data_table, const string &view_query_sql,
                                          const string &delta_ts_filter, const string &catalog_prefix,
                                          const string &recompute_alias);
string
CompileProjectionRefresh(RefreshMetadata &metadata, const string &view_name, const vector<string> &column_names,
                         const vector<string> &delta_table_names, const string &data_table,
                         const string &view_query_sql, const string &delta_ts_filter, const string &catalog_prefix,
                         bool has_full_outer, bool has_left_join, bool skip_proj_delete, bool insert_only = false,
                         const vector<string> &active_delta_table_names = {}, bool can_use_runtime_delta_shape = false,
                         ProjectionDeleteRetryPlan *delete_retry_plan = nullptr, string *appended_rows = nullptr);
bool TryBuildDuckLakeProjectionKeyRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                          const vector<string> &delta_table_names, const string &data_table,
                                          const string &view_query_sql, const string &view_catalog_name,
                                          const string &view_schema_name, const string &attached_db_catalog_name,
                                          const string &attached_db_schema_name, string &upsert_query,
                                          RefreshPublicationScope *publication_scope = nullptr);

ViewLocation ResolveViewLocation(Connection &con, const string &view_name, const string &fallback_catalog,
                                 const string &fallback_schema);

//! Resolves the (catalog, schema, cross_system) tuple for `view_name` by
//! consulting the active ClientContext's default catalog/schema, falling
//! back to information_schema if the entry isn't present in that default.
//! Setting `throw_if_not_found = true` raises a CatalogException when no
//! view of the given short name exists in any attached catalog — used by
//! the `openivm_compile_with_facts` bind to fail fast with a useful
//! message.
struct ResolvedViewCatalog {
	string view_name;
	string view_catalog_name;
	string view_schema_name;
	bool cross_system = false;
};
ResolvedViewCatalog ResolveViewCatalogFromContext(ClientContext &context, Connection &con, const string &view_name);
DuckLakeSourceLocation ResolveDuckLakeSourceLocation(Connection &con, const string &view_name, const string &table_name,
                                                     const string &fallback_catalog, const string &fallback_schema,
                                                     const string &attached_catalog, const string &attached_schema);
// Resolves each active source's delta rows; DuckLake sources read their snapshot range.
vector<GroupRecomputeDeltaSpec> BuildGroupRecomputeDeltaSpecs(RefreshMetadata &metadata, const string &view_name,
                                                              Connection &con, const vector<string> &delta_table_names,
                                                              const string &view_catalog_name,
                                                              const string &view_schema_name,
                                                              const string &attached_db_catalog_name,
                                                              const string &attached_db_schema_name);
openivm::TimeTravelPins PrepareViewQuerySources(Connection &con, const string &view_name, string &view_query_sql,
                                                const vector<RefreshMetadata::DeltaSource> &delta_sources,
                                                const string &view_catalog_name, const string &view_schema_name,
                                                const string &attached_db_catalog_name,
                                                const string &attached_db_schema_name);
string DuckLakeSnapshotPlaceholder(const string &catalog_name);

DeltaFastPathFlags ResolveDeltaFastPathFlags(ClientContext &context, RefreshMetadata &metadata, Connection &con,
                                             const string &view_name, const vector<string> &delta_table_names,
                                             const string &view_catalog_name, const string &view_schema_name,
                                             const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name,
                                             const DeltaActivityResult *precomputed_delta_activity = nullptr,
                                             const openivm::CompileFacts *facts = nullptr);
DeltaActivityResult BuildDeltaActivityResult(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                             const vector<string> &delta_table_names, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name);
DuckLakeTableActivity ProbeDuckLakeSnapshotActivity(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                                    const string &table_name, const string &catalog_name,
                                                    const string &schema_name, int64_t last_snapshot_id,
                                                    int64_t current_snapshot_id);
// Whether the DuckLake table at `loc` (id `table_id`, -1 if unknown) changed in a snapshot
// after `after_snapshot_id` up to `through_snapshot_id`. `ok` is false when unverifiable.
DuckLakeTableActivity ProbeDuckLakeTableChanges(Connection &con, const DuckLakeSourceLocation &loc, int64_t table_id,
                                                int64_t after_snapshot_id, int64_t through_snapshot_id);

string BuildWindowPartitionRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const string &view_query_sql, const vector<string> &delta_table_names,
                                   const vector<string> &column_names, const string &data_table,
                                   const string &delta_ts_filter, const string &internal_catalog_prefix,
                                   const string &view_catalog_name, const string &view_schema_name,
                                   const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                   bool cross_system, bool emit_cascade_delta = false,
                                   bool running_window_incremental = false, bool *uses_running_suffix = nullptr,
                                   RefreshPublicationScope *publication_scope = nullptr);
string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                          const string &view_name, bool cross_system, const string &attached_db_catalog_name,
                          const string &attached_db_schema_name, string *out_pre_meta = nullptr,
                          string *out_post_meta = nullptr, RefreshCompileProfile *compile_profile = nullptr,
                          const DeltaActivityResult *precomputed_delta_activity = nullptr,
                          RefreshCostEstimate *out_adaptive_estimate = nullptr,
                          const openivm::CompileFacts *facts = nullptr, Connection *metadata_connection = nullptr,
                          ProjectionDeleteRetryPlan *delete_retry_plan = nullptr, bool write_query_file = true,
                          vector<string> *deferred_cleanup = nullptr);

} // namespace duckdb

#endif // OPENIVM_REFRESH_INTERNAL_HPP
