#ifndef REFRESH_METADATA_HPP
#define REFRESH_METADATA_HPP

#include "core/derived_aggregate_output.hpp"
#include "core/metadata_location.hpp"
#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "core/openivm_constants.hpp"

namespace duckdb {

class TableCatalogEntry;
class TransactionalMVMetadataState;

// Centralized access to IVM metadata stored in system tables.
// Wraps all raw SQL queries to openivm_views and openivm_delta_tables
// behind typed methods. Takes a Connection reference — does NOT create its own.
class RefreshMetadata {
	Connection &con;
	Value ReadViewValue(const string &view_name, const string &column);
	string ReadViewString(const string &view_name, const string &column);
	vector<string> ReadViewList(const string &view_name, const string &column);

public:
	explicit RefreshMetadata(Connection &con) : con(con) {
	}

	// Native view metadata belongs to the view catalog; external catalogs use the native default.
	// Within that catalog, metadata stays in the schema that already holds it. New metadata uses
	// openivm_metadata_schema, or main when it is unset. Explicit transactions retain their
	// recorded or caller-visible registry even if the setting changes before commit/rollback.
	static MetadataLocation ResolveLocation(ClientContext &context, Connection &con, const string &view_catalog = "");
	static MetadataLocation UseCatalog(ClientContext &context, Connection &con, const string &view_catalog = "");
	// Select the location already resolved for a helper context; resolve it for any other context.
	static void UseSameLocation(ClientContext &context, Connection &con);
	// Select an already resolved catalog/schema identity without rediscovering it.
	static void UseLocation(Connection &con, const MetadataLocation &location);
	// The metadata location selected for a helper connection.
	static MetadataLocation CurrentLocation(Connection &con);
	static string MetadataTable(Connection &con, const string &table);
	// The explicitly configured metadata schema; empty means keep existing metadata where it is.
	static string ConfiguredMetadataSchema(ClientContext &context);
	// Schemas of a native catalog that contain `table`, in name order.
	static vector<string> FindMetadataSchemas(Connection &con, const string &catalog, const string &table);
	static string ResolveMetadataSchema(Connection &con, const string &catalog, const string &preferred);
	// In autocommit mode, move a catalog's metadata into the configured schema and, for a new
	// materialized view, create that schema. Other metadata objects keep their placement.
	static void PrepareMetadataSchema(ClientContext &context, const string &view_catalog, bool create_schema);
	// Every native metadata location containing `table`, and the qualified table names.
	static vector<MetadataLocation> MetadataLocations(Connection &con, const string &table);
	// Caller-snapshot discovery includes uncommitted schemas and registries.
	static vector<MetadataLocation> MetadataLocations(ClientContext &context, const string &table);
	static vector<string> MetadataTables(Connection &con, const string &table);

	void SnapshotTransaction(ClientContext &context);
	string ResolveViewName(const string &view_name, const string &catalog = "", const string &schema = "");
	string FindViewKey(const string &catalog, const string &schema, const string &name);
	string GetViewSQLName(const string &view_key);
	string AllocateViewKey(const string &catalog, const string &schema, const string &name);

	// Returns true if the given table name is NOT a tracked materialized view.
	// (i.e., it's a base table that should have its deltas captured)
	bool IsBaseTable(const string &table_name);

	// Get the stored SQL query for a materialized view.
	// Returns empty string if not found.
	string GetViewQuery(const string &view_name);

	// Get the IVM type for a materialized view.
	RefreshType GetViewType(const string &view_name);

	// Check if the view uses MIN/MAX aggregates (requires group-recompute).
	bool HasMinMax(const string &view_name);

	// Check if the view involves a LEFT/RIGHT JOIN.
	bool HasLeftJoin(const string &view_name);

	// Check if the view involves any join operator.
	bool HasJoin(const string &view_name);

	// Check if the view involves a FULL OUTER JOIN.
	bool HasFullOuter(const string &view_name);

	// Get the join condition column names for FULL OUTER JOIN ("left_col,right_col").
	string GetFullOuterJoinCols(const string &view_name);

	// Get delta table names associated with a view.
	vector<string> GetDeltaTables(const string &view_name);

	// Get the last_update timestamp for a specific delta table entry.
	string GetLastUpdate(const string &view_name, const string &table_name);

	// Every metadata location that may consume delta tables (see MetadataLocator::All).
	static vector<MetadataLocation> MetadataLocations(Connection &con);
	static vector<string> MetadataCatalogs(Connection &con);
	// Accept both legacy short keys and qualified keys for sources with colliding names.
	static string SourceTableName(const string &key, const string &catalog, const string &schema);
	static string SourcePredicate(const string &table, const string &catalog, const string &schema);

	// A source's delta table lives beside the source unless openivm_internal_schema placed it in
	// an internal schema of the same catalog; there its name encodes the source schema.
	struct SourceLocation {
		string catalog_name;
		string schema_name;
		string table_name;
		string delta_schema_name;
		string delta_table_name;
	};
	struct StoredViewLocation {
		string catalog_name;
		string schema_name;
	};
	struct DeltaSource {
		string table_name;
		string catalog_type;
		string catalog_name;
		string schema_name;
		string delta_schema_name;
		string delta_table_name;
	};
	// Call under the mutation gate. Replay every location for caller-transaction cleanup;
	// after commit, omit transaction_state and inspect durable ownership. Exclusions are
	// keyed by the fully qualified metadata table because view keys are location-local.
	static bool HasSourceDeltaConsumers(Connection &con, const DeltaSource &source,
	                                    optional_ptr<TransactionalMVMetadataState> transaction_state = nullptr,
	                                    const unordered_map<string, string> &excluded_consumers = {});

	// Physical source-delta naming. Internal-schema deltas encode the source schema so that
	// same-named sources of different schemas have different preferred names. CREATE allocates
	// a free name if a legacy colocated delta occupies that name and persists the result.
	static string InternalSourceDeltaName(const string &source_schema, const string &table);
	static string SourceDeltaTableName(const string &source_schema, const string &table, const string &delta_schema);
	// Resolve a source delta through its local catalog mapping, without consulting control metadata.
	// Requires an active transaction on `context`.
	static optional_ptr<TableCatalogEntry> FindSourceDelta(ClientContext &context, const string &catalog,
	                                                       const string &schema, const string &table);
	// CREATE additionally validates control metadata ownership, including legacy physical names.
	static optional_ptr<TableCatalogEntry> FindRegisteredSourceDelta(ClientContext &context, const string &catalog,
	                                                                 const string &schema, const string &table);
	static string SourceGenerationDDL(const string &catalog, const string &schema, const string &table);
	static string SourceGeneration(ClientContext &context, const string &catalog, const string &schema,
	                               const string &table);
	static void InvalidateSourceGeneration(ClientContext &context, const string &catalog, const string &schema,
	                                       const string &table, bool drop_schema = false);
	static string SourceDeltaMappingDDL(const string &catalog, const string &schema, const string &table,
	                                    const string &delta_schema, const string &delta_table);
	static void RemoveSourceDeltaMapping(ClientContext &context, const string &catalog, const string &schema,
	                                     const string &name, bool drop_schema);
	// Allocation and native DML admission must also see ownership committed after
	// the caller's snapshot began; an invisible/replaced delta requires a retry.
	// Allocation/ALTER and the final DML lookup require the database mutation gate.
	// The initial DML probe instead holds source admission, which excludes first
	// publication; tracked writers must recheck after acquiring the mutation gate.
	static optional_ptr<TableCatalogEntry> FindSourceDeltaForMutation(ClientContext &context, const string &catalog,
	                                                                  const string &schema, const string &table);

	SourceLocation GetSourceLocation(const string &view_name, const string &table_name,
	                                 const string &fallback_catalog = "", const string &fallback_schema = "");
	StoredViewLocation GetStoredViewLocation(const string &view_name, const string &fallback_catalog = "",
	                                         const string &fallback_schema = "");
	// Schema of a view's backing, publication, auxiliary-state and MV delta tables when
	// openivm_internal_schema placed them apart from the view; empty means the view's schema.
	string GetInternalSchema(const string &view_name);
	StoredViewLocation GetInternalLocation(const string &view_name, const string &fallback_catalog = "",
	                                       const string &fallback_schema = "");
	// MV companions remain owned without consumers, across metadata locations and replay.
	bool IsMaterializedViewDelta(const DeltaSource &source,
	                             optional_ptr<TransactionalMVMetadataState> transaction_state = nullptr);
	vector<DeltaSource> GetDeltaSources(const string &view_name, const string &fallback_catalog = "",
	                                    const string &fallback_schema = "");
	string ResolveDeltaQualifiedName(const string &view_name, const string &delta_table_name,
	                                 const string &fallback_catalog = "", const string &fallback_schema = "");

	struct DeltaChangeStats {
		bool ok = false;
		int64_t total = 0;
		int64_t deletes = 0;
	};

	DeltaChangeStats GetStandardDeltaChangeStats(const string &delta_table_sql, const string &last_update);

	vector<string> GetTableColumns(const string &catalog_name, const string &schema_name, const string &table_name);
	bool TableColumnsMatch(const string &catalog_name, const string &schema_name, const string &table_name,
	                       const vector<string> &expected);

	// Get all upstream MV dependencies in topological order (ancestors first).
	// For table→mv1→mv2→mv3, GetUpstreamViews("mv3") returns ["mv1", "mv2"].
	vector<string> GetUpstreamViews(const string &view_name);

	// Get all downstream MV dependents in topological order (closest first).
	// For table→mv1→mv2→mv3, GetDownstreamViews("mv1") returns ["mv2", "mv3"].
	vector<string> GetDownstreamViews(const string &view_name);
	vector<string> GetDownstreamViewsStrict(const string &view_name);
	bool HasDownstreamViews(const string &view_name, bool include_published = true,
	                        optional_ptr<TransactionalMVMetadataState> transaction_state = nullptr);

	// Select a multi-target pipeline and topologically order its induced dependency graph.
	vector<string> GetPipelineRefreshOrder(const vector<string> &targets, const string &cascade_mode);

	// Get refresh_interval in seconds for a view. Returns -1 if not set (manual only).
	int64_t GetRefreshInterval(const string &view_name);

	// Get all views with a non-null refresh_interval.
	// Returns the stored relation identity plus its schedule and last refresh watermark.
	struct ScheduledView {
		string metadata_catalog;
		string metadata_schema;
		string view_name;
		string catalog_name;
		string schema_name;
		int64_t interval_seconds;
		string last_update;
	};
	vector<ScheduledView> GetScheduledViews();

	// Set/clear the refresh_in_progress flag for crash safety.
	void SetRefreshInProgress(const string &view_name, bool in_progress);

	// Watermarks of every consumer of this physical delta, across metadata locations.
	// Publication and cleanup must use the same qualified source identity and inventory.
	static string BuildDeltaConsumersSQL(const string &target, const string &metadata_key,
	                                     const string &delta_metadata_table = "",
	                                     const vector<string> &delta_metadata_tables = {},
	                                     const string &source_catalog = "", const string &source_schema = "");

	// Build SQL to delete old delta rows that all dependent views have already consumed.
	// Execute after advancing the refreshing consumer's watermark, or defer until commit
	// when cleanup writes a different native catalog.
	// target: the (possibly schema-qualified) table to delete from.
	// metadata_key: the name used in openivm_delta_tables (unqualified delta name).
	// delta_metadata_tables: qualified openivm_delta_tables of every native metadata location.
	// source_catalog/source_schema identify the source when the target is an internal-schema
	// delta whose physical location differs from its source.
	static string BuildDeltaCleanupSQL(const string &target, const string &metadata_key,
	                                   const string &delta_metadata_table = "",
	                                   vector<string> *deferred_cleanup = nullptr,
	                                   const vector<string> &delta_metadata_tables = {},
	                                   const string &source_catalog = "", const string &source_schema = "",
	                                   const string &data_catalog = "");

	// Get GROUP BY column names for a view. Returns empty vector if not stored.
	vector<string> GetGroupColumns(const string &view_name);

	// Get common window ORDER BY output/source mappings. Empty means the view
	// predates this metadata or its window specifications do not share a simple key.
	vector<string> GetWindowOrderColumns(const string &view_name);

	// Get per-column aggregate function types (min, max, sum, count_star, etc.).
	vector<string> GetAggregateTypes(const string &view_name);

	DerivedAggregateOutputInfo GetDerivedAggregateOutputs(const string &view_name);
	static string DerivedAggregateOutputsToJson(const DerivedAggregateOutputInfo &info);

	// Get the HAVING predicate extracted into the user-facing view. Empty means no
	// predicate was extracted, which can happen when HAVING is nested inside a CTE.
	string GetHavingPredicate(const string &view_name);

	struct GroupRecomputeSourceOccurrence {
		string table_name;
		idx_t count = 0;
	};

	GroupRecomputeAffectedMode GetGroupRecomputeAffectedMode(const string &view_name);
	vector<GroupRecomputeSourceOccurrence> GetGroupRecomputeSourceOccurrences(const string &view_name);
	static string GroupRecomputeSourceOccurrencesToJson(const vector<GroupRecomputeSourceOccurrence> &occurrences);

	// --- DuckLake support ---

	// Get the catalog type for a base table entry ('duckdb' or 'ducklake').
	string GetCatalogType(const string &view_name, const string &table_name);

	// Check if a base table entry is backed by DuckLake.
	bool IsDuckLakeTable(const string &view_name, const string &table_name);

	bool IsDuckLakeCatalog(const string &catalog_name);
	bool IsNativeCatalog(const string &catalog_name);

	int64_t GetCurrentDuckLakeSnapshot(const string &catalog_name);

	// Get the DuckLake snapshot ID at last refresh. Returns -1 if not set.
	int64_t GetLastSnapshotId(const string &view_name, const string &table_name);

	struct DuckLakeSourceIdentity {
		int64_t stored_table_id = -1;
		int64_t current_table_id = -1;
		bool resolved = false;
		bool changed = false;
	};

	// Resolve the DuckLake table id recorded for a source table and repair stale/missing metadata.
	DuckLakeSourceIdentity ResolveDuckLakeSourceIdentity(const string &view_name, const string &table_name,
	                                                     const string &catalog_name, const string &schema_name);
	// The DuckLake table id that `catalog.schema.table` named at `snapshot_id`; -1 if unknown.
	int64_t GetDuckLakeTableIdAt(const string &catalog_name, const string &schema_name, const string &table_name,
	                             int64_t snapshot_id);

	static string BuildDuckLakeRefreshMetadataSQL(const string &view_name, const string &table_name,
	                                              const string &snapshot_expr, const string &delta_metadata_table = "");
	void UpdateDuckLakeRefreshMetadata(const string &view_name, const string &table_name, int64_t snapshot_id);

	// --- Refresh history (learned cost model) ---

	// Record a refresh execution in the history table. Prunes entries beyond the window.
	void RecordRefreshHistory(const string &view_name, const string &method, double incremental_compute_est,
	                          double incremental_upsert_est, double recompute_compute_est, double recompute_replace_est,
	                          int64_t actual_duration_ms, idx_t max_history = 20);

	// Refresh history entry for regression fitting.
	struct RefreshHistoryEntry {
		double compute_est; // incremental_compute_est or recompute_compute_est (depending on method)
		double upsert_est;  // incremental_upsert_est or recompute_replace_est
		double actual_ms;
	};

	// Get the last N history entries for a given method ('incremental' or 'full').
	vector<RefreshHistoryEntry> GetRefreshHistory(const string &view_name, const string &method, idx_t limit = 20);

	// --- Aux-state DISTINCT (RefreshType::DISTINCT_INCREMENTAL) ---

	// Metadata captured at CREATE-MV time for the aux-state DISTINCT pipeline.
	// `aux_table` is the per-tuple count table (e.g. `openivm_distinct_count_<view>`);
	// `cols` are the columns being deduplicated; `input_sql` is the DISTINCT subquery
	// body with the DISTINCT keyword stripped; `source` is the base table referenced
	// by `input_sql`'s FROM clause; `filter` is the WHERE predicate (empty if none);
	// `sum_arg` and `sum_out` describe the parent aggregate's single SUM expression
	// (`SUM(<sum_arg>) AS <sum_out>`). v0 supports exactly one SUM column.
	struct DistinctAuxMeta {
		string aux_table;
		vector<string> cols;
		vector<string> source_exprs;
		string input_sql;
		string source;
		string filter;
		string sum_arg;
		string sum_out;
	};

	// Read the JSON-encoded distinct-aux metadata. Returns false if the column is NULL
	// (view isn't DISTINCT_INCREMENTAL) or parsing fails.
	bool GetDistinctAuxMeta(const string &view_name, DistinctAuxMeta &out);
	static string DistinctAuxMetaToJson(const DistinctAuxMeta &meta);

	struct CountDistinctAuxMeta {
		string aux_table;
		string source;
		vector<string> group_cols;
		vector<string> group_source_exprs;
		string distinct_col;
		string distinct_expr;
		string output_col;
		string filter;
	};

	bool GetCountDistinctAuxMeta(const string &view_name, CountDistinctAuxMeta &out);
	static string CountDistinctAuxMetaToJson(const CountDistinctAuxMeta &meta);

	struct SemiAntiAuxMeta {
		string aux_table;
		string join_type;
		string left_table;
		string left_alias;
		string right_table;
		string right_alias;
		string predicate;
		string post_filter;
		string right_filter;
		bool null_aware = false;
		string null_aware_left_col;
		string null_aware_right_expr;
		vector<string> left_cols;
		vector<string> left_exprs;
		vector<string> output_cols;
	};

	bool GetSemiAntiAuxMeta(const string &view_name, SemiAntiAuxMeta &out);
	static string SemiAntiAuxMetaToJson(const SemiAntiAuxMeta &meta);

	struct WindowPartitionLineageOp {
		string kind;
		string output_col;
		string source;
		string source_col;
		// A target type for one CAST, or an encoded expression template for nested/TRY_CAST chains.
		string source_cast;
		string lookup;
		string lookup_col;
		string lookup_cast;
		string lookup_out;
		string lookup_out_cast;
	};

	bool GetWindowPartitionLineage(const string &view_name, vector<WindowPartitionLineageOp> &out);
	static string WindowPartitionLineageToJson(const vector<WindowPartitionLineageOp> &ops);

	struct ProjectionKeyLineageStep {
		string table;
		idx_t occurrence = 0;
		string lookup_col;
		string lookup_out;
	};

	struct ProjectionKeyLineageArm {
		string source;
		idx_t occurrence = 0;
		string source_col;
		vector<ProjectionKeyLineageStep> steps;
	};

	struct ProjectionKeyLineage {
		string output_col;
		string key_source;
		idx_t key_occurrence = 0;
		string key_col;
		vector<ProjectionKeyLineageArm> arms;
	};

	bool GetProjectionKeyLineage(const string &view_name, ProjectionKeyLineage &out);
	static string ProjectionKeyLineageToJson(const ProjectionKeyLineage &lineage);

	struct LeftJoinKeySource {
		string table;
		idx_t occurrence = 0;
		string column;
		string cast_type;
		bool cardinality_transition_check_safe = false;
	};

	bool GetLeftJoinKeySource(const string &view_name, LeftJoinKeySource &out);
	static string LeftJoinKeySourceToJson(const LeftJoinKeySource &source);

	// Nested outer-join inputs used to discover NULL-padding keys of a FULL projection.
	struct FullOuterScopeInput {
		string table;
		idx_t occurrence = 0;
		idx_t presence_index = 0;
	};
	struct FullOuterScopeSource {
		string key_column;
		string presence_column;
		vector<FullOuterScopeInput> inputs;
	};
	struct FullOuterScope {
		FullOuterScopeSource left, right;
	};
	vector<FullOuterScope> GetFullOuterScopes(const string &view_name);
	static string FullOuterScopesToJson(const vector<FullOuterScope> &scopes);

	struct LeftJoinNullableSources {
		vector<string> tables;
		bool complete = false;
	};

	bool GetLeftJoinNullableSources(const string &view_name, LeftJoinNullableSources &out);
	static string LeftJoinNullableSourcesToJson(const LeftJoinNullableSources &src);

	struct FilteredGroupCountAuxMeta {
		string aux_table;
		string source;
		string group_col;
		string sum_col;
		string source_group_expr;
		string source_sum_expr;
		string output_col;
		string comparison_op;
		string threshold_sql;
	};

	bool GetFilteredGroupCountAuxMeta(const string &view_name, FilteredGroupCountAuxMeta &out);
	static string FilteredGroupCountAuxMetaToJson(const FilteredGroupCountAuxMeta &meta);

	// LEFT JOIN pipeline secondary-delta maintenance (Larson & Zhou). The secondary-delta SQL is
	// generated once at CREATE time (from the plan) and stored; refresh substitutes the per-source
	// delta timestamps and appends it between the primary-delta INSERT and the MERGE. Zero refresh-time
	// plan walks.
	struct LeftJoinSecondaryMeta {
		string sql; // secondary-delta INSERT (run before the MERGE; carries LJSEC_* placeholders)
		vector<string> preserved_cols;
		// Identities of the two sides whose pending changes the placeholders stand for. Refresh needs
		// them to build the right row source per storage backend (delta table vs DuckLake snapshots).
		vector<string> inner_tables; // qualified base-table names of the inner (null-supplying) sides
		vector<string> inner_keys;   // join key columns on the inner sides
		vector<string> pres_tables;  // qualified base-table names of the preserved sides
		vector<string> pres_keys;    // join key columns on the preserved sides
	};

	bool GetLeftJoinSecondaryMeta(const string &view_name, LeftJoinSecondaryMeta &out);
	static string LeftJoinSecondaryMetaToJson(const LeftJoinSecondaryMeta &meta);

	static vector<string> ExpectedDistinctAuxColumns(const DistinctAuxMeta &meta);
	static vector<string> ExpectedCountDistinctAuxColumns(const CountDistinctAuxMeta &meta);
	static vector<string> ExpectedFilteredGroupCountAuxColumns(const FilteredGroupCountAuxMeta &meta);
	static vector<string> ExpectedSemiAntiAuxColumns(const SemiAntiAuxMeta &meta);
	bool AuxStateNeedsRepair(const string &view_name, const string &catalog_name, const string &schema_name);
	vector<string> GetAuxiliaryTables(const string &view_name);
};

} // namespace duckdb

#endif // REFRESH_METADATA_HPP
