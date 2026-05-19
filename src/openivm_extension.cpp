#define DUCKDB_EXTENSION_MAIN

#include "core/openivm_extension.hpp"
#include "core/openivm_constants.hpp"
#include "core/refresh_metadata.hpp"
#include "core/refresh_daemon.hpp"
#include "core/refresh_locks.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_cost_model.hpp"
#include "upsert/refresh.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "core/parser.hpp"
#include "rules/incremental_rewrite_rule.hpp"
#include "rules/refresh_insert_rule.hpp"
#include "core/openivm_debug.hpp"

#include <map>
#include <mutex>

namespace duckdb {

// Global daemon instance — started unconditionally at extension load.
// The daemon sleeps and periodically checks for scheduled views; no work if none exist.
static shared_ptr<RefreshDaemon> global_daemon;

struct ComputeDeltaData : public GlobalTableFunctionState {
	ComputeDeltaData() : offset(0) {
	}
	idx_t offset;
	string view_name;
};

unique_ptr<GlobalTableFunctionState> ComputeDeltaInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<ComputeDeltaData>();
	return std::move(result);
}

static duckdb::unique_ptr<FunctionData> ComputeDeltaBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<string> &names) {
	string view_catalog_name = StringValue::Get(input.inputs[0]);
	string view_schema_name = StringValue::Get(input.inputs[1]);
	string view_name = StringValue::Get(input.inputs[2]);
	OPENIVM_DEBUG_PRINT("View to be incrementally maintained: %s \n", view_name.c_str());

	input.named_parameters["view_name"] = view_name;
	input.named_parameters["view_catalog_name"] = view_catalog_name;
	input.named_parameters["view_schema_name"] = view_schema_name;

	Connection con(*context.db);
	string view_query = RefreshMetadata(con).GetViewQuery(view_name);
	if (view_query.empty()) {
		throw Exception(ExceptionType::CATALOG,
		                "IVM: materialized view '" + view_name + "' not found or its definition is missing");
	}
	OPENIVM_DEBUG_PRINT("[ComputeDelta Bind] View: %s, Query: %s\n", view_name.c_str(), view_query.c_str());

	Parser parser;
	parser.ParseQuery(view_query);
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	OPENIVM_DEBUG_PRINT("[ComputeDelta Bind] Plan:\n%s\n", planner.plan->ToString().c_str());

	auto result = make_uniq<TableFunctionData>();
	for (size_t i = 0; i < planner.names.size(); i++) {
		return_types.emplace_back(planner.types[i]);
		names.emplace_back(planner.names[i]);
		OPENIVM_DEBUG_PRINT("[ComputeDelta Bind] Column %zu: %s (%s)\n", i, planner.names[i].c_str(),
		                    planner.types[i].ToString().c_str());
	}

	return_types.emplace_back(LogicalTypeId::INTEGER);
	names.emplace_back(openivm::MULTIPLICITY_COL);

	return std::move(result);
}

static void ComputeDeltaFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<ComputeDeltaData &>(*data_p.global_state);
	if (data.offset >= 1) {
		return;
	}
	return;
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &db_config = duckdb::DBConfig::GetConfig(instance);

	// OpenIVM materializes and refreshes relations, not ordered result streams.
	// Make unordered execution the database-wide default for connections created
	// after the extension is loaded. Entry points also set the current ClientContext
	// explicitly because pre-existing local settings override global defaults.
	db_config.SetOption(PreserveInsertionOrderSetting::SettingIndex, Value::BOOLEAN(false));

	db_config.AddExtensionOption("openivm_files_path", "path for compiled SQL reference files", LogicalType::VARCHAR);
	db_config.AddExtensionOption("openivm_refresh_mode", "refresh strategy: incremental, full, or auto",
	                             LogicalType::VARCHAR, Value("incremental"));
	db_config.AddExtensionOption("openivm_adaptive_refresh",
	                             "experimental: enable adaptive cost model (when off, always use IVM)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("openivm_cascade_refresh", "cascade mode: off, upstream, downstream, or both",
	                             LogicalType::VARCHAR, Value("downstream"));
	db_config.AddExtensionOption("openivm_adaptive_backoff",
	                             "auto-increase refresh interval when refresh takes longer than the interval",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_disable_daemon", "disable the refresh daemon (for shadow/compile-only DBs)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Per-optimization flags (default: all enabled)
	db_config.AddExtensionOption("openivm_skip_empty_deltas", "skip refresh or join terms when deltas are empty",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_compact_deltas",
	                             "compact raw delta rows into net logical Z-set deltas before refresh",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_ducklake_nterm",
	                             "use N-term telescoping for DuckLake joins (vs 2^N-1 inclusion-exclusion)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_fk_pruning", "prune inclusion-exclusion join terms using FK constraints",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_skip_aggregate_delete",
	                             "skip zero-row DELETE for grouped aggregates when deltas are insert-only",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_skip_projection_delete",
	                             "skip DELETE and consolidation for projections when deltas are insert-only",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_minmax_incremental",
	                             "use GREATEST/LEAST for MIN/MAX when deltas are insert-only", LogicalType::BOOLEAN,
	                             Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_having_merge",
	                             "use MERGE for HAVING views (store all groups, VIEW filters) "
	                             "instead of group-recompute",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));

	db_config.AddExtensionOption("openivm_left_join_merge",
	                             "use incremental MERGE for LEFT JOIN aggregates (Larson & Zhou) "
	                             "instead of group-recompute",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_full_outer_merge",
	                             "use incremental MERGE for FULL OUTER JOIN aggregates (Zhang & Larson) "
	                             "instead of group-recompute",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("openivm_distinct_aux_state",
	                             "use auxiliary count state for inner DISTINCT under aggregate (DBSP "
	                             "distinct(R)=sgn(R[t]) — emit ±1 only on count transitions across zero) "
	                             "instead of GROUP_RECOMPUTE. Single-source views only in v0; multi-source "
	                             "views fall back to GROUP_RECOMPUTE.",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Learned cost model
	db_config.AddExtensionOption("openivm_cost_decay",
	                             "decay factor for learned cost model regression (0.0-1.0, higher = slower adaptation)",
	                             LogicalType::DOUBLE, Value::DOUBLE(0.9));

	// View matching (master flag — ALL matcher behavior gated by this).
	// Default false. See `feedback_view_matching_flag` memory note.
	db_config.AddExtensionOption("openivm_enable_view_matching",
	                             "enable smart view matching at query time (master flag)", LogicalType::BOOLEAN,
	                             Value::BOOLEAN(false));
	db_config.AddExtensionOption("openivm_predicate_oracle",
	                             "predicate implication oracle: 'syntactic', 'interval' (default), or 'sat' (stub)",
	                             LogicalType::VARCHAR, Value("interval"));
	db_config.AddExtensionOption("openivm_match_strategies",
	                             "allowed strategies (csv): 'tier1','tier2','tier3','partial','full' or 'all'",
	                             LogicalType::VARCHAR, Value("all"));
	db_config.AddExtensionOption("openivm_match_estimate_ttl_ms",
	                             "max staleness (ms) for cached pending-delta-row estimate before refresh",
	                             LogicalType::BIGINT, Value::BIGINT(5000));
	db_config.AddExtensionOption("openivm_match_log_decisions", "log per-query matcher decisions to openivm_match_log",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("openivm_match_log_retention", "max rows per query_hash retained in openivm_match_log",
	                             LogicalType::BIGINT, Value::BIGINT(50));
	db_config.AddExtensionOption("openivm_profile_refresh", "record per-step materialized view refresh timings",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("openivm_profile_retention_days",
	                             "delete refresh profile rows older than this many days when profiling is written",
	                             LogicalType::BIGINT, Value::BIGINT(31));
	db_config.AddExtensionOption("openivm_explain_initial_load",
	                             "print CREATE MATERIALIZED VIEW initial-load SQL and EXPLAIN plans",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("openivm_explain_initial_load_only",
	                             "diagnose CREATE MATERIALIZED VIEW initial load without executing DDL",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Compile-only mode. When true, `PRAGMA refresh(...)` (and the
	// `PRAGMA compile_refresh(...)` convenience) generate the refresh SQL
	// artifact (controlled by `openivm_files_path`) but skip the actual
	// execution. Used by openivm-spark to compile refresh plans without
	// mutating the source DuckDB database.
	db_config.AddExtensionOption("openivm_compile_only",
	                             "when true, PRAGMA refresh writes the SQL artifact but does not execute it",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Force-emit the AGGREGATE_GROUP / AGGREGATE_HAVING downstream "retract
	// companion" INSERT into openivm_delta_<view> even when no downstream MV
	// is registered yet. In native DuckDB mode the companion is gated by a
	// runtime metadata check (`openivm_delta_tables` rows referencing this
	// MV's delta view); openivm-spark compiles each MV in an isolated DuckDB
	// subprocess where no such entries exist, so without this flag the
	// AGGREGATE_GROUP refresh produces an additive-only view-delta that
	// breaks cascade for downstream MVs whose semantics depend on multiset
	// retraction (COUNT(*), MIN/MAX, DISTINCT etc.). Setting this flag to
	// true makes the compiled SQL self-contained for the cascade case.
	//
	// Defaults to false to preserve native DuckDB-mode behavior. openivm-spark
	// sets it to true in its compile script.
	db_config.AddExtensionOption("openivm_force_view_delta_cascade",
	                             "force emission of the AGGREGATE_GROUP retract companion to openivm_delta_<view> "
	                             "even when no downstream MV is registered (compile-for-cascade mode)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Force-emit signed cascade rows for WINDOW_PARTITION / GROUP_RECOMPUTE even
	// when no downstream MV is registered yet. Used by openivm-spark's
	// compile-only bridge, which runs each MV in an isolated DuckDB subprocess
	// with an empty openivm_delta_tables catalog.
	db_config.AddExtensionOption(openivm::EMIT_CASCADE_DELTA_FOR_RECOMPUTE,
	                             "force emission of signed cascade rows for WINDOW_PARTITION / GROUP_RECOMPUTE "
	                             "into openivm_delta_<view> even when no downstream MV is registered",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Target SQL dialect for the lpts pipeline. Forwarded to LogicalPlanToAst
	// and AstToCteList at every refresh-SQL generation call site so the
	// emitted SQL is in the requested dialect ('duckdb' (default), 'postgres',
	// or 'spark').
	db_config.AddExtensionOption("openivm_target_dialect",
	                             "target SQL dialect for refresh-SQL generation "
	                             "('duckdb' (default), 'postgres', or 'spark')",
	                             LogicalType::VARCHAR, Value("duckdb"));

	Connection con(instance);

	// Migration: add new columns to existing openivm_views tables
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS refresh_interval BIGINT DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS refresh_in_progress BOOLEAN DEFAULT false");

	// Migration: create refresh history table for learned cost model.
	// Silently fails on fresh DB (core_functions not yet loaded → DEFAULT
	// current_timestamp can't resolve). The parser recreates it with the
	// default at first MV creation, so INSERTs (which omit refresh_timestamp)
	// work. This stanza only matters for legacy DBs where the table already
	// exists without the default.
	con.Query("CREATE TABLE IF NOT EXISTS " + string(openivm::HISTORY_TABLE) +
	          " (view_name VARCHAR, refresh_timestamp TIMESTAMP DEFAULT current_timestamp,"
	          " method VARCHAR, incremental_compute_est DOUBLE, incremental_upsert_est DOUBLE,"
	          " recompute_compute_est DOUBLE, recompute_replace_est DOUBLE,"
	          " actual_duration_ms BIGINT,"
	          " PRIMARY KEY(view_name, refresh_timestamp))");
	con.Query("CREATE TABLE IF NOT EXISTS " + string(openivm::PROFILE_TABLE) +
	          " (refresh_id VARCHAR, view_name VARCHAR, profile_timestamp TIMESTAMP DEFAULT current_timestamp,"
	          " step_order INTEGER, step_name VARCHAR, duration_ms BIGINT, detail VARCHAR,"
	          " PRIMARY KEY(refresh_id, step_order))");

	// View-matching CREATEs first (always succeed), ALTERs after. ALTERs that
	// hit not-yet-existing tables on a fresh DB fail silently — the parser's
	// CREATE TABLE includes these columns directly, so fresh DBs are fine.
	// ALTERs are backward-compat for legacy DBs.
	con.Query("CREATE TABLE IF NOT EXISTS " + string(openivm::MATCH_LOG_TABLE) +
	          " (query_hash UBIGINT,"
	          " log_timestamp TIMESTAMP,"
	          " matched_view VARCHAR,"
	          " chosen_strategy VARCHAR,"
	          " bypass_cost_est DOUBLE,"
	          " chosen_cost_est DOUBLE,"
	          " actual_duration_ms BIGINT,"
	          " PRIMARY KEY (query_hash, log_timestamp))");
	con.Query("CREATE TABLE IF NOT EXISTS " + string(openivm::MV_DEPS_TABLE) +
	          " (parent_view VARCHAR, child_view VARCHAR,"
	          " edge_kind VARCHAR DEFAULT 'direct',"
	          " PRIMARY KEY (parent_view, child_view))");
	con.Query("CREATE TABLE IF NOT EXISTS " + string(openivm::CONSTRAINTS_CACHE_TABLE) +
	          " (table_name VARCHAR, constraint_kind VARCHAR,"
	          " columns_json VARCHAR, referenced_table VARCHAR,"
	          " referenced_columns_json VARCHAR,"
	          " is_trusted BOOLEAN DEFAULT true,"
	          " PRIMARY KEY (table_name, constraint_kind, columns_json))");

	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS signature_hash UBIGINT DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS canonical_plan_blob BLOB DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS output_columns_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS predicate_summary_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS fd_summary_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS source_tables_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS aggregate_decomposition_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS nullified_columns_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS distinct_aux_meta_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS semi_anti_aux_meta_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS window_partition_lineage_json VARCHAR DEFAULT NULL");

	con.Query("ALTER TABLE " + string(openivm::DELTA_TABLES_TABLE) +
	          " ADD COLUMN IF NOT EXISTS pending_row_estimate BIGINT DEFAULT NULL");
	con.Query("ALTER TABLE " + string(openivm::DELTA_TABLES_TABLE) +
	          " ADD COLUMN IF NOT EXISTS pending_estimate_ts TIMESTAMP DEFAULT NULL");

	con.Query("ALTER TABLE " + string(openivm::HISTORY_TABLE) +
	          " ADD COLUMN IF NOT EXISTS strategy VARCHAR DEFAULT 'incremental'");

	auto materialized_view_parser = duckdb::MaterializedViewParserExtension();

	auto incremental_rewrite_rule = duckdb::IncrementalRewriteRule();
	auto refresh_insert_rule = duckdb::RefreshInsertRule();

	ParserExtension::Register(db_config, std::move(materialized_view_parser));
	OptimizerExtension::Register(db_config, std::move(incremental_rewrite_rule));
	OptimizerExtension::Register(db_config, std::move(refresh_insert_rule));

	TableFunction compute_delta_function("ComputeDelta",
	                                     {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                     ComputeDeltaFunction, ComputeDeltaBind, ComputeDeltaInit);

	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	compute_delta_function.name = "ComputeDelta";
	compute_delta_function.named_parameters["view_catalog_name"];
	compute_delta_function.named_parameters["view_schema_name"];
	compute_delta_function.named_parameters["view_name"];
	CreateTableFunctionInfo compute_delta_function_info(compute_delta_function);
	catalog.CreateTableFunction(*con.context, &compute_delta_function_info);
	con.Commit();

	// Use the locked pragma_function_t variant: generates SQL and executes it under a
	// per-view mutex, preventing concurrent refresh from double-applying deltas.
	auto refresh_options =
	    PragmaFunction::PragmaCall("refresh_options", UpsertDeltaQueriesLocked,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_options);
	auto refresh = PragmaFunction::PragmaCall("refresh", UpsertDeltaQueriesLocked, {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh);
	auto refresh_cost = PragmaFunction::PragmaCall("refresh_cost", RefreshCostQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_cost);
	auto refresh_history =
	    PragmaFunction::PragmaCall("refresh_history", RefreshCostHistoryQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_history);
	auto refresh_cross_system = PragmaFunction::PragmaCall(
	    "refresh_cross_system", UpsertDeltaQueriesLocked,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_cross_system);

	// PRAGMA compile_refresh('view_name') — compile-only path used by openivm-spark.
	//
	// Generates the refresh SQL for `view_name` using the current value of
	// `openivm_target_dialect` (defaults to 'duckdb'), but never executes it.
	// The artifact file under `openivm_files_path` is still written as a
	// side effect.
	//
	// The PRAGMA emits a single result row with the columns:
	//   refresh_type        INTEGER  — RefreshType enum value (see openivm_constants.hpp).
	//   refresh_type_name   VARCHAR  — human-readable form (e.g. 'AGGREGATE_GROUP').
	//   sql                 VARCHAR  — assembled refresh SQL in the target dialect.
	auto compile_refresh = PragmaFunction::PragmaCall("compile_refresh", CompileRefreshQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(compile_refresh);

	// PRAGMA refresh_status('view_name') — returns refresh status for a materialized view.
	auto refresh_status = PragmaFunction::PragmaCall(
	    "refresh_status",
	    [](ClientContext &context, const FunctionParameters &parameters) -> string {
		    string view_name = StringValue::Get(parameters.values[0]);
		    Connection con(*context.db.get());
		    RefreshMetadata metadata(con);

		    auto interval = metadata.GetRefreshInterval(view_name);
		    string interval_str = interval > 0 ? to_string(interval) : "NULL";

		    // Get the earliest last_update across all delta tables for this view
		    auto last_update_result = con.Query("SELECT MIN(last_update) FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
		    string last_refresh = "NULL";
		    string next_refresh = "NULL";
		    if (!last_update_result->HasError() && last_update_result->RowCount() > 0 &&
		        !last_update_result->GetValue(0, 0).IsNull()) {
			    last_refresh = "'" + last_update_result->GetValue(0, 0).ToString() + "'";
			    if (interval > 0) {
				    next_refresh = "'" + last_update_result->GetValue(0, 0).ToString() + "'::TIMESTAMP + INTERVAL '" +
				                   to_string(interval) + " seconds'";
			    }
		    }

		    // Check daemon status
		    string status = "'idle'";
		    string effective_interval = interval_str;
		    if (global_daemon) {
			    if (global_daemon->IsRefreshing(view_name)) {
				    status = "'refreshing'";
			    }
			    auto eff = global_daemon->GetEffectiveInterval(view_name);
			    if (eff > 0) {
				    effective_interval = to_string(eff);
			    }
		    }

		    // Query RefreshType directly, same pattern as GetRefreshInterval — returns
		    // 'unknown' on failure rather than throwing. Order MUST mirror RefreshType enum
		    // in src/include/core/openivm_constants.hpp.
		    static const char *kTypeNames[] = {
		        "aggregate_group",      "simple_aggregate",   "simple_projection", "full_refresh",
		        "aggregate_having",     "window_partition",   "group_recompute",   "top_k",
		        "distinct_incremental", "semi_anti_recompute"};
		    string strategy_str = "'unknown'";
		    auto type_result = con.Query("SELECT type FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
		                                 SqlUtils::EscapeValue(view_name) + "'");
		    if (!type_result->HasError() && type_result->RowCount() > 0 && !type_result->GetValue(0, 0).IsNull()) {
			    auto idx = static_cast<size_t>(type_result->GetValue(0, 0).GetValue<int8_t>());
			    if (idx < sizeof(kTypeNames) / sizeof(kTypeNames[0])) {
				    strategy_str = "'" + string(kTypeNames[idx]) + "'";
			    }
		    }

		    return "SELECT '" + SqlUtils::EscapeValue(view_name) + "' AS view_name, " + interval_str +
		           " AS refresh_interval, " + last_refresh + " AS last_refresh, " + next_refresh +
		           " AS next_refresh, " + status + " AS status, " + effective_interval + " AS effective_interval, " +
		           strategy_str + " AS refresh_strategy;";
	    },
	    {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_status);

	// PRAGMA refresh_start_daemon — (re)start the daemon on the caller's DB instance.
	auto refresh_start_daemon =
	    PragmaFunction::PragmaCall("refresh_start_daemon",
	                               [](ClientContext &context, const FunctionParameters &) -> string {
		                               if (global_daemon) {
			                               global_daemon->Stop();
		                               }
		                               global_daemon = make_shared_ptr<RefreshDaemon>();
		                               global_daemon->Start(*context.db);
		                               return "SELECT true AS started;";
	                               },
	                               {});
	loader.RegisterFunction(refresh_start_daemon);

	// Start the refresh daemon unless disabled (e.g. shadow/compile-only DBs).
	bool daemon_disabled = false;
	{
		Value disable_val;
		if (con.context->TryGetCurrentSetting("openivm_disable_daemon", disable_val) && !disable_val.IsNull()) {
			daemon_disabled = disable_val.GetValue<bool>();
		}
	}
	if (!daemon_disabled) {
		global_daemon = make_shared_ptr<RefreshDaemon>();
		global_daemon->Start(instance);
	}
}

void OpenivmExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string OpenivmExtension::Name() {
	return "openivm";
}

std::string OpenivmExtension::Version() const {
#ifdef EXT_VERSION_OPENIVM
	return EXT_VERSION_OPENIVM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(openivm, loader) {
	duckdb::LoadInternal(loader);
}
}
