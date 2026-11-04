#include "core/parser_create_mv_helpers.hpp"
#include "match/constraint_cache.hpp"
#define DUCKDB_EXTENSION_MAIN

#include "core/openivm_extension.hpp"
#include "compile_facts.hpp"
#include "spark_scalar_functions.hpp"
#include "core/openivm_constants.hpp"
#include "core/refresh_metadata.hpp"
#include "core/refresh_daemon.hpp"
#include "core/refresh_locks.hpp"
#include "core/sql_utils.hpp"
#include "core/time_travel_pins.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_cost_model.hpp"
#include "upsert/refresh.hpp"
#include "upsert/refresh_internal.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/common/file_system.hpp"
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
#include "duckdb/planner/planner_extension.hpp"
#include "core/parser.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/parser_ddl.hpp"
#include "rules/incremental_rewrite_rule.hpp"
#include "rules/refresh_insert_rule.hpp"
#include "core/openivm_debug.hpp"

#include <map>
#include <mutex>

namespace duckdb {

// Global daemon instance — started unconditionally at extension load.
// The daemon sleeps and periodically checks for scheduled views; no work if none exist.
static shared_ptr<RefreshDaemon> global_daemon;

static duckdb::unique_ptr<FunctionData> ComputeDeltaBind(ClientContext &context, TableFunctionBindInput &input,
                                                         vector<LogicalType> &return_types, vector<Identifier> &names) {
	string view_catalog_name = StringValue::Get(input.inputs[0]);
	string view_schema_name = StringValue::Get(input.inputs[1]);
	string view_name = StringValue::Get(input.inputs[2]);
	OPENIVM_DEBUG_PRINT("View to be incrementally maintained: %s \n", view_name.c_str());

	Connection con(*context.db);
	RefreshMetadata::UseCatalog(context, con, view_catalog_name);
	if (!context.transaction.IsAutoCommit()) {
		RefreshMetadata(con).SnapshotTransaction(context);
	}
	string view_query = RefreshMetadata(con).GetViewQuery(view_name);
	if (view_query.empty()) {
		throw Exception(ExceptionType::CATALOG,
		                "IVM: materialized view '" + view_name + "' not found or its definition is missing");
	}
	OPENIVM_DEBUG_PRINT("[ComputeDelta Bind] View: %s, Query: %s\n", view_name.c_str(), view_query.c_str());

	auto parser = Parser::GetBuiltinParser();
	parser.ParseQuery(view_query);
	duckdb::openivm::TimeTravelPins::Peel(context, *parser.statements[0]);
	Planner planner(context);
	planner.CreatePlan(std::move(parser.statements[0]));
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

// ComputeDelta only marks a plan for IncrementalRewriteRule, which replaces it; it never produces rows.
static void ComputeDeltaFunction(ClientContext &, TableFunctionInput &, DataChunk &) {
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &db_config = duckdb::DBConfig::GetConfig(instance);

	// OpenIVM materializes and refreshes relations, not ordered result streams.
	// Make unordered execution the database-wide default for connections created
	// after the extension is loaded. Entry points also set the current ClientContext
	// explicitly because pre-existing local settings override global defaults.
	db_config.SetOption(PreserveInsertionOrderSetting::SettingIndex, Value::BOOLEAN(false));
	// CREATE/ALTER MATERIALIZED VIEW and transactional DROP VIEW are expanded into
	// ordinary DuckDB statements by OpenIVM's parser override. FALLBACK leaves every
	// statement OpenIVM does not recognize with DuckDB's native parser.
	db_config.SetOption(AllowParserOverrideExtensionSetting::SettingIndex, Value("fallback"));

	RegisterSparkScalarFunctions(loader);

	// Plain options: {name, description, type, default}. Options with validation are registered below.
	struct PlainOption {
		const char *name;
		const char *description;
		LogicalType type;
		Value default_value;
	};
	const auto on = Value::BOOLEAN(true);
	const auto off = Value::BOOLEAN(false);
	const vector<PlainOption> plain_options = {
	    {"openivm_files_path", "path for compiled SQL reference files", LogicalType::VARCHAR, Value()},
	    {"openivm_adaptive_refresh", "experimental: enable adaptive cost model (when off, always use IVM)",
	     LogicalType::BOOLEAN, off},
	    {"openivm_cascade_refresh", "cascade mode: off, upstream, downstream, or both", LogicalType::VARCHAR,
	     Value("downstream")},
	    {"openivm_adaptive_backoff", "auto-increase refresh interval when refresh takes longer than the interval",
	     LogicalType::BOOLEAN, on},
	    {"openivm_disable_daemon", "disable the refresh daemon (for shadow/compile-only DBs)", LogicalType::BOOLEAN,
	     off},
	    {openivm::METADATA_LEASE_SETTING,
	     "seconds a client of a remote metadata catalog owns a view's refresh before another client may take it over "
	     "(renewed while the refresh runs)",
	     LogicalType::BIGINT, Value::BIGINT(600)},
	    {openivm::TEST_FAIL_POINT_SETTING,
	     "testing only: simulate a crash at a refresh protocol step (after_intent, after_view_delta, "
	     "before_data_commit, after_data_commit), a failing data statement (data_statement_error), a lease takeover "
	     "(lease_lost_before_data, lease_lost_mid_data, lease_lost_before_watermark) or a concurrent commit "
	     "(concurrent_commit_before_data, concurrent_commit_after_data, or concurrent_commit_then_data_error after the "
	     "interruption marker, followed by a failing data statement)",
	     LogicalType::VARCHAR, Value("")},
	    {openivm::TEST_CONCURRENT_SQL_SETTING,
	     "testing only: SQL that a concurrent_commit_* fail point commits from another connection during the first "
	     "refresh attempt",
	     LogicalType::VARCHAR, Value("")},
	    // Native refresh can optimize the finished incremental plan against the current deltas because the
	    // generated SQL is executed immediately. The reusable base-view template always stays data-independent.
	    {"openivm_enable_data_dependent_optimizers",
	     "optimize the finished incremental plan using current delta statistics", LogicalType::BOOLEAN, on},
	    // Per-optimization flags
	    {"openivm_skip_empty_deltas", "skip refresh or join terms when deltas are empty", LogicalType::BOOLEAN, on},
	    {"openivm_compact_deltas", "compact raw delta rows into net logical Z-set deltas before refresh",
	     LogicalType::BOOLEAN, on},
	    {"openivm_ducklake_nterm", "use N-term telescoping for DuckLake joins (vs 2^N-1 inclusion-exclusion)",
	     LogicalType::BOOLEAN, on},
	    {"openivm_regular_nterm", "use N-term telescoping for compile-only regular inner joins", LogicalType::BOOLEAN,
	     on},
	    {"openivm_regular_nterm_left", "extend compile-only N-term telescoping to LEFT-join projection views",
	     LogicalType::BOOLEAN, on},
	    {"openivm_fk_pruning", "prune inclusion-exclusion join terms using FK constraints", LogicalType::BOOLEAN, on},
	    {"openivm_emit_spark_hints", "emit Spark optimizer hints in target_dialect=spark compiled refresh SQL",
	     LogicalType::BOOLEAN, off},
	    {"openivm_skip_aggregate_delete", "skip zero-row DELETE for grouped aggregates when deltas are insert-only",
	     LogicalType::BOOLEAN, on},
	    {"openivm_skip_projection_delete", "skip DELETE and consolidation for projections when deltas are insert-only",
	     LogicalType::BOOLEAN, on},
	    {"openivm_minmax_incremental", "use GREATEST/LEAST for MIN/MAX when deltas are insert-only",
	     LogicalType::BOOLEAN, on},
	    {"openivm_running_window_incremental",
	     "extend cumulative running window aggregates for append-only suffix batches", LogicalType::BOOLEAN, off},
	    {"openivm_having_merge",
	     "use MERGE for HAVING views (store all groups, VIEW filters) instead of group-recompute", LogicalType::BOOLEAN,
	     on},
	    {"openivm_left_join_merge",
	     "use incremental MERGE for LEFT JOIN aggregates (Larson & Zhou) instead of group-recompute",
	     LogicalType::BOOLEAN, on},
	    {"openivm_full_outer_merge",
	     "use incremental MERGE for FULL OUTER JOIN aggregates (Zhang & Larson) instead of group-recompute",
	     LogicalType::BOOLEAN, on},
	    {"openivm_distinct_aux_state",
	     "use auxiliary count state for inner DISTINCT under aggregate (DBSP distinct(R)=sgn(R[t]) — emit ±1 only on "
	     "count transitions across zero) instead of GROUP_RECOMPUTE. Single-source views only in v0; multi-source "
	     "views fall back to GROUP_RECOMPUTE.",
	     LogicalType::BOOLEAN, off},
	    {"openivm_stateful_auxstate",
	     "use persisted auxiliary state for stateful operators such as COUNT(DISTINCT) instead of GROUP_RECOMPUTE",
	     LogicalType::BOOLEAN, off},
	    // Learned cost model
	    {"openivm_cost_decay", "decay factor for learned cost model regression (0.0-1.0, higher = slower adaptation)",
	     LogicalType::DOUBLE, Value::DOUBLE(0.9)},
	    {"openivm_profile_refresh", "record per-step materialized view refresh timings", LogicalType::BOOLEAN, off},
	    {"openivm_archive_compiled_sql", "archive executed CREATE and refresh SQL in metadata tables",
	     LogicalType::BOOLEAN, off},
	    {"openivm_profile_retention_days",
	     "delete refresh profile rows older than this many days when profiling is written", LogicalType::BIGINT,
	     Value::BIGINT(31)},
	    {"openivm_explain_initial_load", "print CREATE MATERIALIZED VIEW initial-load SQL and EXPLAIN plans",
	     LogicalType::BOOLEAN, off},
	    {"openivm_snapshot_publication", "publish unordered DuckLake materialized views through retained snapshots",
	     LogicalType::BOOLEAN, off},
	    {"openivm_explain_initial_load_only", "diagnose CREATE MATERIALIZED VIEW initial load without executing DDL",
	     LogicalType::BOOLEAN, off},
	    {openivm::INTERNAL_SCHEMA_SETTING,
	     "schema for OpenIVM internal maintenance tables within each data catalog; empty keeps them beside the "
	     "materialized view and its sources",
	     LogicalType::VARCHAR, Value("")},
	};
	for (auto &option : plain_options) {
		db_config.AddExtensionOption(option.name, option.description, option.type, option.default_value);
	}

	// 'auto' is the non-forcing mode: like 'incremental', it never forces a full recompute; adaptive
	// IVM-vs-full selection is controlled separately by openivm_adaptive_refresh.
	db_config.AddExtensionOption(
	    "openivm_refresh_mode",
	    "refresh strategy: incremental, full, or auto (auto never forces full; see openivm_adaptive_refresh)",
	    LogicalType::VARCHAR, Value("incremental"), [](ClientContext &, SetScope, Value &parameter) {
		    auto mode = parameter.IsNull() ? string() : StringUtil::Lower(parameter.ToString());
		    if (mode != "incremental" && mode != "full" && mode != "auto") {
			    throw InvalidInputException(
			        "Invalid openivm_refresh_mode '%s': expected one of incremental, full, auto",
			        parameter.IsNull() ? "NULL" : parameter.ToString());
		    }
		    parameter = Value(mode);
	    });

	// Metadata placement is a property of the database, not of one session: two sessions
	// using different locations would split one database's materialized views. Plain SET
	// therefore applies globally, so helper connections and the daemon see it too.
	auto validate_placement = [](ClientContext &, SetScope scope, Value &parameter) {
		if (scope == SetScope::SESSION) {
			throw InvalidInputException("OpenIVM metadata placement is database-wide; use SET or SET GLOBAL, not "
			                            "SET SESSION");
		}
		if (parameter.IsNull()) {
			parameter = Value("");
		}
	};
	db_config.AddExtensionOption(openivm::METADATA_CATALOG_SETTING,
	                             "attached catalog that stores OpenIVM metadata (empty: each native view catalog, or "
	                             "the default database for external catalogs)",
	                             LogicalType::VARCHAR, Value(""), validate_placement, SetScope::GLOBAL);
	db_config.AddExtensionOption(openivm::METADATA_SCHEMA_SETTING,
	                             "schema that stores OpenIVM metadata; empty retains its existing schema",
	                             LogicalType::VARCHAR, Value(""), validate_placement, SetScope::GLOBAL);
	db_config.AddExtensionOption(duckdb::OPENIVM_INPUT_DIALECT_SETTING,
	                             "SQL dialect materialized-view bodies are written in: duckdb (default) or spark",
	                             LogicalType::VARCHAR, Value("duckdb"), duckdb::SetOpenIvmInputDialect);
	db_config.AddExtensionOption("openivm_compiled_sql_retention", "maximum archived versions per view and operation",
	                             LogicalType::BIGINT, Value::BIGINT(10), [](ClientContext &, SetScope, Value &value) {
		                             if (value.IsNull() || value.GetValue<int64_t>() < 1) {
			                             throw InvalidInputException(
			                                 "openivm_compiled_sql_retention must be at least 1");
		                             }
	                             });

	// View matching (master flag — ALL matcher behavior gated by this).
	// Default false: view matching is experimental.
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
	Connection con(instance);
	// Upgrade existing metadata in the schema that already holds it.
	try {
		RefreshMetadata::UseCatalog(*con.context, con);
	} catch (std::exception &ex) {
		OPENIVM_DEBUG_PRINT("[METADATA] Keeping the default schema for load-time upgrades: %s\n", ex.what());
	}

	// Use the complete definitions at LOAD as well as CREATE MV. Creating history
	// without strategy and then ALTERing it leaves a timestamp-default rebind in
	// the WAL, which cannot run before DuckDB initializes its default database.
	for (auto &table : {openivm::HISTORY_TABLE, openivm::PROFILE_TABLE, openivm::MATCH_LOG_TABLE,
	                    openivm::MV_DEPS_TABLE, openivm::CONSTRAINTS_CACHE_TABLE}) {
		con.Query(CreateMetadataTableSQL(table));
	}
	// Upgrade view and source metadata created by older releases; fresh databases have none yet, so these
	// fail harmlessly. CREATE MV runs the same migration for every other metadata location.
	vector<string> migrations;
	for (auto &table : {openivm::VIEWS_TABLE, openivm::DELTA_TABLES_TABLE, openivm::HISTORY_TABLE}) {
		AppendMetadataMigrationSQL(migrations, table);
	}
	for (auto &sql : HasJoinBackfillSQL()) {
		migrations.push_back(sql);
	}
	for (auto &sql : migrations) {
		con.Query(sql);
	}

	RegisterMaterializedViewGrammar(instance);
	auto materialized_view_parser = duckdb::MaterializedViewParserExtension();

	auto incremental_rewrite_rule = duckdb::IncrementalRewriteRule();
	auto refresh_insert_rule = duckdb::RefreshInsertRule();

	ParserExtension::Register(db_config, std::move(materialized_view_parser));
	PlannerExtension mv_lifecycle;
	mv_lifecycle.post_bind_function = BindMaterializedViewLifecycle;
	PlannerExtension::Register(db_config, std::move(mv_lifecycle));
	OptimizerExtension::Register(db_config, std::move(incremental_rewrite_rule));
	OptimizerExtension::Register(db_config, std::move(refresh_insert_rule));

	loader.RegisterFunction(SourceWatermarkFunction());
	loader.RegisterFunction(SourceColumnsFunction());
	loader.RegisterFunction(CreateDeltaFromDataFunction());
	loader.RegisterFunction(DropEntryFunction(CatalogType::VIEW_ENTRY));
	loader.RegisterFunction(DropEntryFunction(CatalogType::TABLE_ENTRY));
	loader.RegisterFunction(DropEntryFunction(CatalogType::SCHEMA_ENTRY));

	loader.RegisterFunction(TableFunction("ComputeDelta",
	                                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                      ComputeDeltaFunction, ComputeDeltaBind));

	// openivm_compile_with_facts(view_name VARCHAR, facts_json VARCHAR)
	//   → (refresh_type INTEGER, refresh_type_name VARCHAR, stmt_order INTEGER,
	//      stmt_kind VARCHAR, sql VARCHAR)
	//
	// Per-call compile-only entry point. Emits one row per top-level
	// refresh statement; concatenate by stmt_order to reconstruct the full
	// refresh program. The facts_json controls dialect (target_dialect),
	// compile-only behaviour, and downstream-cascade hints; see
	// compile_facts.hpp for the schema.
	loader.RegisterFunction(TableFunction("openivm_compile_with_facts", {LogicalType::VARCHAR, LogicalType::VARCHAR},
	                                      openivm::OpenIvmCompileWithFactsExecute, openivm::OpenIvmCompileWithFactsBind,
	                                      openivm::OpenIvmCompileWithFactsInit));

	auto refresh_options =
	    PragmaFunction::PragmaCall("refresh_options", TransactionalRefreshQuery,
	                               {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_options);
	auto refresh = PragmaFunction::PragmaCall("refresh", TransactionalRefreshQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh);
	loader.RegisterFunction(PragmaFunction::PragmaCall("refresh_pipeline", RefreshPipelineQuery, {LogicalType::VARCHAR},
	                                                   LogicalType::VARCHAR));
	loader.RegisterFunction(openivm::DeclareRelyFKFunction());
	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "openivm_declare_rely_fk",
	    [](ClientContext &, const FunctionParameters &parameters) {
		    vector<string> arguments;
		    for (auto &value : parameters.values) {
			    arguments.push_back(value.ToSQLString());
		    }
		    return "SELECT * FROM openivm_register_rely_fk(" + StringUtil::Join(arguments, ",") + ")";
	    },
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}));
	auto refresh_cost = PragmaFunction::PragmaCall("refresh_cost", RefreshCostQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_cost);
	auto refresh_history =
	    PragmaFunction::PragmaCall("refresh_history", RefreshCostHistoryQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_history);
	auto refresh_cross_system = PragmaFunction::PragmaCall(
	    "refresh_cross_system", TransactionalRefreshQuery,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_cross_system);

	loader.RegisterFunction(PragmaFunction::PragmaCall(
	    "openivm_files",
	    [](ClientContext &context, const FunctionParameters &parameters) -> string {
		    auto view_name = StringValue::Get(parameters.values[0]);
		    Connection con(*context.db);
		    view_name = ResolveViewCatalogFromContext(context, con, view_name).view_name;
		    RefreshMetadata(con).GetViewType(view_name);
		    Value directory;
		    bool enabled =
		        context.TryGetCurrentSetting(Identifier("openivm_files_path"), directory) && !directory.IsNull();
		    auto &fs = FileSystem::GetFileSystem(context);
		    auto cwd = FileSystem::GetWorkingDirectory();
		    vector<string> rows;
		    const vector<pair<string, string>> files = {
		        {"system", "openivm_system_tables.sql"},
		        {"create", "openivm_compiled_queries_" + view_name + ".sql"},
		        {"refresh", "openivm_upsert_queries_" + view_name + ".sql"},
		        {"explain", "openivm_initial_load_explain_" + view_name + ".txt"}};
		    for (const auto &file : files) {
			    string path;
			    bool exists = false;
			    string status =
			        "disabled: SET openivm_files_path to an existing writable directory before CREATE/refresh";
			    if (enabled) {
				    path = directory.ToString() + "/" + file.second;
				    if (!fs.IsPathAbsolute(path)) {
					    path = fs.JoinPath(cwd, path);
				    }
				    exists = fs.FileExists(path);
				    status = exists ? "exists; may be from an earlier compilation" : "not generated at this path";
			    }
			    rows.push_back("(" + Value(file.first).ToSQLString() + ", " +
			                   (enabled ? Value(path).ToSQLString() : "NULL::VARCHAR") + ", " +
			                   (exists ? "true" : "false") + ", " + Value(status).ToSQLString() + ", " +
			                   Value(cwd).ToSQLString() + ")");
		    }
		    OPENIVM_DEBUG_PRINT("[FILES] Inspecting reference files for %s (enabled=%d)\n", view_name.c_str(), enabled);
		    return "SELECT * FROM (VALUES " + StringUtil::Join(rows, ", ") +
		           ") AS files(kind, path, file_exists, status, working_directory)";
	    },
	    {LogicalType::VARCHAR}));

	// PRAGMA refresh_status('view_name') — returns refresh status for a materialized view.
	auto refresh_status = PragmaFunction::PragmaCall(
	    "refresh_status",
	    [](ClientContext &context, const FunctionParameters &parameters) -> string {
		    string view_name = StringValue::Get(parameters.values[0]);
		    Connection con(*context.db.get());
		    view_name = ResolveViewCatalogFromContext(context, con, view_name).view_name;
		    RefreshMetadata metadata(con);

		    auto interval = metadata.GetRefreshInterval(view_name);
		    string interval_str = interval > 0 ? to_string(interval) : "NULL";

		    // Get the earliest last_update across all delta tables for this view
		    auto last_update_result = con.Query("SELECT MIN(last_update) FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
		    string last_refresh = "NULL";
		    string next_refresh = "NULL";
		    if (!last_update_result->HasError() && last_update_result->RowCount() > 0 &&
		        !last_update_result->Collection().GetValue(0, 0).IsNull()) {
			    last_refresh = "'" + last_update_result->Collection().GetValue(0, 0).ToString() + "'";
			    if (interval > 0) {
				    next_refresh = "'" + last_update_result->Collection().GetValue(0, 0).ToString() +
				                   "'::TIMESTAMP + INTERVAL '" + to_string(interval) + " seconds'";
			    }
		    }

		    auto location = metadata.GetStoredViewLocation(view_name);
		    auto view_key = SqlUtils::FullName(location.catalog_name, location.schema_name, view_name);
		    // Check daemon status
		    string status = "'idle'";
		    string effective_interval = interval_str;
		    if (global_daemon) {
			    if (global_daemon->IsRefreshing(view_key)) {
				    status = "'refreshing'";
			    }
			    auto eff = global_daemon->GetEffectiveInterval(view_key);
			    if (eff > 0) {
				    effective_interval = to_string(eff);
			    }
		    }

		    // Report 'unknown' rather than failing when the type is missing or invalid.
		    string strategy_str = "'unknown'";
		    try {
			    strategy_str = "'" + StringUtil::Lower(RefreshTypeName(metadata.GetViewType(view_name))) + "'";
		    } catch (std::exception &) {
		    }

		    return "SELECT " + parameters.values[0].ToSQLString() + " AS view_name, " + interval_str +
		           " AS refresh_interval, " + last_refresh + " AS last_refresh, " + next_refresh +
		           " AS next_refresh, " + status + " AS status, " + effective_interval + " AS effective_interval, " +
		           strategy_str + " AS refresh_strategy;";
	    },
	    {LogicalType::VARCHAR});
	loader.RegisterFunction(refresh_status);

	// PRAGMA refresh_start_daemon — (re)start the daemon on the caller's DB instance.
	auto refresh_start_daemon = PragmaFunction::PragmaCall(
	    "refresh_start_daemon",
	    [](ClientContext &context, const FunctionParameters &) -> string {
		    if (!context.transaction.IsAutoCommit()) {
			    throw TransactionException("The OpenIVM refresh daemon cannot be restarted inside a transaction");
		    }
		    if (global_daemon) {
			    global_daemon->Stop();
		    }
		    global_daemon = make_shared_ptr<RefreshDaemon>();
		    global_daemon->Start(*context.db);
		    global_daemon->Wake();
		    return "SELECT true AS started;";
	    },
	    {});
	loader.RegisterFunction(refresh_start_daemon);

	// Start the refresh daemon unless disabled (e.g. shadow/compile-only DBs).
	if (!SqlUtils::GetBoolSetting(*con.context, "openivm_disable_daemon", false)) {
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
