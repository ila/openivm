#define DUCKDB_EXTENSION_MAIN

#include "core/openivm_extension.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_refresh_daemon.hpp"
#include "core/openivm_refresh_locks.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/openivm_cost_model.hpp"
#include "upsert/openivm_upsert.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "core/openivm_parser.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "rules/openivm_insert_rule.hpp"
#include "core/openivm_debug.hpp"

#include <map>
#include <mutex>

namespace duckdb {

// Global daemon instance — started unconditionally at extension load.
// The daemon sleeps and periodically checks for scheduled views; no work if none exist.
static shared_ptr<IVMRefreshDaemon> global_daemon;

struct DoIVMData : public GlobalTableFunctionState {
	DoIVMData() : offset(0) {
	}
	idx_t offset;
	string view_name;
};

unique_ptr<GlobalTableFunctionState> DoIVMInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DoIVMData>();
	return std::move(result);
}

static duckdb::unique_ptr<FunctionData> DoIVMBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	string view_catalog_name = StringValue::Get(input.inputs[0]);
	string view_schema_name = StringValue::Get(input.inputs[1]);
	string view_name = StringValue::Get(input.inputs[2]);
	OPENIVM_DEBUG_PRINT("View to be incrementally maintained: %s \n", view_name.c_str());

	input.named_parameters["view_name"] = view_name;
	input.named_parameters["view_catalog_name"] = view_catalog_name;
	input.named_parameters["view_schema_name"] = view_schema_name;

	Connection con(*context.db);
	string view_query = IVMMetadata(con).GetViewQuery(view_name);
	if (view_query.empty()) {
		throw Exception(ExceptionType::CATALOG,
		                "IVM: materialized view '" + view_name + "' not found or its definition is missing");
	}
	OPENIVM_DEBUG_PRINT("[DoIVM Bind] View: %s, Query: %s\n", view_name.c_str(), view_query.c_str());

	Parser parser;
	parser.ParseQuery(view_query);
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	OPENIVM_DEBUG_PRINT("[DoIVM Bind] Plan:\n%s\n", planner.plan->ToString().c_str());

	auto result = make_uniq<TableFunctionData>();
	for (size_t i = 0; i < planner.names.size(); i++) {
		return_types.emplace_back(planner.types[i]);
		names.emplace_back(planner.names[i]);
		OPENIVM_DEBUG_PRINT("[DoIVM Bind] Column %zu: %s (%s)\n", i, planner.names[i].c_str(),
		                    planner.types[i].ToString().c_str());
	}

	return_types.emplace_back(LogicalTypeId::INTEGER);
	names.emplace_back(ivm::MULTIPLICITY_COL);

	return std::move(result);
}

static void DoIVMFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<DoIVMData &>(*data_p.global_state);
	if (data.offset >= 1) {
		return;
	}
	return;
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &db_config = duckdb::DBConfig::GetConfig(instance);

	db_config.AddExtensionOption("ivm_files_path", "path for compiled SQL reference files", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_refresh_mode", "refresh strategy: incremental, full, or auto",
	                             LogicalType::VARCHAR, Value("incremental"));
	db_config.AddExtensionOption("ivm_adaptive_refresh",
	                             "experimental: enable adaptive cost model (when off, always use IVM)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("ivm_cascade_refresh", "cascade mode: off, upstream, downstream, or both",
	                             LogicalType::VARCHAR, Value("downstream"));
	db_config.AddExtensionOption("ivm_adaptive_backoff",
	                             "auto-increase refresh interval when refresh takes longer than the interval",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_disable_daemon", "disable the refresh daemon (for shadow/compile-only DBs)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Per-optimization flags (default: all enabled)
	db_config.AddExtensionOption("ivm_skip_empty_deltas", "skip refresh or join terms when deltas are empty",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_ducklake_nterm",
	                             "use N-term telescoping for DuckLake joins (vs 2^N-1 inclusion-exclusion)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_fk_pruning", "prune inclusion-exclusion join terms using FK constraints",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_skip_aggregate_delete",
	                             "skip zero-row DELETE for grouped aggregates when deltas are insert-only",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_skip_projection_delete",
	                             "skip DELETE and consolidation for projections when deltas are insert-only",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_minmax_incremental", "use GREATEST/LEAST for MIN/MAX when deltas are insert-only",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_having_merge",
	                             "use MERGE for HAVING views (store all groups, VIEW filters) "
	                             "instead of group-recompute",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));

	db_config.AddExtensionOption("ivm_left_join_merge",
	                             "use incremental MERGE for LEFT JOIN aggregates (Larson & Zhou) "
	                             "instead of group-recompute",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_full_outer_merge",
	                             "use incremental MERGE for FULL OUTER JOIN aggregates (Zhang & Larson) "
	                             "instead of group-recompute",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(true));
	db_config.AddExtensionOption("ivm_distinct_aux_state",
	                             "use auxiliary count state for inner DISTINCT under aggregate (DBSP "
	                             "distinct(R)=sgn(R[t]) — emit ±1 only on count transitions across zero) "
	                             "instead of GROUP_RECOMPUTE. Single-source views only in v0; multi-source "
	                             "views fall back to GROUP_RECOMPUTE.",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));

	// Learned cost model
	db_config.AddExtensionOption("ivm_cost_decay",
	                             "decay factor for learned cost model regression (0.0-1.0, higher = slower adaptation)",
	                             LogicalType::DOUBLE, Value::DOUBLE(0.9));

	// View matching (master flag — ALL matcher behavior gated by this).
	// Default false. See `feedback_view_matching_flag` memory note.
	db_config.AddExtensionOption("ivm_enable_view_matching", "enable smart view matching at query time (master flag)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("ivm_predicate_oracle",
	                             "predicate implication oracle: 'syntactic', 'interval' (default), or 'sat' (stub)",
	                             LogicalType::VARCHAR, Value("interval"));
	db_config.AddExtensionOption("ivm_match_strategies",
	                             "allowed strategies (csv): 'tier1','tier2','tier3','partial','full' or 'all'",
	                             LogicalType::VARCHAR, Value("all"));
	db_config.AddExtensionOption("ivm_match_estimate_ttl_ms",
	                             "max staleness (ms) for cached pending-delta-row estimate before refresh",
	                             LogicalType::BIGINT, Value::BIGINT(5000));
	db_config.AddExtensionOption("ivm_match_log_decisions", "log per-query matcher decisions to _duckdb_ivm_match_log",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("ivm_match_log_retention", "max rows per query_hash retained in _duckdb_ivm_match_log",
	                             LogicalType::BIGINT, Value::BIGINT(50));

	Connection con(instance);

	// Migration: add new columns to existing _duckdb_ivm_views tables
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS refresh_interval BIGINT DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS refresh_in_progress BOOLEAN DEFAULT false");

	// Migration: create refresh history table for learned cost model.
	// Silently fails on fresh DB (core_functions not yet loaded → DEFAULT
	// current_timestamp can't resolve). The parser recreates it with the
	// default at first MV creation, so INSERTs (which omit refresh_timestamp)
	// work. This stanza only matters for legacy DBs where the table already
	// exists without the default.
	con.Query("CREATE TABLE IF NOT EXISTS " + string(ivm::HISTORY_TABLE) +
	          " (view_name VARCHAR, refresh_timestamp TIMESTAMP DEFAULT current_timestamp,"
	          " method VARCHAR, ivm_compute_est DOUBLE, ivm_upsert_est DOUBLE,"
	          " recompute_compute_est DOUBLE, recompute_replace_est DOUBLE,"
	          " actual_duration_ms BIGINT,"
	          " PRIMARY KEY(view_name, refresh_timestamp))");

	// View-matching CREATEs first (always succeed), ALTERs after. ALTERs that
	// hit not-yet-existing tables on a fresh DB fail silently — the parser's
	// CREATE TABLE includes these columns directly, so fresh DBs are fine.
	// ALTERs are backward-compat for legacy DBs.
	con.Query("CREATE TABLE IF NOT EXISTS " + string(ivm::MATCH_LOG_TABLE) +
	          " (query_hash UBIGINT,"
	          " log_timestamp TIMESTAMP,"
	          " matched_view VARCHAR,"
	          " chosen_strategy VARCHAR,"
	          " bypass_cost_est DOUBLE,"
	          " chosen_cost_est DOUBLE,"
	          " actual_duration_ms BIGINT,"
	          " PRIMARY KEY (query_hash, log_timestamp))");
	con.Query("CREATE TABLE IF NOT EXISTS " + string(ivm::MV_DEPS_TABLE) +
	          " (parent_view VARCHAR, child_view VARCHAR,"
	          " edge_kind VARCHAR DEFAULT 'direct',"
	          " PRIMARY KEY (parent_view, child_view))");
	con.Query("CREATE TABLE IF NOT EXISTS " + string(ivm::CONSTRAINTS_CACHE_TABLE) +
	          " (table_name VARCHAR, constraint_kind VARCHAR,"
	          " columns_json VARCHAR, referenced_table VARCHAR,"
	          " referenced_columns_json VARCHAR,"
	          " is_trusted BOOLEAN DEFAULT true,"
	          " PRIMARY KEY (table_name, constraint_kind, columns_json))");

	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS signature_hash UBIGINT DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS canonical_plan_blob BLOB DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS output_columns_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS predicate_summary_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS fd_summary_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS source_tables_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS aggregate_decomposition_json VARCHAR DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::VIEWS_TABLE) +
	          " ADD COLUMN IF NOT EXISTS nullified_columns_json VARCHAR DEFAULT NULL");

	con.Query("ALTER TABLE " + string(ivm::DELTA_TABLES_TABLE) +
	          " ADD COLUMN IF NOT EXISTS pending_row_estimate BIGINT DEFAULT NULL");
	con.Query("ALTER TABLE " + string(ivm::DELTA_TABLES_TABLE) +
	          " ADD COLUMN IF NOT EXISTS pending_estimate_ts TIMESTAMP DEFAULT NULL");

	con.Query("ALTER TABLE " + string(ivm::HISTORY_TABLE) +
	          " ADD COLUMN IF NOT EXISTS strategy VARCHAR DEFAULT 'incremental'");

	auto ivm_parser = duckdb::IVMParserExtension();

	auto ivm_rewrite_rule = duckdb::IVMRewriteRule();
	auto ivm_insert_rule = duckdb::IVMInsertRule();

	ParserExtension::Register(db_config, std::move(ivm_parser));
	OptimizerExtension::Register(db_config, std::move(ivm_rewrite_rule));
	OptimizerExtension::Register(db_config, std::move(ivm_insert_rule));

	TableFunction ivm_func("DoIVM", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, DoIVMFunction,
	                       DoIVMBind, DoIVMInit);

	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	ivm_func.name = "DoIVM";
	ivm_func.named_parameters["view_catalog_name"];
	ivm_func.named_parameters["view_schema_name"];
	ivm_func.named_parameters["view_name"];
	CreateTableFunctionInfo ivm_func_info(ivm_func);
	catalog.CreateTableFunction(*con.context, &ivm_func_info);
	con.Commit();

	// Use the locked pragma_function_t variant: generates SQL and executes it under a
	// per-view mutex, preventing concurrent refresh from double-applying deltas.
	auto ivm_options = PragmaFunction::PragmaCall("ivm_options", UpsertDeltaQueriesLocked,
	                                              {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_options);
	auto ivm = PragmaFunction::PragmaCall("ivm", UpsertDeltaQueriesLocked, {LogicalType::VARCHAR});
	loader.RegisterFunction(ivm);
	auto ivm_cost = PragmaFunction::PragmaCall("ivm_cost", IVMCostQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_cost);
	auto ivm_history = PragmaFunction::PragmaCall("ivm_history", IVMCostHistoryQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_history);
	auto ivm_cross_system = PragmaFunction::PragmaCall(
	    "ivm_cross_system", UpsertDeltaQueriesLocked,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_cross_system);

	// PRAGMA ivm_status('view_name') — returns refresh status for a materialized view.
	auto ivm_status = PragmaFunction::PragmaCall(
	    "ivm_status",
	    [](ClientContext &context, const FunctionParameters &parameters) -> string {
		    string view_name = StringValue::Get(parameters.values[0]);
		    Connection con(*context.db.get());
		    IVMMetadata metadata(con);

		    auto interval = metadata.GetRefreshInterval(view_name);
		    string interval_str = interval > 0 ? to_string(interval) : "NULL";

		    // Get the earliest last_update across all delta tables for this view
		    auto last_update_result = con.Query("SELECT MIN(last_update) FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                                        " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "'");
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

		    // Query IVMType directly, same pattern as GetRefreshInterval — returns
		    // 'unknown' on failure rather than throwing. Order MUST mirror IVMType enum
		    // in src/include/core/openivm_constants.hpp.
		    static const char *kTypeNames[] = {
		        "aggregate_group",     "simple_aggregate", "simple_projection", "full_refresh",
		        "aggregate_having",    "window_partition", "group_recompute",   "top_k",
		        "distinct_incremental"};
		    string strategy_str = "'unknown'";
		    auto type_result = con.Query("SELECT type FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
		                                 OpenIVMUtils::EscapeValue(view_name) + "'");
		    if (!type_result->HasError() && type_result->RowCount() > 0 && !type_result->GetValue(0, 0).IsNull()) {
			    auto idx = static_cast<size_t>(type_result->GetValue(0, 0).GetValue<int8_t>());
			    if (idx < sizeof(kTypeNames) / sizeof(kTypeNames[0])) {
				    strategy_str = "'" + string(kTypeNames[idx]) + "'";
			    }
		    }

		    return "SELECT '" + OpenIVMUtils::EscapeValue(view_name) + "' AS view_name, " + interval_str +
		           " AS refresh_interval, " + last_refresh + " AS last_refresh, " + next_refresh +
		           " AS next_refresh, " + status + " AS status, " + effective_interval + " AS effective_interval, " +
		           strategy_str + " AS refresh_strategy;";
	    },
	    {LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_status);

	// PRAGMA ivm_start_daemon — (re)start the daemon on the caller's DB instance.
	auto ivm_start_daemon =
	    PragmaFunction::PragmaCall("ivm_start_daemon",
	                               [](ClientContext &context, const FunctionParameters &) -> string {
		                               if (global_daemon) {
			                               global_daemon->Stop();
		                               }
		                               global_daemon = make_shared_ptr<IVMRefreshDaemon>();
		                               global_daemon->Start(*context.db);
		                               return "SELECT true AS started;";
	                               },
	                               {});
	loader.RegisterFunction(ivm_start_daemon);

	// Start the refresh daemon unless disabled (e.g. shadow/compile-only DBs).
	bool daemon_disabled = false;
	{
		Value disable_val;
		if (con.context->TryGetCurrentSetting("ivm_disable_daemon", disable_val) && !disable_val.IsNull()) {
			daemon_disabled = disable_val.GetValue<bool>();
		}
	}
	if (!daemon_disabled) {
		global_daemon = make_shared_ptr<IVMRefreshDaemon>();
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
