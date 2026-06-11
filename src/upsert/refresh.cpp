#include "upsert/refresh.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/refresh_locks.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/client_data.hpp"
#include "upsert/refresh_cost_model.hpp"
#include "upsert/refresh_internal.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/main/settings.hpp"
#include <chrono>

namespace duckdb {

struct RefreshProfileStep {
	int32_t step_order;
	string step_name;
	int64_t duration_ms;
	string detail;
};

class RefreshProfiler {
public:
	RefreshProfiler(ClientContext &context, string view_name_p)
	    : enabled(false), retention_days(31), view_name(std::move(view_name_p)), next_step(0),
	      total_start(std::chrono::steady_clock::now()) {
		Value profile_val;
		enabled = context.TryGetCurrentSetting("openivm_profile_refresh", profile_val) && !profile_val.IsNull() &&
		          profile_val.GetValue<bool>();
		if (enabled) {
			Value retention_val;
			if (context.TryGetCurrentSetting("openivm_profile_retention_days", retention_val) &&
			    !retention_val.IsNull()) {
				retention_days = std::max<int64_t>(0, retention_val.GetValue<int64_t>());
			}
			auto now = std::chrono::steady_clock::now().time_since_epoch();
			refresh_id = view_name + "_" + to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
		}
	}

	bool Enabled() const {
		return enabled;
	}

	void AddStep(const string &step_name, std::chrono::steady_clock::time_point start,
	             const string &detail = string()) {
		if (!enabled) {
			return;
		}
		auto end = std::chrono::steady_clock::now();
		auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
		steps.push_back({next_step++, step_name, duration_ms, detail});
	}

	void AddMeasuredStep(const string &step_name, int64_t duration_ms, const string &detail = string()) {
		if (!enabled) {
			return;
		}
		steps.push_back({next_step++, step_name, duration_ms, detail});
	}

	void AddTotal() {
		AddStep("total_refresh", total_start);
	}

	void Flush(DatabaseInstance &db) {
		if (!enabled || steps.empty()) {
			return;
		}
		Connection profile_con(db);
		profile_con.Query("DELETE FROM " + string(openivm::PROFILE_TABLE) +
		                  " WHERE profile_timestamp < current_timestamp::TIMESTAMP - INTERVAL '" +
		                  to_string(retention_days) + " days'");
		for (auto &step : steps) {
			auto result = profile_con.Query(
			    "INSERT OR REPLACE INTO " + string(openivm::PROFILE_TABLE) +
			    " (refresh_id, view_name, step_order, step_name, duration_ms, detail) VALUES ('" +
			    SqlUtils::EscapeValue(refresh_id) + "', '" + SqlUtils::EscapeValue(view_name) + "', " +
			    to_string(step.step_order) + ", '" + SqlUtils::EscapeValue(step.step_name) + "', " +
			    to_string(step.duration_ms) + ", '" + SqlUtils::EscapeValue(step.detail) + "')");
			if (result->HasError()) {
				OPENIVM_DEBUG_PRINT("[PROFILE] Failed to record refresh step '%s': %s\n", step.step_name.c_str(),
				                    result->GetError().c_str());
				return;
			}
		}
	}

private:
	bool enabled;
	int64_t retention_days;
	string view_name;
	string refresh_id;
	int32_t next_step;
	std::chrono::steady_clock::time_point total_start;
	vector<RefreshProfileStep> steps;
};

static bool TrySkipEmptyRefresh(ClientContext &context, RefreshMetadata &metadata, Connection &con,
                                const string &view_catalog_name, const string &view_schema_name,
                                const string &view_name, const string &attached_db_catalog_name,
                                const string &attached_db_schema_name, DeltaActivityResult *active_activity);

// Generate and execute refresh SQL for a single view under its per-view lock.
// When openivm_adaptive_refresh is on, also computes a cost estimate before execution
// and records execution history for the learned cost model.
static bool RefreshViewLocked(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                              const string &vn, bool cross_system, const string &attached_db_catalog_name,
                              const string &attached_db_schema_name, bool skip_empty_refresh) {
	RefreshProfiler profiler(context, vn);
	auto lock_start = std::chrono::steady_clock::now();
	ViewLockGuard view_guard(vn);
	// Acquire delta-table locks in sorted order to serialize parallel refreshes that
	// share base tables (e.g. mv_A and mv_B both reading STOCK → both write to
	// `delta_STOCK` inside their transactions → "Conflict on tuple deletion!" when
	// the second tx tries to delete rows the first already processed). Sorting
	// guarantees the same acquisition order across all views, so no deadlock is
	// possible between concurrent refreshes.
	vector<unique_ptr<DeltaLockGuard>> delta_guards;
	Connection probe_con(*context.db.get());
	RefreshMetadata probe_meta(probe_con);
	auto delta_table_names = probe_meta.GetDeltaTables(vn);
	std::sort(delta_table_names.begin(), delta_table_names.end());
	delta_table_names.erase(std::unique(delta_table_names.begin(), delta_table_names.end()), delta_table_names.end());
	for (auto &dt : delta_table_names) {
		delta_guards.push_back(make_uniq<DeltaLockGuard>(dt));
	}
	profiler.AddStep("acquire_locks", lock_start, to_string(delta_table_names.size()) + " delta locks");
	DeltaActivityResult delta_activity;
	DeltaActivityResult *precomputed_delta_activity = nullptr;
	if (skip_empty_refresh) {
		if (TrySkipEmptyRefresh(context, probe_meta, probe_con, view_catalog_name, view_schema_name, vn,
		                        attached_db_catalog_name, attached_db_schema_name, &delta_activity)) {
			profiler.AddTotal();
			profiler.Flush(*context.db.get());
			return true;
		}
		if (!delta_activity.active_delta_table_names.empty() || delta_activity.requires_full_refresh) {
			precomputed_delta_activity = &delta_activity;
		}
	}
	// Track whether we're inside an open exec_con transaction so any exception path can
	// rollback cleanly. Without explicit rollback, a throw mid-transaction relies on the
	// Connection destructor, which can leak uncommitted writes into the WAL under some
	// failure modes (e.g. rebinding errors thrown by Query itself, not reported as
	// HasError()). Rollback-then-throw keeps the WAL clean and leaves the DB valid.
	Connection exec_con(*context.db.get());
	bool tx_open = false;
	try {
		bool adaptive_refresh = SqlUtils::GetBoolSetting(context, "openivm_adaptive_refresh", false);
		RefreshCostEstimate cost_estimate = {};

		// For cross_system (DuckLake) MVs, split the refresh SQL into data ops (dl catalog)
		// and metadata ops (physical-default catalog) to avoid the cross-catalog write error.
		string meta_pre_sql, meta_post_sql;
		RefreshCompileProfile compile_profile;
		auto generate_start = std::chrono::steady_clock::now();
		string sql =
		    GenerateRefreshSQL(context, view_catalog_name, view_schema_name, vn, cross_system, attached_db_catalog_name,
		                       attached_db_schema_name, cross_system ? &meta_pre_sql : nullptr,
		                       cross_system ? &meta_post_sql : nullptr, profiler.Enabled() ? &compile_profile : nullptr,
		                       precomputed_delta_activity, adaptive_refresh ? &cost_estimate : nullptr);
		for (const auto &step : compile_profile.steps) {
			profiler.AddMeasuredStep(step.step_name, step.duration_ms, step.detail);
		}
		profiler.AddStep("generate_refresh_sql", generate_start,
		                 "sql_bytes=" + to_string(sql.size()) + ", meta_pre_bytes=" + to_string(meta_pre_sql.size()) +
		                     ", meta_post_bytes=" + to_string(meta_post_sql.size()));

		// IVM-generated SQL can nest deeply for multi-table joins + CTEs (N-term telescoping
		// over 7+ tables produces hundreds of chained projections). Lift the default 1000
		// expression-depth limit so the binder doesn't reject legitimate generated plans.
		exec_con.Query("SET max_expression_depth = 10000");
		// The generated refresh SQL already contains an explicit decorrelated plan. DuckDB's
		// deliminator can recurse through very deep stress-query CTE/subquery expansions and
		// overflow in the optimizer before the refresh query runs.
		exec_con.Query("SET disabled_optimizers='" + string(openivm::REFRESH_DISABLED_OPTIMIZERS) + "'");
		// Refreshes update relational MV state; physical insertion order is not part of
		// the contract. Let DuckDB avoid large order-preservation buffers for big
		// INSERT/CREATE TABLE style refresh plans.
		exec_con.Query("SET preserve_insertion_order=false");
		// Refresh SQL uses fully qualified internal data/delta names. DuckLake-targeted
		// MVs write those objects in DuckLake; native MVs keep them in the physical DB.
		OPENIVM_DEBUG_PRINT("[UPSERT] Executing refresh SQL:\n%s\n", sql.c_str());

		// Wrap the entire refresh in a transaction so that a failed refresh leaves the MV
		// and delta tables in a clean state (atomically rolled back). The refresh_in_progress
		// flag is inside the same transaction, so it also rolls back on failure — WAL recovery
		// handles the crash case automatically.
		// DuckLake (cross_system) skips this: DuckDB forbids writing to two attached databases
		// (metadata catalog + dl) within a single transaction. Instead, we run metadata ops on
		// a separate meta_con (physical-default catalog) and data ops on exec_con (dl catalog).
		if (!cross_system) {
			exec_con.BeginTransaction();
			tx_open = true;
		} else if (!meta_pre_sql.empty()) {
			auto meta_pre_start = std::chrono::steady_clock::now();
			Connection meta_con(*context.db.get());
			auto meta_result = meta_con.Query(meta_pre_sql);
			if (meta_result->HasError()) {
				throw Exception(ExceptionType::EXECUTOR,
				                "IVM refresh of '" + vn + "' failed before data refresh: " + meta_result->GetError());
			}
			profiler.AddStep("metadata_pre_sql", meta_pre_start, "bytes=" + to_string(meta_pre_sql.size()));
		}
		auto start = std::chrono::steady_clock::now();
		unique_ptr<MaterializedQueryResult> result;
		idx_t executed_statement_count = 1;
		if (profiler.Enabled()) {
			// Diagnostic mode runs the generated batch one statement at a time so
			// the profile table can show which refresh operation dominates runtime.
			auto statements = SqlUtils::SplitSQLStatements(sql);
			executed_statement_count = statements.size();
			if (statements.empty()) {
				result = exec_con.Query(sql);
			}
			for (idx_t stmt_idx = 0; stmt_idx < statements.size(); stmt_idx++) {
				auto stmt_start = std::chrono::steady_clock::now();
				result = exec_con.Query(statements[stmt_idx]);
				profiler.AddStep("execute_refresh_sql_stmt", stmt_start,
				                 "statement=" + to_string(stmt_idx + 1) + "/" + to_string(statements.size()) +
				                     ", bytes=" + to_string(statements[stmt_idx].size()) +
				                     ", sql=" + SqlUtils::SQLStatementPreview(statements[stmt_idx]));
				if (result->HasError()) {
					break;
				}
			}
		} else {
			result = exec_con.Query(sql);
		}
		auto end = std::chrono::steady_clock::now();
		profiler.AddStep("execute_refresh_sql", start,
		                 "bytes=" + to_string(sql.size()) + ", statements=" + to_string(executed_statement_count));

		if (result->HasError()) {
			if (tx_open) {
				exec_con.Rollback();
				tx_open = false;
			}
			// Use a regular Exception (not InternalException) — a failed refresh is
			// recoverable (e.g. transaction-conflict on a shared delta table from two
			// parallel refreshes). InternalException causes DuckDB to flag the whole
			// database as invalidated, forcing a restart. We've already rolled back, so
			// the DB is in a clean state; the next refresh attempt should succeed.
			throw Exception(ExceptionType::EXECUTOR, "IVM refresh of '" + vn + "' failed: " + result->GetError());
		}
		if (tx_open) {
			exec_con.Commit();
			tx_open = false;
		} else if (cross_system && !meta_post_sql.empty()) {
			auto meta_post_start = std::chrono::steady_clock::now();
			if (meta_post_sql.find(DUCKLAKE_SNAPSHOT_PLACEHOLDER) != string::npos) {
				// DuckLake can keep read snapshot state on the connection that compiled
				// the data refresh. Read the post-refresh watermark through a fresh
				// connection so we do not persist an old snapshot and replay deltas.
				Connection snap_con(*context.db.get());
				RefreshMetadata snap_metadata(snap_con);
				auto catalogs = snap_con.Query("SELECT database_name FROM duckdb_databases() WHERE type = 'ducklake'");
				if (catalogs->HasError()) {
					throw Exception(ExceptionType::EXECUTOR,
					                "IVM refresh of '" + vn +
					                    "' failed: could not list DuckLake catalogs after data "
					                    "refresh: " +
					                    catalogs->GetError());
				}
				for (idx_t row = 0; row < catalogs->RowCount(); row++) {
					if (catalogs->GetValue(0, row).IsNull()) {
						continue;
					}
					string dl_catalog = catalogs->GetValue(0, row).ToString();
					string placeholder = DuckLakeSnapshotPlaceholder(dl_catalog);
					if (meta_post_sql.find(placeholder) == string::npos) {
						continue;
					}
					auto snapshot_id = snap_metadata.GetCurrentDuckLakeSnapshot(dl_catalog);
					if (snapshot_id < 0) {
						throw Exception(ExceptionType::EXECUTOR, "IVM refresh of '" + vn +
						                                             "' failed: could not read DuckLake snapshot for "
						                                             "catalog '" +
						                                             dl_catalog + "' after data refresh");
					}
					meta_post_sql = StringUtil::Replace(meta_post_sql, placeholder, to_string(snapshot_id));
				}
				if (meta_post_sql.find(DUCKLAKE_SNAPSHOT_PLACEHOLDER) != string::npos) {
					throw Exception(ExceptionType::EXECUTOR,
					                "IVM refresh of '" + vn +
					                    "' failed: unresolved DuckLake snapshot placeholder after data refresh");
				}
			}
			Connection meta_con(*context.db.get());
			auto meta_result = meta_con.Query(meta_post_sql);
			if (meta_result->HasError()) {
				throw Exception(ExceptionType::EXECUTOR,
				                "IVM refresh of '" + vn + "' failed after data refresh: " + meta_result->GetError());
			}
			profiler.AddStep("metadata_post_sql", meta_post_start, "bytes=" + to_string(meta_post_sql.size()));
		}

		// Record execution history for the learned cost model.
		if (!cost_estimate.strategy_label.empty()) {
			auto history_start = std::chrono::steady_clock::now();
			auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
			// Determine which method was used. Priority:
			//   1) `openivm_refresh_mode = 'full'` overrides everything → "full".
			//   2) If the adaptive cost model picked full recompute, record "full".
			//   3) Otherwise record the selected refresh strategy label.
			string method = cost_estimate.strategy_label.empty() ? "incremental" : cost_estimate.strategy_label;
			if (cost_estimate.ShouldRecompute()) {
				method = "full";
			}
			Value mode_val;
			if (context.TryGetCurrentSetting("openivm_refresh_mode", mode_val) && !mode_val.IsNull()) {
				auto mode = StringUtil::Lower(mode_val.ToString());
				if (mode == "full") {
					method = "full";
				}
			}

			RefreshMetadata(exec_con).RecordRefreshHistory(
			    vn, method, cost_estimate.incremental_compute, cost_estimate.incremental_upsert,
			    cost_estimate.recompute_compute, cost_estimate.recompute_replace, duration_ms);
			OPENIVM_DEBUG_PRINT("[HISTORY] Recorded: view=%s, method=%s, duration=%ldms\n", vn.c_str(), method.c_str(),
			                    (long)duration_ms);
			profiler.AddStep("record_refresh_history", history_start, method);
		}
		profiler.AddTotal();
		profiler.Flush(*context.db.get());
		return false;
	} catch (...) {
		// Ensure the transaction is rolled back before we propagate the exception.
		// This covers the case where Query() itself threw (vs returning HasError) —
		// without this, the tx would stay open until exec_con's destructor runs,
		// possibly leaving dirty WAL entries.
		if (tx_open) {
			try {
				exec_con.Rollback();
			} catch (...) {
				// Rollback-inside-rollback failure is benign: the Connection destructor
				// will still clean up. Swallow so we don't mask the original error.
			}
			tx_open = false;
		}
		profiler.AddTotal();
		profiler.Flush(*context.db.get());
		throw;
	}
}

static bool SkipEmptyDeltasEnabled(ClientContext &context) {
	return SqlUtils::GetBoolSetting(context, "openivm_skip_empty_deltas", true);
}

struct AuxCatalogTarget {
	string catalog_name;
	string schema_name;
};

static AuxCatalogTarget ResolveAuxCatalogTarget(RefreshMetadata &metadata, Connection &con,
                                                const string &view_catalog_name, const string &view_schema_name) {
	string default_db;
	string default_schema = "main";
	auto db_res = con.Query("SELECT current_database()");
	if (!db_res->HasError() && db_res->RowCount() > 0 && !db_res->GetValue(0, 0).IsNull()) {
		default_db = db_res->GetValue(0, 0).ToString();
	}
	auto schema_res = con.Query("SELECT current_schema()");
	if (!schema_res->HasError() && schema_res->RowCount() > 0 && !schema_res->GetValue(0, 0).IsNull()) {
		default_schema = schema_res->GetValue(0, 0).ToString();
	}

	AuxCatalogTarget target;
	target.catalog_name = view_catalog_name.empty() ? default_db : view_catalog_name;
	target.schema_name = view_schema_name.empty() ? default_schema : view_schema_name;
	bool target_is_ducklake = metadata.IsDuckLakeCatalog(view_catalog_name);
	if (!target_is_ducklake && !default_db.empty() && default_db != "memory" && !view_catalog_name.empty() &&
	    view_catalog_name != default_db) {
		target.catalog_name = default_db;
		target.schema_name = default_schema;
	}
	return target;
}

static bool TrySkipEmptyRefresh(ClientContext &context, RefreshMetadata &metadata, Connection &con,
                                const string &view_catalog_name, const string &view_schema_name,
                                const string &view_name, const string &attached_db_catalog_name,
                                const string &attached_db_schema_name, DeltaActivityResult *active_activity) {
	if (!SkipEmptyDeltasEnabled(context)) {
		return false;
	}
	if (metadata.GetViewType(view_name) == RefreshType::FULL_REFRESH) {
		return false;
	}
	auto aux_target = ResolveAuxCatalogTarget(metadata, con, view_catalog_name, view_schema_name);
	if (metadata.AuxStateNeedsRepair(view_name, aux_target.catalog_name, aux_target.schema_name)) {
		return false;
	}

	auto view_query_sql = metadata.GetViewQuery(view_name);
	auto delta_tables = metadata.GetDeltaTables(view_name);
	auto activity = BuildDeltaActivityResult(metadata, con, view_name, view_query_sql, delta_tables, view_catalog_name,
	                                         view_schema_name, attached_db_catalog_name, attached_db_schema_name);
	if (!activity.active_delta_table_names.empty() || activity.requires_full_refresh) {
		if (active_activity) {
			*active_activity = std::move(activity);
		}
		return false;
	}

	for (auto &advance : activity.ducklake_snapshot_advances) {
		if (advance.snapshot_id >= 0) {
			metadata.UpdateDuckLakeRefreshMetadata(view_name, advance.table_name, advance.snapshot_id);
		}
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] All delta tables empty — skipping refresh for '%s'\n", view_name.c_str());
	return true;
}

void UpsertDeltaQueriesLocked(ClientContext &context, const FunctionParameters &parameters) {
	OPENIVM_DEBUG_PRINT("[UPSERT] UpsertDeltaQueriesLocked START\n");
	// PRAGMA refresh refreshes relational state, not ordered output. Force the
	// OpenIVM entry point onto DuckDB's unordered execution mode even if this
	// connection previously had a local preserve_insertion_order=true setting.
	ClientConfig::GetConfig(context).user_settings.SetUserSetting(PreserveInsertionOrderSetting::SettingIndex,
	                                                              Value::BOOLEAN(false));
	string view_catalog_name;
	string view_schema_name;
	string attached_db_catalog_name;
	string attached_db_schema_name;
	string view_name;
	bool cross_system = false;

	Connection con(*context.db.get());

	if (parameters.values.size() == 3) {
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		view_name = StringValue::Get(parameters.values[2]);
	} else if (parameters.values.size() == 5) {
		view_catalog_name = StringValue::Get(parameters.values[0]);
		view_schema_name = StringValue::Get(parameters.values[1]);
		attached_db_catalog_name = StringValue::Get(parameters.values[2]);
		attached_db_schema_name = StringValue::Get(parameters.values[3]);
		view_name = StringValue::Get(parameters.values[4]);
		cross_system = true;
	} else {
		auto resolved = ResolveViewCatalogFromContext(context, con, StringValue::Get(parameters.values[0]));
		view_catalog_name = resolved.view_catalog_name;
		view_schema_name = resolved.view_schema_name;
		view_name = StringValue::Get(parameters.values[0]);
		cross_system = resolved.cross_system;
		OPENIVM_DEBUG_PRINT("[UPSERT] Resolved catalog='%s', schema='%s', cross_system=%d\n", view_catalog_name.c_str(),
		                    view_schema_name.c_str(), cross_system ? 1 : 0);
	}

	// cross_system detection: the view's catalog differs from the fresh connection's physical
	// default. Metadata tables (openivm_views etc.) live in the physical default; data/view
	// tables live in view_catalog_name. DuckDB forbids cross-catalog writes in one transaction,
	// so RefreshViewLocked must split the refresh SQL into data ops and metadata ops.
	if (!view_catalog_name.empty()) {
		Connection probe(*context.db.get());
		string probe_default;
		auto res = probe.Query("SELECT current_database()");
		if (!res->HasError() && res->RowCount() > 0) {
			probe_default = res->GetValue(0, 0).ToString();
		}
		if (!probe_default.empty() && view_catalog_name != probe_default) {
			cross_system = true;
		}
	}

	// Check cascade mode
	string cascade_mode = "downstream";
	Value cascade_val;
	if (context.TryGetCurrentSetting("openivm_cascade_refresh", cascade_val) && !cascade_val.IsNull()) {
		cascade_mode = StringUtil::Lower(cascade_val.ToString());
	}

	RefreshMetadata metadata(con);

	// Each view is generated + executed under its own per-view lock.
	// This ensures cascaded views are also protected from concurrent refresh.

	// Upstream cascade: refresh ancestors first (this may populate our delta tables).
	if (cascade_mode == "upstream" || cascade_mode == "both") {
		auto upstream = metadata.GetUpstreamViews(view_name);
		for (auto &dep : upstream) {
			auto dep_location = ResolveViewLocation(con, dep, view_catalog_name, view_schema_name);
			RefreshViewLocked(context, dep_location.catalog_name, dep_location.schema_name, dep,
			                  dep_location.cross_system, attached_db_catalog_name, attached_db_schema_name,
			                  /*skip_empty_refresh=*/true);
		}
	}

	// Check for refresh hooks (custom SQL to run before/after/instead of IVM)
	string hook_sql;
	string hook_mode;
	{
		auto hook_r = con.Query("SELECT hook_sql, mode FROM openivm_refresh_hooks WHERE view_name = '" +
		                        SqlUtils::EscapeValue(view_name) + "'");
		if (!hook_r->HasError() && hook_r->RowCount() > 0) {
			hook_sql = hook_r->GetValue(0, 0).ToString();
			hook_mode = StringUtil::Lower(hook_r->GetValue(1, 0).ToString());
		}
	}
	bool has_refresh_hook = !hook_sql.empty();

	// Hook-bearing refreshes keep the old pre-hook empty skip semantics. Hook-free refreshes
	// compute the same delta activity under the view lock and reuse it during SQL generation.
	if (has_refresh_hook && TrySkipEmptyRefresh(context, metadata, con, view_catalog_name, view_schema_name, view_name,
	                                            attached_db_catalog_name, attached_db_schema_name, nullptr)) {
		return;
	}

	if (!hook_sql.empty() && hook_mode == "before") {
		auto hr = con.Query(hook_sql);
		if (hr->HasError()) {
			Printer::Print("Warning: before-hook for '" + view_name + "' failed: " + hr->GetError());
		}
	}

	if (hook_mode != "replace") {
		if (RefreshViewLocked(context, view_catalog_name, view_schema_name, view_name, cross_system,
		                      attached_db_catalog_name, attached_db_schema_name, !has_refresh_hook)) {
			return;
		}
	}

	if (!hook_sql.empty() && (hook_mode == "after" || hook_mode == "replace")) {
		auto hr = con.Query(hook_sql);
		if (hr->HasError()) {
			Printer::Print("Warning: " + hook_mode + "-hook for '" + view_name + "' failed: " + hr->GetError());
		}
	}

	// Downstream cascade: refresh dependents after
	if (cascade_mode == "downstream" || cascade_mode == "both") {
		auto downstream = metadata.GetDownstreamViews(view_name);
		for (auto &dep : downstream) {
			auto dep_location = ResolveViewLocation(con, dep, view_catalog_name, view_schema_name);
			RefreshViewLocked(context, dep_location.catalog_name, dep_location.schema_name, dep,
			                  dep_location.cross_system, attached_db_catalog_name, attached_db_schema_name,
			                  /*skip_empty_refresh=*/true);
		}
	}
}

} // namespace duckdb
