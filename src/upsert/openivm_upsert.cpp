#include "upsert/openivm_upsert.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_refresh_locks.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/main/client_data.hpp"
#include "core/openivm_utils.hpp"
#include "upsert/openivm_compile_upsert.hpp"
#include "upsert/openivm_cost_model.hpp"
#include "lpts_pipeline.hpp"
#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include <chrono>

namespace duckdb {

static string BuildRecomputeQuery(IVMMetadata &metadata, const string &view_name, const string &view_query_sql,
                                  bool cross_system, const string &attached_catalog = "",
                                  const string &attached_schema = "", const string &catalog_prefix = "") {
	string qdt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(view_name));
	string query = "DELETE FROM " + qdt + ";\n";
	query += "INSERT INTO " + qdt + " " + view_query_sql + ";\n\n";

	metadata.UpdateTimestamp(view_name);
	string update_ts = "UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_update = now() WHERE view_name = '" +
	                   OpenIVMUtils::EscapeValue(view_name) + "';\n";

	string delta_cleanup;
	auto delta_tables = metadata.GetDeltaTables(view_name);
	for (auto &dt : delta_tables) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			continue;
		}
		string resolved = dt;
		if (cross_system) {
			resolved = attached_catalog + "." + attached_schema + "." + dt;
		}
		delta_cleanup += IVMMetadata::BuildDeltaCleanupSQL(resolved, dt);
	}

	return query + update_ts + "\n" + delta_cleanup;
}

// Generate refresh SQL for a single view (no cascade logic).
static string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name,
                                 const string &view_schema_name, const string &view_name, bool cross_system,
                                 const string &attached_db_catalog_name, const string &attached_db_schema_name);

// Generate and execute refresh SQL for a single view under its per-view lock.
// When ivm_adaptive_refresh is on, also computes a cost estimate before execution
// and records execution history for the learned cost model.
static void RefreshViewLocked(ClientContext &context, const string &view_catalog_name, const string &view_schema_name,
                              const string &vn, bool cross_system, const string &attached_db_catalog_name,
                              const string &attached_db_schema_name) {
	IVMRefreshLocks::LockView(vn);
	try {
		// Check if adaptive cost model is active — if so, compute estimate for history recording.
		bool record_history = false;
		IVMCostEstimate cost_estimate = {};
		Value adaptive_val;
		if (context.TryGetCurrentSetting("ivm_adaptive_refresh", adaptive_val) && !adaptive_val.IsNull() &&
		    adaptive_val.GetValue<bool>()) {
			// Compute cost estimate before refresh (for history recording).
			// GenerateRefreshSQL also computes this when adaptive is on, but we need
			// the estimate here to record alongside the actual execution time.
			Connection cost_con(*context.db.get());
			cost_con.BeginTransaction();
			IVMMetadata cost_meta(cost_con);
			auto vq = cost_meta.GetViewQuery(vn);
			if (!vq.empty()) {
				Parser cp;
				cp.ParseQuery(vq);
				Planner pl(*cost_con.context);
				pl.CreatePlan(cp.statements[0]->Copy());
				Optimizer opt(*pl.binder, *cost_con.context);
				auto plan = opt.Optimize(std::move(pl.plan));
				cost_estimate = EstimateIVMCost(*cost_con.context, *plan, vn);
				record_history = true;
			}
			cost_con.Rollback();
		}

		string sql = GenerateRefreshSQL(context, view_catalog_name, view_schema_name, vn, cross_system,
		                                attached_db_catalog_name, attached_db_schema_name);
		Connection exec_con(*context.db.get());
		OPENIVM_DEBUG_PRINT("[UPSERT] Executing refresh SQL:\n%s\n", sql.c_str());

		auto start = std::chrono::steady_clock::now();
		auto result = exec_con.Query(sql);
		auto end = std::chrono::steady_clock::now();

		if (result->HasError()) {
			IVMMetadata(exec_con).SetRefreshInProgress(vn, false);
			throw InternalException("IVM refresh of '" + vn + "' failed: " + result->GetError());
		}

		// Record execution history for the learned cost model.
		if (record_history) {
			auto duration_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
			// Determine which method was used: if the cost model said recompute AND adaptive is on,
			// then full; otherwise incremental (unless force_full, but that's already in the SQL).
			string method = cost_estimate.ShouldRecompute() ? "full" : "incremental";

			// Check if force_full_refresh overrides (ivm_refresh_mode = 'full')
			Value mode_val;
			if (context.TryGetCurrentSetting("ivm_refresh_mode", mode_val) && !mode_val.IsNull()) {
				auto mode = StringUtil::Lower(mode_val.ToString());
				if (mode == "full") {
					method = "full";
				}
			}

			IVMMetadata(exec_con).RecordRefreshHistory(vn, method, cost_estimate.ivm_compute, cost_estimate.ivm_upsert,
			                                           cost_estimate.recompute_compute, cost_estimate.recompute_replace,
			                                           duration_ms);
			OPENIVM_DEBUG_PRINT("[HISTORY] Recorded: view=%s, method=%s, duration=%ldms\n", vn.c_str(), method.c_str(),
			                    (long)duration_ms);
		}
	} catch (...) {
		IVMRefreshLocks::UnlockView(vn);
		throw;
	}
	IVMRefreshLocks::UnlockView(vn);
}

void UpsertDeltaQueriesLocked(ClientContext &context, const FunctionParameters &parameters) {
	OPENIVM_DEBUG_PRINT("[UPSERT] UpsertDeltaQueriesLocked START\n");
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
		auto &search_path = ClientData::Get(context).catalog_search_path;
		auto default_entry = search_path->GetDefault();
		view_catalog_name = default_entry.catalog;
		view_schema_name = default_entry.schema;
		view_name = StringValue::Get(parameters.values[0]);

		OPENIVM_DEBUG_PRINT("[UPSERT] Default catalog=%s, schema=%s, view=%s\n", view_catalog_name.c_str(),
		                    view_schema_name.c_str(), view_name.c_str());
		// If the view doesn't exist in the default catalog, search attached catalogs.
		// This handles DuckLake MVs created as "dl.mv_name" where the view lives in "dl".
		{
			QueryErrorContext err_ctx;
			auto entry = Catalog::GetEntry(context, view_catalog_name, view_schema_name,
			                               EntryLookupInfo(CatalogType::VIEW_ENTRY, view_name, err_ctx),
			                               OnEntryNotFound::RETURN_NULL);
			OPENIVM_DEBUG_PRINT("[UPSERT] Default catalog entry: %s\n", entry ? "found" : "not found");
			if (!entry) {
				auto &db_manager = DatabaseManager::Get(context);
				auto databases = db_manager.GetDatabases(context);
				OPENIVM_DEBUG_PRINT("[UPSERT] Searching %zu databases...\n", databases.size());
				for (auto &db : databases) {
					auto &cat_name = db->GetName();
					OPENIVM_DEBUG_PRINT("[UPSERT] Checking catalog '%s'...\n", cat_name.c_str());
					auto found = Catalog::GetEntry(context, cat_name, DEFAULT_SCHEMA,
					                               EntryLookupInfo(CatalogType::VIEW_ENTRY, view_name, err_ctx),
					                               OnEntryNotFound::RETURN_NULL);
					if (found) {
						view_catalog_name = cat_name;
						view_schema_name = DEFAULT_SCHEMA;
						OPENIVM_DEBUG_PRINT("[UPSERT] Found view in catalog '%s'\n", cat_name.c_str());
						break;
					}
				}
			}
			OPENIVM_DEBUG_PRINT("[UPSERT] Resolved catalog='%s', schema='%s'\n", view_catalog_name.c_str(),
			                    view_schema_name.c_str());
		}
	}

	// Check cascade mode
	string cascade_mode = "downstream";
	Value cascade_val;
	if (context.TryGetCurrentSetting("ivm_cascade_refresh", cascade_val) && !cascade_val.IsNull()) {
		cascade_mode = StringUtil::Lower(cascade_val.ToString());
	}

	IVMMetadata metadata(con);

	// Each view is generated + executed under its own per-view lock.
	// This ensures cascaded views are also protected from concurrent refresh.

	// Upstream cascade: refresh ancestors first (this may populate our delta tables).
	if (cascade_mode == "upstream" || cascade_mode == "both") {
		auto upstream = metadata.GetUpstreamViews(view_name);
		for (auto &dep : upstream) {
			RefreshViewLocked(context, view_catalog_name, view_schema_name, dep, cross_system, attached_db_catalog_name,
			                  attached_db_schema_name);
		}
	}

	// Early exit: skip refresh if all delta tables are empty.
	// Placed AFTER upstream cascade so that upstream refreshes have a chance to populate
	// our delta tables before we check.
	Value skip_empty_val;
	bool skip_empty_enabled = true;
	if (context.TryGetCurrentSetting("ivm_skip_empty_deltas", skip_empty_val) && !skip_empty_val.IsNull()) {
		skip_empty_enabled = skip_empty_val.GetValue<bool>();
	}
	if (skip_empty_enabled) {
		auto view_type = metadata.GetViewType(view_name);
		if (view_type != IVMType::FULL_REFRESH) {
			auto delta_tables = metadata.GetDeltaTables(view_name);
			bool all_empty = true;

			// For DuckLake tables, get the current snapshot ID once (shared across all tables).
			int64_t ducklake_current_snap = -1;
			bool ducklake_snap_queried = false;

			for (auto &dt : delta_tables) {
				if (metadata.IsDuckLakeTable(view_name, dt)) {
					// DuckLake tables: compare last_snapshot_id with current snapshot.
					if (!ducklake_snap_queried) {
						auto cur_snap_result = con.Query("SELECT id FROM " + view_catalog_name + ".current_snapshot()");
						if (!cur_snap_result->HasError() && cur_snap_result->RowCount() > 0) {
							ducklake_current_snap = cur_snap_result->GetValue(0, 0).GetValue<int64_t>();
						}
						ducklake_snap_queried = true;
					}
					if (ducklake_current_snap < 0) {
						// Can't determine current snapshot — conservatively assume changes.
						all_empty = false;
						break;
					}
					auto last_snap = metadata.GetLastSnapshotId(view_name, dt);
					if (last_snap != ducklake_current_snap) {
						all_empty = false;
						break;
					}
					continue;
				}
				auto count_result = con.Query("SELECT COUNT(*) FROM " + OpenIVMUtils::QuoteIdentifier(dt));
				if (!count_result->HasError() && count_result->GetValue(0, 0).GetValue<int64_t>() > 0) {
					all_empty = false;
					break;
				}
			}
			if (all_empty) {
				OPENIVM_DEBUG_PRINT("[UPSERT] All delta tables empty — skipping refresh for '%s'\n", view_name.c_str());
				return;
			}
		}
	} // skip_empty_enabled

	// Check for refresh hooks (custom SQL to run before/after/instead of IVM)
	string hook_sql;
	string hook_mode;
	{
		auto hook_r = con.Query("SELECT hook_sql, mode FROM _duckdb_ivm_refresh_hooks WHERE view_name = '" +
		                        OpenIVMUtils::EscapeValue(view_name) + "'");
		if (!hook_r->HasError() && hook_r->RowCount() > 0) {
			hook_sql = hook_r->GetValue(0, 0).ToString();
			hook_mode = StringUtil::Lower(hook_r->GetValue(1, 0).ToString());
		}
	}

	if (!hook_sql.empty() && hook_mode == "before") {
		auto hr = con.Query(hook_sql);
		if (hr->HasError()) {
			Printer::Print("Warning: before-hook for '" + view_name + "' failed: " + hr->GetError());
		}
	}

	if (hook_mode != "replace") {
		RefreshViewLocked(context, view_catalog_name, view_schema_name, view_name, cross_system,
		                  attached_db_catalog_name, attached_db_schema_name);
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
			RefreshViewLocked(context, view_catalog_name, view_schema_name, dep, cross_system, attached_db_catalog_name,
			                  attached_db_schema_name);
		}
	}
}

static string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name,
                                 const string &view_schema_name, const string &view_name, bool cross_system,
                                 const string &attached_db_catalog_name, const string &attached_db_schema_name) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();
	Connection con(*context.db.get());
	// Catalog-qualified prefix for SQL references (e.g. "dl.main." or "" for default).
	// Only add the prefix when the catalog is non-default (e.g. DuckLake attached DB).
	string catalog_prefix;
	if (!view_catalog_name.empty() && view_catalog_name != "memory" && view_catalog_name != INVALID_CATALOG) {
		catalog_prefix = view_catalog_name + "." + view_schema_name + ".";
	}
	// Bare table names for catalog lookups; qualified names for SQL
	string data_table_bare = IVMTableNames::DataTableName(view_name);
	string data_table = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(data_table_bare);

	// Look up delta view and index.
	OPENIVM_DEBUG_PRINT("[UPSERT] Looking up delta view '%s' in catalog '%s.%s'\n",
	                    OpenIVMUtils::DeltaName(view_name).c_str(), view_catalog_name.c_str(),
	                    view_schema_name.c_str());
	optional_ptr<TableCatalogEntry> delta_view_catalog_entry;
	optional_ptr<CatalogEntry> index_delta_view_catalog_entry;
	if (catalog_prefix.empty()) {
		// Standard catalog: use Catalog API directly
		con.BeginTransaction();
		delta_view_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    *con.context, view_catalog_name, view_schema_name, OpenIVMUtils::DeltaName(view_name),
		    OnEntryNotFound::THROW_EXCEPTION, error_context);
		index_delta_view_catalog_entry = Catalog::GetEntry(
		    *con.context, view_catalog_name, view_schema_name,
		    EntryLookupInfo(CatalogType::INDEX_ENTRY, data_table_bare + ivm::INDEX_SUFFIX, error_context),
		    OnEntryNotFound::RETURN_NULL);
		con.Rollback();
	}
	// DuckLake: skip Catalog API (requires DuckLake transaction). Column names and
	// group columns come from metadata instead. delta_view_catalog_entry stays null.

	// IVMMetadata uses auto-commit queries (no explicit transaction needed)
	IVMMetadata metadata(con);
	auto view_query_sql = metadata.GetViewQuery(view_name);
	if (view_query_sql.empty()) {
		throw ParserException("View not found! Please call IVM with a materialized view.");
	}
	IVMType view_query_type = metadata.GetViewType(view_name);
	OPENIVM_DEBUG_PRINT("[UPSERT] View: %s, Type: %d, Query: %s\n", view_name.c_str(), (int)view_query_type,
	                    view_query_sql.c_str());

	// Crash recovery: if a previous refresh was interrupted (process died between MERGE and
	// last_update), the flag is still true. Recover via full recompute to avoid double-applying deltas.
	{
		auto flag_result = con.Query("SELECT refresh_in_progress FROM " + string(ivm::VIEWS_TABLE) +
		                             " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "'");
		if (!flag_result->HasError() && flag_result->RowCount() > 0 && !flag_result->GetValue(0, 0).IsNull() &&
		    flag_result->GetValue(0, 0).GetValue<bool>()) {
			Printer::Print("Warning: recovering '" + view_name + "' from interrupted refresh via full recompute.");
			metadata.SetRefreshInProgress(view_name, false);
			return BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
			                           attached_db_schema_name, catalog_prefix);
		}
	}

	// AVG, MIN, MAX, HAVING use group-recompute strategy (not decomposable as simple deltas).
	// HAVING needs recompute because groups may enter/leave the result set after aggregate changes.
	// MIN, MAX use group-recompute. AVG is decomposed to SUM+COUNT by the parser (fully incremental).
	// HAVING needs recompute because groups may enter/leave the result set.
	// Aggregates over LEFT JOIN sources also need group-recompute: SUM(NULL) != SUM(0)
	// Read MIN/MAX and LEFT JOIN flags from metadata (set at CREATE MV time).
	// These determine whether to use incremental MERGE or full group-recompute.
	bool source_has_left_join = metadata.HasLeftJoin(view_name);
	bool has_minmax =
	    metadata.HasMinMax(view_name) || view_query_type == IVMType::AGGREGATE_HAVING || source_has_left_join;

	// Check ivm_refresh_mode: 'full' forces full recompute, skipping the IVM pipeline.
	Value refresh_mode_val;
	bool force_full_refresh = false;
	if (context.TryGetCurrentSetting("ivm_refresh_mode", refresh_mode_val) && !refresh_mode_val.IsNull()) {
		auto mode = StringUtil::Lower(refresh_mode_val.ToString());
		if (mode == "full") {
			force_full_refresh = true;
		}
	}

	if (force_full_refresh || view_query_type == IVMType::FULL_REFRESH) {
		return BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
		                           attached_db_schema_name, catalog_prefix);
	}

	// Adaptive cost model (experimental): estimate IVM vs full recompute cost.
	// Gated by ivm_adaptive_refresh setting (default off — always use IVM).
	Value ivm_adaptive_val;
	bool ivm_adaptive = false;
	if (context.TryGetCurrentSetting("ivm_adaptive_refresh", ivm_adaptive_val) && !ivm_adaptive_val.IsNull()) {
		ivm_adaptive = ivm_adaptive_val.GetValue<bool>();
	}
	if (ivm_adaptive) {
		con.BeginTransaction();
		Parser cost_parser;
		cost_parser.ParseQuery(view_query_sql);
		Planner cost_planner(*con.context);
		cost_planner.CreatePlan(cost_parser.statements[0]->Copy());
		Optimizer cost_optimizer(*cost_planner.binder, *con.context);
		auto cost_plan = cost_optimizer.Optimize(std::move(cost_planner.plan));

		auto cost_estimate = EstimateIVMCost(*con.context, *cost_plan, view_name);
		con.Rollback();
		if (cost_estimate.ShouldRecompute()) {
			OPENIVM_DEBUG_PRINT("[ADAPTIVE] Full recompute is cheaper — skipping IVM\n");
			return BuildRecomputeQuery(metadata, view_name, view_query_sql, cross_system, attached_db_catalog_name,
			                           attached_db_schema_name, catalog_prefix);
		}
	}

	// IVM path: proceed with incremental maintenance

	// Get column names from the delta view table.
	vector<string> column_names;
	bool list_mode = false;
	if (delta_view_catalog_entry) {
		// Standard catalog: use catalog entry directly
		auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
		const ColumnList &delta_view_columns = delta_view_entry->GetColumns();
		column_names = delta_view_columns.GetColumnNames();
		for (auto &col : delta_view_columns.Logical()) {
			if (col.GetName() != ivm::MULTIPLICITY_COL && col.GetType().id() == LogicalTypeId::LIST) {
				list_mode = true;
				break;
			}
		}
	} else {
		// DuckLake: get column names via SQL
		string delta_full = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
		auto col_result =
		    con.Query("SELECT column_name FROM information_schema.columns WHERE table_catalog = '" +
		              OpenIVMUtils::EscapeValue(view_catalog_name) + "' AND table_schema = '" +
		              OpenIVMUtils::EscapeValue(view_schema_name) + "' AND table_name = '" +
		              OpenIVMUtils::EscapeValue(OpenIVMUtils::DeltaName(view_name)) + "' ORDER BY ordinal_position");
		if (!col_result->HasError()) {
			for (idx_t i = 0; i < col_result->RowCount(); i++) {
				column_names.push_back(col_result->GetValue(0, i).ToString());
			}
		}
	}

	// Check if the delta view has a timestamp column (present when created via CREATE MATERIALIZED VIEW)
	bool has_ts_col =
	    std::find(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL)) != column_names.end();
	// Remove _duckdb_ivm_timestamp — it's auto-filled by DEFAULT (for chained MV support)
	column_names.erase(std::remove(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL)),
	                   column_names.end());
	OPENIVM_DEBUG_PRINT("[UPSERT] List mode: %s\n", list_mode ? "true" : "false");

	string upsert_query;

	// Build a timestamp filter for the delta_view reads in the upsert query.
	// This prevents double-counting when chained MVs accumulate delta_view rows
	// from multiple refresh rounds (because downstream views haven't consumed them yet).
	string delta_ts_filter;
	if (has_ts_col) {
		auto last_update_result =
		    con.Query("SELECT last_update FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
		              OpenIVMUtils::EscapeValue(view_name) + "' LIMIT 1");
		if (!last_update_result->HasError() && last_update_result->RowCount() > 0) {
			auto ts = last_update_result->GetValue(0, 0);
			if (!ts.IsNull()) {
				delta_ts_filter = string(ivm::TIMESTAMP_COL) + " >= '" + ts.ToString() + "'::TIMESTAMP";
			}
		}
	} // has_ts_col

	// Detect LEFT JOIN: the parser adds _ivm_left_key as a hidden column for LEFT/RIGHT JOIN views.
	// If the MV has this column, use it for the partial recompute filter.
	bool has_left_join = std::find(column_names.begin(), column_names.end(), ivm::LEFT_KEY_COL) != column_names.end();
	OPENIVM_DEBUG_PRINT("[UPSERT] has_left_join=%d\n", has_left_join);

	// Detect insert-only deltas: when the delta view contains only insert rows,
	// we can skip the zero-row DELETE (aggregates) and the DELETE+consolidation (projections).
	//
	// Safety rules:
	// - Non-join views: safe if all base deltas are insert-only
	// - DuckLake join views: safe if all base deltas are insert-only (N-term telescoping
	//   has no XOR cross-terms, so insert-only base = insert-only delta view)
	// - Standard join views: safe only if exactly ONE table changed AND it's insert-only
	//   (no cross-terms fire when other deltas are empty, so no XOR)
	bool insert_only = false;
	{
		auto delta_table_names = metadata.GetDeltaTables(view_name);
		bool has_join =
		    (view_query_sql.find(" JOIN ") != string::npos || view_query_sql.find(" join ") != string::npos);

		// Per-table analysis: is each delta empty, insert-only, or has deletes?
		idx_t tables_with_changes = 0;
		bool any_has_deletes = false;
		bool all_ducklake = true;

		// Cache DuckLake current snapshot (queried at most once).
		int64_t ducklake_cur_snap = -1;
		bool ducklake_snap_queried = false;

		for (auto &dt : delta_table_names) {
			if (metadata.IsDuckLakeTable(view_name, dt)) {
				if (!ducklake_snap_queried) {
					auto cur_snap_result = con.Query("SELECT id FROM " + view_catalog_name + ".current_snapshot()");
					if (!cur_snap_result->HasError() && cur_snap_result->RowCount() > 0) {
						ducklake_cur_snap = cur_snap_result->GetValue(0, 0).GetValue<int64_t>();
					}
					ducklake_snap_queried = true;
				}
				auto last_snap = metadata.GetLastSnapshotId(view_name, dt);
				if (ducklake_cur_snap < 0) {
					any_has_deletes = true; // conservative
					tables_with_changes++;
					continue;
				}
				auto cur_snap = ducklake_cur_snap;
				if (last_snap == cur_snap) {
					continue; // no changes — empty delta
				}
				tables_with_changes++;
				auto del_result = con.Query("SELECT COUNT(*) FROM ducklake_table_deletions('" +
				                            OpenIVMUtils::EscapeValue(view_catalog_name) + "', 'main', '" +
				                            OpenIVMUtils::EscapeValue(dt) + "', " + to_string(last_snap) + ", " +
				                            to_string(cur_snap) + ")");
				if (!del_result->HasError() && del_result->GetValue(0, 0).GetValue<int64_t>() > 0) {
					any_has_deletes = true;
				}
			} else {
				all_ducklake = false;
				auto ts_string = metadata.GetLastUpdate(view_name, dt);
				if (ts_string.empty()) {
					continue;
				}
				// Check if delta has any rows at all
				auto total_result = con.Query("SELECT COUNT(*) FROM " + OpenIVMUtils::QuoteIdentifier(dt) + " WHERE " +
				                              string(ivm::TIMESTAMP_COL) + " >= '" +
				                              OpenIVMUtils::EscapeValue(ts_string) + "'::TIMESTAMP");
				if (total_result->HasError() || total_result->GetValue(0, 0).GetValue<int64_t>() == 0) {
					continue; // empty delta
				}
				tables_with_changes++;
				// Check for deletes
				auto del_result =
				    con.Query("SELECT COUNT(*) FROM " + OpenIVMUtils::QuoteIdentifier(dt) + " WHERE " +
				              string(ivm::TIMESTAMP_COL) + " >= '" + OpenIVMUtils::EscapeValue(ts_string) +
				              "'::TIMESTAMP AND " + string(ivm::MULTIPLICITY_COL) + " = false");
				if (!del_result->HasError() && del_result->GetValue(0, 0).GetValue<int64_t>() > 0) {
					any_has_deletes = true;
				}
			}
		}

		if (any_has_deletes) {
			insert_only = false;
		} else if (!has_join) {
			// Non-join views: safe whenever all deltas are insert-only
			insert_only = true;
		} else if (all_ducklake) {
			// DuckLake join views: N-term telescoping has no XOR, always safe
			insert_only = true;
		} else if (tables_with_changes <= 1 && delta_table_names.size() > 1) {
			// Standard join views: safe when only one table changed AND there are
			// multiple distinct delta tables (rules out self-joins, where 1 delta
			// table maps to multiple join leaves that all change together).
			insert_only = true;
		}
	} // append_only_enabled
	// Read per-optimization flags to gate insert-only fast paths independently.
	Value skip_agg_del_val, skip_proj_del_val, minmax_incr_val;
	bool skip_agg_delete = insert_only;
	bool skip_proj_delete = insert_only;
	bool minmax_incremental = insert_only;
	if (context.TryGetCurrentSetting("ivm_skip_aggregate_delete", skip_agg_del_val) && !skip_agg_del_val.IsNull() &&
	    !skip_agg_del_val.GetValue<bool>()) {
		skip_agg_delete = false;
	}
	if (context.TryGetCurrentSetting("ivm_skip_projection_delete", skip_proj_del_val) && !skip_proj_del_val.IsNull() &&
	    !skip_proj_del_val.GetValue<bool>()) {
		skip_proj_delete = false;
	}
	if (context.TryGetCurrentSetting("ivm_minmax_incremental", minmax_incr_val) && !minmax_incr_val.IsNull() &&
	    !minmax_incr_val.GetValue<bool>()) {
		minmax_incremental = false;
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] insert_only=%d, skip_agg_delete=%d, skip_proj_delete=%d, minmax_incremental=%d\n",
	                    insert_only, skip_agg_delete, skip_proj_delete, minmax_incremental);

	// Compile the upsert query based on view type
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: %s\n",
	                    view_query_type == IVMType::AGGREGATE_HAVING    ? "AGGREGATE_HAVING"
	                    : view_query_type == IVMType::AGGREGATE_GROUP   ? "AGGREGATE_GROUP"
	                    : view_query_type == IVMType::SIMPLE_AGGREGATE  ? "SIMPLE_AGGREGATE"
	                    : view_query_type == IVMType::SIMPLE_PROJECTION ? "SIMPLE_PROJECTION"
	                                                                    : "UNKNOWN");
	// All DML (INSERT, DELETE, UPDATE, MERGE) targets the physical data table,
	// not the user-facing VIEW which excludes internal _ivm_* columns.
	// The compile functions receive view_name and compute data_table internally.
	// GROUP BY columns: from index (standard) or metadata (DuckLake fallback).
	auto group_cols = metadata.GetGroupColumns(view_name);
	auto agg_types = metadata.GetAggregateTypes(view_name);
	switch (view_query_type) {
	case IVMType::AGGREGATE_HAVING: {
		// When ivm_having_merge is enabled, the data table stores ALL groups (HAVING filter
		// is in the VIEW). Use standard MERGE — same as AGGREGATE_GROUP.
		// When disabled, fall back to group-recompute (has_minmax=true forces delete+re-insert).
		Value having_merge_val;
		bool having_merge = true;
		if (context.TryGetCurrentSetting("ivm_having_merge", having_merge_val) && !having_merge_val.IsNull()) {
			having_merge = having_merge_val.GetValue<bool>();
		}
		if (having_merge) {
			bool effective_insert_only = has_minmax ? minmax_incremental : skip_agg_delete;
			upsert_query = CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names,
			                                      view_query_sql, has_minmax, list_mode, delta_ts_filter, group_cols,
			                                      catalog_prefix, effective_insert_only, agg_types);
		} else {
			upsert_query = CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names,
			                                      view_query_sql, /*has_minmax=*/true, list_mode, delta_ts_filter,
			                                      group_cols, catalog_prefix, /*insert_only=*/false, agg_types);
		}
		break;
	}
	case IVMType::AGGREGATE_GROUP: {
		// For MIN/MAX views, use minmax_incremental (gated by ivm_minmax_incremental).
		// For non-MIN/MAX views, use skip_agg_delete (gated by ivm_skip_aggregate_delete).
		bool effective_insert_only = has_minmax ? minmax_incremental : skip_agg_delete;
		upsert_query = CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names,
		                                      view_query_sql, has_minmax, list_mode, delta_ts_filter, group_cols,
		                                      catalog_prefix, effective_insert_only, agg_types);
		break;
	}
	case IVMType::SIMPLE_PROJECTION: {
		if (has_left_join) {
			string delta_where = delta_ts_filter.empty() ? "" : " AND " + delta_ts_filter;
			string qdt = data_table;
			string qdv = KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
			string lk = KeywordHelper::WriteOptionallyQuoted(string(ivm::LEFT_KEY_COL));
			string affected = "EXISTS (SELECT 1 FROM " + qdv + " _d WHERE _d." + lk + " IS NOT DISTINCT FROM ";
			upsert_query = "DELETE FROM " + qdt + " WHERE " + affected + qdt + "." + lk + delta_where + ");\n" +
			               "INSERT INTO " + qdt + "\nSELECT * FROM (" + view_query_sql + ") _ivm_lj\nWHERE " +
			               affected + "_ivm_lj." + lk + delta_where + ");\n";
		} else {
			upsert_query =
			    CompileProjectionsFilters(view_name, column_names, delta_ts_filter, catalog_prefix, skip_proj_delete);
		}
		break;
	}

	case IVMType::SIMPLE_AGGREGATE: {
		upsert_query = CompileSimpleAggregates(view_name, column_names, view_query_sql, has_minmax, list_mode,
		                                       delta_ts_filter, catalog_prefix, insert_only);
		if (!has_minmax) {
			auto source_tables = metadata.GetDeltaTables(view_name);
			for (auto &dt : source_tables) {
				// Strip "delta_" prefix to get base table name
				string source =
				    StringUtil::StartsWith(dt, ivm::DELTA_PREFIX) ? dt.substr(strlen(ivm::DELTA_PREFIX)) : dt;
				string null_cols;
				for (auto &col : column_names) {
					if (col != string(ivm::MULTIPLICITY_COL)) {
						if (!null_cols.empty()) {
							null_cols += ", ";
						}
						null_cols += KeywordHelper::WriteOptionallyQuoted(col) + " = NULL";
					}
				}
				string qdt = data_table;
				upsert_query += "UPDATE " + qdt + " SET " + null_cols + " WHERE NOT EXISTS (SELECT 1 FROM " + source +
				                " LIMIT 1);\n";
			}
		}
		break;
	}
	case IVMType::FULL_REFRESH: {
		// Should not reach here — full refresh is handled earlier via BuildRecomputeQuery.
		throw InternalException("FULL_REFRESH views should not reach incremental upsert compilation");
	}
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] Upsert query:\n%s\n", upsert_query.c_str());
	// DoIVM is a table function (root of the tree)
	string ivm_query;

	// splitting the query in two to make it easier to turn into string (insertions are the same)
	string do_ivm = "select * from DoIVM('" + OpenIVMUtils::EscapeValue(view_catalog_name) + "','" +
	                OpenIVMUtils::EscapeValue(view_schema_name) + "','" + OpenIVMUtils::EscapeValue(view_name) + "');";

	auto delta_table_names = metadata.GetDeltaTables(view_name);

	// now we can plan the query
	OPENIVM_DEBUG_PRINT("[UPSERT] Planning DoIVM query: %s\n", do_ivm.c_str());
	Parser p;
	p.ParseQuery(do_ivm);

	con.BeginTransaction();
	auto &con_ctx = *con.context;
	OPENIVM_DEBUG_PRINT("[UPSERT] Creating planner...\n");
	Planner planner(con_ctx);
	OPENIVM_DEBUG_PRINT("[UPSERT] CreatePlan...\n");
	planner.CreatePlan(std::move(p.statements[0]));
	auto plan = std::move(planner.plan);
	OPENIVM_DEBUG_PRINT("[UPSERT] Plan created. Running optimizer...\n");
	Optimizer optimizer(*planner.binder, con_ctx);
	plan = optimizer.Optimize(std::move(plan)); // this transforms the plan into an incremental plan
	OPENIVM_DEBUG_PRINT("[UPSERT] Optimizer done.\n");
	con.Rollback();

	// Convert the rewritten plan to SQL via the AST pipeline
	auto ast = LogicalPlanToAst(con_ctx, plan);
	auto cte_list = AstToCteList(*ast);
	string raw_ivm_sql = cte_list->ToQuery(false);
	OPENIVM_DEBUG_PRINT("[UPSERT] ToQuery done. SQL:\n%s\n", raw_ivm_sql.c_str());

	// Use explicit column list in INSERT INTO delta_view, excluding _duckdb_ivm_timestamp
	// so the DEFAULT now() fills it in (for chained MV support)
	string delta_view_name = catalog_prefix + OpenIVMUtils::DeltaName(view_name);
	string insert_target_bare = "INSERT INTO " + OpenIVMUtils::DeltaName(view_name);
	auto insert_pos = raw_ivm_sql.find(insert_target_bare);
	if (insert_pos != string::npos) {
		// Replace bare delta table name with catalog-qualified version
		if (!catalog_prefix.empty()) {
			raw_ivm_sql.replace(insert_pos, insert_target_bare.size(), "INSERT INTO " + delta_view_name);
			insert_pos = raw_ivm_sql.find("INSERT INTO " + delta_view_name);
		}
		string col_list = "(";
		for (size_t i = 0; i < column_names.size(); i++) {
			if (i > 0) {
				col_list += ", ";
			}
			col_list += OpenIVMUtils::QuoteIdentifier(column_names[i]);
		}
		col_list += ") ";
		string full_insert = "INSERT INTO " + delta_view_name;
		raw_ivm_sql.insert(insert_pos + full_insert.size(), " " + col_list);
	}
	ivm_query += raw_ivm_sql;

	// Delete from delta view: timestamp-based if downstream views depend on it, unconditional otherwise
	auto downstream_check = con.Query("SELECT COUNT(*) FROM " + string(ivm::DELTA_TABLES_TABLE) +
	                                  " WHERE table_name = '" + OpenIVMUtils::EscapeValue(delta_view_name) + "'");
	bool has_downstream = !downstream_check->HasError() && downstream_check->RowCount() > 0 &&
	                      downstream_check->GetValue(0, 0).GetValue<int64_t>() > 0;

	// Companion rows for downstream consumers.
	// When a view has downstream MVs that read its delta, those downstream views need
	// both the OLD and NEW state to correctly compute their own deltas.
	// The IVM query produces the NEW state (delta with mul=true).
	// The companion query records the OLD state (current MV rows with mul=false)
	// BEFORE the upsert modifies the MV.
	string companion_query;

	// For SIMPLE_AGGREGATE / SIMPLE_PROJECTION with downstream consumers:
	// The IVM delta represents the CHANGE (+5), but downstream projections need
	// the ABSOLUTE old and new values to compute their own deltas correctly.
	// Strategy: record old state (mul=false) BEFORE upsert, then record new state
	// (mul=true) AFTER upsert. The IVM delta in delta_view is replaced by these
	// absolute snapshots so downstream sees a clean old→new transition.
	string pre_companion;  // old MV state → delta_view with false (runs BEFORE IVM+upsert)
	string post_companion; // new MV state → delta_view with true (runs AFTER upsert)

	if (has_downstream &&
	    (view_query_type == IVMType::SIMPLE_AGGREGATE || view_query_type == IVMType::SIMPLE_PROJECTION)) {
		// Save old MV state to a temp table BEFORE the IVM+upsert modifies the MV.
		// After the upsert, clear IVM delta from delta_view and replace with
		// old(false) + new(true) absolute snapshots for downstream consumption.
		string col_list;
		for (auto &col : column_names) {
			if (!col_list.empty()) {
				col_list += ", ";
			}
			col_list += OpenIVMUtils::QuoteIdentifier(col);
		}
		string select_false, select_true;
		bool first = true;
		for (auto &col : column_names) {
			if (!first) {
				select_false += ", ";
				select_true += ", ";
			}
			first = false;
			if (col == string(ivm::MULTIPLICITY_COL)) {
				select_false += "false";
				select_true += "true";
			} else {
				select_false += OpenIVMUtils::QuoteIdentifier(col);
				select_true += OpenIVMUtils::QuoteIdentifier(col);
			}
		}
		// Pre: snapshot old state into temp table
		string temp_name = string(ivm::TEMP_TABLE_PREFIX) + view_name;
		string qt = KeywordHelper::WriteOptionallyQuoted(temp_name);
		string qdvn = KeywordHelper::WriteOptionallyQuoted(delta_view_name);
		string qdt2 = data_table;
		pre_companion = "CREATE TEMP TABLE " + qt + " AS SELECT * FROM " + qdt2 + ";\n";
		// Post: clear ALL IVM delta rows (both true and false), replace with absolute snapshots
		post_companion = "DELETE FROM " + qdvn + " WHERE 1=1";
		if (!delta_ts_filter.empty()) {
			post_companion += " AND " + delta_ts_filter;
		}
		post_companion += ";\n";
		post_companion += "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_false + " FROM " + qt + ";\n";
		post_companion += "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_true + " FROM " + qdt2 + ";\n";
		post_companion += "DROP TABLE " + qt + ";\n";
		OPENIVM_DEBUG_PRINT("[UPSERT] Pre-companion: %s\n", pre_companion.c_str());
		OPENIVM_DEBUG_PRINT("[UPSERT] Post-companion: %s\n", post_companion.c_str());
	} else if ((view_query_type == IVMType::AGGREGATE_GROUP || view_query_type == IVMType::AGGREGATE_HAVING) &&
	           has_downstream && index_delta_view_catalog_entry) {
		auto *idx = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
		auto key_ids = idx->column_ids;
		vector<string> keys;
		unordered_set<string> keys_set;
		for (auto &kid : key_ids) {
			keys.push_back(column_names[kid]);
			keys_set.insert(column_names[kid]);
		}

		string col_list, val_list, join_cond;
		for (auto &col : column_names) {
			if (!col_list.empty()) {
				col_list += ", ";
				val_list += ", ";
			}
			col_list += col;
			if (keys_set.count(col)) {
				val_list += "d." + col;
			} else if (col == ivm::MULTIPLICITY_COL) {
				val_list += "false";
			} else {
				val_list += "0";
			}
		}
		for (size_t i = 0; i < keys.size(); i++) {
			if (i > 0) {
				join_cond += " AND ";
			}
			join_cond += "d." + keys[i] + " IS NOT DISTINCT FROM m." + keys[i];
		}

		companion_query = "INSERT INTO " + delta_view_name + " (" + col_list + ") SELECT " + val_list + " FROM " +
		                  delta_view_name + " d WHERE d." + string(ivm::MULTIPLICITY_COL) + " = true";
		if (!delta_ts_filter.empty()) {
			companion_query += " AND d." + delta_ts_filter;
		}
		companion_query += " AND EXISTS (SELECT 1 FROM " + data_table + " m WHERE " + join_cond + ");\n";
		OPENIVM_DEBUG_PRINT("[UPSERT] Companion query:\n%s\n", companion_query.c_str());
	}

	string delete_from_view_query;
	if (has_downstream) {
		delete_from_view_query = IVMMetadata::BuildDeltaCleanupSQL(delta_view_name, delta_view_name);
	} else {
		delete_from_view_query = "DELETE FROM " + delta_view_name + ";";
	}

	// now we can also delete from the delta table, but only if all the dependent views have been refreshed
	// example: if two views A and B are on the same table T, we can only remove tuples from T
	// if both A and B have been refreshed (up to some timestamp)
	// to check this, we extract the minimum timestamp from _duckdb_ivm_delta_tables
	string delete_from_delta_table_query;
	string update_timestamp_query = "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
	                                " SET last_update = now() WHERE view_name = '" +
	                                OpenIVMUtils::EscapeValue(view_name) + "';\n";

	// Update DuckLake snapshot IDs so the next refresh only sees new changes.
	string snapshot_update_query;
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			// Find the catalog name for this DuckLake table from the view query
			// (the table name in metadata is the bare name, e.g. "products")
			string cat_name = view_catalog_name; // default to view's catalog
			snapshot_update_query += "UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
			                         " SET last_snapshot_id = (SELECT id FROM " + cat_name + ".current_snapshot())" +
			                         " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
			                         "' AND table_name = '" + OpenIVMUtils::EscapeValue(dt) + "';\n";
		}
	}

	for (auto &dt : delta_table_names) {
		// DuckLake tables have no delta tables to clean up
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			continue;
		}
		string resolved = dt;
		if (cross_system) {
			resolved = attached_db_catalog_name + "." + attached_db_schema_name + "." + dt;
		}
		delete_from_delta_table_query += IVMMetadata::BuildDeltaCleanupSQL(resolved, dt);
	}

	// Crash safety: set flag before the critical section, clear after last_update is set.
	// If the process crashes between these two points, the next refresh detects the flag
	// and recovers via full recompute (see check at the top of GenerateRefreshSQL).
	string set_in_progress = "UPDATE " + string(ivm::VIEWS_TABLE) +
	                         " SET refresh_in_progress = true WHERE view_name = '" +
	                         OpenIVMUtils::EscapeValue(view_name) + "';\n";
	string clear_in_progress = "UPDATE " + string(ivm::VIEWS_TABLE) +
	                           " SET refresh_in_progress = false WHERE view_name = '" +
	                           OpenIVMUtils::EscapeValue(view_name) + "';\n";

	// Assembly order:
	// 0. set_in_progress: mark refresh as in-flight (crash safety)
	// 1. pre_companion: snapshot old MV state into delta_view (for downstream old→new)
	// 2. ivm_query: compute delta, INSERT INTO delta_view
	// 3. companion_query: (AGGREGATE_GROUP) insert false/zero rows for existing groups
	// 4. upsert_query: apply delta to MV
	// 5. post_companion: replace IVM delta in delta_view with absolute new MV state
	// 6. update_timestamp: mark this refresh complete
	// 7. clear_in_progress: crash safety — flag cleared after timestamp is set
	// 8. delete_from_view: clean old delta_view rows
	// 9. delete_from_delta: clean old base delta rows
	string clean_query = set_in_progress + pre_companion + ivm_query + "\n" + companion_query + "\n" + upsert_query +
	                     "\n" + post_companion + update_timestamp_query + snapshot_update_query + "\n" +
	                     clear_in_progress + delete_from_view_query + "\n" + delete_from_delta_table_query;

	// Write reference SQL to disk only if ivm_files_path is explicitly set
	Value files_path_val;
	if (context.TryGetCurrentSetting("ivm_files_path", files_path_val) && !files_path_val.IsNull()) {
		string ivm_file_path = files_path_val.ToString() + "/ivm_upsert_queries_" + view_name + ".sql";
		duckdb::OpenIVMUtils::WriteFile(ivm_file_path, false, clean_query);
	}

	OPENIVM_DEBUG_PRINT("[UPSERT] Generated query:\n%s\n", clean_query.c_str());

	return clean_query;
}

} // namespace duckdb
