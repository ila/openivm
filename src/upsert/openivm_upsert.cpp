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

/// Parsed FULL OUTER JOIN metadata: table and column names for both sides.
struct FojJoinInfo {
	string left_table, left_col, right_table, right_col;
	string dt_left_name, dt_right_name; // delta table names

	static FojJoinInfo Parse(IVMMetadata &metadata, const string &view_name, const vector<string> &delta_table_names) {
		FojJoinInfo info;
		string raw = metadata.GetFullOuterJoinCols(view_name);
		auto comma_pos = raw.find(',');
		if (comma_pos != string::npos) {
			string left_part = raw.substr(0, comma_pos);
			string right_part = raw.substr(comma_pos + 1);
			auto lc = left_part.find(':');
			if (lc != string::npos) {
				info.left_table = left_part.substr(0, lc);
				info.left_col = left_part.substr(lc + 1);
			}
			auto rc = right_part.find(':');
			if (rc != string::npos) {
				info.right_table = right_part.substr(0, rc);
				info.right_col = right_part.substr(rc + 1);
			}
		}
		// Case-insensitive compare: delta_table_names are stored lowercased (DuckDB's parser
		// lowercases unquoted identifiers), while full_outer_join_cols preserves the case of
		// the MV's FROM clause (e.g. "OORDER:O_W_ID").
		for (auto &dt_name : delta_table_names) {
			string base = StringUtil::StartsWith(dt_name, ivm::DELTA_PREFIX) ? dt_name.substr(strlen(ivm::DELTA_PREFIX))
			                                                                 : dt_name;
			if (StringUtil::CIEquals(base, info.left_table)) {
				info.dt_left_name = dt_name;
			}
			if (StringUtil::CIEquals(base, info.right_table)) {
				info.dt_right_name = dt_name;
			}
		}
		return info;
	}
};

static string BuildRecomputeQuery(IVMMetadata &metadata, const string &view_name, const string &view_query_sql,
                                  bool cross_system, const string &attached_catalog = "",
                                  const string &attached_schema = "", const string &catalog_prefix = "") {
	string qdt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(view_name));
	string query = "DELETE FROM " + qdt + ";\n";
	query += "INSERT INTO " + qdt + " " + view_query_sql + ";\n\n";

	// metadata.UpdateTimestamp runs on the native-catalog metadata connection immediately.
	// For cross_system (DuckLake), do NOT include update_ts in the returned string — exec_con
	// will have already modified dl, and writing to the physical-default catalog in the same
	// implicit transaction is forbidden by DuckDB's cross-catalog write restriction.
	metadata.UpdateTimestamp(view_name);
	string update_ts;
	if (!cross_system) {
		update_ts = "UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_update = now() WHERE view_name = '" +
		            OpenIVMUtils::EscapeValue(view_name) + "';\n";
	}

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
                                 const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                 string *out_pre_meta = nullptr, string *out_post_meta = nullptr);

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

		// For cross_system (DuckLake) MVs, split the refresh SQL into data ops (dl catalog)
		// and metadata ops (physical-default catalog) to avoid the cross-catalog write error.
		string meta_pre_sql, meta_post_sql;
		string sql = GenerateRefreshSQL(
		    context, view_catalog_name, view_schema_name, vn, cross_system, attached_db_catalog_name,
		    attached_db_schema_name, cross_system ? &meta_pre_sql : nullptr, cross_system ? &meta_post_sql : nullptr);
		Connection exec_con(*context.db.get());
		// IVM-generated SQL can nest deeply for multi-table joins + CTEs (N-term telescoping
		// over 7+ tables produces hundreds of chained projections). Lift the default 1000
		// expression-depth limit so the binder doesn't reject legitimate plans.
		exec_con.Query("SET max_expression_depth = 10000");
		// When the stored view_query_sql came from LPTS fallback (e.g. WINDOW functions
		// serialized back to original user SQL), it contains unqualified base-table
		// references. Switch to the MV's catalog so those resolve correctly during refresh.
		if (cross_system && !view_catalog_name.empty()) {
			exec_con.Query("USE " + view_catalog_name + "." + view_schema_name);
		}
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
		} else if (!meta_pre_sql.empty()) {
			Connection meta_con(*context.db.get());
			meta_con.Query(meta_pre_sql);
		}
		auto start = std::chrono::steady_clock::now();
		auto result = exec_con.Query(sql);
		auto end = std::chrono::steady_clock::now();

		if (result->HasError()) {
			if (!cross_system) {
				exec_con.Rollback();
			}
			throw InternalException("IVM refresh of '" + vn + "' failed: " + result->GetError());
		}
		if (!cross_system) {
			exec_con.Commit();
		} else if (!meta_post_sql.empty()) {
			Connection meta_con(*context.db.get());
			meta_con.Query(meta_post_sql);
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
						// Non-default catalog (e.g. DuckLake attached DB): the MV data lives in a
						// different database than the metadata tables (which are in "memory").
						// DuckDB forbids writing to two databases in one transaction, so set
						// cross_system to skip the transaction wrapper in RefreshViewLocked.
						if (cat_name != "memory") {
							cross_system = true;
						}
						OPENIVM_DEBUG_PRINT("[UPSERT] Found view in catalog '%s' (cross_system=%d)\n", cat_name.c_str(),
						                    cross_system);
						break;
					}
				}
			}
			OPENIVM_DEBUG_PRINT("[UPSERT] Resolved catalog='%s', schema='%s'\n", view_catalog_name.c_str(),
			                    view_schema_name.c_str());
		}
	}

	// cross_system detection: the view's catalog differs from the fresh connection's physical
	// default. Metadata tables (_duckdb_ivm_views etc.) live in the physical default; data/view
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

// When cross_system is true (DuckLake MVs), the refresh SQL touches two catalogs:
// data ops write to dl.main.*, metadata ops write to the physical default.
// DuckDB forbids cross-catalog writes in one transaction. We split the SQL into
// three parts: pre-metadata (set_in_progress), data ops, post-metadata (timestamps).
// out_pre_meta / out_post_meta receive those parts; the return value is data-only.
// When cross_system is false (or out_pre_meta is null), returns the combined SQL.
static string GenerateRefreshSQL(ClientContext &context, const string &view_catalog_name,
                                 const string &view_schema_name, const string &view_name, bool cross_system,
                                 const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                 string *out_pre_meta, string *out_post_meta) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	QueryErrorContext error_context = QueryErrorContext();
	Connection con(*context.db.get());
	// Catalog-qualified prefix for SQL references (e.g. "dl.main." or "" for default).
	// Only add the prefix when the catalog is non-default (e.g. DuckLake attached DB).
	// Metadata tables (_duckdb_ivm_views etc.) are always unqualified — they live in the
	// physical default catalog, resolved via the fresh connection without any USE.
	string catalog_prefix;
	if (!view_catalog_name.empty() && view_catalog_name != "memory") {
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
	// LEFT JOIN aggregates: group-recompute unless ivm_left_join_merge is on (Larson & Zhou).
	// FULL OUTER JOIN aggregates: group-recompute unless ivm_full_outer_merge is on (Zhang & Larson).
	bool source_has_left_join = metadata.HasLeftJoin(view_name);
	// Only query has_full_outer when the view has an outer join (avoids extra SQL for INNER-only views)
	bool source_has_full_outer = source_has_left_join && metadata.HasFullOuter(view_name);
	bool left_join_merge = false;
	Value lj_merge_val;
	if (source_has_left_join && !source_has_full_outer) {
		if (context.TryGetCurrentSetting("ivm_left_join_merge", lj_merge_val) && !lj_merge_val.IsNull()) {
			left_join_merge = lj_merge_val.GetValue<bool>();
		}
	}
	bool full_outer_merge = false;
	if (source_has_full_outer) {
		Value foj_merge_val;
		if (context.TryGetCurrentSetting("ivm_full_outer_merge", foj_merge_val) && !foj_merge_val.IsNull()) {
			full_outer_merge = foj_merge_val.GetValue<bool>();
		}
	}
	bool has_minmax = metadata.HasMinMax(view_name) || view_query_type == IVMType::AGGREGATE_HAVING ||
	                  (source_has_left_join && !source_has_full_outer && !left_join_merge) ||
	                  (source_has_full_outer && !full_outer_merge);

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
	vector<LogicalType> column_types;
	bool list_mode = false;
	if (delta_view_catalog_entry) {
		// Standard catalog: use catalog entry directly
		auto delta_view_entry = dynamic_cast<TableCatalogEntry *>(delta_view_catalog_entry.get());
		const ColumnList &delta_view_columns = delta_view_entry->GetColumns();
		column_names = delta_view_columns.GetColumnNames();
		for (auto &col : delta_view_columns.Logical()) {
			column_types.push_back(col.GetType());
			if (col.GetName() != ivm::MULTIPLICITY_COL && col.GetType().id() == LogicalTypeId::LIST) {
				list_mode = true;
			}
		}
	} else {
		// DuckLake: get column names + types via SQL
		string delta_full = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
		auto col_result =
		    con.Query("SELECT column_name, data_type FROM information_schema.columns WHERE table_catalog = '" +
		              OpenIVMUtils::EscapeValue(view_catalog_name) + "' AND table_schema = '" +
		              OpenIVMUtils::EscapeValue(view_schema_name) + "' AND table_name = '" +
		              OpenIVMUtils::EscapeValue(OpenIVMUtils::DeltaName(view_name)) + "' ORDER BY ordinal_position");
		if (!col_result->HasError()) {
			for (idx_t i = 0; i < col_result->RowCount(); i++) {
				column_names.push_back(col_result->GetValue(0, i).ToString());
				try {
					column_types.push_back(
					    TransformStringToLogicalType(col_result->GetValue(1, i).ToString(), context));
				} catch (...) {
					column_types.push_back(LogicalType::VARCHAR);
				}
			}
		}
	}

	// Check if the delta view has a timestamp column (present when created via CREATE MATERIALIZED VIEW)
	bool has_ts_col =
	    std::find(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL)) != column_names.end();
	// Remove _duckdb_ivm_timestamp — it's auto-filled by DEFAULT (for chained MV support).
	// Keep column_names and column_types aligned: drop the type at the same position.
	auto it = std::find(column_names.begin(), column_names.end(), string(ivm::TIMESTAMP_COL));
	if (it != column_names.end()) {
		auto offset = it - column_names.begin();
		column_names.erase(it);
		if (offset < static_cast<decltype(offset)>(column_types.size())) {
			column_types.erase(column_types.begin() + offset);
		}
	}
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
				delta_ts_filter = string(ivm::TIMESTAMP_COL) + " > '" + ts.ToString() + "'::TIMESTAMP";
			}
		}
	} // has_ts_col

	// Detect LEFT JOIN: the parser adds _ivm_left_key as a hidden column for LEFT/RIGHT JOIN views.
	// If the MV has this column, use it for the partial recompute filter.
	bool has_left_join = std::find(column_names.begin(), column_names.end(), ivm::LEFT_KEY_COL) != column_names.end();
	// Detect FULL OUTER JOIN: the parser also adds _ivm_right_key for bidirectional recompute.
	bool has_full_outer = std::find(column_names.begin(), column_names.end(), ivm::RIGHT_KEY_COL) != column_names.end();
	OPENIVM_DEBUG_PRINT("[UPSERT] has_left_join=%d has_full_outer=%d\n", has_left_join, has_full_outer);

	// Detect insert-only deltas: when the delta view contains only insert rows,
	// we can skip the zero-row DELETE (aggregates) and the DELETE+consolidation (projections).
	//
	// Safety rules:
	// - Non-join views: safe if all base deltas are insert-only
	// - DuckLake join views: safe if all base deltas are insert-only (N-term telescoping
	//   has no XOR cross-terms, so insert-only base = insert-only delta view)
	// - Standard join views: safe only if exactly ONE table changed AND it's insert-only
	//   (no cross-terms fire when other deltas are empty, so no XOR)
	auto delta_table_names = metadata.GetDeltaTables(view_name);
	bool insert_only = false;
	{
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
	                    : view_query_type == IVMType::WINDOW_PARTITION  ? "WINDOW_PARTITION"
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
			                                      catalog_prefix, effective_insert_only, agg_types, column_types);
		} else {
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql, /*has_minmax=*/true,
			    list_mode, delta_ts_filter, group_cols, catalog_prefix, /*insert_only=*/false, agg_types, column_types);
		}
		break;
	}
	case IVMType::AGGREGATE_GROUP: {
		if (source_has_full_outer && !full_outer_merge) {
			// FULL OUTER JOIN aggregate group-recompute (Zhang & Larson):
			// 4 sources of affected group keys to cover all change types.
			string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
			string keys_tuple;
			for (size_t i = 0; i < group_cols.size(); i++) {
				keys_tuple += KeywordHelper::WriteOptionallyQuoted(group_cols[i]);
				if (i < group_cols.size() - 1) {
					keys_tuple += ", ";
				}
			}

			auto foj = FojJoinInfo::Parse(metadata, view_name, delta_table_names);
			string q_dt_left = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_left_name);
			string q_dt_right = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_right_name);
			string q_left_base = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.left_table);
			string q_left_col = KeywordHelper::WriteOptionallyQuoted(foj.left_col);
			string q_right_col = KeywordHelper::WriteOptionallyQuoted(foj.right_col);
			string qdv = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
			// DuckLake base tables lack `_duckdb_ivm_timestamp` — applying the ts filter to
			// `delta_oorder` works (it's a twin table with the ts column), but applying it to
			// `dl.main.OORDER` errors with "column not found". Sources 2 and 3 query the
			// DuckLake base table directly (delta_table_names stores the DuckLake name, no
			// delta_ twin exists), so drop the filter for those sources on DuckLake.
			bool dt_left_is_ducklake =
			    !foj.dt_left_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_left_name);
			bool dt_right_is_ducklake =
			    !foj.dt_right_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_right_name);
			string delta_where_left = dt_left_is_ducklake ? "" : delta_where;
			string delta_where_right = dt_right_is_ducklake ? "" : delta_where;

			// Source 1: delta view — matched-row changes (INNER-demoted delta has group keys)
			string affected = "SELECT DISTINCT " + keys_tuple + " FROM " + qdv + delta_where;

			// Sources 2 and 3 select `keys_tuple` from a base/delta table that doesn't have
			// those columns by name, so the reference becomes a correlated reference to the
			// outer `_ivm_data_mv_X.keys`. Single-column correlated IN is supported by DuckDB
			// and forces full-delete of affected rows (INSERT then restores correctness);
			// multi-column correlated IN is not yet supported, so skip sources 2/3 when the
			// MV has a compound group key. Matched changes already covered by source 1.
			bool single_col_keys = (group_cols.size() == 1);

			// Source 2: left delta table — directly has group columns (unmatched-left changes)
			if (single_col_keys && !foj.dt_left_name.empty()) {
				affected += "\n  UNION\n  SELECT DISTINCT " + keys_tuple + " FROM " + q_dt_left + delta_where_left;
			}

			// Source 3: left base table lookup — map right-side join keys to group keys
			if (single_col_keys && !foj.dt_right_name.empty() && !foj.left_table.empty()) {
				affected += "\n  UNION\n  SELECT DISTINCT " + keys_tuple + " FROM " + q_left_base + " WHERE " +
				            q_left_col + " IN (" + "SELECT DISTINCT " + q_right_col + " FROM " + q_dt_right +
				            delta_where_right + ")";
			}

			// Source 4: NULL group — always recompute (unmatched-right changes)
			// SQL IN doesn't match NULL, so use OR ... IS NULL
			string null_check;
			for (size_t i = 0; i < group_cols.size(); i++) {
				if (i > 0) {
					null_check += " AND ";
				}
				null_check += KeywordHelper::WriteOptionallyQuoted(group_cols[i]) + " IS NULL";
			}

			string where_clause = "(" + keys_tuple + ") IN (\n  " + affected + "\n) OR (" + null_check + ")";
			upsert_query = "DELETE FROM " + data_table + " WHERE " + where_clause + ";\n" + "INSERT INTO " +
			               data_table + "\nSELECT * FROM (" + view_query_sql + ") _ivm_recompute\nWHERE " +
			               where_clause + ";\n";
		} else if (source_has_full_outer && full_outer_merge) {
			// Zhang & Larson MERGE for FULL OUTER JOIN aggregates:
			// Phase 1: MERGE handles matched-row changes via _ivm_match_count
			//          (same as LEFT JOIN MERGE — INNER-demoted delta view)
			// Phase 2: Recompute groups affected by UNMATCHED changes
			//          (left delta table + right→left join key mapping + NULL group)
			bool effective_insert_only = skip_agg_delete;
			upsert_query = CompileAggregateGroups(
			    view_name, index_delta_view_catalog_entry.get(), column_names, view_query_sql, /*has_minmax=*/false,
			    list_mode, delta_ts_filter, group_cols, catalog_prefix, effective_insert_only, agg_types, column_types);

			// Phase 2: recompute groups affected by unmatched changes
			string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
			string keys_tuple;
			for (size_t i = 0; i < group_cols.size(); i++) {
				keys_tuple += KeywordHelper::WriteOptionallyQuoted(group_cols[i]);
				if (i < group_cols.size() - 1) {
					keys_tuple += ", ";
				}
			}

			auto foj = FojJoinInfo::Parse(metadata, view_name, delta_table_names);
			// DuckLake base tables lack `_duckdb_ivm_timestamp` — see group-recompute path above.
			bool dt_left_is_ducklake =
			    !foj.dt_left_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_left_name);
			bool dt_right_is_ducklake =
			    !foj.dt_right_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_right_name);
			string delta_where_left = dt_left_is_ducklake ? "" : delta_where;
			string delta_where_right = dt_right_is_ducklake ? "" : delta_where;

			// Same correlated-IN limitation as the group-recompute path: skip sources 2/3 for
			// multi-column group keys (DuckDB doesn't support correlated multi-column IN).
			bool single_col_keys = (group_cols.size() == 1);

			// Unmatched-affected groups: left delta (source 2) + base table lookup (source 3)
			string unmatched_affected;
			if (single_col_keys && !foj.dt_left_name.empty()) {
				string q_dt_left = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_left_name);
				unmatched_affected = "SELECT DISTINCT " + keys_tuple + " FROM " + q_dt_left + delta_where_left;
			}
			if (single_col_keys && !foj.dt_right_name.empty() && !foj.left_table.empty()) {
				string q_dt_right = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_right_name);
				string q_left_base = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.left_table);
				if (!unmatched_affected.empty()) {
					unmatched_affected += "\n  UNION\n  ";
				}
				unmatched_affected += "SELECT DISTINCT " + keys_tuple + " FROM " + q_left_base + " WHERE " +
				                      KeywordHelper::WriteOptionallyQuoted(foj.left_col) + " IN (SELECT DISTINCT " +
				                      KeywordHelper::WriteOptionallyQuoted(foj.right_col) + " FROM " + q_dt_right +
				                      delta_where_right + ")";
			}

			// Build NULL check for source 4
			string null_check;
			for (size_t i = 0; i < group_cols.size(); i++) {
				if (i > 0) {
					null_check += " AND ";
				}
				null_check += KeywordHelper::WriteOptionallyQuoted(group_cols[i]) + " IS NULL";
			}

			if (!unmatched_affected.empty()) {
				string where = "(" + keys_tuple + ") IN (\n  " + unmatched_affected + "\n) OR (" + null_check + ")";
				upsert_query += "DELETE FROM " + data_table + " WHERE " + where + ";\n";
				upsert_query += "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql +
				                ") _ivm_unmatched\nWHERE " + where + ";\n";
			} else {
				upsert_query += "DELETE FROM " + data_table + " WHERE " + null_check + ";\n";
				upsert_query += "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql +
				                ") _ivm_null_recompute\nWHERE " + null_check + ";\n";
			}
		} else {
			// Standard path: MIN/MAX group-recompute or incremental MERGE
			bool effective_insert_only = has_minmax ? minmax_incremental : skip_agg_delete;
			upsert_query = CompileAggregateGroups(view_name, index_delta_view_catalog_entry.get(), column_names,
			                                      view_query_sql, has_minmax, list_mode, delta_ts_filter, group_cols,
			                                      catalog_prefix, effective_insert_only, agg_types, column_types);
		}
		break;
	}
	case IVMType::SIMPLE_PROJECTION: {
		if (has_full_outer) {
			// FULL OUTER JOIN bidirectional partial recompute (Zhang & Larson):
			// Metadata format: "left_table:left_col,right_table:right_col"
			string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
			const string &qdt = data_table;
			string lk = KeywordHelper::WriteOptionallyQuoted(string(ivm::LEFT_KEY_COL));
			string rk = KeywordHelper::WriteOptionallyQuoted(string(ivm::RIGHT_KEY_COL));

			auto foj = FojJoinInfo::Parse(metadata, view_name, delta_table_names);
			// DuckLake base tables lack `_duckdb_ivm_timestamp` — drop the ts filter when the
			// delta source is a DuckLake base (delta_table_names stores the base name itself).
			bool dt_left_is_ducklake =
			    !foj.dt_left_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_left_name);
			bool dt_right_is_ducklake =
			    !foj.dt_right_name.empty() && metadata.IsDuckLakeTable(view_name, foj.dt_right_name);
			string delta_where_left = dt_left_is_ducklake ? "" : delta_where;
			string delta_where_right = dt_right_is_ducklake ? "" : delta_where;

			// Build affected-keys CTE: query each delta table for its join column
			string union_parts;
			if (!foj.dt_left_name.empty() && !foj.left_col.empty()) {
				string dt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_left_name);
				union_parts += "SELECT DISTINCT " + KeywordHelper::WriteOptionallyQuoted(foj.left_col) +
				               " AS _k FROM " + dt + delta_where_left;
			}
			if (!foj.dt_right_name.empty() && !foj.right_col.empty()) {
				if (!union_parts.empty()) {
					union_parts += "\n  UNION\n  ";
				}
				string dt = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(foj.dt_right_name);
				union_parts += "SELECT DISTINCT " + KeywordHelper::WriteOptionallyQuoted(foj.right_col) +
				               " AS _k FROM " + dt + delta_where_right;
			}

			string where_clause;
			string affected_ctes;
			if (!union_parts.empty()) {
				affected_ctes = "WITH _ivm_affected AS (\n  " + union_parts + "\n)\n";
				where_clause =
				    lk + " IN (SELECT _k FROM _ivm_affected) OR " + rk + " IN (SELECT _k FROM _ivm_affected)";
			} else {
				// Fallback: full recompute
				where_clause = "TRUE";
			}

			// CTEs are per-statement — include in both DELETE and INSERT
			upsert_query = affected_ctes + "DELETE FROM " + qdt + " WHERE " + where_clause + ";\n" + affected_ctes +
			               "INSERT INTO " + qdt + "\nSELECT * FROM (" + view_query_sql + ") _ivm_foj\nWHERE " +
			               where_clause + ";\n";
		} else if (has_left_join) {
			string delta_where = delta_ts_filter.empty() ? "" : " AND " + delta_ts_filter;
			const string &qdt = data_table;
			string qdv = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
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
		                                       delta_ts_filter, catalog_prefix, insert_only, column_types);
		if (!has_minmax) {
			auto source_tables = metadata.GetDeltaTables(view_name);
			for (auto &dt : source_tables) {
				// Strip "delta_" prefix to get base table name (standard tables only;
				// DuckLake tables are stored without the prefix)
				string base_name =
				    StringUtil::StartsWith(dt, ivm::DELTA_PREFIX) ? dt.substr(strlen(ivm::DELTA_PREFIX)) : dt;
				string source = catalog_prefix + KeywordHelper::WriteOptionallyQuoted(base_name);
				string null_cols;
				for (auto &col : column_names) {
					if (col != string(ivm::MULTIPLICITY_COL)) {
						if (!null_cols.empty()) {
							null_cols += ", ";
						}
						null_cols += KeywordHelper::WriteOptionallyQuoted(col) + " = NULL";
					}
				}
				upsert_query += "UPDATE " + data_table + " SET " + null_cols + " WHERE NOT EXISTS (SELECT 1 FROM " +
				                source + " LIMIT 1);\n";
			}
		}
		break;
	}
	case IVMType::WINDOW_PARTITION: {
		// Window functions: partition-level recompute — delete+re-insert affected partitions.
		auto partition_cols = metadata.GetGroupColumns(view_name); // reuses group_columns field
		// For DuckLake base tables the delta source isn't a `delta_<T>` twin with
		// `_duckdb_ivm_timestamp` — the delta_table_names entry is the DuckLake base name
		// itself. Build the affected-partition filter via DuckLake snapshot diff instead:
		// symmetric difference between current and last_snapshot_id.
		bool any_ducklake = false;
		for (auto &dt : delta_table_names) {
			if (metadata.IsDuckLakeTable(view_name, dt)) {
				any_ducklake = true;
				break;
			}
		}
		// Only use snapshot-diff recompute when safe: single base table AND every partition col
		// is in the MV's output (so the WHERE can filter). Otherwise fall back to full recompute —
		// CompileWindowRecompute's native path references _duckdb_ivm_timestamp which DuckLake
		// tables lack, so we must not reach it for any DuckLake window.
		bool safe_for_snapdiff = any_ducklake && !partition_cols.empty() && delta_table_names.size() == 1;
		if (safe_for_snapdiff) {
			for (auto &pc : partition_cols) {
				if (std::find(column_names.begin(), column_names.end(), pc) == column_names.end()) {
					safe_for_snapdiff = false;
					break;
				}
			}
		}
		if (safe_for_snapdiff) {
			const string &base_name = delta_table_names[0];
			int64_t old_snap = metadata.GetLastSnapshotId(view_name, base_name);
			// For PRAGMA ivm('mv'), attached_db_catalog_name is empty — use view_catalog_name
			// (the catalog hosting the DuckLake MV, which also hosts its base tables).
			string dl_catalog = attached_db_catalog_name.empty() ? view_catalog_name : attached_db_catalog_name;
			string dl_schema = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
			string qualified_base =
			    dl_catalog + "." + dl_schema + "." + KeywordHelper::WriteOptionallyQuoted(base_name);
			string snap_clause = " AT (VERSION => " + to_string(old_snap) + ")";
			string diff_sql = "(SELECT * FROM " + qualified_base + " EXCEPT ALL SELECT * FROM " + qualified_base +
			                  snap_clause + ") UNION ALL (SELECT * FROM " + qualified_base + snap_clause +
			                  " EXCEPT ALL SELECT * FROM " + qualified_base + ")";
			string affected_filter;
			for (size_t i = 0; i < partition_cols.size(); i++) {
				if (i > 0) {
					affected_filter += " OR ";
				}
				string col = KeywordHelper::WriteOptionallyQuoted(partition_cols[i]);
				affected_filter +=
				    col + " IN (SELECT DISTINCT " + col + " FROM (" + diff_sql + ") _ivm_diff_" + to_string(i) + ")";
			}
			upsert_query = "DELETE FROM " + data_table + " WHERE " + affected_filter + ";\n" + "INSERT INTO " +
			               data_table + "\nSELECT * FROM (" + view_query_sql + ") _ivm_recompute\nWHERE " +
			               affected_filter + ";\n";
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake snapshot-diff, %zu "
			                    "partition cols, old_snap=%ld)\n",
			                    partition_cols.size(), (long)old_snap);
		} else if (any_ducklake) {
			// DuckLake edge case: multi-table window or partition col not in MV output.
			// Full recompute — documented in docs/limitations.md.
			upsert_query =
			    "DELETE FROM " + data_table + ";\n" + "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
			OPENIVM_DEBUG_PRINT(
			    "[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake, full recompute fallback)\n");
		} else {
			upsert_query = CompileWindowRecompute(view_name, view_query_sql, delta_ts_filter, catalog_prefix,
			                                      partition_cols, delta_table_names);
			OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (%zu partition cols)\n",
			                    partition_cols.size());
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
	string companion_query;
	string pre_companion;
	string post_companion;
	string delete_from_view_query;

	if (view_query_type == IVMType::WINDOW_PARTITION) {
		// Window views skip the DoIVM/LPTS path because LPTS doesn't support WINDOW.
		// Partition detection uses base delta tables directly via CompileWindowRecompute.
		OPENIVM_DEBUG_PRINT("[UPSERT] Skipping DoIVM for WINDOW_PARTITION (LPTS limitation)\n");
		ivm_query = "";
	} else {
		// splitting the query in two to make it easier to turn into string (insertions are the same)
		string do_ivm = "select * from DoIVM('" + OpenIVMUtils::EscapeValue(view_catalog_name) + "','" +
		                OpenIVMUtils::EscapeValue(view_schema_name) + "','" + OpenIVMUtils::EscapeValue(view_name) +
		                "');";

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
		// For SIMPLE_AGGREGATE / SIMPLE_PROJECTION with downstream consumers:
		// The IVM delta represents the CHANGE (+5), but downstream projections need
		// the ABSOLUTE old and new values to compute their own deltas correctly.
		// Strategy: record old state (mul=false) BEFORE upsert, then record new state
		// (mul=true) AFTER upsert. The IVM delta in delta_view is replaced by these
		// absolute snapshots so downstream sees a clean old→new transition.

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
			const string &qdt2 = data_table;
			pre_companion = "CREATE TEMP TABLE " + qt + " AS SELECT * FROM " + qdt2 + ";\n";
			// Post: clear ALL IVM delta rows (both true and false), replace with absolute snapshots
			post_companion = "DELETE FROM " + qdvn + " WHERE 1=1";
			if (!delta_ts_filter.empty()) {
				post_companion += " AND " + delta_ts_filter;
			}
			post_companion += ";\n";
			post_companion +=
			    "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_false + " FROM " + qt + ";\n";
			post_companion +=
			    "INSERT INTO " + qdvn + " (" + col_list + ") SELECT " + select_true + " FROM " + qdt2 + ";\n";
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

		if (has_downstream) {
			delete_from_view_query = IVMMetadata::BuildDeltaCleanupSQL(delta_view_name, delta_view_name);
		} else {
			delete_from_view_query = "DELETE FROM " + delta_view_name + ";";
		}
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
			const string &cat_name = view_catalog_name; // default to view's catalog
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
	//
	// For cross_system (DuckLake) MVs, steps 0 and 6-7 write to the physical-default catalog
	// (metadata tables) while steps 1-5 and 8-9 write to dl. DuckDB forbids cross-catalog
	// writes in one transaction. When out_pre_meta/out_post_meta are provided, we split them.
	string data_sql = pre_companion + ivm_query + "\n" + companion_query + "\n" + upsert_query + "\n" + post_companion +
	                  delete_from_view_query + "\n" + delete_from_delta_table_query;
	string meta_pre_sql = set_in_progress;
	string meta_post_sql = update_timestamp_query + snapshot_update_query + "\n" + clear_in_progress;

	string clean_query;
	if (cross_system && out_pre_meta != nullptr && out_post_meta != nullptr) {
		*out_pre_meta = meta_pre_sql;
		*out_post_meta = meta_post_sql;
		clean_query = data_sql;
	} else {
		clean_query = meta_pre_sql + data_sql + meta_post_sql;
	}

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
