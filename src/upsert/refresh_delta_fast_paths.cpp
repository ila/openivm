#include "upsert/refresh_internal.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

static bool QueryLooksLikeJoin(const string &view_query_sql, const vector<string> &delta_table_names) {
	return delta_table_names.size() > 1 || view_query_sql.find("JOIN") != string::npos ||
	       view_query_sql.find("join") != string::npos;
}

static string DuckLakeChangeContains(const string &key, const string &table_id) {
	return "COALESCE(list_contains(changes['" + key + "'], '" + table_id + "'), false)";
}

static string BuildDuckLakeSnapshotActivitySQL(const string &catalog_name, int64_t table_id, int64_t last_snapshot_id,
                                               int64_t current_snapshot_id) {
	string id = to_string(table_id);
	string insert_expr =
	    DuckLakeChangeContains("tables_inserted_into", id) + " OR " + DuckLakeChangeContains("inlined_insert", id);
	string delete_expr =
	    DuckLakeChangeContains("tables_deleted_from", id) + " OR " + DuckLakeChangeContains("inlined_delete", id);
	string structural_expr =
	    DuckLakeChangeContains("tables_altered", id) + " OR " + DuckLakeChangeContains("tables_dropped", id);
	return "SELECT COALESCE(bool_or(" + insert_expr + "), false), COALESCE(bool_or(" + delete_expr +
	       "), false), COALESCE(bool_or(" + structural_expr + "), false) FROM ducklake_snapshots('" +
	       SqlUtils::EscapeValue(catalog_name) + "') WHERE snapshot_id > " + to_string(last_snapshot_id) +
	       " AND snapshot_id <= " + to_string(current_snapshot_id);
}

DuckLakeTableActivity ProbeDuckLakeSnapshotActivity(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                                    const string &table_name, const string &catalog_name,
                                                    const string &schema_name, int64_t last_snapshot_id,
                                                    int64_t current_snapshot_id) {
	DuckLakeTableActivity activity;
	if (current_snapshot_id < 0 || last_snapshot_id < 0) {
		return activity;
	}
	if (last_snapshot_id == current_snapshot_id) {
		activity.ok = true;
		return activity;
	}

	auto identity = metadata.ResolveDuckLakeSourceIdentity(view_name, table_name, catalog_name, schema_name);
	if (!identity.resolved || identity.current_table_id < 0) {
		return activity;
	}
	if (identity.changed) {
		activity.ok = true;
		activity.has_changes = true;
		activity.has_deletes = true;
		activity.requires_full_refresh = true;
		return activity;
	}

	// `ducklake_snapshots().changes` is a cheap catalog-level manifest keyed by DuckLake table id.
	// It lets us skip unrelated catalog snapshots without opening table_insertions/deletions.
	// Structural changes invalidate row-level deltas, so they force a full recompute.
	auto result = con.Query(BuildDuckLakeSnapshotActivitySQL(catalog_name, identity.current_table_id, last_snapshot_id,
	                                                         current_snapshot_id));
	if (result->HasError() || result->RowCount() == 0) {
		return activity;
	}

	auto has_inserts = !result->GetValue(0, 0).IsNull() && result->GetValue(0, 0).GetValue<bool>();
	auto has_deletes = !result->GetValue(1, 0).IsNull() && result->GetValue(1, 0).GetValue<bool>();
	auto has_structural = !result->GetValue(2, 0).IsNull() && result->GetValue(2, 0).GetValue<bool>();
	activity.ok = true;
	activity.has_changes = has_inserts || has_deletes || has_structural;
	activity.has_deletes = has_deletes || has_structural;
	activity.requires_full_refresh = has_structural;
	return activity;
}

static void AccumulateDuckLakeDeltaSummary(DeltaActivityResult &summary, RefreshMetadata &metadata, Connection &con,
                                           const string &view_name, const string &delta_table,
                                           const string &view_catalog_name, const string &view_schema_name,
                                           const string &attached_db_catalog_name,
                                           const string &attached_db_schema_name) {
	auto loc = ResolveDuckLakeSourceLocation(con, view_name, delta_table, view_catalog_name, view_schema_name,
	                                         attached_db_catalog_name, attached_db_schema_name);
	auto last_snap = metadata.GetLastSnapshotId(view_name, delta_table);
	auto cur_snap = metadata.GetCurrentDuckLakeSnapshot(loc.catalog_name);
	DeltaActivityResult::Source source;
	source.delta_table_name = delta_table;
	source.source_table_name = loc.table_name.empty() ? delta_table : loc.table_name;
	source.catalog_name = loc.catalog_name;
	source.schema_name = loc.schema_name;
	source.ducklake = true;
	source.last_snapshot_id = last_snap;
	source.current_snapshot_id = cur_snap;
	if (cur_snap < 0 || last_snap < 0) {
		source.has_changes = true;
		source.has_deletes = true;
		summary.any_has_deletes = true; // conservative
		summary.tables_with_changes++;
		summary.active_delta_table_names.push_back(delta_table);
		summary.sources.push_back(std::move(source));
		return;
	}
	if (last_snap == cur_snap) {
		source.ok = true;
		summary.sources.push_back(std::move(source));
		return;
	}
	auto metadata_activity = ProbeDuckLakeSnapshotActivity(metadata, con, view_name, delta_table, loc.catalog_name,
	                                                       loc.schema_name, last_snap, cur_snap);
	if (metadata_activity.ok) {
		source.ok = true;
		source.has_changes = metadata_activity.has_changes;
		source.has_deletes = metadata_activity.has_deletes;
		source.requires_full_refresh = metadata_activity.requires_full_refresh;
		if (!metadata_activity.has_changes) {
			summary.ducklake_snapshot_advances.push_back({delta_table, cur_snap});
			summary.sources.push_back(std::move(source));
			return;
		}
		if (metadata_activity.requires_full_refresh) {
			summary.requires_full_refresh = true;
		}
		summary.tables_with_changes++;
		summary.active_delta_table_names.push_back(delta_table);
		if (metadata_activity.has_deletes) {
			summary.any_has_deletes = true;
		}
		summary.sources.push_back(std::move(source));
		return;
	}

	string insertions = SqlUtils::DuckLakeTableFunction("ducklake_table_insertions", loc.catalog_name, loc.schema_name,
	                                                    loc.table_name, last_snap, cur_snap);
	string deletions = SqlUtils::DuckLakeTableFunction("ducklake_table_deletions", loc.catalog_name, loc.schema_name,
	                                                   loc.table_name, last_snap, cur_snap);
	auto count_result =
	    con.Query("SELECT (SELECT COUNT(*) FROM " + insertions + "), (SELECT COUNT(*) FROM " + deletions + ")");
	if (count_result->HasError()) {
		source.has_changes = true;
		source.has_deletes = true;
		summary.any_has_deletes = true;
		summary.tables_with_changes++;
		summary.active_delta_table_names.push_back(delta_table);
		summary.sources.push_back(std::move(source));
		return;
	}
	auto insert_count = count_result->GetValue(0, 0).GetValue<int64_t>();
	auto delete_count = count_result->GetValue(1, 0).GetValue<int64_t>();
	source.ok = true;
	source.pending_rows = insert_count + delete_count;
	source.pending_deletes = delete_count;
	source.has_changes = source.pending_rows > 0;
	source.has_deletes = delete_count > 0;
	if (insert_count == 0 && delete_count == 0) {
		summary.ducklake_snapshot_advances.push_back({delta_table, cur_snap});
		summary.sources.push_back(std::move(source));
		return;
	}
	summary.tables_with_changes++;
	summary.active_delta_table_names.push_back(delta_table);
	if (delete_count > 0) {
		summary.any_has_deletes = true;
	}
	summary.sources.push_back(std::move(source));
}

static void AccumulateStandardDeltaSummary(DeltaActivityResult &summary, RefreshMetadata &metadata,
                                           const string &view_name, const string &delta_table,
                                           const string &view_catalog_name, const string &view_schema_name) {
	summary.all_ducklake = false;
	DeltaActivityResult::Source source;
	source.delta_table_name = delta_table;
	source.source_table_name = BaseTableNameFromDeltaKey(delta_table);
	source.ducklake = false;
	auto ts_string = metadata.GetLastUpdate(view_name, delta_table);
	if (ts_string.empty()) {
		source.has_changes = true;
		source.has_deletes = true;
		summary.any_has_deletes = true; // conservative
		summary.tables_with_changes++;
		summary.active_delta_table_names.push_back(delta_table);
		summary.sources.push_back(std::move(source));
		return;
	}
	string delta_table_sql =
	    metadata.ResolveDeltaQualifiedName(view_name, delta_table, view_catalog_name, view_schema_name);
	auto stats = metadata.GetStandardDeltaChangeStats(delta_table_sql, ts_string);
	if (!stats.ok) {
		source.has_changes = true;
		source.has_deletes = true;
		summary.any_has_deletes = true; // conservative
		summary.tables_with_changes++;
		summary.active_delta_table_names.push_back(delta_table);
		summary.sources.push_back(std::move(source));
		return;
	}
	source.ok = true;
	source.pending_rows = stats.total;
	source.pending_deletes = stats.deletes;
	source.has_changes = stats.total > 0;
	source.has_deletes = stats.deletes > 0;
	if (stats.total == 0) {
		summary.sources.push_back(std::move(source));
		return;
	}
	summary.tables_with_changes++;
	summary.active_delta_table_names.push_back(delta_table);
	if (stats.deletes > 0) {
		summary.any_has_deletes = true;
	}
	summary.sources.push_back(std::move(source));
}

static bool IsInsertOnlyDeltaSummary(const DeltaActivityResult &summary, const vector<string> &delta_table_names) {
	if (summary.requires_full_refresh) {
		return false;
	}
	if (summary.any_has_deletes) {
		return false;
	}
	if (!summary.has_join) {
		return true;
	}
	if (summary.all_ducklake) {
		return true;
	}
	if (summary.tables_with_changes <= 1 && delta_table_names.size() > 1) {
		return true;
	}
	return false;
}

DeltaActivityProvider DeltaActivityProvider::Build(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                                   const string &view_query_sql,
                                                   const vector<string> &delta_table_names,
                                                   const string &view_catalog_name, const string &view_schema_name,
                                                   const string &attached_db_catalog_name,
                                                   const string &attached_db_schema_name) {
	DeltaActivityProvider provider;
	auto &summary = provider.summary;
	summary.has_join = QueryLooksLikeJoin(view_query_sql, delta_table_names);
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			AccumulateDuckLakeDeltaSummary(summary, metadata, con, view_name, dt, view_catalog_name, view_schema_name,
			                               attached_db_catalog_name, attached_db_schema_name);
		} else {
			AccumulateStandardDeltaSummary(summary, metadata, view_name, dt, view_catalog_name, view_schema_name);
		}
	}
	return provider;
}

DeltaActivityResult BuildDeltaActivityResult(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                             const string &view_query_sql, const vector<string> &delta_table_names,
                                             const string &view_catalog_name, const string &view_schema_name,
                                             const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name) {
	auto provider =
	    DeltaActivityProvider::Build(metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name,
	                                 view_schema_name, attached_db_catalog_name, attached_db_schema_name);
	return provider.Summary();
}

DeltaFastPathFlags ResolveDeltaFastPathFlags(ClientContext &context, RefreshMetadata &metadata, Connection &con,
                                             const string &view_name, const string &view_query_sql,
                                             const vector<string> &delta_table_names, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name, bool cross_system,
                                             const DeltaActivityResult *precomputed_delta_activity,
                                             const openivm::CompileFacts *facts) {
	(void)cross_system;
	if (facts && facts->compile_only) {
		DeltaFastPathFlags flags;
		flags.active_delta_table_names = delta_table_names;
		// NOTE: in compile-only mode (openivm-spark) we cannot probe live delta tables, so insert_only
		// stays false and the active set is ALL sources. The LEFT-JOIN Tier-1 insert-only append
		// therefore never fires here. To enable it on Spark, openivm-spark must pass the per-batch delta
		// activity (which sources changed + insert-only) through CompileFacts so these can be set.
		OPENIVM_DEBUG_PRINT("[UPSERT] compile_only=true: disabling local delta fast paths, active_sources=%zu\n",
		                    flags.active_delta_table_names.size());
		return flags;
	}
	DeltaActivityResult summary;
	if (precomputed_delta_activity) {
		summary = *precomputed_delta_activity;
	} else {
		auto provider =
		    DeltaActivityProvider::Build(metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name,
		                                 view_schema_name, attached_db_catalog_name, attached_db_schema_name);
		summary = provider.Summary();
	}
	DeltaFastPathFlags flags;
	flags.insert_only = IsInsertOnlyDeltaSummary(summary, delta_table_names);
	flags.skip_agg_delete = flags.insert_only;
	flags.skip_proj_delete = flags.insert_only;
	flags.minmax_incremental = flags.insert_only;
	flags.active_delta_table_names = summary.active_delta_table_names;

	if (!SqlUtils::GetBoolSetting(context, "openivm_skip_aggregate_delete", true)) {
		flags.skip_agg_delete = false;
	}
	if (!SqlUtils::GetBoolSetting(context, "openivm_skip_projection_delete", true)) {
		flags.skip_proj_delete = false;
	}
	if (!SqlUtils::GetBoolSetting(context, "openivm_minmax_incremental", true)) {
		flags.minmax_incremental = false;
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] insert_only=%d, skip_agg_delete=%d, skip_proj_delete=%d, minmax_incremental=%d\n",
	                    flags.insert_only, flags.skip_agg_delete, flags.skip_proj_delete, flags.minmax_incremental);
	return flags;
}

} // namespace duckdb
