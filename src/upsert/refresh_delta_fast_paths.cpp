#include "upsert/refresh_internal.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

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

	auto has_inserts =
	    !result->Collection().GetValue(0, 0).IsNull() && result->Collection().GetValue(0, 0).GetValue<bool>();
	auto has_deletes =
	    !result->Collection().GetValue(1, 0).IsNull() && result->Collection().GetValue(1, 0).GetValue<bool>();
	auto has_structural =
	    !result->Collection().GetValue(2, 0).IsNull() && result->Collection().GetValue(2, 0).GetValue<bool>();
	activity.ok = true;
	activity.has_changes = has_inserts || has_deletes || has_structural;
	activity.has_deletes = has_deletes || has_structural;
	activity.requires_full_refresh = has_structural;
	return activity;
}

DuckLakeTableActivity ProbeDuckLakeTableChanges(Connection &con, const DuckLakeSourceLocation &loc, int64_t table_id,
                                                int64_t after_snapshot_id, int64_t through_snapshot_id) {
	DuckLakeTableActivity activity;
	if (after_snapshot_id < 0 || through_snapshot_id < after_snapshot_id) {
		return activity;
	}
	if (after_snapshot_id == through_snapshot_id) {
		activity.ok = true;
		return activity;
	}
	if (table_id >= 0) {
		// The manifest also reports ALTER and DROP, and a recreated table drops the old id.
		auto sql = BuildDuckLakeSnapshotActivitySQL(loc.catalog_name, table_id, after_snapshot_id, through_snapshot_id);
		auto result = con.Query(sql);
		if (!result->HasError() && result->RowCount() == 1) {
			activity.ok = true;
			for (idx_t col = 0; col < 3; col++) {
				auto changed = result->Collection().GetValue(col, 0);
				activity.has_changes = activity.has_changes || (!changed.IsNull() && changed.GetValue<bool>());
			}
			return activity;
		}
	}
	RefreshMetadata metadata(con);
	auto stats = GetDuckLakeDeltaChangeStats(metadata, loc, after_snapshot_id, through_snapshot_id);
	activity.ok = stats.ok;
	activity.has_changes = stats.total > 0;
	return activity;
}

// Records a source. One whose changes could not be verified (`!ok`) counts as changed with deletes.
static void AddDeltaSource(DeltaActivityResult &summary, DeltaActivityResult::Source source) {
	if (!source.ok) {
		source.has_changes = true;
		source.has_deletes = true;
	}
	if (source.has_changes) {
		summary.tables_with_changes++;
		summary.active_delta_table_names.push_back(source.delta_table_name);
		summary.any_has_deletes = summary.any_has_deletes || source.has_deletes;
		summary.requires_full_refresh = summary.requires_full_refresh || source.requires_full_refresh;
	}
	summary.sources.push_back(std::move(source));
}

static void ApplyChangeStats(DeltaActivityResult::Source &source, const RefreshMetadata::DeltaChangeStats &stats) {
	source.ok = stats.ok;
	if (stats.ok) {
		source.pending_rows = stats.total;
		source.pending_deletes = stats.deletes;
		source.has_changes = stats.total > 0;
		source.has_deletes = stats.deletes > 0;
	}
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
	if (cur_snap < 0 || last_snap < 0 || last_snap == cur_snap) {
		source.ok = last_snap == cur_snap && cur_snap >= 0;
		AddDeltaSource(summary, std::move(source));
		return;
	}
	auto metadata_activity = ProbeDuckLakeSnapshotActivity(metadata, con, view_name, delta_table, loc.catalog_name,
	                                                       loc.schema_name, last_snap, cur_snap);
	if (metadata_activity.ok) {
		source.ok = true;
		source.has_changes = metadata_activity.has_changes;
		source.has_deletes = metadata_activity.has_deletes;
		source.requires_full_refresh = metadata_activity.requires_full_refresh;
	} else {
		ApplyChangeStats(source, GetDuckLakeDeltaChangeStats(metadata, loc, last_snap, cur_snap));
	}
	if (source.ok && !source.has_changes) {
		// Nothing in the range touched this table; consume the unrelated snapshots.
		summary.ducklake_snapshot_advances.push_back({delta_table, cur_snap});
	}
	AddDeltaSource(summary, std::move(source));
}

static void AccumulateStandardDeltaSummary(DeltaActivityResult &summary, RefreshMetadata &metadata,
                                           const string &view_name, const string &delta_table,
                                           const string &view_catalog_name, const string &view_schema_name) {
	summary.all_ducklake = false;
	DeltaActivityResult::Source source;
	source.delta_table_name = delta_table;
	source.source_table_name = BaseTableNameFromDeltaKey(delta_table);
	// An empty watermark yields unverified stats.
	ApplyChangeStats(
	    source, metadata.GetStandardDeltaChangeStats(
	                metadata.ResolveDeltaQualifiedName(view_name, delta_table, view_catalog_name, view_schema_name),
	                metadata.GetLastUpdate(view_name, delta_table)));
	AddDeltaSource(summary, std::move(source));
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

DeltaActivityResult BuildDeltaActivityResult(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                             const vector<string> &delta_table_names, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name) {
	DeltaActivityResult summary;
	summary.has_join = metadata.HasJoin(view_name) || delta_table_names.size() > 1;
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			AccumulateDuckLakeDeltaSummary(summary, metadata, con, view_name, dt, view_catalog_name, view_schema_name,
			                               attached_db_catalog_name, attached_db_schema_name);
		} else {
			AccumulateStandardDeltaSummary(summary, metadata, view_name, dt, view_catalog_name, view_schema_name);
		}
	}
	return summary;
}

DeltaFastPathFlags ResolveDeltaFastPathFlags(ClientContext &context, RefreshMetadata &metadata, Connection &con,
                                             const string &view_name, const vector<string> &delta_table_names,
                                             const string &view_catalog_name, const string &view_schema_name,
                                             const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name,
                                             const DeltaActivityResult *precomputed_delta_activity,
                                             const openivm::CompileFacts *facts) {
	if (facts && facts->compile_only) {
		DeltaFastPathFlags flags;
		flags.active_delta_table_names = delta_table_names;
		// When an external classifier has PROVEN this batch is append-only,
		// re-enable the insert-only fast paths even under compile_only (they are
		// otherwise disabled because compile_only sees empty tables and cannot
		// detect insert-only itself). Honor the same per-setting kill switches as
		// the normal path. Without the flag we keep the conservative all-delta SQL.
		if (facts->assume_insert_only) {
			flags.insert_only = true;
			flags.skip_agg_delete = SqlUtils::GetBoolSetting(context, "openivm_skip_aggregate_delete", true);
			flags.skip_proj_delete = SqlUtils::GetBoolSetting(context, "openivm_skip_projection_delete", true);
			flags.minmax_incremental = SqlUtils::GetBoolSetting(context, "openivm_minmax_incremental", true);
		}
		OPENIVM_DEBUG_PRINT("[UPSERT] compile_only=true: assume_insert_only=%d insert_only=%d skip_agg_delete=%d "
		                    "skip_proj_delete=%d minmax_incremental=%d active_sources=%zu\n",
		                    facts->assume_insert_only, flags.insert_only, flags.skip_agg_delete, flags.skip_proj_delete,
		                    flags.minmax_incremental, flags.active_delta_table_names.size());
		return flags;
	}
	auto summary = precomputed_delta_activity
	                   ? *precomputed_delta_activity
	                   : BuildDeltaActivityResult(metadata, con, view_name, delta_table_names, view_catalog_name,
	                                              view_schema_name, attached_db_catalog_name, attached_db_schema_name);
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
