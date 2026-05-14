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

struct DeltaChangeSummary {
	bool has_join = false;
	idx_t tables_with_changes = 0;
	bool any_has_deletes = false;
	bool all_ducklake = true;
};

static void AccumulateDuckLakeDeltaSummary(DeltaChangeSummary &summary, RefreshMetadata &metadata, Connection &con,
                                           const string &view_name, const string &delta_table,
                                           const string &view_catalog_name, const string &view_schema_name,
                                           const string &attached_db_catalog_name,
                                           const string &attached_db_schema_name) {
	auto loc = ResolveDuckLakeSourceLocation(con, view_name, delta_table, view_catalog_name, view_schema_name,
	                                         attached_db_catalog_name, attached_db_schema_name);
	auto last_snap = metadata.GetLastSnapshotId(view_name, delta_table);
	auto cur_snap = metadata.GetCurrentDuckLakeSnapshot(loc.catalog_name);
	if (cur_snap < 0) {
		summary.any_has_deletes = true; // conservative
		summary.tables_with_changes++;
		return;
	}
	if (last_snap == cur_snap) {
		return;
	}
	string insertions = SqlUtils::DuckLakeTableFunction("ducklake_table_insertions", loc.catalog_name, loc.schema_name,
	                                                    loc.table_name, last_snap, cur_snap);
	string deletions = SqlUtils::DuckLakeTableFunction("ducklake_table_deletions", loc.catalog_name, loc.schema_name,
	                                                   loc.table_name, last_snap, cur_snap);
	auto count_result =
	    con.Query("SELECT (SELECT COUNT(*) FROM " + insertions + "), (SELECT COUNT(*) FROM " + deletions + ")");
	if (count_result->HasError()) {
		summary.any_has_deletes = true;
		summary.tables_with_changes++;
		return;
	}
	auto insert_count = count_result->GetValue(0, 0).GetValue<int64_t>();
	auto delete_count = count_result->GetValue(1, 0).GetValue<int64_t>();
	if (insert_count == 0 && delete_count == 0) {
		return;
	}
	summary.tables_with_changes++;
	if (delete_count > 0) {
		summary.any_has_deletes = true;
	}
}

static void AccumulateStandardDeltaSummary(DeltaChangeSummary &summary, RefreshMetadata &metadata,
                                           const string &view_name, const string &delta_table, bool cross_system) {
	summary.all_ducklake = false;
	auto ts_string = metadata.GetLastUpdate(view_name, delta_table);
	if (ts_string.empty()) {
		return;
	}
	string delta_table_sql = cross_system ? metadata.ResolveDeltaQualifiedName(view_name, delta_table)
	                                      : SqlUtils::QuoteIdentifier(delta_table);
	auto stats = metadata.GetStandardDeltaChangeStats(delta_table_sql, ts_string);
	if (!stats.ok || stats.total == 0) {
		return;
	}
	summary.tables_with_changes++;
	if (stats.deletes > 0) {
		summary.any_has_deletes = true;
	}
}

static bool IsInsertOnlyDeltaSummary(const DeltaChangeSummary &summary, const vector<string> &delta_table_names) {
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

static DeltaChangeSummary AnalyzeDeltaChanges(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                              const string &view_query_sql, const vector<string> &delta_table_names,
                                              const string &view_catalog_name, const string &view_schema_name,
                                              const string &attached_db_catalog_name,
                                              const string &attached_db_schema_name, bool cross_system) {
	DeltaChangeSummary summary;
	summary.has_join = QueryLooksLikeJoin(view_query_sql, delta_table_names);
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			AccumulateDuckLakeDeltaSummary(summary, metadata, con, view_name, dt, view_catalog_name, view_schema_name,
			                               attached_db_catalog_name, attached_db_schema_name);
		} else {
			AccumulateStandardDeltaSummary(summary, metadata, view_name, dt, cross_system);
		}
	}
	return summary;
}

DeltaFastPathFlags ResolveDeltaFastPathFlags(ClientContext &context, RefreshMetadata &metadata, Connection &con,
                                             const string &view_name, const string &view_query_sql,
                                             const vector<string> &delta_table_names, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name, bool cross_system) {
	auto summary =
	    AnalyzeDeltaChanges(metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name,
	                        view_schema_name, attached_db_catalog_name, attached_db_schema_name, cross_system);
	DeltaFastPathFlags flags;
	flags.insert_only = IsInsertOnlyDeltaSummary(summary, delta_table_names);
	flags.skip_agg_delete = flags.insert_only;
	flags.skip_proj_delete = flags.insert_only;
	flags.minmax_incremental = flags.insert_only;

	Value skip_agg_del_val, skip_proj_del_val, minmax_incr_val;
	if (context.TryGetCurrentSetting("openivm_skip_aggregate_delete", skip_agg_del_val) && !skip_agg_del_val.IsNull() &&
	    !skip_agg_del_val.GetValue<bool>()) {
		flags.skip_agg_delete = false;
	}
	if (context.TryGetCurrentSetting("openivm_skip_projection_delete", skip_proj_del_val) &&
	    !skip_proj_del_val.IsNull() && !skip_proj_del_val.GetValue<bool>()) {
		flags.skip_proj_delete = false;
	}
	if (context.TryGetCurrentSetting("openivm_minmax_incremental", minmax_incr_val) && !minmax_incr_val.IsNull() &&
	    !minmax_incr_val.GetValue<bool>()) {
		flags.minmax_incremental = false;
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] insert_only=%d, skip_agg_delete=%d, skip_proj_delete=%d, minmax_incremental=%d\n",
	                    flags.insert_only, flags.skip_agg_delete, flags.skip_proj_delete, flags.minmax_incremental);
	return flags;
}

} // namespace duckdb
