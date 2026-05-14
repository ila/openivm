#include "upsert/refresh_internal.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"

namespace duckdb {

static std::pair<string, string> SplitPartitionSpec(const string &raw) {
	auto pos = raw.find('=');
	if (pos == string::npos) {
		return std::make_pair(raw, raw);
	}
	return std::make_pair(raw.substr(0, pos), raw.substr(pos + 1));
}

static bool DeltaHasColumn(Connection &con, const string &delta_table, const string &column_name) {
	auto col_result =
	    con.Query("SELECT 1 FROM information_schema.columns WHERE table_name = '" + SqlUtils::EscapeValue(delta_table) +
	              "' AND lower(column_name) = lower('" + SqlUtils::EscapeValue(column_name) + "') LIMIT 1");
	return !col_result->HasError() && col_result->RowCount() > 0;
}

static vector<WindowPartitionDeltaSpec> BuildWindowPartitionDeltaSpecs(RefreshMetadata &metadata, Connection &con,
                                                                       const string &view_name,
                                                                       const vector<string> &delta_table_names,
                                                                       const vector<string> &partition_cols,
                                                                       bool cross_system) {
	vector<WindowPartitionDeltaSpec> partition_delta_specs;
	for (auto &raw_partition_col : partition_cols) {
		auto parsed = SplitPartitionSpec(raw_partition_col);
		for (auto &dt : delta_table_names) {
			if (metadata.IsDuckLakeTable(view_name, dt)) {
				continue;
			}
			if (DeltaHasColumn(con, dt, parsed.second)) {
				string delta_table_sql =
				    cross_system ? metadata.ResolveDeltaQualifiedName(view_name, dt) : SqlUtils::QuoteIdentifier(dt);
				partition_delta_specs.push_back({dt, delta_table_sql, parsed.first, parsed.second});
			}
		}
	}
	return partition_delta_specs;
}

static bool AnyDuckLakeSource(RefreshMetadata &metadata, const string &view_name,
                              const vector<string> &delta_table_names) {
	for (auto &dt : delta_table_names) {
		if (metadata.IsDuckLakeTable(view_name, dt)) {
			return true;
		}
	}
	return false;
}

static bool IsSafeForDuckLakeSnapshotDiff(const vector<string> &partition_cols, const vector<string> &column_names,
                                          bool any_ducklake) {
	if (!any_ducklake || partition_cols.empty()) {
		return false;
	}
	for (auto &pc : partition_cols) {
		auto parsed = SplitPartitionSpec(pc);
		if (std::find(column_names.begin(), column_names.end(), parsed.first) == column_names.end()) {
			return false;
		}
	}
	return true;
}

static string BuildSingleSourceDuckLakeWindowRefresh(RefreshMetadata &metadata, Connection &con,
                                                     const string &view_name, const string &view_query_sql,
                                                     const vector<string> &partition_cols, const string &data_table,
                                                     const string &view_catalog_name, const string &view_schema_name,
                                                     const string &attached_db_catalog_name,
                                                     const string &attached_db_schema_name, const string &base_name) {
	int64_t old_snap = metadata.GetLastSnapshotId(view_name, base_name);
	auto loc = ResolveDuckLakeSourceLocation(con, view_name, base_name, view_catalog_name, view_schema_name,
	                                         attached_db_catalog_name, attached_db_schema_name);
	int64_t current_snap = old_snap;
	auto snapshot_id = metadata.GetCurrentDuckLakeSnapshot(loc.catalog_name);
	if (snapshot_id >= 0) {
		current_snap = snapshot_id;
	}

	string affected_cols;
	string affected_select;
	string affected_tuple;
	for (size_t i = 0; i < partition_cols.size(); i++) {
		if (i > 0) {
			affected_cols += ", ";
			affected_select += ", ";
			affected_tuple += ", ";
		}
		auto parsed = SplitPartitionSpec(partition_cols[i]);
		affected_cols += KeywordHelper::WriteOptionallyQuoted(parsed.first);
		affected_select += KeywordHelper::WriteOptionallyQuoted(parsed.second) + " AS " +
		                   KeywordHelper::WriteOptionallyQuoted(parsed.first);
		affected_tuple += KeywordHelper::WriteOptionallyQuoted(parsed.first);
	}
	string temp_affected = string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name;
	string qtemp_affected = KeywordHelper::WriteOptionallyQuoted(temp_affected);
	string insertions = "SELECT " + affected_select + " FROM ducklake_table_insertions('" +
	                    SqlUtils::EscapeValue(loc.catalog_name) + "', '" + SqlUtils::EscapeValue(loc.schema_name) +
	                    "', '" + SqlUtils::EscapeValue(loc.table_name) + "', " + to_string(old_snap) + ", " +
	                    to_string(current_snap) + ")";
	string deletions = "SELECT " + affected_select + " FROM ducklake_table_deletions('" +
	                   SqlUtils::EscapeValue(loc.catalog_name) + "', '" + SqlUtils::EscapeValue(loc.schema_name) +
	                   "', '" + SqlUtils::EscapeValue(loc.table_name) + "', " + to_string(old_snap) + ", " +
	                   to_string(current_snap) + ")";
	string affected_filter;
	if (partition_cols.size() == 1) {
		affected_filter = affected_tuple + " IN (SELECT " + affected_cols + " FROM " + qtemp_affected + ")";
	} else {
		affected_filter = "(" + affected_tuple + ") IN (SELECT " + affected_cols + " FROM " + qtemp_affected + ")";
	}
	string upsert_query = "CREATE TEMP TABLE " + qtemp_affected + " AS SELECT DISTINCT " + affected_cols + " FROM ((" +
	                      insertions + ") UNION ALL (" + deletions + ")) openivm_changed_partitions;\n";
	upsert_query += "DELETE FROM " + data_table + " WHERE " + affected_filter + ";\n";
	upsert_query += "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql + ") openivm_recompute\nWHERE " +
	                affected_filter + ";\n";
	upsert_query += "DROP TABLE " + qtemp_affected + ";\n";
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake change-feed, %zu "
	                    "partition cols, old_snap=%ld, current_snap=%ld)\n",
	                    partition_cols.size(), (long)old_snap, (long)current_snap);
	return upsert_query;
}

static string BuildMultiSourceDuckLakeWindowRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                                    const string &view_query_sql,
                                                    const vector<string> &delta_table_names,
                                                    const vector<string> &partition_cols, const string &data_table,
                                                    const string &view_catalog_name, const string &view_schema_name,
                                                    const string &attached_db_catalog_name,
                                                    const string &attached_db_schema_name) {
	string old_view_query =
	    BuildDuckLakeSnapshotQuery(metadata, con, view_name, view_query_sql, delta_table_names, view_catalog_name,
	                               view_schema_name, attached_db_catalog_name, attached_db_schema_name);
	string key_cols;
	string key_tuple;
	for (size_t i = 0; i < partition_cols.size(); i++) {
		if (i > 0) {
			key_cols += ", ";
			key_tuple += ", ";
		}
		auto parsed = SplitPartitionSpec(partition_cols[i]);
		string output_col = KeywordHelper::WriteOptionallyQuoted(parsed.first);
		key_cols += output_col;
		key_tuple += output_col;
	}
	string current_rows = "SELECT * FROM (" + view_query_sql + ") openivm_current_rows";
	string old_rows = "SELECT * FROM (" + old_view_query + ") openivm_old_rows";
	string changed_rows = "((" + current_rows + ") EXCEPT ALL (" + old_rows + ")) UNION ALL ((" + old_rows +
	                      ") EXCEPT ALL (" + current_rows + "))";
	string affected_keys = "SELECT DISTINCT " + key_cols + " FROM (" + changed_rows + ") openivm_changed_rows";
	string affected_filter;
	if (partition_cols.size() == 1) {
		affected_filter = key_tuple + " IN (" + affected_keys + ")";
	} else {
		affected_filter = "(" + key_tuple + ") IN (" + affected_keys + ")";
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake view-diff, %zu "
	                    "partition cols, %zu sources)\n",
	                    partition_cols.size(), delta_table_names.size());
	return "DELETE FROM " + data_table + " WHERE " + affected_filter + ";\n" + "INSERT INTO " + data_table +
	       "\nSELECT * FROM (" + view_query_sql + ") openivm_recompute\nWHERE " + affected_filter + ";\n";
}

string BuildWindowPartitionRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const string &view_query_sql, const vector<string> &delta_table_names,
                                   const vector<string> &column_names, const string &data_table,
                                   const string &delta_ts_filter, const string &internal_catalog_prefix,
                                   const string &view_catalog_name, const string &view_schema_name,
                                   const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                   bool cross_system) {
	auto partition_cols = metadata.GetGroupColumns(view_name); // reuses group_columns field
	auto partition_delta_specs =
	    BuildWindowPartitionDeltaSpecs(metadata, con, view_name, delta_table_names, partition_cols, cross_system);
	bool any_ducklake = AnyDuckLakeSource(metadata, view_name, delta_table_names);
	bool safe_for_snapdiff = IsSafeForDuckLakeSnapshotDiff(partition_cols, column_names, any_ducklake);

	if (safe_for_snapdiff && delta_table_names.size() == 1) {
		return BuildSingleSourceDuckLakeWindowRefresh(
		    metadata, con, view_name, view_query_sql, partition_cols, data_table, view_catalog_name, view_schema_name,
		    attached_db_catalog_name, attached_db_schema_name, delta_table_names[0]);
	}
	if (safe_for_snapdiff && any_ducklake) {
		return BuildMultiSourceDuckLakeWindowRefresh(metadata, con, view_name, view_query_sql, delta_table_names,
		                                             partition_cols, data_table, view_catalog_name, view_schema_name,
		                                             attached_db_catalog_name, attached_db_schema_name);
	}
	if (any_ducklake) {
		OPENIVM_DEBUG_PRINT(
		    "[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake, full recompute fallback)\n");
		return "DELETE FROM " + data_table + ";\n" + "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (%zu partition cols)\n",
	                    partition_cols.size());
	return CompileWindowRecompute(view_name, view_query_sql, delta_ts_filter, internal_catalog_prefix, partition_cols,
	                              partition_delta_specs);
}

} // namespace duckdb
