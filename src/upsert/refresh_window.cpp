#include "upsert/refresh_internal.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"
#include "rules/column_hider.hpp"

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

static string StripTrackedPrefix(const string &name) {
	static const string data_prefix(openivm::DATA_TABLE_PREFIX);
	static const string delta_prefix(openivm::DELTA_PREFIX);
	string last = SqlUtils::LastIdentifierPart(name);
	if (last.size() > data_prefix.size() && last.rfind(data_prefix, 0) == 0) {
		return last.substr(data_prefix.size());
	}
	if (last.size() > delta_prefix.size() && last.rfind(delta_prefix, 0) == 0) {
		return last.substr(delta_prefix.size());
	}
	return last;
}

static string FindTrackedDeltaKey(const vector<string> &delta_table_names, const string &table_name) {
	for (auto &dt : delta_table_names) {
		if (StringUtil::CIEquals(StripTrackedPrefix(dt), StripTrackedPrefix(table_name))) {
			return dt;
		}
	}
	return "";
}

static string ResolveStandardTrackedTableSQL(RefreshMetadata &metadata, const string &view_name,
                                             const vector<string> &delta_table_names, const string &table_name,
                                             bool delta_table, const string &view_catalog_name,
                                             const string &view_schema_name, const string &attached_db_catalog_name,
                                             const string &attached_db_schema_name) {
	string metadata_key = FindTrackedDeltaKey(delta_table_names, table_name);
	if (metadata_key.empty() && !StringUtil::StartsWith(table_name, openivm::DATA_TABLE_PREFIX)) {
		metadata_key = SqlUtils::DeltaName(SqlUtils::LastIdentifierPart(table_name));
	}
	string fallback_catalog = attached_db_catalog_name.empty() ? view_catalog_name : attached_db_catalog_name;
	string fallback_schema = attached_db_schema_name.empty() ? view_schema_name : attached_db_schema_name;
	auto loc = metadata.GetSourceLocation(view_name, metadata_key, fallback_catalog, fallback_schema);
	string resolved_name = delta_table ? metadata_key : SqlUtils::LastIdentifierPart(table_name);
	if (resolved_name.empty()) {
		return "";
	}
	if (loc.catalog_name.empty()) {
		return SqlUtils::QuoteIdentifier(resolved_name);
	}
	if (loc.schema_name.empty()) {
		loc.schema_name = "main";
	}
	return SqlUtils::FullName(loc.catalog_name, loc.schema_name, resolved_name);
}

static string BuildStandardChangedValuesSQL(RefreshMetadata &metadata, const string &view_name,
                                            const vector<string> &delta_table_names, const string &table_name,
                                            const string &source_col, const string &output_col,
                                            const string &delta_ts_filter, const string &view_catalog_name,
                                            const string &view_schema_name, const string &attached_db_catalog_name,
                                            const string &attached_db_schema_name) {
	string delta_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, table_name,
	                                                    /*delta_table=*/true, view_catalog_name, view_schema_name,
	                                                    attached_db_catalog_name, attached_db_schema_name);
	if (delta_table.empty()) {
		return "";
	}
	string filter = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	return "SELECT DISTINCT " + SqlUtils::QuoteIdentifier(source_col) + " AS " + SqlUtils::QuoteIdentifier(output_col) +
	       " FROM " + delta_table + filter;
}

static string BuildStandardLookupChangedKeysSQL(RefreshMetadata &metadata, const string &view_name,
                                                const vector<string> &delta_table_names,
                                                const RefreshMetadata::WindowPartitionLineageOp &op,
                                                const string &delta_ts_filter, const string &view_catalog_name,
                                                const string &view_schema_name, const string &attached_db_catalog_name,
                                                const string &attached_db_schema_name) {
	string delta_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, op.source,
	                                                    /*delta_table=*/true, view_catalog_name, view_schema_name,
	                                                    attached_db_catalog_name, attached_db_schema_name);
	string lookup_table = ResolveStandardTrackedTableSQL(metadata, view_name, delta_table_names, op.lookup,
	                                                     /*delta_table=*/false, view_catalog_name, view_schema_name,
	                                                     attached_db_catalog_name, attached_db_schema_name);
	if (delta_table.empty() || lookup_table.empty()) {
		return "";
	}
	string qsource_col = SqlUtils::QuoteIdentifier(op.source_col);
	string qlookup_col = SqlUtils::QuoteIdentifier(op.lookup_col);
	string qlookup_out = SqlUtils::QuoteIdentifier(op.lookup_out);
	string qoutput_col = SqlUtils::QuoteIdentifier(op.output_col);
	string filter = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	return "SELECT DISTINCT " + qlookup_out + " AS " + qoutput_col + " FROM " + lookup_table + " WHERE " + qlookup_col +
	       " IN (SELECT DISTINCT " + qsource_col + " FROM " + delta_table + filter + ")";
}

static bool BuildLineageStandardAffectedKeysSQL(RefreshMetadata &metadata, const string &view_name,
                                                const vector<string> &delta_table_names,
                                                const vector<string> &partition_cols, const string &delta_ts_filter,
                                                const string &view_catalog_name, const string &view_schema_name,
                                                const string &attached_db_catalog_name,
                                                const string &attached_db_schema_name, string &affected_keys_sql,
                                                string &key_cols, string &key_tuple) {
	if (partition_cols.size() != 1) {
		return false;
	}
	auto parsed = SplitPartitionSpec(partition_cols[0]);
	key_cols = SqlUtils::QuoteIdentifier(parsed.first);
	key_tuple = key_cols;

	vector<RefreshMetadata::WindowPartitionLineageOp> lineage_ops;
	if (!metadata.GetWindowPartitionLineage(view_name, lineage_ops)) {
		return false;
	}

	vector<string> arms;
	unordered_set<string> covered_sources;
	for (auto &op : lineage_ops) {
		if (!StringUtil::CIEquals(op.output_col, parsed.first)) {
			continue;
		}
		string arm_sql;
		if (op.kind == "direct") {
			arm_sql = BuildStandardChangedValuesSQL(metadata, view_name, delta_table_names, op.source, op.source_col,
			                                        op.output_col, delta_ts_filter, view_catalog_name, view_schema_name,
			                                        attached_db_catalog_name, attached_db_schema_name);
		} else if (op.kind == "lookup") {
			arm_sql = BuildStandardLookupChangedKeysSQL(metadata, view_name, delta_table_names, op, delta_ts_filter,
			                                            view_catalog_name, view_schema_name, attached_db_catalog_name,
			                                            attached_db_schema_name);
		}
		if (arm_sql.empty()) {
			continue;
		}
		arms.push_back("(" + arm_sql + ")");
		covered_sources.insert(StripTrackedPrefix(op.source));
	}

	for (auto &dt : delta_table_names) {
		if (!covered_sources.count(StripTrackedPrefix(dt))) {
			return false;
		}
	}
	if (arms.empty()) {
		return false;
	}

	string union_sql;
	for (idx_t i = 0; i < arms.size(); i++) {
		if (i > 0) {
			union_sql += " UNION ALL ";
		}
		union_sql += arms[i];
	}
	affected_keys_sql = "SELECT DISTINCT " + key_cols + " FROM (" + union_sql + ") openivm_changed_partitions";
	return true;
}

static bool AllWindowPartitionSourcesCovered(const vector<string> &delta_table_names,
                                             const vector<WindowPartitionDeltaSpec> &partition_delta_specs) {
	unordered_set<string> covered_sources;
	for (auto &spec : partition_delta_specs) {
		covered_sources.insert(StringUtil::Lower(spec.delta_table));
	}
	for (auto &dt : delta_table_names) {
		if (!covered_sources.count(StringUtil::Lower(dt))) {
			return false;
		}
	}
	return !delta_table_names.empty();
}

struct DuckLakeWindowSourceSpec {
	string metadata_key;
	DuckLakeSourceLocation loc;
	int64_t old_snap = -1;
	int64_t current_snap = -1;
};

static string StripDataPrefix(const string &name) {
	static const string data_prefix(openivm::DATA_TABLE_PREFIX);
	string last = SqlUtils::LastIdentifierPart(name);
	if (last.size() > data_prefix.size() && last.rfind(data_prefix, 0) == 0) {
		return last.substr(data_prefix.size());
	}
	return last;
}

static bool NamesMatch(const string &left, const string &right) {
	return StringUtil::CIEquals(StripDataPrefix(left), StripDataPrefix(right));
}

static bool SourceSpecMatches(const DuckLakeWindowSourceSpec &spec, const string &table_name) {
	return NamesMatch(spec.metadata_key, table_name) || NamesMatch(spec.loc.table_name, table_name);
}

static const DuckLakeWindowSourceSpec *FindSourceSpec(const vector<DuckLakeWindowSourceSpec> &specs,
                                                      const string &table_name) {
	for (auto &spec : specs) {
		if (SourceSpecMatches(spec, table_name)) {
			return &spec;
		}
	}
	return nullptr;
}

static bool BuildDuckLakeWindowSourceSpecs(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                           const vector<string> &delta_table_names, const string &view_catalog_name,
                                           const string &view_schema_name, const string &attached_db_catalog_name,
                                           const string &attached_db_schema_name,
                                           vector<DuckLakeWindowSourceSpec> &specs) {
	for (auto &dt : delta_table_names) {
		if (!metadata.IsDuckLakeTable(view_name, dt)) {
			return false;
		}
		DuckLakeWindowSourceSpec spec;
		spec.metadata_key = dt;
		spec.loc = ResolveDuckLakeSourceLocation(con, view_name, dt, view_catalog_name, view_schema_name,
		                                         attached_db_catalog_name, attached_db_schema_name);
		spec.old_snap = metadata.GetLastSnapshotId(view_name, dt);
		spec.current_snap = metadata.GetCurrentDuckLakeSnapshot(spec.loc.catalog_name);
		if (spec.loc.catalog_name.empty() || spec.loc.schema_name.empty() || spec.loc.table_name.empty() ||
		    spec.old_snap < 0 || spec.current_snap < 0) {
			return false;
		}
		specs.push_back(std::move(spec));
	}
	return !specs.empty();
}

static string BuildDuckLakeChangedValuesSQL(const DuckLakeWindowSourceSpec &spec, const string &source_col,
                                            const string &output_col) {
	string qsource_col = SqlUtils::QuoteIdentifier(source_col);
	string qoutput_col = SqlUtils::QuoteIdentifier(output_col);
	string insertions =
	    "SELECT " + qsource_col + " AS " + qoutput_col + " FROM " +
	    SqlUtils::DuckLakeTableFunction("ducklake_table_insertions", spec.loc.catalog_name, spec.loc.schema_name,
	                                    spec.loc.table_name, spec.old_snap, spec.current_snap);
	string deletions =
	    "SELECT " + qsource_col + " AS " + qoutput_col + " FROM " +
	    SqlUtils::DuckLakeTableFunction("ducklake_table_deletions", spec.loc.catalog_name, spec.loc.schema_name,
	                                    spec.loc.table_name, spec.old_snap, spec.current_snap);
	return "(" + insertions + " UNION ALL " + deletions + ")";
}

static string BuildDuckLakeLookupChangedKeysSQL(const DuckLakeWindowSourceSpec &source_spec,
                                                const DuckLakeWindowSourceSpec &lookup_spec,
                                                const RefreshMetadata::WindowPartitionLineageOp &op) {
	string changed = BuildDuckLakeChangedValuesSQL(source_spec, op.source_col, "openivm_join_key");
	string lookup_table =
	    SqlUtils::FullName(lookup_spec.loc.catalog_name, lookup_spec.loc.schema_name, lookup_spec.loc.table_name);
	string lookup_col = SqlUtils::QuoteIdentifier(op.lookup_col);
	string lookup_out = SqlUtils::QuoteIdentifier(op.lookup_out);
	string output_col = SqlUtils::QuoteIdentifier(op.output_col);
	string current_lookup = "SELECT l." + lookup_out + " AS " + output_col + " FROM " + changed + " c JOIN " +
	                        lookup_table + " AS l AT (VERSION => " + to_string(lookup_spec.current_snap) + ") ON l." +
	                        lookup_col + " = c.openivm_join_key";
	string old_lookup = "SELECT l." + lookup_out + " AS " + output_col + " FROM " + changed + " c JOIN " +
	                    lookup_table + " AS l AT (VERSION => " + to_string(lookup_spec.old_snap) + ") ON l." +
	                    lookup_col + " = c.openivm_join_key";
	return "(" + current_lookup + " UNION ALL " + old_lookup + ")";
}

static string BuildAffectedPartitionFilter(const vector<string> &partition_cols, const string &affected_table) {
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
	if (partition_cols.size() == 1) {
		return key_tuple + " IN (SELECT " + key_cols + " FROM " + affected_table + ")";
	}
	return "(" + key_tuple + ") IN (SELECT " + key_cols + " FROM " + affected_table + ")";
}

static bool BuildLineageDuckLakeAffectedKeysSQL(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                                const vector<string> &delta_table_names,
                                                const vector<string> &partition_cols, const string &view_catalog_name,
                                                const string &view_schema_name, const string &attached_db_catalog_name,
                                                const string &attached_db_schema_name, string &affected_keys_sql,
                                                string &key_cols, string &key_tuple) {
	if (partition_cols.size() != 1) {
		return false;
	}
	auto parsed = SplitPartitionSpec(partition_cols[0]);
	key_cols = SqlUtils::QuoteIdentifier(parsed.first);
	key_tuple = key_cols;

	vector<DuckLakeWindowSourceSpec> specs;
	if (!BuildDuckLakeWindowSourceSpecs(metadata, con, view_name, delta_table_names, view_catalog_name,
	                                    view_schema_name, attached_db_catalog_name, attached_db_schema_name, specs)) {
		return false;
	}

	vector<RefreshMetadata::WindowPartitionLineageOp> lineage_ops;
	if (!metadata.GetWindowPartitionLineage(view_name, lineage_ops)) {
		return false;
	}

	vector<string> arms;
	unordered_set<string> covered_sources;
	for (auto &op : lineage_ops) {
		if (!StringUtil::CIEquals(op.output_col, parsed.first)) {
			continue;
		}
		auto *source_spec = FindSourceSpec(specs, op.source);
		if (!source_spec) {
			continue;
		}
		if (op.kind == "direct") {
			arms.push_back(BuildDuckLakeChangedValuesSQL(*source_spec, op.source_col, op.output_col));
			covered_sources.insert(StripDataPrefix(source_spec->metadata_key));
			continue;
		}
		if (op.kind == "lookup") {
			auto *lookup_spec = FindSourceSpec(specs, op.lookup);
			if (!lookup_spec) {
				continue;
			}
			arms.push_back(BuildDuckLakeLookupChangedKeysSQL(*source_spec, *lookup_spec, op));
			covered_sources.insert(StripDataPrefix(source_spec->metadata_key));
		}
	}

	for (auto &spec : specs) {
		if (!covered_sources.count(StripDataPrefix(spec.metadata_key))) {
			return false;
		}
	}
	if (arms.empty()) {
		return false;
	}

	string union_sql;
	for (idx_t i = 0; i < arms.size(); i++) {
		if (i > 0) {
			union_sql += " UNION ALL ";
		}
		union_sql += arms[i];
	}
	affected_keys_sql = "SELECT DISTINCT " + key_cols + " FROM (" + union_sql + ") openivm_changed_partitions";
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
	for (size_t i = 0; i < partition_cols.size(); i++) {
		if (i > 0) {
			affected_cols += ", ";
			affected_select += ", ";
		}
		auto parsed = SplitPartitionSpec(partition_cols[i]);
		affected_cols += KeywordHelper::WriteOptionallyQuoted(parsed.first);
		affected_select += KeywordHelper::WriteOptionallyQuoted(parsed.second) + " AS " +
		                   KeywordHelper::WriteOptionallyQuoted(parsed.first);
	}
	string temp_affected = string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name;
	string qtemp_affected = KeywordHelper::WriteOptionallyQuoted(temp_affected);
	string insertions = "SELECT " + affected_select + " FROM " +
	                    SqlUtils::DuckLakeTableFunction("ducklake_table_insertions", loc.catalog_name, loc.schema_name,
	                                                    loc.table_name, old_snap, current_snap);
	string deletions = "SELECT " + affected_select + " FROM " +
	                   SqlUtils::DuckLakeTableFunction("ducklake_table_deletions", loc.catalog_name, loc.schema_name,
	                                                   loc.table_name, old_snap, current_snap);
	string affected_filter = BuildAffectedPartitionFilter(partition_cols, qtemp_affected);
	string upsert_query = "CREATE TEMP TABLE " + qtemp_affected + " AS SELECT DISTINCT " + affected_cols + " FROM ((" +
	                      insertions + ") UNION ALL (" + deletions + ")) openivm_changed_partitions;\n";
	upsert_query +=
	    BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_recompute", affected_filter, affected_filter);
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
	string key_cols;
	string key_tuple;
	string affected_keys;
	if (BuildLineageDuckLakeAffectedKeysSQL(metadata, con, view_name, delta_table_names, partition_cols,
	                                        view_catalog_name, view_schema_name, attached_db_catalog_name,
	                                        attached_db_schema_name, affected_keys, key_cols, key_tuple)) {
		string temp_affected = string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name;
		string qtemp_affected = KeywordHelper::WriteOptionallyQuoted(temp_affected);
		string affected_filter = key_tuple + " IN (SELECT " + key_cols + " FROM " + qtemp_affected + ")";
		OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake lineage change-feed, %zu "
		                    "sources)\n",
		                    delta_table_names.size());
		// Conservative lineage can over-include partitions, but must cover every changed source.
		// If lineage is incomplete, the full logical view diff below preserves correctness.
		string upsert_query = "CREATE OR REPLACE TEMP TABLE " + qtemp_affected + " AS " + affected_keys + ";\n";
		upsert_query += BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_recompute", affected_filter,
		                                            affected_filter);
		upsert_query += "DROP TABLE IF EXISTS " + qtemp_affected + ";\n";
		return upsert_query;
	}

	key_cols.clear();
	for (size_t i = 0; i < partition_cols.size(); i++) {
		if (i > 0) {
			key_cols += ", ";
		}
		auto parsed = SplitPartitionSpec(partition_cols[i]);
		string output_col = KeywordHelper::WriteOptionallyQuoted(parsed.first);
		key_cols += output_col;
	}
	string current_rows = "SELECT * FROM (" + view_query_sql + ") openivm_current_rows";
	// At refresh start the MV data table is the last committed result. Diffing against it
	// avoids replaying every DuckLake source at its previous snapshot just to find changed partitions.
	string old_rows = "SELECT * FROM " + data_table + " openivm_old_rows";
	string changed_rows = "((" + current_rows + ") EXCEPT ALL (" + old_rows + ")) UNION ALL ((" + old_rows +
	                      ") EXCEPT ALL (" + current_rows + "))";
	string temp_affected = string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name;
	string qtemp_affected = KeywordHelper::WriteOptionallyQuoted(temp_affected);
	string fallback_affected_keys = "SELECT DISTINCT " + key_cols + " FROM (" + changed_rows + ") openivm_changed_rows";
	string affected_filter = BuildAffectedPartitionFilter(partition_cols, qtemp_affected);
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (DuckLake view-diff, %zu "
	                    "partition cols, %zu sources)\n",
	                    partition_cols.size(), delta_table_names.size());
	// Materialize the affected partition keys once; otherwise DuckDB/DuckLake repeats the
	// full view diff independently for DELETE and INSERT.
	string upsert_query = "CREATE OR REPLACE TEMP TABLE " + qtemp_affected + " AS " + fallback_affected_keys + ";\n";
	upsert_query +=
	    BuildDeleteInsertRefreshSQL(data_table, view_query_sql, "openivm_recompute", affected_filter, affected_filter);
	upsert_query += "DROP TABLE IF EXISTS " + qtemp_affected + ";\n";
	return upsert_query;
}

string BuildWindowPartitionRefresh(RefreshMetadata &metadata, Connection &con, const string &view_name,
                                   const string &view_query_sql, const vector<string> &delta_table_names,
                                   const vector<string> &column_names, const string &data_table,
                                   const string &delta_ts_filter, const string &internal_catalog_prefix,
                                   const string &view_catalog_name, const string &view_schema_name,
                                   const string &attached_db_catalog_name, const string &attached_db_schema_name,
                                   bool cross_system, bool emit_cascade_delta, bool running_window_incremental) {
	auto partition_cols = metadata.GetGroupColumns(view_name); // reuses group_columns field
	auto partition_delta_specs =
	    BuildWindowPartitionDeltaSpecs(metadata, con, view_name, delta_table_names, partition_cols, cross_system);
	bool any_ducklake = AnyDuckLakeSource(metadata, view_name, delta_table_names);
	bool safe_for_snapdiff = IsSafeForDuckLakeSnapshotDiff(partition_cols, column_names, any_ducklake);
	string affected_keys_sql;
	string affected_key_cols;
	string affected_key_tuple;
	bool have_lineage_affected_keys = false;

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
	if (delta_table_names.size() > 1) {
		have_lineage_affected_keys = BuildLineageStandardAffectedKeysSQL(
		    metadata, view_name, delta_table_names, partition_cols, delta_ts_filter, view_catalog_name,
		    view_schema_name, attached_db_catalog_name, attached_db_schema_name, affected_keys_sql, affected_key_cols,
		    affected_key_tuple);
	}
	if (!have_lineage_affected_keys && delta_table_names.size() > 1 &&
	    !AllWindowPartitionSourcesCovered(delta_table_names, partition_delta_specs)) {
		OPENIVM_DEBUG_PRINT("[UPSERT] WINDOW_PARTITION lineage incomplete for '%s' (%zu sources) — full recompute "
		                    "fallback\n",
		                    view_name.c_str(), delta_table_names.size());
		return CompileFullRecompute(view_name, view_query_sql, internal_catalog_prefix);
	}
	OPENIVM_DEBUG_PRINT("[UPSERT] Compiling upsert for type: WINDOW_PARTITION (%zu partition cols, lineage keys: %s)\n",
	                    partition_cols.size(), have_lineage_affected_keys ? "yes" : "no");
	vector<string> running_window_column_names = column_names;
	if (running_window_incremental) {
		auto data_cols = con.Query("SELECT column_name FROM information_schema.columns WHERE table_name = '" +
		                           SqlUtils::EscapeValue(IncrementalTableNames::DataTableName(view_name)) +
		                           "' ORDER BY ordinal_position");
		if (!data_cols->HasError() && data_cols->RowCount() > 0) {
			running_window_column_names.clear();
			for (idx_t i = 0; i < data_cols->RowCount(); i++) {
				running_window_column_names.push_back(data_cols->GetValue(0, i).ToString());
			}
		}
	}
	return CompileWindowRecompute(view_name, view_query_sql, delta_ts_filter, internal_catalog_prefix, partition_cols,
	                              partition_delta_specs, emit_cascade_delta, affected_keys_sql, affected_key_cols,
	                              affected_key_tuple, running_window_column_names, running_window_incremental);
}

} // namespace duckdb
