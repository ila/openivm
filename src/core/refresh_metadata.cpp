#include "core/refresh_metadata.hpp"

#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include <algorithm>
#include <functional>
#include <sstream>
#include <unordered_set>

namespace duckdb {

bool RefreshMetadata::IsBaseTable(const string &table_name) {
	auto result = con.Query("SELECT 1 FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(table_name) + "'");
	return !result->HasError() && result->RowCount() == 0;
}

string RefreshMetadata::GetViewQuery(const string &view_name) {
	auto result = con.Query("SELECT sql_string FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		return "";
	}
	return result->GetValue(0, 0).ToString();
}

RefreshType RefreshMetadata::GetViewType(const string &view_name) {
	auto result = con.Query("SELECT type FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		throw ParserException("Materialized view '%s' does not exist in IVM metadata.", view_name);
	}
	return static_cast<RefreshType>(result->GetValue(0, 0).GetValue<int8_t>());
}

bool RefreshMetadata::HasMinMax(const string &view_name) {
	auto result = con.Query("SELECT has_minmax FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

bool RefreshMetadata::HasLeftJoin(const string &view_name) {
	auto result = con.Query("SELECT has_left_join FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

bool RefreshMetadata::HasFullOuter(const string &view_name) {
	auto result = con.Query("SELECT has_full_outer FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

string RefreshMetadata::GetFullOuterJoinCols(const string &view_name) {
	auto result = con.Query("SELECT full_outer_join_cols FROM " + string(openivm::VIEWS_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return "";
	}
	return result->GetValue(0, 0).ToString();
}

vector<string> RefreshMetadata::GetDeltaTables(const string &view_name) {
	auto result = con.Query("SELECT table_name FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	vector<string> tables;
	if (!result->HasError()) {
		for (size_t i = 0; i < result->RowCount(); i++) {
			tables.push_back(result->GetValue(0, i).ToString());
		}
	}
	return tables;
}

string RefreshMetadata::GetLastUpdate(const string &view_name, const string &table_name) {
	auto result =
	    con.Query("SELECT last_update FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		return "";
	}
	return result->GetValue(0, 0).ToString();
}

RefreshMetadata::SourceLocation RefreshMetadata::GetSourceLocation(const string &view_name, const string &table_name,
                                                                   const string &fallback_catalog,
                                                                   const string &fallback_schema) {
	SourceLocation loc;
	loc.catalog_name = fallback_catalog;
	loc.schema_name = fallback_schema;
	loc.table_name = table_name;
	auto result = con.Query("SELECT source_catalog, source_schema FROM " + string(openivm::DELTA_TABLES_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND table_name = '" +
	                        SqlUtils::EscapeValue(table_name) + "'");
	if (!result->HasError() && result->RowCount() > 0) {
		if (!result->GetValue(0, 0).IsNull()) {
			loc.catalog_name = result->GetValue(0, 0).ToString();
		}
		if (!result->GetValue(1, 0).IsNull()) {
			loc.schema_name = result->GetValue(1, 0).ToString();
		}
	}
	return loc;
}

string RefreshMetadata::ResolveDeltaQualifiedName(const string &view_name, const string &delta_table_name,
                                                  const string &fallback_catalog, const string &fallback_schema) {
	auto loc = GetSourceLocation(view_name, delta_table_name, fallback_catalog, fallback_schema);
	if (loc.catalog_name.empty()) {
		return SqlUtils::QuoteIdentifier(delta_table_name);
	}
	if (loc.schema_name.empty()) {
		loc.schema_name = "main";
	}
	return SqlUtils::FullName(loc.catalog_name, loc.schema_name, delta_table_name);
}

RefreshMetadata::DeltaChangeStats RefreshMetadata::GetStandardDeltaChangeStats(const string &delta_table_sql,
                                                                               const string &last_update) {
	DeltaChangeStats stats;
	if (last_update.empty()) {
		return stats;
	}
	auto result =
	    con.Query("SELECT COUNT(*), SUM(CASE WHEN " + string(openivm::MULTIPLICITY_COL) +
	              " < 0 THEN 1 ELSE 0 END) FROM " + delta_table_sql + " WHERE " + string(openivm::TIMESTAMP_COL) +
	              " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return stats;
	}
	stats.ok = true;
	stats.total = result->GetValue(0, 0).GetValue<int64_t>();
	if (!result->GetValue(1, 0).IsNull()) {
		stats.deletes = result->GetValue(1, 0).GetValue<int64_t>();
	}
	return stats;
}

static string InformationSchemaRelationFilter(const string &catalog_name, const string &schema_name,
                                              const string &table_name) {
	string filter = "table_name = '" + SqlUtils::EscapeValue(table_name) + "'";
	if (!catalog_name.empty()) {
		filter += " AND table_catalog = '" + SqlUtils::EscapeValue(catalog_name) + "'";
	}
	if (!schema_name.empty()) {
		filter += " AND table_schema = '" + SqlUtils::EscapeValue(schema_name) + "'";
	}
	return filter;
}

vector<string> RefreshMetadata::GetTableColumns(const string &catalog_name, const string &schema_name,
                                                const string &table_name) {
	vector<string> columns;
	auto result = con.Query("SELECT column_name FROM information_schema.columns WHERE " +
	                        InformationSchemaRelationFilter(catalog_name, schema_name, table_name) +
	                        " ORDER BY ordinal_position");
	if (result->HasError()) {
		return columns;
	}
	for (idx_t i = 0; i < result->RowCount(); i++) {
		columns.push_back(result->GetValue(0, i).ToString());
	}
	return columns;
}

bool RefreshMetadata::TableColumnsMatch(const string &catalog_name, const string &schema_name, const string &table_name,
                                        const vector<string> &expected) {
	auto actual = GetTableColumns(catalog_name, schema_name, table_name);
	if (actual.size() != expected.size()) {
		return false;
	}
	for (idx_t i = 0; i < actual.size(); i++) {
		if (!StringUtil::CIEquals(actual[i], expected[i])) {
			return false;
		}
	}
	return true;
}

vector<string> RefreshMetadata::GetUpstreamViews(const string &view_name) {
	// Walk upstream: for view V, find its delta tables. If a delta table is "delta_X"
	// and X is a registered MV, then X is an upstream dependency. Recurse.
	vector<string> result;
	unordered_set<string> visited;

	std::function<void(const string &)> collect = [&](const string &vn) {
		auto delta_tables = GetDeltaTables(vn);
		for (auto &dt : delta_tables) {
			// Extract source MV name from delta table name.
			// Standard: "delta_<source>", DuckLake: "openivm_data_<source>"
			string source;
			static const string delta_prefix(openivm::DELTA_PREFIX);
			static const string data_prefix(openivm::DATA_TABLE_PREFIX);
			if (dt.size() > delta_prefix.size() && dt.substr(0, delta_prefix.size()) == delta_prefix) {
				source = dt.substr(delta_prefix.size());
			} else if (dt.size() > data_prefix.size() && dt.substr(0, data_prefix.size()) == data_prefix) {
				source = dt.substr(data_prefix.size());
			}
			if (!source.empty() && !IsBaseTable(source) && visited.find(source) == visited.end()) {
				visited.insert(source);
				collect(source); // recurse deeper first (ancestors before descendants)
				result.push_back(source);
			}
		}
	};
	collect(view_name);
	return result; // topological order: ancestors first
}

vector<string> RefreshMetadata::GetDownstreamViews(const string &view_name) {
	// Find all reachable views that depend on delta_<view_name> or openivm_data_<view_name> as a source.
	// DuckLake chained MVs use openivm_data_* (data table) instead of delta_* (delta table).
	//
	// The returned order must be topological over the reachable downstream DAG. Reverse postorder DFS keeps
	// fanout branches before their fan-in children.
	vector<string> result;
	unordered_set<string> visited;

	std::function<void(const string &)> collect = [&](const string &vn) {
		string delta_name = SqlUtils::DeltaName(vn);
		string data_name = IncrementalTableNames::DataTableName(vn);
		auto dependents = con.Query("SELECT DISTINCT view_name FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                            " WHERE table_name = '" + SqlUtils::EscapeValue(delta_name) +
		                            "' OR table_name = '" + SqlUtils::EscapeValue(data_name) + "' ORDER BY view_name");
		if (!dependents->HasError()) {
			for (size_t i = 0; i < dependents->RowCount(); i++) {
				string dep = dependents->GetValue(0, i).ToString();
				if (dep == view_name) {
					continue;
				}
				if (visited.insert(dep).second) {
					collect(dep);
					result.push_back(dep);
				}
			}
		}
	};
	collect(view_name);
	std::reverse(result.begin(), result.end());
	return result; // topological order: downstream parents before fan-in children
}

vector<string> RefreshMetadata::GetGroupColumns(const string &view_name) {
	auto result = con.Query("SELECT group_columns FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	vector<string> cols;
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return cols;
	}
	string raw = result->GetValue(0, 0).ToString();
	// Split comma-separated column names
	std::istringstream ss(raw);
	string token;
	while (std::getline(ss, token, ',')) {
		if (!token.empty()) {
			cols.push_back(token);
		}
	}
	return cols;
}

vector<string> RefreshMetadata::GetAggregateTypes(const string &view_name) {
	auto result = con.Query("SELECT aggregate_types FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	vector<string> types;
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return types;
	}
	string raw = result->GetValue(0, 0).ToString();
	std::istringstream ss(raw);
	string token;
	while (std::getline(ss, token, ',')) {
		if (!token.empty()) {
			types.push_back(token);
		}
	}
	return types;
}

int64_t RefreshMetadata::GetRefreshInterval(const string &view_name) {
	auto result = con.Query("SELECT refresh_interval FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return -1;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

vector<RefreshMetadata::ScheduledView> RefreshMetadata::GetScheduledViews() {
	auto result = con.Query("SELECT v.view_name, v.refresh_interval, "
	                        "(SELECT MIN(d.last_update) FROM " +
	                        string(openivm::DELTA_TABLES_TABLE) +
	                        " d WHERE d.view_name = v.view_name) AS last_update "
	                        "FROM " +
	                        string(openivm::VIEWS_TABLE) + " v WHERE v.refresh_interval IS NOT NULL");
	vector<ScheduledView> views;
	if (result->HasError()) {
		OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] GetScheduledViews query error: %s\n", result->GetError().c_str());
	}
	if (!result->HasError()) {
		for (size_t i = 0; i < result->RowCount(); i++) {
			ScheduledView sv;
			sv.view_name = result->GetValue(0, i).ToString();
			sv.interval_seconds = result->GetValue(1, i).GetValue<int64_t>();
			sv.last_update = result->GetValue(2, i).IsNull() ? "" : result->GetValue(2, i).ToString();
			views.push_back(sv);
		}
	}
	return views;
}

void RefreshMetadata::SetRefreshInProgress(const string &view_name, bool in_progress) {
	con.Query("UPDATE " + string(openivm::VIEWS_TABLE) + " SET refresh_in_progress = " +
	          (in_progress ? "true" : "false") + " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
}

string RefreshMetadata::BuildDeltaCleanupSQL(const string &target, const string &metadata_key) {
	string qtarget = target.find('.') == string::npos ? KeywordHelper::WriteOptionallyQuoted(target) : target;
	return "DELETE FROM " + qtarget + " WHERE " + string(openivm::TIMESTAMP_COL) + " < (SELECT MIN(last_update) FROM " +
	       string(openivm::DELTA_TABLES_TABLE) + " WHERE table_name = '" + SqlUtils::EscapeValue(metadata_key) +
	       "');\n";
}

// --- DuckLake support ---

string RefreshMetadata::GetCatalogType(const string &view_name, const string &table_name) {
	auto result =
	    con.Query("SELECT catalog_type FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return "duckdb";
	}
	return result->GetValue(0, 0).ToString();
}

bool RefreshMetadata::IsDuckLakeTable(const string &view_name, const string &table_name) {
	return GetCatalogType(view_name, table_name) == "ducklake";
}

bool RefreshMetadata::IsDuckLakeCatalog(const string &catalog_name) {
	if (catalog_name.empty()) {
		return false;
	}
	auto result = con.Query("SELECT type FROM duckdb_databases() WHERE database_name = '" +
	                        SqlUtils::EscapeValue(catalog_name) + "' LIMIT 1");
	return !result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull() &&
	       StringUtil::CIEquals(result->GetValue(0, 0).ToString(), "ducklake");
}

int64_t RefreshMetadata::GetCurrentDuckLakeSnapshot(const string &catalog_name) {
	if (catalog_name.empty()) {
		return -1;
	}
	auto result = con.Query("SELECT id FROM " + SqlUtils::QuoteIdentifier(catalog_name) + ".current_snapshot()");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return -1;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

int64_t RefreshMetadata::GetLastSnapshotId(const string &view_name, const string &table_name) {
	auto result =
	    con.Query("SELECT last_snapshot_id FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return -1;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

RefreshMetadata::DuckLakeSourceIdentity RefreshMetadata::ResolveDuckLakeSourceIdentity(const string &view_name,
                                                                                       const string &table_name,
                                                                                       const string &catalog_name,
                                                                                       const string &schema_name) {
	DuckLakeSourceIdentity identity;
	auto result =
	    con.Query("SELECT source_table_id FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "'");
	if (!result->HasError() && result->RowCount() > 0 && !result->GetValue(0, 0).IsNull()) {
		identity.stored_table_id = result->GetValue(0, 0).GetValue<int64_t>();
	}
	if (catalog_name.empty()) {
		return identity;
	}

	string catalog_prefix = SqlUtils::QuoteIdentifier("__ducklake_metadata_" + catalog_name) + ".";
	string schema_filter = schema_name.empty() ? "main" : schema_name;
	auto current_result =
	    con.Query("SELECT t.table_id FROM " + catalog_prefix + "ducklake_table t JOIN " + catalog_prefix +
	              "ducklake_schema s ON t.schema_id = s.schema_id WHERE t.end_snapshot IS NULL AND "
	              "s.end_snapshot IS NULL AND t.table_name = '" +
	              SqlUtils::EscapeValue(table_name) + "' AND s.schema_name = '" + SqlUtils::EscapeValue(schema_filter) +
	              "' ORDER BY t.table_id DESC LIMIT 1");
	if (current_result->HasError() || current_result->RowCount() == 0 || current_result->GetValue(0, 0).IsNull()) {
		return identity;
	}

	identity.resolved = true;
	identity.current_table_id = current_result->GetValue(0, 0).GetValue<int64_t>();
	identity.changed = identity.stored_table_id >= 0 && identity.stored_table_id != identity.current_table_id;
	if (identity.stored_table_id == identity.current_table_id) {
		return identity;
	}
	auto update =
	    con.Query("UPDATE " + string(openivm::DELTA_TABLES_TABLE) +
	              " SET source_table_id = " + to_string(identity.current_table_id) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "'");
	if (update->HasError()) {
		OPENIVM_DEBUG_PRINT("[DuckLake] Could not backfill source_table_id for %s.%s: %s\n", view_name.c_str(),
		                    table_name.c_str(), update->GetError().c_str());
	}
	return identity;
}

string RefreshMetadata::BuildDuckLakeRefreshMetadataSQL(const string &view_name, const string &table_name,
                                                        const string &snapshot_expr) {
	return "UPDATE " + string(openivm::DELTA_TABLES_TABLE) + " SET last_snapshot_id = " + snapshot_expr +
	       ", last_update = now(), last_refresh_ts = now() WHERE view_name = '" + SqlUtils::EscapeValue(view_name) +
	       "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "';\n";
}

void RefreshMetadata::UpdateDuckLakeRefreshMetadata(const string &view_name, const string &table_name,
                                                    int64_t snapshot_id) {
	auto result = con.Query(BuildDuckLakeRefreshMetadataSQL(view_name, table_name, to_string(snapshot_id)));
	if (result->HasError()) {
		throw Exception(ExceptionType::EXECUTOR, "Cannot update DuckLake refresh metadata: " + result->GetError());
	}
}

// --- Refresh history (learned cost model) ---

void RefreshMetadata::RecordRefreshHistory(const string &view_name, const string &method,
                                           double incremental_compute_est, double incremental_upsert_est,
                                           double recompute_compute_est, double recompute_replace_est,
                                           int64_t actual_duration_ms, idx_t max_history) {
	auto result = con.Query("INSERT INTO " + string(openivm::HISTORY_TABLE) +
	                        " (view_name, method, incremental_compute_est, incremental_upsert_est,"
	                        " recompute_compute_est, recompute_replace_est, actual_duration_ms)"
	                        " VALUES ('" +
	                        SqlUtils::EscapeValue(view_name) + "', '" + SqlUtils::EscapeValue(method) + "', " +
	                        to_string(incremental_compute_est) + ", " + to_string(incremental_upsert_est) + ", " +
	                        to_string(recompute_compute_est) + ", " + to_string(recompute_replace_est) + ", " +
	                        to_string(actual_duration_ms) + ")");
	if (result->HasError()) {
		OPENIVM_DEBUG_PRINT("[HISTORY] Failed to record: %s\n", result->GetError().c_str());
		return;
	}

	// Prune old entries beyond the window
	con.Query("DELETE FROM " + string(openivm::HISTORY_TABLE) + " WHERE view_name = '" +
	          SqlUtils::EscapeValue(view_name) + "' AND refresh_timestamp NOT IN (SELECT refresh_timestamp FROM " +
	          string(openivm::HISTORY_TABLE) + " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) +
	          "' ORDER BY refresh_timestamp DESC LIMIT " + to_string(max_history) + ")");
}

vector<RefreshMetadata::RefreshHistoryEntry> RefreshMetadata::GetRefreshHistory(const string &view_name,
                                                                                const string &method, idx_t limit) {
	// Select the cost components for the given method:
	// - 'full': use recompute_compute_est, recompute_replace_est
	// - 'incremental' / 'group_recompute' / 'window_partition' (anything else):
	//   use incremental_compute_est, incremental_upsert_est. The cost model writes the chosen
	//   strategy's compute/upsert cost into those columns at refresh time, so the
	//   regression learns weights specific to whichever non-full path the view uses.
	bool is_full = (method == "full");
	string col1 = is_full ? "recompute_compute_est" : "incremental_compute_est";
	string col2 = is_full ? "recompute_replace_est" : "incremental_upsert_est";

	auto result =
	    con.Query("SELECT " + col1 + ", " + col2 + ", actual_duration_ms FROM " + string(openivm::HISTORY_TABLE) +
	              " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND method = '" +
	              SqlUtils::EscapeValue(method) + "' ORDER BY refresh_timestamp ASC LIMIT " + to_string(limit));

	vector<RefreshHistoryEntry> entries;
	if (result->HasError() || result->RowCount() == 0) {
		return entries;
	}
	for (idx_t i = 0; i < result->RowCount(); i++) {
		RefreshHistoryEntry entry;
		entry.compute_est = result->GetValue(0, i).GetValue<double>();
		entry.upsert_est = result->GetValue(1, i).GetValue<double>();
		entry.actual_ms = result->GetValue(2, i).GetValue<double>();
		entries.push_back(entry);
	}
	return entries;
}

namespace {

// Tiny JSON parser sufficient for `distinct_aux_meta_json` — we own the writer in
// parser.cpp, so the input always matches `{"k":"v","k2":[...]}` shape with
// just our minimal escaping (`\"` and `\\`). Not a general parser; do not reuse.
static bool ExtractJsonString(const string &json, const string &key, string &val) {
	string needle = "\"" + key + "\":\"";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	pos += needle.size();
	val.clear();
	while (pos < json.size()) {
		char c = json[pos];
		if (c == '\\' && pos + 1 < json.size()) {
			char esc = json[pos + 1];
			if (esc == 'n') {
				val += '\n';
			} else {
				val += esc;
			}
			pos += 2;
			continue;
		}
		if (c == '"') {
			return true;
		}
		val += c;
		pos++;
	}
	return false;
}

static bool ExtractJsonStringArray(const string &json, const string &key, vector<string> &val) {
	string needle = "\"" + key + "\":[";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return false;
	}
	pos += needle.size();
	val.clear();
	while (pos < json.size() && json[pos] != ']') {
		while (pos < json.size() && json[pos] != '"' && json[pos] != ']') {
			pos++;
		}
		if (pos >= json.size() || json[pos] == ']') {
			break;
		}
		pos++; // opening quote
		string elem;
		while (pos < json.size() && json[pos] != '"') {
			if (json[pos] == '\\' && pos + 1 < json.size()) {
				elem += json[pos + 1];
				pos += 2;
				continue;
			}
			elem += json[pos];
			pos++;
		}
		if (pos < json.size()) {
			pos++; // closing quote
		}
		val.push_back(std::move(elem));
	}
	return true;
}

static vector<string> ExtractJsonObjectsFromArray(const string &json, const string &key) {
	vector<string> objects;
	string needle = "\"" + key + "\":[";
	size_t pos = json.find(needle);
	if (pos == string::npos) {
		return objects;
	}
	pos += needle.size();
	int depth = 0;
	bool in_string = false;
	bool escaped = false;
	size_t object_start = string::npos;
	for (; pos < json.size(); pos++) {
		char c = json[pos];
		if (in_string) {
			if (escaped) {
				escaped = false;
			} else if (c == '\\') {
				escaped = true;
			} else if (c == '"') {
				in_string = false;
			}
			continue;
		}
		if (c == '"') {
			in_string = true;
			continue;
		}
		if (c == '{') {
			if (depth == 0) {
				object_start = pos;
			}
			depth++;
			continue;
		}
		if (c == '}') {
			if (depth > 0) {
				depth--;
				if (depth == 0 && object_start != string::npos) {
					objects.push_back(json.substr(object_start, pos - object_start + 1));
					object_start = string::npos;
				}
			}
			continue;
		}
		if (c == ']' && depth == 0) {
			break;
		}
	}
	return objects;
}

static vector<string> SplitPipeFields(const string &value) {
	vector<string> fields;
	string current;
	for (char c : value) {
		if (c == '|') {
			fields.push_back(std::move(current));
			current.clear();
			continue;
		}
		current += c;
	}
	fields.push_back(std::move(current));
	return fields;
}

static bool ParseJsonIndex(const string &json, const string &key, idx_t &out) {
	string text;
	if (!ExtractJsonString(json, key, text)) {
		return false;
	}
	try {
		out = static_cast<idx_t>(std::stoull(text));
		return true;
	} catch (...) {
		return false;
	}
}

static bool ReadRefreshLineageEntry(Connection &con, const string &view_name, const string &kind, string &entry) {
	auto result = con.Query("SELECT lineage_json FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	string json = result->GetValue(0, 0).ToString();
	if (json.empty()) {
		return false;
	}
	auto objects = ExtractJsonObjectsFromArray(json, "items");
	for (auto &object : objects) {
		string object_kind;
		if (ExtractJsonString(object, "k", object_kind) && object_kind == kind) {
			entry = std::move(object);
			return true;
		}
	}
	return false;
}

} // namespace

bool RefreshMetadata::GetDistinctAuxMeta(const string &view_name, DistinctAuxMeta &out) {
	auto result = con.Query("SELECT distinct_aux_meta_json FROM " + string(openivm::VIEWS_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	string json = result->GetValue(0, 0).ToString();
	if (json.empty()) {
		return false;
	}
	bool ok = true;
	ok &= ExtractJsonString(json, "aux_table", out.aux_table);
	ok &= ExtractJsonStringArray(json, "cols", out.cols);
	ExtractJsonStringArray(json, "source_exprs", out.source_exprs);
	ok &= ExtractJsonString(json, "input_sql", out.input_sql);
	ok &= ExtractJsonString(json, "source", out.source);
	// filter is optional — empty when the user didn't write a WHERE clause.
	ExtractJsonString(json, "filter", out.filter);
	ok &= ExtractJsonString(json, "sum_arg", out.sum_arg);
	ok &= ExtractJsonString(json, "sum_out", out.sum_out);
	return ok;
}

string RefreshMetadata::DistinctAuxMetaToJson(const DistinctAuxMeta &meta) {
	return "{\"aux_table\":" + SqlUtils::JsonQuote(meta.aux_table) + ",\"cols\":" + SqlUtils::JsonArray(meta.cols) +
	       ",\"source_exprs\":" + SqlUtils::JsonArray(meta.source_exprs) +
	       ",\"input_sql\":" + SqlUtils::JsonQuote(meta.input_sql) + ",\"source\":" + SqlUtils::JsonQuote(meta.source) +
	       ",\"filter\":" + SqlUtils::JsonQuote(meta.filter) + ",\"sum_arg\":" + SqlUtils::JsonQuote(meta.sum_arg) +
	       ",\"sum_out\":" + SqlUtils::JsonQuote(meta.sum_out) + "}";
}

bool RefreshMetadata::GetSemiAntiAuxMeta(const string &view_name, SemiAntiAuxMeta &out) {
	auto result = con.Query("SELECT semi_anti_aux_meta_json FROM " + string(openivm::VIEWS_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	string json = result->GetValue(0, 0).ToString();
	if (json.empty()) {
		return false;
	}
	bool ok = true;
	ok &= ExtractJsonString(json, "aux_table", out.aux_table);
	ok &= ExtractJsonString(json, "join_type", out.join_type);
	ok &= ExtractJsonString(json, "left_table", out.left_table);
	ok &= ExtractJsonString(json, "left_alias", out.left_alias);
	ok &= ExtractJsonString(json, "right_table", out.right_table);
	ok &= ExtractJsonString(json, "right_alias", out.right_alias);
	ok &= ExtractJsonString(json, "predicate", out.predicate);
	ExtractJsonString(json, "post_filter", out.post_filter);
	ok &= ExtractJsonStringArray(json, "left_cols", out.left_cols);
	ExtractJsonStringArray(json, "left_exprs", out.left_exprs);
	ok &= ExtractJsonStringArray(json, "output_cols", out.output_cols);
	return ok;
}

string RefreshMetadata::SemiAntiAuxMetaToJson(const SemiAntiAuxMeta &meta) {
	return "{\"aux_table\":" + SqlUtils::JsonQuote(meta.aux_table) +
	       ",\"join_type\":" + SqlUtils::JsonQuote(meta.join_type) +
	       ",\"left_table\":" + SqlUtils::JsonQuote(meta.left_table) +
	       ",\"left_alias\":" + SqlUtils::JsonQuote(meta.left_alias) +
	       ",\"right_table\":" + SqlUtils::JsonQuote(meta.right_table) +
	       ",\"right_alias\":" + SqlUtils::JsonQuote(meta.right_alias) +
	       ",\"predicate\":" + SqlUtils::JsonQuote(meta.predicate) +
	       ",\"post_filter\":" + SqlUtils::JsonQuote(meta.post_filter) +
	       ",\"left_cols\":" + SqlUtils::JsonArray(meta.left_cols) +
	       ",\"left_exprs\":" + SqlUtils::JsonArray(meta.left_exprs) +
	       ",\"output_cols\":" + SqlUtils::JsonArray(meta.output_cols) + "}";
}

bool RefreshMetadata::GetWindowPartitionLineage(const string &view_name, vector<WindowPartitionLineageOp> &out) {
	out.clear();
	string json;
	if (!ReadRefreshLineageEntry(con, view_name, "window_partition", json)) {
		return false;
	}
	vector<string> objects = ExtractJsonObjectsFromArray(json, "ops");
	for (auto &object : objects) {
		WindowPartitionLineageOp op;
		if (!ExtractJsonString(object, "k", op.kind) || !ExtractJsonString(object, "out", op.output_col) ||
		    !ExtractJsonString(object, "source", op.source) ||
		    !ExtractJsonString(object, "source_col", op.source_col)) {
			continue;
		}
		if (op.kind == "lookup") {
			if (!ExtractJsonString(object, "lookup", op.lookup) ||
			    !ExtractJsonString(object, "lookup_col", op.lookup_col) ||
			    !ExtractJsonString(object, "lookup_out", op.lookup_out)) {
				continue;
			}
		} else if (op.kind != "direct") {
			continue;
		}
		out.push_back(std::move(op));
	}
	return !out.empty();
}

string RefreshMetadata::WindowPartitionLineageToJson(const vector<WindowPartitionLineageOp> &ops) {
	string json = "{\"k\":\"window_partition\",\"ops\":[";
	for (idx_t i = 0; i < ops.size(); i++) {
		auto &op = ops[i];
		if (i > 0) {
			json += ",";
		}
		json += "{\"k\":" + SqlUtils::JsonQuote(op.kind) + ",\"out\":" + SqlUtils::JsonQuote(op.output_col) +
		        ",\"source\":" + SqlUtils::JsonQuote(op.source) +
		        ",\"source_col\":" + SqlUtils::JsonQuote(op.source_col);
		if (op.kind == "lookup") {
			json += ",\"lookup\":" + SqlUtils::JsonQuote(op.lookup) +
			        ",\"lookup_col\":" + SqlUtils::JsonQuote(op.lookup_col) +
			        ",\"lookup_out\":" + SqlUtils::JsonQuote(op.lookup_out);
		}
		json += "}";
	}
	json += "]}";
	return json;
}

bool RefreshMetadata::GetProjectionKeyLineage(const string &view_name, ProjectionKeyLineage &out) {
	string json;
	if (!ReadRefreshLineageEntry(con, view_name, "projection_key", json)) {
		return false;
	}
	if (!ExtractJsonString(json, "out", out.output_col) || !ExtractJsonString(json, "key_source", out.key_source) ||
	    !ParseJsonIndex(json, "key_occ", out.key_occurrence) || !ExtractJsonString(json, "key_col", out.key_col)) {
		return false;
	}
	out.arms.clear();
	auto objects = ExtractJsonObjectsFromArray(json, "arms");
	for (auto &object : objects) {
		ProjectionKeyLineageArm arm;
		if (!ExtractJsonString(object, "source", arm.source) || !ParseJsonIndex(object, "occ", arm.occurrence) ||
		    !ExtractJsonString(object, "source_col", arm.source_col)) {
			continue;
		}
		vector<string> steps;
		ExtractJsonStringArray(object, "steps", steps);
		for (auto &step_text : steps) {
			auto fields = SplitPipeFields(step_text);
			if (fields.size() != 4) {
				continue;
			}
			ProjectionKeyLineageStep step;
			step.table = fields[0];
			try {
				step.occurrence = static_cast<idx_t>(std::stoull(fields[1]));
			} catch (...) {
				continue;
			}
			step.lookup_col = fields[2];
			step.lookup_out = fields[3];
			arm.steps.push_back(std::move(step));
		}
		out.arms.push_back(std::move(arm));
	}
	return !out.arms.empty();
}

string RefreshMetadata::ProjectionKeyLineageToJson(const ProjectionKeyLineage &lineage) {
	string json = "{\"k\":\"projection_key\",\"out\":" + SqlUtils::JsonQuote(lineage.output_col) +
	              ",\"key_source\":" + SqlUtils::JsonQuote(lineage.key_source) +
	              ",\"key_occ\":" + SqlUtils::JsonQuote(to_string(lineage.key_occurrence)) +
	              ",\"key_col\":" + SqlUtils::JsonQuote(lineage.key_col) + ",\"arms\":[";
	for (idx_t i = 0; i < lineage.arms.size(); i++) {
		auto &arm = lineage.arms[i];
		if (i > 0) {
			json += ",";
		}
		json += "{\"source\":" + SqlUtils::JsonQuote(arm.source) +
		        ",\"occ\":" + SqlUtils::JsonQuote(to_string(arm.occurrence)) +
		        ",\"source_col\":" + SqlUtils::JsonQuote(arm.source_col) + ",\"steps\":[";
		for (idx_t j = 0; j < arm.steps.size(); j++) {
			auto &step = arm.steps[j];
			if (j > 0) {
				json += ",";
			}
			json += SqlUtils::JsonQuote(step.table + "|" + to_string(step.occurrence) + "|" + step.lookup_col + "|" +
			                            step.lookup_out);
		}
		json += "]}";
	}
	json += "]}";
	return json;
}

bool RefreshMetadata::GetFilteredGroupCountAuxMeta(const string &view_name, FilteredGroupCountAuxMeta &out) {
	auto result = con.Query("SELECT aggregate_decomposition_json FROM " + string(openivm::VIEWS_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	string json = result->GetValue(0, 0).ToString();
	if (json.empty()) {
		return false;
	}
	string kind;
	if (!ExtractJsonString(json, "kind", kind) || kind != "filtered_group_count") {
		return false;
	}
	bool ok = true;
	ok &= ExtractJsonString(json, "aux_table", out.aux_table);
	ok &= ExtractJsonString(json, "source", out.source);
	ok &= ExtractJsonString(json, "group_col", out.group_col);
	ok &= ExtractJsonString(json, "sum_col", out.sum_col);
	ExtractJsonString(json, "source_group_expr", out.source_group_expr);
	ExtractJsonString(json, "source_sum_expr", out.source_sum_expr);
	ok &= ExtractJsonString(json, "output_col", out.output_col);
	ok &= ExtractJsonString(json, "op", out.comparison_op);
	ok &= ExtractJsonString(json, "threshold", out.threshold_sql);
	return ok;
}

string RefreshMetadata::FilteredGroupCountAuxMetaToJson(const FilteredGroupCountAuxMeta &meta) {
	return "{\"kind\":\"filtered_group_count\",\"aux_table\":" + SqlUtils::JsonQuote(meta.aux_table) +
	       ",\"source\":" + SqlUtils::JsonQuote(meta.source) + ",\"group_col\":" + SqlUtils::JsonQuote(meta.group_col) +
	       ",\"sum_col\":" + SqlUtils::JsonQuote(meta.sum_col) +
	       ",\"source_group_expr\":" + SqlUtils::JsonQuote(meta.source_group_expr) +
	       ",\"source_sum_expr\":" + SqlUtils::JsonQuote(meta.source_sum_expr) +
	       ",\"output_col\":" + SqlUtils::JsonQuote(meta.output_col) +
	       ",\"op\":" + SqlUtils::JsonQuote(meta.comparison_op) +
	       ",\"threshold\":" + SqlUtils::JsonQuote(meta.threshold_sql) + "}";
}

vector<string> RefreshMetadata::ExpectedDistinctAuxColumns(const DistinctAuxMeta &meta) {
	auto expected = meta.cols;
	expected.push_back("_count");
	return expected;
}

vector<string> RefreshMetadata::ExpectedFilteredGroupCountAuxColumns(const FilteredGroupCountAuxMeta &meta) {
	return vector<string> {meta.group_col, "openivm_sum"};
}

vector<string> RefreshMetadata::ExpectedSemiAntiAuxColumns(const SemiAntiAuxMeta &meta) {
	auto expected = meta.left_cols;
	expected.push_back("_left_count");
	expected.push_back("_match_count");
	return expected;
}

bool RefreshMetadata::AuxStateNeedsRepair(const string &view_name, const string &catalog_name,
                                          const string &schema_name) {
	DistinctAuxMeta distinct;
	if (GetDistinctAuxMeta(view_name, distinct) &&
	    !TableColumnsMatch(catalog_name, schema_name, distinct.aux_table, ExpectedDistinctAuxColumns(distinct))) {
		return true;
	}
	FilteredGroupCountAuxMeta filtered;
	if (GetFilteredGroupCountAuxMeta(view_name, filtered) &&
	    !TableColumnsMatch(catalog_name, schema_name, filtered.aux_table,
	                       ExpectedFilteredGroupCountAuxColumns(filtered))) {
		return true;
	}
	SemiAntiAuxMeta semi;
	if (GetSemiAntiAuxMeta(view_name, semi) &&
	    !TableColumnsMatch(catalog_name, schema_name, semi.aux_table, ExpectedSemiAntiAuxColumns(semi))) {
		return true;
	}
	return false;
}

} // namespace duckdb
