#include "core/openivm_metadata.hpp"

#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"
#include <sstream>

namespace duckdb {

bool IVMMetadata::IsBaseTable(const string &table_name) {
	auto result = con.Query("SELECT 1 FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(table_name) + "'");
	return !result->HasError() && result->RowCount() == 0;
}

string IVMMetadata::GetViewQuery(const string &view_name) {
	auto result = con.Query("SELECT sql_string FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		return "";
	}
	return result->GetValue(0, 0).ToString();
}

IVMType IVMMetadata::GetViewType(const string &view_name) {
	auto result = con.Query("SELECT type FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		throw ParserException("Materialized view '%s' does not exist in IVM metadata.", view_name);
	}
	return static_cast<IVMType>(result->GetValue(0, 0).GetValue<int8_t>());
}

bool IVMMetadata::HasMinMax(const string &view_name) {
	auto result = con.Query("SELECT has_minmax FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

bool IVMMetadata::HasLeftJoin(const string &view_name) {
	auto result = con.Query("SELECT has_left_join FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return false;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

vector<string> IVMMetadata::GetDeltaTables(const string &view_name) {
	auto result = con.Query("SELECT table_name FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
	vector<string> tables;
	if (!result->HasError()) {
		for (size_t i = 0; i < result->RowCount(); i++) {
			tables.push_back(result->GetValue(0, i).ToString());
		}
	}
	return tables;
}

string IVMMetadata::GetLastUpdate(const string &view_name, const string &table_name) {
	auto result = con.Query("SELECT last_update FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
	                        OpenIVMUtils::EscapeValue(table_name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		return "";
	}
	return result->GetValue(0, 0).ToString();
}

void IVMMetadata::UpdateTimestamp(const string &view_name) {
	auto result =
	    con.Query("UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_update = now() WHERE view_name = '" +
	              OpenIVMUtils::EscapeValue(view_name) + "'");
	if (result->HasError()) {
		throw InternalException("Cannot update IVM metadata timestamp: " + result->GetError());
	}
}

vector<string> IVMMetadata::GetUpstreamViews(const string &view_name) {
	// Walk upstream: for view V, find its delta tables. If a delta table is "delta_X"
	// and X is a registered MV, then X is an upstream dependency. Recurse.
	vector<string> result;
	unordered_set<string> visited;

	std::function<void(const string &)> collect = [&](const string &vn) {
		auto delta_tables = GetDeltaTables(vn);
		for (auto &dt : delta_tables) {
			// Extract source MV name from delta table name.
			// Standard: "delta_<source>", DuckLake: "_ivm_data_<source>"
			string source;
			static const string delta_prefix(ivm::DELTA_PREFIX);
			static const string data_prefix(ivm::DATA_TABLE_PREFIX);
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

vector<string> IVMMetadata::GetDownstreamViews(const string &view_name) {
	// Find all views that depend on delta_<view_name> or _ivm_data_<view_name> as a source.
	// DuckLake chained MVs use _ivm_data_* (data table) instead of delta_* (delta table).
	vector<string> result;
	unordered_set<string> visited;

	std::function<void(const string &)> collect = [&](const string &vn) {
		string delta_name = OpenIVMUtils::DeltaName(vn);
		string data_name = IVMTableNames::DataTableName(vn);
		auto dependents = con.Query("SELECT DISTINCT view_name FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                            " WHERE table_name = '" + OpenIVMUtils::EscapeValue(delta_name) +
		                            "' OR table_name = '" + OpenIVMUtils::EscapeValue(data_name) + "'");
		if (!dependents->HasError()) {
			for (size_t i = 0; i < dependents->RowCount(); i++) {
				string dep = dependents->GetValue(0, i).ToString();
				if (visited.find(dep) == visited.end()) {
					visited.insert(dep);
					result.push_back(dep); // closest first
					collect(dep);          // then recurse deeper
				}
			}
		}
	};
	collect(view_name);
	return result; // topological order: closest descendants first
}

vector<string> IVMMetadata::GetGroupColumns(const string &view_name) {
	auto result = con.Query("SELECT group_columns FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
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

vector<string> IVMMetadata::GetAggregateTypes(const string &view_name) {
	auto result = con.Query("SELECT aggregate_types FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
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

int64_t IVMMetadata::GetRefreshInterval(const string &view_name) {
	auto result = con.Query("SELECT refresh_interval FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return -1;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

vector<IVMMetadata::ScheduledView> IVMMetadata::GetScheduledViews() {
	auto result = con.Query("SELECT v.view_name, v.refresh_interval, "
	                        "(SELECT MIN(d.last_update) FROM " +
	                        string(ivm::DELTA_TABLES_TABLE) +
	                        " d WHERE d.view_name = v.view_name) AS last_update "
	                        "FROM " +
	                        string(ivm::VIEWS_TABLE) + " v WHERE v.refresh_interval IS NOT NULL");
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

void IVMMetadata::SetRefreshInProgress(const string &view_name, bool in_progress) {
	con.Query("UPDATE " + string(ivm::VIEWS_TABLE) + " SET refresh_in_progress = " + (in_progress ? "true" : "false") +
	          " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "'");
}

string IVMMetadata::BuildDeltaCleanupSQL(const string &target, const string &metadata_key) {
	return "DELETE FROM " + KeywordHelper::WriteOptionallyQuoted(target) + " WHERE " + string(ivm::TIMESTAMP_COL) +
	       " < (SELECT MIN(last_update) FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE table_name = '" +
	       OpenIVMUtils::EscapeValue(metadata_key) + "');\n";
}

// --- DuckLake support ---

string IVMMetadata::GetCatalogType(const string &view_name, const string &table_name) {
	auto result = con.Query("SELECT catalog_type FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
	                        OpenIVMUtils::EscapeValue(table_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return "duckdb";
	}
	return result->GetValue(0, 0).ToString();
}

bool IVMMetadata::IsDuckLakeTable(const string &view_name, const string &table_name) {
	return GetCatalogType(view_name, table_name) == "ducklake";
}

int64_t IVMMetadata::GetLastSnapshotId(const string &view_name, const string &table_name) {
	auto result = con.Query("SELECT last_snapshot_id FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                        OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
	                        OpenIVMUtils::EscapeValue(table_name) + "'");
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return -1;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

void IVMMetadata::UpdateSnapshotId(const string &view_name, const string &table_name, int64_t snapshot_id) {
	auto result =
	    con.Query("UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_snapshot_id = " + to_string(snapshot_id) +
	              " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "' AND table_name = '" +
	              OpenIVMUtils::EscapeValue(table_name) + "'");
	if (result->HasError()) {
		throw InternalException("Cannot update DuckLake snapshot ID: " + result->GetError());
	}
}

// --- Refresh history (learned cost model) ---

void IVMMetadata::RecordRefreshHistory(const string &view_name, const string &method, double ivm_compute_est,
                                       double ivm_upsert_est, double recompute_compute_est,
                                       double recompute_replace_est, int64_t actual_duration_ms, idx_t max_history) {
	auto result = con.Query("INSERT INTO " + string(ivm::HISTORY_TABLE) +
	                        " (view_name, method, ivm_compute_est, ivm_upsert_est,"
	                        " recompute_compute_est, recompute_replace_est, actual_duration_ms)"
	                        " VALUES ('" +
	                        OpenIVMUtils::EscapeValue(view_name) + "', '" + OpenIVMUtils::EscapeValue(method) + "', " +
	                        to_string(ivm_compute_est) + ", " + to_string(ivm_upsert_est) + ", " +
	                        to_string(recompute_compute_est) + ", " + to_string(recompute_replace_est) + ", " +
	                        to_string(actual_duration_ms) + ")");
	if (result->HasError()) {
		OPENIVM_DEBUG_PRINT("[HISTORY] Failed to record: %s\n", result->GetError().c_str());
		return;
	}

	// Prune old entries beyond the window
	con.Query("DELETE FROM " + string(ivm::HISTORY_TABLE) + " WHERE view_name = '" +
	          OpenIVMUtils::EscapeValue(view_name) + "' AND refresh_timestamp NOT IN (SELECT refresh_timestamp FROM " +
	          string(ivm::HISTORY_TABLE) + " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
	          "' ORDER BY refresh_timestamp DESC LIMIT " + to_string(max_history) + ")");
}

vector<IVMMetadata::RefreshHistoryEntry> IVMMetadata::GetRefreshHistory(const string &view_name, const string &method,
                                                                        idx_t limit) {
	// Select the cost components for the given method:
	// - For 'incremental': use ivm_compute_est, ivm_upsert_est
	// - For 'full': use recompute_compute_est, recompute_replace_est
	string col1 = (method == "incremental") ? "ivm_compute_est" : "recompute_compute_est";
	string col2 = (method == "incremental") ? "ivm_upsert_est" : "recompute_replace_est";

	auto result =
	    con.Query("SELECT " + col1 + ", " + col2 + ", actual_duration_ms FROM " + string(ivm::HISTORY_TABLE) +
	              " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) + "' AND method = '" +
	              OpenIVMUtils::EscapeValue(method) + "' ORDER BY refresh_timestamp ASC LIMIT " + to_string(limit));

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

} // namespace duckdb
