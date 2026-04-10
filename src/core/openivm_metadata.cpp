#include "core/openivm_metadata.hpp"

#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
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
		throw ParserException("View not found! Please call IVM with a materialized view.");
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
			// delta table name is "delta_<source>"; extract source name
			if (dt.size() > 6 && dt.substr(0, 6) == string(ivm::DELTA_PREFIX)) {
				string source = dt.substr(6);
				if (!IsBaseTable(source) && visited.find(source) == visited.end()) {
					visited.insert(source);
					collect(source); // recurse deeper first (ancestors before descendants)
					result.push_back(source);
				}
			}
		}
	};
	collect(view_name);
	return result; // topological order: ancestors first
}

vector<string> IVMMetadata::GetDownstreamViews(const string &view_name) {
	// Find all views that depend on delta_<view_name> as a source.
	vector<string> result;
	unordered_set<string> visited;

	std::function<void(const string &)> collect = [&](const string &vn) {
		string delta_name = OpenIVMUtils::DeltaName(vn);
		auto dependents = con.Query("SELECT DISTINCT view_name FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                            " WHERE table_name = '" + OpenIVMUtils::EscapeValue(delta_name) + "'");
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

} // namespace duckdb
