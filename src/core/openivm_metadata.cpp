#include "core/openivm_metadata.hpp"

#include "core/openivm_utils.hpp"

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

} // namespace duckdb
