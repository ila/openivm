#include "openivm_metadata.hpp"

namespace duckdb {

bool IVMMetadata::IsBaseTable(const string &table_name) {
	auto result =
	    con.Query("SELECT 1 FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" + table_name + "'");
	return !result->HasError() && result->RowCount() == 0;
}

string IVMMetadata::GetViewQuery(const string &view_name) {
	auto result =
	    con.Query("SELECT sql_string FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" + view_name + "'");
	if (result->HasError() || result->RowCount() == 0) {
		return "";
	}
	return result->GetValue(0, 0).ToString();
}

IVMType IVMMetadata::GetViewType(const string &view_name) {
	auto result =
	    con.Query("SELECT type FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" + view_name + "'");
	if (result->HasError() || result->RowCount() == 0) {
		throw ParserException("View not found! Please call IVM with a materialized view.");
	}
	return static_cast<IVMType>(result->GetValue(0, 0).GetValue<int8_t>());
}

vector<string> IVMMetadata::GetDeltaTables(const string &view_name) {
	auto result = con.Query("SELECT table_name FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                         view_name + "'");
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
	                         view_name + "' AND table_name = '" + table_name + "'");
	if (result->HasError() || result->RowCount() == 0) {
		return "";
	}
	return result->GetValue(0, 0).ToString();
}

void IVMMetadata::UpdateTimestamp(const string &view_name) {
	auto result = con.Query("UPDATE " + string(ivm::DELTA_TABLES_TABLE) +
	                         " SET last_update = now() WHERE view_name = '" + view_name + "'");
	if (result->HasError()) {
		throw InternalException("Cannot update IVM metadata timestamp: " + result->GetError());
	}
}

} // namespace duckdb
