#include "match/constraint_cache.hpp"

#include "core/openivm_constants.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/connection.hpp"

#include <cctype>

namespace duckdb {
namespace openivm {

namespace {

static vector<string> ParseColumnList(const string &raw) {
	vector<string> columns;
	string input = raw;
	StringUtil::Trim(input);
	if (input.empty()) {
		return columns;
	}
	if (input.front() == '[') {
		size_t pos = 1;
		while (pos < input.size()) {
			while (pos < input.size() && (std::isspace(static_cast<unsigned char>(input[pos])) || input[pos] == ',')) {
				pos++;
			}
			if (pos >= input.size() || input[pos] == ']') {
				break;
			}
			if (input[pos] != '"') {
				return {};
			}
			pos++;
			string value;
			while (pos < input.size()) {
				char c = input[pos++];
				if (c == '\\' && pos < input.size()) {
					char esc = input[pos++];
					value += (esc == 'n') ? '\n' : esc;
					continue;
				}
				if (c == '"') {
					columns.push_back(value);
					break;
				}
				value += c;
			}
		}
		return columns;
	}

	size_t start = 0;
	while (start <= input.size()) {
		size_t end = input.find(',', start);
		string value = input.substr(start, end == string::npos ? string::npos : end - start);
		StringUtil::Trim(value);
		if ((value.size() >= 2 && value.front() == '"' && value.back() == '"') ||
		    (value.size() >= 2 && value.front() == '\'' && value.back() == '\'')) {
			value = value.substr(1, value.size() - 2);
		}
		if (!value.empty()) {
			columns.push_back(value);
		}
		if (end == string::npos) {
			break;
		}
		start = end + 1;
	}
	return columns;
}

static bool SameColumns(const vector<string> &left, const vector<string> &right) {
	if (left.size() != right.size()) {
		return false;
	}
	for (idx_t i = 0; i < left.size(); i++) {
		if (!StringUtil::CIEquals(left[i], right[i])) {
			return false;
		}
	}
	return true;
}

} // namespace

vector<CachedConstraint> ConstraintCache::GetConstraints(ClientContext &context, const string &table_name) {
	vector<CachedConstraint> constraints;
	Connection con(*context.db);
	auto result = con.Query("SELECT table_name, constraint_kind, columns_json, referenced_table, "
	                        "referenced_columns_json, is_trusted FROM " +
	                        string(CONSTRAINTS_CACHE_TABLE));
	if (result->HasError()) {
		return constraints;
	}
	for (idx_t r = 0; r < result->RowCount(); r++) {
		string stored_table = result->GetValue(0, r).ToString();
		if (!SqlUtils::IdentifierMatchesTable(stored_table, table_name)) {
			continue;
		}
		CachedConstraint c;
		c.table_name = stored_table;
		c.kind = result->GetValue(1, r).ToString();
		c.columns = ParseColumnList(result->GetValue(2, r).ToString());
		if (!result->GetValue(3, r).IsNull()) {
			c.referenced_table = result->GetValue(3, r).ToString();
		}
		if (!result->GetValue(4, r).IsNull()) {
			c.referenced_columns = ParseColumnList(result->GetValue(4, r).ToString());
		}
		c.is_trusted = result->GetValue(5, r).IsNull() || result->GetValue(5, r).GetValue<bool>();
		if (!c.columns.empty()) {
			constraints.push_back(std::move(c));
		}
	}

	std::lock_guard<std::mutex> lock(cache_mutex_);
	cache_[table_name] = constraints;
	return constraints;
}

bool ConstraintCache::IsUniqueKey(ClientContext &context, const string &table_name, const vector<string> &columns) {
	auto cs = GetConstraints(context, table_name);
	for (const auto &c : cs) {
		if ((StringUtil::CIEquals(c.kind, "PK") || StringUtil::CIEquals(c.kind, "UNIQUE")) &&
		    SameColumns(c.columns, columns)) {
			return true;
		}
	}
	return false;
}

bool ConstraintCache::HasFKToParent(ClientContext &context, const string &child_table,
                                    const vector<string> &child_columns, const string &parent_table,
                                    const vector<string> &parent_columns) {
	auto cs = GetConstraints(context, child_table);
	for (const auto &c : cs) {
		if ((StringUtil::CIEquals(c.kind, "FK") || StringUtil::CIEquals(c.kind, "RELY_FK")) && c.is_trusted &&
		    SameColumns(c.columns, child_columns) &&
		    SqlUtils::IdentifierMatchesTable(c.referenced_table, parent_table) &&
		    SameColumns(c.referenced_columns, parent_columns)) {
			return true;
		}
	}
	return false;
}

void ConstraintCache::DeclareRelyFK(ClientContext &context, const CachedConstraint &c) {
	Connection con(*context.db);
	con.Query("INSERT OR REPLACE INTO " + string(CONSTRAINTS_CACHE_TABLE) +
	          " (table_name, constraint_kind, columns_json, referenced_table, referenced_columns_json, is_trusted) "
	          "VALUES ('" +
	          SqlUtils::EscapeValue(c.table_name) + "', 'RELY_FK', '" +
	          SqlUtils::EscapeValue(SqlUtils::JsonArray(c.columns)) + "', '" +
	          SqlUtils::EscapeValue(c.referenced_table) + "', '" +
	          SqlUtils::EscapeValue(SqlUtils::JsonArray(c.referenced_columns)) + "', true)");
	std::lock_guard<std::mutex> lock(cache_mutex_);
	cache_[c.table_name].push_back(c);
}

void ConstraintCache::InvalidateTable(const string &table_name) {
	std::lock_guard<std::mutex> lock(cache_mutex_);
	cache_.erase(table_name);
}

} // namespace openivm
} // namespace duckdb
