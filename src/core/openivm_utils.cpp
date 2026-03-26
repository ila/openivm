#include "core/openivm_utils.hpp"

#include "core/openivm_constants.hpp"
#include "duckdb.hpp"

#include <fstream>
#include <regex>
#include <sstream>

namespace duckdb {

void OpenIVMUtils::WriteFile(const string &filename, bool append, const string &compiled_query) {
	std::ofstream file;
	if (append) {
		file.open(filename, std::ios_base::app);
	} else {
		file.open(filename);
	}
	file << compiled_query << '\n';
	file.close();
}

string OpenIVMUtils::ReadFile(const string &file_path) {
	string content;
	std::ifstream file(file_path);
	if (file.is_open()) {
		string line;
		while (std::getline(file, line)) {
			content += line + "\n";
		}
		file.close();
	}
	return content;
}

string OpenIVMUtils::ExtractTableName(const string &sql) {
	// Matches: CREATE TABLE [IF NOT EXISTS] name|"quoted name" (AS ...|(...))
	std::regex table_name_regex(
	    R"re(create\s+table\s+(?:if\s+not\s+exists\s+)?("(?:[^"]+)"|[a-zA-Z0-9_.]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))re");
	std::smatch match;
	if (std::regex_search(sql, match, table_name_regex)) {
		auto name = match[1].str();
		if (name.size() >= 2 && name.front() == '"' && name.back() == '"') {
			name = name.substr(1, name.size() - 2);
		}
		return name;
	}
	return "";
}

string OpenIVMUtils::ExtractViewName(const string &sql) {
	std::regex view_name_regex(
	    R"re(create\s+(?:materialized\s+)?view\s+(?:if\s+not\s+exists\s+)?("(?:[^"]+)"|[a-zA-Z0-9_.]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))re");
	std::smatch match;
	if (std::regex_search(sql, match, view_name_regex)) {
		auto name = match[1].str();
		if (name.size() >= 2 && name.front() == '"' && name.back() == '"') {
			name = name.substr(1, name.size() - 2);
		}
		return name;
	}
	return "";
}

string OpenIVMUtils::EscapeSingleQuotes(const string &input) {
	std::stringstream escaped_stream;
	for (char c : input) {
		if (c == '\'') {
			escaped_stream << "''";
		} else {
			escaped_stream << c;
		}
	}
	return escaped_stream.str();
}

void OpenIVMUtils::ReplaceMaterializedView(string &query) {
	query = std::regex_replace(query, std::regex("\\bmaterialized\\s+view\\b"), "table if not exists");
	query = regex_replace(query, std::regex("\\s*;$"), "");
}

string OpenIVMUtils::ExtractViewQuery(string &query) {
	std::regex rgx_create_view(
	    R"re(create\s+(table|materialized view)\s+(?:if\s+not\s+exists\s+)?("(?:[^"]+)"|[a-zA-Z0-9_.]+)\s+as\s+(.*))re");
	std::smatch match;
	if (std::regex_search(query, match, rgx_create_view)) {
		return match[3].str();
	}
	return "";
}

string OpenIVMUtils::SQLToLowercase(const string &sql) {
	std::stringstream lowercase_stream;
	bool in_string = false;
	for (char c : sql) {
		if (c == '\'') {
			in_string = !in_string;
		}
		if (!in_string) {
			lowercase_stream << (char)tolower(c);
		} else {
			lowercase_stream << c;
		}
	}
	return lowercase_stream.str();
}

string OpenIVMUtils::GenerateDeltaTable(string &input) {
	input = SQLToLowercase(input);
	input = std::regex_replace(input, std::regex(R"(\")"), "");

	std::regex create_table_re(R"(create\s+table\s+([^\s\(\)]+(?:\.[^\s\(\)]+){0,2})\s*\(([^;]+)\);)",
	                           std::regex::icase);
	std::regex primary_key_re(R"((primary\s+key\s*\([^\)]+\)))", std::regex::icase);
	std::regex inline_primary_key_re(R"(([^\s,]+[^\),]*\s+primary\s+key))", std::regex::icase);

	std::string multiplicity_col = string(ivm::MULTIPLICITY_COL) + " boolean default true";
	std::string timestamp_col = "timestamp timestamp default now()";

	std::smatch match;
	std::string output = input;

	if (std::regex_search(input, match, create_table_re)) {
		std::string full_table_name = match[1].str();
		std::string columns = match[2].str();
		std::string primary_key;
		std::string pk_columns;

		size_t last_dot_pos = full_table_name.find_last_of('.');
		std::string prefix, table_name;
		if (last_dot_pos != std::string::npos) {
			prefix = full_table_name.substr(0, last_dot_pos + 1);
			table_name = full_table_name.substr(last_dot_pos + 1);
		} else {
			table_name = full_table_name;
		}

		std::string new_table_name = prefix + "delta_" + table_name;

		if (std::regex_search(columns, match, primary_key_re)) {
			primary_key = match[0].str();
			pk_columns =
			    primary_key.substr(primary_key.find('(') + 1, primary_key.find(')') - primary_key.find('(') - 1);
			columns = std::regex_replace(columns, primary_key_re, "");
		}

		if (std::regex_search(columns, match, inline_primary_key_re)) {
			primary_key = match[0].str();
			std::string col_name = primary_key.substr(0, primary_key.find(' '));
			pk_columns = col_name;
			columns = std::regex_replace(columns, inline_primary_key_re, col_name);
		}

		if (!pk_columns.empty()) {
			pk_columns += ", " + string(ivm::MULTIPLICITY_COL);
		} else {
			pk_columns = string(ivm::MULTIPLICITY_COL);
		}

		columns += ", " + multiplicity_col + ", " + timestamp_col;
		columns += ", PRIMARY KEY(" + pk_columns + ")";

		output = "create table if not exists " + new_table_name + " (" + columns + ");\n";
	}

	return output;
}

/// Sanitize an expression for use as a SQL alias: replace non-alphanumeric chars with '_'.
static string SanitizeAlias(const string &expr) {
	string alias;
	for (char c : expr) {
		alias += (isalnum(c) || c == '_') ? c : '_';
	}
	return alias;
}

/// Generic aggregate function alias rewriter.
/// Matches func_name(expr) that aren't already aliased with AS,
/// and adds "as func_name_expr" (with sanitized alias).
static void ReplaceAggregate(string &query, const string &func_name) {
	string pat = func_name + R"(\(([^)]+)\)(?![^()]*\bas\b))";
	std::regex pattern(pat, std::regex_constants::icase);
	std::smatch match;
	string result;
	string remaining = query;
	while (std::regex_search(remaining, match, pattern)) {
		string arg = match[1].str();
		string alias = func_name + "_" + SanitizeAlias(arg);
		result += match.prefix().str() + func_name + "(" + arg + ") as " + alias;
		remaining = match.suffix().str();
	}
	result += remaining;
	query = result;
}

void OpenIVMUtils::ReplaceCount(string &query) {
	// count(*) needs special handling for the alias
	std::regex star_pattern(R"(count\(\*\)(?![^()]*\bas\b))", std::regex_constants::icase);
	query = std::regex_replace(query, star_pattern, "count(*) as count_star");
	// count(expr) uses the generic rewriter
	ReplaceAggregate(query, "count");
}

void OpenIVMUtils::ReplaceSum(string &query) {
	ReplaceAggregate(query, "sum");
}

void OpenIVMUtils::ReplaceMin(string &query) {
	ReplaceAggregate(query, "min");
}

void OpenIVMUtils::ReplaceMax(string &query) {
	ReplaceAggregate(query, "max");
}

void OpenIVMUtils::ReplaceAvg(string &query) {
	ReplaceAggregate(query, "avg");
}

void OpenIVMUtils::RemoveRedundantWhitespaces(string &query) {
	query = std::regex_replace(query, std::regex("\\s+"), " ");
}

string OpenIVMUtils::DeltaName(const string &name) {
	return string(ivm::DELTA_PREFIX) + name;
}

string OpenIVMUtils::FullName(const string &catalog, const string &schema, const string &table) {
	return catalog + "." + schema + "." + table;
}

string OpenIVMUtils::FullDeltaName(const string &catalog, const string &schema, const string &table) {
	return catalog + "." + schema + ".delta_" + table;
}

bool OpenIVMUtils::IsDelta(const string &name) {
	return name.size() >= 6 && name.rfind("delta_", 0) == 0;
}

string OpenIVMUtils::DbPath(ClientContext &context) {
	string db_path;
	if (!context.db->config.options.database_path.empty()) {
		db_path = context.db->GetFileSystem().GetWorkingDirectory();
	}
	Value db_path_value;
	context.TryGetCurrentSetting("ivm_files_path", db_path_value);
	if (!db_path_value.IsNull()) {
		db_path = db_path_value.ToString();
	}
	return db_path;
}

} // namespace duckdb
