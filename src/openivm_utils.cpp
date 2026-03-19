#include "openivm_utils.hpp"

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
	std::regex table_name_regex(
	    R"(create\s+table\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");
	std::smatch match;
	if (std::regex_search(sql, match, table_name_regex)) {
		return match[1].str();
	}
	return "";
}

string OpenIVMUtils::ExtractViewName(const string &sql) {
	std::regex view_name_regex(
	    R"(create\s+(?:materialized\s+)?view\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)(?:\s*\([^)]*\)|\s+as\s+(.*)))");
	std::smatch match;
	if (std::regex_search(sql, match, view_name_regex)) {
		return match[1].str();
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
	query = std::regex_replace(query, std::regex("\\bmaterialized\\s+view\\b"), "table");
	query = regex_replace(query, std::regex("\\s*;$"), "");
}

string OpenIVMUtils::ExtractViewQuery(string &query) {
	std::regex rgx_create_view(
	    R"(create\s+(table|materialized view)\s+(?:if\s+not\s+exists\s+)?([a-zA-Z0-9_]+)\s+as\s+(.*))");
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

	std::string multiplicity_col = "_duckdb_ivm_multiplicity boolean default true";
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
			pk_columns += ", _duckdb_ivm_multiplicity";
		} else {
			pk_columns = "_duckdb_ivm_multiplicity";
		}

		columns += ", " + multiplicity_col + ", " + timestamp_col;
		columns += ", PRIMARY KEY(" + pk_columns + ")";

		output = "create table if not exists " + new_table_name + " (" + columns + ");\n";
	}

	return output;
}

void OpenIVMUtils::ReplaceCount(string &query) {
	std::regex pattern("(count\\((\\*|\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "count($2) as count_$2");
	query = std::regex_replace(query, std::regex("count_\\*"), "count_star");
}

void OpenIVMUtils::ReplaceSum(string &query) {
	std::regex pattern("(sum\\((\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "sum($2) as sum_$2");
}

void OpenIVMUtils::ReplaceMin(string &query) {
	std::regex pattern("(min\\((\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "min($2) as min_$2");
}

void OpenIVMUtils::ReplaceMax(string &query) {
	std::regex pattern("(max\\((\\w+)\\))(?![^()]*\\bas\\b)", std::regex_constants::icase);
	query = std::regex_replace(query, pattern, "max($2) as max_$2");
}

void OpenIVMUtils::RemoveRedundantWhitespaces(string &query) {
	query = std::regex_replace(query, std::regex("\\s+"), " ");
}

} // namespace duckdb
