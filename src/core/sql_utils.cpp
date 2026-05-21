#include "core/sql_utils.hpp"

#include "core/openivm_constants.hpp"
#include "duckdb.hpp"

#include <cctype>
#include <cstring>
#include <fstream>
#include <regex>
#include <sstream>

namespace duckdb {

static bool IsIdentifierChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static void SkipWhitespace(const string &sql, size_t &pos) {
	while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
		pos++;
	}
}

static bool StartsWithKeyword(const string &sql, size_t pos, const string &keyword) {
	if (pos + keyword.size() > sql.size()) {
		return false;
	}
	if (!StringUtil::CIEquals(sql.substr(pos, keyword.size()), keyword)) {
		return false;
	}
	return pos + keyword.size() == sql.size() || !IsIdentifierChar(sql[pos + keyword.size()]);
}

static string TrimSQLFragment(const string &input) {
	idx_t start = 0;
	while (start < input.size() && std::isspace(static_cast<unsigned char>(input[start]))) {
		start++;
	}
	idx_t end = input.size();
	while (end > start && std::isspace(static_cast<unsigned char>(input[end - 1]))) {
		end--;
	}
	return input.substr(start, end - start);
}

static bool ReadCreateTargetName(const string &sql, const string &object_keyword, string &out) {
	string lower = StringUtil::Lower(sql);
	size_t pos = lower.find("create");
	if (pos == string::npos) {
		return false;
	}
	pos += strlen("create");
	SkipWhitespace(sql, pos);
	if (StartsWithKeyword(lower, pos, "or")) {
		pos += strlen("or");
		SkipWhitespace(sql, pos);
		if (!StartsWithKeyword(lower, pos, "replace")) {
			return false;
		}
		pos += strlen("replace");
		SkipWhitespace(sql, pos);
	}
	if (object_keyword == "table") {
		if (!StartsWithKeyword(lower, pos, "table")) {
			return false;
		}
		pos += strlen("table");
	} else {
		if (StartsWithKeyword(lower, pos, "materialized")) {
			pos += strlen("materialized");
			SkipWhitespace(sql, pos);
		}
		if (!StartsWithKeyword(lower, pos, "view")) {
			return false;
		}
		pos += strlen("view");
	}
	SkipWhitespace(sql, pos);
	if (StartsWithKeyword(lower, pos, "if")) {
		pos += strlen("if");
		SkipWhitespace(sql, pos);
		if (!StartsWithKeyword(lower, pos, "not")) {
			return false;
		}
		pos += strlen("not");
		SkipWhitespace(sql, pos);
		if (!StartsWithKeyword(lower, pos, "exists")) {
			return false;
		}
		pos += strlen("exists");
		SkipWhitespace(sql, pos);
	}

	vector<string> parts;
	while (pos < sql.size()) {
		SkipWhitespace(sql, pos);
		string part;
		if (pos < sql.size() && sql[pos] == '"') {
			pos++;
			while (pos < sql.size()) {
				if (sql[pos] == '"') {
					if (pos + 1 < sql.size() && sql[pos + 1] == '"') {
						part += '"';
						pos += 2;
						continue;
					}
					pos++;
					break;
				}
				part += sql[pos++];
			}
		} else {
			while (pos < sql.size() && (IsIdentifierChar(sql[pos]) || sql[pos] == '-')) {
				part += sql[pos++];
			}
		}
		if (part.empty()) {
			break;
		}
		parts.push_back(part);
		SkipWhitespace(sql, pos);
		if (pos >= sql.size() || sql[pos] != '.') {
			break;
		}
		pos++;
	}
	if (parts.empty()) {
		return false;
	}
	out.clear();
	for (idx_t i = 0; i < parts.size(); i++) {
		if (i > 0) {
			out += ".";
		}
		out += parts[i];
	}
	return true;
}

static bool ReadIdentifierSegment(const string &sql, idx_t &pos, string &segment) {
	if (pos >= sql.size()) {
		return false;
	}
	idx_t start = pos;
	if (sql[pos] == '"') {
		pos++;
		string unquoted;
		while (pos < sql.size()) {
			char c = sql[pos++];
			if (c == '"') {
				if (pos < sql.size() && sql[pos] == '"') {
					unquoted += '"';
					pos++;
					continue;
				}
				segment = unquoted;
				return true;
			}
			unquoted += c;
		}
		pos = start;
		return false;
	}
	if (!(std::isalpha(static_cast<unsigned char>(sql[pos])) || sql[pos] == '_')) {
		return false;
	}
	pos++;
	while (pos < sql.size() && IsIdentifierChar(sql[pos])) {
		pos++;
	}
	segment = sql.substr(start, pos - start);
	return true;
}

static bool ReadQualifiedIdentifier(const string &sql, idx_t start, idx_t &end, string &identifier) {
	idx_t pos = start;
	string segment;
	if (!ReadIdentifierSegment(sql, pos, segment)) {
		return false;
	}
	identifier = segment;
	while (pos < sql.size() && sql[pos] == '.') {
		idx_t dot_pos = pos++;
		if (!ReadIdentifierSegment(sql, pos, segment)) {
			pos = dot_pos;
			break;
		}
		identifier += "." + segment;
	}
	end = pos;
	return true;
}

void SqlUtils::WriteFile(const string &filename, bool append, const string &compiled_query) {
	std::ofstream file;
	if (append) {
		file.open(filename, std::ios_base::app);
	} else {
		file.open(filename);
	}
	file << compiled_query << '\n';
	file.close();
}

string SqlUtils::ExtractTableName(const string &sql) {
	string name;
	if (ReadCreateTargetName(sql, "table", name)) {
		return name;
	}
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

string SqlUtils::ExtractViewName(const string &sql) {
	string name;
	if (ReadCreateTargetName(sql, "view", name)) {
		return name;
	}
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

string SqlUtils::EscapeSingleQuotes(const string &input) {
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

void SqlUtils::ReplaceMaterializedView(string &query) {
	query = std::regex_replace(query, std::regex("\\bmaterialized\\s+view\\b"), "table if not exists");
	query = regex_replace(query, std::regex("\\s*;$"), "");
}

string SqlUtils::ExtractViewQuery(string &query) {
	string lower = StringUtil::Lower(query);
	size_t pos = lower.find("create");
	if (pos != string::npos) {
		pos += strlen("create");
		SkipWhitespace(query, pos);
		if (StartsWithKeyword(lower, pos, "or")) {
			pos += strlen("or");
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "replace")) {
				pos += strlen("replace");
				SkipWhitespace(query, pos);
			}
		}
		bool found_object = false;
		if (StartsWithKeyword(lower, pos, "table")) {
			pos += strlen("table");
			found_object = true;
		} else if (StartsWithKeyword(lower, pos, "materialized")) {
			pos += strlen("materialized");
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "view")) {
				pos += strlen("view");
				found_object = true;
			}
		}
		if (found_object) {
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "if")) {
				pos += strlen("if");
				SkipWhitespace(query, pos);
				if (StartsWithKeyword(lower, pos, "not")) {
					pos += strlen("not");
					SkipWhitespace(query, pos);
				}
				if (StartsWithKeyword(lower, pos, "exists")) {
					pos += strlen("exists");
					SkipWhitespace(query, pos);
				}
			}
			while (pos < query.size()) {
				SkipWhitespace(query, pos);
				if (pos < query.size() && query[pos] == '"') {
					pos++;
					while (pos < query.size()) {
						if (query[pos] == '"') {
							pos++;
							if (pos < query.size() && query[pos] == '"') {
								pos++;
								continue;
							}
							break;
						}
						pos++;
					}
				} else {
					while (pos < query.size() && (IsIdentifierChar(query[pos]) || query[pos] == '-')) {
						pos++;
					}
				}
				SkipWhitespace(query, pos);
				if (pos >= query.size() || query[pos] != '.') {
					break;
				}
				pos++;
			}
			SkipWhitespace(query, pos);
			if (StartsWithKeyword(lower, pos, "as")) {
				pos += strlen("as");
				return query.substr(pos);
			}
		}
	}
	// Match only up through "AS " — then return everything after as a raw substr so
	// multi-line CTE bodies (which contain '\n') are not truncated by regex '.' semantics.
	std::regex rgx_create_view(
	    R"re(create\s+(table|materialized view)\s+(?:if\s+not\s+exists\s+)?("(?:[^"]+)"|[a-zA-Z0-9_.]+)\s+as\s+)re");
	std::smatch match;
	if (std::regex_search(query, match, rgx_create_view)) {
		return query.substr(static_cast<size_t>(match.position()) + static_cast<size_t>(match.length()));
	}
	return "";
}

string SqlUtils::SQLToLowercase(const string &sql) {
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

string SqlUtils::GenerateDeltaTable(string &input) {
	input = SQLToLowercase(input);
	input = std::regex_replace(input, std::regex(R"(\")"), "");

	std::regex create_table_re(R"(create\s+table\s+([^\s\(\)]+(?:\.[^\s\(\)]+){0,2})\s*\(([^;]+)\);)",
	                           std::regex::icase);
	std::regex primary_key_re(R"((primary\s+key\s*\([^\)]+\)))", std::regex::icase);
	std::regex inline_primary_key_re(R"(([^\s,]+[^\),]*\s+primary\s+key))", std::regex::icase);

	std::string multiplicity_col = string(openivm::MULTIPLICITY_COL) + " integer default 1";
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

		std::string new_table_name = prefix + string(openivm::DELTA_PREFIX) + table_name;

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
			pk_columns += ", " + string(openivm::MULTIPLICITY_COL);
		} else {
			pk_columns = string(openivm::MULTIPLICITY_COL);
		}

		columns += ", " + multiplicity_col + ", " + timestamp_col;
		columns += ", PRIMARY KEY(" + pk_columns + ")";

		output = "create table if not exists " + new_table_name + " (" + columns + ");\n";
	}

	return output;
}

void SqlUtils::RemoveRedundantWhitespaces(string &query) {
	query = std::regex_replace(query, std::regex("\\s+"), " ");
}

void SqlUtils::StripLineComments(string &query) {
	string out;
	out.reserve(query.size());
	bool in_string = false;
	for (size_t i = 0; i < query.size(); ++i) {
		char c = query[i];
		if (c == '\'' && !in_string) {
			in_string = true;
			out += c;
		} else if (c == '\'' && in_string) {
			in_string = false;
			out += c;
		} else if (!in_string && c == '-' && i + 1 < query.size() && query[i + 1] == '-') {
			// Skip to end of line, preserve the newline so downstream \s+ collapse still works
			while (i < query.size() && query[i] != '\n') {
				++i;
			}
			if (i < query.size()) {
				out += '\n';
			}
		} else {
			out += c;
		}
	}
	query = std::move(out);
}

vector<string> SqlUtils::SplitSQLStatements(const string &sql) {
	vector<string> statements;
	idx_t start = 0;
	bool in_single_quote = false;
	bool in_double_quote = false;
	bool in_line_comment = false;
	bool in_block_comment = false;

	for (idx_t i = 0; i < sql.size(); i++) {
		auto c = sql[i];
		auto next = i + 1 < sql.size() ? sql[i + 1] : '\0';

		if (in_line_comment) {
			if (c == '\n') {
				in_line_comment = false;
			}
			continue;
		}
		if (in_block_comment) {
			if (c == '*' && next == '/') {
				in_block_comment = false;
				i++;
			}
			continue;
		}
		if (in_single_quote) {
			if (c == '\'' && next == '\'') {
				i++;
			} else if (c == '\'') {
				in_single_quote = false;
			}
			continue;
		}
		if (in_double_quote) {
			if (c == '"' && next == '"') {
				i++;
			} else if (c == '"') {
				in_double_quote = false;
			}
			continue;
		}

		if (c == '-' && next == '-') {
			in_line_comment = true;
			i++;
			continue;
		}
		if (c == '/' && next == '*') {
			in_block_comment = true;
			i++;
			continue;
		}
		if (c == '\'') {
			in_single_quote = true;
			continue;
		}
		if (c == '"') {
			in_double_quote = true;
			continue;
		}
		if (c == ';') {
			auto statement = TrimSQLFragment(sql.substr(start, i - start));
			if (!statement.empty()) {
				statements.push_back(std::move(statement));
			}
			start = i + 1;
		}
	}

	auto statement = TrimSQLFragment(sql.substr(start));
	if (!statement.empty()) {
		statements.push_back(std::move(statement));
	}
	return statements;
}

string SqlUtils::SQLStatementPreview(const string &statement) {
	auto trimmed = TrimSQLFragment(statement);
	string preview;
	bool in_space = false;
	for (auto c : trimmed) {
		if (std::isspace(static_cast<unsigned char>(c))) {
			if (!in_space && !preview.empty()) {
				preview += ' ';
				in_space = true;
			}
			continue;
		}
		preview += c;
		in_space = false;
		if (preview.size() >= 120) {
			preview += "...";
			break;
		}
	}
	return preview;
}

string SqlUtils::DeltaName(const string &name) {
	// Delta tables use the user-facing view name, not the internal data table name.
	// Strip openivm_data_ prefix if present (handles LPTS-generated SQL that references
	// the data table directly via VIEW expansion).
	static const string data_prefix(openivm::DATA_TABLE_PREFIX);
	if (name.size() > data_prefix.size() && name.rfind(data_prefix, 0) == 0) {
		return string(openivm::DELTA_PREFIX) + name.substr(data_prefix.size());
	}
	return string(openivm::DELTA_PREFIX) + name;
}

string SqlUtils::LastIdentifierPart(string name) {
	StringUtil::Trim(name);
	idx_t dot_pos = DConstants::INVALID_INDEX;
	bool in_quote = false;
	for (idx_t i = 0; i < name.size(); i++) {
		if (name[i] == '"') {
			in_quote = !in_quote;
			continue;
		}
		if (!in_quote && name[i] == '.') {
			dot_pos = i;
		}
	}
	if (dot_pos != DConstants::INVALID_INDEX) {
		name = name.substr(dot_pos + 1);
	}
	if (name.size() >= 2 && name.front() == '"' && name.back() == '"') {
		name = name.substr(1, name.size() - 2);
	}
	return name;
}

string SqlUtils::FullName(const string &catalog, const string &schema, const string &table) {
	return QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + "." + QuoteIdentifier(table);
}

string SqlUtils::FullDeltaName(const string &catalog, const string &schema, const string &table) {
	static const string data_prefix(openivm::DATA_TABLE_PREFIX);
	string base = table;
	if (base.size() > data_prefix.size() && base.rfind(data_prefix, 0) == 0) {
		base = base.substr(data_prefix.size());
	}
	return QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + "." + QuoteIdentifier(DeltaName(base));
}

string SqlUtils::QualifiedPrefix(const string &catalog, const string &schema) {
	return QuoteIdentifier(catalog) + "." + QuoteIdentifier(schema) + ".";
}

string SqlUtils::QuoteQualifiedPrefix(const string &prefix) {
	if (prefix.empty()) {
		return "";
	}
	vector<string> parts;
	string current;
	for (auto c : prefix) {
		if (c == '.') {
			if (!current.empty()) {
				parts.push_back(current);
				current.clear();
			}
			continue;
		}
		current += c;
	}
	if (!current.empty()) {
		parts.push_back(current);
	}
	string quoted;
	for (auto &part : parts) {
		quoted += QuoteIdentifier(part) + ".";
	}
	return quoted;
}

string SqlUtils::JsonQuote(const string &value) {
	string result = "\"";
	for (char c : value) {
		if (c == '"' || c == '\\') {
			result += '\\';
			result += c;
		} else if (c == '\n') {
			result += "\\n";
		} else {
			result += c;
		}
	}
	result += "\"";
	return result;
}

string SqlUtils::JsonArray(const vector<string> &values) {
	string result = "[";
	for (idx_t i = 0; i < values.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		result += JsonQuote(values[i]);
	}
	result += "]";
	return result;
}

string SqlUtils::DuckLakeTableFunction(const string &function_name, const string &catalog, const string &schema,
                                       const string &table, int64_t last_snapshot_id, int64_t current_snapshot_id) {
	return function_name + "('" + EscapeValue(catalog) + "', '" + EscapeValue(schema) + "', '" + EscapeValue(table) +
	       "', " + to_string(last_snapshot_id) + ", " + to_string(current_snapshot_id) + ")";
}

bool SqlUtils::IsDelta(const string &name) {
	static const string prefix(openivm::DELTA_PREFIX);
	return name.size() >= prefix.size() && name.rfind(prefix, 0) == 0;
}

string SqlUtils::JoinQuotedColumns(const vector<string> &columns) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += QuoteIdentifier(columns[i]);
	}
	return result;
}

string SqlUtils::JoinQualifiedQuotedColumns(const vector<string> &columns, const string &alias) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += alias + "." + QuoteIdentifier(columns[i]);
	}
	return result;
}

string SqlUtils::BuildAllNullPredicate(const vector<string> &columns) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += QuoteIdentifier(columns[i]) + " IS NULL";
	}
	return result;
}

string SqlUtils::BuildNullSafeMatch(const vector<string> &columns, const string &lhs_alias, const string &rhs_alias) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += lhs_alias + "." + QuoteIdentifier(columns[i]) + " IS NOT DISTINCT FROM " + rhs_alias + "." +
		          QuoteIdentifier(columns[i]);
	}
	return result;
}

string SqlUtils::BuildNullSafeKeyPredicate(const vector<string> &columns, const string &left_prefix,
                                           const string &right_prefix) {
	string result;
	for (idx_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		auto column = QuoteIdentifier(columns[i]);
		result += left_prefix + column + " IS NOT DISTINCT FROM " + right_prefix + column;
	}
	return result;
}

string SqlUtils::BuildFullRecomputeSQL(const string &data_table, const string &view_query_sql) {
	return "DELETE FROM " + data_table + ";\n" + "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
}

string SqlUtils::ReplaceAllOccurrences(string haystack, const string &needle, const string &replacement) {
	if (needle.empty()) {
		return haystack;
	}
	size_t pos = 0;
	while ((pos = haystack.find(needle, pos)) != string::npos) {
		haystack.replace(pos, needle.size(), replacement);
		pos += replacement.size();
	}
	return haystack;
}

vector<string> SqlUtils::ReplaceEachPlainOccurrence(const string &haystack, const string &needle,
                                                    const string &replacement) {
	vector<string> variants;
	if (needle.empty()) {
		return variants;
	}
	size_t pos = 0;
	while ((pos = haystack.find(needle, pos)) != string::npos) {
		string variant = haystack;
		variant.replace(pos, needle.size(), replacement);
		variants.push_back(std::move(variant));
		pos += needle.size();
	}
	return variants;
}

bool SqlUtils::IdentifierMatchesTable(const string &identifier, const string &table_name) {
	if (StringUtil::CIEquals(identifier, table_name)) {
		return true;
	}
	if (identifier.size() <= table_name.size()) {
		return false;
	}
	auto suffix = identifier.substr(identifier.size() - table_name.size());
	return identifier[identifier.size() - table_name.size() - 1] == '.' && StringUtil::CIEquals(suffix, table_name);
}

struct TableReferenceMatch {
	idx_t start;
	idx_t end;
};

static bool IsIdentifierTokenStart(char c) {
	return std::isalpha(static_cast<unsigned char>(c)) || c == '_' || c == '"';
}

static bool IsTableReferenceKeyword(const string &identifier) {
	return StringUtil::CIEquals(identifier, "from") || StringUtil::CIEquals(identifier, "join") ||
	       StringUtil::CIEquals(identifier, "update") || StringUtil::CIEquals(identifier, "into");
}

static idx_t ExtendPastTimeTravelClause(const string &sql, idx_t end) {
	idx_t at_pos = end;
	while (at_pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[at_pos]))) {
		at_pos++;
	}
	if (!StringUtil::CIStartsWith(sql.substr(at_pos), "AT (")) {
		return end;
	}
	idx_t depth = 0;
	while (at_pos < sql.size()) {
		if (sql[at_pos] == '(') {
			depth++;
		} else if (sql[at_pos] == ')') {
			depth--;
			if (depth == 0) {
				return at_pos + 1;
			}
		}
		at_pos++;
	}
	return end;
}

static vector<TableReferenceMatch> CollectTableReferenceMatches(const string &sql, const string &table_name,
                                                                bool include_time_travel_clause) {
	vector<TableReferenceMatch> matches;
	if (table_name.empty()) {
		return matches;
	}
	bool in_single_quote = false;
	bool expect_table = false;
	for (idx_t i = 0; i < sql.size();) {
		char c = sql[i];
		if (c == '\'') {
			i++;
			if (in_single_quote && i < sql.size() && sql[i] == '\'') {
				i++;
				continue;
			}
			in_single_quote = !in_single_quote;
			continue;
		}
		if (!in_single_quote && IsIdentifierTokenStart(c)) {
			idx_t start = i;
			idx_t end = i;
			string qualified_identifier;
			if (ReadQualifiedIdentifier(sql, start, end, qualified_identifier)) {
				if (expect_table && SqlUtils::IdentifierMatchesTable(qualified_identifier, table_name)) {
					matches.push_back({start, include_time_travel_clause ? ExtendPastTimeTravelClause(sql, end) : end});
					i = matches.back().end;
					expect_table = false;
					continue;
				}
				i = end;
				expect_table = IsTableReferenceKeyword(qualified_identifier);
				continue;
			}
		}
		if (!std::isspace(static_cast<unsigned char>(c))) {
			expect_table = false;
		}
		i++;
	}
	return matches;
}

string SqlUtils::FindTableReference(const string &sql, const string &table_name) {
	auto matches = CollectTableReferenceMatches(sql, table_name, false);
	if (matches.empty()) {
		return "";
	}
	auto &match = matches[0];
	return sql.substr(match.start, match.end - match.start);
}

idx_t SqlUtils::CountTableReferences(const string &sql, const string &table_name) {
	return CollectTableReferenceMatches(sql, table_name, false).size();
}

string SqlUtils::ReplaceTableReferences(const string &sql, const string &table_name, const string &replacement) {
	auto matches = CollectTableReferenceMatches(sql, table_name, true);
	if (matches.empty()) {
		return sql;
	}
	string result;
	result.reserve(sql.size());
	idx_t last = 0;
	for (auto &match : matches) {
		result += sql.substr(last, match.start - last);
		result += replacement;
		last = match.end;
	}
	result += sql.substr(last);
	return result;
}

string SqlUtils::ReplaceTableReferenceOccurrence(const string &sql, const string &table_name, idx_t occurrence,
                                                 const string &replacement, bool &replaced) {
	replaced = false;
	auto matches = CollectTableReferenceMatches(sql, table_name, true);
	if (occurrence >= matches.size()) {
		return sql;
	}
	auto &match = matches[occurrence];
	string result;
	result.reserve(sql.size());
	result += sql.substr(0, match.start);
	result += replacement;
	result += sql.substr(match.end);
	replaced = true;
	return result;
}

vector<string> SqlUtils::ReplaceEachTableReference(const string &sql, const string &table_name,
                                                   const string &replacement) {
	vector<string> variants;
	for (auto &match : CollectTableReferenceMatches(sql, table_name, false)) {
		string variant = sql;
		variant.replace(match.start, match.end - match.start, replacement);
		variants.push_back(std::move(variant));
	}
	return variants;
}

int64_t SqlUtils::ParseRefreshInterval(const string &interval_str) {
	std::regex interval_re(R"((\d+)\s*(minute|minutes|min|hour|hours|day|days))", std::regex::icase);
	std::smatch match;
	if (!std::regex_match(interval_str, match, interval_re)) {
		throw ParserException("Invalid REFRESH EVERY interval: '" + interval_str +
		                      "'. Expected format: '<N> <minutes|hours|days>'");
	}
	int64_t value = std::stoll(match[1].str());
	if (value <= 0) {
		throw ParserException("REFRESH EVERY interval must be a positive number (got " + to_string(value) + ")");
	}
	string unit = StringUtil::Lower(match[2].str());
	int64_t seconds;
	if (unit == "minute" || unit == "minutes" || unit == "min") {
		seconds = value * 60;
	} else if (unit == "hour" || unit == "hours") {
		seconds = value * 3600;
	} else if (unit == "day" || unit == "days") {
		seconds = value * 86400;
	} else {
		throw ParserException("Unknown time unit: '" + unit + "'");
	}
	return seconds;
}

int64_t SqlUtils::ExtractRefreshInterval(string &query) {
	std::regex refresh_re(R"(refresh\s+every\s+'([^']+)')", std::regex::icase);
	std::smatch match;
	if (!std::regex_search(query, match, refresh_re)) {
		return -1;
	}
	string interval_str = match[1].str();
	// Strip the REFRESH EVERY clause from the query
	query = std::regex_replace(query, refresh_re, "");
	SqlUtils::RemoveRedundantWhitespaces(query);
	return ParseRefreshInterval(interval_str);
}

} // namespace duckdb
