#include "core/parser_sql_extractors.hpp"

#include "core/sql_utils.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <cstring>

namespace duckdb {

static bool IsIdentifierTokenChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static size_t FindKeywordToken(const string &text, const string &keyword, size_t from) {
	bool in_string = false;
	bool in_identifier_quote = false;
	for (size_t pos = from; pos + keyword.size() <= text.size(); pos++) {
		char c = text[pos];
		if (in_string) {
			if (c == '\'' && pos + 1 < text.size() && text[pos + 1] == '\'') {
				pos++;
				continue;
			}
			if (c == '\'') {
				in_string = false;
			}
			continue;
		}
		if (in_identifier_quote) {
			if (c == '"' && pos + 1 < text.size() && text[pos + 1] == '"') {
				pos++;
				continue;
			}
			if (c == '"') {
				in_identifier_quote = false;
			}
			continue;
		}
		if (c == '\'') {
			in_string = true;
			continue;
		}
		if (c == '"') {
			in_identifier_quote = true;
			continue;
		}
		if (text.compare(pos, keyword.size(), keyword) != 0) {
			continue;
		}
		bool ok_left = (pos == 0) || std::isspace(static_cast<unsigned char>(text[pos - 1])) || text[pos - 1] == '(';
		bool ok_right = (pos + keyword.size() == text.size()) ||
		                std::isspace(static_cast<unsigned char>(text[pos + keyword.size()])) ||
		                text[pos + keyword.size()] == '(';
		if (ok_left && ok_right) {
			return pos;
		}
	}
	return string::npos;
}

static bool StartsKeywordToken(const string &text, size_t pos, const string &keyword) {
	if (pos + keyword.size() > text.size()) {
		return false;
	}
	if (text.compare(pos, keyword.size(), keyword) != 0) {
		return false;
	}
	return (pos + keyword.size() == text.size()) || !IsIdentifierTokenChar(text[pos + keyword.size()]);
}

static bool StartsAnyKeywordToken(const string &text, size_t pos, std::initializer_list<const char *> keywords) {
	for (auto keyword : keywords) {
		if (StartsKeywordToken(text, pos, keyword)) {
			return true;
		}
	}
	return false;
}

/// Extract a `(SELECT DISTINCT cols FROM source [WHERE p])` subquery from the
/// user's CREATE-MV SQL. Single-source v0 — succeeds only for the simple shape
/// where the DISTINCT body has exactly one base table after FROM (joins/CTEs
/// fail extraction; caller demotes to GROUP_RECOMPUTE).
///
/// Output:
///   `out_cols`        — column names from `DISTINCT a, b, c`
///   `out_input_sql`   — same SELECT body with `DISTINCT` keyword stripped
///   `out_source`      — the table referenced after `FROM` (alias-stripped)
///   `out_filter_sql`  — anything between `WHERE` and the next clause boundary,
///                       or empty if no WHERE; included so the aux-population
///                       and refresh-time delta queries apply the same filter
///
/// Returns false on: no DISTINCT, multiple DISTINCTs, a non-table FROM (subquery,
/// CTE, JOIN), unbalanced parens, or any structural surprise. The caller treats
/// false as "demote to GROUP_RECOMPUTE".
bool ExtractInnerDistinct(const string &original_sql, vector<string> &out_cols, string &out_input_sql,
                          string &out_source, string &out_filter_sql) {
	string lower = StringUtil::Lower(original_sql);
	// Find "select distinct" — must be a token (preceded by '(' or whitespace).
	size_t distinct_pos = FindKeywordToken(lower, "select distinct", 0);
	if (distinct_pos == string::npos) {
		return false;
	}
	// Multiple DISTINCTs in the same view → unsupported in v0.
	if (FindKeywordToken(lower, "select distinct", distinct_pos + 1) != string::npos) {
		return false;
	}

	// Find " from " at the same paren level as the SELECT DISTINCT.
	size_t cols_start = distinct_pos + strlen("select distinct ");
	int depth = 0;
	size_t from_pos = string::npos;
	for (size_t i = cols_start; i < lower.size(); i++) {
		if (lower[i] == '(') {
			depth++;
		} else if (lower[i] == ')') {
			if (depth == 0) {
				return false; // unbalanced, abort
			}
			depth--;
		} else if (depth == 0) {
			static const string from_kw = " from ";
			if (i + from_kw.size() <= lower.size() && lower.compare(i, from_kw.size(), from_kw) == 0) {
				from_pos = i;
				break;
			}
		}
	}
	if (from_pos == string::npos) {
		return false;
	}

	// Parse the column list (comma-split at depth 0). Use the original-case substring.
	string cols_text = original_sql.substr(cols_start, from_pos - cols_start);
	vector<string> cols;
	{
		int pd = 0;
		size_t last = 0;
		for (size_t i = 0; i < cols_text.size(); i++) {
			if (cols_text[i] == '(') {
				pd++;
			} else if (cols_text[i] == ')') {
				pd--;
			} else if (pd == 0 && cols_text[i] == ',') {
				string c = cols_text.substr(last, i - last);
				StringUtil::Trim(c);
				cols.push_back(std::move(c));
				last = i + 1;
			}
		}
		string last_c = cols_text.substr(last);
		StringUtil::Trim(last_c);
		cols.push_back(std::move(last_c));
	}
	if (cols.empty()) {
		return false;
	}
	for (auto &c : cols) {
		if (c.empty() || c == "*") {
			return false; // unqualified `*` would need source-schema introspection — punt.
		}
	}

	// Read the FROM clause. Source must be a single bare identifier (or
	// schema.table); subqueries, JOINs, and CTE references abort.
	size_t after_from = from_pos + strlen(" from ");
	// Skip leading whitespace.
	while (after_from < lower.size() && std::isspace(static_cast<unsigned char>(lower[after_from]))) {
		after_from++;
	}
	if (after_from >= lower.size() || lower[after_from] == '(') {
		return false; // FROM is a subquery — multi-source/complex shape, demote.
	}
	// Read identifier (allow letters, digits, underscores, dots, double-quotes).
	size_t src_end = after_from;
	bool in_quote = false;
	while (src_end < lower.size()) {
		char c = lower[src_end];
		if (in_quote) {
			if (c == '"') {
				in_quote = false;
			}
			src_end++;
			continue;
		}
		if (c == '"') {
			in_quote = true;
			src_end++;
			continue;
		}
		if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.') {
			src_end++;
			continue;
		}
		break;
	}
	out_source = original_sql.substr(after_from, src_end - after_from);
	if (out_source.empty()) {
		return false;
	}

	// Skip optional alias (single bare word after the source identifier).
	size_t alias_skip = src_end;
	while (alias_skip < lower.size() && std::isspace(static_cast<unsigned char>(lower[alias_skip]))) {
		alias_skip++;
	}
	// Recognised keywords that terminate the FROM section.
	if (alias_skip < lower.size() && lower[alias_skip] != ',' && lower[alias_skip] != ')' &&
	    !StartsAnyKeywordToken(lower, alias_skip,
	                           {"where", "group", "order", "having", "limit", "union", "join", "left", "right", "inner",
	                            "full", "cross", "on"})) {
		// Treat as an alias word; consume it.
		size_t alias_end = alias_skip;
		while (alias_end < lower.size() &&
		       (std::isalnum(static_cast<unsigned char>(lower[alias_end])) || lower[alias_end] == '_')) {
			alias_end++;
		}
		alias_skip = alias_end;
	}
	// Anything other than WHERE / end-of-subquery here means we hit a JOIN or comma,
	// which we don't support in v0.
	while (alias_skip < lower.size() && std::isspace(static_cast<unsigned char>(lower[alias_skip]))) {
		alias_skip++;
	}
	size_t where_pos = string::npos;
	if (alias_skip < lower.size()) {
		if (StartsKeywordToken(lower, alias_skip, "where")) {
			where_pos = alias_skip;
		} else if (lower[alias_skip] != ')' &&
		           !StartsAnyKeywordToken(lower, alias_skip, {"group", "order", "limit", "union"})) {
			return false; // unsupported shape (JOIN, comma, etc.)
		}
	}

	// Compute the end of the DISTINCT subquery. If we're inside parens (the common
	// case `... FROM (SELECT DISTINCT ...) sub ...`), match the closing ')'. If not,
	// the subquery ends at the next clause boundary or end of statement.
	size_t end_pos = string::npos;
	int d = 0;
	for (size_t i = (where_pos != string::npos ? where_pos : alias_skip); i < lower.size(); i++) {
		if (lower[i] == '(') {
			d++;
		} else if (lower[i] == ')') {
			if (d == 0) {
				end_pos = i;
				break;
			}
			d--;
		} else if (d == 0 && StartsAnyKeywordToken(lower, i, {"group", "order", "limit", "union"})) {
			end_pos = i;
			break;
		}
	}
	if (end_pos == string::npos) {
		end_pos = lower.size();
	}

	// Build out_filter_sql from `WHERE ... <end>` (excluding WHERE keyword).
	if (where_pos != string::npos) {
		size_t filter_start = where_pos + strlen("where ");
		out_filter_sql = original_sql.substr(filter_start, end_pos - filter_start);
		StringUtil::Trim(out_filter_sql);
	} else {
		out_filter_sql.clear();
	}

	// Build input_sql: the original DISTINCT subquery span with `DISTINCT ` removed.
	string subq = original_sql.substr(distinct_pos, end_pos - distinct_pos);
	{
		string subq_lower = StringUtil::Lower(subq);
		size_t kw = subq_lower.find("distinct ");
		if (kw == string::npos) {
			return false;
		}
		out_input_sql = subq.substr(0, kw) + subq.substr(kw + strlen("distinct "));
		StringUtil::Trim(out_input_sql);
	}

	out_cols = std::move(cols);
	return true;
}

static bool ReadIdentifierToken(const string &sql, size_t &pos, string &out) {
	while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
		pos++;
	}
	if (pos >= sql.size()) {
		return false;
	}
	size_t start = pos;
	bool in_quote = false;
	while (pos < sql.size()) {
		char c = sql[pos];
		if (in_quote) {
			if (c == '"') {
				in_quote = false;
			}
			pos++;
			continue;
		}
		if (c == '"') {
			in_quote = true;
			pos++;
			continue;
		}
		if (std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.') {
			pos++;
			continue;
		}
		break;
	}
	out = sql.substr(start, pos - start);
	return !out.empty();
}

static size_t FindTopLevelKeywordToken(const string &text, const string &keyword, size_t from) {
	bool in_quote = false;
	int depth = 0;
	for (size_t i = from; i < text.size(); i++) {
		char c = text[i];
		if (in_quote) {
			if (c == '\'') {
				in_quote = false;
			}
			continue;
		}
		if (c == '\'') {
			in_quote = true;
			continue;
		}
		if (c == '(') {
			depth++;
			continue;
		}
		if (c == ')') {
			if (depth > 0) {
				depth--;
			}
			continue;
		}
		if (depth == 0 && StartsKeywordToken(text, i, keyword)) {
			return i;
		}
	}
	return string::npos;
}

static size_t FindMatchingParen(const string &text, size_t open_pos) {
	if (open_pos >= text.size() || text[open_pos] != '(') {
		return string::npos;
	}
	bool in_quote = false;
	int depth = 0;
	for (size_t i = open_pos; i < text.size(); i++) {
		char c = text[i];
		if (in_quote) {
			if (c == '\'') {
				in_quote = false;
			}
			continue;
		}
		if (c == '\'') {
			in_quote = true;
			continue;
		}
		if (c == '(') {
			depth++;
		} else if (c == ')') {
			depth--;
			if (depth == 0) {
				return i;
			}
		}
	}
	return string::npos;
}

struct SelectOutputItem {
	string expr;
	string name;
};

static bool ParseSelectOutputItems(const string &original_sql, size_t select_pos, size_t from_pos,
                                   vector<SelectOutputItem> &items) {
	string select_list = original_sql.substr(select_pos + strlen("select"), from_pos - (select_pos + strlen("select")));
	auto parse_item = [](string item, SelectOutputItem &out) {
		StringUtil::Trim(item);
		size_t as_pos = FindTopLevelKeywordToken(StringUtil::Lower(item), "as", 0);
		if (as_pos != string::npos) {
			out.expr = item.substr(0, as_pos);
			StringUtil::Trim(out.expr);
			string alias = item.substr(as_pos + strlen("as"));
			StringUtil::Trim(alias);
			size_t alias_pos = 0;
			string alias_token;
			if (ReadIdentifierToken(alias, alias_pos, alias_token)) {
				out.name = SqlUtils::LastIdentifierPart(alias_token);
				return !out.expr.empty() && !out.name.empty();
			}
		}
		out.expr = item;
		out.name = SqlUtils::LastIdentifierPart(item);
		return !out.expr.empty() && !out.name.empty();
	};
	int depth = 0;
	size_t last = 0;
	for (size_t i = 0; i < select_list.size(); i++) {
		if (select_list[i] == '(') {
			depth++;
		} else if (select_list[i] == ')') {
			depth--;
		} else if (depth == 0 && select_list[i] == ',') {
			SelectOutputItem item;
			if (!parse_item(select_list.substr(last, i - last), item) || item.name == "*") {
				return false;
			}
			items.push_back(std::move(item));
			last = i + 1;
		}
	}
	SelectOutputItem item;
	if (!parse_item(select_list.substr(last), item) || item.name == "*") {
		return false;
	}
	items.push_back(std::move(item));
	return true;
}

static bool ParseSelectOutputColumns(const string &original_sql, size_t select_pos, size_t from_pos,
                                     vector<string> &output_cols, vector<string> *output_exprs = nullptr) {
	vector<SelectOutputItem> items;
	if (!ParseSelectOutputItems(original_sql, select_pos, from_pos, items)) {
		return false;
	}
	for (auto &item : items) {
		output_cols.push_back(item.name);
		if (output_exprs != nullptr) {
			output_exprs->push_back(item.expr);
		}
	}
	return true;
}

static bool ParseSumCall(const string &expr, string &arg) {
	string trimmed = expr;
	StringUtil::Trim(trimmed);
	string lower = StringUtil::Lower(trimmed);
	if (lower.rfind("sum", 0) != 0) {
		return false;
	}
	size_t open = lower.find('(');
	if (open == string::npos) {
		return false;
	}
	size_t close = FindMatchingParen(trimmed, open);
	if (close == string::npos) {
		return false;
	}
	string tail = trimmed.substr(close + 1);
	StringUtil::Trim(tail);
	if (!tail.empty()) {
		return false;
	}
	arg = trimmed.substr(open + 1, close - open - 1);
	StringUtil::Trim(arg);
	return !arg.empty() && arg.find(',') == string::npos;
}

static bool ReadComparisonOp(const string &sql, size_t &pos, string &op) {
	while (pos < sql.size() && std::isspace(static_cast<unsigned char>(sql[pos]))) {
		pos++;
	}
	for (auto &candidate : {"<=", ">=", "<>", "!=", "=", "<", ">"}) {
		string cand(candidate);
		if (pos + cand.size() <= sql.size() && sql.compare(pos, cand.size(), cand) == 0) {
			op = cand;
			pos += cand.size();
			return true;
		}
	}
	return false;
}

static bool IsZeroSqlLiteral(string expr) {
	StringUtil::Trim(expr);
	if (expr.size() >= 2 && expr.front() == '(' && expr.back() == ')') {
		expr = expr.substr(1, expr.size() - 2);
		StringUtil::Trim(expr);
	}
	if (!expr.empty() && (expr.front() == '+' || expr.front() == '-')) {
		expr = expr.substr(1);
	}
	bool found_digit = false;
	for (char c : expr) {
		if (c == '.') {
			continue;
		}
		if (!std::isdigit(static_cast<unsigned char>(c))) {
			return false;
		}
		found_digit = true;
		if (c != '0') {
			return false;
		}
	}
	return found_digit;
}

static size_t FindClauseEnd(const string &lower, size_t from) {
	bool in_quote = false;
	bool in_identifier_quote = false;
	int depth = 0;
	for (size_t i = from; i < lower.size(); i++) {
		char c = lower[i];
		if (in_quote) {
			if (c == '\'') {
				in_quote = false;
			}
			continue;
		}
		if (in_identifier_quote) {
			if (c == '"' && i + 1 < lower.size() && lower[i + 1] == '"') {
				i++;
				continue;
			}
			if (c == '"') {
				in_identifier_quote = false;
			}
			continue;
		}
		if (c == '\'') {
			in_quote = true;
			continue;
		}
		if (c == '"') {
			in_identifier_quote = true;
			continue;
		}
		if (c == '(') {
			depth++;
			continue;
		}
		if (c == ')') {
			if (depth == 0) {
				return i;
			}
			depth--;
			continue;
		}
		if (depth == 0 &&
		    (c == ',' || StartsAnyKeywordToken(lower, i, {"group", "having", "order", "limit", "union"}))) {
			return i;
		}
	}
	return lower.size();
}

bool ExtractFilteredGroupCount(const string &original_sql, const vector<string> &output_names,
                               FilteredGroupCountExtract &out) {
	if (output_names.size() != 1) {
		return false;
	}
	string lower = StringUtil::Lower(original_sql);
	if (lower.find("count(*)") == string::npos && lower.find("count_star()") == string::npos) {
		return false;
	}

	size_t select_pos = FindKeywordToken(lower, "select", 0);
	while (select_pos != string::npos) {
		size_t from_pos = FindTopLevelKeywordToken(lower, "from", select_pos + strlen("select"));
		size_t group_pos = from_pos == string::npos ? string::npos : FindTopLevelKeywordToken(lower, "group", from_pos);
		if (from_pos != string::npos && group_pos != string::npos) {
			vector<SelectOutputItem> items;
			if (ParseSelectOutputItems(original_sql, select_pos, from_pos, items) && items.size() == 2) {
				idx_t group_idx = DConstants::INVALID_INDEX;
				idx_t sum_idx = DConstants::INVALID_INDEX;
				string sum_arg;
				for (idx_t i = 0; i < items.size(); i++) {
					string parsed_sum_arg;
					if (ParseSumCall(items[i].expr, parsed_sum_arg)) {
						sum_idx = i;
						sum_arg = parsed_sum_arg;
					} else {
						group_idx = i;
					}
				}
				if (group_idx != DConstants::INVALID_INDEX && sum_idx != DConstants::INVALID_INDEX) {
					size_t source_pos = from_pos + strlen("from");
					string source;
					if (!ReadIdentifierToken(original_sql, source_pos, source)) {
						return false;
					}
					string source_tail = original_sql.substr(source_pos, group_pos - source_pos);
					StringUtil::Trim(source_tail);
					if (source_tail.find(',') != string::npos ||
					    FindKeywordToken(StringUtil::Lower(source_tail), "join", 0) != string::npos) {
						return false;
					}

					size_t group_by_pos = group_pos + strlen("group");
					while (group_by_pos < lower.size() &&
					       std::isspace(static_cast<unsigned char>(lower[group_by_pos]))) {
						group_by_pos++;
					}
					if (!StartsKeywordToken(lower, group_by_pos, "by")) {
						return false;
					}
					group_by_pos += strlen("by");
					size_t group_end = FindClauseEnd(lower, group_by_pos);
					string group_by_expr = original_sql.substr(group_by_pos, group_end - group_by_pos);
					StringUtil::Trim(group_by_expr);
					if (group_by_expr.find(',') != string::npos) {
						return false;
					}
					string group_col = SqlUtils::LastIdentifierPart(items[group_idx].expr);
					if (!StringUtil::CIEquals(SqlUtils::LastIdentifierPart(group_by_expr), group_col)) {
						return false;
					}

					out.source = source;
					out.group_col = group_col;
					out.sum_col = SqlUtils::LastIdentifierPart(sum_arg);
					out.sum_alias = items[sum_idx].name;
					break;
				}
			}
		}
		select_pos = FindKeywordToken(lower, "select", select_pos + strlen("select"));
	}
	if (out.source.empty() || out.group_col.empty() || out.sum_col.empty() || out.sum_alias.empty()) {
		return false;
	}

	size_t where_pos = FindKeywordToken(lower, "where", 0);
	while (where_pos != string::npos) {
		size_t pos = where_pos + strlen("where");
		string lhs;
		if (ReadIdentifierToken(original_sql, pos, lhs) &&
		    StringUtil::CIEquals(SqlUtils::LastIdentifierPart(lhs), out.sum_alias) &&
		    ReadComparisonOp(original_sql, pos, out.comparison_op)) {
			size_t threshold_end = FindClauseEnd(lower, pos);
			out.threshold_sql = original_sql.substr(pos, threshold_end - pos);
			StringUtil::Trim(out.threshold_sql);
			break;
		}
		where_pos = FindKeywordToken(lower, "where", where_pos + strlen("where"));
	}
	if (out.comparison_op.empty() || out.threshold_sql.empty()) {
		return false;
	}
	if ((out.comparison_op != "<" && out.comparison_op != ">") || !IsZeroSqlLiteral(out.threshold_sql)) {
		return false;
	}
	out.output_col = output_names[0];
	return !out.output_col.empty();
}

static string TrimAndConjunctions(string expr) {
	StringUtil::Trim(expr);
	string lower = StringUtil::Lower(expr);
	if (lower.rfind("and ", 0) == 0) {
		expr = expr.substr(strlen("and "));
		StringUtil::Trim(expr);
		lower = StringUtil::Lower(expr);
	}
	static const string and_suffix = " and";
	if (lower.size() >= and_suffix.size() &&
	    lower.compare(lower.size() - and_suffix.size(), and_suffix.size(), and_suffix) == 0) {
		expr = expr.substr(0, expr.size() - and_suffix.size());
		StringUtil::Trim(expr);
	}
	return expr;
}

static bool ExtractSemiAntiJoin(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	size_t semi_pos = FindKeywordToken(lower, "semi join", 0);
	size_t anti_pos = FindKeywordToken(lower, "anti join", 0);
	if (semi_pos != string::npos && anti_pos != string::npos) {
		return false;
	}
	bool is_semi = semi_pos != string::npos;
	size_t join_pos = is_semi ? semi_pos : anti_pos;
	if (join_pos == string::npos) {
		return false;
	}
	out.join_type = is_semi ? "semi" : "anti";

	size_t from_pos = FindKeywordToken(lower, "from", 0);
	if (from_pos == string::npos || from_pos > join_pos) {
		return false;
	}
	size_t select_pos = FindKeywordToken(lower, "select", 0);
	if (select_pos == string::npos || select_pos > from_pos) {
		return false;
	}

	vector<string> output_cols;
	vector<string> output_exprs;
	if (!ParseSelectOutputColumns(original_sql, select_pos, from_pos, output_cols, &output_exprs)) {
		return false;
	}

	size_t pos = from_pos + strlen("from");
	if (!ReadIdentifierToken(original_sql, pos, out.left_table)) {
		return false;
	}
	size_t alias_pos = pos;
	string maybe_alias;
	if (ReadIdentifierToken(original_sql, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "semi" && maybe_lower != "anti") {
			out.left_alias = maybe_alias;
			pos = alias_pos;
		}
	}
	if (out.left_alias.empty()) {
		out.left_alias = SqlUtils::LastIdentifierPart(out.left_table);
	}
	string between_left_and_join = original_sql.substr(pos, join_pos - pos);
	string between_left_and_join_lower = StringUtil::Lower(between_left_and_join);
	if (FindKeywordToken(between_left_and_join_lower, "join", 0) != string::npos ||
	    between_left_and_join_lower.find(',') != string::npos) {
		return false;
	}

	size_t right_pos = join_pos + (is_semi ? strlen("semi join") : strlen("anti join"));
	if (!ReadIdentifierToken(original_sql, right_pos, out.right_table)) {
		return false;
	}
	alias_pos = right_pos;
	maybe_alias.clear();
	if (ReadIdentifierToken(original_sql, alias_pos, maybe_alias)) {
		if (StringUtil::Lower(maybe_alias) != "on") {
			out.right_alias = maybe_alias;
			right_pos = alias_pos;
		}
	}
	if (out.right_alias.empty()) {
		out.right_alias = SqlUtils::LastIdentifierPart(out.right_table);
	}

	size_t on_pos = FindKeywordToken(lower, "on", right_pos);
	if (on_pos == string::npos) {
		return false;
	}
	size_t pred_start = on_pos + strlen("on");
	size_t pred_end = original_sql.size();
	size_t where_pos = string::npos;
	for (auto kw : {"where", "group", "order", "limit", "union"}) {
		size_t kw_pos = FindKeywordToken(lower, kw, pred_start);
		if (kw_pos != string::npos) {
			if (string(kw) == "where") {
				where_pos = kw_pos;
			}
			pred_end = std::min(pred_end, kw_pos);
		}
	}
	out.predicate = original_sql.substr(pred_start, pred_end - pred_start);
	StringUtil::Trim(out.predicate);
	if (out.predicate.empty()) {
		return false;
	}
	if (where_pos != string::npos) {
		size_t filter_start = where_pos + strlen("where");
		size_t filter_end = original_sql.size();
		for (auto kw : {"group", "order", "limit", "union"}) {
			size_t kw_pos = FindKeywordToken(lower, kw, filter_start);
			if (kw_pos != string::npos) {
				filter_end = std::min(filter_end, kw_pos);
			}
		}
		out.post_filter = original_sql.substr(filter_start, filter_end - filter_start);
		StringUtil::Trim(out.post_filter);
	}
	out.output_cols = std::move(output_cols);
	out.output_exprs = std::move(output_exprs);
	return true;
}

static bool ExtractExistsSubquery(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	size_t select_pos = FindKeywordToken(lower, "select", 0);
	size_t from_pos = FindTopLevelKeywordToken(lower, "from", select_pos == string::npos ? 0 : select_pos);
	if (select_pos == string::npos || from_pos == string::npos || select_pos > from_pos) {
		return false;
	}

	vector<string> output_cols;
	vector<string> output_exprs;
	if (!ParseSelectOutputColumns(original_sql, select_pos, from_pos, output_cols, &output_exprs)) {
		return false;
	}

	size_t pos = from_pos + strlen("from");
	if (!ReadIdentifierToken(original_sql, pos, out.left_table)) {
		return false;
	}
	size_t alias_pos = pos;
	string maybe_alias;
	if (ReadIdentifierToken(original_sql, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "where") {
			out.left_alias = maybe_alias;
			pos = alias_pos;
		}
	}
	if (out.left_alias.empty()) {
		out.left_alias = SqlUtils::LastIdentifierPart(out.left_table);
	}

	size_t where_pos = FindTopLevelKeywordToken(lower, "where", pos);
	if (where_pos == string::npos) {
		return false;
	}
	string between_left_and_where = StringUtil::Lower(original_sql.substr(pos, where_pos - pos));
	if (FindKeywordToken(between_left_and_where, "join", 0) != string::npos ||
	    between_left_and_where.find(',') != string::npos) {
		return false;
	}

	size_t not_exists_pos = FindTopLevelKeywordToken(lower, "not exists", where_pos);
	size_t exists_pos = FindTopLevelKeywordToken(lower, "exists", where_pos);
	bool is_anti = not_exists_pos != string::npos;
	size_t exists_kw_pos = is_anti ? not_exists_pos : exists_pos;
	if (exists_kw_pos == string::npos || (is_anti && exists_pos != string::npos && exists_pos < not_exists_pos)) {
		return false;
	}
	out.join_type = is_anti ? "anti" : "semi";

	size_t after_exists = exists_kw_pos + (is_anti ? strlen("not exists") : strlen("exists"));
	while (after_exists < original_sql.size() && std::isspace(static_cast<unsigned char>(original_sql[after_exists]))) {
		after_exists++;
	}
	size_t close_pos = FindMatchingParen(original_sql, after_exists);
	if (close_pos == string::npos) {
		return false;
	}

	string outer_filter =
	    original_sql.substr(where_pos + strlen("where"), exists_kw_pos - (where_pos + strlen("where")));
	outer_filter += original_sql.substr(close_pos + 1);
	out.post_filter = TrimAndConjunctions(outer_filter);

	string subquery = original_sql.substr(after_exists + 1, close_pos - after_exists - 1);
	string sub_lower = StringUtil::Lower(subquery);
	size_t sub_select = FindKeywordToken(sub_lower, "select", 0);
	size_t sub_from = FindTopLevelKeywordToken(sub_lower, "from", sub_select == string::npos ? 0 : sub_select);
	if (sub_select == string::npos || sub_from == string::npos || sub_select > sub_from) {
		return false;
	}
	size_t right_pos = sub_from + strlen("from");
	if (!ReadIdentifierToken(subquery, right_pos, out.right_table)) {
		return false;
	}
	alias_pos = right_pos;
	maybe_alias.clear();
	if (ReadIdentifierToken(subquery, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "where" && maybe_lower != "group" && maybe_lower != "order" && maybe_lower != "limit") {
			out.right_alias = maybe_alias;
			right_pos = alias_pos;
		}
	}
	if (out.right_alias.empty()) {
		out.right_alias = SqlUtils::LastIdentifierPart(out.right_table);
	}

	size_t sub_where = FindTopLevelKeywordToken(sub_lower, "where", right_pos);
	string between_right_and_filter =
	    sub_lower.substr(right_pos, (sub_where == string::npos ? sub_lower.size() : sub_where) - right_pos);
	if (FindKeywordToken(between_right_and_filter, "join", 0) != string::npos ||
	    between_right_and_filter.find(',') != string::npos) {
		return false;
	}
	if (sub_where == string::npos) {
		out.predicate = "true";
	} else {
		size_t pred_start = sub_where + strlen("where");
		size_t pred_end = subquery.size();
		for (auto kw : {"group", "order", "limit", "union"}) {
			size_t kw_pos = FindTopLevelKeywordToken(sub_lower, kw, pred_start);
			if (kw_pos != string::npos) {
				pred_end = std::min(pred_end, kw_pos);
			}
		}
		out.predicate = subquery.substr(pred_start, pred_end - pred_start);
		StringUtil::Trim(out.predicate);
	}
	if (out.predicate.empty()) {
		return false;
	}
	out.output_cols = std::move(output_cols);
	out.output_exprs = std::move(output_exprs);
	return true;
}

static bool ExtractInSubquery(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	size_t select_pos = FindKeywordToken(lower, "select", 0);
	size_t from_pos = FindTopLevelKeywordToken(lower, "from", select_pos == string::npos ? 0 : select_pos);
	if (select_pos == string::npos || from_pos == string::npos || select_pos > from_pos) {
		return false;
	}

	vector<string> output_cols;
	vector<string> output_exprs;
	if (!ParseSelectOutputColumns(original_sql, select_pos, from_pos, output_cols, &output_exprs)) {
		return false;
	}

	size_t where_pos = FindTopLevelKeywordToken(lower, "where", from_pos + strlen("from"));
	if (where_pos == string::npos) {
		return false;
	}
	string left_from = original_sql.substr(from_pos + strlen("from"), where_pos - (from_pos + strlen("from")));
	StringUtil::Trim(left_from);
	if (left_from.empty()) {
		return false;
	}
	string left_table_expr = left_from;
	string left_alias_expr = "openivm_left";
	bool simple_left_table = false;
	size_t left_pos = 0;
	string left_ident;
	if (ReadIdentifierToken(left_from, left_pos, left_ident)) {
		const string &candidate_table = left_ident;
		string candidate_alias = SqlUtils::LastIdentifierPart(left_ident);
		size_t left_alias_pos = left_pos;
		string maybe_left_alias;
		if (ReadIdentifierToken(left_from, left_alias_pos, maybe_left_alias)) {
			candidate_alias = maybe_left_alias;
			left_pos = left_alias_pos;
		}
		size_t tail_pos = left_pos;
		while (tail_pos < left_from.size() && std::isspace(static_cast<unsigned char>(left_from[tail_pos]))) {
			tail_pos++;
		}
		if (tail_pos == left_from.size()) {
			left_table_expr = candidate_table;
			left_alias_expr = candidate_alias;
			simple_left_table = true;
		}
	}
	if (!simple_left_table) {
		string select_list =
		    original_sql.substr(select_pos + strlen("select"), from_pos - (select_pos + strlen("select")));
		StringUtil::Trim(select_list);
		left_table_expr = "(SELECT " + select_list + " FROM " + left_from + ")";
		left_alias_expr = "openivm_left";
	}

	size_t not_in_pos = FindTopLevelKeywordToken(lower, "not in", where_pos);
	size_t in_pos = FindTopLevelKeywordToken(lower, "in", where_pos);
	bool is_anti = not_in_pos != string::npos;
	size_t in_kw_pos = is_anti ? not_in_pos : in_pos;
	if (in_kw_pos == string::npos || (is_anti && in_pos != string::npos && in_pos < not_in_pos)) {
		return false;
	}

	string lhs = original_sql.substr(where_pos + strlen("where"), in_kw_pos - (where_pos + strlen("where")));
	lhs = TrimAndConjunctions(lhs);
	if (lhs.empty()) {
		return false;
	}

	size_t after_in = in_kw_pos + (is_anti ? strlen("not in") : strlen("in"));
	while (after_in < original_sql.size() && std::isspace(static_cast<unsigned char>(original_sql[after_in]))) {
		after_in++;
	}
	size_t close_pos = FindMatchingParen(original_sql, after_in);
	if (close_pos == string::npos) {
		return false;
	}
	string trailing_filter = original_sql.substr(close_pos + 1);
	out.post_filter = TrimAndConjunctions(trailing_filter);

	string subquery = original_sql.substr(after_in + 1, close_pos - after_in - 1);
	string sub_lower = StringUtil::Lower(subquery);
	if (FindTopLevelKeywordToken(sub_lower, "union", 0) != string::npos) {
		return false;
	}
	size_t sub_select = FindKeywordToken(sub_lower, "select", 0);
	size_t sub_from = FindTopLevelKeywordToken(sub_lower, "from", sub_select == string::npos ? 0 : sub_select);
	if (sub_select == string::npos || sub_from == string::npos || sub_select > sub_from) {
		return false;
	}
	string rhs_expr = subquery.substr(sub_select + strlen("select"), sub_from - (sub_select + strlen("select")));
	StringUtil::Trim(rhs_expr);
	string rhs_lower = StringUtil::Lower(rhs_expr);
	if (rhs_lower.rfind("distinct ", 0) == 0) {
		rhs_expr = rhs_expr.substr(strlen("distinct "));
		StringUtil::Trim(rhs_expr);
	}
	if (rhs_expr.empty() || rhs_expr.find(',') != string::npos) {
		return false;
	}

	size_t right_pos = sub_from + strlen("from");
	if (!ReadIdentifierToken(subquery, right_pos, out.right_table)) {
		return false;
	}
	size_t alias_pos = right_pos;
	string maybe_alias;
	if (ReadIdentifierToken(subquery, alias_pos, maybe_alias)) {
		string maybe_lower = StringUtil::Lower(maybe_alias);
		if (maybe_lower != "where" && maybe_lower != "group" && maybe_lower != "order" && maybe_lower != "limit") {
			out.right_alias = maybe_alias;
			right_pos = alias_pos;
		}
	}
	if (out.right_alias.empty()) {
		out.right_alias = SqlUtils::LastIdentifierPart(out.right_table);
	}
	string original_right_alias = out.right_alias;

	size_t sub_where = FindTopLevelKeywordToken(sub_lower, "where", right_pos);
	string right_filter;
	if (sub_where != string::npos) {
		size_t pred_start = sub_where + strlen("where");
		size_t pred_end = subquery.size();
		for (auto kw : {"group", "order", "limit"}) {
			size_t kw_pos = FindTopLevelKeywordToken(sub_lower, kw, pred_start);
			if (kw_pos != string::npos) {
				pred_end = std::min(pred_end, kw_pos);
			}
		}
		right_filter = subquery.substr(pred_start, pred_end - pred_start);
		StringUtil::Trim(right_filter);
	}

	out.join_type = is_anti ? "anti" : "semi";
	out.left_table = left_table_expr;
	out.left_alias = left_alias_expr;
	out.right_alias = "openivm_right";
	if (simple_left_table) {
		out.predicate = StringUtil::Replace(lhs, out.left_alias + ".", out.left_alias + ".");
		out.predicate = StringUtil::Replace(out.predicate, SqlUtils::LastIdentifierPart(out.left_table) + ".",
		                                    out.left_alias + ".");
	} else {
		out.predicate = out.left_alias + "." + KeywordHelper::WriteOptionallyQuoted(SqlUtils::LastIdentifierPart(lhs));
	}
	out.predicate += " IS NOT DISTINCT FROM ";
	out.predicate += StringUtil::Replace(rhs_expr, original_right_alias + ".", out.right_alias + ".");
	out.predicate =
	    StringUtil::Replace(out.predicate, SqlUtils::LastIdentifierPart(out.right_table) + ".", out.right_alias + ".");
	if (!right_filter.empty()) {
		string rewritten_filter = StringUtil::Replace(right_filter, original_right_alias + ".", out.right_alias + ".");
		rewritten_filter = StringUtil::Replace(rewritten_filter, SqlUtils::LastIdentifierPart(out.right_table) + ".",
		                                       out.right_alias + ".");
		out.predicate += " AND (" + rewritten_filter + ")";
	}
	out.output_cols = std::move(output_cols);
	out.output_exprs = std::move(output_exprs);
	return true;
}

bool ExtractSemiAntiQuery(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	if (FindTopLevelKeywordToken(lower, "union", 0) != string::npos ||
	    FindTopLevelKeywordToken(lower, "intersect", 0) != string::npos ||
	    FindTopLevelKeywordToken(lower, "except", 0) != string::npos) {
		return false;
	}
	return ExtractSemiAntiJoin(original_sql, out) || ExtractExistsSubquery(original_sql, out) ||
	       ExtractInSubquery(original_sql, out);
}

} // namespace duckdb
