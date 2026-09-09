#include "upsert/refresh_compiler.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_internal.hpp"

#include <map>
#include <regex>

namespace duckdb {

namespace {

static string DeltaSourceRef(const string &source, const string &catalog_prefix) {
	if (!source.empty() && (source[0] == '(' || source.find('.') != string::npos)) {
		return source;
	}
	return catalog_prefix + SqlUtils::QuoteIdentifier(source);
}

static string TrimCopy(const string &input) {
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

static string LowerCopy(string input) {
	std::transform(input.begin(), input.end(), input.begin(),
	               [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
	return input;
}

static string StripIdentifierQuotes(string input) {
	input = TrimCopy(input);
	if (input.size() >= 2 && input.front() == '"' && input.back() == '"') {
		return input.substr(1, input.size() - 2);
	}
	return SqlUtils::LastIdentifierPart(input);
}

static string PartitionOutputColumn(const string &input) {
	auto pos = input.find('=');
	return StripIdentifierQuotes(pos == string::npos ? input : input.substr(0, pos));
}

static vector<string> PartitionOutputColumns(const vector<string> &partition_columns) {
	vector<string> output_columns;
	output_columns.reserve(partition_columns.size());
	for (auto &partition_column : partition_columns) {
		output_columns.push_back(PartitionOutputColumn(partition_column));
	}
	return output_columns;
}

static string BuildAffectedTableFilter(const vector<string> &columns, const string &outer_alias,
                                       const string &affected_table) {
	string match = SqlUtils::BuildNullSafeMatch(columns, "openivm_aff", outer_alias);
	return "EXISTS (SELECT 1 FROM " + affected_table + " openivm_aff WHERE " + match + ")";
}

static string BuildDeltaAffectedFilter(const vector<WindowPartitionDeltaSpec> &partition_delta_specs,
                                       const string &delta_where, const string &outer_alias) {
	string filter;
	for (idx_t i = 0; i < partition_delta_specs.size(); i++) {
		if (i > 0) {
			filter += " OR ";
		}
		const auto &spec = partition_delta_specs[i];
		string output_col = SqlUtils::QuoteIdentifier(spec.output_column);
		string source_col = SqlUtils::QuoteIdentifier(spec.source_column);
		string delta_table =
		    spec.delta_table_sql.empty() ? SqlUtils::QuoteIdentifier(spec.delta_table) : spec.delta_table_sql;
		string affected_keys =
		    "SELECT DISTINCT " + source_col + " AS " + output_col + " FROM " + delta_table + delta_where;
		filter += "EXISTS (SELECT 1 FROM (" + affected_keys + ") openivm_aff WHERE " + outer_alias + "." + output_col +
		          " IS NOT DISTINCT FROM openivm_aff." + output_col + ")";
	}
	return filter;
}

static vector<string> SplitTopLevelComma(const string &input) {
	vector<string> parts;
	idx_t start = 0;
	int depth = 0;
	for (idx_t i = 0; i < input.size(); i++) {
		char c = input[i];
		if (c == '(') {
			depth++;
		} else if (c == ')' && depth > 0) {
			depth--;
		} else if (c == ',' && depth == 0) {
			parts.push_back(TrimCopy(input.substr(start, i - start)));
			start = i + 1;
		}
	}
	parts.push_back(TrimCopy(input.substr(start)));
	return parts;
}

static idx_t FindTopLevelFrom(const string &input) {
	string lower = LowerCopy(input);
	int depth = 0;
	for (idx_t i = 0; i + 6 <= lower.size(); i++) {
		char c = lower[i];
		if (c == '(') {
			depth++;
		} else if (c == ')' && depth > 0) {
			depth--;
		}

		if (depth == 0 && lower.compare(i, 6, " from ") == 0) {
			return i;
		}
	}
	return string::npos;
}

static idx_t FindTopLevelKeyword(const string &input, const string &keyword, idx_t start = 0) {
	string lower = LowerCopy(input);
	string needle = " " + keyword + " ";
	int depth = 0;
	for (idx_t i = start; i + needle.size() <= lower.size(); i++) {
		char c = lower[i];
		if (c == '(') {
			depth++;
		} else if (c == ')' && depth > 0) {
			depth--;
		}
		if (depth == 0 && lower.compare(i, needle.size(), needle) == 0) {
			return i;
		}
	}
	return string::npos;
}

struct RunningWindowExpr {
	string function_name;
	string argument;
	string output_column;
};

struct RunningDerivedExpr {
	string output_column;
	string expression;
};

struct RunningWindowPlan {
	string source_table;
	string partition_column;
	string order_column;
	vector<string> output_columns;
	vector<pair<string, string>> passthrough_columns;
	vector<RunningWindowExpr> window_exprs;
	vector<RunningDerivedExpr> derived_exprs;
};

static bool ParsePassthroughProjection(const string &item, pair<string, string> &out) {
	static const std::regex alias_regex(R"(^\s*(.+?)\s+as\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*$)",
	                                    std::regex_constants::icase);
	std::smatch match;
	string expr = item;
	string output;
	if (std::regex_match(item, match, alias_regex)) {
		expr = TrimCopy(match[1].str());
		output = StripIdentifierQuotes(match[2].str());
	} else {
		output = StripIdentifierQuotes(expr);
	}
	string source = StripIdentifierQuotes(expr);
	if (source.empty() || source.find(' ') != string::npos || source.find('(') != string::npos) {
		return false;
	}
	out = std::make_pair(source, output);
	return true;
}

static bool ParseWindowSpec(const string &spec_input, string &partition_col, string &order_col) {
	string spec = TrimCopy(spec_input);
	string spec_lower = LowerCopy(spec);
	auto part_pos = spec_lower.find("partition by ");
	auto order_pos = spec_lower.find(" order by ");
	if (part_pos == string::npos || order_pos == string::npos || order_pos <= part_pos) {
		return false;
	}
	string part_expr = TrimCopy(spec.substr(part_pos + 13, order_pos - (part_pos + 13)));
	string order_expr = TrimCopy(spec.substr(order_pos + 10));
	string order_lower = LowerCopy(order_expr);
	auto frame_pos = order_lower.find(" rows ");
	if (frame_pos == string::npos) {
		frame_pos = order_lower.find(" range ");
	}
	if (frame_pos != string::npos) {
		string frame = order_lower.substr(frame_pos);
		if (frame.find("unbounded preceding") == string::npos || frame.find("current row") == string::npos) {
			return false;
		}
		order_expr = TrimCopy(order_expr.substr(0, frame_pos));
	}
	auto nulls_pos = LowerCopy(order_expr).find(" nulls ");
	if (nulls_pos != string::npos) {
		order_expr = TrimCopy(order_expr.substr(0, nulls_pos));
	}
	auto order_space = order_expr.find(' ');
	if (order_space != string::npos) {
		string suffix = LowerCopy(TrimCopy(order_expr.substr(order_space + 1)));
		if (suffix != "asc") {
			return false;
		}
		order_expr = TrimCopy(order_expr.substr(0, order_space));
	}
	string parsed_part = StripIdentifierQuotes(part_expr);
	string parsed_order = StripIdentifierQuotes(order_expr);
	if (parsed_part.empty() || parsed_order.empty() || part_expr.find(',') != string::npos ||
	    order_expr.find(',') != string::npos) {
		return false;
	}
	if (!partition_col.empty() && !StringUtil::CIEquals(partition_col, parsed_part)) {
		return false;
	}
	if (!order_col.empty() && !StringUtil::CIEquals(order_col, parsed_order)) {
		return false;
	}
	partition_col = parsed_part;
	order_col = parsed_order;
	return true;
}

static bool ParseNamedWindows(const string &tail, std::map<string, string> &named_windows) {
	for (auto &item : SplitTopLevelComma(tail)) {
		static const std::regex named_regex(R"(^\s*("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s+as\s*\((.*)\)\s*$)",
		                                    std::regex_constants::icase);
		std::smatch match;
		if (!std::regex_match(item, match, named_regex)) {
			return false;
		}
		named_windows[StringUtil::Lower(StripIdentifierQuotes(match[1].str()))] = TrimCopy(match[2].str());
	}
	return !named_windows.empty();
}

static bool ParseRunningWindowProjection(const string &item, const std::map<string, string> &named_windows,
                                         RunningWindowExpr &out, string &partition_col, string &order_col) {
	static const std::regex alias_regex(R"(^\s*(.+?)\s+as\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*$)",
	                                    std::regex_constants::icase);
	std::smatch alias_match;
	if (!std::regex_match(item, alias_match, alias_regex)) {
		return false;
	}
	string expr = TrimCopy(alias_match[1].str());
	out.output_column = StripIdentifierQuotes(alias_match[2].str());
	static const std::regex window_regex(R"(^\s*(sum|min|max|count|avg)\s*\(\s*([^)]+?)\s*\)\s+over\s*\((.*)\)\s*$)",
	                                     std::regex_constants::icase);
	static const std::regex named_window_regex(
	    R"(^\s*(sum|min|max|count|avg)\s*\(\s*([^)]+?)\s*\)\s+over\s+("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)\s*$)",
	    std::regex_constants::icase);
	std::smatch window_match;
	string spec;
	if (!std::regex_match(expr, window_match, window_regex)) {
		if (!std::regex_match(expr, window_match, named_window_regex)) {
			return false;
		}
		auto found = named_windows.find(StringUtil::Lower(StripIdentifierQuotes(window_match[3].str())));
		if (found == named_windows.end()) {
			return false;
		}
		spec = found->second;
	} else {
		spec = TrimCopy(window_match[3].str());
	}
	if (!ParseWindowSpec(spec, partition_col, order_col)) {
		return false;
	}
	out.function_name = LowerCopy(window_match[1].str());
	out.argument = StripIdentifierQuotes(window_match[2].str());
	return true;
}

static bool ParseRunningWindowExpression(const string &expr, RunningWindowExpr &out, string &partition_col,
                                         string &order_col) {
	static const std::regex window_regex(R"(^\s*(sum|min|max|count|avg)\s*\(\s*([^)]+?)\s*\)\s+over\s*\((.*)\)\s*$)",
	                                     std::regex_constants::icase);
	std::smatch window_match;
	if (!std::regex_match(expr, window_match, window_regex)) {
		return false;
	}
	if (!ParseWindowSpec(TrimCopy(window_match[3].str()), partition_col, order_col)) {
		return false;
	}
	out.function_name = LowerCopy(window_match[1].str());
	out.argument = StripIdentifierQuotes(window_match[2].str());
	return true;
}

static string TranslateExpressionIdentifiers(const string &expr, const std::map<string, string> &alias_to_output) {
	static const std::regex ident_regex(R"("[^"]+"|[A-Za-z_][A-Za-z0-9_]*)");
	string result;
	idx_t pos = 0;
	for (std::sregex_iterator it(expr.begin(), expr.end(), ident_regex), end; it != end; ++it) {
		const auto &match = *it;
		result += expr.substr(pos, match.position() - pos);
		string token = match.str();
		string key = StringUtil::Lower(StripIdentifierQuotes(token));
		auto found = alias_to_output.find(key);
		result += found == alias_to_output.end() ? token : SqlUtils::QuoteIdentifier(found->second);
		pos = match.position() + match.length();
	}
	result += expr.substr(pos);
	return result;
}

static bool LooksLikeLptsAlias(const string &expr) {
	static const std::regex lpts_alias_regex(R"(\bt[0-9]+_[A-Za-z0-9_]+\b)");
	return std::regex_search(expr, lpts_alias_regex);
}

static bool TryParseRunningWindowPlan(const string &view_query_sql, const vector<string> &partition_columns,
                                      const vector<string> &column_names, RunningWindowPlan &plan) {
	string query = TrimCopy(view_query_sql);
	if (!StringUtil::StartsWith(LowerCopy(query), "select ")) {
		return false;
	}
	auto from_pos = FindTopLevelFrom(query);
	if (from_pos == string::npos) {
		return false;
	}
	string select_list = query.substr(7, from_pos - 7);
	string from_tail = TrimCopy(query.substr(from_pos + 6));
	std::map<string, string> named_windows;
	auto window_pos = FindTopLevelKeyword(" " + from_tail, "window");
	if (window_pos != string::npos) {
		string source_tail = TrimCopy(from_tail.substr(0, window_pos - 1));
		string window_tail = TrimCopy(from_tail.substr(window_pos + 7));
		if (!ParseNamedWindows(window_tail, named_windows)) {
			return false;
		}
		from_tail = source_tail;
	}
	if (from_tail.empty() || from_tail.find(' ') != string::npos || from_tail.find(',') != string::npos) {
		return false;
	}
	plan.source_table = StripIdentifierQuotes(from_tail);
	auto parsed_partition = partition_columns.empty() ? "" : PartitionOutputColumn(partition_columns[0]);
	string parsed_order;
	for (auto &item : SplitTopLevelComma(select_list)) {
		if (LowerCopy(item).find(" over ") != string::npos) {
			RunningWindowExpr expr;
			if (!ParseRunningWindowProjection(item, named_windows, expr, parsed_partition, parsed_order)) {
				return false;
			}
			plan.window_exprs.push_back(expr);
		} else {
			pair<string, string> pass;
			if (!ParsePassthroughProjection(item, pass)) {
				return false;
			}
			plan.passthrough_columns.push_back(pass);
		}
	}
	if (plan.window_exprs.empty() || parsed_partition.empty() || parsed_order.empty() ||
	    partition_columns.size() != 1) {
		return false;
	}
	plan.partition_column = parsed_partition;
	plan.order_column = parsed_order;
	for (auto &col : column_names) {
		bool found = false;
		for (auto &pass : plan.passthrough_columns) {
			found = found || StringUtil::CIEquals(col, pass.second);
		}
		for (auto &expr : plan.window_exprs) {
			found = found || StringUtil::CIEquals(col, expr.output_column);
		}
		if (!found) {
			return false;
		}
	}
	plan.output_columns = column_names;
	return true;
}

// Normalize an LPTS-emitted CTE program into the compact, single-space token stream that the
// structural navigation in TryParseLptsRunningWindowPlan expects. The LPTS refactor (cwida lpts
// Release 1.0.0) switched CTE bodies to a multi-line, aligned pretty-print (e.g. "AS (\n    SELECT
// ...\n    FROM  ...\n)") and renamed CTEs from "scan_0"/"projection_1" to "t0_scan"/"t1_projection".
// Collapsing whitespace and tightening parentheses restores "AS (SELECT ... source)" so the same
// find()/rfind() navigation continues to work regardless of the layout. The window/order/partition
// expressions themselves are re-parsed with whitespace-tolerant regexes, so no information is lost.
static string NormalizeLptsRunningWindowSql(const string &sql) {
	string collapsed;
	collapsed.reserve(sql.size());
	bool prev_space = false;
	for (unsigned char c : sql) {
		if (std::isspace(c)) {
			if (!prev_space) {
				collapsed += ' ';
				prev_space = true;
			}
		} else {
			collapsed += static_cast<char>(c);
			prev_space = false;
		}
	}
	auto replace_all = [](string &s, const string &from, const string &to) {
		size_t pos = 0;
		while ((pos = s.find(from, pos)) != string::npos) {
			s.replace(pos, from.size(), to);
			pos += to.size();
		}
	};
	replace_all(collapsed, "( ", "(");
	replace_all(collapsed, " )", ")");
	return collapsed;
}

static bool TryParseLptsRunningWindowPlan(const string &raw_view_query_sql, const vector<string> &partition_columns,
                                          const vector<string> &column_names, RunningWindowPlan &plan) {
	string view_query_sql = NormalizeLptsRunningWindowSql(raw_view_query_sql);
	string lower = LowerCopy(view_query_sql);
	// The first CTE in a running-window LPTS program is the base table scan. Locate it via the
	// leading WITH rather than the CTE name, which the refactor changed from "scan_0" to "t0_scan".
	auto scan_pos = lower.find("with");
	if (scan_pos == string::npos) {
		return false;
	}
	scan_pos += 4;
	auto alias_start = view_query_sql.find('(', scan_pos);
	if (alias_start == string::npos) {
		return false;
	}
	auto alias_end = view_query_sql.find(')', alias_start + 1);
	if (alias_end == string::npos) {
		return false;
	}
	auto select_pos = lower.find(" as (select ", alias_end);
	if (select_pos == string::npos) {
		return false;
	}
	select_pos += 12;
	auto from_pos = lower.find(" from ", select_pos);
	if (from_pos == string::npos) {
		return false;
	}
	auto source_end = view_query_sql.find(')', from_pos + 6);
	if (source_end == string::npos) {
		return false;
	}
	auto scan_aliases = SplitTopLevelComma(view_query_sql.substr(alias_start + 1, alias_end - alias_start - 1));
	auto source_cols = SplitTopLevelComma(view_query_sql.substr(select_pos, from_pos - select_pos));
	if (scan_aliases.size() != source_cols.size()) {
		return false;
	}
	std::map<string, string> alias_to_source;
	std::map<string, string> alias_to_output;
	for (idx_t i = 0; i < scan_aliases.size(); i++) {
		string alias = StringUtil::Lower(StripIdentifierQuotes(scan_aliases[i]));
		string source = StripIdentifierQuotes(source_cols[i]);
		alias_to_source[alias] = source;
		alias_to_output[alias] = source;
	}
	plan.source_table = StripIdentifierQuotes(view_query_sql.substr(from_pos + 6, source_end - from_pos - 6));
	vector<string> output_names = column_names;
	auto final_select = lower.rfind("\nselect ");
	if (final_select == string::npos) {
		final_select = lower.rfind(" select ");
	}
	if (final_select != string::npos) {
		auto final_from = lower.find(" from ", final_select + 8);
		if (final_from != string::npos) {
			vector<string> parsed_outputs;
			for (auto &item :
			     SplitTopLevelComma(view_query_sql.substr(final_select + 8, final_from - final_select - 8))) {
				pair<string, string> pass;
				if (!ParsePassthroughProjection(item, pass)) {
					parsed_outputs.clear();
					break;
				}
				parsed_outputs.push_back(pass.second);
			}
			if (!parsed_outputs.empty()) {
				output_names = parsed_outputs;
			}
		}
	}
	vector<string> non_base_outputs;
	for (auto &col : output_names) {
		bool is_base = false;
		for (auto &entry : alias_to_source) {
			if (StringUtil::CIEquals(col, entry.second)) {
				plan.passthrough_columns.push_back(std::make_pair(entry.second, col));
				is_base = true;
				break;
			}
		}
		if (!is_base) {
			non_base_outputs.push_back(col);
		}
	}
	plan.output_columns = output_names;
	if (non_base_outputs.empty()) {
		return false;
	}
	idx_t window_idx = 0;
	string expected_partition = partition_columns.empty() ? "" : PartitionOutputColumn(partition_columns[0]);
	string parsed_partition;
	string parsed_order;
	idx_t cte_search = 0;
	while (true) {
		auto cte_as = lower.find(" as (select ", cte_search);
		if (cte_as == string::npos) {
			break;
		}
		auto cte_alias_start = view_query_sql.rfind('(', cte_as);
		auto cte_alias_end = view_query_sql.rfind(')', cte_as);
		if (cte_alias_start == string::npos || cte_alias_end == string::npos || cte_alias_end < cte_alias_start) {
			return false;
		}
		auto cte_aliases =
		    SplitTopLevelComma(view_query_sql.substr(cte_alias_start + 1, cte_alias_end - cte_alias_start - 1));
		auto cte_select = cte_as + 12;
		auto cte_from = lower.find(" from ", cte_select);
		if (cte_from == string::npos) {
			break;
		}
		auto cte_items = SplitTopLevelComma(view_query_sql.substr(cte_select, cte_from - cte_select));
		if (cte_aliases.size() != cte_items.size()) {
			return false;
		}
		std::map<string, string> next_alias_to_output;
		for (idx_t item_idx = 0; item_idx < cte_items.size(); item_idx++) {
			auto &item = cte_items[item_idx];
			string output_alias = StripIdentifierQuotes(cte_aliases[item_idx]);
			string output_key = StringUtil::Lower(output_alias);
			if (LowerCopy(item).find(" over ") == string::npos) {
				string source_key = StringUtil::Lower(StripIdentifierQuotes(item));
				auto passthrough = alias_to_output.find(source_key);
				if (passthrough != alias_to_output.end()) {
					next_alias_to_output[output_key] = passthrough->second;
					continue;
				}
				auto scan_passthrough = alias_to_source.find(output_key);
				if (scan_passthrough != alias_to_source.end()) {
					next_alias_to_output[output_key] = scan_passthrough->second;
					continue;
				}
				string item_lower = LowerCopy(TrimCopy(item));
				if (StringUtil::StartsWith(item_lower, "case ")) {
					RunningDerivedExpr derived;
					derived.output_column = output_alias;
					derived.expression = TranslateExpressionIdentifiers(item, alias_to_output);
					if (LooksLikeLptsAlias(derived.expression)) {
						return false;
					}
					plan.derived_exprs.push_back(derived);
					next_alias_to_output[output_key] = derived.output_column;
					continue;
				}
				continue;
			}
			if (window_idx >= non_base_outputs.size()) {
				return false;
			}
			RunningWindowExpr expr;
			string expr_partition;
			string expr_order;
			if (!ParseRunningWindowExpression(item, expr, expr_partition, expr_order)) {
				return false;
			}
			auto translate = [&](const string &alias) -> string {
				auto found = alias_to_output.find(StringUtil::Lower(StripIdentifierQuotes(alias)));
				return found == alias_to_output.end() ? "" : found->second;
			};
			if (expr.argument != "*") {
				expr.argument = translate(expr.argument);
			}
			expr_partition = translate(expr_partition);
			expr_order = translate(expr_order);
			if (expr.argument.empty() || expr_partition.empty() || expr_order.empty()) {
				return false;
			}
			if (!expected_partition.empty() && !StringUtil::CIEquals(expected_partition, expr_partition)) {
				return false;
			}
			if (!parsed_partition.empty() && !StringUtil::CIEquals(parsed_partition, expr_partition)) {
				return false;
			}
			if (!parsed_order.empty() && !StringUtil::CIEquals(parsed_order, expr_order)) {
				return false;
			}
			parsed_partition = expr_partition;
			parsed_order = expr_order;
			expr.output_column = non_base_outputs[window_idx++];
			plan.window_exprs.push_back(expr);
			next_alias_to_output[output_key] = expr.output_column;
		}
		alias_to_output = next_alias_to_output;
		cte_search = cte_from + 6;
	}
	plan.partition_column = parsed_partition;
	plan.order_column = parsed_order;
	return !plan.window_exprs.empty() && window_idx == non_base_outputs.size() && partition_columns.size() == 1;
}

static string QualifiedColumn(const string &alias, const string &column) {
	return alias + "." + SqlUtils::QuoteIdentifier(column);
}

static string RunningLocalExpr(const RunningWindowExpr &expr, const RunningWindowPlan &plan) {
	string arg = expr.argument == "*" ? "*" : QualifiedColumn("d", expr.argument);
	string over = " OVER (PARTITION BY " + QualifiedColumn("d", plan.partition_column) + " ORDER BY " +
	              QualifiedColumn("d", plan.order_column) + ")";
	return StringUtil::Upper(expr.function_name) + "(" + arg + ")" + over;
}

static bool IsRunningDerivedArgument(const RunningWindowExpr &expr, const RunningWindowPlan &plan) {
	for (auto &derived : plan.derived_exprs) {
		if (StringUtil::CIEquals(expr.argument, derived.output_column)) {
			return true;
		}
	}
	return false;
}

static string RunningSeedColumn(const RunningWindowExpr &expr) {
	return "openivm_seed_" + expr.output_column;
}

static string RunningLocalExprFromAlias(const RunningWindowExpr &expr, const RunningWindowPlan &plan,
                                        const string &alias) {
	string arg = expr.argument == "*" ? "*" : QualifiedColumn(alias, expr.argument);
	string over = " OVER (PARTITION BY " + QualifiedColumn(alias, plan.partition_column) + " ORDER BY " +
	              QualifiedColumn(alias, plan.order_column) + ")";
	return StringUtil::Upper(expr.function_name) + "(" + arg + ")" + over;
}

static string RunningAvgPriorCountColumn(const RunningWindowExpr &expr) {
	return "openivm_prior_count_" + expr.output_column;
}

static string RunningAdjustedExpr(const RunningWindowExpr &expr, const RunningWindowPlan &plan) {
	string local = RunningLocalExpr(expr, plan);
	string state_col = QualifiedColumn("s", expr.output_column);
	if (expr.function_name == "sum") {
		return "CASE WHEN " + state_col + " IS NULL THEN " + local + " ELSE " + state_col + " + " + local + " END";
	}
	if (expr.function_name == "count") {
		return "COALESCE(" + state_col + ", 0) + " + local;
	}
	if (expr.function_name == "min") {
		return "CASE WHEN " + state_col + " IS NULL THEN " + local + " WHEN " + local + " IS NULL THEN " + state_col +
		       " ELSE LEAST(" + state_col + ", " + local + ") END";
	}
	if (expr.function_name == "max") {
		return "CASE WHEN " + state_col + " IS NULL THEN " + local + " WHEN " + local + " IS NULL THEN " + state_col +
		       " ELSE GREATEST(" + state_col + ", " + local + ") END";
	}
	if (expr.function_name == "avg" && expr.argument != "*") {
		string sum_local = "SUM(" + QualifiedColumn("d", expr.argument) + ") OVER (PARTITION BY " +
		                   QualifiedColumn("d", plan.partition_column) + " ORDER BY " +
		                   QualifiedColumn("d", plan.order_column) + ")";
		string count_local = "COUNT(" + QualifiedColumn("d", expr.argument) + ") OVER (PARTITION BY " +
		                     QualifiedColumn("d", plan.partition_column) + " ORDER BY " +
		                     QualifiedColumn("d", plan.order_column) + ")";
		string prior_count_col = QualifiedColumn("s", RunningAvgPriorCountColumn(expr));
		string prior_count = "COALESCE(" + prior_count_col + ", 0)";
		return "((COALESCE(" + state_col + " * " + prior_count_col + ", 0)) + COALESCE(" + sum_local +
		       ", 0)) / NULLIF(" + prior_count + " + " + count_local + ", 0)";
	}
	return "";
}

static string RunningAdjustedExprWithSeed(const RunningWindowExpr &expr, const string &local, const string &state_col) {
	if (expr.function_name == "sum") {
		return "CASE WHEN " + state_col + " IS NULL THEN " + local + " ELSE " + state_col + " + " + local + " END";
	}
	if (expr.function_name == "count") {
		return "COALESCE(" + state_col + ", 0) + " + local;
	}
	if (expr.function_name == "min") {
		return "CASE WHEN " + state_col + " IS NULL THEN " + local + " WHEN " + local + " IS NULL THEN " + state_col +
		       " ELSE LEAST(" + state_col + ", " + local + ") END";
	}
	if (expr.function_name == "max") {
		return "CASE WHEN " + state_col + " IS NULL THEN " + local + " WHEN " + local + " IS NULL THEN " + state_col +
		       " ELSE GREATEST(" + state_col + ", " + local + ") END";
	}
	return "";
}

static string SparkPortableTimestampCasts(const string &sql) {
	static const std::regex timestamp_cast_regex(R"(('[^']*(?:''[^']*)*')::TIMESTAMP)", std::regex_constants::icase);
	return std::regex_replace(sql, timestamp_cast_regex, "CAST($1 AS TIMESTAMP)");
}

static string BuildRunningWindowSuffixRefreshSQL(const string &view_name, const string &view_query_sql,
                                                 const string &delta_ts_filter, const string &catalog_prefix,
                                                 const vector<string> &partition_columns,
                                                 const vector<WindowPartitionDeltaSpec> &partition_delta_specs,
                                                 const vector<string> &column_names, bool emit_cascade_delta) {
	if (partition_delta_specs.size() != 1) {
		return "";
	}
	vector<string> visible_column_names;
	for (auto &col : column_names) {
		if (!StringUtil::CIEquals(col, openivm::MULTIPLICITY_COL) &&
		    !StringUtil::CIEquals(col, openivm::TIMESTAMP_COL)) {
			visible_column_names.push_back(col);
		}
	}
	RunningWindowPlan plan;
	if (plan.window_exprs.empty() &&
	    !TryParseRunningWindowPlan(view_query_sql, partition_columns, visible_column_names, plan)) {
		plan = RunningWindowPlan();
		if (!TryParseLptsRunningWindowPlan(view_query_sql, partition_columns, visible_column_names, plan)) {
			return "";
		}
	}
	const auto &spec = partition_delta_specs[0];
	if (!StringUtil::CIEquals(spec.output_column, plan.partition_column)) {
		return "";
	}
	string delta_q = spec.delta_table_sql.empty() ? SqlUtils::QuoteIdentifier(spec.delta_table) : spec.delta_table_sql;
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string affected_table = SqlUtils::QuoteIdentifier("openivm_run_affected_" + view_name);
	string bounds_table = SqlUtils::QuoteIdentifier("openivm_run_bounds_" + view_name);
	string fast_table = SqlUtils::QuoteIdentifier("openivm_run_fast_" + view_name);
	string fallback_table = SqlUtils::QuoteIdentifier("openivm_run_fallback_" + view_name);
	string state_table = SqlUtils::QuoteIdentifier("openivm_run_state_" + view_name);
	string delta_table = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
	string old_temp_table = SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + view_name);
	string new_temp_table = SqlUtils::QuoteIdentifier(string("openivm_new_") + view_name);
	string portable_delta_ts_filter = SparkPortableTimestampCasts(delta_ts_filter);
	string delta_filter = portable_delta_ts_filter.empty() ? "" : " AND " + portable_delta_ts_filter;
	string delta_positive = QualifiedColumn("d", openivm::MULTIPLICITY_COL) + " > 0" + delta_filter;
	string part_q = SqlUtils::QuoteIdentifier(plan.partition_column);
	string order_q = SqlUtils::QuoteIdentifier(plan.order_column);
	string key_match_df = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "d", "fk");
	string key_match_dt_fk = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "dt", "fk");
	string key_match_b_m = "b." + part_q + " IS NOT DISTINCT FROM m." + part_q;
	string key_match_d_fk = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "d", "fk");
	string affected_data_filter =
	    BuildAffectedTableFilter(vector<string> {plan.partition_column}, "dt", affected_table);

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + affected_table + " AS\nSELECT DISTINCT " +
	       QualifiedColumn("d", plan.partition_column) + " AS " + part_q + "\nFROM " + delta_q + " d\nWHERE " +
	       delta_positive + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + bounds_table + " AS\nWITH old_max AS (\n  SELECT dt." + part_q +
	       ", MAX(dt." + order_q + ") AS openivm_old_max_order FROM " + data_table + " dt WHERE " +
	       affected_data_filter + " GROUP BY dt." + part_q + "\n), delta_min AS (\n  SELECT " +
	       QualifiedColumn("d", plan.partition_column) + " AS " + part_q + ", MIN(" +
	       QualifiedColumn("d", plan.order_column) + ") AS openivm_delta_min_order\n  FROM " + delta_q +
	       " d\n  WHERE " + delta_positive + "\n  GROUP BY " + QualifiedColumn("d", plan.partition_column) +
	       "\n)\nSELECT a." + part_q + ", m.openivm_old_max_order, b.openivm_delta_min_order\nFROM " + affected_table +
	       " a\nLEFT JOIN old_max m ON a." + part_q + " IS NOT DISTINCT FROM m." + part_q + "\nJOIN delta_min b ON a." +
	       part_q + " IS NOT DISTINCT FROM b." + part_q + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + fast_table + " AS\nSELECT " + part_q + " FROM " + bounds_table +
	       "\nWHERE openivm_old_max_order IS NULL OR openivm_delta_min_order > openivm_old_max_order;\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + fallback_table + " AS\nSELECT " + part_q + " FROM " + bounds_table +
	       "\nWHERE openivm_old_max_order IS NOT NULL AND openivm_delta_min_order <= openivm_old_max_order;\n\n";
	auto state_columns = plan.output_columns.empty() ? visible_column_names : plan.output_columns;
	string state_outer_cols = SqlUtils::JoinQuotedColumns(state_columns);
	state_outer_cols += ", openivm_prior_count";
	string state_inner_cols;
	for (idx_t i = 0; i < state_columns.size(); i++) {
		if (i > 0) {
			state_inner_cols += ", ";
		}
		state_inner_cols +=
		    QualifiedColumn("dt", state_columns[i]) + " AS " + SqlUtils::QuoteIdentifier(state_columns[i]);
	}
	for (auto &expr : plan.window_exprs) {
		if (expr.function_name == "avg" && expr.argument != "*") {
			string prior_count_col = RunningAvgPriorCountColumn(expr);
			state_outer_cols += ", " + SqlUtils::QuoteIdentifier(prior_count_col);
			state_inner_cols += ", COUNT(" + QualifiedColumn("dt", expr.argument) + ") OVER (PARTITION BY " +
			                    QualifiedColumn("dt", plan.partition_column) + ") AS " +
			                    SqlUtils::QuoteIdentifier(prior_count_col);
		}
	}
	sql += "CREATE OR REPLACE TEMP TABLE " + state_table + " AS\nSELECT " + state_outer_cols + " FROM (\n  SELECT " +
	       state_inner_cols + ", COUNT(*) OVER (PARTITION BY " + QualifiedColumn("dt", plan.partition_column) +
	       ") AS openivm_prior_count, ROW_NUMBER() OVER (PARTITION BY " + QualifiedColumn("dt", plan.partition_column) +
	       " ORDER BY " + QualifiedColumn("dt", plan.order_column) + " DESC) AS openivm_rn\n  FROM " + data_table +
	       " dt\n  JOIN " + fast_table + " fk ON " + key_match_dt_fk +
	       "\n) openivm_state_ranked\nWHERE openivm_rn = 1;\n\n";
	string fallback_keys = "SELECT " + part_q + " FROM " + fallback_table;
	string fallback_target_match =
	    SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "openivm_aff", "openivm_target");
	string fallback_recompute_match =
	    SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "openivm_aff", "openivm_recompute");
	if (emit_cascade_delta) {
		string fallback_old_filter =
		    BuildAffectedTableFilter(vector<string> {plan.partition_column}, "openivm_old", fallback_table);
		string fallback_new_filter =
		    BuildAffectedTableFilter(vector<string> {plan.partition_column}, "openivm_recompute", fallback_table);
		string fallback_delete_filter =
		    BuildAffectedTableFilter(vector<string> {plan.partition_column}, "openivm_target", fallback_table);
		sql += "CREATE OR REPLACE TEMP TABLE " + old_temp_table + " AS\nSELECT * FROM " + data_table +
		       " openivm_old\nWHERE " + fallback_old_filter + ";\n\n";
		sql += "CREATE OR REPLACE TEMP TABLE " + new_temp_table + " AS\nSELECT * FROM (" + view_query_sql +
		       ") openivm_recompute\nWHERE " + fallback_new_filter + ";\n\n";
		sql += "DELETE FROM " + data_table + " AS openivm_target WHERE " + fallback_delete_filter + ";\n";
		sql += "INSERT INTO " + data_table + "\nSELECT * FROM " + new_temp_table + ";\n";
		sql += "\n" + BuildSignedMultisetDeltaInsertSQL(delta_table, old_temp_table, new_temp_table);
	} else {
		sql +=
		    BuildAffectedKeyRefreshSQL(data_table, view_query_sql, fallback_keys, "openivm_target", "openivm_recompute",
		                               "openivm_aff", fallback_target_match, fallback_recompute_match);
	}

	auto emit_column_names = plan.output_columns.empty() ? visible_column_names : plan.output_columns;
	string insert_cols = SqlUtils::JoinQuotedColumns(emit_column_names);
	if (!plan.derived_exprs.empty()) {
		vector<RunningWindowExpr> level1_exprs;
		vector<RunningWindowExpr> level3_exprs;
		for (auto &expr : plan.window_exprs) {
			if (IsRunningDerivedArgument(expr, plan)) {
				if (expr.function_name != "max") {
					return "";
				}
				level3_exprs.push_back(expr);
			} else {
				level1_exprs.push_back(expr);
			}
		}
		if (level1_exprs.empty() || level3_exprs.empty()) {
			return "";
		}
		bool has_partition = false;
		bool has_order = false;
		for (auto &pass : plan.passthrough_columns) {
			has_partition = has_partition || StringUtil::CIEquals(pass.second, plan.partition_column);
			has_order = has_order || StringUtil::CIEquals(pass.second, plan.order_column);
		}
		if (!has_partition || !has_order) {
			return "";
		}
		string l1_select;
		auto append_l1 = [&](const string &expr_sql, const string &alias) {
			if (!l1_select.empty()) {
				l1_select += ", ";
			}
			l1_select += expr_sql + " AS " + SqlUtils::QuoteIdentifier(alias);
		};
		for (auto &pass : plan.passthrough_columns) {
			append_l1(QualifiedColumn("d", pass.first), pass.second);
		}
		for (auto &expr : level1_exprs) {
			append_l1(RunningAdjustedExpr(expr, plan), expr.output_column);
		}
		for (auto &expr : level3_exprs) {
			append_l1(QualifiedColumn("s", expr.output_column), RunningSeedColumn(expr));
		}
		string lflags_select = "*";
		for (auto &derived : plan.derived_exprs) {
			lflags_select += ", " + derived.expression + " AS " + SqlUtils::QuoteIdentifier(derived.output_column);
		}
		string l3_select;
		auto append_l3 = [&](const string &expr_sql, const string &alias) {
			if (!l3_select.empty()) {
				l3_select += ", ";
			}
			l3_select += expr_sql + " AS " + SqlUtils::QuoteIdentifier(alias);
		};
		for (auto &pass : plan.passthrough_columns) {
			append_l3(QualifiedColumn("f", pass.second), pass.second);
		}
		for (auto &expr : level1_exprs) {
			append_l3(QualifiedColumn("f", expr.output_column), expr.output_column);
		}
		for (auto &expr : level3_exprs) {
			string local = RunningLocalExprFromAlias(expr, plan, "f");
			string adjusted = RunningAdjustedExprWithSeed(expr, local, QualifiedColumn("f", RunningSeedColumn(expr)));
			if (adjusted.empty()) {
				return "";
			}
			append_l3(adjusted, expr.output_column);
		}
		string final_select;
		for (idx_t i = 0; i < emit_column_names.size(); i++) {
			if (i > 0) {
				final_select += ", ";
			}
			final_select += QualifiedColumn("r", emit_column_names[i]);
		}
		string state_match = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "d", "s");
		sql += "INSERT INTO " + data_table + " (" + insert_cols + ")\nWITH openivm_l1 AS (\n  SELECT " + l1_select +
		       "\n  FROM " + delta_q + " d\n  JOIN " + fast_table + " fk ON " + key_match_d_fk + "\n  LEFT JOIN " +
		       state_table + " s ON " + state_match + "\n  WHERE " + delta_positive +
		       "\n), openivm_flags AS (\n  SELECT " + lflags_select +
		       "\n  FROM openivm_l1\n), openivm_l3 AS (\n  SELECT " + l3_select +
		       "\n  FROM openivm_flags f\n)\nSELECT " + final_select + "\nFROM openivm_l3 r;\n\n";
		if (emit_cascade_delta) {
			sql += "INSERT INTO " + delta_table + "\nWITH openivm_l1 AS (\n  SELECT " + l1_select + "\n  FROM " +
			       delta_q + " d\n  JOIN " + fast_table + " fk ON " + key_match_d_fk + "\n  LEFT JOIN " + state_table +
			       " s ON " + state_match + "\n  WHERE " + delta_positive + "\n), openivm_flags AS (\n  SELECT " +
			       lflags_select + "\n  FROM openivm_l1\n), openivm_l3 AS (\n  SELECT " + l3_select +
			       "\n  FROM openivm_flags f\n)\nSELECT " + final_select +
			       ", CAST(1 AS INTEGER), CURRENT_TIMESTAMP\nFROM openivm_l3 r;\n\n";
			sql += "DROP TABLE IF EXISTS " + old_temp_table + ";\n";
			sql += "DROP TABLE IF EXISTS " + new_temp_table + ";\n";
		}
		sql += "DROP TABLE IF EXISTS " + state_table + ";\n";
		sql += "DROP TABLE IF EXISTS " + fallback_table + ";\n";
		sql += "DROP TABLE IF EXISTS " + fast_table + ";\n";
		sql += "DROP TABLE IF EXISTS " + bounds_table + ";\n";
		sql += "DROP TABLE IF EXISTS " + affected_table + ";\n";
		OPENIVM_DEBUG_PRINT(
		    "[CompileWindowSuffixExtend] view=%s partition=%s order=%s window_exprs=%zu derived_exprs=%zu\n",
		    view_name.c_str(), plan.partition_column.c_str(), plan.order_column.c_str(), plan.window_exprs.size(),
		    plan.derived_exprs.size());
		(void)key_match_df;
		(void)key_match_b_m;
		return sql;
	}
	string select_list;
	for (idx_t i = 0; i < emit_column_names.size(); i++) {
		if (i > 0) {
			select_list += ", ";
		}
		string expr_sql;
		for (auto &pass : plan.passthrough_columns) {
			if (StringUtil::CIEquals(emit_column_names[i], pass.second)) {
				expr_sql = QualifiedColumn("d", pass.first);
				break;
			}
		}
		for (auto &expr : plan.window_exprs) {
			if (StringUtil::CIEquals(emit_column_names[i], expr.output_column)) {
				expr_sql = RunningAdjustedExpr(expr, plan);
				break;
			}
		}
		if (expr_sql.empty()) {
			return "";
		}
		select_list += expr_sql + " AS " + SqlUtils::QuoteIdentifier(emit_column_names[i]);
	}
	string state_match = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "d", "s");
	sql += "INSERT INTO " + data_table + " (" + insert_cols + ")\nSELECT " + select_list + "\nFROM " + delta_q +
	       " d\nJOIN " + fast_table + " fk ON " + key_match_d_fk + "\nLEFT JOIN " + state_table + " s ON " +
	       state_match + "\nWHERE " + delta_positive + ";\n\n";
	if (emit_cascade_delta) {
		sql += "INSERT INTO " + delta_table + "\nSELECT " + select_list +
		       ", CAST(1 AS INTEGER), CURRENT_TIMESTAMP\nFROM " + delta_q + " d\nJOIN " + fast_table + " fk ON " +
		       key_match_d_fk + "\nLEFT JOIN " + state_table + " s ON " + state_match + "\nWHERE " + delta_positive +
		       ";\n\n";
		sql += "DROP TABLE IF EXISTS " + old_temp_table + ";\n";
		sql += "DROP TABLE IF EXISTS " + new_temp_table + ";\n";
	}
	sql += "DROP TABLE IF EXISTS " + state_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + fallback_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + fast_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + bounds_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + affected_table + ";\n";
	OPENIVM_DEBUG_PRINT("[CompileWindowSuffixExtend] view=%s partition=%s order=%s window_exprs=%zu\n",
	                    view_name.c_str(), plan.partition_column.c_str(), plan.order_column.c_str(),
	                    plan.window_exprs.size());
	(void)key_match_df;
	(void)key_match_b_m;
	return sql;
}

static string CreateAuxTablePrefix(const string &target_table, bool replace) {
	return string(replace ? "CREATE OR REPLACE TABLE " : "CREATE TABLE IF NOT EXISTS ") + target_table;
}

static string SourceExprForColumn(const vector<string> &cols, const vector<string> &source_exprs, idx_t column_idx,
                                  const string &fallback_alias = string()) {
	if (column_idx < source_exprs.size() && !source_exprs[column_idx].empty()) {
		return source_exprs[column_idx];
	}
	string col = SqlUtils::QuoteIdentifier(cols[column_idx]);
	if (!fallback_alias.empty()) {
		return fallback_alias + "." + col;
	}
	return cols[column_idx];
}

static void BuildAliasedSourceLists(const vector<string> &cols, const vector<string> &source_exprs, string &select_list,
                                    string &group_list, const string &fallback_alias = string()) {
	select_list.clear();
	group_list.clear();
	for (idx_t i = 0; i < cols.size(); i++) {
		if (i > 0) {
			select_list += ", ";
			group_list += ", ";
		}
		string expr = SourceExprForColumn(cols, source_exprs, i, fallback_alias);
		select_list += expr + " AS " + SqlUtils::QuoteIdentifier(cols[i]);
		group_list += expr;
	}
}

static bool IsIdentifierTokenChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static bool MatchesPatternCI(const string &text, idx_t pos, const string &pattern) {
	if (pos + pattern.size() > text.size()) {
		return false;
	}
	for (idx_t i = 0; i < pattern.size(); i++) {
		if (std::tolower(static_cast<unsigned char>(text[pos + i])) !=
		    std::tolower(static_cast<unsigned char>(pattern[i]))) {
			return false;
		}
	}
	return true;
}

static string RewriteQualifiedAliasPrefix(string expr, const string &source_alias, const string &target_alias) {
	string pattern = source_alias + ".";
	string replacement = target_alias + ".";
	string result;
	for (idx_t pos = 0; pos < expr.size();) {
		if (expr[pos] == '\'') {
			idx_t start = pos++;
			while (pos < expr.size()) {
				if (expr[pos] == '\'' && pos + 1 < expr.size() && expr[pos + 1] == '\'') {
					pos += 2;
					continue;
				}
				if (expr[pos++] == '\'') {
					break;
				}
			}
			result += expr.substr(start, pos - start);
			continue;
		}
		bool left_boundary = pos == 0 || !IsIdentifierTokenChar(expr[pos - 1]);
		if (left_boundary && MatchesPatternCI(expr, pos, pattern)) {
			result += replacement;
			pos += pattern.size();
			continue;
		}
		result += expr[pos++];
	}
	return result;
}

static bool ReferencesQualifiedAlias(const string &expr, const string &alias) {
	string pattern = alias + ".";
	for (idx_t pos = 0; pos < expr.size();) {
		if (expr[pos] == '\'') {
			pos++;
			while (pos < expr.size()) {
				if (expr[pos] == '\'' && pos + 1 < expr.size() && expr[pos + 1] == '\'') {
					pos += 2;
					continue;
				}
				if (expr[pos++] == '\'') {
					break;
				}
			}
			continue;
		}
		bool left_boundary = pos == 0 || !IsIdentifierTokenChar(expr[pos - 1]);
		if (left_boundary && MatchesPatternCI(expr, pos, pattern)) {
			return true;
		}
		pos++;
	}
	return false;
}

} // namespace

string BuildDistinctAuxStateCreateSQL(const string &target_table, const vector<string> &distinct_cols,
                                      const vector<string> &source_exprs, const string &source_relation,
                                      const string &filter_sql, bool replace) {
	string select_list;
	string group_list;
	BuildAliasedSourceLists(distinct_cols, source_exprs, select_list, group_list);
	string filter = filter_sql.empty() ? "" : " WHERE " + filter_sql;
	return CreateAuxTablePrefix(target_table, replace) + " AS SELECT " + select_list +
	       ", count(*)::BIGINT AS _count FROM " + source_relation + filter + " GROUP BY " + group_list;
}

string CompileDistinctIncremental(const string &view_name, const string &aux_table, const vector<string> &distinct_cols,
                                  const vector<string> &source_exprs, const string &delta_source,
                                  const string &last_update, const string &filter_sql,
                                  const vector<string> &group_columns, const string &sum_arg, const string &sum_out,
                                  const string &count_star_col, const string &catalog_prefix) {
	if (distinct_cols.empty() || group_columns.empty() || sum_arg.empty() || sum_out.empty()) {
		throw InternalException("CompileDistinctIncremental called with incomplete metadata for view '%s'", view_name);
	}
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	string delta_q = DeltaSourceRef(delta_source, catalog_prefix);
	string sum_arg_q = SqlUtils::QuoteIdentifier(sum_arg);
	string sum_out_q = SqlUtils::QuoteIdentifier(sum_out);
	string count_q = SqlUtils::QuoteIdentifier(count_star_col);

	string distinct_cols_csv = SqlUtils::JoinQuotedColumns(distinct_cols);
	string distinct_cols_csv_i = SqlUtils::JoinQualifiedQuotedColumns(distinct_cols, "i");
	string group_cols_csv = SqlUtils::JoinQuotedColumns(group_columns);
	string mv_match = SqlUtils::BuildNullSafeMatch(group_columns, "v", "d");
	string source_select;
	string source_group;
	BuildAliasedSourceLists(distinct_cols, source_exprs, source_select, source_group);

	string dinput_table = "openivm_dinput_" + view_name;
	string ts_filter =
	    " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP";
	string filter_clause = filter_sql.empty() ? "" : " AND (" + filter_sql + ")";

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dinput_table) + " AS\n  SELECT " +
	       source_select + ", SUM(" + string(openivm::MULTIPLICITY_COL) + ")::BIGINT AS dmult\n  FROM " + delta_q +
	       ts_filter + filter_clause + "\n  GROUP BY " + source_group + "\n  HAVING SUM(" +
	       string(openivm::MULTIPLICITY_COL) + ") <> 0;\n\n";

	string aux_match_aliased = SqlUtils::BuildNullSafeMatch(distinct_cols, "_aux", "i");
	string ddist_cte =
	    "WITH ddist AS (\n  SELECT " + distinct_cols_csv_i +
	    ", CASE WHEN COALESCE(_aux._count, 0) = 0 AND i.dmult > 0 THEN 1 "
	    "WHEN COALESCE(_aux._count, 0) > 0 AND COALESCE(_aux._count, 0) + i.dmult <= 0 THEN -1 ELSE 0 END AS dd\n"
	    "  FROM " +
	    SqlUtils::QuoteIdentifier(dinput_table) + " i LEFT JOIN " + aux_q + " _aux ON " + aux_match_aliased +
	    "\n),\ndagg AS (\n  SELECT " + group_cols_csv + ", SUM(" + sum_arg_q +
	    " * dd) AS d_sum, SUM(dd)::BIGINT AS d_count\n  FROM ddist WHERE dd <> 0\n  GROUP BY " + group_cols_csv +
	    "\n)\n";

	string insert_cols = group_cols_csv + ", " + sum_out_q + ", " + count_q;
	string insert_vals = SqlUtils::JoinQualifiedQuotedColumns(group_columns, "d") + ", d.d_sum, d.d_count";

	sql += ddist_cte + "MERGE INTO " + data_table + " v USING dagg d ON " + mv_match +
	       "\nWHEN MATCHED THEN UPDATE SET " + sum_out_q + " = COALESCE(v." + sum_out_q + ", 0) + d.d_sum, " + count_q +
	       " = v." + count_q + " + d.d_count\nWHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (" +
	       insert_vals + ");\n\n";

	sql += "DELETE FROM " + data_table + " WHERE " + count_q + " <= 0;\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dinput_table) + " i ON " +
	       aux_match_aliased +
	       "\nWHEN MATCHED THEN UPDATE SET _count = _aux._count + i.dmult\nWHEN NOT MATCHED AND i.dmult > 0 "
	       "THEN INSERT (" +
	       distinct_cols_csv + ", _count) VALUES (" + distinct_cols_csv_i + ", i.dmult);\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE _count <= 0;\n\n";
	sql += "DROP TABLE IF EXISTS " + SqlUtils::QuoteIdentifier(dinput_table) + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileDistinctIncremental] %zu distinct cols, %zu group cols, sum %s(%s)→%s, aux=%s\n",
	                    distinct_cols.size(), group_columns.size(), "SUM", sum_arg.c_str(), sum_out.c_str(),
	                    aux_table.c_str());
	return sql;
}

string BuildCountDistinctAuxStateCreateSQL(const string &target_table, const string &source_relation,
                                           const vector<string> &group_cols, const vector<string> &group_source_exprs,
                                           const string &distinct_col, const string &distinct_expr,
                                           const string &filter_sql, bool replace) {
	if (group_cols.empty() || group_cols.size() != group_source_exprs.size() || distinct_col.empty() ||
	    distinct_expr.empty()) {
		throw InternalException("BuildCountDistinctAuxStateCreateSQL called with incomplete metadata");
	}
	string select_list;
	string group_list;
	for (idx_t i = 0; i < group_cols.size(); i++) {
		if (i > 0) {
			select_list += ", ";
			group_list += ", ";
		}
		select_list += group_source_exprs[i] + " AS " + SqlUtils::QuoteIdentifier(group_cols[i]);
		group_list += group_source_exprs[i];
	}
	select_list += ", " + distinct_expr + " AS " + SqlUtils::QuoteIdentifier(distinct_col);
	group_list += ", " + distinct_expr;
	string filter = filter_sql.empty() ? "" : "(" + filter_sql + ") AND ";
	return CreateAuxTablePrefix(target_table, replace) + " AS SELECT " + select_list +
	       ", count(*)::BIGINT AS _count FROM " + source_relation + " WHERE " + filter + distinct_expr +
	       " IS NOT NULL GROUP BY " + group_list;
}

string CompileCountDistinctIncremental(const string &view_name, const string &aux_table, const string &delta_source,
                                       const string &last_update, const vector<string> &group_cols,
                                       const vector<string> &group_source_exprs, const string &distinct_col,
                                       const string &distinct_expr, const string &output_col,
                                       const string &count_star_col, const string &filter_sql,
                                       const string &catalog_prefix) {
	if (group_cols.empty() || group_cols.size() != group_source_exprs.size() || aux_table.empty() ||
	    delta_source.empty() || last_update.empty() || distinct_col.empty() || distinct_expr.empty() ||
	    output_col.empty()) {
		throw InternalException("CompileCountDistinctIncremental called with incomplete metadata for view '%s'",
		                        view_name);
	}
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	string delta_q = DeltaSourceRef(delta_source, catalog_prefix);
	string dinput_table = SqlUtils::QuoteIdentifier("openivm_cd_dinput_" + view_name);
	string dgroups_table = SqlUtils::QuoteIdentifier("openivm_cd_dgroups_" + view_name);
	string dagg_table = SqlUtils::QuoteIdentifier("openivm_cd_dagg_" + view_name);
	string mul = string(openivm::MULTIPLICITY_COL);
	string ts_filter = string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP";
	string base_filter = filter_sql.empty() ? ts_filter : ts_filter + " AND (" + filter_sql + ")";

	string group_select;
	string group_by;
	string group_csv = SqlUtils::JoinQuotedColumns(group_cols);
	string group_csv_i = SqlUtils::JoinQualifiedQuotedColumns(group_cols, "i");
	for (idx_t i = 0; i < group_cols.size(); i++) {
		if (i > 0) {
			group_select += ", ";
			group_by += ", ";
		}
		group_select += group_source_exprs[i] + " AS " + SqlUtils::QuoteIdentifier(group_cols[i]);
		group_by += group_source_exprs[i];
	}

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + dinput_table + " AS\n  SELECT " + group_select + ", " + distinct_expr +
	       " AS " + SqlUtils::QuoteIdentifier(distinct_col) + ", SUM(" + mul + ")::BIGINT AS dmult\n  FROM " + delta_q +
	       "\n  WHERE " + base_filter + " AND " + distinct_expr + " IS NOT NULL\n  GROUP BY " + group_by + ", " +
	       distinct_expr + "\n  HAVING SUM(" + mul + ") <> 0;\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + dgroups_table + " AS\n  SELECT " + group_select + ", SUM(" + mul +
	       ")::BIGINT AS d_count_star\n  FROM " + delta_q + "\n  WHERE " + base_filter + "\n  GROUP BY " + group_by +
	       "\n  HAVING SUM(" + mul + ") <> 0;\n\n";

	vector<string> aux_match_cols = group_cols;
	aux_match_cols.push_back(distinct_col);
	string aux_match = SqlUtils::BuildNullSafeMatch(aux_match_cols, "_aux", "i");
	sql += "CREATE OR REPLACE TEMP TABLE " + dagg_table + " AS\nWITH ddist AS (\n  SELECT " + group_csv_i +
	       ", CASE WHEN COALESCE(_aux._count, 0) = 0 AND i.dmult > 0 THEN 1 "
	       "WHEN COALESCE(_aux._count, 0) > 0 AND COALESCE(_aux._count, 0) + i.dmult <= 0 THEN -1 ELSE 0 END AS dd\n"
	       "  FROM " +
	       dinput_table + " i LEFT JOIN " + aux_q + " _aux ON " + aux_match + "\n), dcount AS (\n  SELECT " +
	       group_csv +
	       ", SUM(dd)::BIGINT AS d_count_distinct, 0::BIGINT AS d_count_star\n  FROM ddist WHERE dd <> 0 GROUP BY " +
	       group_csv + "\n  UNION ALL\n  SELECT " + group_csv + ", 0::BIGINT AS d_count_distinct, d_count_star FROM " +
	       dgroups_table + "\n)\nSELECT " + group_csv +
	       ", SUM(d_count_distinct)::BIGINT AS d_count_distinct, SUM(d_count_star)::BIGINT AS d_count_star\nFROM "
	       "dcount GROUP BY " +
	       group_csv + " HAVING SUM(d_count_distinct) <> 0 OR SUM(d_count_star) <> 0;\n\n";

	string output_q = SqlUtils::QuoteIdentifier(output_col);
	string mv_match = SqlUtils::BuildNullSafeMatch(group_cols, "v", "d");
	string insert_cols = group_csv + ", " + output_q;
	string insert_vals = SqlUtils::JoinQualifiedQuotedColumns(group_cols, "d") + ", d.d_count_distinct";
	string update_set = output_q + " = COALESCE(v." + output_q + ", 0) + d.d_count_distinct";
	if (!count_star_col.empty()) {
		string count_q = SqlUtils::QuoteIdentifier(count_star_col);
		insert_cols += ", " + count_q;
		insert_vals += ", d.d_count_star";
		update_set += ", " + count_q + " = COALESCE(v." + count_q + ", 0) + d.d_count_star";
	}
	sql += "MERGE INTO " + data_table + " v USING " + dagg_table + " d ON " + mv_match +
	       "\nWHEN MATCHED THEN UPDATE SET " + update_set + "\nWHEN NOT MATCHED THEN INSERT (" + insert_cols +
	       ") VALUES (" + insert_vals + ");\n\n";
	if (!count_star_col.empty()) {
		sql += "DELETE FROM " + data_table + " WHERE " + SqlUtils::QuoteIdentifier(count_star_col) + " <= 0;\n\n";
	} else {
		sql += "DELETE FROM " + data_table + " WHERE " + output_q + " <= 0;\n\n";
	}
	sql += "MERGE INTO " + aux_q + " _aux USING " + dinput_table + " i ON " + aux_match +
	       "\nWHEN MATCHED THEN UPDATE SET _count = _aux._count + i.dmult\nWHEN NOT MATCHED AND i.dmult > 0 "
	       "THEN INSERT (" +
	       group_csv + ", " + SqlUtils::QuoteIdentifier(distinct_col) + ", _count) VALUES (" + group_csv_i + ", i." +
	       SqlUtils::QuoteIdentifier(distinct_col) + ", i.dmult);\n\n";
	sql += "DELETE FROM " + aux_q + " WHERE _count <= 0;\n\n";
	sql += "DROP TABLE IF EXISTS " + dinput_table + ";\nDROP TABLE IF EXISTS " + dgroups_table +
	       ";\nDROP TABLE IF EXISTS " + dagg_table + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileCountDistinctIncremental] %zu group cols, distinct=%s, out=%s, aux=%s\n",
	                    group_cols.size(), distinct_expr.c_str(), output_col.c_str(), aux_table.c_str());
	return sql;
}

string BuildSemiAntiAuxStateCreateSQL(const string &target_table, const string &left_source, const string &left_alias,
                                      const string &right_source, const string &right_alias, const string &predicate,
                                      const string &post_filter, const string &right_filter,
                                      const vector<string> &left_cols, const vector<string> &left_exprs, bool replace,
                                      bool null_aware, const string &null_aware_right_expr) {
	string left_cols_csv = SqlUtils::JoinQuotedColumns(left_cols);
	string left_cols_qualified = SqlUtils::JoinQualifiedQuotedColumns(left_cols, left_alias);
	string left_cols_lc = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "lc");
	string left_cols_mc = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "mc");
	string lc_mc_match = SqlUtils::BuildNullSafeMatch(left_cols, "lc", "mc");
	string left_source_select;
	string unused_group_list;
	BuildAliasedSourceLists(left_cols, left_exprs, left_source_select, unused_group_list, left_alias);
	string left_source_filter = post_filter.empty() ? "" : " WHERE " + post_filter;
	string match_predicate = predicate + (right_filter.empty() ? "" : " AND (" + right_filter + ")");
	string right_filter_sql = right_filter.empty() ? "" : " WHERE " + right_filter;
	bool null_aware_anti = null_aware && !null_aware_right_expr.empty();
	bool correlated_right_filter = null_aware_anti && ReferencesQualifiedAlias(right_filter, left_alias);
	string right_stats_cte;
	string right_stats_select;
	string right_stats_from;
	if (null_aware_anti) {
		if (correlated_right_filter) {
			string right_filter_lc = RewriteQualifiedAliasPrefix(right_filter, left_alias, "lc");
			right_stats_select = ", (SELECT count(*)::BIGINT FROM " + right_source + " " + right_alias + " WHERE " +
			                     right_filter_lc + ") AS _right_count, (SELECT count(*) FILTER (WHERE " +
			                     null_aware_right_expr + " IS NULL)::BIGINT FROM " + right_source + " " + right_alias +
			                     " WHERE " + right_filter_lc + ") AS _right_null_count";
		} else {
			right_stats_cte = ", right_stats AS (SELECT count(*)::BIGINT AS _right_count, count(*) FILTER (WHERE " +
			                  null_aware_right_expr + " IS NULL)::BIGINT AS _right_null_count FROM " + right_source +
			                  " " + right_alias + right_filter_sql + ")";
			right_stats_select = ", rs._right_count, rs._right_null_count";
			right_stats_from = " CROSS JOIN right_stats rs";
		}
	}
	return CreateAuxTablePrefix(target_table, replace) + " AS WITH left_source AS (SELECT " + left_source_select +
	       " FROM " + left_source + " " + left_alias + left_source_filter + "), left_counts AS (SELECT " +
	       left_cols_csv + ", count(*)::BIGINT AS _left_count FROM left_source GROUP BY " + left_cols_csv +
	       "), match_counts AS (SELECT " + left_cols_qualified +
	       ", count(*)::BIGINT AS _match_count FROM (SELECT DISTINCT " + left_cols_csv + " FROM left_source) " +
	       left_alias + " JOIN " + right_source + " " + right_alias + " ON " + match_predicate + " GROUP BY " +
	       left_cols_qualified + ")" + right_stats_cte + " SELECT " + left_cols_lc +
	       ", lc._left_count, coalesce(mc._match_count, 0)::BIGINT AS _match_count" + right_stats_select +
	       " FROM left_counts lc LEFT JOIN match_counts mc ON " + lc_mc_match + right_stats_from;
}

static string SemiAntiVisibleExpr(const string &join_type, bool null_aware, const string &null_aware_left_col,
                                  const string &alias) {
	string prefix = alias.empty() ? "" : alias + ".";
	bool is_anti = StringUtil::Lower(join_type) == "anti";
	if (!is_anti) {
		return prefix + "_match_count > 0";
	}
	if (null_aware && !null_aware_left_col.empty()) {
		string left_not_null = "NOT coalesce(" + prefix + SqlUtils::QuoteIdentifier(null_aware_left_col) + ", true)";
		return prefix + "_match_count = 0 AND (" + prefix + "_right_count = 0 OR (" + left_not_null + " AND " + prefix +
		       "_right_null_count = 0))";
	}
	return prefix + "_match_count = 0";
}

string BuildSemiAntiInitialDataSQL(const string &data_table, const string &aux_table, const string &join_type,
                                   const vector<string> &left_cols, const vector<string> &output_cols, bool null_aware,
                                   const string &null_aware_left_col) {
	if (output_cols.empty()) {
		throw InternalException("BuildSemiAntiInitialDataSQL called without output columns");
	}
	string output_cur = SqlUtils::JoinQualifiedQuotedColumns(output_cols, "_cur");
	string visible = SemiAntiVisibleExpr(join_type, null_aware, null_aware_left_col, "_cur");
	return "create table " + data_table + " as SELECT " + output_cur + " FROM " + aux_table +
	       " _cur, generate_series(1, _cur._left_count::BIGINT) WHERE " + visible + " AND _cur._left_count > 0";
}

string CompileSemiAntiRecompute(const string &view_name, const string &aux_table, const string &join_type,
                                const string &left_table, const string &left_alias, const string &right_table,
                                const string &right_alias, const string &predicate, const string &post_filter,
                                const string &right_filter, const vector<string> &left_cols,
                                const vector<string> &left_exprs, const vector<string> &output_cols,
                                const string &left_delta_source, const string &right_delta_source,
                                const string &left_last_update, const string &right_last_update,
                                const string &catalog_prefix, bool null_aware, const string &null_aware_left_col,
                                const string &null_aware_right_expr) {
	if (left_cols.empty() || output_cols.empty() || aux_table.empty() || right_delta_source.empty() ||
	    right_last_update.empty()) {
		throw InternalException("CompileSemiAntiRecompute called with incomplete metadata for view '%s'", view_name);
	}

	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	bool has_left_delta = !left_delta_source.empty() && !left_last_update.empty();
	string left_delta_q = DeltaSourceRef(left_delta_source, catalog_prefix);
	string right_delta_q = DeltaSourceRef(right_delta_source, catalog_prefix);
	string dleft_table = "openivm_saj_dleft_" + view_name;
	string dright_table = "openivm_saj_dright_" + view_name;
	string dright_stats_table = "openivm_saj_dright_stats_" + view_name;
	string right_stats_table = "openivm_saj_right_stats_" + view_name;
	string old_table = "openivm_saj_old_" + view_name;
	string aff_table = "openivm_saj_aff_" + view_name;
	bool null_aware_anti = null_aware && StringUtil::Lower(join_type) == "anti" && !null_aware_left_col.empty() &&
	                       !null_aware_right_expr.empty();
	string visible = SemiAntiVisibleExpr(join_type, null_aware_anti, null_aware_left_col, "");
	string cur_visible = SemiAntiVisibleExpr(join_type, null_aware_anti, null_aware_left_col, "_cur");
	string left_delta_filter = post_filter.empty() ? "" : " AND (" + post_filter + ")";
	string match_predicate = predicate + (right_filter.empty() ? "" : " AND (" + right_filter + ")");
	string right_delta_filter = right_filter.empty() ? "" : " AND (" + right_filter + ")";
	string right_filter_sql = right_filter.empty() ? "" : " WHERE " + right_filter;
	bool correlated_right_filter = null_aware_anti && ReferencesQualifiedAlias(right_filter, left_alias);

	string left_cols_csv = SqlUtils::JoinQuotedColumns(left_cols);
	string output_cols_csv = SqlUtils::JoinQuotedColumns(output_cols);
	string left_cols_i = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "i");
	string left_cols_l = SqlUtils::JoinQualifiedQuotedColumns(left_cols, left_alias);
	string left_cols_old = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "_old");
	string left_cols_cur = SqlUtils::JoinQualifiedQuotedColumns(left_cols, "_cur");
	string output_old = SqlUtils::JoinQualifiedQuotedColumns(output_cols, "_old");
	string output_cur = SqlUtils::JoinQualifiedQuotedColumns(output_cols, "_cur");

	string aux_i_match = SqlUtils::BuildNullSafeMatch(left_cols, "_aux", "i");
	string old_cur_match = SqlUtils::BuildNullSafeMatch(left_cols, "_old", "_cur");
	string aff_old_match = SqlUtils::BuildNullSafeMatch(left_cols, "_aff", "_old");
	string aff_cur_match = SqlUtils::BuildNullSafeMatch(left_cols, "_aff", "_cur");
	string data_match = SqlUtils::BuildNullSafeMatch(output_cols, "_v", "_d");
	string left_delta_select;
	string left_delta_group;
	BuildAliasedSourceLists(left_cols, left_exprs, left_delta_select, left_delta_group, left_alias);

	string left_ts =
	    string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(left_last_update) + "'::TIMESTAMP";
	string right_ts = right_alias + "." + string(openivm::TIMESTAMP_COL) + " >= '" +
	                  SqlUtils::EscapeValue(right_last_update) + "'::TIMESTAMP";

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(old_table) + " AS SELECT *, (" + visible +
	       ") AS _visible FROM " + aux_q + ";\n\n";

	if (has_left_delta) {
		sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dleft_table) + " AS\n  SELECT " +
		       left_delta_select + ", SUM(" + left_alias + "." + string(openivm::MULTIPLICITY_COL) +
		       ")::BIGINT AS dmult\n  FROM " + left_delta_q + " " + left_alias + "\n  WHERE " + left_alias + "." +
		       left_ts + left_delta_filter + "\n  GROUP BY " + left_delta_group + "\n  HAVING SUM(" + left_alias + "." +
		       string(openivm::MULTIPLICITY_COL) + ") <> 0;\n\n";
	} else {
		sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dleft_table) + " AS\n  SELECT " +
		       left_cols_csv + ", 0::BIGINT AS dmult FROM " + aux_q + " WHERE false;\n\n";
	}

	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dright_table) + " AS\n  SELECT " + left_cols_l +
	       ", SUM(" + right_alias + "." + string(openivm::MULTIPLICITY_COL) + ")::BIGINT AS dmatch\n  FROM " + aux_q +
	       " " + left_alias + " JOIN " + right_delta_q + " " + right_alias + " ON " + match_predicate + "\n  WHERE " +
	       right_ts + "\n  GROUP BY " + left_cols_l + "\n  HAVING SUM(" + right_alias + "." +
	       string(openivm::MULTIPLICITY_COL) + ") <> 0;\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dright_table) + " _d ON " +
	       SqlUtils::BuildNullSafeMatch(left_cols, "_aux", "_d") +
	       "\nWHEN MATCHED THEN UPDATE SET _match_count = _aux._match_count + _d.dmatch;\n\n";

	if (null_aware_anti) {
		if (correlated_right_filter) {
			sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dright_stats_table) + " AS\n  SELECT " +
			       left_cols_l + ", SUM(" + right_alias + "." + string(openivm::MULTIPLICITY_COL) +
			       ")::BIGINT AS d_right_count, SUM(CASE WHEN " + null_aware_right_expr + " IS NULL THEN " +
			       right_alias + "." + string(openivm::MULTIPLICITY_COL) +
			       " ELSE 0 END)::BIGINT AS d_right_null_count\n  FROM " + aux_q + " " + left_alias + " JOIN " +
			       right_delta_q + " " + right_alias + " ON " + right_filter + "\n  WHERE " + right_ts +
			       "\n  GROUP BY " + left_cols_l + "\n  HAVING SUM(" + right_alias + "." +
			       string(openivm::MULTIPLICITY_COL) + ") <> 0 OR SUM(CASE WHEN " + null_aware_right_expr +
			       " IS NULL THEN " + right_alias + "." + string(openivm::MULTIPLICITY_COL) + " ELSE 0 END) <> 0;\n\n";
			sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dright_stats_table) + " _d ON " +
			       SqlUtils::BuildNullSafeMatch(left_cols, "_aux", "_d") +
			       "\nWHEN MATCHED THEN UPDATE SET _right_count = _aux._right_count + _d.d_right_count, "
			       "_right_null_count = _aux._right_null_count + _d.d_right_null_count;\n\n";
		} else {
			sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dright_stats_table) +
			       " AS\n  SELECT COALESCE(SUM(" + right_alias + "." + string(openivm::MULTIPLICITY_COL) +
			       "), 0)::BIGINT AS d_right_count, COALESCE(SUM(CASE WHEN " + null_aware_right_expr +
			       " IS NULL THEN " + right_alias + "." + string(openivm::MULTIPLICITY_COL) +
			       " ELSE 0 END), 0)::BIGINT AS d_right_null_count\n  FROM " + right_delta_q + " " + right_alias +
			       "\n  WHERE " + right_ts + right_delta_filter + ";\n\n";
			sql += "UPDATE " + aux_q + " SET _right_count = _right_count + (SELECT d_right_count FROM " +
			       SqlUtils::QuoteIdentifier(dright_stats_table) +
			       "), _right_null_count = _right_null_count + (SELECT d_right_null_count FROM " +
			       SqlUtils::QuoteIdentifier(dright_stats_table) + ") WHERE EXISTS (SELECT 1 FROM " +
			       SqlUtils::QuoteIdentifier(dright_stats_table) +
			       " WHERE d_right_count <> 0 OR d_right_null_count <> 0);\n\n";
			sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(right_stats_table) +
			       " AS\n  SELECT count(*)::BIGINT AS _right_count, count(*) FILTER (WHERE " + null_aware_right_expr +
			       " IS NULL)::BIGINT AS _right_null_count FROM " + right_table + " " + right_alias + right_filter_sql +
			       ";\n\n";
		}
	}

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dleft_table) + " i ON " + aux_i_match +
	       "\nWHEN MATCHED THEN UPDATE SET _left_count = _aux._left_count + i.dmult;\n\n";

	string insert_cols = left_cols_csv + ", _left_count, _match_count";
	string insert_stats_select;
	string insert_stats_from;
	if (null_aware_anti) {
		insert_cols += ", _right_count, _right_null_count";
		if (correlated_right_filter) {
			string right_filter_i = RewriteQualifiedAliasPrefix(right_filter, left_alias, "i");
			insert_stats_select = ", (SELECT count(*)::BIGINT FROM " + right_table + " " + right_alias + " WHERE " +
			                      right_filter_i + "), (SELECT count(*) FILTER (WHERE " + null_aware_right_expr +
			                      " IS NULL)::BIGINT FROM " + right_table + " " + right_alias + " WHERE " +
			                      right_filter_i + ")";
		} else {
			insert_stats_select = ", rs._right_count, rs._right_null_count";
			insert_stats_from = "\nCROSS JOIN " + SqlUtils::QuoteIdentifier(right_stats_table) + " rs";
		}
	}
	sql += "INSERT INTO " + aux_q + " (" + insert_cols + ")\nSELECT " + left_cols_i +
	       ", i.dmult, COALESCE(mc._match_count, 0)::BIGINT" + insert_stats_select + "\nFROM " +
	       SqlUtils::QuoteIdentifier(dleft_table) + " i\nLEFT JOIN " + aux_q + " _aux ON " + aux_i_match +
	       "\nLEFT JOIN (\n  SELECT " + left_cols_l + ", COUNT(*)::BIGINT AS _match_count\n  FROM " +
	       SqlUtils::QuoteIdentifier(dleft_table) + " " + left_alias + " JOIN " + right_table + " " + right_alias +
	       " ON " + match_predicate + "\n  GROUP BY " + left_cols_l + "\n) mc ON " +
	       SqlUtils::BuildNullSafeMatch(left_cols, "mc", "i") + insert_stats_from +
	       "\nWHERE _aux._left_count IS NULL AND i.dmult > 0;\n\n";

	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(aff_table) + " AS\nSELECT " + left_cols_old +
	       " FROM " + SqlUtils::QuoteIdentifier(old_table) + " _old LEFT JOIN " + aux_q + " _cur ON " + old_cur_match +
	       "\nWHERE _cur._left_count IS NULL OR _old._left_count IS DISTINCT FROM _cur._left_count OR "
	       "_old._visible IS DISTINCT FROM (" +
	       cur_visible + ")\nUNION\nSELECT " + left_cols_cur + " FROM " + aux_q + " _cur LEFT JOIN " +
	       SqlUtils::QuoteIdentifier(old_table) + " _old ON " + old_cur_match +
	       "\nWHERE _old._left_count IS NULL OR _old._left_count IS DISTINCT FROM _cur._left_count OR "
	       "_old._visible IS DISTINCT FROM (" +
	       cur_visible + ");\n\n";

	sql += "WITH _old_rows AS (\n  SELECT " + output_old + " FROM " + SqlUtils::QuoteIdentifier(old_table) +
	       " _old JOIN " + SqlUtils::QuoteIdentifier(aff_table) + " _aff ON " + aff_old_match +
	       ", generate_series(1, _old._left_count::BIGINT)\n  WHERE _old._visible AND _old._left_count > 0" +
	       "\n), "
	       "_net AS (\n  SELECT " +
	       output_cols_csv + ", COUNT(*)::BIGINT AS _cnt FROM _old_rows GROUP BY " + output_cols_csv +
	       "\n)\nDELETE FROM " + data_table + " WHERE rowid IN (\n  SELECT _v.rowid FROM (\n    SELECT rowid, " +
	       output_cols_csv + ", ROW_NUMBER() OVER (PARTITION BY " + output_cols_csv + " ORDER BY rowid) AS _rn FROM " +
	       data_table + "\n  ) _v JOIN _net _d ON " + data_match + " WHERE _v._rn <= _d._cnt\n);\n\n";

	sql += "INSERT INTO " + data_table + " SELECT " + output_cur + "\nFROM " + aux_q + " _cur JOIN " +
	       SqlUtils::QuoteIdentifier(aff_table) + " _aff ON " + aff_cur_match +
	       ", generate_series(1, _cur._left_count::BIGINT)\nWHERE " + cur_visible + " AND _cur._left_count > 0;\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE _left_count <= 0;\n";
	sql += "DROP TABLE IF EXISTS " + SqlUtils::QuoteIdentifier(old_table) + ";\nDROP TABLE IF EXISTS " +
	       SqlUtils::QuoteIdentifier(dleft_table) + ";\nDROP TABLE IF EXISTS " +
	       SqlUtils::QuoteIdentifier(dright_table) + ";\nDROP TABLE IF EXISTS " +
	       SqlUtils::QuoteIdentifier(dright_stats_table) + ";\nDROP TABLE IF EXISTS " +
	       SqlUtils::QuoteIdentifier(right_stats_table) + ";\nDROP TABLE IF EXISTS " +
	       SqlUtils::QuoteIdentifier(aff_table) + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileSemiAntiRecompute] %s join, %zu left cols, aux=%s\n", join_type.c_str(),
	                    left_cols.size(), aux_table.c_str());
	return sql;
}

string BuildFilteredGroupCountAuxStateCreateSQL(const string &target_table, const string &source_table,
                                                const string &group_col, const string &sum_col,
                                                const string &source_group_expr, const string &source_sum_expr,
                                                bool replace) {
	string group_q = SqlUtils::QuoteIdentifier(group_col);
	string sum_q = SqlUtils::QuoteIdentifier(sum_col);
	string group_expr = source_group_expr.empty() ? group_q : source_group_expr;
	string sum_expr = source_sum_expr.empty() ? sum_q : source_sum_expr;
	return CreateAuxTablePrefix(target_table, replace) + " AS SELECT " + group_expr + " AS " + group_q + ", sum(" +
	       sum_expr + ") AS openivm_sum FROM " + source_table + " GROUP BY " + group_expr;
}

string CompileFilteredGroupCount(const string &view_name, const string &aux_table, const string &delta_source,
                                 const string &last_update, const string &group_col, const string &sum_col,
                                 const string &source_group_expr, const string &source_sum_expr,
                                 const string &output_col, const string &comparison_op, const string &threshold_sql,
                                 const string &catalog_prefix) {
	if (aux_table.empty() || delta_source.empty() || last_update.empty() || group_col.empty() || sum_col.empty() ||
	    output_col.empty() || comparison_op.empty() || threshold_sql.empty()) {
		throw InternalException("CompileFilteredGroupCount called with incomplete metadata for view '%s'", view_name);
	}

	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + SqlUtils::QuoteIdentifier(aux_table);
	string delta_q = DeltaSourceRef(delta_source, catalog_prefix);
	string dsum_table = "openivm_fgc_delta_" + view_name;
	string group_q = SqlUtils::QuoteIdentifier(group_col);
	string sum_q = SqlUtils::QuoteIdentifier(sum_col);
	string source_group = source_group_expr.empty() ? group_q : source_group_expr;
	string source_sum = source_sum_expr.empty() ? sum_q : source_sum_expr;
	string output_q = SqlUtils::QuoteIdentifier(output_col);
	string dsum_expr = "SUM(" + string(openivm::MULTIPLICITY_COL) + " * " + source_sum + ")";
	string old_sum = "COALESCE(_aux.openivm_sum, 0)";
	string new_sum = "(" + old_sum + " + d.openivm_delta_sum)";
	string old_visible = "CASE WHEN " + old_sum + " " + comparison_op + " " + threshold_sql + " THEN 1 ELSE 0 END";
	string new_visible = "CASE WHEN " + new_sum + " " + comparison_op + " " + threshold_sql + " THEN 1 ELSE 0 END";
	string aux_match = SqlUtils::BuildNullSafeMatch(vector<string> {group_col}, "_aux", "d");

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(dsum_table) + " AS\n  SELECT " + source_group +
	       " AS " + group_q + ", " + dsum_expr + " AS openivm_delta_sum\n  FROM " + delta_q + "\n  WHERE " +
	       string(openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP\n  GROUP BY " +
	       source_group + "\n  HAVING " + dsum_expr + " <> 0;\n\n";

	sql += "WITH openivm_transition AS (\n  SELECT SUM((" + new_visible + ") - (" + old_visible +
	       ")) AS openivm_delta_count\n  FROM " + SqlUtils::QuoteIdentifier(dsum_table) + " d LEFT JOIN " + aux_q +
	       " _aux ON " + aux_match + "\n)\nUPDATE " + data_table + " SET " + output_q + " = COALESCE(" + output_q +
	       ", 0) + COALESCE((SELECT openivm_delta_count FROM openivm_transition), 0);\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + SqlUtils::QuoteIdentifier(dsum_table) + " d ON " + aux_match +
	       "\nWHEN MATCHED THEN UPDATE SET openivm_sum = COALESCE(_aux.openivm_sum, 0) + d.openivm_delta_sum\n"
	       "WHEN NOT MATCHED THEN INSERT (" +
	       group_q + ", openivm_sum) VALUES (d." + group_q + ", d.openivm_delta_sum);\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE openivm_sum = 0;\n";
	sql += "DROP TABLE IF EXISTS " + SqlUtils::QuoteIdentifier(dsum_table) + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileFilteredGroupCount] group=%s, sum=%s, op=%s, aux=%s\n", group_col.c_str(),
	                    sum_col.c_str(), comparison_op.c_str(), aux_table.c_str());
	return sql;
}

string CompileWindowRecompute(const string &view_name, const string &view_query_sql, const string &delta_ts_filter,
                              const string &catalog_prefix, const vector<string> &partition_columns,
                              const vector<WindowPartitionDeltaSpec> &partition_delta_specs, bool emit_cascade_delta,
                              const string &affected_keys_sql, const vector<string> &column_names,
                              bool running_window_incremental) {
	bool have_affected_keys = !affected_keys_sql.empty();
	if (!have_affected_keys && (partition_columns.empty() || partition_delta_specs.empty())) {
		// No PARTITION BY (global surrogate-key window) or no partition key resolvable in any
		// source delta table → nothing to scope the recompute to. Keep the cascade delta the
		// caller asked for so downstream MVs stay incremental.
		return emit_cascade_delta ? CompileFullRecomputeWithCascadeDelta(view_name, view_query_sql, catalog_prefix)
		                          : CompileFullRecompute(view_name, view_query_sql, catalog_prefix);
	}
	if (running_window_incremental) {
		auto suffix_sql = BuildRunningWindowSuffixRefreshSQL(view_name, view_query_sql, delta_ts_filter, catalog_prefix,
		                                                     partition_columns, partition_delta_specs, column_names,
		                                                     emit_cascade_delta);
		if (!suffix_sql.empty()) {
			return suffix_sql;
		}
	}
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
	string affected_temp_table = SqlUtils::QuoteIdentifier("openivm_affected_" + view_name);
	auto output_columns = PartitionOutputColumns(partition_columns);

	OPENIVM_DEBUG_PRINT(
	    "[CompileWindowRecompute] Partition columns: %zu, delta specs: %zu, lineage keys: %s, cascade delta: %s\n",
	    partition_columns.size(), partition_delta_specs.size(), have_affected_keys ? "yes" : "no",
	    emit_cascade_delta ? "enabled" : "disabled");
	if (!emit_cascade_delta) {
		if (!have_affected_keys) {
			string target_filter = BuildDeltaAffectedFilter(partition_delta_specs, delta_where, "openivm_target");
			string recompute_filter = BuildDeltaAffectedFilter(partition_delta_specs, delta_where, "openivm_recompute");
			return "DELETE FROM " + data_table + " AS openivm_target WHERE " + target_filter + ";\n" + "INSERT INTO " +
			       data_table + "\nSELECT * FROM (" + view_query_sql + ") openivm_recompute\nWHERE " +
			       recompute_filter + ";\n";
		}
		string target_match = SqlUtils::BuildNullSafeMatch(output_columns, "openivm_aff", "openivm_target");
		string recompute_match = SqlUtils::BuildNullSafeMatch(output_columns, "openivm_aff", "openivm_recompute");
		return BuildAffectedKeyRefreshSQL(data_table, view_query_sql, affected_keys_sql, "openivm_target",
		                                  "openivm_recompute", "openivm_aff", target_match, recompute_match,
		                                  affected_temp_table);
	}

	string delta_table = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
	string old_temp_table = SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + view_name);
	string new_temp_table = SqlUtils::QuoteIdentifier(string("openivm_new_") + view_name);
	string sql;
	if (have_affected_keys) {
		sql += "CREATE OR REPLACE TEMP TABLE " + affected_temp_table + " AS\n" + affected_keys_sql + ";\n\n";
	}
	string old_filter = have_affected_keys
	                        ? BuildAffectedTableFilter(output_columns, "openivm_old", affected_temp_table)
	                        : BuildDeltaAffectedFilter(partition_delta_specs, delta_where, "openivm_old");
	string recompute_filter = have_affected_keys
	                              ? BuildAffectedTableFilter(output_columns, "openivm_recompute", affected_temp_table)
	                              : BuildDeltaAffectedFilter(partition_delta_specs, delta_where, "openivm_recompute");
	string target_filter = have_affected_keys
	                           ? BuildAffectedTableFilter(output_columns, "openivm_target", affected_temp_table)
	                           : BuildDeltaAffectedFilter(partition_delta_specs, delta_where, "openivm_target");
	sql += "CREATE OR REPLACE TEMP TABLE " + old_temp_table + " AS\nSELECT * FROM " + data_table +
	       " openivm_old\nWHERE " + old_filter + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + new_temp_table + " AS\nSELECT * FROM (" + view_query_sql +
	       ") openivm_recompute\nWHERE " + recompute_filter + ";\n\n";
	sql += "DELETE FROM " + data_table + " AS openivm_target WHERE " + target_filter + ";\n";
	sql += "INSERT INTO " + data_table + "\nSELECT * FROM " + new_temp_table + ";\n";
	sql += "\n" + BuildSignedMultisetDeltaInsertSQL(delta_table, old_temp_table, new_temp_table);
	if (have_affected_keys) {
		sql += "\nDROP TABLE IF EXISTS " + affected_temp_table + ";\n";
	}
	sql += "DROP TABLE IF EXISTS " + old_temp_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + new_temp_table + ";\n";
	return sql;
}

} // namespace duckdb
