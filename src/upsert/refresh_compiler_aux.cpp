#include "upsert/refresh_compiler.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_internal.hpp"

#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_expression_iterator.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"
#include "duckdb/parser/expression/window_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"

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

static string PartitionOutputColumn(const string &input) {
	auto pos = input.find('=');
	// Metadata stores a column name, not a qualified SQL identifier.
	return pos == string::npos ? input : input.substr(0, pos);
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

struct RunningWindowExpr {
	string function_name;
	string argument;
	string output_column;
	bool rows_frame = false;
	bool nulls_first = false;
	string position_column;
};

struct RunningDerivedExpr {
	string output_column;
	string expression;
};

struct RunningWindowPlan {
	string partition_column;
	string order_column;
	vector<string> output_columns;
	vector<pair<string, string>> passthrough_columns;
	vector<RunningWindowExpr> window_exprs;
	vector<RunningDerivedExpr> derived_exprs;
	vector<string> position_columns;
	vector<string> source_columns;
	string suffix_position = "openivm_rows_position_suffix";
	bool nulls_first = false;
};

static string RunningColumn(const ParsedExpression &expr) {
	return expr.GetExpressionClass() == ExpressionClass::COLUMN_REF ? expr.Cast<ColumnRefExpression>().GetColumnName()
	                                                                : "";
}

static bool ParseRunningWindowExpression(const ParsedExpression &expression, RunningWindowExpr &out,
                                         string &partition_col, string &order_col) {
	if (expression.GetExpressionClass() != ExpressionClass::WINDOW) {
		return false;
	}
	auto &window = expression.Cast<WindowExpression>();
	auto function = StringUtil::Lower(window.function_name);
	if (function == "count_star") {
		function = "count";
	}
	if ((function != "sum" && function != "min" && function != "max" && function != "count" && function != "avg" &&
	     function != "row_number") ||
	    window.partitions.size() != 1 || window.orders.size() != 1 || window.children.size() > 1 ||
	    window.filter_expr || window.distinct || !window.arg_orders.empty() ||
	    window.exclude_clause != WindowExcludeMode::NO_OTHER || window.start != WindowBoundary::UNBOUNDED_PRECEDING ||
	    (window.end != WindowBoundary::CURRENT_ROW_RANGE && window.end != WindowBoundary::CURRENT_ROW_ROWS) ||
	    window.orders[0].type == OrderType::DESCENDING) {
		return false;
	}
	auto partition = RunningColumn(*window.partitions[0]);
	auto order = RunningColumn(*window.orders[0].expression);
	if (partition.empty() || order.empty() ||
	    (!partition_col.empty() && !StringUtil::CIEquals(partition_col, partition)) ||
	    (!order_col.empty() && !StringUtil::CIEquals(order_col, order))) {
		return false;
	}
	out.function_name = function;
	out.rows_frame = window.end == WindowBoundary::CURRENT_ROW_ROWS;
	out.nulls_first = window.orders[0].null_order == OrderByNullType::NULLS_FIRST;
	out.argument = window.children.empty() && (function == "count" || function == "row_number") ? "*"
	               : window.children.empty()                                                    ? ""
	                                         : RunningColumn(*window.children[0]);
	out.output_column = expression.GetName();
	partition_col = partition;
	order_col = order;
	return !out.argument.empty();
}

static unique_ptr<SelectStatement> ParseRunningSelect(const string &sql) {
	Parser parser;
	try {
		parser.ParseQuery(sql);
	} catch (const ParserException &) {
		return nullptr;
	}
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		return nullptr;
	}
	return unique_ptr_cast<SQLStatement, SelectStatement>(std::move(parser.statements[0]));
}

static SelectNode *RunningSelect(QueryNode &node) {
	if (node.type != QueryNodeType::SELECT_NODE) {
		return nullptr;
	}
	auto &select = node.Cast<SelectNode>();
	if (select.where_clause || select.having || select.qualify || select.sample ||
	    !select.groups.group_expressions.empty() || !select.modifiers.empty() || !select.from_table ||
	    select.from_table->type != TableReferenceType::BASE_TABLE) {
		return nullptr;
	}
	return &select;
}

static string RefreshExpressionSQL(ParsedExpression &expression) {
	// Resolve builtin cast names so portable SQL uses DATE rather than an unresolved quoted type name.
	ParsedExpressionIterator::VisitExpressionMutable<CastExpression>(
	    expression, [&](CastExpression &cast) { cast.cast_type = UnboundType::TryDefaultBind(cast.cast_type); });
	return expression.ToString();
}

static string TranslateExpressionIdentifiers(const ParsedExpression &expression,
                                             const std::map<string, string> &alias_to_output) {
	auto copy = expression.Copy();
	bool resolved = true;
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(*copy, [&](ColumnRefExpression &column) {
		auto found = alias_to_output.find(StringUtil::Lower(column.GetColumnName()));
		if (found == alias_to_output.end()) {
			resolved = false;
		} else {
			column.column_names = {found->second};
		}
	});
	return resolved ? RefreshExpressionSQL(*copy) : "";
}

static bool TryParseRunningWindowPlan(SelectNode &select, const vector<string> &partition_columns,
                                      const vector<string> &column_names, RunningWindowPlan &plan) {
	if (partition_columns.size() != 1 || !select.cte_map.map.empty()) {
		return false;
	}
	plan.partition_column = PartitionOutputColumn(partition_columns[0]);
	for (auto &item : select.select_list) {
		if (item->GetExpressionClass() == ExpressionClass::WINDOW) {
			RunningWindowExpr expr;
			if (!ParseRunningWindowExpression(*item, expr, plan.partition_column, plan.order_column)) {
				return false;
			}
			if (expr.rows_frame || expr.function_name == "row_number" ||
			    (!plan.window_exprs.empty() && plan.nulls_first != expr.nulls_first)) {
				return false;
			}
			plan.nulls_first = expr.nulls_first;
			plan.window_exprs.push_back(std::move(expr));
		} else {
			auto source = RunningColumn(*item);
			if (source.empty()) {
				return false;
			}
			plan.passthrough_columns.emplace_back(source, item->GetName());
		}
	}
	for (auto &column : column_names) {
		bool found = false;
		for (auto &item : select.select_list) {
			found = found || StringUtil::CIEquals(column, item->GetName());
		}
		if (!found) {
			return false;
		}
	}
	plan.output_columns = column_names;
	return !plan.window_exprs.empty();
}

static bool TryParseLptsRunningWindowPlan(SelectNode &select, const vector<string> &partition_columns,
                                          const vector<string> &column_names, RunningWindowPlan &plan) {
	if (select.cte_map.map.empty()) {
		return false;
	}
	auto &scan_cte = *select.cte_map.map.begin()->second;
	auto *scan = RunningSelect(*scan_cte.query->node);
	if (!scan || scan_cte.aliases.size() != scan->select_list.size()) {
		return false;
	}
	std::map<string, string> alias_to_source;
	std::map<string, string> alias_to_output;
	for (idx_t i = 0; i < scan_cte.aliases.size(); i++) {
		auto source = RunningColumn(*scan->select_list[i]);
		if (source.empty()) {
			return false;
		}
		auto alias = StringUtil::Lower(scan_cte.aliases[i]);
		plan.source_columns.push_back(source);
		alias_to_source[alias] = source;
		alias_to_output[alias] = source;
	}
	vector<string> output_names;
	for (auto &item : select.select_list) {
		if (RunningColumn(*item).empty()) {
			output_names = column_names;
			break;
		}
		output_names.push_back(item->GetName());
	}
	plan.output_columns = output_names;
	// Resolve final output names through passthrough CTEs once. Window outputs need
	// not appear in function order, and hidden positions are appended at the end.
	std::map<string, string> final_names;
	for (auto &item : select.select_list) {
		final_names[StringUtil::Lower(RunningColumn(*item))] = item->GetName();
	}
	vector<CommonTableExpressionInfo *> ctes;
	for (auto &entry : select.cte_map.map) {
		ctes.push_back(entry.second.get());
	}
	for (auto it = ctes.rbegin(); it != ctes.rend(); ++it) {
		auto *body = RunningSelect(*(*it)->query->node);
		if (!body || (*it)->aliases.size() != body->select_list.size()) {
			return false;
		}
		for (idx_t i = 0; i < body->select_list.size(); i++) {
			auto target = final_names.find(StringUtil::Lower((*it)->aliases[i]));
			auto source = RunningColumn(*body->select_list[i]);
			if (target != final_names.end() && !source.empty()) {
				final_names.emplace(StringUtil::Lower(source), target->second);
			}
		}
	}
	for (auto &source : alias_to_source) {
		auto output = final_names.find(source.first);
		if (output != final_names.end()) {
			plan.passthrough_columns.emplace_back(source.second, output->second);
		}
	}
	auto window_count = output_names.size() - plan.passthrough_columns.size();
	idx_t window_idx = 0;
	string expected_partition = partition_columns.empty() ? "" : PartitionOutputColumn(partition_columns[0]);
	string parsed_partition;
	string parsed_order;
	for (auto &entry : select.cte_map.map) {
		auto &cte = *entry.second;
		auto *body = RunningSelect(*cte.query->node);
		if (!body || cte.aliases.size() != body->select_list.size()) {
			return false;
		}
		auto &cte_aliases = cte.aliases;
		auto &cte_items = body->select_list;
		std::map<string, string> next_alias_to_output;
		idx_t first_window = plan.window_exprs.size();
		string position_column;
		for (idx_t item_idx = 0; item_idx < cte_items.size(); item_idx++) {
			auto &item = cte_items[item_idx];
			string output_alias = cte_aliases[item_idx];
			string output_key = StringUtil::Lower(output_alias);
			if (item->GetExpressionClass() != ExpressionClass::WINDOW) {
				string source_key = StringUtil::Lower(RunningColumn(*item));
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
				if (item->GetExpressionClass() == ExpressionClass::CASE) {
					RunningDerivedExpr derived;
					derived.output_column = output_alias;
					derived.expression = TranslateExpressionIdentifiers(*item, alias_to_output);
					if (derived.expression.empty()) {
						return false;
					}
					plan.derived_exprs.push_back(derived);
					next_alias_to_output[output_key] = derived.output_column;
					continue;
				}
				continue;
			}
			if (window_idx >= window_count) {
				return false;
			}
			RunningWindowExpr expr;
			string expr_partition;
			string expr_order;
			if (!ParseRunningWindowExpression(*item, expr, expr_partition, expr_order)) {
				return false;
			}
			auto translate = [&](const string &alias) -> string {
				auto found = alias_to_output.find(StringUtil::Lower(alias));
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
			auto output = final_names.find(output_key);
			if (output == final_names.end()) {
				return false;
			}
			expr.output_column = output->second;
			window_idx++;
			if (window_idx > 1 && plan.nulls_first != expr.nulls_first) {
				return false;
			}
			plan.nulls_first = expr.nulls_first;
			if (expr.function_name == "row_number") {
				if (!StringUtil::StartsWith(expr.output_column, openivm::ROWS_POSITION_PREFIX)) {
					return false;
				}
				position_column = expr.output_column;
				plan.position_columns.push_back(position_column);
			} else {
				plan.window_exprs.push_back(expr);
			}
			next_alias_to_output[output_key] = expr.output_column;
		}
		for (idx_t i = first_window; i < plan.window_exprs.size(); i++) {
			if (plan.window_exprs[i].rows_frame) {
				if (position_column.empty()) {
					return false;
				}
				plan.window_exprs[i].position_column = position_column;
			}
		}
		alias_to_output = std::move(next_alias_to_output);
	}
	while (std::any_of(plan.source_columns.begin(), plan.source_columns.end(),
	                   [&](const string &name) { return StringUtil::CIEquals(name, plan.suffix_position); })) {
		plan.suffix_position += "_";
	}
	plan.partition_column = parsed_partition;
	plan.order_column = parsed_order;
	return !plan.window_exprs.empty() && window_idx == window_count && partition_columns.size() == 1;
}

static string QualifiedColumn(const string &alias, const string &column) {
	return alias + "." + SqlUtils::QuoteIdentifier(column);
}

static bool IsRunningDerivedArgument(const RunningWindowExpr &expr, const RunningWindowPlan &plan) {
	for (auto &derived : plan.derived_exprs) {
		if (StringUtil::CIEquals(expr.argument, derived.output_column)) {
			return true;
		}
	}
	return false;
}

static string RunningStoredColumn(const RunningWindowPlan &plan, const string &source) {
	for (auto &column : plan.passthrough_columns) {
		if (StringUtil::CIEquals(column.first, source)) {
			return column.second;
		}
	}
	return "";
}

static string RunningSeedColumn(const RunningWindowExpr &expr) {
	return "openivm_seed_" + expr.output_column;
}

static string RunningWindowClause(const RunningWindowExpr &expr, const RunningWindowPlan &plan, const string &alias) {
	string order = QualifiedColumn(alias, plan.order_column) + (plan.nulls_first ? " NULLS FIRST" : " NULLS LAST");
	if (expr.rows_frame) {
		order += ", " + QualifiedColumn(alias, plan.suffix_position);
	}
	return " OVER (PARTITION BY " + QualifiedColumn(alias, plan.partition_column) + " ORDER BY " + order +
	       (expr.rows_frame ? " ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)" : ")");
}

static string RunningLocalExprFromAlias(const RunningWindowExpr &expr, const RunningWindowPlan &plan,
                                        const string &alias) {
	string arg = expr.argument == "*" ? "*" : QualifiedColumn(alias, expr.argument);
	return StringUtil::Upper(expr.function_name) + "(" + arg + ")" + RunningWindowClause(expr, plan, alias);
}

static string RunningAvgPriorCountColumn(const RunningWindowExpr &expr) {
	return "openivm_running_count_" + expr.output_column;
}

static string RunningAdjustedExprWithSeed(const RunningWindowExpr &expr, const string &local, const string &state_col) {
	if (expr.function_name == "sum") {
		return "CASE WHEN " + state_col + " IS NULL THEN " + local + " ELSE " + state_col + " + COALESCE(" + local +
		       ", 0) END";
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

static string RunningAdjustedExpr(const RunningWindowExpr &expr, const RunningWindowPlan &plan) {
	string local = RunningLocalExprFromAlias(expr, plan, "d");
	string state_col = QualifiedColumn("s", expr.output_column);
	if (expr.function_name == "avg" && expr.argument != "*") {
		string over = RunningWindowClause(expr, plan, "d");
		string sum_local = "SUM(" + QualifiedColumn("d", expr.argument) + ")" + over;
		string count_local = "COUNT(" + QualifiedColumn("d", expr.argument) + ")" + over;
		string prior_count_col = QualifiedColumn("s", RunningAvgPriorCountColumn(expr));
		string prior_count = "COALESCE(" + prior_count_col + ", 0)";
		return "(COALESCE(" + state_col + ", 0) + COALESCE(" + sum_local + ", 0)) / NULLIF(" + prior_count + " + " +
		       count_local + ", 0)";
	}
	return RunningAdjustedExprWithSeed(expr, local, state_col);
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
	auto statement = ParseRunningSelect(view_query_sql);
	auto *select = statement ? RunningSelect(*statement->node) : nullptr;
	if (!select) {
		return "";
	}
	RunningWindowPlan plan;
	if (!TryParseRunningWindowPlan(*select, partition_columns, visible_column_names, plan)) {
		plan = RunningWindowPlan();
		if (!TryParseLptsRunningWindowPlan(*select, partition_columns, visible_column_names, plan)) {
			return "";
		}
	}
	auto stored_order = RunningStoredColumn(plan, plan.order_column);
	if (stored_order.empty()) {
		return "";
	}
	for (auto &expr : plan.window_exprs) {
		if (expr.function_name == "avg" && RunningStoredColumn(plan, expr.argument).empty()) {
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
	string key_match_d_fk = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "d", "fk");
	string affected_data_filter =
	    BuildAffectedTableFilter(vector<string> {plan.partition_column}, "dt", affected_table);

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + affected_table + " AS\nSELECT DISTINCT " +
	       QualifiedColumn("d", plan.partition_column) + " AS " + part_q + "\nFROM " + delta_q + " d\nWHERE " +
	       delta_positive + ";\n\n";
	// One scan supplies both append bounds and seeds. AVG retains its exact sum
	// and count instead of reconstructing a sum from an already rounded mean.
	string order = QualifiedColumn("dt", stored_order);
	string seeds = "dt." + part_q + ", MAX(" + order +
	               ") AS openivm_running_max, "
	               "COUNT(*) AS openivm_running_count, COUNT(*)-COUNT(" +
	               order + ") AS openivm_running_nulls";
	bool rows_only = true;
	for (auto &expr : plan.window_exprs) {
		rows_only = rows_only && expr.rows_frame;
		string value = QualifiedColumn("dt", expr.output_column);
		string seed;
		if (expr.function_name == "avg") {
			auto argument = QualifiedColumn("dt", RunningStoredColumn(plan, expr.argument));
			seed = "SUM(" + argument + ")";
			seeds += ", COUNT(" + argument + ") AS " + SqlUtils::QuoteIdentifier(RunningAvgPriorCountColumn(expr));
		} else if (expr.rows_frame) {
			seed = "MAX_BY(" + value + ", " + QualifiedColumn("dt", expr.position_column) + ")";
		} else {
			// RANGE peers have identical cumulative outputs, including NULL peers.
			auto nonnull = "MAX_BY(" + value + ", " + order + ")";
			auto nulls = "MAX(CASE WHEN " + order + " IS NULL THEN " + value + " END)";
			seed = "COALESCE(" + (plan.nulls_first ? nonnull + ", " + nulls : nulls + ", " + nonnull) + ")";
		}
		seeds += ", " + seed + " AS " + SqlUtils::QuoteIdentifier(expr.output_column);
	}
	sql += "CREATE OR REPLACE TEMP TABLE " + state_table + " AS SELECT " + seeds + " FROM " + data_table +
	       " dt WHERE " + affected_data_filter + " GROUP BY dt." + part_q + ";\n\n";
	string old_null =
	    plan.nulls_first ? "m.openivm_running_nulls = m.openivm_running_count" : "m.openivm_running_nulls > 0";
	string new_null = plan.nulls_first ? "b.openivm_running_delta_nulls > 0"
	                                   : "b.openivm_running_delta_nulls = b.openivm_running_delta_count";
	string advances = "CASE WHEN m.openivm_running_count IS NULL THEN true WHEN (" + old_null + ") AND (" + new_null +
	                  ") THEN " + (rows_only ? "true" : "false") + " WHEN (" + old_null + ") <> (" + new_null +
	                  ") THEN " + (plan.nulls_first ? "(" + old_null + ")" : "(" + new_null + ")") +
	                  " ELSE b.openivm_running_delta_min_order " + (rows_only ? ">= " : "> ") +
	                  "m.openivm_running_max END";
	sql += "CREATE OR REPLACE TEMP TABLE " + bounds_table + " AS\nWITH delta_min AS (\n SELECT d." + part_q +
	       ", MIN(d." + order_q +
	       ") AS openivm_running_delta_min_order, COUNT(*) AS openivm_running_delta_count, "
	       "COUNT(*)-COUNT(d." +
	       order_q + ") AS openivm_running_delta_nulls FROM " + delta_q + " d WHERE " + delta_positive +
	       " GROUP BY d." + part_q + "\n) SELECT b." + part_q + ", " + advances +
	       " AS openivm_running_append FROM delta_min b LEFT JOIN " + state_table + " m ON b." + part_q +
	       " IS NOT DISTINCT FROM m." + part_q + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + fast_table + " AS SELECT " + part_q + " FROM " + bounds_table +
	       " WHERE openivm_running_append;\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + fallback_table + " AS SELECT " + part_q + " FROM " + bounds_table +
	       " WHERE NOT openivm_running_append;\n\n";
	string suffix_table;
	if (!plan.position_columns.empty()) {
		suffix_table = SqlUtils::QuoteIdentifier("openivm_run_suffix_" + view_name);
		string order = QualifiedColumn("d", plan.order_column) + (plan.nulls_first ? " NULLS FIRST" : " NULLS LAST");
		// Materialize positions once so repeated data/cascade statements use the
		// same peer order, independent of join or parallel scan scheduling.
		sql += "CREATE OR REPLACE TEMP TABLE " + suffix_table + " AS SELECT " +
		       SqlUtils::JoinQualifiedQuotedColumns(plan.source_columns, "d") + ", d." + openivm::MULTIPLICITY_COL +
		       ", d." + openivm::TIMESTAMP_COL +
		       ", "
		       "COALESCE(s.openivm_running_count,0) + ROW_NUMBER() OVER (PARTITION BY d." +
		       part_q + " ORDER BY " + order + ") AS " + SqlUtils::QuoteIdentifier(plan.suffix_position) + " FROM " +
		       delta_q + " d JOIN " + fast_table + " fk ON " + key_match_d_fk + " LEFT JOIN " + state_table +
		       " s ON d." + part_q + " IS NOT DISTINCT FROM s." + part_q + " WHERE " + delta_positive + ";\n\n";
		delta_q = suffix_table;
	}
	// Apply the partition restriction at the source, before blocking window operators.
	// The suffix parser proved that every window uses this same partition key.
	auto *scan =
	    select->cte_map.map.empty() ? select : RunningSelect(*select->cte_map.map.begin()->second->query->node);
	D_ASSERT(scan && !scan->where_clause);
	auto &source = scan->from_table->Cast<BaseTableRef>();
	auto source_alias = source.alias.empty() ? source.table_name : source.alias;
	auto predicate = "EXISTS (SELECT 1 FROM " + fallback_table + " openivm_aff WHERE " +
	                 QualifiedColumn("openivm_aff", plan.partition_column) + " IS NOT DISTINCT FROM " +
	                 QualifiedColumn(source_alias, spec.source_column) + ")";
	auto filters = Parser::ParseExpressionList(predicate);
	scan->where_clause = std::move(filters[0]);
	string fallback_query = statement->ToString();
	OPENIVM_DEBUG_PRINT("[CompileWindowSuffixExtend] Pushed fallback partition filter into source scan\n");
	string fallback_keys = "SELECT " + part_q + " FROM " + fallback_table;
	string fallback_target_match =
	    SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "openivm_aff", "openivm_target");
	string fallback_recompute_match =
	    SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "openivm_aff", "openivm_recompute");
	if (emit_cascade_delta) {
		sql += BuildSnapshotDeltaRefreshSQL(
		    data_table, fallback_query, delta_table, old_temp_table, new_temp_table,
		    BuildAffectedTableFilter(vector<string> {plan.partition_column}, "openivm_target", fallback_table));
	} else {
		sql +=
		    BuildAffectedKeyRefreshSQL(data_table, fallback_query, fallback_keys, "openivm_target", "openivm_recompute",
		                               "openivm_aff", fallback_target_match, fallback_recompute_match);
	}

	auto emit_column_names = plan.output_columns.empty() ? visible_column_names : plan.output_columns;
	string insert_cols = SqlUtils::JoinQuotedColumns(emit_column_names);
	auto append_output = [](string &list, const string &expression, const string &name) {
		if (!list.empty()) {
			list += ", ";
		}
		list += expression + " AS " + SqlUtils::QuoteIdentifier(name);
	};
	string suffix_query;
	if (!plan.derived_exprs.empty()) {
		vector<const RunningWindowExpr *> level1_exprs;
		vector<const RunningWindowExpr *> level3_exprs;
		for (auto &expr : plan.window_exprs) {
			if (IsRunningDerivedArgument(expr, plan)) {
				if (expr.function_name != "max") {
					return "";
				}
				level3_exprs.push_back(&expr);
			} else {
				level1_exprs.push_back(&expr);
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

		for (auto &pass : plan.passthrough_columns) {
			append_output(l1_select, QualifiedColumn("d", pass.first), pass.second);
		}
		for (auto *expr : level1_exprs) {
			append_output(l1_select, RunningAdjustedExpr(*expr, plan), expr->output_column);
		}
		for (auto *expr : level3_exprs) {
			append_output(l1_select, QualifiedColumn("s", expr->output_column), RunningSeedColumn(*expr));
		}
		if (!plan.position_columns.empty()) {
			append_output(l1_select, QualifiedColumn("d", plan.suffix_position), plan.suffix_position);
		}
		string lflags_select = "*";
		for (auto &derived : plan.derived_exprs) {
			lflags_select += ", " + derived.expression + " AS " + SqlUtils::QuoteIdentifier(derived.output_column);
		}
		string l3_select;

		for (auto &pass : plan.passthrough_columns) {
			append_output(l3_select, QualifiedColumn("f", pass.second), pass.second);
		}
		for (auto *expr : level1_exprs) {
			append_output(l3_select, QualifiedColumn("f", expr->output_column), expr->output_column);
		}
		for (auto *expr : level3_exprs) {
			string local = RunningLocalExprFromAlias(*expr, plan, "f");
			string adjusted = RunningAdjustedExprWithSeed(*expr, local, QualifiedColumn("f", RunningSeedColumn(*expr)));
			if (adjusted.empty()) {
				return "";
			}
			append_output(l3_select, adjusted, expr->output_column);
		}
		for (auto &position : plan.position_columns) {
			append_output(l3_select, QualifiedColumn("f", plan.suffix_position), position);
		}
		string final_select = SqlUtils::JoinQualifiedQuotedColumns(emit_column_names, "r");
		string state_match = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "d", "s");
		suffix_query = "WITH openivm_l1 AS (\n  SELECT " + l1_select + "\n  FROM " + delta_q + " d\n  JOIN " +
		               fast_table + " fk ON " + key_match_d_fk + "\n  LEFT JOIN " + state_table + " s ON " +
		               state_match + "\n  WHERE " + delta_positive + "\n), openivm_flags AS (\n  SELECT " +
		               lflags_select + "\n  FROM openivm_l1\n), openivm_l3 AS (\n  SELECT " + l3_select +
		               "\n  FROM openivm_flags f\n)\nSELECT " + final_select + "\nFROM openivm_l3 r";
	} else {
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
			for (auto &position : plan.position_columns) {
				if (StringUtil::CIEquals(emit_column_names[i], position)) {
					expr_sql = QualifiedColumn("d", plan.suffix_position);
				}
			}
			if (expr_sql.empty()) {
				return "";
			}
			select_list += expr_sql + " AS " + SqlUtils::QuoteIdentifier(emit_column_names[i]);
		}
		string state_match = SqlUtils::BuildNullSafeMatch(vector<string> {plan.partition_column}, "d", "s");
		suffix_query = "SELECT " + select_list + "\nFROM " + delta_q + " d\nJOIN " + fast_table + " fk ON " +
		               key_match_d_fk + "\nLEFT JOIN " + state_table + " s ON " + state_match + "\nWHERE " +
		               delta_positive;
	}
	// Materialize the computed suffix once, including its window calculations,
	// so data and cascade consumers share both the work and the chosen peer order.
	string result_table = SqlUtils::QuoteIdentifier("openivm_run_result_" + view_name);
	sql += "CREATE OR REPLACE TEMP TABLE " + result_table + " AS " + suffix_query + ";\n";
	sql += "INSERT INTO " + data_table + " (" + insert_cols + ") SELECT * FROM " + result_table + ";\n";
	if (emit_cascade_delta) {
		sql += "INSERT INTO " + delta_table + " SELECT *, CAST(1 AS INTEGER), CURRENT_TIMESTAMP FROM " + result_table +
		       ";\n";
	}
	for (auto &table :
	     {result_table, suffix_table, state_table, fallback_table, fast_table, bounds_table, affected_table}) {
		if (!table.empty()) {
			sql += "DROP TABLE IF EXISTS " + table + ";\n";
		}
	}
	OPENIVM_DEBUG_PRINT("[CompileWindowSuffixExtend] view=%s partition=%s order=%s window_exprs=%zu\n",
	                    view_name.c_str(), plan.partition_column.c_str(), plan.order_column.c_str(),
	                    plan.window_exprs.size());
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

static bool VisitQualifiedAlias(ParsedExpression &expression, const string &alias, const string *replacement) {
	bool found = false;
	ParsedExpressionIterator::VisitExpressionMutable<ColumnRefExpression>(expression, [&](ColumnRefExpression &column) {
		if (column.column_names.size() >= 2 && StringUtil::CIEquals(column.column_names[0], alias)) {
			found = true;
			if (replacement) {
				column.column_names[0] = *replacement;
			}
		}
	});
	return found;
}

static string RewriteQualifiedAliasPrefix(const string &expr, const string &source_alias, const string &target_alias) {
	auto expressions = Parser::ParseExpressionList(expr);
	D_ASSERT(expressions.size() == 1);
	VisitQualifiedAlias(*expressions[0], source_alias, &target_alias);
	return RefreshExpressionSQL(*expressions[0]);
}

static bool ReferencesQualifiedAlias(const string &expr, const string &alias) {
	if (expr.empty()) {
		return false;
	}
	auto expressions = Parser::ParseExpressionList(expr);
	D_ASSERT(expressions.size() == 1);
	return VisitQualifiedAlias(*expressions[0], alias, nullptr);
}

} // namespace

vector<string> PartitionOutputColumns(const vector<string> &partition_columns) {
	vector<string> output_columns;
	output_columns.reserve(partition_columns.size());
	for (auto &partition_column : partition_columns) {
		output_columns.push_back(PartitionOutputColumn(partition_column));
	}
	return output_columns;
}

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
                              bool running_window_incremental, bool *uses_running_suffix) {
	if (uses_running_suffix) {
		*uses_running_suffix = false;
	}
	bool have_affected_keys = !affected_keys_sql.empty();
	if (!have_affected_keys && (partition_columns.empty() || partition_delta_specs.empty())) {
		// No PARTITION BY (global surrogate-key window) or no partition key resolvable in any
		// source delta table → nothing to scope the recompute to. Keep the cascade delta the
		// caller asked for so downstream MVs stay incremental.
		return CompileFullRecompute(view_name, view_query_sql, catalog_prefix, emit_cascade_delta);
	}
	if (running_window_incremental) {
		auto suffix_sql = BuildRunningWindowSuffixRefreshSQL(view_name, view_query_sql, delta_ts_filter, catalog_prefix,
		                                                     partition_columns, partition_delta_specs, column_names,
		                                                     emit_cascade_delta);
		if (!suffix_sql.empty()) {
			if (uses_running_suffix) {
				*uses_running_suffix = true;
			}
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
	string filter = have_affected_keys ? BuildAffectedTableFilter(output_columns, "openivm_target", affected_temp_table)
	                                   : BuildDeltaAffectedFilter(partition_delta_specs, delta_where, "openivm_target");
	sql +=
	    BuildSnapshotDeltaRefreshSQL(data_table, view_query_sql, delta_table, old_temp_table, new_temp_table, filter);
	if (have_affected_keys) {
		sql += "\nDROP TABLE IF EXISTS " + affected_temp_table + ";\n";
	}
	return sql;
}

} // namespace duckdb
