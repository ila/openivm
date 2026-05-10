#include "core/openivm_parser.hpp"

#include "core/ivm_plan_rewrite.hpp"
#include "core/ivm_delta_model.hpp"
#include "core/ivm_view_classifier.hpp"
#include "core/openivm_constants.hpp"
#include "lpts_pipeline.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/cte_inlining.hpp"

#include "core/openivm_debug.hpp"
#include "storage/ducklake_scan.hpp"
#include "storage/ducklake_table_entry.hpp"

#include <regex>

namespace duckdb {

static constexpr const char *OPENIVM_DDL_RECONNECT = "__openivm_reconnect__";
static constexpr const char *OPENIVM_DDL_CLEANUP_PREFIX = "__openivm_cleanup__:";

/// Build "ORDER BY col1 ASC, col2 DESC LIMIT k [OFFSET n]".
/// Works for both LOGICAL_TOP_N (fused) and separate LOGICAL_ORDER_BY + LOGICAL_LIMIT nodes.
/// output_col_names is the sanitized output column list; BoundColumnRefs are resolved via
/// their column_index into that list.
static string BuildTopKSuffix(const vector<BoundOrderByNode> &orders, idx_t limit_val, idx_t offset_val,
                              const vector<string> &output_col_names) {
	string sql = "ORDER BY ";
	for (size_t i = 0; i < orders.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		auto &ord = orders[i];
		bool resolved = false;
		if (ord.expression->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col_ref = ord.expression->Cast<BoundColumnRefExpression>();
			idx_t cidx = col_ref.binding.column_index;
			if (cidx < output_col_names.size() && !output_col_names[cidx].empty()) {
				sql += KeywordHelper::WriteOptionallyQuoted(output_col_names[cidx]);
				resolved = true;
			}
		}
		if (!resolved) {
			const string &alias = ord.expression->alias;
			if (!alias.empty()) {
				sql += KeywordHelper::WriteOptionallyQuoted(alias);
			} else {
				sql += ord.expression->ToString();
			}
		}
		sql += " " + ord.GetOrderModifier();
	}
	if (limit_val > 0) {
		sql += " LIMIT " + to_string(limit_val);
		if (offset_val > 0) {
			sql += " OFFSET " + to_string(offset_val);
		}
	}
	return sql;
}

static bool PlanContainsCte(LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_CTE_REF || op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return true;
	}
	for (auto &child : op->children) {
		if (PlanContainsCte(child.get())) {
			return true;
		}
	}
	return false;
}

static void RelaxMaterializedCtes(LogicalOperator *op) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		auto &cte = op->Cast<LogicalMaterializedCTE>();
		if (cte.materialize == CTEMaterialize::CTE_MATERIALIZE_ALWAYS) {
			cte.materialize = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;
		}
	}
	for (auto &child : op->children) {
		RelaxMaterializedCtes(child.get());
	}
}

static void InlineCtesIfPresent(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan) {
	if (!PlanContainsCte(plan.get())) {
		return;
	}
	RelaxMaterializedCtes(plan.get());
	Optimizer cte_opt(binder, context);
	CTEInlining cte_inlining(cte_opt);
	plan = cte_inlining.Optimize(std::move(plan));
}

struct OpenIVMDuckLakeTableInfo {
	string table_name;   // actual name as stored in DuckLake (case-preserved)
	string catalog_name; // DuckLake catalog name (e.g., "dl")
	string schema_name;  // DuckLake schema name
};

struct OpenIVMSourceTableInfo {
	string table_name;
	string catalog_name;
	string schema_name;
};

static void CollectDuckLakeTables(LogicalOperator *op, const string &current_catalog,
                                  unordered_map<string, OpenIVMDuckLakeTableInfo> &dl_table_info) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		if (get.function.name == "ducklake_scan" && get.function.function_info) {
			auto &info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
			string lc = info.table_name;
			std::transform(lc.begin(), lc.end(), lc.begin(), [](unsigned char c) { return std::tolower(c); });
			if (dl_table_info.find(lc) == dl_table_info.end()) {
				// Always pull the catalog from the DuckLakeTableEntry — it's the
				// one the `dl.ORDER_LINE` reference actually resolved to. Falling
				// back to `current_catalog` is wrong for cross-system MVs (native
				// MV reading from dl.*) because `current_catalog` is the physical
				// default, not the DuckLake catalog.
				string cat = info.table.ParentCatalog().GetName();
				if (cat.empty()) {
					cat = current_catalog.empty() ? "dl" : current_catalog;
				}
				dl_table_info[lc] = {info.table_name, cat, info.table.schema.name};
			}
		}
	}
	for (auto &child : op->children) {
		CollectDuckLakeTables(child.get(), current_catalog, dl_table_info);
	}
}

static void CollectSourceTables(LogicalOperator *op, unordered_map<string, OpenIVMSourceTableInfo> &source_table_info) {
	if (!op) {
		return;
	}
	if (op->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op->Cast<LogicalGet>();
		auto table_ref = get.GetTable();
		if (table_ref.get()) {
			auto &table = *table_ref.get();
			string table_name = table.name;
			if (!table_name.empty() && !OpenIVMUtils::IsDelta(table_name)) {
				source_table_info[table_name] = {table_name, table.ParentCatalog().GetName(), table.schema.name};
			}
		}
	}
	for (auto &child : op->children) {
		CollectSourceTables(child.get(), source_table_info);
	}
}

static string QualifiedTablePrefix(const string &catalog_name, const string &schema_name) {
	return KeywordHelper::WriteOptionallyQuoted(catalog_name) + "." +
	       KeywordHelper::WriteOptionallyQuoted(schema_name) + ".";
}

static string QuoteQualifiedPrefix(const string &prefix) {
	string result;
	size_t start = 0;
	while (start < prefix.size()) {
		size_t dot = prefix.find('.', start);
		string part = prefix.substr(start, dot == string::npos ? string::npos : dot - start);
		if (!part.empty()) {
			if (!result.empty()) {
				result += ".";
			}
			result += KeywordHelper::WriteOptionallyQuoted(part);
		}
		if (dot == string::npos) {
			break;
		}
		start = dot + 1;
	}
	return result.empty() ? "" : result + ".";
}

static bool RelationExists(Connection &con, const string &qualified_name) {
	auto result = con.Query("SELECT * FROM " + qualified_name + " LIMIT 0");
	return !result->HasError();
}

static string SanitizeOutputName(const string &name) {
	if (IVMTableNames::IsInternalColumn(name)) {
		return name;
	}
	string clean;
	bool last_was_underscore = false;
	for (auto c : name) {
		if (isalnum(c)) {
			clean += c;
			last_was_underscore = false;
		} else if (!last_was_underscore && !clean.empty()) {
			clean += '_';
			last_was_underscore = true;
		}
	}
	if (!clean.empty() && clean.back() == '_') {
		clean.pop_back();
	}
	return clean.empty() ? name : clean;
}

static void SanitizeOutputNames(vector<string> &output_names) {
	for (auto &name : output_names) {
		name = SanitizeOutputName(name);
	}
}

static LogicalOperator *FindTopNameSource(LogicalOperator *plan) {
	for (auto *walk = plan; walk;) {
		if (walk->type == LogicalOperatorType::LOGICAL_PROJECTION ||
		    walk->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return walk;
		}
		if ((walk->type == LogicalOperatorType::LOGICAL_ORDER_BY || walk->type == LogicalOperatorType::LOGICAL_LIMIT ||
		     walk->type == LogicalOperatorType::LOGICAL_TOP_N || walk->type == LogicalOperatorType::LOGICAL_DISTINCT ||
		     walk->type == LogicalOperatorType::LOGICAL_FILTER) &&
		    !walk->children.empty()) {
			walk = walk->children[0].get();
			continue;
		}
		break;
	}
	return nullptr;
}

static void AppendProjectionOutputNames(LogicalProjection &projection, idx_t output_count,
                                        vector<string> &output_names) {
	while (output_names.size() < output_count) {
		idx_t idx = output_names.size();
		if (idx < projection.expressions.size() && !projection.expressions[idx]->alias.empty()) {
			output_names.push_back(projection.expressions[idx]->alias);
		} else {
			output_names.push_back("_ivm_col_" + to_string(idx));
		}
	}
}

static void AppendAggregateOutputNames(LogicalAggregate &aggregate, idx_t output_count, vector<string> &output_names) {
	idx_t group_count = aggregate.groups.size();
	while (output_names.size() < output_count) {
		idx_t idx = output_names.size();
		if (idx >= group_count) {
			idx_t expr_idx = idx - group_count;
			if (expr_idx < aggregate.expressions.size() && !aggregate.expressions[expr_idx]->alias.empty()) {
				output_names.push_back(aggregate.expressions[expr_idx]->alias);
				continue;
			}
		}
		output_names.push_back("_ivm_col_" + to_string(idx));
	}
}

static void AppendHiddenOutputNames(LogicalOperator *plan, vector<string> &output_names) {
	auto output_count = plan->GetColumnBindings().size();
	auto *name_source = FindTopNameSource(plan);
	if (!name_source) {
		return;
	}
	if (name_source->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		AppendProjectionOutputNames(name_source->Cast<LogicalProjection>(), output_count, output_names);
	} else {
		AppendAggregateOutputNames(name_source->Cast<LogicalAggregate>(), output_count, output_names);
	}
}

static void DeduplicateOutputNames(vector<string> &output_names) {
	unordered_set<string> seen;
	for (auto &name : output_names) {
		if (IVMTableNames::IsInternalColumn(name)) {
			continue;
		}
		string candidate = name;
		idx_t suffix = 1;
		while (seen.count(candidate)) {
			candidate = name + "_" + to_string(suffix++);
		}
		seen.insert(candidate);
		name = candidate;
	}
}

static vector<string> PrepareOutputNames(LogicalOperator *select_plan, const vector<string> &planner_names) {
	auto output_names = planner_names;
	SanitizeOutputNames(output_names);
	AppendHiddenOutputNames(select_plan, output_names);
	DeduplicateOutputNames(output_names);
	return output_names;
}

static size_t FindKeywordToken(const string &text, const string &keyword, size_t from) {
	size_t pos = from;
	while (true) {
		pos = text.find(keyword, pos);
		if (pos == string::npos) {
			return string::npos;
		}
		bool ok_left = (pos == 0) || std::isspace(static_cast<unsigned char>(text[pos - 1])) || text[pos - 1] == '(';
		bool ok_right = (pos + keyword.size() == text.size()) ||
		                std::isspace(static_cast<unsigned char>(text[pos + keyword.size()])) ||
		                text[pos + keyword.size()] == '(';
		if (ok_left && ok_right) {
			return pos;
		}
		pos++;
	}
}

static bool StartsKeywordToken(const string &text, size_t pos, const string &keyword) {
	if (pos + keyword.size() > text.size()) {
		return false;
	}
	if (text.compare(pos, keyword.size(), keyword) != 0) {
		return false;
	}
	return (pos + keyword.size() == text.size()) ||
	       !std::isalnum(static_cast<unsigned char>(text[pos + keyword.size()]));
}

struct QueryConstructFlags {
	bool needs_original_sql_for_lpts = false;
	bool has_unsupported_incremental_construct = false;
};

static QueryConstructFlags AnalyzeQueryConstructs(const string &query) {
	string lower = StringUtil::Lower(query);
	bool has_pivot = lower.find("pivot ") != string::npos || lower.find("(pivot ") != string::npos;
	bool has_unnest = lower.find(" unnest(") != string::npos || lower.find(" cross join unnest(") != string::npos;
	QueryConstructFlags result;
	result.needs_original_sql_for_lpts = has_pivot;
	result.has_unsupported_incremental_construct = has_pivot || has_unnest;
	return result;
}

static LogicalAggregate *FindOuterAggregate(LogicalOperator *op) {
	if (!op) {
		return nullptr;
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return &op->Cast<LogicalAggregate>();
	}
	for (auto &child : op->children) {
		auto *aggregate = FindOuterAggregate(child.get());
		if (aggregate) {
			return aggregate;
		}
	}
	return nullptr;
}

static string JsonQuote(const string &value) {
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

static bool IsPacLoaded(ClientContext &context) {
	Value pac_check_val;
	return context.TryGetCurrentSetting("pac_check", pac_check_val);
}

static void ForwardPacSettingsIfLoaded(ClientContext &context, Connection &con) {
	if (!IsPacLoaded(context)) {
		return;
	}
	for (auto &name : {"pac_mi", "pac_seed", "pac_m", "pac_noise", "pac_hash_repair", "pac_check", "pac_rewrite",
	                   "pac_conservative_mode"}) {
		Value val;
		if (context.TryGetCurrentSetting(name, val) && !val.IsNull()) {
			con.Query("SET " + string(name) + " = " + val.ToString());
		}
	}
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
static bool ExtractInnerDistinct(const string &original_sql, vector<string> &out_cols, string &out_input_sql,
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
	    !StartsKeywordToken(lower, alias_skip, "where") && !StartsKeywordToken(lower, alias_skip, "group") &&
	    !StartsKeywordToken(lower, alias_skip, "order") && !StartsKeywordToken(lower, alias_skip, "having") &&
	    !StartsKeywordToken(lower, alias_skip, "limit") && !StartsKeywordToken(lower, alias_skip, "union") &&
	    !StartsKeywordToken(lower, alias_skip, "join") && !StartsKeywordToken(lower, alias_skip, "left") &&
	    !StartsKeywordToken(lower, alias_skip, "right") && !StartsKeywordToken(lower, alias_skip, "inner") &&
	    !StartsKeywordToken(lower, alias_skip, "full") && !StartsKeywordToken(lower, alias_skip, "cross") &&
	    !StartsKeywordToken(lower, alias_skip, "on")) {
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
		} else if (lower[alias_skip] != ')' && !StartsKeywordToken(lower, alias_skip, "group") &&
		           !StartsKeywordToken(lower, alias_skip, "order") && !StartsKeywordToken(lower, alias_skip, "limit") &&
		           !StartsKeywordToken(lower, alias_skip, "union")) {
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
		} else if (d == 0 && (StartsKeywordToken(lower, i, "group") || StartsKeywordToken(lower, i, "order") ||
		                      StartsKeywordToken(lower, i, "limit") || StartsKeywordToken(lower, i, "union"))) {
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

struct SemiAntiExtract {
	string join_type;
	string left_table;
	string left_alias;
	string right_table;
	string right_alias;
	string predicate;
	string post_filter;
	vector<string> output_cols;
	vector<string> output_exprs;
};

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

static string LastIdentifierPart(string ident) {
	StringUtil::Trim(ident);
	size_t dot = ident.find_last_of('.');
	if (dot != string::npos) {
		ident = ident.substr(dot + 1);
	}
	if (ident.size() >= 2 && ident.front() == '"' && ident.back() == '"') {
		ident = ident.substr(1, ident.size() - 2);
	}
	return ident;
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
				out.name = LastIdentifierPart(alias_token);
				return !out.expr.empty() && !out.name.empty();
			}
		}
		out.expr = item;
		out.name = LastIdentifierPart(item);
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
		out.left_alias = LastIdentifierPart(out.left_table);
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
		out.right_alias = LastIdentifierPart(out.right_table);
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
		out.left_alias = LastIdentifierPart(out.left_table);
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
		out.right_alias = LastIdentifierPart(out.right_table);
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
	string left_alias_expr = "_ivm_left";
	bool simple_left_table = false;
	size_t left_pos = 0;
	string left_ident;
	if (ReadIdentifierToken(left_from, left_pos, left_ident)) {
		const string &candidate_table = left_ident;
		string candidate_alias = LastIdentifierPart(left_ident);
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
		left_alias_expr = "_ivm_left";
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
		out.right_alias = LastIdentifierPart(out.right_table);
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
	out.right_alias = "_ivm_right";
	if (simple_left_table) {
		out.predicate = StringUtil::Replace(lhs, out.left_alias + ".", out.left_alias + ".");
		out.predicate =
		    StringUtil::Replace(out.predicate, LastIdentifierPart(out.left_table) + ".", out.left_alias + ".");
	} else {
		out.predicate = out.left_alias + "." + KeywordHelper::WriteOptionallyQuoted(LastIdentifierPart(lhs));
	}
	out.predicate += " IS NOT DISTINCT FROM ";
	out.predicate += StringUtil::Replace(rhs_expr, original_right_alias + ".", out.right_alias + ".");
	out.predicate =
	    StringUtil::Replace(out.predicate, LastIdentifierPart(out.right_table) + ".", out.right_alias + ".");
	if (!right_filter.empty()) {
		string rewritten_filter = StringUtil::Replace(right_filter, original_right_alias + ".", out.right_alias + ".");
		rewritten_filter =
		    StringUtil::Replace(rewritten_filter, LastIdentifierPart(out.right_table) + ".", out.right_alias + ".");
		out.predicate += " AND (" + rewritten_filter + ")";
	}
	out.output_cols = std::move(output_cols);
	out.output_exprs = std::move(output_exprs);
	return true;
}

static bool ExtractSemiAntiQuery(const string &original_sql, SemiAntiExtract &out) {
	string lower = StringUtil::Lower(original_sql);
	if (FindTopLevelKeywordToken(lower, "union", 0) != string::npos ||
	    FindTopLevelKeywordToken(lower, "intersect", 0) != string::npos ||
	    FindTopLevelKeywordToken(lower, "except", 0) != string::npos) {
		return false;
	}
	return ExtractSemiAntiJoin(original_sql, out) || ExtractExistsSubquery(original_sql, out) ||
	       ExtractInSubquery(original_sql, out);
}

static unique_ptr<FunctionData> IVMDDLBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	// DDL statements are passed via result.parameters from the plan function.
	if (!input.inputs.empty()) {
		auto &db = DatabaseInstance::GetDatabase(context);
		auto conn = make_uniq<Connection>(db);
		vector<string> cleanup_ddl;
		auto run_cleanup = [&]() {
			for (const auto &cleanup : cleanup_ddl) {
				OPENIVM_DEBUG_PRINT("[IVMDDLBindFunction] Cleanup DDL: %s\n", cleanup.c_str());
				auto cleanup_result = conn->Query(cleanup);
				if (cleanup_result->HasError()) {
					OPENIVM_DEBUG_PRINT("[IVMDDLBindFunction] Cleanup failed: %s\n",
					                    cleanup_result->GetError().c_str());
				}
			}
		};
		for (auto &param : input.inputs) {
			auto q = param.GetValue<string>();
			if (q.empty()) {
				continue;
			}
			if (StringUtil::StartsWith(q, OPENIVM_DDL_CLEANUP_PREFIX)) {
				cleanup_ddl.push_back(q.substr(strlen(OPENIVM_DDL_CLEANUP_PREFIX)));
				continue;
			}
			if (q == OPENIVM_DDL_RECONNECT) {
				// DuckLake keeps read transaction state on the connection. Reconnect
				// between staged DuckLake reads and DuckLake writes so the write-side
				// commit cannot self-block on an earlier read from the same CREATE MV.
				conn = make_uniq<Connection>(db);
				continue;
			}
			OPENIVM_DEBUG_PRINT("[IVMDDLBindFunction] Executing DDL: %s\n", q.c_str());
			auto r = conn->Query(q);
			if (r->HasError()) {
				// The unique index on the MV data table is an upsert-time optimization
				// (helps MERGE find the matching row quickly). If the classifier's
				// group_columns don't actually form a unique key for the MV (e.g. a
				// CTE with GROUP BY joined against another table — the outer JOIN
				// duplicates rows), CREATE UNIQUE INDEX fails with "Data contains
				// duplicates". Skip just that statement and continue; the refresh
				// still works via key-based MERGE, just a bit slower.
				bool is_unique_index = StringUtil::Contains(StringUtil::Lower(q), "create unique index") &&
				                       StringUtil::Contains(r->GetError(), "Data contains duplicates");
				if (is_unique_index) {
					Printer::Print("Warning: could not create unique index for MV — group_columns "
					               "are not unique in MV output. Refresh will still work (no index).");
					continue;
				}
				run_cleanup();
				throw CatalogException("Failed to execute IVM DDL: " + r->GetError());
			}
		}
	}
	names.emplace_back("MATERIALIZED VIEW CREATION");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return make_uniq<IVMFunction::IVMBindData>(true);
}

static void IVMDDLExecuteFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<IVMFunction::IVMBindData>();
	auto &gdata = dynamic_cast<IVMFunction::IVMGlobalData &>(*data_p.global_state);
	if (gdata.offset >= 1) {
		return;
	}
	output.SetValue(0, 0, Value::BOOLEAN(bind_data.result));
	output.SetCardinality(1);
	gdata.offset++;
}

ParserExtensionParseResult IVMParserExtension::IVMParseFunction(ParserExtensionInfo *info, const string &query) {
	auto query_lower = OpenIVMUtils::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);
	// Strip SQL line comments (-- to end of line) before whitespace normalization.
	// RemoveRedundantWhitespaces collapses '\n' to ' ', which would turn
	// "-- comment\n rest" into "-- comment rest" where the rest is eaten by the comment.
	OpenIVMUtils::StripLineComments(query_lower);
	OpenIVMUtils::RemoveRedundantWhitespaces(query_lower);

	// Handle ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' | SET REFRESH MANUAL
	if (StringUtil::Contains(query_lower, "alter materialized view")) {
		std::regex alter_re("alter\\s+materialized\\s+view\\s+(\"(?:[^\"]+)\"|[a-zA-Z0-9_.]+)\\s+set\\s+refresh\\s+("
		                    "every\\s+'([^']+)'|manual)",
		                    std::regex::icase);
		std::smatch match;
		if (!std::regex_search(query_lower, match, alter_re)) {
			throw ParserException("Invalid ALTER MATERIALIZED VIEW syntax. "
			                      "Expected: ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' "
			                      "or ALTER MATERIALIZED VIEW <name> SET REFRESH MANUAL");
		}
		string alter_view_name = match[1].str();
		if (alter_view_name.size() >= 2 && alter_view_name.front() == '"' && alter_view_name.back() == '"') {
			alter_view_name = alter_view_name.substr(1, alter_view_name.size() - 2);
		}
		string refresh_type = StringUtil::Lower(match[2].str());
		string update_sql;
		if (refresh_type == "manual") {
			update_sql = "UPDATE " + string(ivm::VIEWS_TABLE) + " SET refresh_interval = NULL WHERE view_name = '" +
			             OpenIVMUtils::EscapeSingleQuotes(alter_view_name) + "'";
		} else {
			int64_t interval = OpenIVMUtils::ParseRefreshInterval(match[3].str());
			update_sql = "UPDATE " + string(ivm::VIEWS_TABLE) + " SET refresh_interval = " + to_string(interval) +
			             " WHERE view_name = '" + OpenIVMUtils::EscapeSingleQuotes(alter_view_name) + "'";
		}
		// Pass the UPDATE SQL through IVMParseData; IVMPlanFunction will execute it
		Parser alter_parser;
		alter_parser.ParseQuery("SELECT 1");
		auto parse_data =
		    make_uniq_base<ParserExtensionParseData, IVMParseData>(std::move(alter_parser.statements[0]), true);
		dynamic_cast<IVMParseData &>(*parse_data).alter_sql = update_sql;
		return ParserExtensionParseResult(std::move(parse_data));
	}

	if (!StringUtil::Contains(query_lower, "create materialized view") &&
	    !StringUtil::Contains(query_lower, "create or replace materialized view")) {
		return ParserExtensionParseResult();
	}

	OPENIVM_DEBUG_PRINT("[CREATE MV] Intercepted query: %s\n", query_lower.c_str());

	// Detect CREATE OR REPLACE MATERIALIZED VIEW
	bool is_replace = false;
	std::regex or_replace_re("\\bcreate\\s+or\\s+replace\\s+materialized\\s+view\\b", std::regex::icase);
	if (std::regex_search(query_lower, or_replace_re)) {
		is_replace = true;
		// Strip "or replace" so the rest of the pipeline sees "create materialized view"
		query_lower = std::regex_replace(query_lower, std::regex("\\bor\\s+replace\\s+"), "");
		OpenIVMUtils::RemoveRedundantWhitespaces(query_lower);
	}

	// Extract REFRESH EVERY clause before structural rewrite (strips it from the query)
	int64_t refresh_interval = OpenIVMUtils::ExtractRefreshInterval(query_lower);
	OPENIVM_DEBUG_PRINT("[CREATE MV] Refresh interval: %lld seconds\n", (long long)refresh_interval);

	OpenIVMUtils::ReplaceMaterializedView(query_lower);
	// All other rewrites (DISTINCT, AVG, LEFT JOIN key, aggregate aliases) are done
	// at the plan level in IVMPlanFunction via IVMPlanRewrite + LPTS.
	OPENIVM_DEBUG_PRINT("[CREATE MV] After structural rewrite: %s\n", query_lower.c_str());

	Parser p;
	p.ParseQuery(query_lower);

	auto parse_data =
	    make_uniq_base<ParserExtensionParseData, IVMParseData>(std::move(p.statements[0]), true, refresh_interval);
	dynamic_cast<IVMParseData &>(*parse_data).is_replace = is_replace;
	return ParserExtensionParseResult(std::move(parse_data));
}

ParserExtensionPlanResult IVMParserExtension::IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
                                                              unique_ptr<ParserExtensionParseData> parse_data) {
	auto &ivm_parse_data = dynamic_cast<IVMParseData &>(*parse_data);
	auto statement = dynamic_cast<SQLStatement *>(ivm_parse_data.statement.get());

	ParserExtensionPlanResult result;

	if (ivm_parse_data.plan) {
		Connection con(*context.db.get());

		// Capture the current catalog/schema from the originating context. IVMDDLBindFunction
		// creates a fresh Connection that reflects the DatabaseInstance's physical default
		// catalog (not the session's USE setting). We only inject "USE catalog.schema" when
		// the session's active catalog differs from that physical default — e.g. when DuckLake
		// ("dl") is active but the file DB ("rewriter_benchmark_sf1") is the physical default.
		string current_catalog;
		string current_schema;
		{
			auto &sp = ClientData::Get(context).catalog_search_path;
			auto def = sp->GetDefault();
			current_catalog = def.catalog;
			current_schema = def.schema.empty() ? "main" : def.schema;
		}
		// Query the physical default by running SELECT current_database() on the fresh `con`
		// (created above without any USE, so it reflects the DB's true default, not the session).
		string default_db;
		string default_schema = "main";
		{
			auto res = con.Query("SELECT current_database()");
			if (!res->HasError() && res->RowCount() > 0) {
				default_db = res->GetValue(0, 0).ToString();
			}
			auto schema_res = con.Query("SELECT current_schema()");
			if (!schema_res->HasError() && schema_res->RowCount() > 0) {
				default_schema = schema_res->GetValue(0, 0).ToString();
			}
		}
		string default_catalog_schema = KeywordHelper::WriteOptionallyQuoted(default_db) + "." +
		                                KeywordHelper::WriteOptionallyQuoted(default_schema);
		string current_catalog_schema = KeywordHelper::WriteOptionallyQuoted(current_catalog) + "." +
		                                KeywordHelper::WriteOptionallyQuoted(current_schema);

		// Handle ALTER MATERIALIZED VIEW — just execute the metadata UPDATE
		if (!ivm_parse_data.alter_sql.empty()) {
			auto r = con.Query(ivm_parse_data.alter_sql);
			if (r->HasError()) {
				throw CatalogException("Failed to alter materialized view: " + r->GetError());
			}
			// Return via the DDL executor with no DDL to run (the UPDATE already executed)
			result.function =
			    TableFunction("ivm_ddl_executor", {}, IVMDDLExecuteFunction, IVMDDLBindFunction, IVMFunction::IVMInit);
			result.requires_valid_transaction = true;
			result.return_type = StatementReturnType::QUERY_RESULT;
			return result;
		}

		// PAC compatibility boundary: internal planning uses a fresh connection, so
		// forward PAC settings when that extension is loaded in the caller session.
		bool pac_loaded = IsPacLoaded(context);
		ForwardPacSettingsIfLoaded(context, con);

		auto full_view_name = OpenIVMUtils::ExtractTableName(statement->query);
		// Keep the user's raw AS-query as the source of truth for original-SQL fallback.
		// Do not recover this from DuckDB's parsed QueryNode::ToString(): that path is a
		// best-effort pretty-printer and has segfaulted on set-operation query nodes with
		// incomplete CTE/query internals. LPTS remains the normalized serializer below
		// for supported logical plans; this string is only the safe fallback input.
		auto original_view_query = OpenIVMUtils::ExtractViewQuery(statement->query);
		auto statement_constructs = AnalyzeQueryConstructs(statement->query);
		auto query_constructs = AnalyzeQueryConstructs(original_view_query);

		// Split catalog-qualified name (e.g. "dl.mv_totals") into prefix and bare name.
		// The target prefix is used only for the user-facing view. DuckLake-backed MVs keep
		// OpenIVM's internal state in the physical default catalog so CREATE/REFRESH does not
		// need to atomically write DuckLake metadata and DuckDB catalog objects together.
		string view_catalog_prefix; // e.g. "dl." or "" for default catalog
		string view_name;           // bare name without catalog, e.g. "mv_totals"
		auto dot_pos = full_view_name.rfind('.');
		if (dot_pos != string::npos) {
			view_catalog_prefix = QuoteQualifiedPrefix(full_view_name.substr(0, dot_pos + 1));
			view_name = full_view_name.substr(dot_pos + 1);
		} else {
			view_name = full_view_name;
			// When the MV name is unqualified but the session is in a non-default catalog
			// (e.g. USE dl.main), explicitly qualify so data/view tables land in dl rather
			// than the physical default. Metadata tables (unqualified) stay in the physical
			// default — PRAGMA ivm() always uses a fresh connection without USE.
			if (!current_catalog.empty() && current_catalog != default_db) {
				view_catalog_prefix = QualifiedTablePrefix(current_catalog, current_schema);
			}
		}
		string internal_catalog_prefix = view_catalog_prefix;
		if (!view_catalog_prefix.empty() && default_db != "memory") {
			internal_catalog_prefix = QualifiedTablePrefix(default_db, default_schema);
		}
		string data_table = IVMTableNames::DataTableName(view_name);
		string qdt = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(data_table);
		string qvn = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
		string view_query = original_view_query; // will be overwritten by LPTS for DDL
		string top_k_suffix;                     // ORDER BY … LIMIT k, appended to the CREATE VIEW

		// Apply the session's active catalog to `con` so unqualified table references in the
		// MV query resolve in the user's catalog (e.g. `dl.main`) rather than the physical
		// default. Without this, `CREATE MATERIALIZED VIEW mv AS SELECT * FROM WAREHOUSE`
		// issued under `USE dl.main` fails during planning with "Table WAREHOUSE does not
		// exist" because the fresh connection resolves against the physical-default catalog.
		if (!current_catalog.empty() && current_catalog != default_db) {
			con.Query("USE " + current_catalog + "." + current_schema);
		}

		if (!ivm_parse_data.is_replace) {
			// Fail before registering cleanup DDL. Otherwise a duplicate CREATE attempt
			// can fail on the pre-existing backing table and then cleanup would drop the
			// original MV's user-facing view/data table.
			if (RelationExists(con, qvn) || RelationExists(con, qdt)) {
				throw CatalogException("Table with name \"" + view_name + "\" already exists!");
			}
		}

		// Use con for planning — sees all committed state from previous bind-phase DDL
		con.BeginTransaction();
		// GetTableNames binds the query internally. For MV queries that DuckDB's binder
		// can't evaluate out-of-context (e.g. multi-column `(a, b) IN (SELECT x, y FROM t)`
		// triggers an ARRAY-sublink path that rejects the 2-column subquery), the call
		// throws. Catch and use an empty table_names — the later ExtractViewQuery path
		// re-derives what it needs from the plan.
		unordered_set<string> table_names;
		try {
			table_names = con.GetTableNames(statement->query);
		} catch (const std::exception &e) {
			OPENIVM_DEBUG_PRINT("[CREATE MV] GetTableNames failed: %s — continuing\n", e.what());
		}

		// Plan the full CREATE TABLE AS SELECT statement (for plan walking)
		Planner planner(*con.context);
		planner.CreatePlan(statement->Copy());
		auto plan = std::move(planner.plan);

		// Inline CTEs in this plan too so delta-model analysis / find_group_cols / HasLeftJoin
		// walks see the folded structure. DuckDB's binder defaults to
		// CTE_MATERIALIZE_ALWAYS, which makes CTEInlining bail — relax to DEFAULT first.
		// (The SELECT-only `select_plan` below does the same for LPTS serialization.)
		InlineCtesIfPresent(context, *planner.binder, plan);

		unordered_map<string, OpenIVMSourceTableInfo> source_table_info;
		CollectSourceTables(plan.get(), source_table_info);
		if (!source_table_info.empty()) {
			table_names.clear();
			for (const auto &entry : source_table_info) {
				table_names.insert(entry.second.table_name);
			}
		}
		unordered_map<string, OpenIVMDuckLakeTableInfo> dl_table_info_for_classification;
		CollectDuckLakeTables(plan.get(), current_catalog, dl_table_info_for_classification);

		// Plan the raw SELECT query separately for IVM plan rewrite + LPTS conversion
		vector<string> output_names;
		string having_predicate;    // HAVING predicate as SQL (for VIEW WHERE clause, empty if no HAVING)
		bool lpts_fallback = false; // set when LPTS can't serialize the plan and we fall back to SQL
		{
			Parser select_parser;
			select_parser.ParseQuery(original_view_query);
			Planner select_planner(*con.context);
			select_planner.CreatePlan(std::move(select_parser.statements[0]));
			auto select_plan = std::move(select_planner.plan);

			// Inline CTEs so the refresh rewriter sees one maintainable operator tree.
			// Query-bound CTEs become LOGICAL_MATERIALIZED_CTE + LOGICAL_CTE_REF nodes
			// in the bound plan; CTEInlining rewrites small/non-recursive ones into
			// direct subqueries. Running the full DuckDB optimizer here would also
			// reorder joins / push filters, which conflicts with IVM's subsequent plan
			// rewrites — so we run only the CTE-inlining pass, and only when a CTE
			// reference actually appears in the plan (the optimizer isn't always a
			// no-op on CTE-free plans — e.g. it can rewrite DISTINCT subqueries in ways
			// that confuse the downstream structural rewrites).
			// DuckDB's binder sets LogicalMaterializedCTE::materialize to
			// CTE_MATERIALIZE_ALWAYS by default. CTEInlining bails early on ALWAYS
			// and leaves the CTE as a materialized node. The refresh path has no
			// delta-consolidation rule for LOGICAL_CTE_REF, so relax every CTE to
			// CTE_MATERIALIZE_DEFAULT before inlining. Single-ref CTEs always inline;
			// multi-ref CTEs inline when they're cheap and don't end in an aggregate
			// that would be wastefully re-materialized.
			InlineCtesIfPresent(context, *select_planner.binder, select_plan);

			// Apply IVM plan rewrites (DISTINCT → GROUP BY + COUNT, AVG → SUM + COUNT, LEFT JOIN key)
			IVMPlanRewrite(context, *select_planner.binder, select_plan, select_planner.names);

			output_names = PrepareOutputNames(select_plan.get(), select_planner.names);
			// Strip HAVING filter from plan — data table stores all groups.
			// The predicate is extracted as SQL (using output aliases) for the VIEW WHERE clause.
			having_predicate = StripHavingFilter(select_plan, output_names);

			// For aggregate+top-k: extract the ORDER BY/LIMIT suffix, then strip the top-k
			// wrapper(s) so LPTS serializes only the inner aggregate query. The suffix is
			// appended to the CREATE VIEW definition so ORDER BY LIMIT is applied at read
			// time over the fully-maintained data table.
			//
			// Two plan shapes depending on whether top_n optimizer ran:
			//   (A) LOGICAL_TOP_N → child  [top_n disabled by LPTS — never reached here]
			//   (B) LOGICAL_LIMIT → LOGICAL_ORDER_BY → child  [LPTS active — common case]
			//
			// Projection top-k uses the same split: maintain the unlimited projection in
			// the data table and apply ORDER BY ... LIMIT in the user-facing view.
			{
				LogicalOperator *limit_node = nullptr;
				LogicalOperator *order_node = nullptr;

				if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_TOP_N) {
					// Shape (A): fused top-n node
					limit_node = select_plan.get();
					order_node = select_plan.get(); // same node holds both orders + limit
				} else if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_LIMIT &&
				           !select_plan->children.empty() &&
				           select_plan->children[0]->type == LogicalOperatorType::LOGICAL_ORDER_BY) {
					// Shape (B): separate LIMIT + ORDER_BY nodes
					limit_node = select_plan.get();
					order_node = select_plan->children[0].get();
				}

				if (limit_node) {
					// Strip top-k unconditionally. The data table stores the unlimited result,
					// while the user-facing view applies ORDER BY ... LIMIT at read time.
					if (limit_node->type == LogicalOperatorType::LOGICAL_TOP_N) {
						auto &top_n = limit_node->Cast<LogicalTopN>();
						top_k_suffix = BuildTopKSuffix(top_n.orders, top_n.limit, top_n.offset, output_names);
						select_plan = std::move(select_plan->children[0]);
					} else {
						// Shape (B): extract from the two separate nodes
						auto &order_op = order_node->Cast<LogicalOrder>();
						auto &limit_op = limit_node->Cast<LogicalLimit>();
						idx_t lval = 0;
						idx_t oval = 0;
						if (limit_op.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
							lval = limit_op.limit_val.GetConstantValue();
						}
						if (limit_op.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
							oval = limit_op.offset_val.GetConstantValue();
						}
						top_k_suffix = BuildTopKSuffix(order_op.orders, lval, oval, output_names);
						// Strip: select_plan = LOGICAL_ORDER_BY's child
						select_plan = std::move(select_plan->children[0]->children[0]);
					}
					OPENIVM_DEBUG_PRINT("[CREATE MV] Stripped top-k wrapper, suffix='%s'\n", top_k_suffix.c_str());
				}
			}

			// Strip a standalone ORDER_BY at the top of select_plan (e.g. DISTINCT + ORDER BY
			// without LIMIT, or simple projection + ORDER BY). The data table stores unordered
			// rows; the suffix is appended to the CREATE VIEW instead.
			if (select_plan && select_plan->type == LogicalOperatorType::LOGICAL_ORDER_BY && top_k_suffix.empty() &&
			    !select_plan->children.empty()) {
				auto &order_op = select_plan->Cast<LogicalOrder>();
				top_k_suffix = BuildTopKSuffix(order_op.orders, 0, 0, output_names);
				select_plan = std::move(select_plan->children[0]);
				OPENIVM_DEBUG_PRINT("[CREATE MV] Stripped standalone ORDER_BY, suffix='%s'\n", top_k_suffix.c_str());
			}

			if (statement_constructs.needs_original_sql_for_lpts || query_constructs.needs_original_sql_for_lpts) {
				view_query = original_view_query;
				lpts_fallback = true;
				OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS can't round-trip this construct — using original SQL: %s\n",
				                    view_query.c_str());
			} else {
				try {
					auto ast = LogicalPlanToAst(*con.context, select_plan);
					auto cte_list = AstToCteList(*ast);
					view_query = cte_list->ToQuery(true, output_names);
					if (!view_query.empty() && view_query.back() == ';') {
						view_query.pop_back();
					}
					StringUtil::Trim(view_query);
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS view query: %s\n", view_query.c_str());
				} catch (const std::exception &e) {
					// LPTS doesn't support all operators (e.g., WINDOW). Fall back to original SQL.
					// This is fine for partition-recompute views that don't need LPTS-rewritten queries.
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS fallback (%s) to original query: %s\n", e.what(),
					                    view_query.c_str());
				} catch (...) {
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS fallback (unknown exception) to original query: %s\n",
					                    view_query.c_str());
				}
			}
			// For views that LPTS silently mis-serializes (GROUPING SETS / ROLLUP / CUBE
			// → plain GROUP BY; STRUCT_PACK field names → tN_col aliases; etc.), detect
			// structurally and prefer the original SQL. Those constructs never need the
			// LPTS-rewritten form anyway — they're maintained by recompute-style paths
			// using the original SQL, so the rewriter-rule path (which needs LPTS) isn't used.
			{
				auto select_delta_model = BuildDeltaPlanModel(select_plan.get());
				if (select_delta_model.analysis.needs_original_sql_for_lpts) {
					view_query = original_view_query;
					lpts_fallback = true;
					OPENIVM_DEBUG_PRINT("[CREATE MV] LPTS can't round-trip this construct — using original SQL: %s\n",
					                    view_query.c_str());
				}
			}
		}
		con.Rollback();

		OPENIVM_DEBUG_PRINT("[CREATE MV] View name: %s\n", view_name.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] View query: %s\n", view_query.c_str());
		OPENIVM_DEBUG_PRINT("[CREATE MV] Logical plan:\n%s\n", plan->ToString().c_str());

		// Normalize FILTER aggregates in the full plan before analysis so the delta model
		// sees CASE expressions instead of raw FILTER and doesn't set ivm_compatible=false.
		// (IVMPlanRewrite already rewrote select_plan for the LPTS view_query above.)
		RewriteAggregateFilters(context, plan);

		auto delta_model = BuildDeltaPlanModel(plan.get());
		OPENIVM_DEBUG_PRINT("[CREATE MV] delta model: %s\n", delta_model.DebugString().c_str());
		auto analysis = std::move(delta_model.analysis);
		if (analysis.found_delim_join && !analysis.found_aggregation && !analysis.found_single_join) {
			// Preserve DuckDB's dependent/DELIM_JOIN plan shape for refresh. LPTS can
			// round-trip lateral table functions, but its CTE-normalized SQL lowers them
			// into ordinary joins/table-function scans; that bypasses IvmDelimJoinRule
			// and sends the refresh plan through the generic N-way join rule instead.
			view_query = original_view_query;
			lpts_fallback = true;
		}
		if (analysis.found_filtered_list) {
			view_query = original_view_query;
			lpts_fallback = true;
			OPENIVM_DEBUG_PRINT("[CREATE MV] LIST FILTER requires original SQL for group-recompute: %s\n",
			                    view_query.c_str());
		}
		// Window over join: non-DuckLake delta tables may not expose the partition key for
		// every changed source, so native window recompute could miss partitions. DuckLake
		// can compare the old/new view result at source snapshots, so it can safely keep
		// partition keys even for multi-source window joins.
		bool all_sources_are_ducklake = !table_names.empty();
		if (all_sources_are_ducklake) {
			for (const auto &table_name : table_names) {
				string table_lc = table_name;
				std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
				               [](unsigned char c) { return std::tolower(c); });
				bool is_ducklake_scan =
				    dl_table_info_for_classification.find(table_lc) != dl_table_info_for_classification.end();
				// DuckLake views created by OpenIVM expose a DuckLake catalog view over an
				// internal physical _ivm_data_* table. When DuckDB expands such a view while
				// planning a chained MV, the scan is physical even though the source's change
				// tracking is still DuckLake-backed.
				bool is_ducklake_mv_backing =
				    !view_catalog_prefix.empty() && StringUtil::StartsWith(table_name, ivm::DATA_TABLE_PREFIX);
				if (!is_ducklake_scan && !is_ducklake_mv_backing) {
					all_sources_are_ducklake = false;
					break;
				}
			}
		}
		bool single_source_window_join = analysis.found_window && analysis.found_join && table_names.size() == 1;
		bool has_unsupported_set_operation = analysis.found_unsupported_set_operation;
		bool has_unsupported_incremental_construct = query_constructs.has_unsupported_incremental_construct;
		if (has_unsupported_set_operation || has_unsupported_incremental_construct) {
			// These views are maintained by full refresh, so store the user's query directly.
			// The CREATE-time IVM rewrites can add hidden columns for incremental paths (e.g.
			// LEFT JOIN match keys) that do not survive SQL set-operation arity rules.
			view_query = original_view_query;
			lpts_fallback = true;
		}
		bool keep_window_join_partitions =
		    !analysis.found_window || !analysis.found_join || single_source_window_join || all_sources_are_ducklake;

		DeltaViewModelInput model_input;
		model_input.analysis = analysis;
		model_input.plan = plan.get();
		model_input.output_names = output_names;
		model_input.has_unsupported_incremental_construct = has_unsupported_incremental_construct;
		model_input.keep_window_join_partitions = keep_window_join_partitions;
		auto prelim_view_model = BuildDeltaViewModel(model_input);

		// Populated by ExtractInnerDistinct when classified as DISTINCT_INCREMENTAL.
		vector<string> distinct_extracted_cols;
		string distinct_extracted_input_sql;
		string distinct_extracted_source;
		string distinct_extracted_filter;
		// Outer-aggregate spec for the v0 aux-state pipeline. Single SUM(<arg>) only —
		// any other shape (multiple SUMs, AVG, COUNT, etc.) demotes to GROUP_RECOMPUTE.
		// `sum_arg` is the column name from the DISTINCT input (one of distinct_cols).
		// `sum_out` is the user-facing output column name in the data table.
		string distinct_sum_arg;
		string distinct_sum_out;
		SemiAntiExtract semi_anti_extract;
		vector<string> semi_anti_left_cols;

		bool semi_anti_recompute_supported = false;
		if (analysis.found_semi_anti_join && !analysis.found_aggregation) {
			if (ExtractSemiAntiQuery(original_view_query, semi_anti_extract)) {
				string left_table_name = LastIdentifierPart(semi_anti_extract.left_table);
				auto col_result =
				    con.Query("SELECT column_name FROM information_schema.columns WHERE lower(table_name) = lower('" +
				              OpenIVMUtils::EscapeSingleQuotes(left_table_name) + "') AND table_schema = '" +
				              OpenIVMUtils::EscapeSingleQuotes(current_schema) + "' ORDER BY ordinal_position");
				auto add_semi_anti_left_col = [&](const string &col_name) {
					for (auto &existing : semi_anti_left_cols) {
						if (StringUtil::CIEquals(existing, col_name)) {
							return;
						}
					}
					semi_anti_left_cols.push_back(col_name);
				};
				if (!semi_anti_extract.output_cols.empty()) {
					for (auto &col : semi_anti_extract.output_cols) {
						add_semi_anti_left_col(col);
					}
					// The aux state must evaluate the semi/anti predicate on refresh. If the
					// MV projects only a subset of the left table, keep the base columns as
					// hidden aux-state key columns as well (e.g. output C_ID/C_LAST, predicate
					// uses C_W_ID). The user-facing MV still emits only output_cols.
					if (semi_anti_extract.left_table.find('(') == string::npos && !col_result->HasError() &&
					    col_result->RowCount() > 0) {
						for (idx_t i = 0; i < col_result->RowCount(); i++) {
							add_semi_anti_left_col(col_result->GetValue(0, i).ToString());
						}
					}
					semi_anti_recompute_supported = !semi_anti_left_cols.empty();
				} else if (!col_result->HasError() && col_result->RowCount() > 0) {
					for (idx_t i = 0; i < col_result->RowCount(); i++) {
						add_semi_anti_left_col(col_result->GetValue(0, i).ToString());
					}
					semi_anti_recompute_supported = true;
				}
			}
		}

		bool distinct_incremental_supported = false;
		if (analysis.found_distinct && !prelim_view_model.distinct_at_top && analysis.found_aggregation) {
			Value aux_val;
			bool aux_enabled = false;
			if (context.TryGetCurrentSetting("ivm_distinct_aux_state", aux_val) && !aux_val.IsNull()) {
				aux_enabled = aux_val.GetValue<bool>();
			}
			bool single_source = table_names.size() == 1;
			if (aux_enabled && single_source) {
				vector<string> dcols;
				string d_input_sql, d_source, d_filter;
				if (!ExtractInnerDistinct(original_view_query, dcols, d_input_sql, d_source, d_filter)) {
					OPENIVM_DEBUG_PRINT("[CREATE MV] DISTINCT_INCREMENTAL extractor failed — demoting to "
					                    "GROUP_RECOMPUTE\n");
				} else {
					distinct_extracted_cols = std::move(dcols);
					distinct_extracted_input_sql = std::move(d_input_sql);
					distinct_extracted_source = std::move(d_source);
					distinct_extracted_filter = std::move(d_filter);
				}
			}

			if (!distinct_extracted_cols.empty()) {
				LogicalAggregate *outer_agg = FindOuterAggregate(plan.get());
				int sum_count = 0;
				bool unsupported_agg = false;
				if (outer_agg) {
					for (auto &expr : outer_agg->expressions) {
						if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
							continue;
						}
						auto &bound = expr->Cast<BoundAggregateExpression>();
						const string &fname = bound.function.name;
						if (fname == "count_star") {
							continue; // injected by IvmPlanRewrite — fine
						}
						if (fname != "sum") {
							unsupported_agg = true;
							break;
						}
						if (bound.children.empty() ||
						    bound.children[0]->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
							unsupported_agg = true;
							break;
						}
						auto &bcr = bound.children[0]->Cast<BoundColumnRefExpression>();
						distinct_sum_arg = bcr.alias.empty() ? bcr.GetName() : bcr.alias;
						distinct_sum_out = bound.alias;
						sum_count++;
					}
				}
				if (!outer_agg || unsupported_agg || sum_count != 1) {
					distinct_sum_arg.clear();
					distinct_sum_out.clear();
					OPENIVM_DEBUG_PRINT("[CREATE MV] DISTINCT_INCREMENTAL outer-agg not single-SUM — demoting "
					                    "to GROUP_RECOMPUTE\n");
				} else if (distinct_sum_out.empty()) {
					// `SUM(c) AS s` puts the alias `s` on the SELECT-list BCR above the
					// aggregate, not on the BoundAggregateExpression itself. Recover it
					// from output_names: the SUM output column is the first non-group
					// position (group cols come first in the data table layout).
					if (prelim_view_model.group_columns.size() < output_names.size()) {
						distinct_sum_out = output_names[prelim_view_model.group_columns.size()];
					}
				}
				distinct_incremental_supported = !distinct_sum_arg.empty() && !distinct_sum_out.empty();
			}
		}

		model_input.distinct_incremental_supported = distinct_incremental_supported;
		model_input.semi_anti_recompute_supported = semi_anti_recompute_supported;
		auto view_model = BuildDeltaViewModel(model_input);
		IVMType ivm_type = view_model.type;
		auto aggregate_columns = view_model.group_columns;
		auto aggregate_types = view_model.aggregate_types;
		auto window_partition_columns = view_model.window_partition_columns;
		bool has_minmax_metadata = view_model.has_minmax_metadata;
		if (ivm_type != IVMType::DISTINCT_INCREMENTAL) {
			distinct_extracted_cols.clear();
			distinct_extracted_input_sql.clear();
			distinct_extracted_source.clear();
			distinct_extracted_filter.clear();
			distinct_sum_arg.clear();
			distinct_sum_out.clear();
		}
		if (ivm_type != IVMType::SEMI_ANTI_RECOMPUTE) {
			semi_anti_left_cols.clear();
		}
		if (view_model.warn_unsupported_incremental) {
			Printer::Print("Warning: materialized view '" + view_name +
			               "' uses constructs not supported for incremental maintenance. "
			               "Full refresh will be used.");
		}
		if (view_model.warn_unrecognized_pattern) {
			Printer::Print("Warning: materialized view '" + view_name +
			               "' has an unrecognized query pattern. Full refresh will be used.");
		}

		OPENIVM_DEBUG_PRINT("[CREATE MV] Detected IVM type: %s (aggregation=%d, projection=%d, group_cols=%zu)\n",
		                    IVMTypeName(ivm_type), (int)analysis.found_aggregation, (int)analysis.found_projection,
		                    aggregate_columns.size());
		for (auto reason : view_model.strategy_reasons) {
			OPENIVM_DEBUG_PRINT("[CREATE MV] Strategy reason: %s\n", DeltaStrategyReasonName(reason));
		}
		OPENIVM_DEBUG_PRINT("[CREATE MV] Source tables:");
		for (const auto &t : table_names) {
			OPENIVM_DEBUG_PRINT(" %s", t.c_str());
		}
		OPENIVM_DEBUG_PRINT("\n");

		// Build DDL vector directly in memory
		vector<string> ddl;
		vector<string> cleanup_ddl;
		vector<string> metadata_ddl;
		vector<string> aux_metadata_ddl;
		auto add_cleanup = [&](const string &query) {
			cleanup_ddl.push_back(string(OPENIVM_DDL_CLEANUP_PREFIX) + query);
		};

		// --- System tables DDL ---
		// Matcher metadata columns (signature_hash..nullified_columns_json) stay
		// NULL unless ivm_enable_view_matching=true; populated by Stage I wiring.
		ddl.push_back("create table if not exists " + string(ivm::VIEWS_TABLE) +
		              " (view_name varchar primary key, sql_string varchar, type tinyint,"
		              " has_minmax boolean default false, has_left_join boolean default false,"
		              " last_update timestamp, refresh_interval bigint default null,"
		              " refresh_in_progress boolean default false,"
		              " group_columns varchar default null,"
		              " aggregate_types varchar default null,"
		              " having_predicate varchar default null,"
		              " has_full_outer boolean default false,"
		              " full_outer_join_cols varchar default null,"
		              " signature_hash ubigint default null,"
		              " canonical_plan_blob blob default null,"
		              " output_columns_json varchar default null,"
		              " predicate_summary_json varchar default null,"
		              " fd_summary_json varchar default null,"
		              " source_tables_json varchar default null,"
		              " aggregate_decomposition_json varchar default null,"
		              " nullified_columns_json varchar default null,"
		              " distinct_aux_meta_json varchar default null,"
		              " semi_anti_aux_meta_json varchar default null)");
		// Forward-compat ALTER for existing DBs that pre-date `distinct_aux_meta_json`
		// (the CREATE IF NOT EXISTS above is a no-op when the table exists with the older schema).
		ddl.push_back("alter table " + string(ivm::VIEWS_TABLE) +
		              " add column if not exists distinct_aux_meta_json varchar default null");
		ddl.push_back("alter table " + string(ivm::VIEWS_TABLE) +
		              " add column if not exists semi_anti_aux_meta_json varchar default null");
		if (!ivm_parse_data.is_replace) {
			string escaped_view_name = OpenIVMUtils::EscapeSingleQuotes(view_name);
			string escaped_data_table = OpenIVMUtils::EscapeSingleQuotes(IVMTableNames::DataTableName(view_name));
			string stale_mv_condition =
			    "view_name = '" + escaped_view_name +
			    "' AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" + escaped_view_name +
			    "') AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" + escaped_data_table +
			    "')";
			// CREATE MV executes as multiple catalog statements. If a process dies or loses
			// a DuckDB file lock after writing metadata but before creating the physical
			// DuckLake/default-catalog objects, a retry should clean that stale row rather
			// than report a misleading duplicate MV.
			ddl.push_back("DELETE FROM " + string(ivm::VIEWS_TABLE) + " WHERE " + stale_mv_condition);
			ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + string(ivm::VIEWS_TABLE) +
			              " WHERE view_name = '" + escaped_view_name +
			              "') THEN error('Duplicate key: materialized view \"" + escaped_view_name +
			              "\" already exists') ELSE NULL END");
		}

		// Refresh hooks: extensions can register custom SQL to run on MV refresh
		// mode: 'replace' (instead of ivm), 'before' (before ivm), 'after' (after ivm)
		ddl.push_back("create table if not exists _duckdb_ivm_refresh_hooks"
		              " (view_name varchar primary key, hook_sql varchar not null,"
		              " mode varchar not null default 'after')");

		ddl.push_back("create table if not exists " + string(ivm::DELTA_TABLES_TABLE) +
		              " (view_name varchar, table_name varchar, last_update timestamp,"
		              " catalog_type varchar default 'duckdb', last_snapshot_id bigint default null,"
		              " last_refresh_ts timestamp default null,"
		              " pending_row_estimate bigint default null,"
		              " pending_estimate_ts timestamp default null,"
		              " source_catalog varchar default null,"
		              " source_schema varchar default null,"
		              " primary key(view_name, table_name))");
		// Backfill for existing databases without the columns (added post-release).
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists last_refresh_ts timestamp default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists pending_row_estimate bigint default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists pending_estimate_ts timestamp default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists source_catalog varchar default null");
		ddl.push_back("alter table " + string(ivm::DELTA_TABLES_TABLE) +
		              " add column if not exists source_schema varchar default null");

		// Refresh history: stores execution stats for learned cost model calibration.
		// Stage A.5 adds `strategy` (default 'incremental') for per-strategy regression.
		ddl.push_back("create table if not exists " + string(ivm::HISTORY_TABLE) +
		              " (view_name varchar, refresh_timestamp timestamp default current_timestamp,"
		              " method varchar, ivm_compute_est double, ivm_upsert_est double,"
		              " recompute_compute_est double, recompute_replace_est double,"
		              " actual_duration_ms bigint,"
		              " strategy varchar default 'incremental',"
		              " primary key(view_name, refresh_timestamp))");
		ddl.push_back("alter table " + string(ivm::HISTORY_TABLE) +
		              " add column if not exists strategy varchar default 'incremental'");
		ddl.push_back("create table if not exists " + string(ivm::PROFILE_TABLE) +
		              " (refresh_id varchar, view_name varchar,"
		              " profile_timestamp timestamp default current_timestamp,"
		              " step_order integer, step_name varchar, duration_ms bigint, detail varchar,"
		              " primary key(refresh_id, step_order))");

		// --- OR REPLACE: drop old MV if it exists ---
		if (ivm_parse_data.is_replace) {
			string qvn_drop = view_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(view_name);
			string qdt_drop =
			    internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(view_name));
			string qdv_drop =
			    internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
			// Drop the user-facing VIEW, data table, and delta view table
			ddl.push_back("DROP VIEW IF EXISTS " + qvn_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdt_drop);
			ddl.push_back("DROP TABLE IF EXISTS " + qdv_drop);
			// Clean metadata (the INSERT OR REPLACE below handles _duckdb_ivm_views)
			ddl.push_back("DELETE FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
			              OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
			ddl.push_back("DELETE FROM " + string(ivm::HISTORY_TABLE) + " WHERE view_name = '" +
			              OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		}

		// Store the LPTS query in metadata — it has hidden columns (DISTINCT count, AVG sum/count,
		// LEFT JOIN key) and preserves user column names.
		string refresh_val = ivm_parse_data.refresh_interval > 0 ? to_string(ivm_parse_data.refresh_interval) : "null";
		// Store GROUP BY or PARTITION BY columns (mutually exclusive in our type system).
		// For WINDOW_PARTITION, store the PARTITION BY columns so the upsert compiler
		// can identify affected partitions from deltas.
		string group_cols_val = "null";
		auto &cols_to_store = analysis.found_window ? window_partition_columns : aggregate_columns;
		if (!cols_to_store.empty()) {
			group_cols_val = "'";
			for (size_t i = 0; i < cols_to_store.size(); i++) {
				if (i > 0) {
					group_cols_val += ",";
				}
				group_cols_val += OpenIVMUtils::EscapeSingleQuotes(cols_to_store[i]);
			}
			group_cols_val += "'";
		}
		// Store per-column aggregate types for insert-only MIN/MAX optimization
		string agg_types_val = "null";
		if (!aggregate_types.empty()) {
			agg_types_val = "'";
			for (size_t i = 0; i < aggregate_types.size(); i++) {
				if (i > 0) {
					agg_types_val += ",";
				}
				agg_types_val += aggregate_types[i];
			}
			agg_types_val += "'";
		}
		string having_val =
		    having_predicate.empty() ? "null" : "'" + OpenIVMUtils::EscapeSingleQuotes(having_predicate) + "'";

		// Extract FULL OUTER JOIN condition: "left_table:left_col,right_table:right_col"
		string full_outer_join_cols_val = "null";
		if (analysis.found_full_outer && !view_model.full_outer_join_cols.empty()) {
			full_outer_join_cols_val = "'" + OpenIVMUtils::EscapeSingleQuotes(view_model.full_outer_join_cols) + "'";
		}

		// 10 trailing NULLs: 8 matcher metadata columns + distinct/semi-anti aux metadata.
		// Matcher metadata is populated by the Stage I block below when
		// ivm_enable_view_matching=true. distinct_aux_meta_json is populated by a
		// follow-up UPDATE if ivm_type == DISTINCT_INCREMENTAL and the extractor
		// recognised the DISTINCT shape.
		metadata_ddl.push_back("insert or replace into " + string(ivm::VIEWS_TABLE) + " values ('" + view_name +
		                       "', '" + OpenIVMUtils::EscapeSingleQuotes(view_query) + "', " +
		                       to_string((int)ivm_type) + ", " + (has_minmax_metadata ? "true" : "false") + ", " +
		                       (analysis.found_left_join ? "true" : "false") + ", now(), " + refresh_val + ", false, " +
		                       group_cols_val + ", " + agg_types_val + ", " + having_val + ", " +
		                       (analysis.found_full_outer ? "true" : "false") + ", " + full_outer_join_cols_val +
		                       ", NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)");

		Value match_flag_val;
		bool view_matching_enabled = context.TryGetCurrentSetting("ivm_enable_view_matching", match_flag_val) &&
		                             !match_flag_val.IsNull() && BooleanValue::Get(match_flag_val);
		if (view_matching_enabled) {
			// table_names may include _ivm_data_<x> when this MV reads from
			// another MV (DuckDB binds the user-facing view to its data table).
			// Strip the prefix so source_tables_json reflects user-facing names
			// and the dependency-edge lookup hits a registered MV row.
			vector<string> sorted_tables;
			sorted_tables.reserve(table_names.size());
			for (const auto &t : table_names) {
				if (StringUtil::StartsWith(t, ivm::DATA_TABLE_PREFIX)) {
					sorted_tables.push_back(t.substr(strlen(ivm::DATA_TABLE_PREFIX)));
				} else {
					sorted_tables.push_back(t);
				}
			}
			std::sort(sorted_tables.begin(), sorted_tables.end());
			// Build JSON without inner escaping; outer EscapeSingleQuotes runs
			// once when the SQL is assembled.
			string src_json = "[";
			for (idx_t i = 0; i < sorted_tables.size(); i++) {
				if (i) {
					src_json += ",";
				}
				src_json += "\"" + sorted_tables[i] + "\"";
			}
			src_json += "]";
			metadata_ddl.push_back("UPDATE " + string(ivm::VIEWS_TABLE) + " SET source_tables_json = '" +
			                       OpenIVMUtils::EscapeSingleQuotes(src_json) + "' WHERE view_name = '" +
			                       OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
			// Replace any prior edges for this child, then re-emit. INSERTs are
			// conditional on the source being a registered MV (the SELECT
			// returns zero rows for non-MV sources).
			metadata_ddl.push_back("DELETE FROM " + string(ivm::MV_DEPS_TABLE) + " WHERE child_view = '" +
			                       OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
			for (const auto &t : sorted_tables) {
				metadata_ddl.push_back("INSERT INTO " + string(ivm::MV_DEPS_TABLE) +
				                       " (parent_view, child_view, edge_kind) SELECT view_name, '" +
				                       OpenIVMUtils::EscapeSingleQuotes(view_name) + "', 'direct' FROM " +
				                       string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
				                       OpenIVMUtils::EscapeSingleQuotes(t) + "'");
			}
		}

		// DISTINCT_INCREMENTAL: create the per-tuple count auxiliary table and store its
		// metadata so refresh-time can find the source SQL, the column list, and the aux
		// table name. The aux table is populated from the (DISTINCT-stripped) input SQL
		// at CREATE time; refresh-time MERGE keeps it in sync with delta multiplicities.
		if (ivm_type == IVMType::DISTINCT_INCREMENTAL) {
			string aux_table = "_ivm_distinct_count_" + view_name;
			string cols_csv;
			for (size_t i = 0; i < distinct_extracted_cols.size(); i++) {
				if (i > 0) {
					cols_csv += ", ";
				}
				cols_csv += distinct_extracted_cols[i];
			}
			// CREATE+POPULATE the aux table from the extracted DISTINCT input SQL.
			// _count is a signed BIGINT (deltas can transiently push it negative during
			// concurrent refreshes; the post-update DELETE drops rows whose count <= 0).
			string aux_create = "create table if not exists " + internal_catalog_prefix +
			                    KeywordHelper::WriteOptionallyQuoted(aux_table) + " as select " + cols_csv +
			                    ", count(*)::BIGINT as _count from (" + distinct_extracted_input_sql + ") group by " +
			                    cols_csv;
			ddl.push_back(aux_create);
			add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
			            KeywordHelper::WriteOptionallyQuoted(aux_table));
			// Build a JSON metadata blob that the refresh-time compile reads back.
			string cols_json = "[";
			for (size_t i = 0; i < distinct_extracted_cols.size(); i++) {
				if (i > 0) {
					cols_json += ",";
				}
				cols_json += JsonQuote(distinct_extracted_cols[i]);
			}
			cols_json += "]";
			string meta_json = "{\"aux_table\":" + JsonQuote(aux_table) + ",\"cols\":" + cols_json +
			                   ",\"input_sql\":" + JsonQuote(distinct_extracted_input_sql) +
			                   ",\"source\":" + JsonQuote(distinct_extracted_source) +
			                   ",\"filter\":" + JsonQuote(distinct_extracted_filter) +
			                   ",\"sum_arg\":" + JsonQuote(distinct_sum_arg) +
			                   ",\"sum_out\":" + JsonQuote(distinct_sum_out) + "}";
			aux_metadata_ddl.push_back("UPDATE " + string(ivm::VIEWS_TABLE) + " SET distinct_aux_meta_json = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(meta_json) + "' WHERE view_name = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		}

		if (ivm_type == IVMType::SEMI_ANTI_RECOMPUTE) {
			string aux_table = "_ivm_semi_anti_state_" + view_name;
			auto qualify_source_table = [&](const string &table_name) {
				if (current_catalog.empty() || current_catalog == default_db || table_name.find('.') != string::npos ||
				    table_name.find('(') != string::npos) {
					return table_name;
				}
				return current_catalog + "." + current_schema + "." + table_name;
			};
			string left_source_table = qualify_source_table(semi_anti_extract.left_table);
			string right_source_table = qualify_source_table(semi_anti_extract.right_table);
			string left_cols_csv;
			string left_source_select;
			string left_cols_qualified;
			string left_cols_lc;
			string left_cols_mc;
			string lc_mc_match;
			vector<string> semi_anti_left_exprs;
			for (size_t i = 0; i < semi_anti_left_cols.size(); i++) {
				if (i > 0) {
					left_cols_csv += ", ";
					left_source_select += ", ";
					left_cols_qualified += ", ";
					left_cols_lc += ", ";
					left_cols_mc += ", ";
					lc_mc_match += " AND ";
				}
				string qcol = KeywordHelper::WriteOptionallyQuoted(semi_anti_left_cols[i]);
				left_cols_csv += qcol;
				string source_expr = semi_anti_extract.left_alias + "." + qcol;
				for (size_t j = 0; j < semi_anti_extract.output_cols.size(); j++) {
					if (StringUtil::CIEquals(semi_anti_extract.output_cols[j], semi_anti_left_cols[i]) &&
					    j < semi_anti_extract.output_exprs.size()) {
						source_expr = semi_anti_extract.output_exprs[j];
						break;
					}
				}
				semi_anti_left_exprs.push_back(source_expr);
				left_source_select += source_expr + " AS " + qcol;
				left_cols_qualified += semi_anti_extract.left_alias + "." + qcol;
				left_cols_lc += "lc." + qcol;
				left_cols_mc += "mc." + qcol;
				lc_mc_match += "lc." + qcol + " IS NOT DISTINCT FROM mc." + qcol;
			}
			string left_source_filter;
			if (!semi_anti_extract.post_filter.empty()) {
				left_source_filter = " WHERE " + semi_anti_extract.post_filter;
			}
			string aux_create = "create table if not exists " + internal_catalog_prefix +
			                    KeywordHelper::WriteOptionallyQuoted(aux_table) + " as with left_source as (select " +
			                    left_source_select + " from " + left_source_table + " " + semi_anti_extract.left_alias +
			                    left_source_filter + "), left_counts as (select " + left_cols_csv +
			                    ", count(*)::BIGINT as _left_count from left_source group by " + left_cols_csv +
			                    "), match_counts as (select " + left_cols_qualified +
			                    ", count(*)::BIGINT as _match_count from (select distinct " + left_cols_csv +
			                    " from left_source) " + semi_anti_extract.left_alias + " join " + right_source_table +
			                    " " + semi_anti_extract.right_alias + " on " + semi_anti_extract.predicate +
			                    " group by " + left_cols_qualified + ") select " + left_cols_lc +
			                    ", lc._left_count, coalesce(mc._match_count, 0)::BIGINT as _match_count from "
			                    "left_counts lc left join match_counts mc on " +
			                    lc_mc_match;
			ddl.push_back(aux_create);
			add_cleanup("DROP TABLE IF EXISTS " + internal_catalog_prefix +
			            KeywordHelper::WriteOptionallyQuoted(aux_table));

			string left_cols_json = "[";
			string left_exprs_json = "[";
			for (size_t i = 0; i < semi_anti_left_cols.size(); i++) {
				if (i > 0) {
					left_cols_json += ",";
					left_exprs_json += ",";
				}
				left_cols_json += JsonQuote(semi_anti_left_cols[i]);
				left_exprs_json += JsonQuote(semi_anti_left_exprs[i]);
			}
			left_cols_json += "]";
			left_exprs_json += "]";
			string output_cols_json = "[";
			for (size_t i = 0; i < semi_anti_extract.output_cols.size(); i++) {
				if (i > 0) {
					output_cols_json += ",";
				}
				output_cols_json += JsonQuote(semi_anti_extract.output_cols[i]);
			}
			output_cols_json += "]";
			string meta_json =
			    "{\"aux_table\":" + JsonQuote(aux_table) + ",\"join_type\":" + JsonQuote(semi_anti_extract.join_type) +
			    ",\"left_table\":" + JsonQuote(semi_anti_extract.left_table) +
			    ",\"left_alias\":" + JsonQuote(semi_anti_extract.left_alias) +
			    ",\"right_table\":" + JsonQuote(semi_anti_extract.right_table) +
			    ",\"right_alias\":" + JsonQuote(semi_anti_extract.right_alias) +
			    ",\"predicate\":" + JsonQuote(semi_anti_extract.predicate) +
			    ",\"post_filter\":" + JsonQuote(semi_anti_extract.post_filter) + ",\"left_cols\":" + left_cols_json +
			    ",\"left_exprs\":" + left_exprs_json + ",\"output_cols\":" + output_cols_json + "}";
			aux_metadata_ddl.push_back("UPDATE " + string(ivm::VIEWS_TABLE) + " SET semi_anti_aux_meta_json = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(meta_json) + "' WHERE view_name = '" +
			                           OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		}

		// Classify each base table by catalog type (duckdb vs ducklake).
		// DuckLake tables use native change tracking; DuckDB tables use delta tables.
		//
		// Catalog::GetEntry inside BeginTransaction() cannot see DuckLake entries:
		// DuckLake requires its own transaction protocol. Walk the logical plan's
		// DUCKLAKE_SCAN nodes instead — same approach used in ducklake_join.cpp.
		unordered_map<string, OpenIVMDuckLakeTableInfo> dl_table_info; // keyed by lowercased name
		CollectDuckLakeTables(plan.get(), current_catalog, dl_table_info);

		unordered_set<string> ducklake_tables;
		// Single snapshot query per DuckLake catalog (all tables share the same snapshot).
		string dl_snapshot_val = "null";
		if (!dl_table_info.empty()) {
			// Use the first entry's catalog — all source tables in one MV share one catalog.
			string cat = dl_table_info.begin()->second.catalog_name;
			auto snap_result = con.Query("SELECT id FROM " + cat + ".current_snapshot()");
			if (!snap_result->HasError() && snap_result->RowCount() > 0) {
				dl_snapshot_val = snap_result->GetValue(0, 0).ToString();
			}
		}

		vector<string> source_metadata_ddl;
		unordered_set<string> inserted_meta_table_names;
		for (const auto &table_name : table_names) {
			string catalog_type = "duckdb";
			string snapshot_val = "null";
			string meta_table_name = OpenIVMUtils::DeltaName(table_name);
			string source_catalog_val = current_catalog.empty() ? "memory" : current_catalog;
			string source_schema_val = current_schema.empty() ? "main" : current_schema;

			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			auto source_info_it = source_table_info.find(table_name);
			if (source_info_it != source_table_info.end()) {
				source_catalog_val = source_info_it->second.catalog_name;
				source_schema_val = source_info_it->second.schema_name;
			}
			auto it = dl_table_info.find(table_lc);
			if (it != dl_table_info.end()) {
				catalog_type = "ducklake";
				meta_table_name = it->second.table_name; // case-preserved name
				ducklake_tables.insert(it->second.table_name);
				ducklake_tables.insert(table_name); // also insert SQL-parsed name
				snapshot_val = dl_snapshot_val;
				source_catalog_val = it->second.catalog_name;
				source_schema_val = it->second.schema_name;
				OPENIVM_DEBUG_PRINT("[CREATE MV] DuckLake table '%s' → meta_name='%s', snap=%s\n", table_name.c_str(),
				                    meta_table_name.c_str(), snapshot_val.c_str());
			}

			// A single physical source can appear under multiple logical names after planning.
			// DuckLake chained views are the common case: the query can contain both the
			// user-facing MV name and its backing _ivm_data_* table. Metadata is keyed by
			// (view_name, table_name), so emit only one dependency row for the canonical
			// metadata table name after the DuckLake mapping above.
			if (!inserted_meta_table_names.insert(meta_table_name).second) {
				continue;
			}

			source_metadata_ddl.push_back("insert or replace into " + string(ivm::DELTA_TABLES_TABLE) +
			                              " (view_name, table_name, last_update, catalog_type, last_snapshot_id, "
			                              "last_refresh_ts, source_catalog, source_schema) "
			                              "values ('" +
			                              view_name + "', '" + OpenIVMUtils::EscapeSingleQuotes(meta_table_name) +
			                              "', now(), '" + catalog_type + "', " + snapshot_val + ", now(), '" +
			                              OpenIVMUtils::EscapeSingleQuotes(source_catalog_val) + "', '" +
			                              OpenIVMUtils::EscapeSingleQuotes(source_schema_val) + "')");
		}

		// --- Compiled DDL (MV creation, delta tables, delta view) ---
		// Physical data table stores all columns (including _ivm_* internal cols).
		// DuckLake-targeted MVs publish only the user-facing view in DuckLake. Internal
		// state remains in the physical default catalog, because DuckDB/DuckLake cannot
		// roll back one atomic transaction across both catalogs.
		string stage_table = "_ivm_stage_" + view_name;
		string qstage =
		    QualifiedTablePrefix(default_db, default_schema) + KeywordHelper::WriteOptionallyQuoted(stage_table);
		// The view_query may contain unqualified base-table references (e.g. `FROM WAREHOUSE`
		// when the user wrote the MV under `USE dl.main`). The DDL executor's fresh
		// Connection starts in the physical-default catalog, so apply USE before CREATE
		// TABLE AS so those unqualified names resolve in the MV's catalog.
		if (!current_catalog.empty() && current_catalog != default_db) {
			ddl.push_back("use " + current_catalog_schema);
		}
		if (!view_catalog_prefix.empty()) {
			// DuckLake can self-lock when one statement reads DuckLake tables and commits
			// a DuckLake CTAS. Stage the SELECT into the physical default DB first, then
			// persist OpenIVM's internal data table in that same physical catalog. The
			// DuckLake write is limited to the final user-facing view metadata.
			ddl.push_back("DROP TABLE IF EXISTS " + qstage);
			ddl.push_back("create table " + qstage + " as " + view_query);
			ddl.push_back(OPENIVM_DDL_RECONNECT);
			ddl.push_back("use " + default_catalog_schema);
			ddl.push_back("create table " + qdt + " as select * from " + qstage);
			ddl.push_back("DROP TABLE IF EXISTS " + qstage);
		} else {
			ddl.push_back("create table " + qdt + " as " + view_query);
		}
		add_cleanup("DROP VIEW IF EXISTS " + qvn);
		add_cleanup("DROP TABLE IF EXISTS " + qdt);
		add_cleanup("DROP TABLE IF EXISTS " + qstage);
		if (pac_loaded) {
			ddl.push_back("SET pac_check = false");
			ddl.push_back("SET pac_rewrite = false");
		}

		// User-facing VIEW hides internal _ivm_* columns via EXCLUDE.
		// If LPTS fell back to the original SQL, the data table has only the
		// user-visible columns — no `_ivm_*` columns even if the rewritten plan
		// would have added them via AVG/STDDEV decomposition. Skip the EXCLUDE
		// list in that case; otherwise CREATE VIEW fails on nonexistent columns.
		{
			// Collect internal column names from the LPTS output
			vector<string> internal_cols;
			if (!lpts_fallback) {
				for (auto &name : output_names) {
					if (IVMTableNames::IsInternalColumn(name)) {
						internal_cols.push_back(name);
					}
				}
			}
			string having_where = having_predicate.empty() ? "" : " where " + having_predicate;
			// For aggregate+top-k the VIEW appends ORDER BY … LIMIT k after the HAVING WHERE.
			// The data table stores ALL groups; the ORDER BY LIMIT is applied at read time.
			string view_tail = having_where + (top_k_suffix.empty() ? "" : " " + top_k_suffix);
			if (internal_cols.empty()) {
				ddl.push_back("create view " + qvn + " as select * from " + qdt + view_tail);
			} else {
				string exclude_list;
				for (size_t i = 0; i < internal_cols.size(); i++) {
					if (i > 0) {
						exclude_list += ", ";
					}
					exclude_list += internal_cols[i];
				}
				ddl.push_back("create view " + qvn + " as select * exclude (" + exclude_list + ") from " + qdt +
				              view_tail);
			}
		}

		for (const auto &table_name : table_names) {
			// DuckLake tables don't need delta tables — change tracking is native.
			// `ducklake_tables` stores the catalog-normalized (lowercase) name, so
			// compare against a normalized copy of the SQL-parsed name.
			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			if (ducklake_tables.count(table_name) || ducklake_tables.count(table_lc)) {
				OPENIVM_DEBUG_PRINT("[CREATE MV] Skipping delta table for DuckLake table '%s'\n", table_name.c_str());
				continue;
			}

			Value catalog_value;
			Value schema_value;
			auto source_it = source_table_info.find(table_name);
			if (source_it != source_table_info.end()) {
				catalog_value = Value(source_it->second.catalog_name);
				schema_value = Value(source_it->second.schema_name);
			}

			if (catalog_value.IsNull() && !context.db->config.options.database_path.empty()) {
				// Look up the catalog name for this table via Catalog API
				con.BeginTransaction();
				auto entry = Catalog::GetEntry<TableCatalogEntry>(*con.context, INVALID_CATALOG, DEFAULT_SCHEMA,
				                                                  table_name, OnEntryNotFound::RETURN_NULL);
				if (entry) {
					catalog_value = Value(entry->ParentCatalog().GetName());
					schema_value = Value(entry->schema.name);
				}
				con.Rollback();
			}
			if (catalog_value.IsNull()) {
				catalog_value = Value(current_catalog.empty() ? "memory" : current_catalog);
			}

			if (schema_value.IsNull()) {
				schema_value = Value(current_schema.empty() ? "main" : current_schema);
			}

			auto catalog_schema = QualifiedTablePrefix(catalog_value.ToString(), schema_value.ToString());

			ddl.push_back("create table if not exists " + catalog_schema +
			              KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(table_name)) +
			              " as select *, 1::INTEGER as " + string(ivm::MULTIPLICITY_COL) + ", now()::timestamp as " +
			              string(ivm::TIMESTAMP_COL) + " from " + catalog_schema +
			              KeywordHelper::WriteOptionallyQuoted(table_name) + " limit 0");
		}

		// Delta table for the MV — based on the DATA table (has all columns)
		string qdv = internal_catalog_prefix + KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(view_name));
		ddl.push_back("create table if not exists " + qdv + " as select *, 1::INTEGER as " +
		              string(ivm::MULTIPLICITY_COL) + ", now()::timestamp as " + string(ivm::TIMESTAMP_COL) + " from " +
		              qdt + " limit 0");
		ddl.push_back("alter table " + qdv + " alter " + string(ivm::TIMESTAMP_COL) + " set default now()");
		add_cleanup("DROP TABLE IF EXISTS " + qdv);

		// --- Index DDL (for aggregate group queries) ---
		// DuckLake source scans do not support indexes. DuckLake-targeted MVs store the
		// internal data table in the physical default catalog, but source-side DuckLake
		// change tracking still skips this optional index.
		if ((ivm_type == IVMType::AGGREGATE_GROUP || ivm_type == IVMType::AGGREGATE_HAVING) &&
		    !aggregate_columns.empty() && ducklake_tables.empty() && view_catalog_prefix.empty()) {
			string index_name = KeywordHelper::WriteOptionallyQuoted(data_table + ivm::INDEX_SUFFIX);
			string index_query_view = "create unique index " + index_name + " on " + qdt + "(";
			for (size_t i = 0; i < aggregate_columns.size(); i++) {
				index_query_view += KeywordHelper::WriteOptionallyQuoted(aggregate_columns[i]);
				if (i != aggregate_columns.size() - 1) {
					index_query_view += ", ";
				}
			}
			index_query_view += ")";
			ddl.push_back(index_query_view);
		}

		// Restore physical-default catalog so subsequent unqualified references to
		// system tables (_duckdb_ivm_delta_tables, etc.) resolve correctly. The USE
		// inserted before `create table qdt as view_query` routed unqualified base
		// tables through the user's catalog; flip back for the metadata UPDATE below.
		if (!current_catalog.empty() && current_catalog != default_db) {
			ddl.push_back("use " + default_catalog_schema);
		}

		// Record source-table metadata only after physical MV objects exist. If a later
		// DuckLake publish fails, the DDL executor removes these rows before the retry.
		ddl.insert(ddl.end(), source_metadata_ddl.begin(), source_metadata_ddl.end());
		add_cleanup("DELETE FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
		            OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");

		// After all tables are created and populated, update DuckLake snapshot IDs
		// to the current snapshot. This ensures the first refresh only sees changes
		// made AFTER the MV was created (not the initial data load).
		for (const auto &table_name : table_names) {
			string table_lc = table_name;
			std::transform(table_lc.begin(), table_lc.end(), table_lc.begin(),
			               [](unsigned char c) { return std::tolower(c); });
			if (ducklake_tables.count(table_name) || ducklake_tables.count(table_lc)) {
				// Use the DuckLake catalog for current_snapshot() — NOT view_catalog_prefix
				// or current_catalog. For cross-system MVs (native MV reading from dl.*),
				// view_catalog_prefix is empty and current_catalog is the physical-default
				// (e.g. the file DB) which doesn't have `current_snapshot()`.
				// table_names entries may be lowercased by the parser; the metadata row was
				// inserted above with the case-preserved DuckLake name (`dl_table_info`).
				string table_lc_for_lookup = table_name;
				std::transform(table_lc_for_lookup.begin(), table_lc_for_lookup.end(), table_lc_for_lookup.begin(),
				               [](unsigned char c) { return std::tolower(c); });
				auto info_it = dl_table_info.find(table_lc_for_lookup);
				string meta_table_name = (info_it != dl_table_info.end()) ? info_it->second.table_name : table_name;
				string dl_cat_name = (info_it != dl_table_info.end()) ? info_it->second.catalog_name : "dl";
				ddl.push_back("UPDATE " + string(ivm::DELTA_TABLES_TABLE) + " SET last_snapshot_id = (SELECT id FROM " +
				              dl_cat_name + ".current_snapshot()) WHERE view_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(view_name) + "' AND table_name = '" +
				              OpenIVMUtils::EscapeSingleQuotes(meta_table_name) + "'");
			}
		}

		// Publish the MV metadata last. CREATE MV touches both the physical DuckDB catalog
		// and DuckLake's external metadata catalog, so OpenIVM cannot rely on a single
		// cross-catalog transaction. The executor registers cleanup DDL up front and this
		// late publish keeps incomplete attempts out of _duckdb_ivm_views.
		ddl.insert(ddl.end(), metadata_ddl.begin(), metadata_ddl.end());
		ddl.insert(ddl.end(), aux_metadata_ddl.begin(), aux_metadata_ddl.end());
		add_cleanup("DELETE FROM " + string(ivm::MV_DEPS_TABLE) + " WHERE child_view = '" +
		            OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");
		add_cleanup("DELETE FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
		            OpenIVMUtils::EscapeSingleQuotes(view_name) + "'");

		OPENIVM_DEBUG_PRINT("[CREATE MV] Compiled %lu DDL queries for bind phase\n", (unsigned long)ddl.size());

		// Write reference SQL files if ivm_files_path is set
		Value files_path_val;
		if (context.TryGetCurrentSetting("ivm_files_path", files_path_val) && !files_path_val.IsNull()) {
			string base_path = files_path_val.ToString();
			// System tables DDL (first 3 statements: _duckdb_ivm_views, _duckdb_ivm_refresh_hooks,
			// _duckdb_ivm_delta_tables)
			string system_tables_sql;
			// Compiled queries (everything after the system tables)
			string compiled_sql;
			for (size_t i = 0; i < ddl.size(); i++) {
				if (i < 3) {
					system_tables_sql += ddl[i] + ";\n\n";
				} else {
					compiled_sql += ddl[i] + ";\n\n";
				}
			}
			OpenIVMUtils::WriteFile(base_path + "/ivm_system_tables.sql", false, system_tables_sql);
			OpenIVMUtils::WriteFile(base_path + "/ivm_compiled_queries_" + view_name + ".sql", false, compiled_sql);
		}

		// Pass DDL via result.parameters — the bind function receives them as input.inputs.
		// This replaces the fragile thread-local g_ivm_pending_ddl mechanism.
		for (auto &q : cleanup_ddl) {
			result.parameters.push_back(Value(q));
		}
		for (auto &q : ddl) {
			result.parameters.push_back(Value(q));
		}
	}

	// Return DDL executor table function
	result.function =
	    TableFunction("ivm_ddl_executor", {}, IVMDDLExecuteFunction, IVMDDLBindFunction, IVMFunction::IVMInit);
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;
	return result;
}

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement) {
	return BoundStatement();
}
} // namespace duckdb
