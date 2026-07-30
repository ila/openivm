#include "upsert/refresh_compiler.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_internal.hpp"

#include <cctype>
#include <limits>

namespace duckdb {

// Zero-initialized 64-element float list, used as COALESCE default for NULL list aggregates.
static constexpr const char *ZEROS_LIST = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";
static constexpr idx_t GROUP_RECOMPUTE_DIFF_OCCURRENCE_THRESHOLD = 32;
static constexpr idx_t GROUP_RECOMPUTE_DIFF_SQL_BYTES_THRESHOLD = 16ULL * 1024ULL * 1024ULL;

static idx_t EstimateGroupRecomputeAffectedVariants(const vector<GroupRecomputeDeltaSpec> &delta_table_specs) {
	idx_t variants = 0;
	for (auto &spec : delta_table_specs) {
		variants += spec.source_occurrences == 0 ? 1 : spec.source_occurrences;
	}
	return variants;
}

static bool ShouldUseCurrentDiffGroupRecompute(const string &view_query_sql,
                                               const vector<GroupRecomputeDeltaSpec> &delta_table_specs) {
	idx_t variants = EstimateGroupRecomputeAffectedVariants(delta_table_specs);
	return variants >= GROUP_RECOMPUTE_DIFF_OCCURRENCE_THRESHOLD ||
	       variants * view_query_sql.size() >= GROUP_RECOMPUTE_DIFF_SQL_BYTES_THRESHOLD;
}

static bool IsSqlIdentifierChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_';
}

static bool IsKeywordTokenAt(const string &lower, size_t pos, const string &keyword) {
	if (pos + keyword.size() > lower.size() || lower.compare(pos, keyword.size(), keyword) != 0) {
		return false;
	}
	bool left_boundary = pos == 0 || !IsSqlIdentifierChar(lower[pos - 1]);
	bool right_boundary = pos + keyword.size() == lower.size() || !IsSqlIdentifierChar(lower[pos + keyword.size()]);
	return left_boundary && right_boundary;
}

static bool ContainsKeywordToken(const string &sql, const string &keyword) {
	auto lower = StringUtil::Lower(sql);
	bool in_single_quote = false;
	bool in_double_quote = false;
	for (idx_t i = 0; i + keyword.size() <= lower.size(); i++) {
		char c = lower[i];
		if (c == '\'' && !in_double_quote) {
			if (in_single_quote && i + 1 < lower.size() && lower[i + 1] == '\'') {
				i++;
				continue;
			}
			in_single_quote = !in_single_quote;
			continue;
		}
		if (c == '"' && !in_single_quote) {
			in_double_quote = !in_double_quote;
			continue;
		}
		if (in_single_quote || in_double_quote || !IsKeywordTokenAt(lower, i, keyword)) {
			continue;
		}
		return true;
	}
	return false;
}

static void AdvanceSqlScanner(const string &sql, size_t &i, bool &in_single_quote, bool &in_double_quote,
                              size_t &depth) {
	char c = sql[i];
	if (c == '\'' && !in_double_quote) {
		if (in_single_quote && i + 1 < sql.size() && sql[i + 1] == '\'') {
			i++;
			return;
		}
		in_single_quote = !in_single_quote;
		return;
	}
	if (c == '"' && !in_single_quote) {
		in_double_quote = !in_double_quote;
		return;
	}
	if (in_single_quote || in_double_quote) {
		return;
	}
	if (c == '(') {
		depth++;
	} else if (c == ')' && depth > 0) {
		depth--;
	}
}

static size_t FindTopLevelKeyword(const string &sql, const string &keyword, size_t start_pos = 0) {
	auto lower = StringUtil::Lower(sql);
	bool in_single_quote = false;
	bool in_double_quote = false;
	size_t depth = 0;
	for (size_t i = start_pos; i + keyword.size() <= lower.size(); i++) {
		if (!in_single_quote && !in_double_quote && depth == 0 && IsKeywordTokenAt(lower, i, keyword)) {
			return i;
		}
		AdvanceSqlScanner(sql, i, in_single_quote, in_double_quote, depth);
	}
	return string::npos;
}

static size_t FindFirstTopLevelClause(const string &sql, const vector<string> &keywords, size_t start_pos) {
	size_t best = string::npos;
	for (auto &keyword : keywords) {
		size_t pos = FindTopLevelKeyword(sql, keyword, start_pos);
		if (pos != string::npos && (best == string::npos || pos < best)) {
			best = pos;
		}
	}
	return best;
}

static size_t FindMatchingParen(const string &sql, size_t open_pos) {
	bool in_single_quote = false;
	bool in_double_quote = false;
	size_t depth = 0;
	for (size_t i = open_pos; i < sql.size(); i++) {
		char c = sql[i];
		AdvanceSqlScanner(sql, i, in_single_quote, in_double_quote, depth);
		if (!in_single_quote && !in_double_quote && c == ')' && depth == 0) {
			return i;
		}
	}
	return string::npos;
}

static size_t FindCteBodyOpen(const string &sql, size_t cte_name_pos) {
	auto lower = StringUtil::Lower(sql);
	bool in_single_quote = false;
	bool in_double_quote = false;
	size_t depth = 0;
	for (size_t i = cte_name_pos; i < sql.size(); i++) {
		if (!in_single_quote && !in_double_quote && depth == 0 && IsKeywordTokenAt(lower, i, "as")) {
			size_t body_open = i + strlen("as");
			while (body_open < sql.size() && std::isspace(static_cast<unsigned char>(sql[body_open]))) {
				body_open++;
			}
			return body_open < sql.size() && sql[body_open] == '(' ? body_open : string::npos;
		}
		AdvanceSqlScanner(sql, i, in_single_quote, in_double_quote, depth);
	}
	return string::npos;
}

static bool BodyReadsAggregateCte(const string &body, size_t from_pos) {
	auto lower = StringUtil::Lower(body);
	size_t cursor = from_pos + strlen("from");
	while (cursor < lower.size() && std::isspace(static_cast<unsigned char>(lower[cursor]))) {
		cursor++;
	}
	static const string aggregate_prefix = "aggregate_";
	return cursor + aggregate_prefix.size() <= lower.size() &&
	       lower.compare(cursor, aggregate_prefix.size(), aggregate_prefix) == 0;
}

static bool TryStripAggregateFilterCtes(const string &sql, string &out) {
	auto lower = StringUtil::Lower(sql);
	string result;
	size_t copy_pos = 0;
	size_t search_pos = 0;
	bool changed = false;
	while (true) {
		size_t pos = lower.find("filter_", search_pos);
		if (pos == string::npos) {
			break;
		}
		if (pos > 0 && IsSqlIdentifierChar(lower[pos - 1])) {
			search_pos = pos + strlen("filter_");
			continue;
		}
		size_t body_open = FindCteBodyOpen(sql, pos);
		if (body_open == string::npos) {
			search_pos = pos + strlen("filter_");
			continue;
		}
		size_t body_close = FindMatchingParen(sql, body_open);
		if (body_close == string::npos) {
			return false;
		}
		string body = sql.substr(body_open + 1, body_close - body_open - 1);
		size_t from_pos = FindTopLevelKeyword(body, "from");
		size_t where_pos = FindTopLevelKeyword(body, "where");
		if (from_pos == string::npos || where_pos == string::npos || from_pos > where_pos ||
		    !BodyReadsAggregateCte(body, from_pos)) {
			search_pos = body_close + 1;
			continue;
		}
		string stripped_body = body.substr(0, where_pos);
		StringUtil::Trim(stripped_body);
		result += sql.substr(copy_pos, body_open + 1 - copy_pos);
		result += stripped_body;
		copy_pos = body_close;
		search_pos = body_close + 1;
		changed = true;
	}
	if (!changed) {
		return false;
	}
	result += sql.substr(copy_pos);
	out = std::move(result);
	return true;
}

static bool TryStripTopLevelHaving(const string &sql, string &out) {
	size_t having_pos = FindTopLevelKeyword(sql, "having");
	if (having_pos == string::npos) {
		return false;
	}
	size_t suffix_pos =
	    FindFirstTopLevelClause(sql, {"order", "limit", "union", "except", "intersect"}, having_pos + strlen("having"));
	string prefix = sql.substr(0, having_pos);
	StringUtil::Trim(prefix);
	string suffix;
	if (suffix_pos != string::npos) {
		suffix = sql.substr(suffix_pos);
		StringUtil::Trim(suffix);
	}
	out = prefix;
	if (!suffix.empty()) {
		if (!out.empty()) {
			out += "\n";
		}
		out += suffix;
	}
	return !out.empty() && out != sql;
}

static bool ContainsAggregateFilterCte(const string &sql) {
	auto lower = StringUtil::Lower(sql);
	size_t pos = 0;
	while ((pos = lower.find("filter_", pos)) != string::npos) {
		size_t cte_end = lower.find("),", pos);
		if (cte_end == string::npos) {
			cte_end = lower.size();
		}
		auto filter_body = lower.substr(pos, cte_end - pos);
		if (filter_body.find(" from aggregate_") != string::npos && filter_body.find(" where ") != string::npos) {
			return true;
		}
		pos += string("filter_").size();
	}
	return false;
}

static bool TryBuildGroupRecomputeAffectedQuery(const string &view_query_sql, string &affected_query_sql) {
	affected_query_sql = view_query_sql;
	bool has_aggregate_filter_cte = ContainsAggregateFilterCte(affected_query_sql);
	bool has_having = ContainsKeywordToken(affected_query_sql, "having");
	if (!has_aggregate_filter_cte && !has_having) {
		return true;
	}
	if (has_aggregate_filter_cte) {
		string stripped_filters;
		if (!TryStripAggregateFilterCtes(affected_query_sql, stripped_filters)) {
			return false;
		}
		affected_query_sql = std::move(stripped_filters);
		if (ContainsAggregateFilterCte(affected_query_sql)) {
			return false;
		}
	}
	if (ContainsKeywordToken(affected_query_sql, "having")) {
		string stripped_having;
		if (!TryStripTopLevelHaving(affected_query_sql, stripped_having)) {
			return false;
		}
		affected_query_sql = std::move(stripped_having);
		if (ContainsKeywordToken(affected_query_sql, "having")) {
			return false;
		}
	}
	return true;
}

static string BuildCurrentDiffGroupRecomputeSQL(const string &view_name, const string &data_table,
                                                const string &view_query_sql, const vector<string> &group_columns,
                                                const string &catalog_prefix = "", bool emit_cascade_delta = false) {
	string current_temp = SqlUtils::QuoteIdentifier("openivm_current_" + view_name);
	string affected_temp = SqlUtils::QuoteIdentifier("openivm_affected_" + view_name);
	string group_csv = SqlUtils::JoinQuotedColumns(group_columns);
	string target_match = SqlUtils::BuildNullSafeMatch(group_columns, "openivm_aff", "openivm_tgt");
	string recompute_match = SqlUtils::BuildNullSafeMatch(group_columns, "openivm_aff", "openivm_recompute");

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + current_temp + " AS\n" + view_query_sql + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + affected_temp + " AS\nSELECT DISTINCT " + group_csv +
	       "\nFROM (\n  (SELECT * FROM " + current_temp + " EXCEPT ALL SELECT * FROM " + data_table +
	       ")\n  UNION ALL\n  (SELECT * FROM " + data_table + " EXCEPT ALL SELECT * FROM " + current_temp +
	       ")\n) openivm_changed;\n\n";
	if (emit_cascade_delta) {
		string delta_table = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
		string old_temp = SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + view_name);
		string new_temp = SqlUtils::QuoteIdentifier("openivm_new_" + view_name);
		sql += "CREATE OR REPLACE TEMP TABLE " + old_temp + " AS\nSELECT * FROM " + data_table +
		       " AS openivm_tgt\nWHERE EXISTS (\n  SELECT 1 FROM " + affected_temp + " AS openivm_aff WHERE " +
		       target_match + "\n);\n\n";
		sql += "CREATE OR REPLACE TEMP TABLE " + new_temp + " AS\nSELECT * FROM " + current_temp +
		       " AS openivm_recompute\nWHERE EXISTS (\n  SELECT 1 FROM " + affected_temp + " AS openivm_aff WHERE " +
		       recompute_match + "\n);\n\n";
		sql += "DELETE FROM " + data_table + " AS openivm_tgt\nWHERE EXISTS (\n  SELECT 1 FROM " + affected_temp +
		       " AS openivm_aff WHERE " + target_match + "\n);\n\n";
		sql += "INSERT INTO " + data_table + "\nSELECT * FROM " + new_temp + ";\n";
		sql += "\n" + BuildSignedMultisetDeltaInsertSQL(delta_table, old_temp, new_temp);
		sql += "\nDROP TABLE IF EXISTS " + affected_temp + ";\n";
		sql += "DROP TABLE IF EXISTS " + old_temp + ";\n";
		sql += "DROP TABLE IF EXISTS " + new_temp + ";\n";
		sql += "DROP TABLE IF EXISTS " + current_temp + ";\n";
		return sql;
	}
	sql += "DELETE FROM " + data_table + " AS openivm_tgt\nWHERE EXISTS (\n  SELECT 1 FROM " + affected_temp +
	       " AS openivm_aff WHERE " + target_match + "\n);\n\n";
	sql += "INSERT INTO " + data_table + "\nSELECT * FROM " + current_temp +
	       " AS openivm_recompute\nWHERE EXISTS (\n  SELECT 1 FROM " + affected_temp + " AS openivm_aff WHERE " +
	       recompute_match + "\n);\n\n";
	sql += "DROP TABLE IF EXISTS " + affected_temp + ";\nDROP TABLE IF EXISTS " + current_temp + ";\n";
	return sql;
}

static string BuildUpdatedAggregateColumn(const string &col) {
	return "COALESCE(v." + col + " + d." + col + ", v." + col + ", d." + col + ")";
}

static string BuildNullableSum(const string &sum_expr, const string &count_expr) {
	return "CASE WHEN " + count_expr + " = 0 THEN NULL ELSE " + sum_expr + " END";
}

static string BuildNullSafeExtremumUpdate(const string &col, const string &fn) {
	return "CASE WHEN v." + col + " IS NULL THEN d." + col + " WHEN d." + col + " IS NULL THEN v." + col + " ELSE " +
	       fn + "(v." + col + ", d." + col + ") END";
}

static string SubstituteDerivedOutputColumns(const string &expression_sql,
                                             const unordered_map<string, string> &replacements) {
	string result;
	result.reserve(expression_sql.size());
	for (idx_t i = 0; i < expression_sql.size();) {
		if (expression_sql[i] == '\'') {
			idx_t start = i++;
			while (i < expression_sql.size()) {
				if (expression_sql[i++] != '\'') {
					continue;
				}
				if (i < expression_sql.size() && expression_sql[i] == '\'') {
					i++;
					continue;
				}
				break;
			}
			result.append(expression_sql, start, i - start);
			continue;
		}
		if (expression_sql[i] != '"') {
			result += expression_sql[i++];
			continue;
		}

		idx_t start = i++;
		string identifier;
		bool closed = false;
		while (i < expression_sql.size()) {
			if (expression_sql[i] != '"') {
				identifier += expression_sql[i++];
				continue;
			}
			if (i + 1 < expression_sql.size() && expression_sql[i + 1] == '"') {
				identifier += '"';
				i += 2;
				continue;
			}
			i++;
			closed = true;
			break;
		}
		if (!closed) {
			result.append(expression_sql, start, expression_sql.size() - start);
			break;
		}
		auto replacement = replacements.find(identifier);
		if (replacement != replacements.end()) {
			result += replacement->second;
		} else {
			result.append(expression_sql, start, i - start);
		}
	}
	return result;
}

static string BuildAvgFormula(const string &sum_expr, const string &count_expr) {
	return sum_expr + "::DOUBLE / NULLIF(" + count_expr + ", 0)";
}

static string BuildVarianceFormula(const string &sum_expr, const string &sum_sq_expr, const string &count_expr,
                                   bool is_population, bool needs_sqrt, bool clamp_variance) {
	string denom = is_population ? "NULLIF(" + count_expr + ", 0)" : "NULLIF(" + count_expr + " - 1, 0)";
	string var_raw =
	    "((" + sum_sq_expr + ") - (" + sum_expr + ") * (" + sum_expr + ") / (" + count_expr + ")) / " + denom;
	string var_safe = clamp_variance ? "GREATEST(" + var_raw + ", 0::DOUBLE)" : var_raw;
	return needs_sqrt ? "sqrt(" + var_safe + ")" : var_safe;
}

static string BuildGuardedVarianceFormula(const string &sum_expr, const string &sum_sq_expr, const string &count_expr,
                                          bool is_population, bool needs_sqrt) {
	int threshold = is_population ? 0 : 1;
	return "CASE WHEN " + count_expr + " > " + std::to_string(threshold) + " THEN " +
	       BuildVarianceFormula(sum_expr, sum_sq_expr, count_expr, is_population, needs_sqrt, true) + " ELSE NULL END";
}

/// Detect AVG and STDDEV/VARIANCE decomposition columns from the column list.
/// AVG(x) is stored as openivm_sum_<alias>, openivm_count_<alias>, and <alias>.
/// STDDEV/VARIANCE(x) adds a sum-of-squares column with a prefix encoding the
/// function type:
///   openivm_sum_sq_  = stddev_samp (apply sqrt, denominator N-1)
///   openivm_var_sq_  = var_samp    (no sqrt, denominator N-1)
///   openivm_sum_sqp_ = stddev_pop  (apply sqrt, denominator N)
///   openivm_var_sqp_ = var_pop     (no sqrt, denominator N)
struct DerivedAggDecomposition {
	unordered_set<string> derived_cols;        // aliases to skip in MERGE
	unordered_map<string, string> sum_cols;    // alias → openivm_sum_<alias>
	unordered_map<string, string> sum_sq_cols; // alias → openivm_*_sq*_<alias>
	unordered_map<string, string> count_cols;  // alias → openivm_count_<alias>
	// Per-alias flags decoded from the sum_sq prefix
	unordered_map<string, bool> needs_sqrt;    // alias → true if stddev (not variance)
	unordered_map<string, bool> is_population; // alias → true if population variant
};

/// Try to match a column against a prefix. Returns the alias suffix if matched,
/// empty string otherwise.
static string MatchPrefix(const string &col, const string &prefix) {
	if (col.size() > prefix.size() && col.substr(0, prefix.size()) == prefix) {
		return col.substr(prefix.size());
	}
	return "";
}

static bool IsDecomposedAggregateType(const string &aggregate_type) {
	static const unordered_set<string> DECOMPOSED_TYPES = {"avg",      "stddev",   "stddev_samp", "stddev_pop",
	                                                       "variance", "var_samp", "var_pop"};
	return DECOMPOSED_TYPES.count(aggregate_type);
}

static DerivedAggDecomposition DetectDerivedAggColumns(const vector<string> &columns) {
	DerivedAggDecomposition result;
	for (auto &col : columns) {
		// Check sum-of-squares prefixes BEFORE sum (they start with openivm_sum_ or openivm_var_)
		// Assignments moved out of if-conditions to satisfy bugprone-assignment-in-if-condition.
		string alias;
		alias = MatchPrefix(col, openivm::SUM_SQP_COL_PREFIX);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = true;
			result.is_population[alias] = true;
			continue;
		}
		alias = MatchPrefix(col, openivm::VAR_SQP_COL_PREFIX);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = false;
			result.is_population[alias] = true;
			continue;
		}
		alias = MatchPrefix(col, openivm::SUM_SQ_COL_PREFIX);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = true;
			result.is_population[alias] = false;
			continue;
		}
		alias = MatchPrefix(col, openivm::VAR_SQ_COL_PREFIX);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = false;
			result.is_population[alias] = false;
			continue;
		}
		alias = MatchPrefix(col, openivm::SUM_COL_PREFIX);
		if (!alias.empty()) {
			result.sum_cols[alias] = col;
			continue;
		}
		alias = MatchPrefix(col, openivm::COUNT_COL_PREFIX);
		if (!alias.empty()) {
			result.count_cols[alias] = col;
		}
	}
	// A column is derived if it has at least SUM + COUNT
	for (auto &entry : result.sum_cols) {
		if (result.count_cols.count(entry.first)) {
			result.derived_cols.insert(entry.first);
		}
	}
	return result;
}

static unordered_map<string, string> DetectSumNullCountColumns(const vector<string> &columns) {
	unordered_map<string, string> result;
	const string prefix = openivm::SUM_COUNT_COL_PREFIX;
	for (auto &column : columns) {
		if (column.size() <= prefix.size() || column.compare(0, prefix.size(), prefix) != 0) {
			continue;
		}
		idx_t projection_index = 0;
		for (idx_t i = prefix.size(); i < column.size(); i++) {
			if (!std::isdigit(static_cast<unsigned char>(column[i]))) {
				throw InternalException("Invalid SUM count state column '%s'", column);
			}
			idx_t digit = column[i] - '0';
			if (projection_index > (std::numeric_limits<idx_t>::max() - digit) / 10) {
				throw InternalException("Invalid SUM count state column '%s'", column);
			}
			projection_index = projection_index * 10 + digit;
		}
		if (projection_index >= columns.size()) {
			throw InternalException("Invalid SUM count state column '%s'", column);
		}
		result[SqlUtils::QuoteIdentifier(columns[projection_index])] = SqlUtils::QuoteIdentifier(column);
	}
	return result;
}

string CompileAggregateGroups(const string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql, bool has_minmax,
                              bool list_mode, const string &delta_ts_filter, const vector<string> &group_column_names,
                              const string &catalog_prefix, bool insert_only, const vector<string> &aggregate_types,
                              const vector<LogicalType> &column_types, bool use_current_diff_affected_keys,
                              const vector<GroupRecomputeDeltaSpec> *cascade_delta_specs,
                              const string &cascade_lpts_table_prefix, bool emit_cascade_delta,
                              bool inline_cascade_delta, bool *out_handled_cascade_delta,
                              const unordered_map<string, string> &derived_output_expressions,
                              bool derived_output_expressions_complete, const vector<string> &preserved_side_cols,
                              bool *out_used_group_recompute, bool force_group_recompute) {
	if (out_used_group_recompute) {
		*out_used_group_recompute = false;
	}
	auto is_preserved_side = [&](const string &c) {
		return std::find(preserved_side_cols.begin(), preserved_side_cols.end(), c) != preserved_side_cols.end();
	};
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string delta_view = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));

	// Extract GROUP BY keys: from index (standard path) or from metadata (DuckLake fallback)
	vector<string> keys;
	if (index_delta_view_catalog_entry) {
		auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
		auto key_ids = index_catalog_entry->column_ids;
		for (size_t i = 0; i < key_ids.size(); i++) {
			keys.emplace_back(column_names[key_ids[i]]);
		}
	} else {
		// No index (e.g. DuckLake) — use group column names from metadata
		for (auto &col : group_column_names) {
			keys.emplace_back(col);
		}
	}

	unordered_set<std::string> keys_set(keys.begin(), keys.end());
	vector<string> aggregates;
	unordered_map<string, string> aggregate_raw_names;
	unordered_map<string, string> quoted_derived_output_expressions;
	const bool use_derived_output_merge =
	    insert_only && has_minmax && derived_output_expressions_complete && !derived_output_expressions.empty();
	if (use_derived_output_merge) {
		for (auto &entry : derived_output_expressions) {
			quoted_derived_output_expressions[SqlUtils::QuoteIdentifier(entry.first)] = entry.second;
		}
		OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] insert-only derived-output "
		                    "MERGE for '%s' (%zu outputs)\n",
		                    view_name.c_str(), quoted_derived_output_expressions.size());
	}
	// Parallel: true if the matching aggregate column has a non-summable delta type
	// (VARCHAR CASE output, 'GC' literal, UPPER(group_col), LIST(col), etc.).
	unordered_set<string> non_summable_columns;
	for (size_t i = 0; i < column_names.size(); i++) {
		const auto &column = column_names[i];
		if (keys_set.find(column) == keys_set.end() && column != string(openivm::MULTIPLICITY_COL)) {
			string quoted_column = SqlUtils::QuoteIdentifier(column);
			aggregates.push_back(quoted_column);
			aggregate_raw_names[quoted_column] = column;
			if (i < column_types.size() && !IsSummableLogicalType(column_types[i]) &&
			    !quoted_derived_output_expressions.count(quoted_column)) {
				non_summable_columns.insert(quoted_column);
			}
		}
	}
	// A non-summable non-key column can't be maintained by the delta-sum MERGE path.
	// Fall back to group-recompute: delete affected groups, re-insert from view query.
	// This is correct for VARCHAR literals, string functions of group keys, LIST
	// aggregates, and CASE over aggregates alike. Slower than MERGE, faster than
	// full recompute (only affected groups are re-evaluated).
	bool needs_group_recompute = force_group_recompute;
	if (insert_only && has_minmax && !derived_output_expressions_complete) {
		needs_group_recompute = true;
		OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] incomplete derived-output metadata for '%s' "
		                    "→ group-recompute\n",
		                    view_name.c_str());
	}
	if (force_group_recompute) {
		OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] caller requires affected-group recompute\n");
	}

	auto decomp = DetectDerivedAggColumns(aggregates);
	auto sum_null_count_cols = DetectSumNullCountColumns(column_names);

	// Detect AVG/STDDEV-derived columns that can't be maintained incrementally.
	//
	// Case 1 — "orphan derived": hidden openivm_sum_<alias> + openivm_count_<alias> exist
	// but no user-visible column has that alias. Happens when the user wraps
	// AVG/STDDEV in a non-pass-through expression at the top SELECT so the
	// top-level alias diverges from the CTE's alias (query_0540 shape:
	// CTE `avg_bal` → top `ROUND(avg_bal, 2) AS avg`). The MERGE formula
	// `avg = openivm_sum_avg / openivm_count_avg` can't reconstruct ROUND without
	// re-deriving.
	//
	// Case 2 — "computed over derived": the view has derived aggregates AND a
	// non-derived, non-hidden, non-key, non-multiplicity column with no matching
	// aggregate_types entry. That column is a pure SELECT-list expression over
	// the aggregates (query_1307 shape: `ROUND(avg_b / NULLIF(std_b, 0), 4) AS cv`).
	// There is no aggregate function producing it, so summing deltas is wrong.
	//
	// Group-recompute (delete affected groups, re-insert via the full view query)
	// is correct in both cases because it preserves the expression semantics.
	unordered_set<string> aggregate_set(aggregates.begin(), aggregates.end());
	bool has_orphan_derived = false;
	for (auto &alias : decomp.derived_cols) {
		if (!aggregate_set.count(SqlUtils::QuoteIdentifier(alias)) && !aggregate_set.count(alias)) {
			has_orphan_derived = true;
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] Orphan derived alias '%s' "
			                    "→ group-recompute\n",
			                    alias.c_str());
			break;
		}
	}
	// Case 2 — "computed over aggregates": the plan has MORE aggregate expressions
	// than the view has user-visible aggregate columns (excluding keys, hidden,
	// derived, multiplicity). Happens when SELECT-list expressions *consume*
	// aggregates without exposing them directly — e.g. query_1307
	// (`ROUND(avg_b / NULLIF(std_b, 0), 4) AS cv` alongside avg_b and std_b) or
	// query_1987 (`ROUND(SUM(C_BALANCE) / NULLIF(COUNT(C_ID), 0), 2) AS avg_bal`,
	// one output column but two underlying aggregates).
	//
	// Detection: count non-key, non-hidden, non-derived columns vs. the number of
	// aggregate_types entries that are NOT decomposed (AVG/STDDEV become hidden
	// columns, so those entries do not correspond to user columns). If the type
	// count exceeds the column count, at least one aggregate is consumed inside a
	// computed expression — sum-of-deltas is wrong for that expression.
	bool has_computed_over_derived = false;
	if (!aggregate_types.empty()) {
		// Pass 1 — "type excess": more non-decomposed aggregate types than
		// aggregate columns (INCLUDING hidden helpers like openivm_match_count and
		// openivm_*) means at least one aggregate is consumed inside a computed
		// expression (query_1987 `ROUND(SUM(x) / COUNT(y), 2) AS avg_bal`, two
		// underlying aggregates but one output column). Hidden helper columns get
		// counted because they DO correspond to aggregate_types entries (e.g.
		// RewriteLeftJoinMatchCount adds a real COUNT to the plan).
		idx_t non_decomposed_type_count = 0;
		for (auto &t : aggregate_types) {
			if (!IsDecomposedAggregateType(t)) {
				non_decomposed_type_count++;
			}
		}
		idx_t total_agg_col_count = 0;
		for (auto &column : aggregates) {
			if (decomp.derived_cols.count(column)) {
				// Derived user columns (avg_b, std_b) don't consume aggregate_types slots —
				// their hidden openivm_sum_* / openivm_count_* companions do.
				continue;
			}
			if (column.find(string(openivm::SUM_COL_PREFIX)) == 0 ||
			    column.find(string(openivm::SUM_SQ_COL_PREFIX)) == 0 ||
			    column.find(string(openivm::SUM_SQP_COL_PREFIX)) == 0) {
				// Decomposed aggregate hidden columns are counted against the decomposed
				// aggregate_types entries (avg, stddev) which we *did not* count above.
				continue;
			}
			if (column.find(string(openivm::COUNT_COL_PREFIX)) == 0) {
				// Same — the COUNT half of an AVG/STDDEV decomposition.
				continue;
			}
			if (column.find(string(openivm::SUM_COUNT_COL_PREFIX)) == 0) {
				// SUM NULL-state counts are injected after AnalyzePlan captures
				// aggregate_types, so they do not consume a metadata slot.
				continue;
			}
			// openivm_match_count / openivm_right_match_count are added by
			// RewriteLeftJoinMatchCount *after* AnalyzePlan captures aggregate_types,
			// so aggregate_types does NOT include them. Exclude them here for a
			// like-with-like comparison.
			if (column == SqlUtils::QuoteIdentifier(string(openivm::MATCH_COUNT_COL)) ||
			    column == string(openivm::MATCH_COUNT_COL) ||
			    column == SqlUtils::QuoteIdentifier(string(openivm::RIGHT_MATCH_COUNT_COL)) ||
			    column == string(openivm::RIGHT_MATCH_COUNT_COL)) {
				continue;
			}
			total_agg_col_count++;
		}
		if (non_decomposed_type_count > total_agg_col_count) {
			has_computed_over_derived = true;
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] %llu non-decomposed "
			                    "aggregate types > %llu agg columns "
			                    "→ group-recompute\n",
			                    (unsigned long long)non_decomposed_type_count, (unsigned long long)total_agg_col_count);
		}
		// Pass 2 — "column orphan": when derived aggregates exist, any non-derived,
		// non-hidden column that has no aggregate_types entry to consume (after
		// skipping decomposed ones) is a computed expression over the derived
		// aggregates (query_1307 `ROUND(avg_b / NULLIF(std_b, 0), 4) AS cv`).
		if (!has_computed_over_derived && !decomp.derived_cols.empty()) {
			idx_t probe_idx = 0;
			for (auto &column : aggregates) {
				if (decomp.derived_cols.count(column) || column.find("openivm_") != string::npos) {
					continue;
				}
				while (probe_idx < aggregate_types.size() && IsDecomposedAggregateType(aggregate_types[probe_idx])) {
					probe_idx++;
				}
				if (probe_idx >= aggregate_types.size()) {
					has_computed_over_derived = true;
					OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] Column orphan '%s' "
					                    "over derived aggregates → group-recompute\n",
					                    column.c_str());
					break;
				}
				probe_idx++;
			}
		}
	}

	// The Larson & Zhou LEFT JOIN template below has its own column formulas.
	// Until it reconstructs projected expressions and insert-only extrema from
	// the same expression map, keep these shapes on the exact affected-group
	// recompute path.
	string match_count_col;
	bool has_match_count = false;
	for (auto &column : aggregates) {
		if (column == SqlUtils::QuoteIdentifier(string(openivm::MATCH_COUNT_COL))) {
			has_match_count = true;
			match_count_col = column;
			break;
		}
	}
	if (has_match_count && (use_derived_output_merge || (insert_only && has_minmax))) {
		needs_group_recompute = true;
		OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] LEFT JOIN fast-path formula is not supported for '%s' "
		                    "→ group-recompute\n",
		                    view_name.c_str());
	}
	if (has_orphan_derived || has_computed_over_derived) {
		needs_group_recompute = true;
	}

	// has_minmax=true is set by TWO unrelated conditions in the classifier:
	//   (a) the view has an actual MIN/MAX/ARG_MIN/ARG_MAX aggregate — insert-only can use
	//       GREATEST/LEAST in MERGE (fast path for MIN/MAX), mixed needs group-recompute.
	//       For ARG_MIN/ARG_MAX the caller always passes insert_only=false via has_argminmax,
	//       so (has_minmax && !insert_only) below handles group-recompute — this block
	//       correctly stays quiet (has_real_minmax=true) to avoid double-triggering.
	//   (b) the view has a LEFT/RIGHT/OUTER JOIN + a "computed" aggregate
	//       argument (COALESCE/CASE/constant/non-BCR), which breaks the
	//       Larson & Zhou MERGE template — group-recompute is REQUIRED for
	//       every delta shape, including insert-only. See the classifier
	//       comment in parser.cpp (`query_1502/1696/1746/1749`).
	// Distinguish: case (a) has "min"/"max"/"arg_min"/"arg_max" in aggregate_types;
	// case (b) does not. Force group-recompute for (b) since the MERGE path produces
	// incorrect MV state when a new right-side row converts an existing
	// NULL-padded LEFT JOIN row into a match.
	{
		bool has_real_minmax = false;
		for (auto &t : aggregate_types) {
			if (t == "min" || t == "max" || t == "arg_min" || t == "arg_max") {
				has_real_minmax = true;
				break;
			}
		}
		if (has_minmax && !has_real_minmax) {
			needs_group_recompute = true;
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] has_minmax=true with no "
			                    "MIN/MAX in agg_types — LEFT JOIN + computed "
			                    "aggregate case → group-recompute\n");
		}
	}

	// Build per-column aggregate type map from metadata (for insert-only MIN/MAX).
	// aggregate_types aligns with aggregate expressions in the rewritten plan:
	// one entry per BoundAggregateExpression (excludes AVG/STDDEV-derived cols which are projections).
	// Build per-column aggregate type map from aggregate_types metadata.
	// aggregate_types has one entry per ORIGINAL aggregate expression (before plan rewrites).
	// Decomposed aggregates (avg, stddev, variance) are replaced by hidden columns in the
	// plan rewrite, so we skip their entries to keep the mapping aligned with user-visible columns.
	unordered_map<string, string> col_agg_type;
	if (!aggregate_types.empty()) {
		idx_t type_idx = 0;
		for (auto &column : aggregates) {
			if (quoted_derived_output_expressions.count(column) || decomp.derived_cols.count(column) ||
			    column.find("openivm_") != string::npos) {
				continue;
			}
			// Skip decomposed aggregate_types entries (avg → SUM+COUNT hidden cols)
			while (type_idx < aggregate_types.size() && IsDecomposedAggregateType(aggregate_types[type_idx])) {
				type_idx++;
			}
			if (type_idx < aggregate_types.size()) {
				col_agg_type[column] = aggregate_types[type_idx++];
			}
		}
		OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] agg_type map: %zu entries "
		                    "from %zu types, %zu aggregates\n",
		                    col_agg_type.size(), aggregate_types.size(), aggregates.size());
	}

	// Non-numeric aggregates normally cannot use the delta-sum MERGE. A
	// caller-proven insert-only MIN/MAX is different: its delta is consolidated
	// with MIN/MAX and merged with LEAST/GREATEST, which is valid for ordered
	// types such as TIMESTAMP as well as numeric types.
	for (auto &column : non_summable_columns) {
		string agg_type = col_agg_type.count(column) ? col_agg_type[column] : "";
		if (!(insert_only && (agg_type == "min" || agg_type == "max"))) {
			needs_group_recompute = true;
			break;
		}
	}

	if (needs_group_recompute || (has_minmax && !insert_only)) {
		// Tell the caller this call recomputes affected groups from the base tables rather than
		// applying delta arithmetic. Callers that inject extra delta-table rows purely to correct the
		// MERGE arithmetic (e.g. the LEFT JOIN pipeline secondary delta) should skip that injection
		// here: group-recompute rebuilds each affected group from the base tables, so the correction
		// is recomputed anyway and the extra rows only widen the affected-group set.
		if (out_used_group_recompute) {
			*out_used_group_recompute = true;
		}
		// Cascade-delta dispatch: when the caller (refresh_sql.cpp) requested cascade-delta
		// emission AND we have the inputs CompileGroupRecompute needs (non-empty group_columns,
		// non-empty delta_specs), route through the snapshot+signed-multiset path so the
		// downstream cascade-delta is built from real old/new view-query rows instead of the
		// LPTS sum-delta + NULL companion pair (which is wrong for MIN/MAX and breaks
		// SIMPLE_PROJECTION-with-JOIN downstreams).
		if (emit_cascade_delta && cascade_delta_specs && !cascade_delta_specs->empty() && !group_column_names.empty()) {
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] cascade-delta requested + recompute branch "
			                    "→ dispatching "
			                    "to CompileGroupRecompute(emit_cascade_delta=true)\n");
			if (out_handled_cascade_delta) {
				*out_handled_cascade_delta = true;
			}
			return CompileGroupRecompute(view_name, view_query_sql, group_column_names, *cascade_delta_specs,
			                             catalog_prefix, cascade_lpts_table_prefix,
			                             /*emit_cascade_delta=*/true, GroupRecomputeAffectedMode::SOURCE_DELTA);
		}
		// Group-recompute strategy: delete affected groups, re-insert from original query.
		// Always triggered by non-summable columns (LIST aggregates, VARCHAR literals,
		// CASE results, etc.) — even in insert-only mode, the MERGE path emits
		// `sum(case mult=false then -<col> else <col> end)` which fails type-checking
		// for VARCHAR etc. regardless of whether the negative branch is reached.
		// For MIN/MAX without non-summable cols, insert_only uses GREATEST/LEAST in
		// the MERGE path below.
		if (use_current_diff_affected_keys && !view_query_sql.empty()) {
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] using current-diff "
			                    "affected keys for '%s'\n",
			                    view_name.c_str());
			return BuildCurrentDiffGroupRecomputeSQL(view_name, data_table, view_query_sql, keys);
		}
		string keys_tuple = SqlUtils::JoinQuotedColumns(keys);
		string match_delete = SqlUtils::BuildNullSafeKeyPredicate(keys, "openivm_aff.", "openivm_tgt.");
		string match_insert = SqlUtils::BuildNullSafeKeyPredicate(keys, "openivm_aff.", "openivm_recompute.");
		string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
		string affected = "select distinct " + keys_tuple + " from " + delta_view + delta_where;
		// index_delta_view_catalog_entry non-null means the DATA table carries the UNIQUE index that
		// parser.cpp creates for AGGREGATE_GROUP / AGGREGATE_HAVING views. DuckDB's on-disk unique
		// index keeps deleted keys for constraint checking within a transaction, so the plain
		// DELETE-then-INSERT form raises a spurious duplicate-key error for every group that survives
		// the recompute. Use the upsert form there. Without the index (DuckLake, group_column_names
		// fallback) INSERT OR REPLACE is not usable, and plain DELETE+INSERT is already safe.
		vector<string> upsert_keys;
		string recompute_temp;
		if (index_delta_view_catalog_entry) {
			upsert_keys = keys;
			recompute_temp = SqlUtils::QuoteIdentifier("openivm_recompute_" + view_name);
		}
		return BuildAffectedKeyRefreshSQL(data_table, view_query_sql, "  " + affected, "openivm_tgt",
		                                  "openivm_recompute", "openivm_aff", match_delete, match_insert,
		                                  /*affected_temp_table=*/"", upsert_keys, recompute_temp);
	}

	// CTE: consolidate deltas per group
	string cte_select_string = "select ";
	cte_select_string += SqlUtils::JoinQuotedColumns(keys) + ", ";
	for (auto &column : aggregates) {
		if (quoted_derived_output_expressions.count(column)) {
			continue;
		}
		string agg_type = col_agg_type.count(column) ? col_agg_type[column] : "";
		if (insert_only && agg_type == "min") {
			// Insert-only MIN: consolidate with MIN (new min can only be <= current)
			cte_select_string += "\n\tmin(" + column + ") as " + column + ", ";
		} else if (insert_only && agg_type == "max") {
			// Insert-only MAX: consolidate with MAX (new max can only be >= current)
			cte_select_string += "\n\tmax(" + column + ") as " + column + ", ";
		} else if (list_mode) {
			// Z-set bag-aware list sum: per-row scale every element by the row's weight,
			// then list_reduce-add across the group.
			cte_select_string += "\n\tlist_reduce(list(list_transform(" + column +
			                     ", lambda x: " + string(openivm::MULTIPLICITY_COL) +
			                     " * x)), lambda a, b: list_transform(list_zip(a, "
			                     "b), lambda x: x[1] + x[2])) AS " +
			                     column + ", ";
		} else {
			// Z-set bag-aware sum: weight w∈ℤ scales the column value before SUM.
			cte_select_string = cte_select_string + "\n\tsum(" + string(openivm::MULTIPLICITY_COL) + " * " + column +
			                    ") as " + column + ", ";
		}
	}
	cte_select_string.erase(cte_select_string.size() - 2, 2);
	cte_select_string += "\n";
	string cte_from_string = "from " + delta_view;
	if (!delta_ts_filter.empty()) {
		cte_from_string += " WHERE " + delta_ts_filter;
	}
	cte_from_string += "\n";
	string cte_group_by_string = "group by " + SqlUtils::JoinQuotedColumns(keys);

	string cte_body = cte_select_string + cte_from_string + cte_group_by_string;

	// MERGE: single-pass upsert — UPDATE existing groups, INSERT new groups.
	// Uses IS NOT DISTINCT FROM for NULL-safe key matching.
	string on_clause = SqlUtils::BuildNullSafeKeyPredicate(keys, "v.", "d.");

	auto &derived_cols = decomp.derived_cols;
	auto &d_sum_cols = decomp.sum_cols;
	auto &d_sum_sq_cols = decomp.sum_sq_cols;
	auto &d_count_cols = decomp.count_cols;

	unordered_map<string, string> updated_column_expressions;
	unordered_map<string, string> inserted_column_expressions;
	for (auto &key : keys) {
		updated_column_expressions[key] = "v." + SqlUtils::QuoteIdentifier(key);
		inserted_column_expressions[key] = "d." + SqlUtils::QuoteIdentifier(key);
	}
	for (auto &column : aggregates) {
		if (quoted_derived_output_expressions.count(column)) {
			continue;
		}
		string raw_column = aggregate_raw_names.at(column);
		if (derived_cols.count(column)) {
			string sum_col = d_sum_cols.count(column) ? d_sum_cols.at(column) : "";
			string count_col = d_count_cols.count(column) ? d_count_cols.at(column) : "";
			if (sum_col.empty() || count_col.empty()) {
				updated_column_expressions[raw_column] = BuildUpdatedAggregateColumn(column);
				inserted_column_expressions[raw_column] = "d." + column;
				continue;
			}
			if (d_sum_sq_cols.count(column)) {
				string sum_sq_col = d_sum_sq_cols.at(column);
				bool is_pop = decomp.is_population.count(column) && decomp.is_population.at(column);
				bool do_sqrt = decomp.needs_sqrt.count(column) && decomp.needs_sqrt.at(column);
				updated_column_expressions[raw_column] = BuildGuardedVarianceFormula(
				    BuildUpdatedAggregateColumn(sum_col), BuildUpdatedAggregateColumn(sum_sq_col),
				    BuildUpdatedAggregateColumn(count_col), is_pop, do_sqrt);
				inserted_column_expressions[raw_column] =
				    BuildGuardedVarianceFormula("d." + sum_col, "d." + sum_sq_col, "d." + count_col, is_pop, do_sqrt);
			} else {
				updated_column_expressions[raw_column] =
				    BuildAvgFormula(BuildUpdatedAggregateColumn(sum_col), BuildUpdatedAggregateColumn(count_col));
				inserted_column_expressions[raw_column] = BuildAvgFormula("d." + sum_col, "d." + count_col);
			}
		} else {
			string agg_type = col_agg_type.count(column) ? col_agg_type[column] : "";
			auto sum_count = sum_null_count_cols.find(column);
			if (sum_count != sum_null_count_cols.end()) {
				updated_column_expressions[raw_column] = BuildNullableSum(
				    BuildUpdatedAggregateColumn(column), BuildUpdatedAggregateColumn(sum_count->second));
				inserted_column_expressions[raw_column] = BuildNullableSum("d." + column, "d." + sum_count->second);
			} else if (insert_only && agg_type == "min") {
				updated_column_expressions[raw_column] = BuildNullSafeExtremumUpdate(column, "LEAST");
			} else if (insert_only && agg_type == "max") {
				updated_column_expressions[raw_column] = BuildNullSafeExtremumUpdate(column, "GREATEST");
			} else if (list_mode) {
				updated_column_expressions[raw_column] =
				    "list_transform(list_zip(v." + column + ", d." + column + "), lambda x: x[1] + x[2])";
			} else {
				updated_column_expressions[raw_column] = BuildUpdatedAggregateColumn(column);
			}
			inserted_column_expressions[raw_column] = "d." + column;
		}
	}

	string update_set;
	string insert_cols, insert_vals;
	{
		bool first_agg = true;

		// Build update_set and insert_vals in the SAME column order as aggregates.
		// Derived columns (AVG, STDDEV, VARIANCE) get their formula inline rather
		// than being appended at the end — this ensures INSERT column/value alignment.
		for (auto &column : aggregates) {
			if (!first_agg) {
				update_set += ", ";
				insert_vals += ", ";
			}
			first_agg = false;

			if (quoted_derived_output_expressions.count(column)) {
				auto &expression_sql = quoted_derived_output_expressions.at(column);
				update_set +=
				    column + " = " + SubstituteDerivedOutputColumns(expression_sql, updated_column_expressions);
				insert_vals += SubstituteDerivedOutputColumns(expression_sql, inserted_column_expressions);
			} else {
				string raw_column = aggregate_raw_names.at(column);
				update_set += column + " = " + updated_column_expressions.at(raw_column);
				insert_vals += inserted_column_expressions.at(raw_column);
			}
		}
		insert_cols = SqlUtils::JoinQuotedColumns(keys) + ", " + StringUtil::Join(aggregates, ", ");
	}

	string merge_query;
	if (has_match_count) {
		// Larson & Zhou LEFT JOIN MERGE: single WHEN MATCHED with CASE WHEN expressions.
		//
		// Column classification under LEFT JOIN:
		//   (a) count_star — COUNT(*) counts every LEFT JOIN output row, including the
		//       NULL-padded row emitted when a left-side row has no match. It is
		//       left-side-driven: a matched→unmatched transition *does not zero* it;
		//       instead, count_star = #left_rows_in_group (= #NULL-padded rows).
		//       Always apply the normal aggregate update — no CASE gating.
		//   (b) right-side aggregates — COUNT(right_col), SUM(right_col), AVG(right_col),
		//       etc. When mc_new = 0, these must reset to 0/NULL because the NULL-padded
		//       row contributes NULL for right_col. Use the CASE gating.
		//   (c) left-side non-count aggregates (SUM(left_col), AVG(left_col)) — not
		//       currently distinguishable here; fall through to the CASE gating, which
		//       is incorrect for those but rare in practice. (Left-side aggregates
		//       don't go to NULL when mc_new transitions, so this remains a known
		//       limitation for uncommon queries; documented in limitations.md.)
		string mc_new = "(COALESCE(v." + match_count_col + ", 0) + d." + match_count_col + ")";
		string lj_update_set;
		bool first_lj = true;
		for (auto &col : aggregates) {
			if (!first_lj) {
				lj_update_set += ", ";
			}
			first_lj = false;
			if (col == match_count_col) {
				lj_update_set += col + " = " + mc_new;
				continue;
			}
			string agg_type = col_agg_type.count(col) ? col_agg_type.at(col) : "";
			// Match the hidden openivm_count_star column by NAME as well: it has no col_agg_type entry,
			// so the agg_type test alone never caught it and it fell through to the gated branch, which
			// zeroes it on a >0 -> 0 match transition. It counts LEFT JOIN OUTPUT rows, so the
			// NULL-padded row keeps it at 1 and it must follow the left side.
			if (agg_type == "count_star" || col == string(openivm::COUNT_STAR_COL) || is_preserved_side(col)) {
				// count_star and preserved/left-side COUNT columns are left-side-driven: a right-side
				// match_count transition must NOT zero them. Always apply the normal aggregate update.
				lj_update_set += col + " = " + BuildUpdatedAggregateColumn(col);
				continue;
			}
			// Determine if this column should be 0 (count-like) or NULL (sum-like) when unmatched.
			// Check both the aggregate_types metadata AND the hidden column prefix
			// (hidden openivm_count_* columns from AVG/STDDEV decomposition don't have agg_type entries).
			bool is_count = (agg_type == "count" || col.find(string(openivm::COUNT_COL_PREFIX)) == 0 ||
			                 col.find(string(openivm::SUM_COUNT_COL_PREFIX)) == 0);
			string null_val = is_count ? "0" : "NULL";
			// Three-way CASE:
			//   mc_new > 0: group has matches (now or after delta). Normal aggregate update.
			//   v.match_count = 0: group was unmatched and stays unmatched (mc_new implied 0).
			//     Preserve v.col — the CREATE-time value already reflects the NULL-padded row
			//     semantics for this column, including any projection-folded constants like
			//     SUM(COALESCE(x, 0)) = 0 or COUNT(right_col) = 0. Resetting to null_val would
			//     wipe legitimate folded values (q1686 SUM(COALESCE(o.x,0)) stored 0 at CREATE
			//     but would become NULL after any zero-net delta pass).
			//   else: transition from matched to unmatched (v.mc > 0, d.mc = -v.mc). Right-side
			//     data is gone; reset to null_val. This remains imperfect for folded projections
			//     (a transitioning group whose stored COALESCE'd column was 0 resets to NULL)
			//     — a known limitation.
			string matched_update = BuildUpdatedAggregateColumn(col);
			auto sum_count = sum_null_count_cols.find(col);
			if (sum_count != sum_null_count_cols.end()) {
				matched_update = BuildNullableSum(matched_update, BuildUpdatedAggregateColumn(sum_count->second));
			}
			lj_update_set += col + " = CASE WHEN " + mc_new + " > 0 THEN " + matched_update + " WHEN COALESCE(v." +
			                 match_count_col + ", 0) = 0 THEN v." + col + " ELSE " + null_val + " END";
		}
		// INSERT values: mirror the UPDATE classification above.
		//   count_star → d.count_star (always, no CASE)
		//   right-side → CASE d.match_count > 0 THEN d.col ELSE null_val
		string cond_insert_vals;
		bool first_ins = true;
		for (size_t i = 0; i < aggregates.size(); i++) {
			if (!first_ins) {
				cond_insert_vals += ", ";
			}
			first_ins = false;
			if (aggregates[i] == match_count_col) {
				cond_insert_vals += "d." + match_count_col;
				continue;
			}
			string agg_type = col_agg_type.count(aggregates[i]) ? col_agg_type.at(aggregates[i]) : "";
			if (agg_type == "count_star" || aggregates[i] == string(openivm::COUNT_STAR_COL) ||
			    is_preserved_side(aggregates[i])) {
				cond_insert_vals += "d." + aggregates[i];
				continue;
			}
			bool is_count = (agg_type == "count" || aggregates[i].find(string(openivm::COUNT_COL_PREFIX)) == 0 ||
			                 aggregates[i].find(string(openivm::SUM_COUNT_COL_PREFIX)) == 0);
			string null_val = is_count ? "0" : "NULL";
			string matched_insert = "d." + aggregates[i];
			auto sum_count = sum_null_count_cols.find(aggregates[i]);
			if (sum_count != sum_null_count_cols.end()) {
				matched_insert = BuildNullableSum(matched_insert, "d." + sum_count->second);
			}
			cond_insert_vals +=
			    "CASE WHEN d." + match_count_col + " > 0 THEN " + matched_insert + " ELSE " + null_val + " END";
		}
		merge_query = "WITH refresh_cte AS (\n" + cte_body + ")\n" + "MERGE INTO " + data_table +
		              " v USING refresh_cte d\n" + "ON " + on_clause + "\n" + "WHEN MATCHED THEN UPDATE SET " +
		              lj_update_set + "\n" + "WHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (";
		merge_query += SqlUtils::JoinQualifiedQuotedColumns(keys, "d") + ", " + cond_insert_vals + ");\n";
	} else {
		merge_query = "WITH refresh_cte AS (\n" + cte_body + ")\n" + "MERGE INTO " + data_table +
		              " v USING refresh_cte d\n" + "ON " + on_clause + "\n" + "WHEN MATCHED THEN UPDATE SET " +
		              update_set + "\n" + "WHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (";
		merge_query += SqlUtils::JoinQualifiedQuotedColumns(keys, "d") + ", " + insert_vals + ");\n";
	}

	string upsert_query = merge_query + "\n";
	if (insert_only && emit_cascade_delta && inline_cascade_delta) {
		// The inline companion emitted by refresh_sql.cpp is a compact aggregate-delta
		// encoding (-1 NULL marker + positive delta). That is enough for downstream SUM/COUNT,
		// but not for arbitrary downstream consumers and MIN/MAX. Keep the fast MERGE for this
		// MV, then replace the delta view with a true -old/+new affected-group multiset.
		string affected_temp =
		    SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + "affected_" + view_name + "_merge");
		string old_temp =
		    SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + "snapshot_" + view_name + "_merge");
		string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
		string marker_predicate = string(openivm::MULTIPLICITY_COL) + " < 0";
		for (auto &column : aggregates) {
			marker_predicate += " AND " + column + " IS NULL";
		}
		string affected_match = SqlUtils::BuildNullSafeMatch(keys, "openivm_aff", "openivm_data");
		string new_match = SqlUtils::BuildNullSafeMatch(keys, "openivm_aff", "openivm_new");
		string col_list = SqlUtils::JoinQuotedColumns(column_names);
		string select_old, select_new;
		bool first_col = true;
		for (auto &column : column_names) {
			if (!first_col) {
				select_old += ", ";
				select_new += ", ";
			}
			first_col = false;
			if (column == string(openivm::MULTIPLICITY_COL)) {
				select_old += "-1";
				select_new += "1";
			} else {
				select_old += "openivm_old." + SqlUtils::QuoteIdentifier(column);
				select_new += "openivm_new." + SqlUtils::QuoteIdentifier(column);
			}
		}

		string cascade_sql;
		cascade_sql += "DELETE FROM " + delta_view + " WHERE " + marker_predicate;
		if (!delta_ts_filter.empty()) {
			cascade_sql += " AND " + delta_ts_filter;
		}
		cascade_sql += ";\n";
		cascade_sql += "DROP TABLE IF EXISTS " + affected_temp + ";\n";
		cascade_sql += "CREATE TABLE " + affected_temp + " AS SELECT DISTINCT " + SqlUtils::JoinQuotedColumns(keys) +
		               " FROM " + delta_view + delta_where + ";\n";
		cascade_sql += "DROP TABLE IF EXISTS " + old_temp + ";\n";
		cascade_sql += "CREATE TABLE " + old_temp + " AS SELECT openivm_data.* FROM " + data_table +
		               " openivm_data WHERE EXISTS (SELECT 1 FROM " + affected_temp + " openivm_aff WHERE " +
		               affected_match + ");\n";
		cascade_sql += merge_query + "\n";
		cascade_sql += "DELETE FROM " + delta_view + " WHERE 1=1";
		if (!delta_ts_filter.empty()) {
			cascade_sql += " AND " + delta_ts_filter;
		}
		cascade_sql += ";\n";
		cascade_sql += "INSERT INTO " + delta_view + " (" + col_list + ") SELECT " + select_old + " FROM " + old_temp +
		               " openivm_old;\n";
		cascade_sql += "INSERT INTO " + delta_view + " (" + col_list + ") SELECT " + select_new + " FROM " +
		               data_table + " openivm_new WHERE EXISTS (SELECT 1 FROM " + affected_temp +
		               " openivm_aff WHERE " + new_match + ");\n";
		cascade_sql += "DROP TABLE " + old_temp + ";\nDROP TABLE " + affected_temp + ";\n";
		upsert_query = cascade_sql;
	}

	// Delete empty groups — a group is empty iff its row count is 0. We can only detect
	// this when the MV has a COUNT-type aggregate (either an explicit COUNT(*) / COUNT(x),
	// or a hidden count from AVG/STDDEV decomposition). For MVs with only SUM-style
	// aggregates, `SUM=0` does NOT mean the group is empty — e.g.
	//   SUM(CASE WHEN C_BALANCE < 0 THEN C_YTD_PAYMENT ELSE 0 END)
	// legitimately returns 0 for every row when C_YTD_PAYMENT is always 0. Skip the
	// cleanup in that case to avoid deleting valid rows.
	// Also skip when insert_only (groups can't reach zero from inserts alone).
	//
	// LEFT JOIN views (openivm_match_count present) were previously skipped outright, because a
	// preserved-side row with no match legitimately has inner-side COUNT = 0 and would look "empty".
	// That left genuinely emptied groups behind forever as zeroed rows. The right discriminator is the
	// OUTPUT-row count: a NULL-padded group still has count_star >= 1, whereas a group with no
	// preserved-side rows left has count_star = 0. Since the predicate ANDs every count column, the
	// NULL-padded case is preserved as long as count_star is among them -- so LEFT JOIN views are
	// included when a count_star-type column anchors the predicate, and skipped otherwise.
	// This relies on count_star actually counting NULL-padded output rows, which requires the
	// secondary delta to emit the reappearance row at EVERY level including a single LEFT JOIN.
	if (!insert_only) {
		// Find COUNT-type columns. aggregate_types is parallel to the user-facing aggregate
		// list; col_agg_type maps column name -> aggregate function. Also consider hidden
		// openivm_count_<N> columns from AVG/STDDEV decomposition.
		vector<string> count_cols;
		for (auto &entry : col_agg_type) {
			if (entry.second == "count" || entry.second == "count_star") {
				count_cols.push_back(entry.first);
			}
		}
		for (auto &column : aggregates) {
			if (column.rfind(openivm::COUNT_COL_PREFIX, 0) == 0 || column == openivm::COUNT_STAR_COL ||
			    column == openivm::DISTINCT_COUNT_COL) {
				count_cols.push_back(column);
			}
		}
		// LEFT JOIN views may only be cleaned up when an output-row count anchors the predicate.
		bool has_count_star_anchor = false;
		for (auto &c : count_cols) {
			auto it = col_agg_type.find(c);
			if (c == string(openivm::COUNT_STAR_COL) || (it != col_agg_type.end() && it->second == "count_star")) {
				has_count_star_anchor = true;
				break;
			}
		}
		if (has_match_count && !has_count_star_anchor) {
			count_cols.clear();
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] LEFT JOIN view without a count_star anchor — skipping "
			                    "empty-group cleanup so NULL-padded groups are not deleted\n");
		}
		if (!count_cols.empty()) {
			string delete_query = "\ndelete from " + data_table + " where ";
			for (size_t i = 0; i < count_cols.size(); i++) {
				if (i > 0) {
					delete_query += " and ";
				}
				delete_query += "COALESCE(" + count_cols[i] + ", 0) = 0";
			}
			delete_query += ";\n";
			upsert_query += delete_query;
		}
	}

	return upsert_query;
}

string CompileSimpleAggregates(const string &view_name, const vector<string> &column_names,
                               const string &view_query_sql, bool has_minmax, bool list_mode,
                               const string &delta_ts_filter, const string &catalog_prefix, bool /*insert_only*/,
                               const vector<LogicalType> &column_types, bool *out_full_recompute) {
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));

	// Any non-summable column (VARCHAR literal, CASE result, LIST) forces a full
	// MV recompute — the `sum(case mult=false then -<col> else <col> end)` path
	// can't type-check negation or list-zip element-wise for these types.
	// SIMPLE_AGGREGATE has no GROUP BY, so recompute the whole MV.
	bool has_non_summable_col = false;
	for (size_t i = 0; i < column_names.size() && i < column_types.size(); i++) {
		if (column_names[i] == string(openivm::MULTIPLICITY_COL) || column_names[i] == string(openivm::TIMESTAMP_COL)) {
			continue;
		}
		if (!IsSummableLogicalType(column_types[i])) {
			has_non_summable_col = true;
			break;
		}
	}

	bool full_recompute = has_minmax || has_non_summable_col;
	if (out_full_recompute) {
		*out_full_recompute = full_recompute;
	}
	if (full_recompute) {
		string delete_query = "DELETE FROM " + data_table + ";\n";
		string insert_query = "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
		return delete_query + insert_query;
	}

	string delta_view = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
	string mul = string(openivm::MULTIPLICITY_COL);
	string ts_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	auto decomp = DetectDerivedAggColumns(column_names);
	auto sum_null_count_cols = DetectSumNullCountColumns(column_names);
	auto &d_derived = decomp.derived_cols;
	auto &d_sum = decomp.sum_cols;
	auto &d_sum_sq = decomp.sum_sq_cols;
	auto &d_count = decomp.count_cols;

	// Single CTE consolidates all delta columns in one pass.
	string cte = "WITH openivm_delta AS (\n  SELECT ";
	string update_set;
	bool first = true;

	for (auto &raw_col : column_names) {
		if (raw_col == mul || d_derived.count(raw_col)) {
			continue; // skip multiplicity and derived columns (AVG, STDDEV, VARIANCE)
		}
		string column = SqlUtils::QuoteIdentifier(raw_col);
		if (!first) {
			cte += ",\n    ";
			update_set += ",\n  ";
		}
		first = false;
		if (list_mode) {
			// Z-set bag-aware list sum: per-row scale every element by w∈ℤ, then
			// list_reduce-add. NULL-list COALESCE preserves the previous semantics
			// for empty groups.
			cte += "COALESCE(list_reduce(list(list_transform(" + column + ", lambda x: " + mul +
			       " * x)), lambda a, b: list_transform(list_zip(a, b), lambda x: "
			       "x[1] + x[2])), " +
			       string(ZEROS_LIST) + ") AS d_" + column;
			update_set += column + " = list_transform(list_zip(" + column + ", (SELECT d_" + column +
			              " FROM openivm_delta)), lambda x: x[1] + x[2])";
		} else {
			// Z-set bag-aware sum: weight w∈ℤ scales the column value before SUM.
			cte += "SUM(" + mul + " * " + column + ") AS d_" + column;
			auto sum_count = sum_null_count_cols.find(column);
			if (sum_count == sum_null_count_cols.end()) {
				update_set += column + " = COALESCE(" + column + ", 0) + COALESCE((SELECT d_" + column +
				              " FROM openivm_delta), 0)";
			} else {
				string count_col = sum_count->second;
				string updated_sum =
				    "COALESCE(" + column + ", 0) + COALESCE((SELECT d_" + column + " FROM openivm_delta), 0)";
				string updated_count =
				    "COALESCE(" + count_col + ", 0) + COALESCE((SELECT d_" + count_col + " FROM openivm_delta), 0)";
				update_set += column + " = " + BuildNullableSum(updated_sum, updated_count);
			}
		}
	}
	cte += "\n  FROM " + delta_view + ts_where + "\n)\n";

	string result = cte + "UPDATE " + data_table + " SET\n  " + update_set + ";\n";

	// Recompute derived columns (AVG, STDDEV/VARIANCE) from updated hidden columns
	for (auto &entry : d_sum) {
		auto &alias = entry.first;
		auto &sum_col = entry.second;
		if (!d_count.count(alias)) {
			continue;
		}
		string count_col = d_count[alias];

		if (d_sum_sq.count(alias)) {
			// STDDEV/VARIANCE: recompute from sum, sum_sq, count
			string sum_sq_col = d_sum_sq.at(alias);
			bool is_pop = decomp.is_population.count(alias) && decomp.is_population.at(alias);
			bool do_sqrt = decomp.needs_sqrt.count(alias) && decomp.needs_sqrt.at(alias);
			string formula = BuildVarianceFormula(sum_col, sum_sq_col, count_col, is_pop, do_sqrt, false);
			result += "UPDATE " + data_table + " SET " + alias + " = " + formula + ";\n";
		} else {
			// AVG: recompute from sum/count
			result += "UPDATE " + data_table + " SET " + alias + " = " + BuildAvgFormula(sum_col, count_col) + ";\n";
		}
	}

	return result;
}

string CompileProjectionsFilters(const string &view_name, const vector<string> &column_names,
                                 const string &delta_ts_filter, const string &catalog_prefix, bool insert_only) {
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string mul = string(openivm::MULTIPLICITY_COL);
	string delta_view = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
	string ts_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	string select_columns;
	string delete_candidate_columns;
	string match_conditions;
	for (auto &raw_col : column_names) {
		if (raw_col != mul) {
			string column = SqlUtils::QuoteIdentifier(raw_col);
			match_conditions += "v." + column + " IS NOT DISTINCT FROM d." + column + " AND ";
			select_columns += column + ", ";
			delete_candidate_columns += "v." + column + " AS " + column + ", ";
		}
	}
	if (select_columns.empty()) {
		throw InvalidInputException("Cannot compile projection refresh for materialized view '%s': delta "
		                            "view '%s' has no "
		                            "user-visible columns",
		                            view_name, SqlUtils::DeltaName(view_name));
	}
	match_conditions.erase(match_conditions.size() - 5, 5);
	select_columns.erase(select_columns.size() - 2, 2);
	delete_candidate_columns.erase(delete_candidate_columns.size() - 2, 2);

	if (insert_only) {
		// Insert-only fast path: append projected delta rows directly. Do not run the signed net
		// consolidation/delete path here; byte-identical duplicates are distinct bag entries and
		// must be appended once per positive multiplicity.
		string mul_filter = delta_ts_filter.empty() ? "WHERE " + mul + " > 0" : ts_where + " AND " + mul + " > 0";
		string insert_query = "INSERT INTO " + data_table + " SELECT " + select_columns + "\nFROM " + delta_view +
		                      "\nCROSS JOIN generate_series(1, " + mul + "::BIGINT)\n" + mul_filter + ";\n";
		return insert_query;
	}

	// Consolidate deltas into net changes per distinct tuple (1 pass over delta_view).
	// _net > 0 = net insertions, _net < 0 = net deletions. With integer weights this is
	// just SUM(weight) — no CASE round-trip needed.
	string cte_body = "SELECT " + select_columns + ",\n    SUM(" + mul + ") AS _net\n  FROM " + delta_view + ts_where +
	                  "\n  GROUP BY " + select_columns + "\n  HAVING SUM(" + mul + ") != 0";

	// DELETE: remove exactly |_net| copies per tuple using rowid + ROW_NUMBER. Join
	// negative net tuples to the MV first so duplicate ranking only touches affected
	// candidates, not the whole materialized view.
	string delete_query = "WITH openivm_net AS (\n  " + cte_body +
	                      "\n)\n"
	                      ", openivm_delete_net AS (\n"
	                      "  SELECT * FROM openivm_net WHERE _net < 0\n"
	                      "), openivm_delete_candidates AS (\n"
	                      "  SELECT v.rowid, " +
	                      delete_candidate_columns +
	                      ", d._net\n"
	                      "  FROM " +
	                      data_table + " v JOIN openivm_delete_net d ON " + match_conditions +
	                      "\n), openivm_ranked_deletes AS (\n"
	                      "  SELECT rowid, _net,\n"
	                      "    ROW_NUMBER() OVER (PARTITION BY " +
	                      select_columns +
	                      " ORDER BY rowid) AS _rn\n"
	                      "  FROM openivm_delete_candidates\n"
	                      ")\n"
	                      "DELETE FROM " +
	                      data_table +
	                      " WHERE rowid IN (\n"
	                      "  SELECT rowid FROM openivm_ranked_deletes WHERE _rn <= -_net\n"
	                      ");\n\n";

	// INSERT: replicate each net-insert tuple _net times using generate_series.
	string insert_query = "WITH openivm_net AS (\n  " + cte_body +
	                      "\n)\n"
	                      "INSERT INTO " +
	                      data_table + " SELECT " + select_columns +
	                      "\nFROM openivm_net, generate_series(1, openivm_net._net::BIGINT)\nWHERE "
	                      "openivm_net._net > 0;\n";

	return delete_query + insert_query;
}

string CompileFullRecompute(const string &view_name, const string &view_query_sql, const string &catalog_prefix) {
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	return SqlUtils::BuildFullRecomputeSQL(data_table, view_query_sql);
}

string CompileGroupRecompute(const string &view_name, const string &view_query_sql, const vector<string> &group_columns,
                             const vector<GroupRecomputeDeltaSpec> &delta_table_specs, const string &catalog_prefix,
                             const string &lpts_table_prefix, bool emit_cascade_delta,
                             GroupRecomputeAffectedMode affected_mode) {
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));

	// No GROUP BY columns or no source deltas registered → can't scope; fall back to full.
	if (group_columns.empty() || delta_table_specs.empty()) {
		return CompileFullRecompute(view_name, view_query_sql, catalog_prefix);
	}

	string group_csv = SqlUtils::JoinQuotedColumns(group_columns);

	if (ShouldUseCurrentDiffGroupRecompute(view_query_sql, delta_table_specs)) {
		OPENIVM_DEBUG_PRINT("[CompileGroupRecompute] using current-diff affected "
		                    "keys for large affected query\n");
		return BuildCurrentDiffGroupRecomputeSQL(view_name, data_table, view_query_sql, group_columns, catalog_prefix,
		                                         emit_cascade_delta);
	}
	if (affected_mode == GroupRecomputeAffectedMode::CURRENT_DIFF) {
		OPENIVM_DEBUG_PRINT("[CompileGroupRecompute] using current-diff affected "
		                    "keys from model metadata\n");
		return BuildCurrentDiffGroupRecomputeSQL(view_name, data_table, view_query_sql, group_columns, catalog_prefix,
		                                         emit_cascade_delta);
	}
	string affected_view_query_sql = view_query_sql;
	if (affected_mode == GroupRecomputeAffectedMode::SOURCE_DELTA_RELAX_AGGREGATE_FILTER &&
	    !TryBuildGroupRecomputeAffectedQuery(view_query_sql, affected_view_query_sql)) {
		OPENIVM_DEBUG_PRINT("[CompileGroupRecompute] using current-diff affected "
		                    "keys; aggregate filter relaxation "
		                    "was ambiguous\n");
		return BuildCurrentDiffGroupRecomputeSQL(view_name, data_table, view_query_sql, group_columns, catalog_prefix,
		                                         emit_cascade_delta);
	}

	// For each source table T_i, build a "view query restricted to T_i's delta" variant by
	// substituting the qualified `cat.schema.<base>` reference with a delta-filtered subquery,
	// then project DISTINCT group_columns. Union across sources gives the affected-keys set.
	string affected_subquery;
	for (size_t i = 0; i < delta_table_specs.size(); i++) {
		const auto &spec = delta_table_specs[i];
		const string &base = spec.base_table;

		string delta_subselect;
		if (spec.is_ducklake) {
			delta_subselect =
			    "(SELECT * FROM " +
			    SqlUtils::DuckLakeTableFunction("ducklake_table_insertions", spec.ducklake_catalog,
			                                    spec.ducklake_schema, base, spec.last_snapshot_id,
			                                    spec.current_snapshot_id) +
			    "\nUNION ALL\nSELECT * FROM " +
			    SqlUtils::DuckLakeTableFunction("ducklake_table_deletions", spec.ducklake_catalog, spec.ducklake_schema,
			                                    base, spec.last_snapshot_id, spec.current_snapshot_id) +
			    ")";
		} else {
			string delta_basename = string(openivm::DELTA_PREFIX) + base;
			delta_subselect =
			    BuildStandardDeltaRowsSQL(catalog_prefix + SqlUtils::QuoteIdentifier(delta_basename), spec.last_update);
		}

		// LPTS form ALWAYS references base tables as fully-qualified `cat.schema.tbl`, even when
		// the catalog is the default `memory` (so `catalog_prefix` is empty for SQL output, but
		// `lpts_table_prefix` is still `memory.main.` here). Build one affected-key query per
		// source occurrence. Self-joins must replace only one occurrence at a time; replacing every
		// occurrence with the delta would miss base rows affected by a delta row on another arm.
		// Original-SQL recompute paths (e.g. ROLLUP/GROUPING SETS) can contain unqualified
		// table names, so fall back to identifier-safe bare replacement when the exact LPTS
		// form is absent.
		string source_full = (lpts_table_prefix.empty() ? catalog_prefix : lpts_table_prefix) + base;
		auto filtered_variants =
		    SqlUtils::ReplaceEachPlainOccurrence(affected_view_query_sql, source_full, delta_subselect);
		if (filtered_variants.empty()) {
			filtered_variants = SqlUtils::ReplaceEachTableReference(affected_view_query_sql, base, delta_subselect);
		}
		if (filtered_variants.empty()) {
			string filtered = SqlUtils::ReplaceAllOccurrences(affected_view_query_sql, source_full, delta_subselect);
			if (filtered == affected_view_query_sql) {
				filtered = SqlUtils::ReplaceTableReferences(affected_view_query_sql, base, delta_subselect);
			}
			filtered_variants.push_back(std::move(filtered));
		}

		for (idx_t occurrence = 0; occurrence < filtered_variants.size(); occurrence++) {
			if (!affected_subquery.empty()) {
				affected_subquery += "\n  UNION\n  ";
			} else {
				affected_subquery += "  ";
			}
			affected_subquery += "SELECT DISTINCT " + group_csv + " FROM (" + filtered_variants[occurrence] +
			                     ") openivm_src_" + to_string(i) + "_" + to_string(occurrence);
		}
	}

	// EXISTS-based, NULL-safe match (IS NOT DISTINCT FROM). The affected-keys subquery is shared by
	// DELETE and INSERT, so materialize it once per refresh instead of recomputing the same delta-
	// scoped view query twice.
	string match_clause = SqlUtils::BuildNullSafeMatch(group_columns, "openivm_aff", "openivm_tgt");
	string affected_temp_table = SqlUtils::QuoteIdentifier("openivm_affected_" + view_name);

	OPENIVM_DEBUG_PRINT("[CompileGroupRecompute] %zu group cols, %zu source "
	                    "deltas, cascade delta: %s\n",
	                    group_columns.size(), delta_table_specs.size(), emit_cascade_delta ? "enabled" : "disabled");
	if (!emit_cascade_delta) {
		return BuildAffectedKeyRefreshSQL(data_table, view_query_sql, affected_subquery, "openivm_tgt",
		                                  "AS openivm_tgt", "openivm_aff", match_clause, match_clause,
		                                  affected_temp_table);
	}

	string delete_where =
	    "EXISTS (\n  SELECT 1 FROM " + affected_temp_table + " AS openivm_aff WHERE " + match_clause + "\n)";
	string insert_where =
	    "EXISTS (\n  SELECT 1 FROM " + affected_temp_table + " AS openivm_aff WHERE " + match_clause + "\n)";
	string delta_table = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
	string old_temp_table = SqlUtils::QuoteIdentifier(string(openivm::TEMP_TABLE_PREFIX) + view_name);
	string new_temp_table = SqlUtils::QuoteIdentifier(string("openivm_new_") + view_name);
	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + affected_temp_table + " AS\n" + affected_subquery + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + old_temp_table + " AS\nSELECT * FROM " + data_table +
	       " AS openivm_tgt\nWHERE " + delete_where + ";\n\n";
	sql += "CREATE OR REPLACE TEMP TABLE " + new_temp_table + " AS\nSELECT * FROM (" + view_query_sql +
	       ") AS openivm_tgt\nWHERE " + insert_where + ";\n\n";
	sql += "DELETE FROM " + data_table + " AS openivm_tgt\nWHERE " + delete_where + ";\n\n";
	sql += "INSERT INTO " + data_table + "\nSELECT * FROM " + new_temp_table + ";\n";
	sql += "\n" + BuildSignedMultisetDeltaInsertSQL(delta_table, old_temp_table, new_temp_table);
	sql += "\nDROP TABLE IF EXISTS " + affected_temp_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + old_temp_table + ";\n";
	sql += "DROP TABLE IF EXISTS " + new_temp_table + ";\n";
	return sql;
}

} // namespace duckdb
