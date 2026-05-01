#include "upsert/openivm_compile_upsert.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"

namespace duckdb {

// Zero-initialized 64-element float list, used as COALESCE default for NULL list aggregates.
static constexpr const char *ZEROS_LIST = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";

/// Quote a column name for safe use in generated SQL. Handles special characters like ().
static string Q(const string &name) {
	return OpenIVMUtils::QuoteIdentifier(name);
}

static string JoinQuotedColumns(const vector<string> &columns) {
	string result;
	for (size_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += Q(columns[i]);
	}
	return result;
}

static string JoinQualifiedQuotedColumns(const vector<string> &columns, const string &alias) {
	string result;
	for (size_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += ", ";
		}
		result += alias + "." + Q(columns[i]);
	}
	return result;
}

static string BuildNullSafeMatch(const vector<string> &columns, const string &lhs_alias, const string &rhs_alias) {
	string result;
	for (size_t i = 0; i < columns.size(); i++) {
		if (i > 0) {
			result += " AND ";
		}
		result += lhs_alias + "." + Q(columns[i]) + " IS NOT DISTINCT FROM " + rhs_alias + "." + Q(columns[i]);
	}
	return result;
}

static string BuildUpdatedAggregateColumn(const string &col) {
	return "COALESCE(v." + col + " + d." + col + ", v." + col + ", d." + col + ")";
}

static string BuildNullSafeExtremumUpdate(const string &col, const string &fn) {
	return "CASE WHEN v." + col + " IS NULL THEN d." + col + " WHEN d." + col + " IS NULL THEN v." + col +
	       " ELSE " + fn + "(v." + col + ", d." + col + ") END";
}

/// Detect AVG and STDDEV/VARIANCE decomposition columns from the column list.
/// AVG(x) is stored as _ivm_sum_<alias>, _ivm_count_<alias>, and <alias>.
/// STDDEV/VARIANCE(x) adds a sum-of-squares column with a prefix encoding the function type:
///   _ivm_sum_sq_  = stddev_samp (apply sqrt, denominator N-1)
///   _ivm_var_sq_  = var_samp    (no sqrt, denominator N-1)
///   _ivm_sum_sqp_ = stddev_pop  (apply sqrt, denominator N)
///   _ivm_var_sqp_ = var_pop     (no sqrt, denominator N)
struct DerivedAggDecomposition {
	unordered_set<string> derived_cols;        // aliases to skip in MERGE
	unordered_map<string, string> sum_cols;    // alias → _ivm_sum_<alias>
	unordered_map<string, string> sum_sq_cols; // alias → _ivm_*_sq*_<alias>
	unordered_map<string, string> count_cols;  // alias → _ivm_count_<alias>
	// Per-alias flags decoded from the sum_sq prefix
	unordered_map<string, bool> needs_sqrt;    // alias → true if stddev (not variance)
	unordered_map<string, bool> is_population; // alias → true if population variant
};

/// Try to match a column against a prefix. Returns the alias suffix if matched, empty string otherwise.
static string MatchPrefix(const string &col, const string &prefix) {
	if (col.size() > prefix.size() && col.substr(0, prefix.size()) == prefix) {
		return col.substr(prefix.size());
	}
	return "";
}

static DerivedAggDecomposition DetectDerivedAggColumns(const vector<string> &columns) {
	DerivedAggDecomposition result;
	static const string sum_prefix(ivm::SUM_COL_PREFIX);
	static const string count_prefix(ivm::COUNT_COL_PREFIX);
	// Sum-of-squares prefixes (check longer ones first to avoid false matches)
	static const string sum_sqp_prefix(ivm::SUM_SQP_COL_PREFIX); // stddev_pop
	static const string var_sqp_prefix(ivm::VAR_SQP_COL_PREFIX); // var_pop
	static const string sum_sq_prefix(ivm::SUM_SQ_COL_PREFIX);   // stddev_samp
	static const string var_sq_prefix(ivm::VAR_SQ_COL_PREFIX);   // var_samp

	for (auto &col : columns) {
		// Check sum-of-squares prefixes BEFORE sum (they start with _ivm_sum_ or _ivm_var_)
		// Assignments moved out of if-conditions to satisfy bugprone-assignment-in-if-condition.
		string alias;
		alias = MatchPrefix(col, sum_sqp_prefix);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = true;
			result.is_population[alias] = true;
			continue;
		}
		alias = MatchPrefix(col, var_sqp_prefix);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = false;
			result.is_population[alias] = true;
			continue;
		}
		alias = MatchPrefix(col, sum_sq_prefix);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = true;
			result.is_population[alias] = false;
			continue;
		}
		alias = MatchPrefix(col, var_sq_prefix);
		if (!alias.empty()) {
			result.sum_sq_cols[alias] = col;
			result.needs_sqrt[alias] = false;
			result.is_population[alias] = false;
			continue;
		}
		alias = MatchPrefix(col, sum_prefix);
		if (!alias.empty()) {
			result.sum_cols[alias] = col;
			continue;
		}
		alias = MatchPrefix(col, count_prefix);
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

// Types that can be added/subtracted in `sum(case mult=false then -<col> else <col> end)`.
// Anything else (VARCHAR, BOOLEAN, LIST, STRUCT, MAP, ...) breaks the delta path.
static bool IsSummableType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::FLOAT:
	case LogicalTypeId::DOUBLE:
	case LogicalTypeId::DECIMAL:
		return true;
	default:
		return false;
	}
}

string CompileAggregateGroups(const string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql, bool has_minmax,
                              bool list_mode, const string &delta_ts_filter, const vector<string> &group_column_names,
                              const string &catalog_prefix, bool insert_only, const vector<string> &aggregate_types,
                              const vector<LogicalType> &column_types) {
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));
	string delta_view = catalog_prefix + Q(OpenIVMUtils::DeltaName(view_name));

	// Extract GROUP BY keys: from index (standard path) or from metadata (DuckLake fallback)
	vector<string> keys;
	if (index_delta_view_catalog_entry) {
		auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
		auto key_ids = index_catalog_entry->column_ids;
		for (size_t i = 0; i < key_ids.size(); i++) {
			keys.emplace_back(Q(column_names[key_ids[i]]));
		}
	} else {
		// No index (e.g. DuckLake) — use group column names from metadata
		for (auto &col : group_column_names) {
			keys.emplace_back(Q(col));
		}
	}

	unordered_set<std::string> keys_set(keys.begin(), keys.end());
	vector<string> aggregates;
	// Parallel: true if the matching aggregate column has a non-summable delta type
	// (VARCHAR CASE output, 'GC' literal, UPPER(group_col), LIST(col), etc.).
	bool has_non_summable_col = false;
	for (size_t i = 0; i < column_names.size(); i++) {
		const auto &column = column_names[i];
		if (keys_set.find(Q(column)) == keys_set.end() && column != string(ivm::MULTIPLICITY_COL)) {
			aggregates.push_back(Q(column));
			if (i < column_types.size() && !IsSummableType(column_types[i])) {
				has_non_summable_col = true;
			}
		}
	}
	// A non-summable non-key column can't be maintained by the delta-sum MERGE path.
	// Fall back to group-recompute: delete affected groups, re-insert from view query.
	// This is correct for VARCHAR literals, string functions of group keys, LIST
	// aggregates, and CASE over aggregates alike. Slower than MERGE, faster than
	// full recompute (only affected groups are re-evaluated).
	bool needs_group_recompute = has_non_summable_col;

	auto decomp = DetectDerivedAggColumns(aggregates);

	// Detect AVG/STDDEV-derived columns that can't be maintained incrementally.
	//
	// Case 1 — "orphan derived": hidden _ivm_sum_<alias> + _ivm_count_<alias> exist
	// but no user-visible column has that alias. Happens when the user wraps
	// AVG/STDDEV in a non-pass-through expression at the top SELECT so the
	// top-level alias diverges from the CTE's alias (query_0540 shape:
	// CTE `avg_bal` → top `ROUND(avg_bal, 2) AS avg`). The MERGE formula
	// `avg = _ivm_sum_avg / _ivm_count_avg` can't reconstruct ROUND without
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
		if (!aggregate_set.count(Q(alias)) && !aggregate_set.count(alias)) {
			has_orphan_derived = true;
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] Orphan derived alias '%s' → group-recompute\n",
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
		static const unordered_set<string> DECOMPOSED_TYPES_CHK = {"avg",      "stddev",   "stddev_samp", "stddev_pop",
		                                                           "variance", "var_samp", "var_pop"};
		// Pass 1 — "type excess": more non-decomposed aggregate types than
		// aggregate columns (INCLUDING hidden helpers like _ivm_match_count and
		// _ivm_*) means at least one aggregate is consumed inside a computed
		// expression (query_1987 `ROUND(SUM(x) / COUNT(y), 2) AS avg_bal`, two
		// underlying aggregates but one output column). Hidden helper columns get
		// counted because they DO correspond to aggregate_types entries (e.g.
		// RewriteLeftJoinMatchCount adds a real COUNT to the plan).
		idx_t non_decomposed_type_count = 0;
		for (auto &t : aggregate_types) {
			if (!DECOMPOSED_TYPES_CHK.count(t)) {
				non_decomposed_type_count++;
			}
		}
		idx_t total_agg_col_count = 0;
		for (auto &column : aggregates) {
			if (decomp.derived_cols.count(column)) {
				// Derived user columns (avg_b, std_b) don't consume aggregate_types slots —
				// their hidden _ivm_sum_* / _ivm_count_* companions do.
				continue;
			}
			if (column.find(string(ivm::SUM_COL_PREFIX)) == 0 || column.find(string(ivm::SUM_SQ_COL_PREFIX)) == 0 ||
			    column.find(string(ivm::SUM_SQP_COL_PREFIX)) == 0) {
				// Decomposed aggregate hidden columns are counted against the decomposed
				// aggregate_types entries (avg, stddev) which we *did not* count above.
				continue;
			}
			if (column.find(string(ivm::COUNT_COL_PREFIX)) == 0) {
				// Same — the COUNT half of an AVG/STDDEV decomposition.
				continue;
			}
			// _ivm_match_count / _ivm_right_match_count are added by
			// RewriteLeftJoinMatchCount *after* AnalyzePlan captures aggregate_types,
			// so aggregate_types does NOT include them. Exclude them here for a
			// like-with-like comparison.
			if (column == Q(string(ivm::MATCH_COUNT_COL)) || column == string(ivm::MATCH_COUNT_COL) ||
			    column == Q(string(ivm::RIGHT_MATCH_COUNT_COL)) || column == string(ivm::RIGHT_MATCH_COUNT_COL)) {
				continue;
			}
			total_agg_col_count++;
		}
		if (non_decomposed_type_count > total_agg_col_count) {
			has_computed_over_derived = true;
			OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] %llu non-decomposed aggregate types > %llu agg columns "
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
				if (decomp.derived_cols.count(column) || column.find("_ivm_") != string::npos) {
					continue;
				}
				while (probe_idx < aggregate_types.size() && DECOMPOSED_TYPES_CHK.count(aggregate_types[probe_idx])) {
					probe_idx++;
				}
				if (probe_idx >= aggregate_types.size()) {
					has_computed_over_derived = true;
					OPENIVM_DEBUG_PRINT(
					    "[CompileAggregateGroups] Column orphan '%s' over derived aggregates → group-recompute\n",
					    column.c_str());
					break;
				}
				probe_idx++;
			}
		}
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
	//       comment in openivm_parser.cpp (`query_1502/1696/1746/1749`).
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
			OPENIVM_DEBUG_PRINT(
			    "[CompileAggregateGroups] has_minmax=true with no MIN/MAX in agg_types — LEFT JOIN + computed "
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
	static const unordered_set<string> DECOMPOSED_TYPES = {"avg",      "stddev",   "stddev_samp", "stddev_pop",
	                                                       "variance", "var_samp", "var_pop"};
	unordered_map<string, string> col_agg_type;
	if (!aggregate_types.empty()) {
		idx_t type_idx = 0;
		for (auto &column : aggregates) {
			if (decomp.derived_cols.count(column) || column.find("_ivm_") != string::npos) {
				continue;
			}
			// Skip decomposed aggregate_types entries (avg → SUM+COUNT hidden cols)
			while (type_idx < aggregate_types.size() && DECOMPOSED_TYPES.count(aggregate_types[type_idx])) {
				type_idx++;
			}
			if (type_idx < aggregate_types.size()) {
				col_agg_type[column] = aggregate_types[type_idx++];
			}
		}
		OPENIVM_DEBUG_PRINT("[CompileAggregateGroups] agg_type map: %zu entries from %zu types, %zu aggregates\n",
		                    col_agg_type.size(), aggregate_types.size(), aggregates.size());
	}

	if (needs_group_recompute || (has_minmax && !insert_only)) {
		// Group-recompute strategy: delete affected groups, re-insert from original query.
		// Always triggered by non-summable columns (LIST aggregates, VARCHAR literals,
		// CASE results, etc.) — even in insert-only mode, the MERGE path emits
		// `sum(case mult=false then -<col> else <col> end)` which fails type-checking
		// for VARCHAR etc. regardless of whether the negative branch is reached.
		// For MIN/MAX without non-summable cols, insert_only uses GREATEST/LEAST in
		// the MERGE path below.
		string keys_tuple;
		string null_check;
		for (size_t i = 0; i < keys.size(); i++) {
			keys_tuple += keys[i];
			if (i != keys.size() - 1) {
				keys_tuple += ", ";
			}
			if (!null_check.empty()) {
				null_check += " AND ";
			}
			null_check += keys[i] + " IS NULL";
		}
		string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
		// The all-NULL group (every key column IS NULL) is unreachable via `(keys) IN (...)`
		// because NULL IN subquery is NULL, not true. That group is produced by RIGHT/FULL
		// OUTER JOIN unmatched-right-side rows (NULL pads the left keys). If the delta has
		// any row with all-NULL keys, we always recompute the NULL group. This is cheap
		// (at most one group) and correct for INNER/LEFT JOIN too (no such group exists).
		string affected = "select distinct " + keys_tuple + " from " + delta_view + delta_where;
		string where = "(" + keys_tuple + ") in (\n  " + affected + "\n) OR (" + null_check + ")";
		string delete_query = "delete from " + data_table + " where " + where + ";\n";
		string insert_query = "insert into " + data_table + "\n" + "select * from (" + view_query_sql +
		                      ") _ivm_recompute\n" + "where " + where + ";\n";
		return delete_query + "\n" + insert_query;
	}

	// CTE: consolidate deltas per group
	string cte_string = "with ivm_cte AS (\n";
	string cte_select_string = "select ";
	for (auto &key : keys) {
		cte_select_string = cte_select_string + key + ", ";
	}
	for (auto &column : aggregates) {
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
			cte_select_string +=
			    "\n\tlist_reduce(list(list_transform(" + column + ", lambda x: " + string(ivm::MULTIPLICITY_COL) +
			    " * x)), lambda a, b: list_transform(list_zip(a, b), lambda x: x[1] + x[2])) AS " + column + ", ";
		} else {
			// Z-set bag-aware sum: weight w∈ℤ scales the column value before SUM.
			cte_select_string = cte_select_string + "\n\tsum(" + string(ivm::MULTIPLICITY_COL) + " * " + column +
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
	string cte_group_by_string = "group by ";
	for (auto &key : keys) {
		cte_group_by_string = cte_group_by_string + key + ", ";
	}
	cte_group_by_string.erase(cte_group_by_string.size() - 2, 2);

	string cte_body = cte_select_string + cte_from_string + cte_group_by_string;

	// MERGE: single-pass upsert — UPDATE existing groups, INSERT new groups.
	// Uses IS NOT DISTINCT FROM for NULL-safe key matching.
	string on_clause;
	for (size_t i = 0; i < keys.size(); i++) {
		if (i > 0) {
			on_clause += " AND ";
		}
		on_clause += "v." + keys[i] + " IS NOT DISTINCT FROM d." + keys[i];
	}

	auto &derived_cols = decomp.derived_cols;
	auto &d_sum_cols = decomp.sum_cols;
	auto &d_sum_sq_cols = decomp.sum_sq_cols;
	auto &d_count_cols = decomp.count_cols;

	// Detect _ivm_match_count for LEFT JOIN incremental MERGE (Larson & Zhou).
	// When present, use COALESCE(v.col, 0) for aggregate updates to handle NULL→value transitions.
	string match_count_col;
	bool has_match_count = false;
	for (auto &col : aggregates) {
		if (col == Q(string(ivm::MATCH_COUNT_COL))) {
			has_match_count = true;
			match_count_col = col;
			break;
		}
	}

	string update_set;
	string insert_cols, insert_vals;
	{
		const string &zeros_list = ZEROS_LIST;
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

			if (derived_cols.count(column)) {
				// Derived column: compute from hidden columns
				string sum_col = d_sum_cols.count(column) ? d_sum_cols.at(column) : "";
				string count_col = d_count_cols.count(column) ? d_count_cols.at(column) : "";
				if (sum_col.empty() || count_col.empty()) {
					update_set += column + " = " + BuildUpdatedAggregateColumn(column);
					insert_vals += "d." + column;
					continue;
				}
				bool has_sum_sq = d_sum_sq_cols.count(column) > 0;
				if (has_sum_sq) {
					// STDDEV/VARIANCE. Must match the CASE-WHEN + clamp semantics that the
					// CREATE-MV formula in RewriteDerivedAggregates already applies; the raw
					// algebraic formula alone can (a) produce a numeric result for groups
					// with count <= threshold (pop: 0, sample: 1) where the base query
					// yields NULL, and (b) feed sqrt() a tiny negative value after
					// INSERT+DELETE on flat-valued data where floating-point reassociation
					// of (sum_sq - sum²/count) drifts below zero — crashing the refresh.
					string sum_sq_col = d_sum_sq_cols.at(column);
					string new_sum = BuildUpdatedAggregateColumn(sum_col);
					string new_sq = BuildUpdatedAggregateColumn(sum_sq_col);
					string new_n = BuildUpdatedAggregateColumn(count_col);
					bool is_pop = decomp.is_population.count(column) && decomp.is_population.at(column);
					bool do_sqrt = decomp.needs_sqrt.count(column) && decomp.needs_sqrt.at(column);
					int threshold = is_pop ? 0 : 1;

					string denom = is_pop ? "NULLIF(" + new_n + ", 0)" : "NULLIF(" + new_n + " - 1, 0)";
					string var_raw =
					    "((" + new_sq + ") - (" + new_sum + ") * (" + new_sum + ") / (" + new_n + ")) / " + denom;
					// 0::DOUBLE so GREATEST binds to DOUBLE, not INTEGER (would up-cast silently).
					string var_safe = "GREATEST(" + var_raw + ", 0::DOUBLE)";
					string formula = do_sqrt ? "sqrt(" + var_safe + ")" : var_safe;
					update_set += column + " = CASE WHEN " + new_n + " > " + std::to_string(threshold) + " THEN " +
					              formula + " ELSE NULL END";

					string d_denom = is_pop ? "NULLIF(d." + count_col + ", 0)" : "NULLIF(d." + count_col + " - 1, 0)";
					string d_var_raw = "((d." + sum_sq_col + ") - (d." + sum_col + ") * (d." + sum_col + ") / (d." +
					                   count_col + ")) / " + d_denom;
					string d_var_safe = "GREATEST(" + d_var_raw + ", 0::DOUBLE)";
					string d_formula = do_sqrt ? "sqrt(" + d_var_safe + ")" : d_var_safe;
					insert_vals += "CASE WHEN d." + count_col + " > " + std::to_string(threshold) + " THEN " +
					               d_formula + " ELSE NULL END";
				} else {
					// AVG
					update_set += column + " = " + BuildUpdatedAggregateColumn(sum_col) + "::DOUBLE / NULLIF(" +
					              BuildUpdatedAggregateColumn(count_col) + ", 0)";
					insert_vals += "d." + sum_col + "::DOUBLE / NULLIF(d." + count_col + ", 0)";
				}
			} else {
				// Regular aggregate column
				string agg_type = col_agg_type.count(column) ? col_agg_type[column] : "";
				if (insert_only && agg_type == "min") {
					update_set += column + " = " + BuildNullSafeExtremumUpdate(column, "LEAST");
				} else if (insert_only && agg_type == "max") {
					update_set += column + " = " + BuildNullSafeExtremumUpdate(column, "GREATEST");
				} else if (list_mode) {
					update_set += column + " = list_transform(list_zip(v." + column + ", d." + column +
					              "), lambda x: x[1] + x[2])";
				} else {
					update_set += column + " = " + BuildUpdatedAggregateColumn(column);
				}
				insert_vals += "d." + column;
			}
		}
		for (auto &key : keys) {
			insert_cols += key + ", ";
		}
		for (size_t i = 0; i < aggregates.size(); i++) {
			insert_cols += aggregates[i];
			if (i < aggregates.size() - 1) {
				insert_cols += ", ";
			}
		}
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
			if (agg_type == "count_star") {
				// Left-side-driven: always update normally.
				lj_update_set += col + " = " + BuildUpdatedAggregateColumn(col);
				continue;
			}
			// Determine if this column should be 0 (count-like) or NULL (sum-like) when unmatched.
			// Check both the aggregate_types metadata AND the hidden column prefix
			// (hidden _ivm_count_* columns from AVG/STDDEV decomposition don't have agg_type entries).
			bool is_count = (agg_type == "count" || col.find(string(ivm::COUNT_COL_PREFIX)) == 0);
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
			lj_update_set += col + " = CASE WHEN " + mc_new + " > 0 THEN " + BuildUpdatedAggregateColumn(col) +
			                 " WHEN COALESCE(v." + match_count_col + ", 0) = 0 THEN v." + col + " ELSE " + null_val +
			                 " END";
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
			if (agg_type == "count_star") {
				cond_insert_vals += "d." + aggregates[i];
				continue;
			}
			bool is_count = (agg_type == "count" || aggregates[i].find(string(ivm::COUNT_COL_PREFIX)) == 0);
			string null_val = is_count ? "0" : "NULL";
			cond_insert_vals +=
			    "CASE WHEN d." + match_count_col + " > 0 THEN d." + aggregates[i] + " ELSE " + null_val + " END";
		}
		merge_query = "WITH ivm_cte AS (\n" + cte_body + ")\n" + "MERGE INTO " + data_table + " v USING ivm_cte d\n" +
		              "ON " + on_clause + "\n" + "WHEN MATCHED THEN UPDATE SET " + lj_update_set + "\n" +
		              "WHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (";
		for (auto &key : keys) {
			merge_query += "d." + key + ", ";
		}
		merge_query += cond_insert_vals + ");\n";
	} else {
		merge_query = "WITH ivm_cte AS (\n" + cte_body + ")\n" + "MERGE INTO " + data_table + " v USING ivm_cte d\n" +
		              "ON " + on_clause + "\n" + "WHEN MATCHED THEN UPDATE SET " + update_set + "\n" +
		              "WHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (";
		for (auto &key : keys) {
			merge_query += "d." + key + ", ";
		}
		merge_query += insert_vals + ");\n";
	}

	string upsert_query = merge_query + "\n";

	// Delete empty groups — a group is empty iff its row count is 0. We can only detect
	// this when the MV has a COUNT-type aggregate (either an explicit COUNT(*) / COUNT(x),
	// or a hidden count from AVG/STDDEV decomposition). For MVs with only SUM-style
	// aggregates, `SUM=0` does NOT mean the group is empty — e.g.
	//   SUM(CASE WHEN C_BALANCE < 0 THEN C_YTD_PAYMENT ELSE 0 END)
	// legitimately returns 0 for every row when C_YTD_PAYMENT is always 0. Skip the
	// cleanup in that case to avoid deleting valid rows.
	// Also skip when insert_only (groups can't reach zero from inserts alone) and when
	// _ivm_match_count is present (LEFT JOIN preserved-side NULL aggregates are valid).
	if (!insert_only && !has_match_count) {
		// Find COUNT-type columns. aggregate_types is parallel to the user-facing aggregate
		// list; col_agg_type maps column name -> aggregate function. Also consider hidden
		// _ivm_count_<N> columns from AVG/STDDEV decomposition.
		vector<string> count_cols;
		for (auto &entry : col_agg_type) {
			if (entry.second == "count" || entry.second == "count_star") {
				count_cols.push_back(entry.first);
			}
		}
		for (auto &column : aggregates) {
			if (column.rfind(ivm::COUNT_COL_PREFIX, 0) == 0 || column == ivm::COUNT_STAR_COL ||
			    column == ivm::DISTINCT_COUNT_COL) {
				count_cols.push_back(column);
			}
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
                               const vector<LogicalType> &column_types) {
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));

	// Any non-summable column (VARCHAR literal, CASE result, LIST) forces a full
	// MV recompute — the `sum(case mult=false then -<col> else <col> end)` path
	// can't type-check negation or list-zip element-wise for these types.
	// SIMPLE_AGGREGATE has no GROUP BY, so recompute the whole MV.
	bool has_non_summable_col = false;
	for (size_t i = 0; i < column_names.size() && i < column_types.size(); i++) {
		if (column_names[i] == string(ivm::MULTIPLICITY_COL) || column_names[i] == string(ivm::TIMESTAMP_COL)) {
			continue;
		}
		if (!IsSummableType(column_types[i])) {
			has_non_summable_col = true;
			break;
		}
	}

	if (has_minmax || has_non_summable_col) {
		string delete_query = "DELETE FROM " + data_table + ";\n";
		string insert_query = "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
		return delete_query + insert_query;
	}

	string delta_view = catalog_prefix + Q(OpenIVMUtils::DeltaName(view_name));
	string mul = string(ivm::MULTIPLICITY_COL);
	string ts_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	auto decomp = DetectDerivedAggColumns(column_names);
	auto &d_derived = decomp.derived_cols;
	auto &d_sum = decomp.sum_cols;
	auto &d_sum_sq = decomp.sum_sq_cols;
	auto &d_count = decomp.count_cols;

	// Single CTE consolidates all delta columns in one pass.
	string cte = "WITH _ivm_delta AS (\n  SELECT ";
	string update_set;
	bool first = true;
	string zeros_list = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";

	for (auto &raw_col : column_names) {
		if (raw_col == mul || d_derived.count(raw_col)) {
			continue; // skip multiplicity and derived columns (AVG, STDDEV, VARIANCE)
		}
		string column = Q(raw_col);
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
			       " * x)), lambda a, b: list_transform(list_zip(a, b), lambda x: x[1] + x[2])), " + zeros_list +
			       ") AS d_" + column;
			update_set += column + " = list_transform(list_zip(" + column + ", (SELECT d_" + column +
			              " FROM _ivm_delta)), lambda x: x[1] + x[2])";
		} else {
			// Z-set bag-aware sum: weight w∈ℤ scales the column value before SUM.
			cte += "SUM(" + mul + " * " + column + ") AS d_" + column;
			update_set +=
			    column + " = COALESCE(" + column + ", 0) + COALESCE((SELECT d_" + column + " FROM _ivm_delta), 0)";
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
			string denom = is_pop ? "NULLIF(" + count_col + ", 0)" : "NULLIF(" + count_col + " - 1, 0)";
			string var_expr =
			    "((" + sum_sq_col + ") - (" + sum_col + ") * (" + sum_col + ") / (" + count_col + ")) / " + denom;
			string formula = do_sqrt ? "sqrt(" + var_expr + ")" : var_expr;
			result += "UPDATE " + data_table + " SET " + alias + " = " + formula + ";\n";
		} else {
			// AVG: recompute from sum/count
			result += "UPDATE " + data_table + " SET " + alias + " = " + sum_col + "::DOUBLE / NULLIF(" + count_col +
			          ", 0);\n";
		}
	}

	return result;
}

string CompileProjectionsFilters(const string &view_name, const vector<string> &column_names,
                                 const string &delta_ts_filter, const string &catalog_prefix, bool insert_only) {
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));
	string mul = string(ivm::MULTIPLICITY_COL);
	string delta_view = catalog_prefix + Q(OpenIVMUtils::DeltaName(view_name));
	string ts_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	string select_columns;
	string match_conditions;
	for (auto &raw_col : column_names) {
		if (raw_col != mul) {
			string column = Q(raw_col);
			match_conditions += "v." + column + " IS NOT DISTINCT FROM d." + column + " AND ";
			select_columns += column + ", ";
		}
	}
	if (select_columns.empty()) {
		throw InvalidInputException("Cannot compile projection refresh for materialized view '%s': delta view '%s' has no "
		                            "user-visible columns",
		                            view_name, OpenIVMUtils::DeltaName(view_name));
	}
	match_conditions.erase(match_conditions.size() - 5, 5);
	select_columns.erase(select_columns.size() - 2, 2);

	if (insert_only) {
		// Insert-only fast path: all deltas are inserts (w > 0), just INSERT directly.
		// Each row replicated |w| times for bag-correct multiplicity.
		string mul_filter = delta_ts_filter.empty() ? "WHERE " + mul + " > 0" : ts_where + " AND " + mul + " > 0";
		string insert_query = "INSERT INTO " + data_table + " SELECT " + select_columns + "\nFROM " + delta_view +
		                      ", generate_series(1, " + mul + "::BIGINT)\n" + mul_filter + ";\n";
		return insert_query;
	}

	// Consolidate deltas into net changes per distinct tuple (1 pass over delta_view).
	// _net > 0 = net insertions, _net < 0 = net deletions. With integer weights this is
	// just SUM(weight) — no CASE round-trip needed.
	string cte_body = "SELECT " + select_columns + ",\n    SUM(" + mul + ") AS _net\n  FROM " + delta_view + ts_where +
	                  "\n  GROUP BY " + select_columns + "\n  HAVING SUM(" + mul + ") != 0";

	// DELETE: remove exactly |_net| copies per tuple using rowid + ROW_NUMBER.
	string delete_query = "WITH _ivm_net AS (\n  " + cte_body +
	                      "\n)\n"
	                      "DELETE FROM " +
	                      data_table +
	                      " WHERE rowid IN (\n"
	                      "  SELECT v.rowid FROM (\n"
	                      "    SELECT rowid, " +
	                      select_columns +
	                      ",\n"
	                      "      ROW_NUMBER() OVER (PARTITION BY " +
	                      select_columns +
	                      " ORDER BY rowid) AS _rn\n"
	                      "    FROM " +
	                      data_table +
	                      "\n"
	                      "  ) v JOIN _ivm_net d ON " +
	                      match_conditions +
	                      "\n"
	                      "  WHERE d._net < 0 AND v._rn <= -d._net\n"
	                      ");\n\n";

	// INSERT: replicate each net-insert tuple _net times using generate_series.
	string insert_query = "WITH _ivm_net AS (\n  " + cte_body +
	                      "\n)\n"
	                      "INSERT INTO " +
	                      data_table + " SELECT " + select_columns +
	                      "\nFROM _ivm_net, generate_series(1, _ivm_net._net::BIGINT)\nWHERE _ivm_net._net > 0;\n";

	return delete_query + insert_query;
}

string CompileFullRecompute(const string &view_name, const string &view_query_sql, const string &catalog_prefix) {
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));
	return "DELETE FROM " + data_table + ";\n" + "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
}

namespace {

// Replace every plain occurrence of `needle` in `haystack` with `replacement`.
// LPTS-form view queries reference base tables via fully-qualified `cat.schema.tbl` triplets,
// so the needle is unambiguous in practice (the embedded dots prevent it from matching inside
// any identifier or alias).
static string ReplaceAllOccurrences(string haystack, const string &needle, const string &replacement) {
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

static bool IsIdentifierChar(char c) {
	return std::isalnum(static_cast<unsigned char>(c)) || c == '_' || c == '.';
}

static bool IdentifierMatchesTable(const string &identifier, const string &table_name) {
	if (StringUtil::CIEquals(identifier, table_name)) {
		return true;
	}
	if (identifier.size() <= table_name.size()) {
		return false;
	}
	auto suffix = identifier.substr(identifier.size() - table_name.size());
	return identifier[identifier.size() - table_name.size() - 1] == '.' && StringUtil::CIEquals(suffix, table_name);
}

static string ReplaceTableReferences(const string &sql, const string &table_name, const string &replacement) {
	if (table_name.empty()) {
		return sql;
	}
	string result;
	result.reserve(sql.size());
	bool in_single_quote = false;
	bool expect_table = false;
	for (idx_t i = 0; i < sql.size();) {
		char c = sql[i];
		if (c == '\'') {
			result += c;
			i++;
			if (in_single_quote && i < sql.size() && sql[i] == '\'') {
				result += sql[i++];
				continue;
			}
			in_single_quote = !in_single_quote;
			continue;
		}
		if (!in_single_quote && (std::isalpha(static_cast<unsigned char>(c)) || c == '_' || c == '"')) {
			idx_t start = i;
			bool in_identifier_quote = c == '"';
			i++;
			while (i < sql.size()) {
				char nc = sql[i];
				if (in_identifier_quote) {
					i++;
					if (nc == '"') {
						break;
					}
					continue;
				}
				if (!IsIdentifierChar(nc)) {
					break;
				}
				i++;
			}
			string token = sql.substr(start, i - start);
			string unquoted = token;
			if (unquoted.size() >= 2 && unquoted.front() == '"' && unquoted.back() == '"') {
				unquoted = unquoted.substr(1, unquoted.size() - 2);
			}
			if (expect_table && IdentifierMatchesTable(unquoted, table_name)) {
				result += replacement;
				expect_table = false;
				continue;
			}
			result += token;
			string lower = StringUtil::Lower(unquoted);
			expect_table = lower == "from" || lower == "join" || lower == "update" || lower == "into";
			continue;
		}
		result += c;
		if (!std::isspace(static_cast<unsigned char>(c))) {
			expect_table = false;
		}
		i++;
	}
	return result;
}

} // namespace

string CompileGroupRecompute(const string &view_name, const string &view_query_sql, const vector<string> &group_columns,
                             const vector<GroupRecomputeDeltaSpec> &delta_table_specs, const string &catalog_prefix,
                             const string &lpts_table_prefix) {
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));

	// No GROUP BY columns or no source deltas registered → can't scope; fall back to full.
	if (group_columns.empty() || delta_table_specs.empty()) {
		return CompileFullRecompute(view_name, view_query_sql, catalog_prefix);
	}

	string group_csv = JoinQuotedColumns(group_columns);

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
			    "(SELECT * FROM ducklake_table_insertions('" + OpenIVMUtils::EscapeValue(spec.ducklake_catalog) +
			    "', '" + OpenIVMUtils::EscapeValue(spec.ducklake_schema) + "', '" + OpenIVMUtils::EscapeValue(base) +
			    "', " + to_string(spec.last_snapshot_id) + ", " + to_string(spec.current_snapshot_id) +
			    ")\nUNION ALL\nSELECT * FROM ducklake_table_deletions('" +
			    OpenIVMUtils::EscapeValue(spec.ducklake_catalog) + "', '" +
			    OpenIVMUtils::EscapeValue(spec.ducklake_schema) + "', '" + OpenIVMUtils::EscapeValue(base) + "', " +
			    to_string(spec.last_snapshot_id) + ", " + to_string(spec.current_snapshot_id) + "))";
		} else {
			string delta_basename = string(ivm::DELTA_PREFIX) + base;
			string delta_filter;
			if (!spec.last_update.empty()) {
				delta_filter = " WHERE " + string(ivm::TIMESTAMP_COL) + " >= '" +
				               OpenIVMUtils::EscapeValue(spec.last_update) + "'::TIMESTAMP";
			}
			delta_subselect = "(SELECT * EXCLUDE (" + string(ivm::MULTIPLICITY_COL) + ", " +
			                  string(ivm::TIMESTAMP_COL) + ") FROM " + catalog_prefix + Q(delta_basename) +
			                  delta_filter + ")";
		}

		// LPTS form ALWAYS references base tables as fully-qualified `cat.schema.tbl`, even when
		// the catalog is the default `memory` (so `catalog_prefix` is empty for SQL output, but
		// `lpts_table_prefix` is still `memory.main.` here). Substitute that exact form.
		// Original-SQL recompute paths (e.g. ROLLUP/GROUPING SETS) can contain unqualified
		// table names, so fall back to identifier-safe bare replacement when the exact LPTS
		// form is absent.
		string source_full = (lpts_table_prefix.empty() ? catalog_prefix : lpts_table_prefix) + base;
		string filtered = ReplaceAllOccurrences(view_query_sql, source_full, delta_subselect);
		if (filtered == view_query_sql) {
			filtered = ReplaceTableReferences(view_query_sql, base, delta_subselect);
		}

		if (i > 0) {
			affected_subquery += "\n  UNION\n  ";
		} else {
			affected_subquery += "  ";
		}
		affected_subquery += "SELECT DISTINCT " + group_csv + " FROM (" + filtered + ") _ivm_src_" + to_string(i);
	}

	// EXISTS-based, NULL-safe match (IS NOT DISTINCT FROM). DuckDB inlines the subquery into both
	// the DELETE and the INSERT — the affected-keys set is computed once per usage in practice but
	// the planner can fuse them; if profiling shows this is hot, materialize to a TEMP TABLE first.
	string match_clause = BuildNullSafeMatch(group_columns, "_ivm_aff", "_ivm_tgt");
	string aff_block = "(\n" + affected_subquery + "\n)";

	string delete_query = "DELETE FROM " + data_table + " AS _ivm_tgt\nWHERE EXISTS (\n  SELECT 1 FROM " + aff_block +
	                      " AS _ivm_aff WHERE " + match_clause + "\n);\n";
	string insert_query = "INSERT INTO " + data_table + "\nSELECT * FROM (" + view_query_sql +
	                      ") AS _ivm_tgt\nWHERE EXISTS (\n  SELECT 1 FROM " + aff_block + " AS _ivm_aff WHERE " +
	                      match_clause + "\n);\n";

	OPENIVM_DEBUG_PRINT("[CompileGroupRecompute] %zu group cols, %zu source deltas\n", group_columns.size(),
	                    delta_table_specs.size());
	return delete_query + "\n" + insert_query;
}

string CompileDistinctIncremental(const string &view_name, const string &aux_table, const vector<string> &distinct_cols,
                                  const string &delta_source, const string &last_update, const string &filter_sql,
                                  const vector<string> &group_columns, const string &sum_arg, const string &sum_out,
                                  const string &count_star_col, const string &catalog_prefix) {
	if (distinct_cols.empty() || group_columns.empty() || sum_arg.empty() || sum_out.empty()) {
		// Caller should have demoted; defensive — return a no-op refresh.
		return "-- CompileDistinctIncremental called with incomplete metadata; no-op\n";
	}
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + Q(aux_table);
	string delta_q = catalog_prefix + Q(delta_source);
	string sum_arg_q = Q(sum_arg);
	string sum_out_q = Q(sum_out);
	string count_q = Q(count_star_col);

	string distinct_cols_csv = JoinQuotedColumns(distinct_cols);
	string distinct_cols_csv_i = JoinQualifiedQuotedColumns(distinct_cols, "i");
	string group_cols_csv = JoinQuotedColumns(group_columns);
	string mv_match = BuildNullSafeMatch(group_columns, "v", "d");

	// Δinput temp table — `_ivm_dinput_<view>`. Each row is a distinct tuple plus its net
	// multiplicity change since last refresh. CREATE OR REPLACE so the previous refresh's
	// leftover (if any rolled back) is wiped first.
	string dinput_table = "_ivm_dinput_" + view_name;
	string ts_filter =
	    " WHERE " + string(ivm::TIMESTAMP_COL) + " >= '" + OpenIVMUtils::EscapeValue(last_update) + "'::TIMESTAMP";
	string filter_clause = filter_sql.empty() ? "" : " AND (" + filter_sql + ")";

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + Q(dinput_table) + " AS\n  SELECT " + distinct_cols_csv + ", SUM(" +
	       string(ivm::MULTIPLICITY_COL) + ")::BIGINT AS dmult\n  FROM " + delta_q + ts_filter + filter_clause +
	       "\n  GROUP BY " + distinct_cols_csv + "\n  HAVING SUM(" + string(ivm::MULTIPLICITY_COL) + ") <> 0;\n\n";

	// Δdistinct + Δagg + MERGE into MV.
	// Z-set distinct(R)[t] = sgn(R[t]); Δdistinct[t] = sgn(R+ΔR) − sgn(R), which collapses to
	//   +1 if old==0 and dmult>0      (tuple newly enters the distinct set)
	//   −1 if old>0 and old+dmult≤0   (tuple leaves the distinct set)
	//    0 otherwise                  (count changed but didn't cross the existence boundary)
	// We multiply this ±1 by sum_arg to derive the parent-aggregate delta per (group_cols).
	// Use `_aux` (not `c`) as the LEFT-JOIN alias so it can't collide with a distinct
	// column literally called `c` (which is a normal column name in user schemas).
	string aux_match_aliased = BuildNullSafeMatch(distinct_cols, "_aux", "i");
	string ddist_cte =
	    "WITH ddist AS (\n  SELECT " + distinct_cols_csv_i +
	    ", CASE WHEN COALESCE(_aux._count, 0) = 0 AND i.dmult > 0 THEN 1 "
	    "WHEN COALESCE(_aux._count, 0) > 0 AND COALESCE(_aux._count, 0) + i.dmult <= 0 THEN -1 ELSE 0 END AS dd\n"
	    "  FROM " +
	    Q(dinput_table) + " i LEFT JOIN " + aux_q + " _aux ON " + aux_match_aliased + "\n),\ndagg AS (\n  SELECT " +
	    group_cols_csv + ", SUM(" + sum_arg_q +
	    " * dd) AS d_sum, SUM(dd)::BIGINT AS d_count\n  FROM ddist WHERE dd <> 0\n  GROUP BY " + group_cols_csv +
	    "\n)\n";

	// Build INSERT column list (group cols + sum_out + count_star_col).
	string insert_cols = group_cols_csv + ", " + sum_out_q + ", " + count_q;
	string insert_vals;
	for (size_t i = 0; i < group_columns.size(); i++) {
		if (i > 0) {
			insert_vals += ", ";
		}
		insert_vals += "d." + Q(group_columns[i]);
	}
	insert_vals += ", d.d_sum, d.d_count";

	sql += ddist_cte + "MERGE INTO " + data_table + " v USING dagg d ON " + mv_match +
	       "\nWHEN MATCHED THEN UPDATE SET " + sum_out_q + " = COALESCE(v." + sum_out_q + ", 0) + d.d_sum, " + count_q +
	       " = v." + count_q + " + d.d_count\nWHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (" +
	       insert_vals + ");\n\n";

	// Cull groups whose count fell to zero. The visible value column is left alone — the
	// row is being deleted anyway. Same convention as CompileAggregateGroups' cleanup.
	sql += "DELETE FROM " + data_table + " WHERE " + count_q + " <= 0;\n\n";

	// Update the aux table. WHEN NOT MATCHED is gated on `dmult > 0` so deletes that arrive
	// for a tuple we never saw don't accidentally insert a negative-count row.
	// Alias `_aux` (not `c`) for the same column-name-collision reason as the ddist CTE.
	sql += "MERGE INTO " + aux_q + " _aux USING " + Q(dinput_table) + " i ON " + aux_match_aliased +
	       "\nWHEN MATCHED THEN UPDATE SET _count = _aux._count + i.dmult\nWHEN NOT MATCHED AND i.dmult > 0 "
	       "THEN INSERT (" +
	       distinct_cols_csv + ", _count) VALUES (";
	for (size_t i = 0; i < distinct_cols.size(); i++) {
		if (i > 0) {
			sql += ", ";
		}
		sql += "i." + Q(distinct_cols[i]);
	}
	sql += ", i.dmult);\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE _count <= 0;\n\n";

	// Drop the temp table to keep the per-refresh footprint tidy. Subsequent refreshes
	// recreate it; CREATE OR REPLACE above handles failures mid-batch too.
	sql += "DROP TABLE IF EXISTS " + Q(dinput_table) + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileDistinctIncremental] %zu distinct cols, %zu group cols, sum %s(%s)→%s, aux=%s\n",
	                    distinct_cols.size(), group_columns.size(), "SUM", sum_arg.c_str(), sum_out.c_str(),
	                    aux_table.c_str());
	return sql;
}

string CompileSemiAntiRecompute(const string &view_name, const string &aux_table, const string &join_type,
                                const string &left_table, const string &left_alias, const string &right_table,
                                const string &right_alias, const string &predicate, const string &post_filter,
                                const vector<string> &left_cols, const vector<string> &output_cols,
                                const string &left_delta_source, const string &right_delta_source,
                                const string &left_last_update, const string &right_last_update,
                                const string &catalog_prefix) {
	if (left_cols.empty() || output_cols.empty() || aux_table.empty() || left_delta_source.empty() ||
	    right_delta_source.empty() || left_last_update.empty() || right_last_update.empty()) {
		return "-- CompileSemiAntiRecompute called with incomplete metadata; no-op\n";
	}

	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));
	string aux_q = catalog_prefix + Q(aux_table);
	string left_delta_q = catalog_prefix + Q(left_delta_source);
	string right_delta_q = catalog_prefix + Q(right_delta_source);
	string dleft_table = "_ivm_saj_dleft_" + view_name;
	string dright_table = "_ivm_saj_dright_" + view_name;
	string old_table = "_ivm_saj_old_" + view_name;
	string aff_table = "_ivm_saj_aff_" + view_name;
	bool is_anti = StringUtil::Lower(join_type) == "anti";
	string visible = is_anti ? "_match_count = 0" : "_match_count > 0";
	string cur_visible = is_anti ? "_cur._match_count = 0" : "_cur._match_count > 0";
	string old_filter;
	string cur_filter;
	if (!post_filter.empty()) {
		old_filter = " AND (" + ReplaceAllOccurrences(post_filter, left_alias + ".", "_old.") + ")";
		cur_filter = " AND (" + ReplaceAllOccurrences(post_filter, left_alias + ".", "_cur.") + ")";
	}

	string left_cols_csv = JoinQuotedColumns(left_cols);
	string output_cols_csv = JoinQuotedColumns(output_cols);
	string left_cols_i = JoinQualifiedQuotedColumns(left_cols, "i");
	string left_cols_l = JoinQualifiedQuotedColumns(left_cols, left_alias);
	string left_cols_old = JoinQualifiedQuotedColumns(left_cols, "_old");
	string left_cols_cur = JoinQualifiedQuotedColumns(left_cols, "_cur");
	string output_old = JoinQualifiedQuotedColumns(output_cols, "_old");
	string output_cur = JoinQualifiedQuotedColumns(output_cols, "_cur");

	string aux_i_match = BuildNullSafeMatch(left_cols, "_aux", "i");
	string old_cur_match = BuildNullSafeMatch(left_cols, "_old", "_cur");
	string aff_old_match = BuildNullSafeMatch(left_cols, "_aff", "_old");
	string aff_cur_match = BuildNullSafeMatch(left_cols, "_aff", "_cur");
	string data_match = BuildNullSafeMatch(output_cols, "_v", "_d");

	string left_ts =
	    string(ivm::TIMESTAMP_COL) + " >= '" + OpenIVMUtils::EscapeValue(left_last_update) + "'::TIMESTAMP";
	string right_ts = right_alias + "." + string(ivm::TIMESTAMP_COL) + " >= '" +
	                  OpenIVMUtils::EscapeValue(right_last_update) + "'::TIMESTAMP";

	string sql;
	sql += "CREATE OR REPLACE TEMP TABLE " + Q(old_table) + " AS SELECT *, (" + visible + ") AS _visible FROM " +
	       aux_q + ";\n\n";

	sql += "CREATE OR REPLACE TEMP TABLE " + Q(dleft_table) + " AS\n  SELECT " + left_cols_csv + ", SUM(" +
	       string(ivm::MULTIPLICITY_COL) + ")::BIGINT AS dmult\n  FROM " + left_delta_q + "\n  WHERE " + left_ts +
	       "\n  GROUP BY " + left_cols_csv + "\n  HAVING SUM(" + string(ivm::MULTIPLICITY_COL) + ") <> 0;\n\n";

	sql += "CREATE OR REPLACE TEMP TABLE " + Q(dright_table) + " AS\n  SELECT " + left_cols_l + ", SUM(" + right_alias +
	       "." + string(ivm::MULTIPLICITY_COL) + ")::BIGINT AS dmatch\n  FROM " + aux_q + " " + left_alias + " JOIN " +
	       right_delta_q + " " + right_alias + " ON " + predicate + "\n  WHERE " + right_ts + "\n  GROUP BY " +
	       left_cols_l + "\n  HAVING SUM(" + right_alias + "." + string(ivm::MULTIPLICITY_COL) + ") <> 0;\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + Q(dright_table) + " _d ON " +
	       BuildNullSafeMatch(left_cols, "_aux", "_d") +
	       "\nWHEN MATCHED THEN UPDATE SET _match_count = _aux._match_count + _d.dmatch;\n\n";

	sql += "MERGE INTO " + aux_q + " _aux USING " + Q(dleft_table) + " i ON " + aux_i_match +
	       "\nWHEN MATCHED THEN UPDATE SET _left_count = _aux._left_count + i.dmult;\n\n";

	sql += "INSERT INTO " + aux_q + " (" + left_cols_csv + ", _left_count, _match_count)\nSELECT " + left_cols_i +
	       ", i.dmult, COALESCE(mc._match_count, 0)::BIGINT\nFROM " + Q(dleft_table) + " i\nLEFT JOIN " + aux_q +
	       " _aux ON " + aux_i_match + "\nLEFT JOIN (\n  SELECT " + left_cols_l +
	       ", COUNT(*)::BIGINT AS _match_count\n  FROM " + Q(dleft_table) + " " + left_alias + " JOIN " + right_table +
	       " " + right_alias + " ON " + predicate + "\n  GROUP BY " + left_cols_l + "\n) mc ON " +
	       BuildNullSafeMatch(left_cols, "mc", "i") + "\nWHERE _aux._left_count IS NULL AND i.dmult > 0;\n\n";

	sql += "CREATE OR REPLACE TEMP TABLE " + Q(aff_table) + " AS\nSELECT " + left_cols_old + " FROM " + Q(old_table) +
	       " _old LEFT JOIN " + aux_q + " _cur ON " + old_cur_match +
	       "\nWHERE _cur._left_count IS NULL OR _old._left_count IS DISTINCT FROM _cur._left_count OR "
	       "_old._visible IS DISTINCT FROM (" +
	       cur_visible + ")\nUNION\nSELECT " + left_cols_cur + " FROM " + aux_q + " _cur LEFT JOIN " + Q(old_table) +
	       " _old ON " + old_cur_match +
	       "\nWHERE _old._left_count IS NULL OR _old._left_count IS DISTINCT FROM _cur._left_count OR "
	       "_old._visible IS DISTINCT FROM (" +
	       cur_visible + ");\n\n";

	sql += "WITH _old_rows AS (\n  SELECT " + output_old + " FROM " + Q(old_table) + " _old JOIN " + Q(aff_table) +
	       " _aff ON " + aff_old_match +
	       ", generate_series(1, _old._left_count::BIGINT)\n  WHERE _old._visible AND _old._left_count > 0" +
	       old_filter +
	       "\n), "
	       "_net AS (\n  SELECT " +
	       output_cols_csv + ", COUNT(*)::BIGINT AS _cnt FROM _old_rows GROUP BY " + output_cols_csv +
	       "\n)\nDELETE FROM " + data_table + " WHERE rowid IN (\n  SELECT _v.rowid FROM (\n    SELECT rowid, " +
	       output_cols_csv + ", ROW_NUMBER() OVER (PARTITION BY " + output_cols_csv + " ORDER BY rowid) AS _rn FROM " +
	       data_table + "\n  ) _v JOIN _net _d ON " + data_match + " WHERE _v._rn <= _d._cnt\n);\n\n";

	sql += "INSERT INTO " + data_table + " SELECT " + output_cur + "\nFROM " + aux_q + " _cur JOIN " + Q(aff_table) +
	       " _aff ON " + aff_cur_match + ", generate_series(1, _cur._left_count::BIGINT)\nWHERE " + cur_visible +
	       " AND _cur._left_count > 0" + cur_filter + ";\n\n";

	sql += "DELETE FROM " + aux_q + " WHERE _left_count <= 0;\n";
	sql += "DROP TABLE IF EXISTS " + Q(old_table) + ";\nDROP TABLE IF EXISTS " + Q(dleft_table) +
	       ";\nDROP TABLE IF EXISTS " + Q(dright_table) + ";\nDROP TABLE IF EXISTS " + Q(aff_table) + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileSemiAntiRecompute] %s join, %zu left cols, aux=%s\n", join_type.c_str(),
	                    left_cols.size(), aux_table.c_str());
	return sql;
}

string CompileWindowRecompute(const string &view_name, const string &view_query_sql, const string &delta_ts_filter,
                              const string &catalog_prefix, const vector<string> &partition_columns,
                              const vector<string> &delta_table_names) {
	if (partition_columns.empty()) {
		return CompileFullRecompute(view_name, view_query_sql, catalog_prefix);
	}
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	// Single-table window views: identify affected partitions from the base delta table.
	// OR-based per-column filtering handles multiple PARTITION BY columns from different
	// OVER clauses — a change in dept affects ALL rows in that dept regardless of team.
	// (Window+join views have empty partition_columns → caught by full recompute above.)
	string affected_filter;
	for (size_t i = 0; i < partition_columns.size(); i++) {
		if (i > 0) {
			affected_filter += " OR ";
		}
		string col = Q(partition_columns[i]);
		affected_filter += col + " IN (SELECT DISTINCT " + col + " FROM " + Q(delta_table_names[0]) + delta_where + ")";
	}

	string delete_query = "DELETE FROM " + data_table + " WHERE " + affected_filter + ";\n";
	string insert_query = "INSERT INTO " + data_table + "\n" + "SELECT * FROM (" + view_query_sql +
	                      ") _ivm_recompute\n" + "WHERE " + affected_filter + ";\n";

	OPENIVM_DEBUG_PRINT("[CompileWindowRecompute] Partition columns: %zu\n", partition_columns.size());
	return delete_query + "\n" + insert_query;
}

} // namespace duckdb
