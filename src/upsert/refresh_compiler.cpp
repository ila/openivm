#include "upsert/refresh_compiler.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_internal.hpp"

namespace duckdb {

// Zero-initialized 64-element float list, used as COALESCE default for NULL list aggregates.
static constexpr const char *ZEROS_LIST = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";

static string BuildUpdatedAggregateColumn(const string &col) {
	return "COALESCE(v." + col + " + d." + col + ", v." + col + ", d." + col + ")";
}

static string BuildNullSafeExtremumUpdate(const string &col, const string &fn) {
	return "CASE WHEN v." + col + " IS NULL THEN d." + col + " WHEN d." + col + " IS NULL THEN v." + col + " ELSE " +
	       fn + "(v." + col + ", d." + col + ") END";
}

static string JoinPrefixedSQLFragments(const vector<string> &fragments, const string &prefix) {
	return StringUtil::Join(fragments, fragments.size(), ", ",
	                        [&](const string &fragment) { return prefix + fragment; });
}

static string BuildNullSafeSQLFragmentMatch(const vector<string> &columns, const string &left_prefix,
                                            const string &right_prefix) {
	return StringUtil::Join(columns, columns.size(), " AND ", [&](const string &column) {
		return left_prefix + column + " IS NOT DISTINCT FROM " + right_prefix + column;
	});
}

/// Detect AVG and STDDEV/VARIANCE decomposition columns from the column list.
/// AVG(x) is stored as openivm_sum_<alias>, openivm_count_<alias>, and <alias>.
/// STDDEV/VARIANCE(x) adds a sum-of-squares column with a prefix encoding the function type:
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

/// Try to match a column against a prefix. Returns the alias suffix if matched, empty string otherwise.
static string MatchPrefix(const string &col, const string &prefix) {
	if (col.size() > prefix.size() && col.substr(0, prefix.size()) == prefix) {
		return col.substr(prefix.size());
	}
	return "";
}

static DerivedAggDecomposition DetectDerivedAggColumns(const vector<string> &columns) {
	DerivedAggDecomposition result;
	static const string sum_prefix(openivm::SUM_COL_PREFIX);
	static const string count_prefix(openivm::COUNT_COL_PREFIX);
	// Sum-of-squares prefixes (check longer ones first to avoid false matches)
	static const string sum_sqp_prefix(openivm::SUM_SQP_COL_PREFIX); // stddev_pop
	static const string var_sqp_prefix(openivm::VAR_SQP_COL_PREFIX); // var_pop
	static const string sum_sq_prefix(openivm::SUM_SQ_COL_PREFIX);   // stddev_samp
	static const string var_sq_prefix(openivm::VAR_SQ_COL_PREFIX);   // var_samp

	for (auto &col : columns) {
		// Check sum-of-squares prefixes BEFORE sum (they start with openivm_sum_ or openivm_var_)
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
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	string delta_view = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));

	// Extract GROUP BY keys: from index (standard path) or from metadata (DuckLake fallback)
	vector<string> keys;
	if (index_delta_view_catalog_entry) {
		auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
		auto key_ids = index_catalog_entry->column_ids;
		for (size_t i = 0; i < key_ids.size(); i++) {
			keys.emplace_back(SqlUtils::QuoteIdentifier(column_names[key_ids[i]]));
		}
	} else {
		// No index (e.g. DuckLake) — use group column names from metadata
		for (auto &col : group_column_names) {
			keys.emplace_back(SqlUtils::QuoteIdentifier(col));
		}
	}

	unordered_set<std::string> keys_set(keys.begin(), keys.end());
	vector<string> aggregates;
	// Parallel: true if the matching aggregate column has a non-summable delta type
	// (VARCHAR CASE output, 'GC' literal, UPPER(group_col), LIST(col), etc.).
	bool has_non_summable_col = false;
	for (size_t i = 0; i < column_names.size(); i++) {
		const auto &column = column_names[i];
		if (keys_set.find(SqlUtils::QuoteIdentifier(column)) == keys_set.end() &&
		    column != string(openivm::MULTIPLICITY_COL)) {
			aggregates.push_back(SqlUtils::QuoteIdentifier(column));
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
		// aggregate columns (INCLUDING hidden helpers like openivm_match_count and
		// openivm_*) means at least one aggregate is consumed inside a computed
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
				if (decomp.derived_cols.count(column) || column.find("openivm_") != string::npos) {
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
			if (decomp.derived_cols.count(column) || column.find("openivm_") != string::npos) {
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
		string keys_tuple = StringUtil::Join(keys, ", ");
		string match_delete = BuildNullSafeSQLFragmentMatch(keys, "openivm_aff.", "openivm_tgt.");
		string match_insert = BuildNullSafeSQLFragmentMatch(keys, "openivm_aff.", "openivm_recompute.");
		string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
		string affected = "select distinct " + keys_tuple + " from " + delta_view + delta_where;
		return BuildAffectedKeyRefreshSQL(data_table, view_query_sql, "  " + affected, "openivm_tgt",
		                                  "openivm_recompute", "openivm_aff", match_delete, match_insert);
	}

	// CTE: consolidate deltas per group
	string cte_select_string = "select ";
	cte_select_string += StringUtil::Join(keys, ", ") + ", ";
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
			    "\n\tlist_reduce(list(list_transform(" + column + ", lambda x: " + string(openivm::MULTIPLICITY_COL) +
			    " * x)), lambda a, b: list_transform(list_zip(a, b), lambda x: x[1] + x[2])) AS " + column + ", ";
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
	string cte_group_by_string = "group by " + StringUtil::Join(keys, ", ");

	string cte_body = cte_select_string + cte_from_string + cte_group_by_string;

	// MERGE: single-pass upsert — UPDATE existing groups, INSERT new groups.
	// Uses IS NOT DISTINCT FROM for NULL-safe key matching.
	string on_clause = BuildNullSafeSQLFragmentMatch(keys, "v.", "d.");

	auto &derived_cols = decomp.derived_cols;
	auto &d_sum_cols = decomp.sum_cols;
	auto &d_sum_sq_cols = decomp.sum_sq_cols;
	auto &d_count_cols = decomp.count_cols;

	// Detect openivm_match_count for LEFT JOIN incremental MERGE (Larson & Zhou).
	// When present, use COALESCE(v.col, 0) for aggregate updates to handle NULL→value transitions.
	string match_count_col;
	bool has_match_count = false;
	for (auto &col : aggregates) {
		if (col == SqlUtils::QuoteIdentifier(string(openivm::MATCH_COUNT_COL))) {
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
		insert_cols = StringUtil::Join(keys, ", ") + ", " + StringUtil::Join(aggregates, ", ");
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
			// (hidden openivm_count_* columns from AVG/STDDEV decomposition don't have agg_type entries).
			bool is_count = (agg_type == "count" || col.find(string(openivm::COUNT_COL_PREFIX)) == 0);
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
			bool is_count = (agg_type == "count" || aggregates[i].find(string(openivm::COUNT_COL_PREFIX)) == 0);
			string null_val = is_count ? "0" : "NULL";
			cond_insert_vals +=
			    "CASE WHEN d." + match_count_col + " > 0 THEN d." + aggregates[i] + " ELSE " + null_val + " END";
		}
		merge_query = "WITH refresh_cte AS (\n" + cte_body + ")\n" + "MERGE INTO " + data_table +
		              " v USING refresh_cte d\n" + "ON " + on_clause + "\n" + "WHEN MATCHED THEN UPDATE SET " +
		              lj_update_set + "\n" + "WHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (";
		merge_query += JoinPrefixedSQLFragments(keys, "d.") + ", " + cond_insert_vals + ");\n";
	} else {
		merge_query = "WITH refresh_cte AS (\n" + cte_body + ")\n" + "MERGE INTO " + data_table +
		              " v USING refresh_cte d\n" + "ON " + on_clause + "\n" + "WHEN MATCHED THEN UPDATE SET " +
		              update_set + "\n" + "WHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (";
		merge_query += JoinPrefixedSQLFragments(keys, "d.") + ", " + insert_vals + ");\n";
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
	// openivm_match_count is present (LEFT JOIN preserved-side NULL aggregates are valid).
	if (!insert_only && !has_match_count) {
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

	string delta_view = catalog_prefix + SqlUtils::QuoteIdentifier(SqlUtils::DeltaName(view_name));
	string mul = string(openivm::MULTIPLICITY_COL);
	string ts_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	auto decomp = DetectDerivedAggColumns(column_names);
	auto &d_derived = decomp.derived_cols;
	auto &d_sum = decomp.sum_cols;
	auto &d_sum_sq = decomp.sum_sq_cols;
	auto &d_count = decomp.count_cols;

	// Single CTE consolidates all delta columns in one pass.
	string cte = "WITH openivm_delta AS (\n  SELECT ";
	string update_set;
	bool first = true;
	string zeros_list = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";

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
			       " * x)), lambda a, b: list_transform(list_zip(a, b), lambda x: x[1] + x[2])), " + zeros_list +
			       ") AS d_" + column;
			update_set += column + " = list_transform(list_zip(" + column + ", (SELECT d_" + column +
			              " FROM openivm_delta)), lambda x: x[1] + x[2])";
		} else {
			// Z-set bag-aware sum: weight w∈ℤ scales the column value before SUM.
			cte += "SUM(" + mul + " * " + column + ") AS d_" + column;
			update_set +=
			    column + " = COALESCE(" + column + ", 0) + COALESCE((SELECT d_" + column + " FROM openivm_delta), 0)";
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
		throw InvalidInputException(
		    "Cannot compile projection refresh for materialized view '%s': delta view '%s' has no "
		    "user-visible columns",
		    view_name, SqlUtils::DeltaName(view_name));
	}
	match_conditions.erase(match_conditions.size() - 5, 5);
	select_columns.erase(select_columns.size() - 2, 2);
	delete_candidate_columns.erase(delete_candidate_columns.size() - 2, 2);

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
	string insert_query =
	    "WITH openivm_net AS (\n  " + cte_body +
	    "\n)\n"
	    "INSERT INTO " +
	    data_table + " SELECT " + select_columns +
	    "\nFROM openivm_net, generate_series(1, openivm_net._net::BIGINT)\nWHERE openivm_net._net > 0;\n";

	return delete_query + insert_query;
}

string CompileFullRecompute(const string &view_name, const string &view_query_sql, const string &catalog_prefix) {
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));
	return SqlUtils::BuildFullRecomputeSQL(data_table, view_query_sql);
}

string CompileGroupRecompute(const string &view_name, const string &view_query_sql, const vector<string> &group_columns,
                             const vector<GroupRecomputeDeltaSpec> &delta_table_specs, const string &catalog_prefix,
                             const string &lpts_table_prefix, bool emit_cascade_delta) {
	string data_table = catalog_prefix + SqlUtils::QuoteIdentifier(IncrementalTableNames::DataTableName(view_name));

	// No GROUP BY columns or no source deltas registered → can't scope; fall back to full.
	if (group_columns.empty() || delta_table_specs.empty()) {
		return CompileFullRecompute(view_name, view_query_sql, catalog_prefix);
	}

	string group_csv = SqlUtils::JoinQuotedColumns(group_columns);

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
			string delta_filter;
			if (!spec.last_update.empty()) {
				delta_filter = " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" +
				               SqlUtils::EscapeValue(spec.last_update) + "'::TIMESTAMP";
			}
			delta_subselect = "(SELECT * EXCLUDE (" + string(openivm::MULTIPLICITY_COL) + ", " +
			                  string(openivm::TIMESTAMP_COL) + ") FROM " + catalog_prefix +
			                  SqlUtils::QuoteIdentifier(delta_basename) + delta_filter + ")";
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
		auto filtered_variants = SqlUtils::ReplaceEachPlainOccurrence(view_query_sql, source_full, delta_subselect);
		if (filtered_variants.empty()) {
			filtered_variants = SqlUtils::ReplaceEachTableReference(view_query_sql, base, delta_subselect);
		}
		if (filtered_variants.empty()) {
			string filtered = SqlUtils::ReplaceAllOccurrences(view_query_sql, source_full, delta_subselect);
			if (filtered == view_query_sql) {
				filtered = SqlUtils::ReplaceTableReferences(view_query_sql, base, delta_subselect);
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

	OPENIVM_DEBUG_PRINT("[CompileGroupRecompute] %zu group cols, %zu source deltas, cascade delta: %s\n",
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
