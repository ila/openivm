#include "upsert/openivm_compile_upsert.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "rules/column_hider.hpp"

namespace duckdb {

// Zero-initialized 64-element float list, used as COALESCE default for NULL list aggregates.
static const string ZEROS_LIST = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";

/// Quote a column name for safe use in generated SQL. Handles special characters like ().
static string Q(const string &name) {
	return OpenIVMUtils::QuoteIdentifier(name);
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
		for (size_t i = 0; i < keys.size(); i++) {
			keys_tuple += keys[i];
			if (i != keys.size() - 1) {
				keys_tuple += ", ";
			}
		}
		string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
		string delete_query = "delete from " + data_table + " where (" + keys_tuple + ") in (\n" +
		                      "  select distinct " + keys_tuple + " from " + delta_view + delta_where + "\n);\n";
		string insert_query = "insert into " + data_table + "\n" + "select * from (" + view_query_sql +
		                      ") _ivm_recompute\n" + "where (" + keys_tuple + ") in (\n" + "  select distinct " +
		                      keys_tuple + " from " + delta_view + delta_where + "\n);\n";
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
			cte_select_string += "\n\tlist_reduce(list(CASE WHEN " + string(ivm::MULTIPLICITY_COL) +
			                     " = false "
			                     "THEN list_transform(" +
			                     column +
			                     ", lambda x: -x) "
			                     "ELSE " +
			                     column +
			                     " END), "
			                     "lambda a, b: list_transform(list_zip(a, b), lambda x: x[1] + x[2])) AS " +
			                     column + ", ";
		} else {
			cte_select_string = cte_select_string + "\n\tsum(case when " + string(ivm::MULTIPLICITY_COL) +
			                    " = false then -" + column + " else " + column + " end) as " + column + ", ";
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

	// Helper: generate SQL to compute updated SUM from MERGE (COALESCE handles NULLs)
	auto updated_col = [](const string &col) -> string {
		return "COALESCE(v." + col + " + d." + col + ", v." + col + ", d." + col + ")";
	};

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
					update_set += column + " = " + updated_col(column);
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
					string new_sum = updated_col(sum_col);
					string new_sq = updated_col(sum_sq_col);
					string new_n = updated_col(count_col);
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
					update_set +=
					    column + " = " + updated_col(sum_col) + "::DOUBLE / NULLIF(" + updated_col(count_col) + ", 0)";
					insert_vals += "d." + sum_col + "::DOUBLE / NULLIF(d." + count_col + ", 0)";
				}
			} else {
				// Regular aggregate column
				string agg_type = col_agg_type.count(column) ? col_agg_type[column] : "";
				if (insert_only && agg_type == "min") {
					update_set += column + " = LEAST(v." + column + ", d." + column + ")";
				} else if (insert_only && agg_type == "max") {
					update_set += column + " = GREATEST(v." + column + ", d." + column + ")";
				} else if (list_mode) {
					update_set += column + " = list_transform(list_zip(v." + column + ", d." + column +
					              "), lambda x: x[1] + x[2])";
				} else {
					update_set += column + " = " + updated_col(column);
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
				lj_update_set += col + " = " + updated_col(col);
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
			lj_update_set += col + " = CASE WHEN " + mc_new + " > 0 THEN " + updated_col(col) + " WHEN COALESCE(v." +
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

	// Delete zero rows — skip when insert_only (groups can't reach zero from inserts alone).
	// Also skip when _ivm_match_count is present: LEFT JOIN groups with match_count=0
	// are valid (unmatched preserved-side rows with NULL aggregates, not deleted groups).
	if (!insert_only && !has_match_count) {
		string delete_query = "\ndelete from " + data_table + " where ";
		for (auto &column : aggregates) {
			if (derived_cols.count(column)) {
				continue; // skip derived cols (avg, stddev) — checking sum/count is sufficient
			}
			if (list_mode) {
				delete_query += "list_reduce(" + column + ", lambda a, b: a + b) = 0.0 and ";
			} else {
				delete_query += "COALESCE(" + column + ", 0) = 0 and ";
			}
		}
		delete_query.erase(delete_query.size() - 5, 5);
		delete_query += ";\n";
		upsert_query += delete_query;
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
			cte += "list_transform(list_zip("
			       "COALESCE(SUM(CASE WHEN " +
			       mul + " = false THEN list_transform(" + column + ", lambda x: -x) ELSE NULL END), " + zeros_list +
			       "), "
			       "COALESCE(SUM(CASE WHEN " +
			       mul + " = true THEN " + column + " ELSE NULL END), " + zeros_list +
			       ")), lambda x: x[1] + x[2]) AS d_" + column;
			update_set += column + " = list_transform(list_zip(" + column + ", (SELECT d_" + column +
			              " FROM _ivm_delta)), lambda x: x[1] + x[2])";
		} else {
			cte += "SUM(CASE WHEN " + mul + " = false THEN -" + column + " ELSE " + column + " END) AS d_" + column;
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
	match_conditions.erase(match_conditions.size() - 5, 5);
	select_columns.erase(select_columns.size() - 2, 2);

	if (insert_only) {
		// Insert-only fast path: all deltas are inserts, just INSERT directly.
		// No consolidation or DELETE needed.
		string mul_filter = delta_ts_filter.empty() ? "WHERE " + mul + " = true" : ts_where + " AND " + mul + " = true";
		string insert_query = "INSERT INTO " + data_table + " SELECT " + select_columns + "\nFROM " + delta_view +
		                      "\n" + mul_filter + ";\n";
		return insert_query;
	}

	// Consolidate deltas into net changes per distinct tuple (1 pass over delta_view).
	// _net > 0 = net insertions, _net < 0 = net deletions.
	string cte_body = "SELECT " + select_columns + ",\n    SUM(CASE WHEN " + mul +
	                  " THEN 1 ELSE -1 END) AS _net\n  FROM " + delta_view + ts_where + "\n  GROUP BY " +
	                  select_columns + "\n  HAVING SUM(CASE WHEN " + mul + " THEN 1 ELSE -1 END) != 0";

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

string CompileWindowRecompute(const string &view_name, const string &view_query_sql, const string &delta_ts_filter,
                              const string &catalog_prefix, const vector<string> &partition_columns,
                              const vector<string> &delta_table_names) {
	string data_table = catalog_prefix + Q(IVMTableNames::DataTableName(view_name));
	string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	if (partition_columns.empty()) {
		// No PARTITION BY → full recompute (same as FULL_REFRESH but through the IVM pipeline)
		return "DELETE FROM " + data_table + ";\n" + "INSERT INTO " + data_table + " " + view_query_sql + ";\n";
	}

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
