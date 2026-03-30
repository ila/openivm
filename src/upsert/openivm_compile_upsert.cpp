#include "upsert/openivm_compile_upsert.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_utils.hpp"

namespace duckdb {

// Zero-initialized 64-element float list, used as COALESCE default for NULL list aggregates.
static const string ZEROS_LIST = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";

/// Quote a column name for safe use in generated SQL. Handles special characters like ().
static string Q(const string &name) {
	return OpenIVMUtils::QuoteIdentifier(name);
}

/// Detect AVG decomposition columns from the column list.
/// AVG(x) is stored as _ivm_sum_<alias>, _ivm_count_<alias>, and <alias>.
/// Returns: derived_cols (the alias to skip in MERGE), sum_cols (alias→sum_name), count_cols (alias→count_name).
struct AvgDecomposition {
	unordered_set<string> derived_cols;
	unordered_map<string, string> sum_cols;
	unordered_map<string, string> count_cols;
};

static AvgDecomposition DetectAvgColumns(const vector<string> &columns) {
	AvgDecomposition result;
	for (auto &col : columns) {
		if (col.size() > 9 && col.substr(0, 9) == "_ivm_sum_") {
			result.sum_cols[col.substr(9)] = col;
		} else if (col.size() > 11 && col.substr(0, 11) == "_ivm_count_") {
			result.count_cols[col.substr(11)] = col;
		}
	}
	for (auto &[alias, sum_col] : result.sum_cols) {
		if (result.count_cols.count(alias)) {
			result.derived_cols.insert(alias);
		}
	}
	return result;
}

string CompileAggregateGroups(string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql, bool has_minmax,
                              bool list_mode, const string &delta_ts_filter) {
	auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
	auto key_ids = index_catalog_entry->column_ids;

	vector<string> keys;
	vector<string> aggregates;
	for (size_t i = 0; i < key_ids.size(); i++) {
		keys.emplace_back(Q(column_names[key_ids[i]]));
	}

	unordered_set<std::string> keys_set(keys.begin(), keys.end());

	for (auto &column : column_names) {
		if (keys_set.find(Q(column)) == keys_set.end() && column != string(ivm::MULTIPLICITY_COL)) {
			aggregates.push_back(Q(column));
		}
	}

	if (has_minmax) {
		// Group-recompute strategy for MIN/MAX: delete affected groups, re-insert from original query
		string keys_tuple;
		for (size_t i = 0; i < keys.size(); i++) {
			keys_tuple += keys[i];
			if (i != keys.size() - 1) {
				keys_tuple += ", ";
			}
		}
		string delta_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;
		string delete_query = "delete from " + view_name + " where (" + keys_tuple + ") in (\n" + "  select distinct " +
		                      keys_tuple + " from " + OpenIVMUtils::DeltaName(view_name) + delta_where + "\n);\n";
		string insert_query = "insert into " + view_name + "\n" + "select * from (" + view_query_sql +
		                      ") _ivm_recompute\n" + "where (" + keys_tuple + ") in (\n" + "  select distinct " +
		                      keys_tuple + " from " + OpenIVMUtils::DeltaName(view_name) + delta_where + "\n);\n";
		return delete_query + "\n" + insert_query;
	}

	// CTE: consolidate deltas per group
	string cte_string = "with ivm_cte AS (\n";
	string cte_select_string = "select ";
	for (auto &key : keys) {
		cte_select_string = cte_select_string + key + ", ";
	}
	for (auto &column : aggregates) {
		if (list_mode) {
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
	string cte_from_string = "from " + OpenIVMUtils::DeltaName(view_name);
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

	auto avg = DetectAvgColumns(aggregates);
	auto &avg_derived_cols = avg.derived_cols;
	auto &avg_sum_cols = avg.sum_cols;
	auto &avg_count_cols = avg.count_cols;

	string update_set;
	string insert_cols, insert_vals;
	{
		const string &zeros_list = ZEROS_LIST;
		bool first_agg = true;
		for (auto &column : aggregates) {
			// Skip AVG derived columns in MERGE — they'll be recomputed after
			if (avg_derived_cols.count(column)) {
				continue;
			}
			if (!first_agg) {
				update_set += ", ";
				insert_vals += ", ";
			}
			first_agg = false;
			if (list_mode) {
				update_set +=
				    column + " = list_transform(list_zip(v." + column + ", d." + column + "), lambda x: x[1] + x[2])";
			} else {
				update_set +=
				    column + " = COALESCE(v." + column + " + d." + column + ", v." + column + ", d." + column + ")";
			}
			insert_vals += "d." + column;
		}
		// Add AVG derived columns: recompute from updated SUM/COUNT in MERGE
		for (auto &[alias, sum_col] : avg_sum_cols) {
			if (!avg_count_cols.count(alias)) {
				continue;
			}
			string count_col = avg_count_cols[alias];
			if (!update_set.empty()) {
				update_set += ", ";
			}
			if (!insert_vals.empty()) {
				insert_vals += ", ";
			}
			// MATCHED: recompute avg from updated sum and count
			update_set += alias + " = COALESCE(v." + sum_col + " + d." + sum_col + ", v." + sum_col + ", d." + sum_col +
			              ")::DOUBLE / NULLIF(COALESCE(v." + count_col + " + d." + count_col + ", v." + count_col +
			              ", d." + count_col + "), 0)";
			// NOT MATCHED: compute avg from delta sum and count
			insert_vals += "d." + sum_col + "::DOUBLE / NULLIF(d." + count_col + ", 0)";
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

	string merge_query = "WITH ivm_cte AS (\n" + cte_body + ")\n" + "MERGE INTO " + view_name + " v USING ivm_cte d\n" +
	                     "ON " + on_clause + "\n" + "WHEN MATCHED THEN UPDATE SET " + update_set + "\n" +
	                     "WHEN NOT MATCHED THEN INSERT (" + insert_cols + ") VALUES (";
	for (auto &key : keys) {
		merge_query += "d." + key + ", ";
	}
	merge_query += insert_vals + ");\n";

	string upsert_query = merge_query + "\n";

	// Delete zero rows — skip AVG derived columns (check sum/count helpers instead)
	string delete_query = "\ndelete from " + view_name + " where ";
	for (auto &column : aggregates) {
		if (avg_derived_cols.count(column)) {
			continue; // skip avg_x — checking sum/count is sufficient
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

	return upsert_query;
}

string CompileSimpleAggregates(string &view_name, const vector<string> &column_names, const string &view_query_sql,
                               bool has_minmax, bool list_mode, const string &delta_ts_filter) {
	if (has_minmax) {
		string delete_query = "DELETE FROM " + view_name + ";\n";
		string insert_query = "INSERT INTO " + view_name + " " + view_query_sql + ";\n";
		return delete_query + insert_query;
	}

	string delta_view = OpenIVMUtils::DeltaName(view_name);
	string mul = string(ivm::MULTIPLICITY_COL);
	string ts_where = delta_ts_filter.empty() ? "" : " WHERE " + delta_ts_filter;

	auto avg = DetectAvgColumns(column_names);
	auto &avg_derived = avg.derived_cols;
	auto &avg_sum = avg.sum_cols;
	auto &avg_count = avg.count_cols;

	// Single CTE consolidates all delta columns in one pass.
	string cte = "WITH _ivm_delta AS (\n  SELECT ";
	string update_set;
	bool first = true;
	string zeros_list = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";

	for (auto &raw_col : column_names) {
		if (raw_col == mul || avg_derived.count(raw_col)) {
			continue; // skip multiplicity and AVG derived columns
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

	string result = cte + "UPDATE " + view_name + " SET\n  " + update_set + ";\n";

	// Recompute AVG derived columns from updated SUM and COUNT
	for (auto &[alias, sum_col] : avg_sum) {
		if (!avg_count.count(alias)) {
			continue;
		}
		string count_col = avg_count[alias];
		result +=
		    "UPDATE " + view_name + " SET " + alias + " = " + sum_col + "::DOUBLE / NULLIF(" + count_col + ", 0);\n";
	}

	return result;
}

string CompileProjectionsFilters(string &view_name, const vector<string> &column_names, const string &delta_ts_filter) {
	string mul = string(ivm::MULTIPLICITY_COL);
	string delta_view = OpenIVMUtils::DeltaName(view_name);
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

	// Consolidate deltas into net changes per distinct tuple (1 pass over delta_view).
	// _net > 0 = net insertions, _net < 0 = net deletions.
	string cte_body = "SELECT " + select_columns + ",\n    SUM(CASE WHEN " + mul +
	                  " THEN 1 ELSE -1 END) AS _net\n  FROM " + delta_view + ts_where + "\n  GROUP BY " +
	                  select_columns + "\n  HAVING SUM(CASE WHEN " + mul + " THEN 1 ELSE -1 END) != 0";

	// DELETE: remove exactly |_net| copies per tuple using rowid + ROW_NUMBER.
	// ROW_NUMBER partitions by all columns to number duplicate copies, then we
	// delete only the first |_net| of them.
	string delete_query = "WITH _ivm_net AS (\n  " + cte_body +
	                      "\n)\n"
	                      "DELETE FROM " +
	                      view_name +
	                      " WHERE rowid IN (\n"
	                      "  SELECT v.rowid FROM (\n"
	                      "    SELECT rowid, " +
	                      select_columns +
	                      ",\n"
	                      "      ROW_NUMBER() OVER (PARTITION BY " +
	                      select_columns +
	                      " ORDER BY rowid) AS _rn\n"
	                      "    FROM " +
	                      view_name +
	                      "\n"
	                      "  ) v JOIN _ivm_net d ON " +
	                      match_conditions +
	                      "\n"
	                      "  WHERE d._net < 0 AND v._rn <= -d._net\n"
	                      ");\n\n";

	// INSERT: replicate each net-insert tuple _net times using generate_series.
	// Cast _net to BIGINT because SUM returns HUGEINT and generate_series requires BIGINT.
	string insert_query = "WITH _ivm_net AS (\n  " + cte_body +
	                      "\n)\n"
	                      "INSERT INTO " +
	                      view_name + " SELECT " + select_columns +
	                      "\nFROM _ivm_net, generate_series(1, _ivm_net._net::BIGINT)\nWHERE _ivm_net._net > 0;\n";

	return delete_query + insert_query;
}

} // namespace duckdb
