#include "upsert/openivm_compile_upsert.hpp"

namespace duckdb {

string CompileAggregateGroups(string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql, bool has_minmax,
                              bool list_mode, const string &delta_ts_filter) {
	auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
	auto key_ids = index_catalog_entry->column_ids;

	vector<string> keys;
	vector<string> aggregates;
	for (size_t i = 0; i < key_ids.size(); i++) {
		keys.emplace_back(column_names[key_ids[i]]);
	}

	unordered_set<std::string> keys_set(keys.begin(), keys.end());

	for (auto &column : column_names) {
		if (keys_set.find(column) == keys_set.end() && column != "_duckdb_ivm_multiplicity") {
			aggregates.push_back(column);
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
		                      keys_tuple + " from delta_" + view_name + delta_where + "\n);\n";
		string insert_query = "insert into " + view_name + "\n" + "select * from (" + view_query_sql +
		                      ") _ivm_recompute\n" + "where (" + keys_tuple + ") in (\n" + "  select distinct " +
		                      keys_tuple + " from delta_" + view_name + delta_where + "\n);\n";
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
			cte_select_string += "\n\tlist_reduce(list(CASE WHEN _duckdb_ivm_multiplicity = false "
			                     "THEN list_transform(" +
			                     column +
			                     ", lambda x: -x) "
			                     "ELSE " +
			                     column +
			                     " END), "
			                     "lambda a, b: list_transform(list_zip(a, b), lambda x: x[1] + x[2])) AS " +
			                     column + ", ";
		} else {
			cte_select_string = cte_select_string + "\n\tsum(case when _duckdb_ivm_multiplicity = false then -" +
			                    column + " else " + column + " end) as " + column + ", ";
		}
	}
	cte_select_string.erase(cte_select_string.size() - 2, 2);
	cte_select_string += "\n";
	string cte_from_string = "from delta_" + view_name;
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

	// UPDATE existing rows: add delta to current MV values.
	// Uses IS NOT DISTINCT FROM so NULLs in group keys match correctly
	// (unique indexes don't conflict on NULLs, so INSERT OR REPLACE fails for NULL keys).
	string update_set;
	{
		string zeros_list = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";
		bool first = true;
		for (auto &column : aggregates) {
			if (!first) {
				update_set += ", ";
			}
			first = false;
			if (list_mode) {
				update_set += column + " = list_transform(list_zip(" + view_name + "." + column + ", d." + column +
				              "), lambda x: x[1] + x[2])";
			} else {
				update_set += column + " = " + view_name + "." + column + " + d." + column;
			}
		}
	}
	string update_where;
	for (size_t i = 0; i < keys.size(); i++) {
		if (i > 0) {
			update_where += " AND ";
		}
		update_where += view_name + "." + keys[i] + " IS NOT DISTINCT FROM d." + keys[i];
	}
	string update_query = "WITH ivm_cte AS (\n" + cte_body + ")\n" + "UPDATE " + view_name + " SET " + update_set +
	                      "\nFROM ivm_cte d\nWHERE " + update_where + ";\n";

	// INSERT new groups: rows in delta that don't exist in MV yet.
	string insert_cols;
	for (auto &key : keys) {
		insert_cols += "d." + key + ", ";
	}
	for (size_t i = 0; i < aggregates.size(); i++) {
		insert_cols += "d." + aggregates[i];
		if (i < aggregates.size() - 1) {
			insert_cols += ", ";
		}
	}
	string not_exists_cond;
	for (size_t i = 0; i < keys.size(); i++) {
		if (i > 0) {
			not_exists_cond += " AND ";
		}
		not_exists_cond += "v." + keys[i] + " IS NOT DISTINCT FROM d." + keys[i];
	}
	string insert_query = "WITH ivm_cte AS (\n" + cte_body + ")\n" + "INSERT INTO " + view_name + " SELECT " +
	                      insert_cols + "\nFROM ivm_cte d\nWHERE NOT EXISTS (SELECT 1 FROM " + view_name + " v WHERE " +
	                      not_exists_cond + ");\n";

	string upsert_query = update_query + "\n" + insert_query + "\n";

	// Delete zero rows
	string delete_query = "\ndelete from " + view_name + " where ";
	for (auto &column : aggregates) {
		if (list_mode) {
			delete_query += "list_reduce(" + column + ", lambda a, b: a + b) = 0.0 and ";
		} else {
			delete_query += column + " = 0 and ";
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
		string delete_query = "delete from " + view_name + ";\n";
		string insert_query = "insert into " + view_name + " " + view_query_sql + ";\n";
		return delete_query + insert_query;
	}

	string ts_and = delta_ts_filter.empty() ? "" : " AND " + delta_ts_filter;
	string update_query = "update " + view_name + "\nset ";
	bool first = true;
	string zeros_list = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";
	for (auto &column : column_names) {
		if (column != "_duckdb_ivm_multiplicity") {
			if (!first) {
				update_query += ",\n";
			}
			first = false;
			if (list_mode) {
				string negate_del = "coalesce((select list_transform(" + column + ", lambda x: -x) from delta_" +
				                    view_name + " where _duckdb_ivm_multiplicity = false" + ts_and + "), " +
				                    zeros_list + ")";
				string ins = "coalesce((select " + column + " from delta_" + view_name +
				             " where _duckdb_ivm_multiplicity = true" + ts_and + "), " + zeros_list + ")";
				string delta = "list_transform(list_zip(" + negate_del + ", " + ins + "), lambda x: x[1] + x[2])";
				update_query +=
				    column + " = list_transform(list_zip(" + column + ", " + delta + "), lambda x: x[1] + x[2])";
			} else {
				update_query += column + " = \n\tcoalesce(" + column + ", 0) \n\t\t- coalesce((select " + column +
				                " from delta_" + view_name + " where _duckdb_ivm_multiplicity = false" + ts_and +
				                "), 0)\n\t\t+ coalesce((select " + column + " from delta_" + view_name +
				                " where _duckdb_ivm_multiplicity = true" + ts_and + "), 0)";
			}
		}
	}
	update_query += ";\n";
	return update_query;
}

string CompileProjectionsFilters(string &view_name, const vector<string> &column_names, const string &delta_ts_filter) {
	string select_columns;
	string match_conditions;
	for (auto &column : column_names) {
		if (column != "_duckdb_ivm_multiplicity") {
			match_conditions += view_name + "." + column + " IS NOT DISTINCT FROM net_dels." + column + " and ";
			select_columns += column + ", ";
		}
	}
	match_conditions.erase(match_conditions.size() - 5, 5);
	select_columns.erase(select_columns.size() - 2, 2);

	// Consolidate the delta using EXCEPT ALL to cancel out matching insert/delete pairs.
	// This is needed because the inclusion-exclusion join delta rule can produce
	// redundant entries that cancel each other in bag arithmetic.
	string ts_and = delta_ts_filter.empty() ? "" : " AND " + delta_ts_filter;
	string ins_cte =
	    "SELECT " + select_columns + " FROM delta_" + view_name + " WHERE _duckdb_ivm_multiplicity = true" + ts_and;
	string dels_cte =
	    "SELECT " + select_columns + " FROM delta_" + view_name + " WHERE _duckdb_ivm_multiplicity = false" + ts_and;

	string delete_query = "WITH net_dels AS (\n  " + dels_cte + "\n  EXCEPT ALL\n  " + ins_cte + "\n)\ndelete from " +
	                      view_name + " where exists (select 1 from net_dels where " + match_conditions + ");\n\n";

	string insert_query = "WITH net_ins AS (\n  " + ins_cte + "\n  EXCEPT ALL\n  " + dels_cte + "\n)\ninsert into " +
	                      view_name + " select " + select_columns + " from net_ins;\n";

	return delete_query + insert_query;
}

} // namespace duckdb
