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

	cte_string = cte_string + cte_select_string + cte_from_string + cte_group_by_string + ")\n";

	// Outer select: merge delta into MV
	string zeros_list = "[0.0::FLOAT FOR x IN generate_series(1, 64)]";
	string select_string = "select ";
	for (auto &key : keys) {
		select_string = select_string + "delta_" + view_name + "." + key + ", ";
	}
	for (auto &column : aggregates) {
		if (list_mode) {
			select_string += "\n\tlist_transform(list_zip("
			                 "COALESCE(" +
			                 view_name + "." + column + ", " + zeros_list +
			                 "), "
			                 "delta_" +
			                 view_name + "." + column + "), lambda x: x[1] + x[2]) AS " + column + ", ";
		} else {
			select_string = select_string + "\n\tsum(coalesce(" + view_name + "." + column + ", 0) + delta_" +
			                view_name + "." + column + "), ";
		}
	}
	select_string.erase(select_string.size() - 2, 2);
	select_string += "\n";

	string from_string = "from ivm_cte as delta_" + view_name + "\n";
	string join_string = "left join " + view_name + " on ";
	for (auto &key : keys) {
		join_string = join_string + view_name + "." + key + " = delta_" + view_name + "." + key + " and ";
	}
	join_string.erase(join_string.size() - 5, 5);
	join_string += "\n";

	string external_query_string;
	if (list_mode) {
		// No GROUP BY for list mode — CTE consolidates per group, LEFT JOIN is 1:1
		external_query_string = select_string + from_string + join_string + ";";
	} else {
		string group_by_string = "group by ";
		for (auto &key : keys) {
			group_by_string = group_by_string + "delta_" + view_name + "." + key + ", ";
		}
		group_by_string.erase(group_by_string.size() - 2, 2);
		external_query_string = select_string + from_string + join_string + group_by_string + ";";
	}

	string query_string = cte_string + external_query_string;
	string upsert_query = "insert or replace into " + view_name + "\n" + query_string + "\n";

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
