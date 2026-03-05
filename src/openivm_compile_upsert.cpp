#include "openivm_compile_upsert.hpp"

namespace duckdb {

string CompileAggregateGroups(string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names) {
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

	string cte_string = "with ivm_cte AS (\n";
	string cte_select_string = "select ";
	for (auto &key : keys) {
		cte_select_string = cte_select_string + key + ", ";
	}
	for (auto &column : aggregates) {
		cte_select_string = cte_select_string + "\n\tsum(case when _duckdb_ivm_multiplicity = false then -" + column +
		                    " else " + column + " end) as " + column + ", ";
	}
	cte_select_string.erase(cte_select_string.size() - 2, 2);
	cte_select_string += "\n";
	string cte_from_string = "from delta_" + view_name + "\n";
	string cte_group_by_string = "group by ";
	for (auto &key : keys) {
		cte_group_by_string = cte_group_by_string + key + ", ";
	}
	cte_group_by_string.erase(cte_group_by_string.size() - 2, 2);

	cte_string = cte_string + cte_select_string + cte_from_string + cte_group_by_string + ")\n";

	string select_string = "select ";
	for (auto &key : keys) {
		select_string = select_string + "delta_" + view_name + "." + key + ", ";
	}
	for (auto &column : aggregates) {
		select_string = select_string + "\n\tsum(coalesce(" + view_name + "." + column + ", 0) + delta_" + view_name +
		                "." + column + "), ";
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

	string group_by_string = "group by ";
	for (auto &key : keys) {
		group_by_string = group_by_string + "delta_" + view_name + "." + key + ", ";
	}
	group_by_string.erase(group_by_string.size() - 2, 2);

	string external_query_string = select_string + from_string + join_string + group_by_string + ";";
	string query_string = cte_string + external_query_string;
	string upsert_query = "insert or replace into " + view_name + "\n" + query_string + "\n";

	string delete_query = "\ndelete from " + view_name + " where ";
	for (auto &column : aggregates) {
		delete_query += column + " = 0 and ";
	}
	delete_query.erase(delete_query.size() - 5, 5);
	delete_query += ";\n";
	upsert_query += delete_query;

	return upsert_query;
}

string CompileSimpleAggregates(string &view_name, const vector<string> &column_names) {
	string update_query = "update " + view_name + "\nset ";
	bool first = true;
	for (auto &column : column_names) {
		if (column != "_duckdb_ivm_multiplicity") {
			if (!first) {
				update_query += ",\n";
			}
			first = false;
			update_query += column + " = \n\t" + column + " \n\t\t- coalesce((select " + column + " from delta_" +
			                view_name + " where _duckdb_ivm_multiplicity = false), 0)\n\t\t+ coalesce((select " +
			                column + " from delta_" + view_name + " where _duckdb_ivm_multiplicity = true), 0)";
		}
	}
	update_query += ";\n";
	return update_query;
}

string CompileProjectionsFilters(string &view_name, const vector<string> &column_names) {
	string delete_query = "delete from " + view_name + " where exists (select 1 from delta_" + view_name + " where ";
	string select_columns;
	for (auto &column : column_names) {
		if (column != "_duckdb_ivm_multiplicity") {
			delete_query += view_name + "." + column + " = delta_" + view_name + "." + column + " and ";
			select_columns += column + ", ";
		}
	}
	delete_query += "_duckdb_ivm_multiplicity = false);\n\n";
	select_columns.erase(select_columns.size() - 2, 2);

	string insert_query = "insert into " + view_name + " select " + select_columns + " from delta_" + view_name +
	                      " where _duckdb_ivm_multiplicity = true;\n";
	return delete_query + insert_query;
}

} // namespace duckdb
