#include "openivm_compile_upsert.hpp"
#include "openivm_constants.hpp"
#include "openivm_utils.hpp"

namespace duckdb {

string CompileAggregateGroups(string &view_name, optional_ptr<CatalogEntry> index_delta_view_catalog_entry,
                              vector<string> column_names, const string &view_query_sql, bool has_minmax) {
	auto index_catalog_entry = dynamic_cast<IndexCatalogEntry *>(index_delta_view_catalog_entry.get());
	auto key_ids = index_catalog_entry->column_ids;

	vector<string> keys;
	vector<string> aggregates;
	for (size_t i = 0; i < key_ids.size(); i++) {
		keys.emplace_back(column_names[key_ids[i]]);
	}

	unordered_set<std::string> keys_set(keys.begin(), keys.end());

	for (auto &column : column_names) {
		if (keys_set.find(column) == keys_set.end() && column != ivm::MULTIPLICITY_COL) {
			aggregates.push_back(column);
		}
	}

	if (has_minmax) {
		// Group-recompute strategy: delete affected groups, re-insert from original query
		string keys_tuple;
		for (size_t i = 0; i < keys.size(); i++) {
			keys_tuple += keys[i];
			if (i != keys.size() - 1) {
				keys_tuple += ", ";
			}
		}

		auto delta_view = OpenIVMUtils::DeltaName(view_name);
		string delete_query = "delete from " + view_name + " where (" + keys_tuple + ") in (\n" +
		                      "  select distinct " + keys_tuple + " from " + delta_view + "\n);\n";

		string insert_query = "insert into " + view_name + "\n" + "select * from (" + view_query_sql +
		                      ") _ivm_recompute\n" + "where (" + keys_tuple + ") in (\n" + "  select distinct " +
		                      keys_tuple + " from " + delta_view + "\n);\n";

		return delete_query + "\n" + insert_query;
	}

	string cte_string = "with ivm_cte AS (\n";
	string cte_select_string = "select ";
	for (auto &key : keys) {
		cte_select_string = cte_select_string + key + ", ";
	}
	for (auto &column : aggregates) {
		cte_select_string = cte_select_string + "\n\tsum(case when " + string(ivm::MULTIPLICITY_COL) + " = false then -" + column +
		                    " else " + column + " end) as " + column + ", ";
	}
	cte_select_string.erase(cte_select_string.size() - 2, 2);
	cte_select_string += "\n";
	auto delta_view = OpenIVMUtils::DeltaName(view_name);
	string cte_from_string = "from " + delta_view + "\n";
	string cte_group_by_string = "group by ";
	for (auto &key : keys) {
		cte_group_by_string = cte_group_by_string + key + ", ";
	}
	cte_group_by_string.erase(cte_group_by_string.size() - 2, 2);

	cte_string = cte_string + cte_select_string + cte_from_string + cte_group_by_string + ")\n";

	string select_string = "select ";
	for (auto &key : keys) {
		select_string = select_string + delta_view + "." + key + ", ";
	}
	for (auto &column : aggregates) {
		select_string = select_string + "\n\tsum(coalesce(" + view_name + "." + column + ", 0) + " + delta_view +
		                "." + column + "), ";
	}
	select_string.erase(select_string.size() - 2, 2);
	select_string += "\n";

	string from_string = "from ivm_cte as " + delta_view + "\n";
	string join_string = "left join " + view_name + " on ";
	for (auto &key : keys) {
		join_string = join_string + view_name + "." + key + " = " + delta_view + "." + key + " and ";
	}
	join_string.erase(join_string.size() - 5, 5);
	join_string += "\n";

	string group_by_string = "group by ";
	for (auto &key : keys) {
		group_by_string = group_by_string + delta_view + "." + key + ", ";
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

string CompileSimpleAggregates(string &view_name, const vector<string> &column_names, const string &view_query_sql,
                               bool has_minmax) {
	if (has_minmax) {
		// Full recompute for ungrouped MIN/MAX
		string delete_query = "delete from " + view_name + ";\n";
		string insert_query = "insert into " + view_name + " " + view_query_sql + ";\n";
		return delete_query + insert_query;
	}

	string update_query = "update " + view_name + "\nset ";
	bool first = true;
	auto delta_view = OpenIVMUtils::DeltaName(view_name);
	for (auto &column : column_names) {
		if (column != ivm::MULTIPLICITY_COL) {
			if (!first) {
				update_query += ",\n";
			}
			first = false;
			update_query += column + " = \n\t" + column + " \n\t\t- coalesce((select " + column + " from " +
			                delta_view + " where " + string(ivm::MULTIPLICITY_COL) + " = false), 0)\n\t\t+ coalesce((select " +
			                column + " from " + delta_view + " where " + string(ivm::MULTIPLICITY_COL) + " = true), 0)";
		}
	}
	update_query += ";\n";
	return update_query;
}

string CompileProjectionsFilters(string &view_name, const vector<string> &column_names) {
	string select_columns;
	string match_conditions;
	for (auto &column : column_names) {
		if (column != ivm::MULTIPLICITY_COL) {
			match_conditions +=
			    view_name + "." + column + " IS NOT DISTINCT FROM net_dels." + column + " and ";
			select_columns += column + ", ";
		}
	}
	match_conditions.erase(match_conditions.size() - 5, 5);
	select_columns.erase(select_columns.size() - 2, 2);

	// Consolidate the delta using EXCEPT ALL to cancel out matching insert/delete pairs.
	// This is needed because the inclusion-exclusion join delta rule can produce
	// redundant entries that cancel each other in bag arithmetic.
	auto delta_view = OpenIVMUtils::DeltaName(view_name);
	string ins_cte = "SELECT " + select_columns + " FROM " + delta_view + " WHERE " + string(ivm::MULTIPLICITY_COL) + " = true";
	string dels_cte =
	    "SELECT " + select_columns + " FROM " + delta_view + " WHERE " + string(ivm::MULTIPLICITY_COL) + " = false";

	string delete_query = "WITH net_dels AS (\n  " + dels_cte + "\n  EXCEPT ALL\n  " + ins_cte + "\n)\ndelete from " +
	                      view_name + " where exists (select 1 from net_dels where " + match_conditions + ");\n\n";

	string insert_query = "WITH net_ins AS (\n  " + ins_cte + "\n  EXCEPT ALL\n  " + dels_cte + "\n)\ninsert into " +
	                      view_name + " select " + select_columns + " from net_ins;\n";

	return delete_query + insert_query;
}

} // namespace duckdb
