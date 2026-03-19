#include "openivm_insert_rule.hpp"
#include "openivm_debug.hpp"
#include "openivm_parser.hpp"

#include "logical_plan_to_sql.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_expression_get.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/planner.hpp"

#include <iostream>
#include <map>

namespace duckdb {

static bool StartsWith(const string &str, const string &prefix) {
	return str.size() >= prefix.size() && str.rfind(prefix, 0) == 0;
}


IVMInsertRule::IVMInsertRule() {
	optimize_function = IVMInsertRuleFunction;
	optimizer_info = make_shared_ptr<IVMInsertOptimizerInfo>();
}

void IVMInsertRule::IVMInsertRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
	if (plan->children.empty()) {
		return;
	}

	auto root = plan.get();
	if (!StartsWith(root->GetName(), "INSERT") && !StartsWith(root->GetName(), "DELETE") &&
	    !StartsWith(root->GetName(), "UPDATE")) {
		return;
	}

	switch (root->type) {
	case LogicalOperatorType::LOGICAL_INSERT: {
		auto insert_node = dynamic_cast<LogicalInsert *>(root);
		auto insert_table_name = insert_node->table.name;
		OPENIVM_DEBUG_PRINT("[INSERT RULE] INSERT into '%s'\n", insert_table_name.c_str());
		auto delta_insert_table = "delta_" + insert_node->table.name;

		if (StartsWith(insert_table_name, "delta_") || insert_table_name.empty()) {
			return;
		}
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, insert_node->table.catalog.GetName(), insert_node->table.schema.name, delta_insert_table,
		    OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			Connection con(*input.context.db);
			auto t = con.Query("select * from _duckdb_ivm_views where view_name = '" + insert_table_name + "'");
			if (t->RowCount() == 0) {
				string full_delta_table_name = insert_node->table.catalog.GetName() + "." +
				                               insert_node->table.schema.name + ".delta_" + insert_node->table.name;
				if (insert_node->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					string insert_query = "insert into " + full_delta_table_name;

					auto projection = dynamic_cast<LogicalProjection *>(insert_node->children[0].get());
					if (projection->children[0]->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
						insert_query += " values ";
						auto expression_get = dynamic_cast<LogicalExpressionGet *>(projection->children[0].get());
						for (auto &expression : expression_get->expressions) {
							string values = "(";
							for (auto &value : expression) {
								if (value->type == ExpressionType::VALUE_CONSTANT) {
									auto constant = dynamic_cast<BoundConstantExpression *>(value.get());
									values += constant->value.ToSQLString() + ",";
								} else {
									throw NotImplementedException("Only constant values are supported for now!");
								}
							}
							values += "true, now()::timestamp),";
							insert_query += values;
						}
						insert_query.pop_back();
					} else {
						insert_query += " select *, true, now()::timestamp from (";
						LogicalPlanToSql lpts(*con.context, insert_node->children[0]);
						auto cte_list = lpts.LogicalPlanToCteList();
						string subquery_string = LogicalPlanToSql::CteListToSql(cte_list);
						// Strip trailing semicolon from LPTS output since it's inside a subquery
						if (!subquery_string.empty() && subquery_string.back() == ';') {
							subquery_string.pop_back();
						}
						insert_query += subquery_string + ")";
					}
					auto r = con.Query(insert_query);
					if (r->HasError()) {
						throw InternalException("Cannot insert in delta table after insertion! " + r->GetError());
					}

				} else if (insert_node->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
					auto get = dynamic_cast<LogicalGet *>(insert_node->children[0].get());
					auto *bind_data = dynamic_cast<MultiFileBindData *>(get->bind_data.get());
					if (!bind_data) {
						throw NotImplementedException(
						    "Only CSV file imports (read_csv) are supported for IVM delta tracking "
						    "via LOGICAL_GET. Other table functions are not yet supported.");
					}
					auto files = bind_data->file_list->GetAllFiles();
					for (auto &file : files) {
						auto query = "insert into " + full_delta_table_name +
						             " select *, true, now()::timestamp from read_csv('" + file.path + "');";
						auto r = con.Query(query);
						if (r->HasError()) {
							throw InternalException("Cannot insert in delta table! " + r->GetError());
						}
					}
				}
			}
		}
	} break;

	case LogicalOperatorType::LOGICAL_DELETE: {
		auto delete_node = dynamic_cast<LogicalDelete *>(root);
		auto delete_table_name = delete_node->table.name;
		OPENIVM_DEBUG_PRINT("[INSERT RULE] DELETE from '%s' (full: %s.%s.%s)\n",
		                    delete_table_name.c_str(), delete_node->table.catalog.GetName().c_str(),
		                    delete_node->table.schema.name.c_str(), delete_table_name.c_str());
		if (StartsWith(delete_table_name, "delta_")) {
			return;
		}
		auto delta_delete_table = "delta_" + delete_node->table.name;
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, delete_node->table.catalog.GetName(), delete_node->table.schema.name, delta_delete_table,
		    OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			auto full_table_name = delete_node->table.catalog.GetName() + "." + delete_node->table.schema.name + "." +
			                       delete_node->table.name;
			auto full_delta_table_name = delete_node->table.catalog.GetName() + "." + delete_node->table.schema.name +
			                             ".delta_" + delete_node->table.name;
			Connection con(*input.context.db);
			auto t = con.Query("select * from _duckdb_ivm_views where view_name = '" + delete_table_name + "'");
			if (t->RowCount() == 0) {
				string insert_string = "insert into " + full_delta_table_name +
				                       " select *, false, now()::timestamp from " + full_table_name;
				if (plan->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
					auto filter = dynamic_cast<LogicalFilter *>(plan->children[0].get());
					insert_string += " where ";
					for (idx_t i = 0; i < filter->expressions.size(); i++) {
						if (i > 0) {
							insert_string += " AND ";
						}
						insert_string += filter->expressions[i]->ToString();
					}
				} else if (plan->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
					auto get = dynamic_cast<LogicalGet *>(plan->children[0].get());
					if (!get->table_filters.filters.empty()) {
						insert_string += " where ";
						bool first_filter = true;
						for (auto &entry : get->table_filters.filters) {
							if (!first_filter) {
								insert_string += " AND ";
							}
							first_filter = false;
							auto col_name = get->GetColumnName(ColumnIndex(entry.first));
							insert_string += entry.second->ToString(col_name);
						}
					}
				} else if (plan->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
					return;
				} else {
					throw NotImplementedException("Only simple DELETE statements are supported in IVM!");
				}

				auto r = con.Query(insert_string);
				if (r->HasError()) {
					throw InternalException("Cannot insert in delta table after deletion! " + r->GetError());
				}
			}
		}
	} break;

	case LogicalOperatorType::LOGICAL_UPDATE: {
		auto update_node = dynamic_cast<LogicalUpdate *>(root);
		auto update_table_name = update_node->table.name;
		if (StartsWith(update_table_name, "delta_")) {
			return;
		}
		auto delta_update_table_name = "delta_" + update_node->table.name;
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, update_node->table.catalog.GetName(), update_node->table.schema.name,
		    delta_update_table_name, OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			Connection con(*input.context.db);
			// Scope the query result so it's released before reusing the connection
			{
				auto t = con.Query("select * from _duckdb_ivm_views where view_name = '" + update_table_name + "'");
				if (t->RowCount() != 0) {
					break;
				}
			}
			{
				auto full_table_name = update_node->table.catalog.GetName() + "." + update_node->table.schema.name +
				                       "." + update_node->table.name;
				auto full_delta_table_name = update_node->table.catalog.GetName() + "." +
				                             update_node->table.schema.name + ".delta_" + update_node->table.name;
				auto *projection = dynamic_cast<LogicalProjection *>(update_node->children[0].get());
				if (!projection) {
					OPENIVM_DEBUG_PRINT("[INSERT RULE] UPDATE skipped: no projection child (child type: %s)\n",
					                    LogicalOperatorToString(update_node->children[0]->type).c_str());
					break;
				}

				std::map<string, string> update_values;
				string where_string;
				for (size_t i = 0; i < update_node->columns.size(); i++) {
					auto column = update_node->columns[i].index;
					auto *value = dynamic_cast<BoundConstantExpression *>(projection->expressions[i].get());
					if (!value) {
						throw NotImplementedException(
						    "UPDATE with computed SET expressions (e.g., SET col = col + 1) "
						    "is not yet supported for IVM delta tracking!");
					}
					update_values[to_string(column)] = value->value.ToSQLString();
				}

				if (projection->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
					auto filter = dynamic_cast<LogicalFilter *>(projection->children[0].get());
					where_string += " where ";
					for (idx_t i = 0; i < filter->expressions.size(); i++) {
						if (i > 0) {
							where_string += " AND ";
						}
						where_string += filter->expressions[i]->ToString();
					}
				} else if (projection->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
					auto get = dynamic_cast<LogicalGet *>(projection->children[0].get());
					if (!get->table_filters.filters.empty()) {
						where_string += " where ";
						bool first_filter = true;
						for (auto &entry : get->table_filters.filters) {
							if (!first_filter) {
								where_string += " AND ";
							}
							first_filter = false;
							auto col_name = get->GetColumnName(ColumnIndex(entry.first));
							where_string += entry.second->ToString(col_name);
						}
					}
				} else if (projection->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
					return;
				} else {
					throw NotImplementedException("Only simple UPDATE statements are supported in IVM!");
				}

				// Build a single INSERT with UNION ALL of old (delete) + new (insert) rows
				// to avoid multiple con.Query() calls inside the optimizer
				string select_old = "select *, false, now()::timestamp from " + full_table_name + where_string;

				string select_new = "select ";
				auto columns = update_node->table.GetColumns().GetColumnNames();
				for (size_t i = 0; i < columns.size(); i++) {
					if (update_values.find(to_string(i)) != update_values.end()) {
						select_new += update_values[to_string(i)] + ", ";
					} else {
						select_new += columns[i] + ", ";
					}
				}
				select_new += "true, now()::timestamp from " + full_table_name + where_string;

				auto r = con.Query("insert into " + full_delta_table_name + " " + select_old);
				if (r->HasError()) {
					throw InternalException("Cannot insert old values in delta table after update! " + r->GetError());
				}
				OPENIVM_DEBUG_PRINT("[INSERT RULE] select_new: %s\n", select_new.c_str());
				{
					Connection con2(*input.context.db);
					auto r2 = con2.Query("insert into " + full_delta_table_name + " " + select_new);
					if (r2->HasError()) {
						throw InternalException("Cannot insert new values in delta table after update! " +
						                        r2->GetError());
					}
				}
			} // end inner block with full_table_name scope
		}
	} break;
	default:
		return;
	}
}

} // namespace duckdb
