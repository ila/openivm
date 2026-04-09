#include "rules/openivm_insert_rule.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_parser.hpp"
#include "core/openivm_refresh_locks.hpp"
#include "core/openivm_utils.hpp"
#include "rules/ivm_column_hider.hpp"

#include "logical_plan_to_sql.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/column_index.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/function/table/read_csv.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
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
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/planner/planner.hpp"

#include <iostream>
#include <map>

namespace duckdb {

// TODO: Remove PAC coupling — delta writes should not need to know about PAC.
// Disable PAC extension checks if loaded (prevents PAC from interfering with delta writes).
static void DisablePACIfLoaded(ClientContext &context, Connection &con) {
	Value pac_val;
	if (context.TryGetCurrentSetting("pac_check", pac_val)) {
		con.Query("SET pac_check = false");
	}
}

// Build the data column list from a delta table catalog entry, excluding metadata columns.
// Returns e.g. "id, name, val" (quoted) — the base table columns only.
static string BuildDeltaDataColumns(TableCatalogEntry &delta_entry) {
	string cols;
	for (auto &col : delta_entry.GetColumns().Logical()) {
		if (col.GetName() == ivm::MULTIPLICITY_COL || col.GetName() == ivm::TIMESTAMP_COL) {
			continue;
		}
		if (!cols.empty()) {
			cols += ", ";
		}
		cols += KeywordHelper::WriteOptionallyQuoted(col.GetName());
	}
	return cols;
}

// Build "INSERT INTO delta_t (col1, col2, ..., mul, ts)" prefix for delta writes.
static string BuildDeltaInsertPrefix(const string &full_delta_table_name, TableCatalogEntry &delta_entry) {
	string col_list = BuildDeltaDataColumns(delta_entry);
	return "INSERT INTO " + full_delta_table_name + " (" + col_list + ", " + string(ivm::MULTIPLICITY_COL) + ", " +
	       string(ivm::TIMESTAMP_COL) + ")";
}

// Build "SELECT col1, col2, ..., <mul_val>, now()::timestamp FROM <source>" for delta writes.
static string BuildDeltaSelectFrom(TableCatalogEntry &delta_entry, const string &mul_val, const string &source) {
	string cols = BuildDeltaDataColumns(delta_entry);
	return "SELECT " + cols + ", " + mul_val + ", now()::timestamp FROM " + source;
}

IVMInsertRule::IVMInsertRule() {
	optimize_function = IVMInsertRuleFunction;
	optimizer_info = make_shared_ptr<IVMInsertOptimizerInfo>();
}

void IVMInsertRule::IVMInsertRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
	auto root = plan.get();

	// Handle DROP TABLE/VIEW: clean up IVM metadata if the dropped object is an IVM view
	if (root->type == LogicalOperatorType::LOGICAL_DROP) {
		auto *simple = dynamic_cast<LogicalSimple *>(root);
		if (!simple) {
			return;
		}
		auto *drop_info = dynamic_cast<DropInfo *>(simple->info.get());
		if (!drop_info || (drop_info->type != CatalogType::TABLE_ENTRY && drop_info->type != CatalogType::VIEW_ENTRY)) {
			return;
		}

		auto table_name = drop_info->name;
		Connection con(*input.context.db);

		auto view_check = con.Query("SELECT 1 FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
		                            OpenIVMUtils::EscapeValue(table_name) + "'");
		if (!view_check->HasError() && view_check->RowCount() > 0) {
			// Acquire view lock to prevent cleanup during an in-flight refresh
			IVMRefreshLocks::LockView(table_name);
			OPENIVM_DEBUG_PRINT("[INSERT RULE] DROP TABLE '%s' — cleaning up IVM metadata\n", table_name.c_str());
			IVMMetadata metadata(con);
			auto delta_tables = metadata.GetDeltaTables(table_name);

			con.Query("DELETE FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
			          OpenIVMUtils::EscapeValue(table_name) + "'");
			con.Query("DELETE FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
			          OpenIVMUtils::EscapeValue(table_name) + "'");
			con.Query("DROP TABLE IF EXISTS " +
			          KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(table_name)));
			con.Query("DROP TABLE IF EXISTS " +
			          KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(table_name)));

			for (auto &dt : delta_tables) {
				// DuckLake entries store the base table name — never drop it
				if (metadata.IsDuckLakeTable(table_name, dt)) {
					continue;
				}
				auto remaining = con.Query("SELECT count(*) FROM " + string(ivm::DELTA_TABLES_TABLE) +
				                           " WHERE table_name = '" + OpenIVMUtils::EscapeValue(dt) + "'");
				if (!remaining->HasError() && remaining->RowCount() > 0 &&
				    remaining->GetValue(0, 0).GetValue<int64_t>() == 0) {
					con.Query("DROP TABLE IF EXISTS " + KeywordHelper::WriteOptionallyQuoted(dt));
				}
			}
			IVMRefreshLocks::UnlockView(table_name);
		}

		// Handle CASCADE: drop dependent MVs
		auto dep_check =
		    con.Query("SELECT DISTINCT view_name FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE table_name = '" +
		              OpenIVMUtils::EscapeValue(OpenIVMUtils::DeltaName(table_name)) + "'");
		if (!dep_check->HasError() && dep_check->RowCount() > 0 && drop_info->cascade) {
			for (size_t i = 0; i < dep_check->RowCount(); i++) {
				auto dep_view = dep_check->GetValue(0, i).ToString();
				// Lock each dependent view before dropping
				IVMRefreshLocks::LockView(dep_view);
				IVMMetadata dep_metadata(con);
				auto dep_delta_tables = dep_metadata.GetDeltaTables(dep_view);

				con.Query("DELETE FROM " + string(ivm::VIEWS_TABLE) + " WHERE view_name = '" +
				          OpenIVMUtils::EscapeValue(dep_view) + "'");
				con.Query("DELETE FROM " + string(ivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
				          OpenIVMUtils::EscapeValue(dep_view) + "'");
				con.Query("DROP TABLE IF EXISTS " +
				          KeywordHelper::WriteOptionallyQuoted(OpenIVMUtils::DeltaName(dep_view)));
				con.Query("DROP TABLE IF EXISTS " +
				          KeywordHelper::WriteOptionallyQuoted(IVMTableNames::DataTableName(dep_view)));
				con.Query("DROP VIEW IF EXISTS " + KeywordHelper::WriteOptionallyQuoted(dep_view));

				for (auto &dt : dep_delta_tables) {
					auto remaining = con.Query("SELECT count(*) FROM " + string(ivm::DELTA_TABLES_TABLE) +
					                           " WHERE table_name = '" + OpenIVMUtils::EscapeValue(dt) + "'");
					if (!remaining->HasError() && remaining->RowCount() > 0 &&
					    remaining->GetValue(0, 0).GetValue<int64_t>() == 0) {
						con.Query("DROP TABLE IF EXISTS " + KeywordHelper::WriteOptionallyQuoted(dt));
					}
				}
				IVMRefreshLocks::UnlockView(dep_view);
			}
		}

		return;
	}

	// Handle ALTER TABLE: sync delta table schema or block if referenced column is affected
	if (root->type == LogicalOperatorType::LOGICAL_ALTER) {
		auto *simple = dynamic_cast<LogicalSimple *>(root);
		if (!simple) {
			return;
		}
		auto *alter_info = dynamic_cast<AlterTableInfo *>(simple->info.get());
		if (!alter_info) {
			return;
		}

		string table_name = alter_info->name;
		string delta_name = OpenIVMUtils::DeltaName(table_name);
		string qdelta = KeywordHelper::WriteOptionallyQuoted(delta_name);

		Connection con(*input.context.db);
		// Check if a delta table exists for this base table (i.e., it's tracked by IVM)
		auto delta_check = con.Query("SELECT 1 FROM information_schema.tables WHERE table_name = '" +
		                             OpenIVMUtils::EscapeValue(delta_name) + "'");
		if (delta_check->HasError() || delta_check->RowCount() == 0) {
			return; // not an IVM-tracked table
		}

		// Helper: check if a column is referenced by any MV that depends on this base table.
		// Plans each MV's stored query and walks LOGICAL_GET nodes for the target table.
		auto column_referenced_by_mv = [&](const string &col_name) -> string {
			IVMMetadata metadata(con);
			auto mvs = con.Query("SELECT DISTINCT d.view_name FROM " + string(ivm::DELTA_TABLES_TABLE) +
			                     " d WHERE d.table_name = '" + OpenIVMUtils::EscapeValue(delta_name) + "'");
			if (mvs->HasError()) {
				return "";
			}
			for (size_t i = 0; i < mvs->RowCount(); i++) {
				string view_name = mvs->GetValue(0, i).ToString();
				string sql = metadata.GetViewQuery(view_name);
				if (sql.empty()) {
					continue;
				}
				try {
					con.BeginTransaction();
					Parser p;
					p.ParseQuery(sql);
					Planner planner(*con.context);
					planner.CreatePlan(p.statements[0]->Copy());
					auto view_plan = std::move(planner.plan);
					con.Rollback();

					// Walk the plan tree looking for LOGICAL_GET on the target table
					std::function<bool(LogicalOperator &)> check_plan = [&](LogicalOperator &op) -> bool {
						if (op.type == LogicalOperatorType::LOGICAL_GET) {
							auto &get = op.Cast<LogicalGet>();
							auto tbl = get.GetTable();
							if (tbl.get() && tbl->name == table_name) {
								for (auto &cid : get.GetColumnIds()) {
									if (get.GetColumnName(cid) == col_name) {
										return true;
									}
								}
							}
						}
						for (auto &child : op.children) {
							if (check_plan(*child)) {
								return true;
							}
						}
						return false;
					};

					if (check_plan(*view_plan)) {
						return view_name;
					}
				} catch (...) {
					con.Rollback();
				}
			}
			return "";
		};

		switch (alter_info->alter_table_type) {
		case AlterTableType::ADD_COLUMN: {
			auto *add_info = dynamic_cast<AddColumnInfo *>(alter_info);
			if (!add_info) {
				break;
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE ADD COLUMN '%s' — syncing delta table\n",
			                    add_info->new_column.Name().c_str());
			con.Query("ALTER TABLE " + qdelta + " ADD COLUMN IF NOT EXISTS " +
			          KeywordHelper::WriteOptionallyQuoted(add_info->new_column.Name()) + " " +
			          add_info->new_column.Type().ToString());
			break;
		}
		case AlterTableType::REMOVE_COLUMN: {
			auto *remove_info = dynamic_cast<RemoveColumnInfo *>(alter_info);
			if (!remove_info) {
				break;
			}
			string col_name = remove_info->removed_column;
			string referencing_mv = column_referenced_by_mv(col_name);
			if (!referencing_mv.empty()) {
				throw CatalogException("Cannot drop column '" + col_name +
				                       "': it is referenced by materialized view '" + referencing_mv +
				                       "'. Drop the view first.");
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE DROP COLUMN '%s' — syncing delta table\n", col_name.c_str());
			con.Query("ALTER TABLE " + qdelta + " DROP COLUMN IF EXISTS " +
			          KeywordHelper::WriteOptionallyQuoted(col_name));
			break;
		}
		case AlterTableType::RENAME_COLUMN: {
			auto *rename_info = dynamic_cast<RenameColumnInfo *>(alter_info);
			if (!rename_info) {
				break;
			}
			string old_name = rename_info->old_name;
			string new_name = rename_info->new_name;
			string referencing_mv = column_referenced_by_mv(old_name);
			if (!referencing_mv.empty()) {
				throw CatalogException("Cannot rename column '" + old_name +
				                       "': it is referenced by materialized view '" + referencing_mv +
				                       "'. Drop the view first.");
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE RENAME COLUMN '%s' → '%s' — syncing delta table\n",
			                    old_name.c_str(), new_name.c_str());
			con.Query("ALTER TABLE " + qdelta + " RENAME COLUMN " + KeywordHelper::WriteOptionallyQuoted(old_name) +
			          " TO " + KeywordHelper::WriteOptionallyQuoted(new_name));
			break;
		}
		default:
			break;
		}
		return;
	}

	if (plan->children.empty()) {
		return;
	}

	auto root_name = root->GetName();
	if (root_name.rfind("INSERT", 0) != 0 && root_name.rfind("DELETE", 0) != 0 && root_name.rfind("UPDATE", 0) != 0) {
		return;
	}

	switch (root->type) {
	case LogicalOperatorType::LOGICAL_INSERT: {
		auto insert_node = dynamic_cast<LogicalInsert *>(root);
		auto insert_table_name = insert_node->table.name;
		OPENIVM_DEBUG_PRINT("[INSERT RULE] INSERT into '%s'\n", insert_table_name.c_str());

		if (OpenIVMUtils::IsDelta(insert_table_name) || insert_table_name.empty() ||
		    IVMTableNames::IsDataTable(insert_table_name)) {
			return;
		}
		// DuckLake tables have native change tracking — no delta writes needed
		if (insert_node->table.catalog.GetCatalogType() == "ducklake") {
			OPENIVM_DEBUG_PRINT("[INSERT RULE] Skipping delta for DuckLake table '%s'\n", insert_table_name.c_str());
			return;
		}
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, insert_node->table.catalog.GetName(), insert_node->table.schema.name,
		    OpenIVMUtils::DeltaName(insert_table_name), OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			Connection con(*input.context.db);
			DisablePACIfLoaded(input.context, con);
			IVMMetadata metadata(con);
			if (metadata.IsBaseTable(insert_table_name)) {
				string full_delta_table_name = OpenIVMUtils::FullDeltaName(
				    insert_node->table.catalog.GetName(), insert_node->table.schema.name, insert_node->table.name);
				if (insert_node->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					auto &delta_entry_ins = delta_table_catalog_entry->Cast<TableCatalogEntry>();
					string insert_query = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_ins);

					auto projection = dynamic_cast<LogicalProjection *>(insert_node->children[0].get());
					if (projection->children[0]->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
						insert_query += " VALUES ";
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
						auto &delta_entry = delta_table_catalog_entry->Cast<TableCatalogEntry>();
						string prefix = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry);
						LogicalPlanToSql lpts(*con.context, insert_node->children[0]);
						auto cte_list = lpts.LogicalPlanToCteList();
						string subquery_string = LogicalPlanToSql::CteListToSql(cte_list);
						if (!subquery_string.empty() && subquery_string.back() == ';') {
							subquery_string.pop_back();
						}
						insert_query = prefix + " SELECT *, true, now()::timestamp FROM (" + subquery_string + ")";
					}
					OPENIVM_DEBUG_PRINT("[INSERT RULE] insert_query: %s\n", insert_query.c_str());
					{
						DeltaLockGuard guard(OpenIVMUtils::DeltaName(insert_table_name));
						auto r = con.Query(insert_query);
						if (r->HasError()) {
							throw InternalException("Cannot insert in delta table after insertion! " + r->GetError());
						}
					}

				} else if (insert_node->children[0]->type == LogicalOperatorType::LOGICAL_GET) {
					auto get = dynamic_cast<LogicalGet *>(insert_node->children[0].get());
					auto *bind_data = dynamic_cast<MultiFileBindData *>(get->bind_data.get());
					if (!bind_data) {
						throw NotImplementedException(
						    "Only CSV file imports (read_csv) are supported for IVM delta tracking "
						    "via LOGICAL_GET. Other table functions are not yet supported.");
					}
					auto &delta_entry_csv = delta_table_catalog_entry->Cast<TableCatalogEntry>();
					string prefix_csv = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_csv);
					auto files = bind_data->file_list->GetAllFiles();
					for (auto &file : files) {
						auto query =
						    prefix_csv + " SELECT *, true, now()::timestamp FROM read_csv('" + file.path + "');";
						DeltaLockGuard guard(OpenIVMUtils::DeltaName(insert_table_name));
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
		OPENIVM_DEBUG_PRINT("[INSERT RULE] DELETE from '%s'\n", delete_table_name.c_str());
		if (OpenIVMUtils::IsDelta(delete_table_name) || IVMTableNames::IsDataTable(delete_table_name)) {
			return;
		}
		if (delete_node->table.catalog.GetCatalogType() == "ducklake") {
			OPENIVM_DEBUG_PRINT("[INSERT RULE] Skipping delta for DuckLake table '%s'\n", delete_table_name.c_str());
			return;
		}
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, delete_node->table.catalog.GetName(), delete_node->table.schema.name,
		    OpenIVMUtils::DeltaName(delete_table_name), OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			auto full_table_name = OpenIVMUtils::FullName(delete_node->table.catalog.GetName(),
			                                              delete_node->table.schema.name, delete_node->table.name);
			auto full_delta_table_name = OpenIVMUtils::FullDeltaName(
			    delete_node->table.catalog.GetName(), delete_node->table.schema.name, delete_node->table.name);
			Connection con(*input.context.db);
			DisablePACIfLoaded(input.context, con);
			IVMMetadata metadata(con);
			if (metadata.IsBaseTable(delete_table_name)) {
				auto &delta_entry_del = delta_table_catalog_entry->Cast<TableCatalogEntry>();
				string insert_string = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_del) + " " +
				                       BuildDeltaSelectFrom(delta_entry_del, "false", full_table_name);
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
					try {
						string prefix_del = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_del);
						LogicalPlanToSql lpts(*con.context, plan->children[0]);
						auto cte_list = lpts.LogicalPlanToCteList();
						string subquery_string = LogicalPlanToSql::CteListToSql(cte_list);
						if (!subquery_string.empty() && subquery_string.back() == ';') {
							subquery_string.pop_back();
						}
						insert_string =
						    prefix_del + " SELECT *, false, now()::timestamp FROM (" + subquery_string + ")";
					} catch (...) {
						throw NotImplementedException(
						    "DELETE with complex subqueries is not yet fully supported for IVM delta tracking");
					}
				}

				{
					DeltaLockGuard guard(OpenIVMUtils::DeltaName(delete_table_name));
					auto r = con.Query(insert_string);
					if (r->HasError()) {
						throw InternalException("Cannot insert in delta table after deletion! " + r->GetError());
					}
				}
			}
		}
	} break;

	case LogicalOperatorType::LOGICAL_UPDATE: {
		auto update_node = dynamic_cast<LogicalUpdate *>(root);
		auto update_table_name = update_node->table.name;
		if (OpenIVMUtils::IsDelta(update_table_name) || IVMTableNames::IsDataTable(update_table_name)) {
			return;
		}
		if (update_node->table.catalog.GetCatalogType() == "ducklake") {
			OPENIVM_DEBUG_PRINT("[INSERT RULE] Skipping delta for DuckLake table '%s'\n", update_table_name.c_str());
			return;
		}
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, update_node->table.catalog.GetName(), update_node->table.schema.name,
		    OpenIVMUtils::DeltaName(update_table_name), OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			Connection con(*input.context.db);
			DisablePACIfLoaded(input.context, con);
			IVMMetadata metadata(con);
			if (!metadata.IsBaseTable(update_table_name)) {
				break;
			}
			{
				auto full_table_name = OpenIVMUtils::FullName(update_node->table.catalog.GetName(),
				                                              update_node->table.schema.name, update_node->table.name);
				auto full_delta_table_name = OpenIVMUtils::FullDeltaName(
				    update_node->table.catalog.GetName(), update_node->table.schema.name, update_node->table.name);
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
						throw NotImplementedException("UPDATE with computed SET expressions (e.g., SET col = col + 1) "
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

				auto &delta_entry_upd = delta_table_catalog_entry->Cast<TableCatalogEntry>();
				string prefix_upd = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_upd);
				string select_old = BuildDeltaSelectFrom(delta_entry_upd, "false", full_table_name) + where_string;
				// For select_new: use the update_values map to replace modified columns
				string select_new = "SELECT ";
				for (auto &col : delta_entry_upd.GetColumns().Logical()) {
					if (col.GetName() == ivm::MULTIPLICITY_COL || col.GetName() == ivm::TIMESTAMP_COL) {
						continue;
					}
					// Find the column's positional index in the base table
					auto base_columns = update_node->table.GetColumns().GetColumnNames();
					for (size_t i = 0; i < base_columns.size(); i++) {
						if (base_columns[i] == col.GetName()) {
							if (update_values.find(to_string(i)) != update_values.end()) {
								select_new += update_values[to_string(i)] + ", ";
							} else {
								select_new += KeywordHelper::WriteOptionallyQuoted(col.GetName()) + ", ";
							}
							break;
						}
					}
				}
				select_new += "true, now()::timestamp FROM " + full_table_name + where_string;

				{
					DeltaLockGuard guard(OpenIVMUtils::DeltaName(update_table_name));
					auto r = con.Query(prefix_upd + " " + select_old);
					if (r->HasError()) {
						throw InternalException("Cannot insert old values in delta table after update! " +
						                        r->GetError());
					}
					OPENIVM_DEBUG_PRINT("[INSERT RULE] select_new: %s\n", select_new.c_str());
					auto r2 = con.Query(prefix_upd + " " + select_new);
					if (r2->HasError()) {
						throw InternalException("Cannot insert new values in delta table after update! " +
						                        r2->GetError());
					}
				}
			}
		}
	} break;
	default:
		return;
	}
}

} // namespace duckdb
