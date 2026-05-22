#include "rules/refresh_insert_rule.hpp"
#include "rules/schema_evolution.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/parser.hpp"
#include "core/refresh_locks.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"

#include "lpts_pipeline.hpp"
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
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
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

// PAC compatibility boundary: delta writes run through a fresh connection, so
// disable PAC checks when that extension is loaded in the caller session.
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
		if (col.GetName() == openivm::MULTIPLICITY_COL || col.GetName() == openivm::TIMESTAMP_COL) {
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
	return "INSERT INTO " + full_delta_table_name + " (" + col_list + ", " + string(openivm::MULTIPLICITY_COL) + ", " +
	       string(openivm::TIMESTAMP_COL) + ")";
}

// Build "SELECT col1, col2, ..., <mul_val>, now()::timestamp FROM <source>" for delta writes.
static string BuildDeltaSelectFrom(TableCatalogEntry &delta_entry, const string &mul_val, const string &source) {
	string cols = BuildDeltaDataColumns(delta_entry);
	return "SELECT " + cols + ", " + mul_val + ", now()::timestamp FROM " + source;
}

using BoundColumnNameMap = std::map<std::pair<idx_t, idx_t>, string>;

static void CollectBoundColumnNames(LogicalOperator &op, BoundColumnNameMap &column_names) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		auto bindings = get.GetColumnBindings();
		auto column_ids = get.GetColumnIds();
		for (auto &binding : bindings) {
			if (binding.column_index >= column_ids.size()) {
				continue;
			}
			auto column_name = get.GetColumnName(column_ids[binding.column_index]);
			column_names[{binding.table_index, binding.column_index}] =
			    KeywordHelper::WriteOptionallyQuoted(column_name);
		}
	}
	for (auto &child : op.children) {
		CollectBoundColumnNames(*child, column_names);
	}
}

static void QuoteBoundColumnRefs(Expression &expr, const BoundColumnNameMap &column_names) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &bcr = expr.Cast<BoundColumnRefExpression>();
		auto entry = column_names.find({bcr.binding.table_index, bcr.binding.column_index});
		if (entry != column_names.end()) {
			bcr.alias = entry->second;
		}
	}
	ExpressionIterator::EnumerateChildren(expr, [&](unique_ptr<Expression> &child) {
		if (child) {
			QuoteBoundColumnRefs(*child, column_names);
		}
	});
}

static string QuotedExpressionString(const unique_ptr<Expression> &expr, const BoundColumnNameMap &column_names) {
	auto copy = expr->Copy();
	QuoteBoundColumnRefs(*copy, column_names);
	return copy->ToString();
}

static string BuildDeltaInsertFromPlan(ClientContext &context, TableCatalogEntry &delta_entry,
                                       const string &full_delta_table_name, unique_ptr<LogicalOperator> &source_plan) {
	string prefix = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry);
	auto ast = LogicalPlanToAst(context, source_plan);
	auto cte_list = AstToCteList(*ast);
	string subquery_string = cte_list->ToQuery(false);
	if (!subquery_string.empty() && subquery_string.back() == ';') {
		subquery_string.pop_back();
	}
	return prefix + " SELECT *, 1, now()::timestamp FROM (" + subquery_string + ")";
}

static string BuildDeleteDeltaInsertFromPlan(ClientContext &context, TableCatalogEntry &delta_entry,
                                             const string &full_delta_table_name, const string &full_table_name,
                                             unique_ptr<LogicalOperator> &source_plan) {
	string prefix = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry);
	string data_cols = BuildDeltaDataColumns(delta_entry);
	auto ast = LogicalPlanToAst(context, source_plan);
	auto cte_list = AstToCteList(*ast);
	string subquery_string = cte_list->ToQuery(false);
	if (!subquery_string.empty() && subquery_string.back() == ';') {
		subquery_string.pop_back();
	}
	// DuckDB DELETE children identify physical rows by rowid; read the base table
	// columns back through that rowid set to materialize the negative delta tuple.
	return prefix + " SELECT " + data_cols + ", -1, now()::timestamp FROM " + full_table_name +
	       " WHERE rowid IN (SELECT rowid FROM (" + subquery_string + ") openivm_deleted_rows)";
}

RefreshInsertRule::RefreshInsertRule() {
	optimize_function = RefreshInsertRuleFunction;
	optimizer_info = make_shared_ptr<RefreshInsertOptimizerInfo>();
}

void RefreshInsertRule::RefreshInsertRuleFunction(OptimizerExtensionInput &input,
                                                  duckdb::unique_ptr<LogicalOperator> &plan) {
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

		auto view_check = con.Query("SELECT 1 FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
		                            SqlUtils::EscapeValue(table_name) + "'");
		if (!view_check->HasError() && view_check->RowCount() > 0) {
			// Acquire view lock to prevent cleanup during an in-flight refresh
			ViewLockGuard view_guard(table_name);
			OPENIVM_DEBUG_PRINT("[INSERT RULE] DROP TABLE '%s' — cleaning up IVM metadata\n", table_name.c_str());
			RefreshMetadata metadata(con);
			auto delta_tables = metadata.GetDeltaTables(table_name);

			con.Query("DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
			          SqlUtils::EscapeValue(table_name) + "'");
			con.Query("DELETE FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
			          SqlUtils::EscapeValue(table_name) + "'");
			con.Query("DROP TABLE IF EXISTS " + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(table_name)));
			con.Query("DROP TABLE IF EXISTS " +
			          KeywordHelper::WriteOptionallyQuoted(IncrementalTableNames::DataTableName(table_name)));

			for (auto &dt : delta_tables) {
				// DuckLake entries store the base table name — never drop it
				if (metadata.IsDuckLakeTable(table_name, dt)) {
					continue;
				}
				auto remaining = con.Query("SELECT count(*) FROM " + string(openivm::DELTA_TABLES_TABLE) +
				                           " WHERE table_name = '" + SqlUtils::EscapeValue(dt) + "'");
				if (!remaining->HasError() && remaining->RowCount() > 0 &&
				    remaining->GetValue(0, 0).GetValue<int64_t>() == 0) {
					con.Query("DROP TABLE IF EXISTS " + KeywordHelper::WriteOptionallyQuoted(dt));
				}
			}
		}

		// Handle CASCADE: drop dependent MVs
		auto dep_check =
		    con.Query("SELECT DISTINCT view_name FROM " + string(openivm::DELTA_TABLES_TABLE) +
		              " WHERE table_name = '" + SqlUtils::EscapeValue(SqlUtils::DeltaName(table_name)) + "'");
		if (!dep_check->HasError() && dep_check->RowCount() > 0 && drop_info->cascade) {
			for (size_t i = 0; i < dep_check->RowCount(); i++) {
				auto dep_view = dep_check->GetValue(0, i).ToString();
				// Lock each dependent view before dropping
				ViewLockGuard view_guard(dep_view);
				RefreshMetadata dep_metadata(con);
				auto dep_delta_tables = dep_metadata.GetDeltaTables(dep_view);

				con.Query("DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
				          SqlUtils::EscapeValue(dep_view) + "'");
				con.Query("DELETE FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
				          SqlUtils::EscapeValue(dep_view) + "'");
				con.Query("DROP TABLE IF EXISTS " +
				          KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(dep_view)));
				con.Query("DROP TABLE IF EXISTS " +
				          KeywordHelper::WriteOptionallyQuoted(IncrementalTableNames::DataTableName(dep_view)));
				con.Query("DROP VIEW IF EXISTS " + KeywordHelper::WriteOptionallyQuoted(dep_view));

				for (auto &dt : dep_delta_tables) {
					auto remaining = con.Query("SELECT count(*) FROM " + string(openivm::DELTA_TABLES_TABLE) +
					                           " WHERE table_name = '" + SqlUtils::EscapeValue(dt) + "'");
					if (!remaining->HasError() && remaining->RowCount() > 0 &&
					    remaining->GetValue(0, 0).GetValue<int64_t>() == 0) {
						con.Query("DROP TABLE IF EXISTS " + KeywordHelper::WriteOptionallyQuoted(dt));
					}
				}
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
		string delta_name = SqlUtils::DeltaName(table_name);
		string qdelta = KeywordHelper::WriteOptionallyQuoted(delta_name);

		Connection con(*input.context.db);
		// Check if a delta table exists for this base table (i.e., it's tracked by IVM)
		auto delta_check = con.Query("SELECT 1 FROM information_schema.tables WHERE table_name = '" +
		                             SqlUtils::EscapeValue(delta_name) + "'");
		if (delta_check->HasError() || delta_check->RowCount() == 0) {
			return; // not an IVM-tracked table
		}

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
			string referencing_mv = FirstMVReferencingColumn(con, delta_name, table_name, col_name);
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
			RewriteDependentViewMetadataForRename(con, delta_name, table_name, old_name, new_name);
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

		if (SqlUtils::IsDelta(insert_table_name) || insert_table_name.empty() ||
		    IncrementalTableNames::IsDataTable(insert_table_name)) {
			return;
		}
		// DuckLake tables have native change tracking — no delta writes needed
		if (insert_node->table.catalog.GetCatalogType() == "ducklake") {
			OPENIVM_DEBUG_PRINT("[INSERT RULE] Skipping delta for DuckLake table '%s'\n", insert_table_name.c_str());
			return;
		}
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, insert_node->table.catalog.GetName(), insert_node->table.schema.name,
		    SqlUtils::DeltaName(insert_table_name), OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			Connection con(*input.context.db);
			DisablePACIfLoaded(input.context, con);
			RefreshMetadata metadata(con);
			if (metadata.IsBaseTable(insert_table_name)) {
				string full_delta_table_name = SqlUtils::FullDeltaName(
				    insert_node->table.catalog.GetName(), insert_node->table.schema.name, insert_node->table.name);
				if (insert_node->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION) {
					auto &delta_entry_ins = delta_table_catalog_entry->Cast<TableCatalogEntry>();
					string insert_query = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_ins);

					auto projection = dynamic_cast<LogicalProjection *>(insert_node->children[0].get());
					if (projection->children[0]->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET) {
						insert_query += " VALUES ";
						auto expression_get = dynamic_cast<LogicalExpressionGet *>(projection->children[0].get());
						bool all_values_are_constants = true;
						for (auto &expression : expression_get->expressions) {
							for (auto &value : expression) {
								if (value->type != ExpressionType::VALUE_CONSTANT) {
									all_values_are_constants = false;
									break;
								}
							}
							if (!all_values_are_constants) {
								break;
							}
						}
						if (!all_values_are_constants) {
							// DuckDB may bind VALUES literals through casts or other scalar expressions. Serialize the
							// planned insert source instead of rejecting an otherwise valid base-table write.
							insert_query = BuildDeltaInsertFromPlan(*con.context, delta_entry_ins,
							                                        full_delta_table_name, insert_node->children[0]);
						} else {
							for (auto &expression : expression_get->expressions) {
								string values = "(";
								for (auto &value : expression) {
									auto constant = dynamic_cast<BoundConstantExpression *>(value.get());
									values += constant->value.ToSQLString() + ",";
								}
								values += "1, now()::timestamp),";
								insert_query += values;
							}
							insert_query.pop_back();
						}
					} else {
						auto &delta_entry = delta_table_catalog_entry->Cast<TableCatalogEntry>();
						insert_query = BuildDeltaInsertFromPlan(*con.context, delta_entry, full_delta_table_name,
						                                        insert_node->children[0]);
					}
					OPENIVM_DEBUG_PRINT("[INSERT RULE] insert_query: %s\n", insert_query.c_str());
					{
						DeltaLockGuard guard(SqlUtils::DeltaName(insert_table_name));
						auto r = con.Query(insert_query);
						if (r->HasError()) {
							throw Exception(ExceptionType::EXECUTOR,
							                "Cannot insert in delta table after insertion! " + r->GetError());
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
						auto query = prefix_csv + " SELECT *, 1, now()::timestamp FROM read_csv('" + file.path + "');";
						DeltaLockGuard guard(SqlUtils::DeltaName(insert_table_name));
						auto r = con.Query(query);
						if (r->HasError()) {
							throw Exception(ExceptionType::EXECUTOR, "Cannot insert in delta table! " + r->GetError());
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
		if (SqlUtils::IsDelta(delete_table_name) || IncrementalTableNames::IsDataTable(delete_table_name)) {
			return;
		}
		if (delete_node->table.catalog.GetCatalogType() == "ducklake") {
			OPENIVM_DEBUG_PRINT("[INSERT RULE] Skipping delta for DuckLake table '%s'\n", delete_table_name.c_str());
			return;
		}
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, delete_node->table.catalog.GetName(), delete_node->table.schema.name,
		    SqlUtils::DeltaName(delete_table_name), OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			auto full_table_name = SqlUtils::FullName(delete_node->table.catalog.GetName(),
			                                          delete_node->table.schema.name, delete_node->table.name);
			auto full_delta_table_name = SqlUtils::FullDeltaName(
			    delete_node->table.catalog.GetName(), delete_node->table.schema.name, delete_node->table.name);
			Connection con(*input.context.db);
			DisablePACIfLoaded(input.context, con);
			RefreshMetadata metadata(con);
			if (metadata.IsBaseTable(delete_table_name)) {
				auto &delta_entry_del = delta_table_catalog_entry->Cast<TableCatalogEntry>();
				string insert_string = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_del) + " " +
				                       BuildDeltaSelectFrom(delta_entry_del, "-1", full_table_name);
				if (plan->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
					auto filter = dynamic_cast<LogicalFilter *>(plan->children[0].get());
					bool has_subquery = false;
					for (auto &expr : filter->expressions) {
						has_subquery = has_subquery || expr->HasSubquery();
					}
					if (has_subquery) {
						insert_string = BuildDeleteDeltaInsertFromPlan(
						    *con.context, delta_entry_del, full_delta_table_name, full_table_name, plan->children[0]);
					} else {
						BoundColumnNameMap column_names;
						CollectBoundColumnNames(*filter->children[0], column_names);
						insert_string += " where ";
						for (idx_t i = 0; i < filter->expressions.size(); i++) {
							if (i > 0) {
								insert_string += " AND ";
							}
							insert_string += QuotedExpressionString(filter->expressions[i], column_names);
						}
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
							col_name = KeywordHelper::WriteOptionallyQuoted(col_name);
							insert_string += entry.second->ToString(col_name);
						}
					}
				} else if (plan->children[0]->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT) {
					return;
				} else if (plan->children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
					try {
						insert_string = BuildDeleteDeltaInsertFromPlan(
						    *con.context, delta_entry_del, full_delta_table_name, full_table_name, plan->children[0]);
					} catch (...) {
						throw NotImplementedException(
						    "DELETE with complex subqueries is not yet fully supported for IVM delta tracking");
					}
				} else {
					try {
						string prefix_del = BuildDeltaInsertPrefix(full_delta_table_name, delta_entry_del);
						auto ast = LogicalPlanToAst(*con.context, plan->children[0]);
						auto cte_list = AstToCteList(*ast);
						string subquery_string = cte_list->ToQuery(false);
						if (!subquery_string.empty() && subquery_string.back() == ';') {
							subquery_string.pop_back();
						}
						insert_string = prefix_del + " SELECT *, -1, now()::timestamp FROM (" + subquery_string + ")";
					} catch (...) {
						throw NotImplementedException(
						    "DELETE with complex subqueries is not yet fully supported for IVM delta tracking");
					}
				}

				{
					DeltaLockGuard guard(SqlUtils::DeltaName(delete_table_name));
					auto r = con.Query(insert_string);
					if (r->HasError()) {
						throw Exception(ExceptionType::EXECUTOR,
						                "Cannot insert in delta table after deletion! " + r->GetError());
					}
				}
			}
		}
	} break;

	case LogicalOperatorType::LOGICAL_UPDATE: {
		auto update_node = dynamic_cast<LogicalUpdate *>(root);
		auto update_table_name = update_node->table.name;
		if (SqlUtils::IsDelta(update_table_name) || IncrementalTableNames::IsDataTable(update_table_name)) {
			return;
		}
		if (update_node->table.catalog.GetCatalogType() == "ducklake") {
			OPENIVM_DEBUG_PRINT("[INSERT RULE] Skipping delta for DuckLake table '%s'\n", update_table_name.c_str());
			return;
		}
		auto delta_table_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    input.context, update_node->table.catalog.GetName(), update_node->table.schema.name,
		    SqlUtils::DeltaName(update_table_name), OnEntryNotFound::RETURN_NULL);

		if (delta_table_catalog_entry) {
			Connection con(*input.context.db);
			DisablePACIfLoaded(input.context, con);
			RefreshMetadata metadata(con);
			if (!metadata.IsBaseTable(update_table_name)) {
				break;
			}
			{
				auto full_table_name = SqlUtils::FullName(update_node->table.catalog.GetName(),
				                                          update_node->table.schema.name, update_node->table.name);
				auto full_delta_table_name = SqlUtils::FullDeltaName(
				    update_node->table.catalog.GetName(), update_node->table.schema.name, update_node->table.name);
				auto *projection = dynamic_cast<LogicalProjection *>(update_node->children[0].get());
				if (!projection) {
					OPENIVM_DEBUG_PRINT("[INSERT RULE] UPDATE skipped: no projection child (child type: %s)\n",
					                    LogicalOperatorToString(update_node->children[0]->type).c_str());
					break;
				}

				std::map<string, string> update_values;
				string where_string;
				BoundColumnNameMap column_names;
				CollectBoundColumnNames(*projection->children[0], column_names);
				for (size_t i = 0; i < update_node->columns.size(); i++) {
					auto column = update_node->columns[i].index;
					update_values[to_string(column)] = QuotedExpressionString(projection->expressions[i], column_names);
				}

				if (projection->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
					auto filter = dynamic_cast<LogicalFilter *>(projection->children[0].get());
					where_string += " where ";
					for (idx_t i = 0; i < filter->expressions.size(); i++) {
						if (i > 0) {
							where_string += " AND ";
						}
						where_string += QuotedExpressionString(filter->expressions[i], column_names);
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
							col_name = KeywordHelper::WriteOptionallyQuoted(col_name);
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
				string select_old = BuildDeltaSelectFrom(delta_entry_upd, "-1", full_table_name) + where_string;
				// For select_new: use the update_values map to replace modified columns
				string select_new = "SELECT ";
				for (auto &col : delta_entry_upd.GetColumns().Logical()) {
					if (col.GetName() == openivm::MULTIPLICITY_COL || col.GetName() == openivm::TIMESTAMP_COL) {
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
				select_new += "1, now()::timestamp FROM " + full_table_name + where_string;

				{
					DeltaLockGuard guard(SqlUtils::DeltaName(update_table_name));
					// ATOMIC UPDATE DELTA WRITES:
					// The old-delete and new-insert rows MUST commit together. If they land
					// in separate auto-commit transactions, a concurrent refresh can take a
					// snapshot between them — seeing one but not the other. Since both rows
					// target the same group, processing only one breaks consolidation (net
					// count change should be 0, but ends up +1 or -1 → MV drift).
					//
					// Combining into a single multi-row INSERT via UNION ALL ensures both
					// rows share one commit point and one `now()` value — either both visible
					// in a given snapshot or neither.
					string combined = prefix_upd + " " + "SELECT * FROM (" + select_old +
					                  ") UNION ALL SELECT * FROM (" + select_new + ")";
					OPENIVM_DEBUG_PRINT("[INSERT RULE] combined UPDATE delta: %s\n", combined.c_str());
					auto r = con.Query(combined);
					if (r->HasError()) {
						throw Exception(ExceptionType::EXECUTOR, "Cannot insert UPDATE delta rows! " + r->GetError());
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
