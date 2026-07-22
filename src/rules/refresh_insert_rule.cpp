#include "rules/refresh_insert_rule.hpp"
#include "compile_facts.hpp"
#include "rules/schema_evolution.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/refresh_locks.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "rules/transactional_delta_capture.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/planner/operator/logical_update.hpp"

namespace duckdb {

static optional_ptr<TableCatalogEntry> TryGetTrackedDeltaTable(ClientContext &context, TableCatalogEntry &table) {
	const auto &table_name = table.name;
	if (table_name.empty() || SqlUtils::IsDelta(table_name) || IncrementalTableNames::IsDataTable(table_name) ||
	    table.catalog.GetCatalogType() == "ducklake") {
		return nullptr;
	}
	auto delta_table =
	    Catalog::GetEntry<TableCatalogEntry>(context, table.catalog.GetName(), table.schema.name,
	                                         SqlUtils::DeltaName(table_name), OnEntryNotFound::RETURN_NULL);
	if (!delta_table) {
		return nullptr;
	}
	return &delta_table->Cast<TableCatalogEntry>();
}

static void ResolveInsertDefaults(OptimizerExtensionInput &input, LogicalInsert &insert) {
	if (insert.column_index_map.empty()) {
		return;
	}
	auto child_bindings = insert.children[0]->GetColumnBindings();
	vector<unique_ptr<Expression>> expressions;
	for (auto &column : insert.table.GetColumns().Physical()) {
		auto mapped_index = insert.column_index_map[column.Physical()];
		if (mapped_index == DConstants::INVALID_INDEX) {
			expressions.push_back(insert.bound_defaults[column.StorageOid()]->Copy());
		} else {
			if (mapped_index >= child_bindings.size()) {
				throw InternalException("OpenIVM insert column mapping is out of range");
			}
			expressions.push_back(make_uniq<BoundColumnRefExpression>(column.Type(), child_bindings[mapped_index]));
		}
	}
	auto projection = make_uniq<LogicalProjection>(input.optimizer.binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(insert.children[0]));
	insert.children[0] = std::move(projection);
	insert.column_index_map = physical_index_vector_t<idx_t>();
	insert.expected_types = insert.table.GetTypes();
}

static idx_t FindExpressionBindingIndex(LogicalOperator &child, const Expression &expression) {
	if (expression.type != ExpressionType::BOUND_COLUMN_REF) {
		throw InternalException("OpenIVM expected a bound row-id column reference");
	}
	auto &column_ref = expression.Cast<BoundColumnRefExpression>();
	auto bindings = child.GetColumnBindings();
	for (idx_t index = 0; index < bindings.size(); index++) {
		if (bindings[index] == column_ref.binding) {
			return index;
		}
	}
	throw InternalException("OpenIVM could not resolve the DML row-id binding");
}

static void ResolveUpdateDefaults(OptimizerExtensionInput &input, LogicalUpdate &update) {
	bool has_default = false;
	for (auto &expression : update.expressions) {
		has_default = has_default || expression->type == ExpressionType::VALUE_DEFAULT;
	}
	if (!has_default) {
		return;
	}

	auto child_bindings = update.children[0]->GetColumnBindings();
	auto child_types = update.children[0]->types;
	if (child_bindings.empty() || child_bindings.size() != child_types.size()) {
		throw InternalException("OpenIVM cannot normalize UPDATE defaults without a row-id input");
	}

	auto projection_index = input.optimizer.binder.GenerateTableIndex();
	vector<unique_ptr<Expression>> projection_expressions;
	projection_expressions.reserve(child_bindings.size() + update.expressions.size());
	for (idx_t index = 0; index + 1 < child_bindings.size(); index++) {
		projection_expressions.push_back(
		    make_uniq<BoundColumnRefExpression>(child_types[index], child_bindings[index]));
	}

	vector<idx_t> default_indexes(update.expressions.size(), DConstants::INVALID_INDEX);
	for (idx_t index = 0; index < update.expressions.size(); index++) {
		if (update.expressions[index]->type != ExpressionType::VALUE_DEFAULT) {
			continue;
		}
		default_indexes[index] = projection_expressions.size();
		projection_expressions.push_back(update.bound_defaults[update.columns[index].index]->Copy());
	}

	const auto row_id_output_index = projection_expressions.size();
	projection_expressions.push_back(make_uniq<BoundColumnRefExpression>(child_types.back(), child_bindings.back()));
	for (idx_t index = 0; index < update.expressions.size(); index++) {
		auto return_type = update.expressions[index]->return_type;
		idx_t output_index;
		if (default_indexes[index] != DConstants::INVALID_INDEX) {
			output_index = default_indexes[index];
		} else {
			output_index = FindExpressionBindingIndex(*update.children[0], *update.expressions[index]);
			D_ASSERT(output_index + 1 < child_bindings.size());
		}
		update.expressions[index] =
		    make_uniq<BoundColumnRefExpression>(return_type, ColumnBinding(projection_index, output_index));
	}

	auto projection = make_uniq<LogicalProjection>(projection_index, std::move(projection_expressions));
	projection->children.push_back(std::move(update.children[0]));
	update.children[0] = std::move(projection);
	D_ASSERT(update.children[0]->GetColumnBindings().size() == row_id_output_index + 1);
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

	auto dml = root;
	while (dml->type != LogicalOperatorType::LOGICAL_INSERT && dml->type != LogicalOperatorType::LOGICAL_DELETE &&
	       dml->type != LogicalOperatorType::LOGICAL_UPDATE) {
		if (dml->children.size() != 1) {
			return;
		}
		dml = dml->children[0].get();
	}

	switch (dml->type) {
	case LogicalOperatorType::LOGICAL_INSERT: {
		auto &insert = dml->Cast<LogicalInsert>();
		const auto &table_name = insert.table.name;
		auto delta_table = TryGetTrackedDeltaTable(input.context, insert.table);
		if (!delta_table) {
			return;
		}
		ResolveInsertDefaults(input, insert);
		auto capture =
		    make_uniq<LogicalTransactionalDeltaCapture>(insert.table, *delta_table, DeltaCaptureMode::INSERT);
		capture->children.push_back(std::move(insert.children[0]));
		insert.children[0] = std::move(capture);
		OPENIVM_DEBUG_PRINT("[INSERT RULE] transactional INSERT delta capture for '%s'\n", table_name.c_str());
		break;
	}
	case LogicalOperatorType::LOGICAL_DELETE: {
		auto &delete_op = dml->Cast<LogicalDelete>();
		const auto &table_name = delete_op.table.name;
		auto delta_table = TryGetTrackedDeltaTable(input.context, delete_op.table);
		if (!delta_table) {
			return;
		}
		D_ASSERT(delete_op.expressions.size() == 1);
		auto row_id_index = FindExpressionBindingIndex(*delete_op.children[0], *delete_op.expressions[0]);
		auto capture = make_uniq<LogicalTransactionalDeltaCapture>(
		    delete_op.table, *delta_table, DeltaCaptureMode::DELETE, vector<unique_ptr<Expression>> {},
		    vector<PhysicalIndex> {}, optional_idx(row_id_index));
		capture->children.push_back(std::move(delete_op.children[0]));
		delete_op.children[0] = std::move(capture);
		OPENIVM_DEBUG_PRINT("[INSERT RULE] transactional DELETE delta capture for '%s'\n", table_name.c_str());
		break;
	}
	case LogicalOperatorType::LOGICAL_UPDATE: {
		auto &update = dml->Cast<LogicalUpdate>();
		const auto &table_name = update.table.name;
		auto delta_table = TryGetTrackedDeltaTable(input.context, update.table);
		if (!delta_table) {
			return;
		}
		ResolveUpdateDefaults(input, update);
		vector<unique_ptr<Expression>> update_expressions;
		update_expressions.reserve(update.expressions.size());
		for (idx_t index = 0; index < update.expressions.size(); index++) {
			update_expressions.push_back(update.expressions[index]->Copy());
		}
		auto row_id_index = update.children[0]->GetColumnBindings().size() - 1;
		auto capture = make_uniq<LogicalTransactionalDeltaCapture>(update.table, *delta_table, DeltaCaptureMode::UPDATE,
		                                                           std::move(update_expressions), update.columns,
		                                                           optional_idx(row_id_index));
		capture->children.push_back(std::move(update.children[0]));
		update.children[0] = std::move(capture);
		OPENIVM_DEBUG_PRINT("[INSERT RULE] transactional UPDATE delta capture for '%s'\n", table_name.c_str());
		break;
	}
	default:
		return;
	}
}
} // namespace duckdb
