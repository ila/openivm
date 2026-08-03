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
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_delete.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_merge_into.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/planner/operator/logical_update.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

class TransactionalHelperUndoState : public ClientContextState {
public:
	static TransactionalHelperUndoState &Get(ClientContext &context) {
		auto state =
		    context.registered_state->GetOrCreate<TransactionalHelperUndoState>("openivm_transactional_helper_undo");
		if (!state->mutation_guard) {
			state->mutation_guard = make_uniq<MutationLockGuard>(context);
		}
		return *state;
	}

	void AddRestoreSQL(string sql) {
		restore_sql.push_back(std::move(sql));
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		Clear();
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		OPENIVM_DEBUG_PRINT("[TRANSACTIONAL DDL] restoring %zu metadata snapshots\n", restore_sql.size());
		try {
			Connection con(*context.db);
			auto schema_result = con.Query("SET schema='" + string(DEFAULT_SCHEMA) + "'");
			if (schema_result->HasError()) {
				OPENIVM_DEBUG_PRINT("[TRANSACTIONAL DDL] metadata restore setup failed: %s\n",
				                    schema_result->GetError().c_str());
			} else {
				for (auto it = restore_sql.rbegin(); it != restore_sql.rend(); ++it) {
					auto result = con.Query(*it);
					if (result->HasError()) {
						OPENIVM_DEBUG_PRINT("[TRANSACTIONAL DDL] metadata restore failed: %s\n",
						                    result->GetError().c_str());
					}
				}
			}
		} catch (std::exception &ex) {
			// Transaction callbacks must always release the mutation gate. A
			// helper-restore error is diagnostic here; the caller transaction has
			// already rolled back and cannot report a second failure safely.
			OPENIVM_DEBUG_PRINT("[TRANSACTIONAL DDL] metadata rollback callback failed: %s\n", ex.what());
		}
		Clear();
	}

private:
	void Clear() {
		restore_sql.clear();
		mutation_guard.reset();
	}

	vector<string> restore_sql;
	unique_ptr<MutationLockGuard> mutation_guard;
};

static string BuildRestoreRowsSQL(MaterializedQueryResult &rows, const string &table_name) {
	if (rows.RowCount() == 0) {
		return "";
	}
	string columns;
	for (auto &name : rows.names) {
		if (!columns.empty()) {
			columns += ", ";
		}
		columns += SqlUtils::QuoteIdentifier(name);
	}
	string values;
	for (idx_t row = 0; row < rows.RowCount(); row++) {
		if (!values.empty()) {
			values += ", ";
		}
		values += "(";
		for (idx_t col = 0; col < rows.ColumnCount(); col++) {
			if (col > 0) {
				values += ", ";
			}
			values += rows.GetValue(col, row).ToSQLString();
		}
		values += ")";
	}
	return "INSERT OR REPLACE INTO " + SqlUtils::QuoteIdentifier(table_name) + " (" + columns + ") VALUES " + values;
}

static void RegisterMetadataRestore(ClientContext &context, Connection &con, const string &table_name,
                                    const string &predicate) {
	auto rows = con.Query("SELECT * FROM " + SqlUtils::QuoteIdentifier(table_name) + " WHERE " + predicate);
	if (rows->HasError()) {
		throw CatalogException("OpenIVM could not snapshot helper metadata: %s", rows->GetError());
	}
	auto restore = BuildRestoreRowsSQL(*rows, table_name);
	if (!restore.empty()) {
		TransactionalHelperUndoState::Get(context).AddRestoreSQL(std::move(restore));
	}
}

static void ExecuteHelperMetadataSQL(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw CatalogException("OpenIVM metadata update failed: %s", result->GetError());
	}
}

static void DropCatalogEntry(ClientContext &context, const string &catalog_name, const string &schema_name,
                             const string &entry_name, CatalogType type) {
	DropInfo info;
	info.type = type;
	info.catalog = catalog_name;
	info.schema = schema_name;
	info.name = entry_name;
	info.if_not_found = OnEntryNotFound::RETURN_NULL;
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	MetaTransaction::Get(context).ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::DROP_CATALOG_ENTRY);
	catalog.DropEntry(context, info);
}

static void DropQualifiedCatalogEntry(ClientContext &context, const string &qualified_name,
                                      const string &fallback_catalog, const string &fallback_schema, CatalogType type) {
	auto components = QualifiedName::ParseComponents(qualified_name);
	if (components.size() == 1) {
		DropCatalogEntry(context, fallback_catalog, fallback_schema, components[0], type);
	} else if (components.size() == 2) {
		DropCatalogEntry(context, fallback_catalog, components[0], components[1], type);
	} else if (components.size() == 3) {
		DropCatalogEntry(context, components[0], components[1], components[2], type);
	} else {
		throw InternalException("OpenIVM could not resolve internal relation '%s'", qualified_name);
	}
}

static void AlterDeltaInCallerTransaction(ClientContext &context, AlterTableInfo &source_alter,
                                          const string &catalog_name, const string &schema_name,
                                          const string &delta_name) {
	auto delta_alter = source_alter.Copy();
	delta_alter->catalog = catalog_name;
	delta_alter->schema = schema_name;
	delta_alter->name = delta_name;
	auto &catalog = Catalog::GetCatalog(context, catalog_name);
	MetaTransaction::Get(context).ModifyDatabase(catalog.GetAttached(), DatabaseModificationType::ALTER_TABLE);
	catalog.Alter(context, *delta_alter);
}

static pair<string, string> ResolveDDLLocus(ClientContext &context, const string &catalog_name,
                                            const string &schema_name) {
	auto &default_entry = ClientData::Get(context).catalog_search_path->GetDefault();
	string default_catalog =
	    default_entry.catalog.empty() ? DatabaseManager::GetDefaultDatabase(context) : default_entry.catalog;
	return {
	    catalog_name.empty() ? default_catalog : catalog_name,
	    schema_name.empty() ? (default_entry.schema.empty() ? DEFAULT_SCHEMA : default_entry.schema) : schema_name,
	};
}

static bool SameRelationLocus(const string &left_catalog, const string &left_schema, const string &right_catalog,
                              const string &right_schema) {
	return StringUtil::CIEquals(left_catalog, right_catalog) && StringUtil::CIEquals(left_schema, right_schema);
}

static string MVInternalPrefix(ClientContext &context, const RefreshMetadata::StoredViewLocation &location,
                               const string &view_name) {
	QueryErrorContext error_context;
	auto entry = Catalog::GetEntry(context, location.catalog_name, location.schema_name,
	                               EntryLookupInfo(CatalogType::VIEW_ENTRY, view_name, error_context),
	                               OnEntryNotFound::RETURN_NULL);
	if (entry) {
		auto data_table = IncrementalTableNames::DataTableName(view_name);
		auto &view = dynamic_cast<ViewCatalogEntry &>(*entry);
		auto data_ref = SqlUtils::FindTableReference(view.sql, data_table);
		auto separator = data_ref.rfind('.');
		if (separator != string::npos) {
			return data_ref.substr(0, separator + 1);
		}
	}
	return SqlUtils::QualifiedPrefix(location.catalog_name, location.schema_name);
}

static void DropTrackedMaterializedView(ClientContext &context, Connection &con, RefreshMetadata &metadata,
                                        const string &view_name, bool drop_user_view) {
	auto location = metadata.GetStoredViewLocation(view_name);
	auto delta_sources = metadata.GetDeltaSources(view_name, location.catalog_name, location.schema_name);
	auto internal_prefix = MVInternalPrefix(context, location, view_name);
	auto escaped_view_name = SqlUtils::EscapeValue(view_name);
	auto view_predicate = "view_name = '" + escaped_view_name + "'";
	RegisterMetadataRestore(context, con, openivm::VIEWS_TABLE, view_predicate);
	RegisterMetadataRestore(context, con, openivm::DELTA_TABLES_TABLE, view_predicate);
	auto dependency_predicate = "parent_view = '" + escaped_view_name + "' OR child_view = '" + escaped_view_name + "'";
	RegisterMetadataRestore(context, con, openivm::MV_DEPS_TABLE, dependency_predicate);
	ExecuteHelperMetadataSQL(con, "DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                                  escaped_view_name + "'");
	ExecuteHelperMetadataSQL(con, "DELETE FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                                  escaped_view_name + "'");
	ExecuteHelperMetadataSQL(con, "DELETE FROM " + string(openivm::MV_DEPS_TABLE) + " WHERE " + dependency_predicate);
	if (drop_user_view) {
		DropCatalogEntry(context, location.catalog_name, location.schema_name, view_name, CatalogType::VIEW_ENTRY);
	}
	DropQualifiedCatalogEntry(context,
	                          internal_prefix + KeywordHelper::WriteOptionallyQuoted(SqlUtils::DeltaName(view_name)),
	                          location.catalog_name, location.schema_name, CatalogType::TABLE_ENTRY);
	DropQualifiedCatalogEntry(context,
	                          internal_prefix +
	                              KeywordHelper::WriteOptionallyQuoted(IncrementalTableNames::DataTableName(view_name)),
	                          location.catalog_name, location.schema_name, CatalogType::TABLE_ENTRY);

	for (auto &source : delta_sources) {
		// DuckLake entries store the base table name — never drop it.
		if (source.catalog_type == "ducklake") {
			continue;
		}
		auto remaining = con.Query("SELECT count(*) FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                           " WHERE table_name = '" + SqlUtils::EscapeValue(source.table_name) +
		                           "' AND COALESCE(source_catalog, '" + SqlUtils::EscapeValue(source.catalog_name) +
		                           "') = '" + SqlUtils::EscapeValue(source.catalog_name) +
		                           "' AND COALESCE(source_schema, '" + SqlUtils::EscapeValue(source.schema_name) +
		                           "') = '" + SqlUtils::EscapeValue(source.schema_name) + "'");
		if (!remaining->HasError() && remaining->RowCount() > 0 && remaining->GetValue(0, 0).GetValue<int64_t>() == 0) {
			DropCatalogEntry(context, source.catalog_name, source.schema_name, source.table_name,
			                 CatalogType::TABLE_ENTRY);
		}
	}
}

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
		auto target_locus = ResolveDDLLocus(input.context, drop_info->catalog, drop_info->schema);
		Connection con(*input.context.db);

		auto view_check = con.Query("SELECT 1 FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
		                            SqlUtils::EscapeValue(table_name) + "'");
		if (!view_check->HasError() && view_check->RowCount() > 0) {
			RefreshMetadata metadata(con);
			auto location = metadata.GetStoredViewLocation(table_name);
			if (SameRelationLocus(location.catalog_name, location.schema_name, target_locus.first,
			                      target_locus.second)) {
				TransactionalMVLockState::Get(input.context).AcquireMutationLock();
				OPENIVM_DEBUG_PRINT("[INSERT RULE] DROP TABLE '%s' — cleaning up IVM metadata\n", table_name.c_str());
				bool drop_user_view = drop_info->type == CatalogType::VIEW_ENTRY;
				DropTrackedMaterializedView(input.context, con, metadata, table_name, drop_user_view);
				if (drop_user_view) {
					// The original logical DROP remains as an idempotent no-op.
					drop_info->if_not_found = OnEntryNotFound::RETURN_NULL;
				}
			}
		}

		// Handle CASCADE: drop dependent MVs
		auto dep_check = con.Query("SELECT DISTINCT view_name FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                           " WHERE table_name = '" + SqlUtils::EscapeValue(SqlUtils::DeltaName(table_name)) +
		                           "' AND COALESCE(source_catalog, '" + SqlUtils::EscapeValue(target_locus.first) +
		                           "') = '" + SqlUtils::EscapeValue(target_locus.first) +
		                           "' AND COALESCE(source_schema, '" + SqlUtils::EscapeValue(target_locus.second) +
		                           "') = '" + SqlUtils::EscapeValue(target_locus.second) + "'");
		if (!dep_check->HasError() && dep_check->RowCount() > 0 && drop_info->cascade) {
			TransactionalMVLockState::Get(input.context).AcquireMutationLock();
			RefreshMetadata cascade_metadata(con);
			vector<string> dependent_views;
			unordered_set<string> seen_dependents;
			for (size_t i = 0; i < dep_check->RowCount(); i++) {
				auto direct_view = dep_check->GetValue(0, i).ToString();
				auto downstream = cascade_metadata.GetDownstreamViews(direct_view);
				for (auto it = downstream.rbegin(); it != downstream.rend(); ++it) {
					if (seen_dependents.insert(*it).second) {
						dependent_views.push_back(*it);
					}
				}
				if (seen_dependents.insert(direct_view).second) {
					dependent_views.push_back(std::move(direct_view));
				}
			}
			for (auto &dep_view : dependent_views) {
				DropTrackedMaterializedView(input.context, con, cascade_metadata, dep_view, true);
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
		auto source_locus = ResolveDDLLocus(input.context, alter_info->catalog, alter_info->schema);

		Connection con(*input.context.db);
		// Check if a delta table exists for this base table (i.e., it's tracked by IVM)
		auto delta_check = con.Query("SELECT 1 FROM information_schema.tables WHERE table_catalog = '" +
		                             SqlUtils::EscapeValue(source_locus.first) + "' AND table_schema = '" +
		                             SqlUtils::EscapeValue(source_locus.second) + "' AND table_name = '" +
		                             SqlUtils::EscapeValue(delta_name) + "'");
		if (delta_check->HasError() || delta_check->RowCount() == 0) {
			return; // not an IVM-tracked table
		}
		TransactionalMVLockState::Get(input.context).AcquireMutationLock();

		switch (alter_info->alter_table_type) {
		case AlterTableType::ADD_COLUMN: {
			auto *add_info = dynamic_cast<AddColumnInfo *>(alter_info);
			if (!add_info) {
				break;
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE ADD COLUMN '%s' — syncing delta table\n",
			                    add_info->new_column.Name().c_str());
			AlterDeltaInCallerTransaction(input.context, *alter_info, source_locus.first, source_locus.second,
			                              delta_name);
			break;
		}
		case AlterTableType::REMOVE_COLUMN: {
			auto *remove_info = dynamic_cast<RemoveColumnInfo *>(alter_info);
			if (!remove_info) {
				break;
			}
			string col_name = remove_info->removed_column;
			string referencing_mv = FirstMVReferencingColumn(con, delta_name, source_locus.first, source_locus.second,
			                                                 table_name, col_name);
			if (!referencing_mv.empty()) {
				throw CatalogException("Cannot drop column '" + col_name +
				                       "': it is referenced by materialized view '" + referencing_mv +
				                       "'. Drop the view first.");
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE DROP COLUMN '%s' — syncing delta table\n", col_name.c_str());
			AlterDeltaInCallerTransaction(input.context, *alter_info, source_locus.first, source_locus.second,
			                              delta_name);
			break;
		}
		case AlterTableType::RENAME_COLUMN: {
			auto *rename_info = dynamic_cast<RenameColumnInfo *>(alter_info);
			if (!rename_info) {
				break;
			}
			string old_name = rename_info->old_name;
			string new_name = rename_info->new_name;
			auto dependent_view_predicate =
			    "view_name IN (SELECT view_name FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE table_name = '" +
			    SqlUtils::EscapeValue(delta_name) + "' AND COALESCE(source_catalog, '" +
			    SqlUtils::EscapeValue(source_locus.first) + "') = '" + SqlUtils::EscapeValue(source_locus.first) +
			    "' AND COALESCE(source_schema, '" + SqlUtils::EscapeValue(source_locus.second) + "') = '" +
			    SqlUtils::EscapeValue(source_locus.second) + "')";
			RegisterMetadataRestore(input.context, con, openivm::VIEWS_TABLE, dependent_view_predicate);
			RewriteDependentViewMetadataForRename(con, delta_name, source_locus.first, source_locus.second, table_name,
			                                      old_name, new_name);
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE RENAME COLUMN '%s' → '%s' — syncing delta table\n",
			                    old_name.c_str(), new_name.c_str());
			AlterDeltaInCallerTransaction(input.context, *alter_info, source_locus.first, source_locus.second,
			                              delta_name);
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

	auto dml_owner = &plan;
	auto dml = dml_owner->get();
	while (dml->type != LogicalOperatorType::LOGICAL_INSERT && dml->type != LogicalOperatorType::LOGICAL_DELETE &&
	       dml->type != LogicalOperatorType::LOGICAL_UPDATE && dml->type != LogicalOperatorType::LOGICAL_MERGE_INTO) {
		if (dml->children.size() != 1) {
			return;
		}
		dml_owner = &dml->children[0];
		dml = dml_owner->get();
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
	case LogicalOperatorType::LOGICAL_MERGE_INTO: {
		auto &merge = dml->Cast<LogicalMergeInto>();
		const auto &table_name = merge.table.name;
		auto delta_table = TryGetTrackedDeltaTable(input.context, merge.table);
		if (!delta_table) {
			return;
		}
		auto capture = make_uniq<LogicalTransactionalMergeDeltaCapture>(merge.table, *delta_table);
		capture->children.push_back(std::move(*dml_owner));
		*dml_owner = std::move(capture);
		OPENIVM_DEBUG_PRINT("[INSERT RULE] transactional MERGE action delta capture for '%s'\n", table_name.c_str());
		break;
	}
	default:
		return;
	}
}
} // namespace duckdb
