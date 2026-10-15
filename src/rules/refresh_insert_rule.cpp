#include "rules/refresh_insert_rule.hpp"
#include "compile_facts.hpp"
#include "rules/schema_evolution.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_ddl.hpp"
#include "core/refresh_metadata.hpp"
#include "core/refresh_locks.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"
#include "rules/transactional_delta_capture.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/enums/database_modification_type.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
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
			TransactionalMVLockState::Get(*con.context)
			    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
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
	return "INSERT OR REPLACE INTO " + table_name + " (" + columns + ") VALUES " + values;
}

static void RegisterMetadataRestore(ClientContext &context, Connection &con, const string &table_name,
                                    const string &predicate) {
	auto location = RefreshMetadata::CurrentLocation(con);
	auto target = SqlUtils::FullName(location.catalog_name, location.schema_name, table_name);
	auto rows = con.Query("SELECT * FROM " + target + " WHERE " + predicate);
	if (rows->HasError()) {
		throw CatalogException("OpenIVM could not snapshot helper metadata: %s", rows->GetError());
	}
	auto restore = BuildRestoreRowsSQL(*rows, target);
	if (!restore.empty()) {
		TransactionalHelperUndoState::Get(context).AddRestoreSQL(std::move(restore));
	}
}

static void SnapshotSchemaEvolutionMetadata(ClientContext &context, Connection &con,
                                            const RefreshMetadata::MetadataLocation &location) {
	TransactionalMVLockState::Get(*con.context)
	    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
	RefreshMetadata::UseLocation(con, location);
	RefreshMetadata(con).SnapshotTransaction(context);
	if (auto state = TransactionalMVMetadataState::TryGet(context)) {
		state->Apply(con);
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

static optional_ptr<TableCatalogEntry> GetNativeDMLTable(LogicalOperator &dml) {
	optional_ptr<TableCatalogEntry> table;
	switch (dml.type) {
	case LogicalOperatorType::LOGICAL_INSERT:
		table = &dml.Cast<LogicalInsert>().table;
		break;
	case LogicalOperatorType::LOGICAL_DELETE:
		table = &dml.Cast<LogicalDelete>().table;
		break;
	case LogicalOperatorType::LOGICAL_UPDATE:
		table = &dml.Cast<LogicalUpdate>().table;
		break;
	case LogicalOperatorType::LOGICAL_MERGE_INTO:
		table = &dml.Cast<LogicalMergeInto>().table;
		break;
	default:
		throw InternalException("OpenIVM expected a source DML operator");
	}
	if (table->name.empty() || SqlUtils::IsDelta(table->name) || IncrementalTableNames::IsDataTable(table->name) ||
	    table->catalog.GetCatalogType() == "ducklake") {
		return nullptr;
	}
	return table;
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

		auto source = Catalog::GetEntry<TableCatalogEntry>(input.context, alter_info->catalog, alter_info->schema,
		                                                   alter_info->name, OnEntryNotFound::RETURN_NULL);
		if (!source) {
			return;
		}
		// SQL identifiers are case-insensitive; metadata stores the catalog spelling.
		string table_name = source->name;
		string delta_name = SqlUtils::DeltaName(table_name);
		auto source_locus = std::make_pair(source->ParentCatalog().GetName(), source->schema.name);

		TransactionalMVLockState::Get(input.context).AcquireMutationLock();
		// Resolve only the source's delta table in the caller transaction. Scanning
		// information_schema touches unrelated external catalogs and can silently
		// skip schema synchronization when one of them is unavailable.
		auto delta_entry = RefreshMetadata::FindSourceDeltaForMutation(input.context, source_locus.first,
		                                                               source_locus.second, table_name);
		if (!delta_entry) {
			return; // not an IVM-tracked table
		}
		// The physical delta may sit in an internal schema; metadata still keys it by delta_name.
		auto delta_locus = std::make_pair(delta_entry->schema.name, delta_entry->name);

		switch (alter_info->alter_table_type) {
		case AlterTableType::ADD_COLUMN: {
			auto *add_info = dynamic_cast<AddColumnInfo *>(alter_info);
			if (!add_info) {
				break;
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE ADD COLUMN '%s' — syncing delta table\n",
			                    add_info->new_column.Name().c_str());
			AlterDeltaInCallerTransaction(input.context, *alter_info, source_locus.first, delta_locus.first,
			                              delta_locus.second);
			break;
		}
		case AlterTableType::REMOVE_COLUMN: {
			auto *remove_info = dynamic_cast<RemoveColumnInfo *>(alter_info);
			if (!remove_info) {
				break;
			}
			string col_name = remove_info->removed_column;
			for (auto &location : RefreshMetadata::MetadataLocations(input.context, openivm::DELTA_TABLES_TABLE)) {
				Connection con(*input.context.db);
				SnapshotSchemaEvolutionMetadata(input.context, con, location);
				auto referencing_mv = FirstMVReferencingColumn(input.context, con, delta_name, source_locus.first,
				                                               source_locus.second, table_name, col_name);
				if (!referencing_mv.empty()) {
					throw CatalogException("Cannot drop column '" + col_name +
					                       "': it is referenced by materialized view '" + referencing_mv +
					                       "'. Drop the view first.");
				}
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE DROP COLUMN '%s' — syncing delta table\n", col_name.c_str());
			AlterDeltaInCallerTransaction(input.context, *alter_info, source_locus.first, delta_locus.first,
			                              delta_locus.second);
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
			    "view_name IN (SELECT view_name FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE " +
			    RefreshMetadata::SourcePredicate(delta_name, source_locus.first, source_locus.second) + ")";
			for (auto &location : RefreshMetadata::MetadataLocations(input.context, openivm::DELTA_TABLES_TABLE)) {
				Connection con(*input.context.db);
				SnapshotSchemaEvolutionMetadata(input.context, con, location);
				// DuckDB permits writes to one native catalog per transaction. Keep
				// the existing audited helper/undo path for other committed catalogs.
				if (!StringUtil::CIEquals(location.catalog_name, source_locus.first)) {
					RegisterMetadataRestore(input.context, con, openivm::VIEWS_TABLE, dependent_view_predicate);
				}
				RewriteDependentViewMetadataForRename(input.context, con, delta_name, source_locus.first,
				                                      source_locus.second, table_name, old_name, new_name);
			}
			OPENIVM_DEBUG_PRINT("[INSERT RULE] ALTER TABLE RENAME COLUMN '%s' → '%s' — syncing delta table\n",
			                    old_name.c_str(), new_name.c_str());
			AlterDeltaInCallerTransaction(input.context, *alter_info, source_locus.first, delta_locus.first,
			                              delta_locus.second);
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

	auto table = GetNativeDMLTable(*dml);
	if (!table) {
		return;
	}
	auto delta_table = TransactionalMVLockState::Get(input.context)
	                       .FindSourceDeltaForWrite(table->catalog.GetName(), table->schema.name, table->name);
	if (delta_table) {
		switch (dml->type) {
		case LogicalOperatorType::LOGICAL_INSERT: {
			auto &insert = dml->Cast<LogicalInsert>();
			const auto &table_name = insert.table.name;
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
			ResolveUpdateDefaults(input, update);
			vector<unique_ptr<Expression>> update_expressions;
			update_expressions.reserve(update.expressions.size());
			for (idx_t index = 0; index < update.expressions.size(); index++) {
				update_expressions.push_back(update.expressions[index]->Copy());
			}
			auto row_id_index = update.children[0]->GetColumnBindings().size() - 1;
			auto capture = make_uniq<LogicalTransactionalDeltaCapture>(
			    update.table, *delta_table, DeltaCaptureMode::UPDATE, std::move(update_expressions), update.columns,
			    optional_idx(row_id_index));
			capture->children.push_back(std::move(update.children[0]));
			update.children[0] = std::move(capture);
			OPENIVM_DEBUG_PRINT("[INSERT RULE] transactional UPDATE delta capture for '%s'\n", table_name.c_str());
			break;
		}
		case LogicalOperatorType::LOGICAL_MERGE_INTO: {
			auto &merge = dml->Cast<LogicalMergeInto>();
			const auto &table_name = merge.table.name;
			auto capture = make_uniq<LogicalTransactionalMergeDeltaCapture>(merge.table, *delta_table);
			capture->children.push_back(std::move(*dml_owner));
			*dml_owner = std::move(capture);
			OPENIVM_DEBUG_PRINT("[INSERT RULE] transactional MERGE action delta capture for '%s'\n",
			                    table_name.c_str());
			break;
		}
		default:
			return;
		}
	}
	// Put the execution marker in the DML input pipeline, before its native sink.
	// Plain EXPLAIN builds this operator but never initializes its runtime state.
	auto execution = make_uniq<LogicalSourceWriteExecution>(*table, delta_table);
	execution->children.push_back(std::move(dml->children[0]));
	dml->children[0] = std::move(execution);
}
} // namespace duckdb
