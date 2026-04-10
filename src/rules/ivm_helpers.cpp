#include "rules/ivm_rule.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

// ============================================================================
// DuckLake delta scan: plan SQL fragments for table_insertions + table_deletions
// ============================================================================

/// Build a DuckLake delta scan by planning a SQL fragment that calls
/// ducklake_table_insertions/deletions with a multiplicity column, then
/// extracting the plan tree. DuckDB handles the table function expansion.
static DeltaGetResult CreateDuckLakeDeltaNode(ClientContext &context, LogicalGet *old_get, const string &view_name) {
	auto table_ref = old_get->GetTable();
	string catalog_name = table_ref->ParentCatalog().GetName();
	string schema_name = table_ref->schema.name;
	string table_name = table_ref.get()->name;

	OPENIVM_DEBUG_PRINT("[DuckLake] Creating delta node for '%s.%s.%s'\n", catalog_name.c_str(), schema_name.c_str(),
	                    table_name.c_str());

	// Get snapshot range from metadata.
	// Use auto-commit to avoid deadlocking with the outer optimizer transaction.
	OPENIVM_DEBUG_PRINT("[DuckLake] About to create connection for metadata queries\n");
	Connection con(*context.db);
	OPENIVM_DEBUG_PRINT("[DuckLake] Connection created, querying snapshot...\n");
	auto snap_result = con.Query("SELECT last_snapshot_id FROM " + string(ivm::DELTA_TABLES_TABLE) +
	                             " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
	                             "' AND table_name = '" + OpenIVMUtils::EscapeValue(table_name) + "'");
	if (snap_result->HasError() || snap_result->RowCount() == 0 || snap_result->GetValue(0, 0).IsNull()) {
		throw InternalException("No snapshot ID found for DuckLake table '%s' in view '%s'", table_name, view_name);
	}
	int64_t last_snap = snap_result->GetValue(0, 0).GetValue<int64_t>();

	auto cur_snap_result = con.Query("SELECT id FROM " + catalog_name + ".current_snapshot()");
	if (cur_snap_result->HasError() || cur_snap_result->RowCount() == 0) {
		throw InternalException("Cannot get current DuckLake snapshot for catalog '%s'", catalog_name);
	}
	int64_t cur_snap = cur_snap_result->GetValue(0, 0).GetValue<int64_t>();

	OPENIVM_DEBUG_PRINT("[DuckLake] Snapshot range: %ld -> %ld\n", (long)last_snap, (long)cur_snap);

	// Build column list from old_get's projected columns
	string col_list;
	auto &col_ids = old_get->GetColumnIds();
	for (idx_t i = 0; i < col_ids.size(); i++) {
		if (i > 0) {
			col_list += ", ";
		}
		idx_t idx = col_ids[i].GetPrimaryIndex();
		col_list += OpenIVMUtils::QuoteIdentifier(old_get->names[idx]);
	}

	// Construct the delta SQL: insertions (mul=true) UNION ALL deletions (mul=false)
	string ins_sql = "SELECT " + col_list + ", true AS " + string(ivm::MULTIPLICITY_COL) +
	                 " FROM ducklake_table_insertions('" + OpenIVMUtils::EscapeValue(catalog_name) + "', '" +
	                 OpenIVMUtils::EscapeValue(schema_name) + "', '" + OpenIVMUtils::EscapeValue(table_name) + "', " +
	                 to_string(last_snap) + ", " + to_string(cur_snap) + ")";
	string del_sql = "SELECT " + col_list + ", false AS " + string(ivm::MULTIPLICITY_COL) +
	                 " FROM ducklake_table_deletions('" + OpenIVMUtils::EscapeValue(catalog_name) + "', '" +
	                 OpenIVMUtils::EscapeValue(schema_name) + "', '" + OpenIVMUtils::EscapeValue(table_name) + "', " +
	                 to_string(last_snap) + ", " + to_string(cur_snap) + ")";
	string delta_sql = ins_sql + " UNION ALL " + del_sql;

	OPENIVM_DEBUG_PRINT("[DuckLake] Delta SQL: %s\n", delta_sql.c_str());

	// Plan the SQL fragment on a separate connection to avoid deadlocking
	// with the optimizer's active transaction on the main context.
	con.BeginTransaction();
	Parser parser;
	parser.ParseQuery(delta_sql);
	Planner planner(*con.context);
	planner.CreatePlan(parser.statements[0]->Copy());
	auto plan = std::move(planner.plan);
	plan->ResolveOperatorTypes();
	con.Rollback();

	// Wrap in a projection that remaps output bindings to old_get->table_index.
	// The planned SQL fragment has its own table indices (starting from 0) which
	// don't match the parent plan's expectations.
	auto bindings = plan->GetColumnBindings();
	auto types = plan->types;

	vector<unique_ptr<Expression>> remap_exprs;
	for (idx_t i = 0; i < bindings.size(); i++) {
		remap_exprs.push_back(make_uniq<BoundColumnRefExpression>(types[i], bindings[i]));
	}

	auto remap_proj = make_uniq<LogicalProjection>(old_get->table_index, std::move(remap_exprs));
	remap_proj->children.push_back(std::move(plan));
	remap_proj->ResolveOperatorTypes();

	// Multiplicity binding: last column at the remapped table_index
	ColumnBinding mul_binding(old_get->table_index, bindings.size() - 1);

	OPENIVM_DEBUG_PRINT("[DuckLake] Delta plan built: %zu output cols, mul_binding: table=%lu col=%lu\n",
	                    bindings.size(), (unsigned long)mul_binding.table_index,
	                    (unsigned long)mul_binding.column_index);

	return {std::move(remap_proj), mul_binding};
}

// ============================================================================
// Standard DuckDB delta scan (existing logic)
// ============================================================================

DeltaGetResult CreateDeltaGetNode(ClientContext &context, LogicalGet *old_get, const string &view_name) {
	// DuckLake tables: use native change tracking via table_insertions/table_deletions
	auto table_ref = old_get->GetTable();
	if (table_ref.get() && table_ref->ParentCatalog().GetCatalogType() == "ducklake") {
		return CreateDuckLakeDeltaNode(context, old_get, view_name);
	}
	unique_ptr<LogicalGet> delta_get_node;
	ColumnBinding new_mul_binding;
	string table_name;
	OPENIVM_DEBUG_PRINT("[CreateDeltaGet] Creating delta get for view '%s', original table_index=%lu\n",
	                    view_name.c_str(), (unsigned long)old_get->table_index);

	optional_ptr<TableCatalogEntry> opt_catalog_entry;
	{
		string delta_table;
		string delta_table_schema;
		string delta_table_catalog;
		if (old_get->GetTable().get() == nullptr) {
			delta_table_schema = "public";
			delta_table_catalog = "p"; // todo
		} else {
			delta_table = OpenIVMUtils::DeltaName(old_get->GetTable().get()->name);
			delta_table_schema = old_get->GetTable().get()->schema.name;
			delta_table_catalog = old_get->GetTable().get()->catalog.GetName();
		}
		QueryErrorContext error_context;
		opt_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    context, delta_table_catalog, delta_table_schema, delta_table, OnEntryNotFound::RETURN_NULL, error_context);
		if (opt_catalog_entry == nullptr) {
			throw Exception(ExceptionType::BINDER, "Table " + delta_table + " does not exist, no deltas to compute!");
		}
	}
	auto &table_entry = opt_catalog_entry->Cast<TableCatalogEntry>();
	table_name = table_entry.name;
	unique_ptr<FunctionData> bind_data;
	auto scan_function = table_entry.GetScanFunction(context, bind_data);

	vector<ColumnIndex> column_ids = {};
	idx_t mul_oid = 0, ts_oid = 0, max_oid = 0;
	for (auto &col : table_entry.GetColumns().Logical()) {
		if (col.Name() == string(ivm::MULTIPLICITY_COL)) {
			mul_oid = col.Oid();
		} else if (col.Name() == string(ivm::TIMESTAMP_COL)) {
			ts_oid = col.Oid();
		}
		if (col.Oid() > max_oid) {
			max_oid = col.Oid();
		}
	}

	vector<LogicalType> return_types(max_oid + 1, LogicalType::ANY);
	vector<string> return_names(max_oid + 1, "");
	for (auto &col : table_entry.GetColumns().Logical()) {
		return_types[col.Oid()] = col.Type();
		return_names[col.Oid()] = col.Name();
	}

	for (auto &id : old_get->GetColumnIds()) {
		if (id.IsVirtualColumn()) {
			// Virtual columns (e.g., row-id for COUNT(*)) don't exist in delta tables.
			// Map to multiplicity column instead.
			column_ids.push_back(ColumnIndex(mul_oid));
		} else {
			column_ids.push_back(id);
		}
	}
	column_ids.push_back(ColumnIndex(mul_oid));
	column_ids.push_back(ColumnIndex(ts_oid));

	delta_get_node = make_uniq<LogicalGet>(old_get->table_index, scan_function, std::move(bind_data),
	                                       std::move(return_types), std::move(return_names));
	delta_get_node->SetColumnIds(std::move(column_ids));

	// Timestamp filter
	Connection con(*context.db);
	con.SetAutoCommit(false);
	auto timestamp_query = "select last_update from " + string(ivm::DELTA_TABLES_TABLE) + " where view_name = '" +
	                       OpenIVMUtils::EscapeValue(view_name) + "' and table_name = '" +
	                       OpenIVMUtils::EscapeValue(table_name) + "';";
	auto r = con.Query(timestamp_query);
	if (r->HasError()) {
		throw InternalException("Error while querying last_update");
	}
	auto ts_value = r->GetValue(0, 0);
	if (ts_value.type() != LogicalType::TIMESTAMP) {
		ts_value = ts_value.DefaultCastAs(LogicalType::TIMESTAMP);
	}
	auto table_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, ts_value);
	auto &ts_col_id = delta_get_node->GetColumnIds().back();
	idx_t ts_filter_key = ts_col_id.GetPrimaryIndex();
	delta_get_node->table_filters.filters[ts_filter_key] = std::move(table_filter);

	// projection_ids
	delta_get_node->projection_ids.clear();
	idx_t n_base = old_get->GetColumnIds().size();
	if (!old_get->projection_ids.empty()) {
		for (auto &pid : old_get->projection_ids) {
			delta_get_node->projection_ids.push_back(pid);
		}
	} else {
		for (idx_t i = 0; i < n_base; i++) {
			delta_get_node->projection_ids.push_back(i);
		}
	}
	delta_get_node->projection_ids.push_back(n_base); // mul column
	idx_t mul_proj_pos = delta_get_node->projection_ids.size() - 1;
	new_mul_binding = ColumnBinding(old_get->table_index, mul_proj_pos);

	delta_get_node->ResolveOperatorTypes();
	OPENIVM_DEBUG_PRINT("[CreateDeltaGet] Delta table: %s, mul_binding: table=%lu col=%lu, columns: %zu\n",
	                    table_name.c_str(), (unsigned long)new_mul_binding.table_index,
	                    (unsigned long)new_mul_binding.column_index, delta_get_node->GetColumnIds().size());
	return {std::move(delta_get_node), new_mul_binding};
}

} // namespace duckdb
