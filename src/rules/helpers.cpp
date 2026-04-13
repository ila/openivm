#include "rules/rule.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "storage/ducklake_scan.hpp"

namespace duckdb {

// ============================================================================
// DuckLake delta scan: direct plan construction using native catalog types
// ============================================================================

/// Build a DuckLake delta scan by directly constructing LogicalGet nodes
/// with SCAN_INSERTIONS / SCAN_DELETIONS, avoiding SQL string round-trips.
static DeltaGetResult CreateDuckLakeDeltaNode(ClientContext &context, Binder &binder, LogicalGet *old_get,
                                              const string &view_name) {
	auto table_ref = old_get->GetTable();
	string catalog_name = table_ref->ParentCatalog().GetName();
	string schema_name = table_ref->schema.name;
	string table_name = table_ref.get()->name;

	OPENIVM_DEBUG_PRINT("[DuckLake] Creating delta node for '%s.%s'\n", catalog_name.c_str(), table_name.c_str());

	// Get last snapshot from IVM metadata. Uses a separate connection because
	// the optimizer holds a lock on the main context during plan rewriting.
	Connection con(*context.db);
	auto snap_result = con.Query("SELECT last_snapshot_id FROM " + string(ivm::DELTA_TABLES_TABLE) +
	                             " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
	                             "' AND table_name = '" + OpenIVMUtils::EscapeValue(table_name) + "'");
	if (snap_result->HasError() || snap_result->RowCount() == 0 || snap_result->GetValue(0, 0).IsNull()) {
		throw InternalException("No snapshot ID found for DuckLake table '%s' in view '%s'", table_name, view_name);
	}
	int64_t last_snap = snap_result->GetValue(0, 0).GetValue<int64_t>();

	// Get current snapshot from the old_get's existing DuckLake scan info (no SQL needed).
	auto &old_func_info = old_get->function.function_info->Cast<DuckLakeFunctionInfo>();
	int64_t cur_snap = static_cast<int64_t>(old_func_info.snapshot.snapshot_id);

	OPENIVM_DEBUG_PRINT("[DuckLake] Snapshot range: %ld -> %ld\n", (long)last_snap, (long)cur_snap);

	// Build a start snapshot for the change tracking range [last_snap, cur_snap].
	// Only snapshot_id is used by DuckLake's file selection logic.
	DuckLakeSnapshot start_snapshot(static_cast<idx_t>(last_snap), DConstants::INVALID_INDEX, DConstants::INVALID_INDEX,
	                                DConstants::INVALID_INDEX);

	// Determine column IDs from old_get, skipping virtual columns (ROWID etc.)
	// that don't exist in DuckLake change scans.
	vector<ColumnIndex> delta_col_ids;
	for (auto &id : old_get->GetColumnIds()) {
		if (!id.IsVirtualColumn()) {
			delta_col_ids.push_back(id);
		}
	}
	if (delta_col_ids.empty()) {
		delta_col_ids.push_back(ColumnIndex(0));
	}

	// Helper: create a LogicalGet configured as a DuckLake change scan.
	auto make_change_scan = [&](DuckLakeScanType scan_type, idx_t table_idx) -> unique_ptr<LogicalGet> {
		unique_ptr<FunctionData> bind_data;
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, table_name, QueryErrorContext());
		auto scan_fn = table_ref->Cast<TableCatalogEntry>().GetScanFunction(context, bind_data, lookup);

		auto &func_info = scan_fn.function_info->Cast<DuckLakeFunctionInfo>();
		func_info.scan_type = scan_type;
		func_info.start_snapshot = make_uniq<DuckLakeSnapshot>(start_snapshot);

		// Capture column metadata before moving scan_fn into the LogicalGet.
		auto col_types = func_info.column_types;
		auto col_names = func_info.column_names;

		auto get = make_uniq<LogicalGet>(table_idx, std::move(scan_fn), std::move(bind_data), std::move(col_types),
		                                 std::move(col_names));
		get->SetColumnIds(vector<ColumnIndex>(delta_col_ids));

		// Set parameters so LPTS can reconstruct the ducklake_table_insertions/deletions SQL.
		get->parameters = {Value(catalog_name), Value(schema_name), Value(table_name), Value::BIGINT(last_snap),
		                   Value::BIGINT(cur_snap)};

		get->ResolveOperatorTypes();
		return get;
	};

	auto ins_get = make_change_scan(DuckLakeScanType::SCAN_INSERTIONS, binder.GenerateTableIndex());
	auto del_get = make_change_scan(DuckLakeScanType::SCAN_DELETIONS, binder.GenerateTableIndex());

	// Helper: wrap a scan in a projection that appends a constant multiplicity column.
	auto add_mul_projection = [&](unique_ptr<LogicalOperator> scan, bool mul_value) -> unique_ptr<LogicalProjection> {
		auto bindings = scan->GetColumnBindings();
		auto types = scan->types;

		vector<unique_ptr<Expression>> exprs;
		for (idx_t i = 0; i < bindings.size(); i++) {
			exprs.push_back(make_uniq<BoundColumnRefExpression>(types[i], bindings[i]));
		}
		exprs.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(mul_value)));

		auto proj = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(exprs));
		proj->children.push_back(std::move(scan));
		proj->ResolveOperatorTypes();
		return proj;
	};

	auto ins_proj = add_mul_projection(std::move(ins_get), true);
	auto del_proj = add_mul_projection(std::move(del_get), false);

	// UNION ALL the insertions and deletions.
	auto union_types = ins_proj->types;
	idx_t n_output_cols = union_types.size();
	auto union_op =
	    make_uniq<LogicalSetOperation>(binder.GenerateTableIndex(), n_output_cols, std::move(ins_proj),
	                                   std::move(del_proj), LogicalOperatorType::LOGICAL_UNION, true /* setop_all */);
	union_op->types = union_types;

	// Outer projection to remap output bindings to old_get->table_index.
	auto union_bindings = union_op->GetColumnBindings();
	vector<unique_ptr<Expression>> remap_exprs;
	for (idx_t i = 0; i < union_bindings.size(); i++) {
		remap_exprs.push_back(make_uniq<BoundColumnRefExpression>(union_types[i], union_bindings[i]));
	}
	auto remap_proj = make_uniq<LogicalProjection>(old_get->table_index, std::move(remap_exprs));
	remap_proj->children.push_back(std::move(union_op));
	remap_proj->ResolveOperatorTypes();

	ColumnBinding mul_binding(old_get->table_index, union_bindings.size() - 1);

	OPENIVM_DEBUG_PRINT("[DuckLake] Delta plan built: %zu output cols, mul_binding: table=%lu col=%lu\n",
	                    union_bindings.size(), (unsigned long)mul_binding.table_index,
	                    (unsigned long)mul_binding.column_index);

	return {std::move(remap_proj), mul_binding};
}

// ============================================================================
// Standard DuckDB delta scan (existing logic)
// ============================================================================

DeltaGetResult CreateDeltaGetNode(ClientContext &context, Binder &binder, LogicalGet *old_get,
                                  const string &view_name) {
	// DuckLake tables: use native change tracking via direct plan construction
	auto table_ref = old_get->GetTable();
	if (table_ref.get() && table_ref->ParentCatalog().GetCatalogType() == "ducklake") {
		return CreateDuckLakeDeltaNode(context, binder, old_get, view_name);
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
