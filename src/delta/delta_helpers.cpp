#include "delta/delta_helpers.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/plan_rewrite_internal.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "storage/ducklake_scan.hpp"
#include "upsert/refresh_internal.hpp"

namespace duckdb {

namespace {

static std::chrono::steady_clock::time_point ProfileNow() {
	return std::chrono::steady_clock::now();
}

static void AddCompileProfileStep(ClientContext &context, const string &step_name,
                                  std::chrono::steady_clock::time_point start, const string &detail = string()) {
	auto *profile = RefreshCompileProfileContextSlot::Get(context);
	if (profile) {
		profile->AddStep(step_name, start, detail);
	}
}

static string DuckLakeDeltaProfileDetail(const string &catalog, const string &schema, const string &table,
                                         int64_t last_snapshot, int64_t current_snapshot) {
	return "table=" + catalog + "." + schema + "." + table + "; last_snapshot=" + to_string(last_snapshot) +
	       "; current_snapshot=" + to_string(current_snapshot);
}

} // namespace

static bool CompactDeltasEnabled(ClientContext &context) {
	Value compact_val;
	if (context.TryGetCurrentSetting("openivm_compact_deltas", compact_val) && !compact_val.IsNull()) {
		return compact_val.GetValue<bool>();
	}
	return true;
}

static DeltaGetResult RemapDeltaNode(ClientContext &context, unique_ptr<LogicalOperator> delta_node,
                                     idx_t output_table_index, ColumnBinding mul_binding) {
	auto input_bindings = delta_node->GetColumnBindings();
	auto input_types = delta_node->types;
	vector<unique_ptr<Expression>> remap_exprs;
	for (idx_t i = 0; i < input_bindings.size(); i++) {
		if (input_bindings[i] == mul_binding) {
			remap_exprs.push_back(BoundCastExpression::AddCastToType(
			    context, make_uniq<BoundColumnRefExpression>(input_types[i], input_bindings[i]), LogicalType::INTEGER));
		} else {
			remap_exprs.push_back(make_uniq<BoundColumnRefExpression>(input_types[i], input_bindings[i]));
		}
	}
	auto remap_proj = make_uniq<LogicalProjection>(output_table_index, std::move(remap_exprs));
	remap_proj->children.push_back(std::move(delta_node));
	remap_proj->ResolveOperatorTypes();

	ColumnBinding remapped_mul(output_table_index, input_bindings.size() - 1);
	return {std::move(remap_proj), remapped_mul};
}

static DeltaGetResult CompactDeltaNode(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> delta_node,
                                       idx_t output_table_index, ColumnBinding mul_binding) {
	auto input_bindings = delta_node->GetColumnBindings();
	auto input_types = delta_node->types;
	idx_t base_col_count = input_bindings.size() - 1;

	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();

	vector<unique_ptr<Expression>> sum_args;
	sum_args.push_back(make_uniq<BoundColumnRefExpression>(input_types[base_col_count], mul_binding));
	auto sum_func = BindAggregateByName(context, "sum", {input_types[base_col_count]});
	auto sum_expr = make_uniq<BoundAggregateExpression>(std::move(sum_func), std::move(sum_args), nullptr, nullptr,
	                                                    AggregateType::NON_DISTINCT);
	sum_expr->alias = openivm::MULTIPLICITY_COL;

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(sum_expr));
	auto aggregate = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	GroupingSet grouping_set;
	for (idx_t i = 0; i < base_col_count; i++) {
		aggregate->groups.push_back(make_uniq<BoundColumnRefExpression>(input_types[i], input_bindings[i]));
		aggregate->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(input_types[i])));
		grouping_set.insert(i);
	}
	aggregate->grouping_sets.push_back(std::move(grouping_set));
	aggregate->children.push_back(std::move(delta_node));
	aggregate->ResolveOperatorTypes();

	auto agg_bindings = aggregate->GetColumnBindings();
	auto agg_types = aggregate->types;
	auto filter_expr = make_uniq<BoundComparisonExpression>(
	    ExpressionType::COMPARE_NOTEQUAL,
	    make_uniq<BoundColumnRefExpression>(agg_types[base_col_count], agg_bindings[base_col_count]),
	    make_uniq<BoundConstantExpression>(Value::INTEGER(0)));
	auto filter = make_uniq<LogicalFilter>(std::move(filter_expr));
	filter->children.push_back(std::move(aggregate));
	filter->ResolveOperatorTypes();

	auto filter_bindings = filter->GetColumnBindings();
	vector<unique_ptr<Expression>> remap_exprs;
	for (idx_t i = 0; i < base_col_count; i++) {
		remap_exprs.push_back(make_uniq<BoundColumnRefExpression>(agg_types[i], filter_bindings[i]));
	}
	remap_exprs.push_back(BoundCastExpression::AddCastToType(
	    context, make_uniq<BoundColumnRefExpression>(agg_types[base_col_count], filter_bindings[base_col_count]),
	    LogicalType::INTEGER));

	auto remap_proj = make_uniq<LogicalProjection>(output_table_index, std::move(remap_exprs));
	remap_proj->children.push_back(std::move(filter));
	remap_proj->ResolveOperatorTypes();

	ColumnBinding compact_mul_binding(output_table_index, base_col_count);
	return {std::move(remap_proj), compact_mul_binding};
}

// ============================================================================
// DuckLake delta scan: direct plan construction using native catalog types
// ============================================================================

/// Build a DuckLake delta scan by directly constructing LogicalGet nodes
/// with SCAN_INSERTIONS / SCAN_DELETIONS, avoiding SQL string round-trips.
static DeltaGetResult CreateDuckLakeDeltaNode(ClientContext &context, Binder &binder, LogicalGet *old_get,
                                              const string &view_name) {
	auto total_start = ProfileNow();
	auto table_ref = old_get->GetTable();
	string catalog_name = table_ref->ParentCatalog().GetName();
	string schema_name = table_ref->schema.name;
	string table_name = table_ref.get()->name;

	OPENIVM_DEBUG_PRINT("[DuckLake] Creating delta node for '%s.%s'\n", catalog_name.c_str(), table_name.c_str());

	// Get last snapshot from IVM metadata. Uses a separate connection because
	// the optimizer holds a lock on the main context during plan rewriting.
	Connection con(*context.db);
	auto metadata_start = ProfileNow();
	auto snap_result =
	    con.Query("SELECT last_snapshot_id FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "'");
	if (snap_result->HasError() || snap_result->RowCount() == 0 || snap_result->GetValue(0, 0).IsNull()) {
		throw Exception(ExceptionType::CATALOG,
		                "IVM: no snapshot ID recorded for DuckLake table '" + table_name + "' in view '" + view_name +
		                    "' (metadata may be missing — try DROP MATERIALIZED VIEW and recreate)");
	}
	int64_t last_snap = snap_result->GetValue(0, 0).GetValue<int64_t>();

	// Get current snapshot from the old_get's existing DuckLake scan info (no SQL needed).
	auto &old_func_info = old_get->function.function_info->Cast<DuckLakeFunctionInfo>();
	int64_t cur_snap = static_cast<int64_t>(old_func_info.snapshot.snapshot_id);
	auto detail = DuckLakeDeltaProfileDetail(catalog_name, schema_name, table_name, last_snap, cur_snap);
	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.metadata_query", metadata_start,
	                      detail);

	int64_t start_snap = last_snap + 1;
	OPENIVM_DEBUG_PRINT("[DuckLake] Snapshot range: %ld -> %ld\n", (long)start_snap, (long)cur_snap);

	if (start_snap > cur_snap) {
		auto empty_start = ProfileNow();
		vector<LogicalType> empty_types = old_get->types;
		empty_types.push_back(LogicalType::INTEGER);
		vector<ColumnBinding> bindings;
		auto table_index = binder.GenerateTableIndex();
		for (idx_t i = 0; i < empty_types.size(); i++) {
			bindings.emplace_back(table_index, i);
		}
		auto mul_binding = bindings.back();
		auto empty = make_uniq<LogicalEmptyResult>(empty_types, std::move(bindings));
		empty->ResolveOperatorTypes();
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.empty_result", empty_start,
		                      detail + "; start_snapshot=" + to_string(start_snap));
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.total", total_start,
		                      detail + "; result=empty");
		return {std::move(empty), mul_binding};
	}

	// DuckLake's table_insertions/deletions functions treat the start snapshot as
	// inclusive, so the next refresh starts at last_snapshot_id + 1.
	// Only snapshot_id is used by DuckLake's file selection logic.
	auto start_snapshot_start = ProfileNow();
	DuckLakeSnapshot start_snapshot(static_cast<idx_t>(start_snap), DConstants::INVALID_INDEX,
	                                DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);
	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.start_snapshot",
	                      start_snapshot_start, detail + "; start_snapshot=" + to_string(start_snap));

	// Determine column IDs from old_get, skipping virtual columns (ROWID etc.)
	// that don't exist in DuckLake change scans.
	auto column_ids_start = ProfileNow();
	vector<ColumnIndex> delta_col_ids;
	for (auto &id : old_get->GetColumnIds()) {
		if (!id.IsVirtualColumn()) {
			delta_col_ids.push_back(id);
		}
	}
	if (delta_col_ids.empty()) {
		delta_col_ids.push_back(ColumnIndex(0));
	}
	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.column_ids", column_ids_start,
	                      detail + "; columns=" + to_string(delta_col_ids.size()));

	// Helper: create a LogicalGet configured as a DuckLake change scan.
	auto make_change_scan = [&](DuckLakeScanType scan_type, idx_t table_idx) -> unique_ptr<LogicalGet> {
		auto scan_start = ProfileNow();
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
		for (auto &entry : old_get->table_filters.filters) {
			if (entry.second->filter_type == TableFilterType::OPTIONAL_FILTER) {
				continue;
			}
			get->table_filters.filters[entry.first] = entry.second->Copy();
		}

		// Set parameters so LPTS can reconstruct the ducklake_table_insertions/deletions SQL.
		get->parameters = {Value(catalog_name), Value(schema_name), Value(table_name), Value::BIGINT(start_snap),
		                   Value::BIGINT(cur_snap)};

		get->ResolveOperatorTypes();
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.change_scan", scan_start,
		                      detail + "; scan_type=" +
		                          string(scan_type == DuckLakeScanType::SCAN_INSERTIONS ? "insertions" : "deletions"));
		return get;
	};

	auto ins_get = make_change_scan(DuckLakeScanType::SCAN_INSERTIONS, binder.GenerateTableIndex());
	auto del_get = make_change_scan(DuckLakeScanType::SCAN_DELETIONS, binder.GenerateTableIndex());

	// Helper: wrap a scan in a projection that appends a constant multiplicity column.
	// mul_value is the signed Z-set weight: +1 for inserts, -1 for deletes.
	auto add_mul_projection = [&](unique_ptr<LogicalOperator> scan,
	                              int32_t mul_value) -> unique_ptr<LogicalProjection> {
		auto projection_start = ProfileNow();
		auto bindings = scan->GetColumnBindings();
		auto types = scan->types;

		vector<unique_ptr<Expression>> exprs;
		for (idx_t i = 0; i < bindings.size(); i++) {
			exprs.push_back(make_uniq<BoundColumnRefExpression>(types[i], bindings[i]));
		}
		exprs.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(mul_value)));

		auto proj = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(exprs));
		proj->children.push_back(std::move(scan));
		proj->ResolveOperatorTypes();
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.mul_projection",
		                      projection_start, detail + "; mul=" + to_string(mul_value));
		return proj;
	};

	auto ins_proj = add_mul_projection(std::move(ins_get), 1);
	auto del_proj = add_mul_projection(std::move(del_get), -1);

	// UNION ALL the insertions and deletions.
	auto union_start = ProfileNow();
	auto union_types = ins_proj->types;
	idx_t n_output_cols = union_types.size();
	auto union_op =
	    make_uniq<LogicalSetOperation>(binder.GenerateTableIndex(), n_output_cols, std::move(ins_proj),
	                                   std::move(del_proj), LogicalOperatorType::LOGICAL_UNION, true /* setop_all */);
	union_op->types = union_types;
	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.union", union_start,
	                      detail + "; output_cols=" + to_string(n_output_cols));

	ColumnBinding mul_binding(union_op->GetColumnBindings().back());
	if (!CompactDeltasEnabled(context)) {
		OPENIVM_DEBUG_PRINT("[DuckLake] Delta compaction disabled\n");
		auto remap_start = ProfileNow();
		auto result = RemapDeltaNode(context, std::move(union_op), old_get->table_index, mul_binding);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.remap", remap_start, detail);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.total", total_start,
		                      detail + "; compact=false");
		return result;
	}

	auto compact_start = ProfileNow();
	auto compacted = CompactDeltaNode(context, binder, std::move(union_op), old_get->table_index, mul_binding);
	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.compact", compact_start, detail);
	OPENIVM_DEBUG_PRINT("[DuckLake] Delta plan built: %zu output cols, mul_binding: table=%lu col=%lu\n",
	                    compacted.node->types.size(), (unsigned long)compacted.mul_binding.table_index,
	                    (unsigned long)compacted.mul_binding.column_index);
	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_delta.total", total_start,
	                      detail + "; compact=true");
	return compacted;
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
	// Table functions (generate_series, range, etc.) have no catalog-backing table
	// and therefore no delta table. Their output is constant across refreshes, so
	// the delta is always empty — the inclusion-exclusion pruner in join.cpp's
	// DetectDeltaStatus marks these leaves as empty, skipping every term where
	// this leaf's bit is in the delta mask. By the time we reach CreateDeltaGetNode
	// for a join-backed view, this branch shouldn't fire — but keep it as a clear
	// error for direct (non-join) use cases, where a materialized view defined
	// only on a table function has no delta to compute incrementally.
	unique_ptr<LogicalGet> delta_get_node;
	ColumnBinding new_mul_binding;
	string table_name;
	OPENIVM_DEBUG_PRINT("[CreateDeltaGet] Creating delta get for view '%s', original table_index=%lu\n",
	                    view_name.c_str(), (unsigned long)old_get->table_index);

	auto *source_table = old_get->GetTable().get();
	if (!source_table) {
		throw Exception(ExceptionType::BINDER,
		                "IVM: cannot create a delta scan for a table function in view '" + view_name + "'");
	}

	optional_ptr<TableCatalogEntry> opt_catalog_entry;
	{
		string delta_table = SqlUtils::DeltaName(source_table->name);
		string delta_table_schema = source_table->schema.name;
		string delta_table_catalog = source_table->catalog.GetName();
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
		if (col.Name() == string(openivm::MULTIPLICITY_COL)) {
			mul_oid = col.Oid();
		} else if (col.Name() == string(openivm::TIMESTAMP_COL)) {
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
	for (auto &entry : old_get->table_filters.filters) {
		if (entry.second->filter_type == TableFilterType::OPTIONAL_FILTER) {
			continue;
		}
		delta_get_node->table_filters.filters[entry.first] = entry.second->Copy();
	}

	// Timestamp filter
	Connection con(*context.db);
	con.SetAutoCommit(false);
	auto timestamp_query = "select last_update from " + string(openivm::DELTA_TABLES_TABLE) + " where view_name = '" +
	                       SqlUtils::EscapeValue(view_name) + "' and table_name = '" +
	                       SqlUtils::EscapeValue(table_name) + "';";
	auto r = con.Query(timestamp_query);
	if (r->HasError()) {
		throw Exception(ExceptionType::EXECUTOR, "IVM: failed to read last_update for view '" + view_name +
		                                             "', table '" + table_name + "': " + r->GetError());
	}
	if (r->RowCount() == 0 || r->GetValue(0, 0).IsNull()) {
		throw Exception(ExceptionType::CATALOG,
		                "IVM: missing last_update for view '" + view_name + "', table '" + table_name + "'");
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
	for (idx_t i = 0; i < n_base; i++) {
		delta_get_node->projection_ids.push_back(i);
	}
	delta_get_node->projection_ids.push_back(n_base); // mul column

	delta_get_node->ResolveOperatorTypes();
	new_mul_binding = delta_get_node->GetColumnBindings().back();
	OPENIVM_DEBUG_PRINT("[CreateDeltaGet] Delta table: %s, mul_binding: table=%lu col=%lu, columns: %zu\n",
	                    table_name.c_str(), (unsigned long)new_mul_binding.table_index,
	                    (unsigned long)new_mul_binding.column_index, delta_get_node->GetColumnIds().size());
	if (CompactDeltasEnabled(context)) {
		return CompactDeltaNode(context, binder, std::move(delta_get_node), old_get->table_index, new_mul_binding);
	}
	return {std::move(delta_get_node), new_mul_binding};
}

} // namespace duckdb
