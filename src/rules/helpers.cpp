#include "rules/rule.hpp"

#include "core/ivm_delta_model.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
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

namespace duckdb {

static void AssertLinearRewrite(LogicalOperatorType op_type, const string &context) {
	auto spec = GetDeltaOperatorSpec(op_type);
	if (spec.linearity != Linearity::LINEAR) {
		throw InternalException("%s helper used for non-linear operator %s", context,
		                        LogicalOperatorToString(op_type).c_str());
	}
}

idx_t FindColumnBindingIndex(const vector<ColumnBinding> &bindings, ColumnBinding target, const string &context) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (bindings[i] == target) {
			return i;
		}
	}
	throw InternalException("%s does not contain multiplicity binding", context);
}

ColumnBinding RewriteLinearChild(PlanWrapper &pw, idx_t child_index) {
	AssertLinearRewrite(pw.plan->type, "RewriteLinearChild");
	if (child_index >= pw.plan->children.size()) {
		throw InternalException("Linear operator %s does not have child %llu", LogicalOperatorToString(pw.plan->type),
		                        (unsigned long long)child_index);
	}
	auto child_mul = IVMRewriteRule::RewritePlan(pw.input, pw.plan->children[child_index], pw.view, pw.root);
	pw.plan->children[child_index] = std::move(child_mul.op);
	return child_mul.mul_binding;
}

ModifiedPlan RewriteLinearProjectionWithMultiplicity(PlanWrapper pw) {
	AssertLinearRewrite(pw.plan->type, "RewriteLinearProjectionWithMultiplicity");
	auto child_mul_binding = RewriteLinearChild(pw);

	auto projection_node = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(pw.plan));
	auto mul_expression = make_uniq<BoundColumnRefExpression>(ivm::MULTIPLICITY_COL, pw.mul_type, child_mul_binding);
	projection_node->expressions.emplace_back(std::move(mul_expression));
	projection_node->types.clear();
	for (auto &expr : projection_node->expressions) {
		projection_node->types.push_back(expr->return_type);
	}

	auto new_mul_binding = projection_node->GetColumnBindings().back();
	projection_node->Verify(pw.input.context);
	return {std::move(projection_node), new_mul_binding};
}

ModifiedPlan RewriteLinearUnion(PlanWrapper pw) {
	AssertLinearRewrite(pw.plan->type, "RewriteLinearUnion");
	if (pw.plan->children.size() != 2) {
		throw InternalException("Linear UNION rewrite expected 2 children, found %llu",
		                        (unsigned long long)pw.plan->children.size());
	}
	RewriteLinearChild(pw, 0);
	RewriteLinearChild(pw, 1);

	auto *set_op = dynamic_cast<LogicalSetOperation *>(pw.plan.get());
	if (!set_op) {
		throw InternalException("Linear UNION rewrite expected LogicalSetOperation");
	}
	set_op->column_count = pw.plan->children[0]->GetColumnBindings().size();
	pw.plan->ResolveOperatorTypes();

	auto union_bindings = pw.plan->GetColumnBindings();
	ColumnBinding new_mul_binding = union_bindings.back();
	return {std::move(pw.plan), new_mul_binding};
}

static AggregateFunction BindAggregateByName(ClientContext &context, const string &name,
                                             const vector<LogicalType> &arg_types) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
	FunctionBinder function_binder(context);
	ErrorData error;
	auto best = function_binder.BindFunction(entry.name, entry.functions, arg_types, error);
	if (!best.IsValid()) {
		throw InternalException("OpenIVM: failed to bind aggregate '%s'", name);
	}
	return entry.functions.GetFunctionByOffset(best.GetIndex());
}

static bool CompactDeltasEnabled(ClientContext &context) {
	Value compact_val;
	if (context.TryGetCurrentSetting("ivm_compact_deltas", compact_val) && !compact_val.IsNull()) {
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
	sum_expr->alias = ivm::MULTIPLICITY_COL;

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
		throw Exception(ExceptionType::CATALOG,
		                "IVM: no snapshot ID recorded for DuckLake table '" + table_name + "' in view '" + view_name +
		                    "' (metadata may be missing — try DROP MATERIALIZED VIEW and recreate)");
	}
	int64_t last_snap = snap_result->GetValue(0, 0).GetValue<int64_t>();

	// Get current snapshot from the old_get's existing DuckLake scan info (no SQL needed).
	auto &old_func_info = old_get->function.function_info->Cast<DuckLakeFunctionInfo>();
	int64_t cur_snap = static_cast<int64_t>(old_func_info.snapshot.snapshot_id);

	int64_t start_snap = last_snap + 1;
	OPENIVM_DEBUG_PRINT("[DuckLake] Snapshot range: %ld -> %ld\n", (long)start_snap, (long)cur_snap);

	if (start_snap > cur_snap) {
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
		return {std::move(empty), mul_binding};
	}

	// DuckLake's table_insertions/deletions functions treat the start snapshot as
	// inclusive, so the next refresh starts at last_snapshot_id + 1.
	// Only snapshot_id is used by DuckLake's file selection logic.
	DuckLakeSnapshot start_snapshot(static_cast<idx_t>(start_snap), DConstants::INVALID_INDEX,
	                                DConstants::INVALID_INDEX, DConstants::INVALID_INDEX);

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
		get->parameters = {Value(catalog_name), Value(schema_name), Value(table_name), Value::BIGINT(start_snap),
		                   Value::BIGINT(cur_snap)};

		get->ResolveOperatorTypes();
		return get;
	};

	auto ins_get = make_change_scan(DuckLakeScanType::SCAN_INSERTIONS, binder.GenerateTableIndex());
	auto del_get = make_change_scan(DuckLakeScanType::SCAN_DELETIONS, binder.GenerateTableIndex());

	// Helper: wrap a scan in a projection that appends a constant multiplicity column.
	// mul_value is the signed Z-set weight: +1 for inserts, -1 for deletes.
	auto add_mul_projection = [&](unique_ptr<LogicalOperator> scan,
	                              int32_t mul_value) -> unique_ptr<LogicalProjection> {
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
		return proj;
	};

	auto ins_proj = add_mul_projection(std::move(ins_get), 1);
	auto del_proj = add_mul_projection(std::move(del_get), -1);

	// UNION ALL the insertions and deletions.
	auto union_types = ins_proj->types;
	idx_t n_output_cols = union_types.size();
	auto union_op =
	    make_uniq<LogicalSetOperation>(binder.GenerateTableIndex(), n_output_cols, std::move(ins_proj),
	                                   std::move(del_proj), LogicalOperatorType::LOGICAL_UNION, true /* setop_all */);
	union_op->types = union_types;

	ColumnBinding mul_binding(union_op->GetColumnBindings().back());
	if (!CompactDeltasEnabled(context)) {
		OPENIVM_DEBUG_PRINT("[DuckLake] Delta compaction disabled\n");
		return RemapDeltaNode(context, std::move(union_op), old_get->table_index, mul_binding);
	}

	auto compacted = CompactDeltaNode(context, binder, std::move(union_op), old_get->table_index, mul_binding);
	OPENIVM_DEBUG_PRINT("[DuckLake] Delta plan built: %zu output cols, mul_binding: table=%lu col=%lu\n",
	                    compacted.node->types.size(), (unsigned long)compacted.mul_binding.table_index,
	                    (unsigned long)compacted.mul_binding.column_index);
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
		string delta_table = OpenIVMUtils::DeltaName(source_table->name);
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
	if (CompactDeltasEnabled(context)) {
		return CompactDeltaNode(context, binder, std::move(delta_get_node), old_get->table_index, new_mul_binding);
	}
	return {std::move(delta_get_node), new_mul_binding};
}

} // namespace duckdb
