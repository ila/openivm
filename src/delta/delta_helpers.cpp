#include "duckdb/planner/filter/expression_filter.hpp"
#include "delta/delta_helpers.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/plan_rewrite_internal.hpp"
#include "core/sql_utils.hpp"
#include "core/refresh_metadata.hpp"
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
#include "storage/ducklake_table_entry.hpp"

namespace duckdb {

void PopulateDuckLakeChangeScanParameters(LogicalGet &get) {
	if (get.function.GetName().GetIdentifierName() != "ducklake_scan" || !get.function.function_info) {
		return;
	}
	auto &info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
	if (info.scan_type != DuckLakeScanType::SCAN_INSERTIONS && info.scan_type != DuckLakeScanType::SCAN_DELETIONS) {
		return;
	}
	D_ASSERT(info.start_snapshot);
	get.parameters = {Value(info.table.ParentCatalog().GetName()), Value(info.table.schema.name),
	                  Value(info.table_name), Value::BIGINT(static_cast<int64_t>(info.start_snapshot->snapshot_id)),
	                  Value::BIGINT(static_cast<int64_t>(info.snapshot.snapshot_id))};
}

DeltaSourceState &LoadDeltaWatermark(Connection &con, const string &view_name, TableCatalogEntry &source,
                                     DeltaSourceCache &cache) {
	auto &state = cache[&source];
	if (state.watermark_loaded) {
		return state;
	}
	state.watermark_loaded = true;
	state.delta_key = SqlUtils::DeltaName(source.name.GetIdentifierName());
	auto result = con.Query("SELECT last_update, table_name FROM " + string(openivm::DELTA_TABLES_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND " +
	                        RefreshMetadata::SourcePredicate(state.delta_key,
	                                                         source.ParentCatalog().GetName().GetIdentifierName(),
	                                                         source.schema.name.GetIdentifierName()));
	if (result->HasError()) {
		state.watermark_error = result->GetError();
	} else if (result->RowCount() > 0) {
		state.last_update = result->Collection().GetValue(0, 0);
		state.delta_key = result->Collection().GetValue(1, 0).ToString();
	}
	return state;
}

static DeltaGetResult RemapDeltaNode(ClientContext &context, unique_ptr<LogicalOperator> delta_node,
                                     TableIndex output_table_index, ColumnBinding mul_binding) {
	auto input_bindings = delta_node->GetColumnBindings();
	auto input_types = delta_node->types;
	// Logical operators expose one type per binding; equality is the required valid shape.
	if (input_bindings.empty() || input_bindings.size() != input_types.size()) { // mull-ignore: cxx_ne_to_eq
		throw InternalException("Delta remap requires matching non-empty binding and type vectors");
	}
	vector<unique_ptr<Expression>> remap_exprs;
	// >= disables the remap and <= indexes one past both bounded vectors.
	for (idx_t i = 0; i < input_bindings.size(); i++) { // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
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

	// Multiplicity is appended by both delta producers and is therefore the final binding.
	ColumnBinding remapped_mul {output_table_index,
	                            ProjectionIndex(input_bindings.size() - 1)}; // mull-ignore: cxx_sub_to_add
	return {std::move(remap_proj), remapped_mul};
}

// Copy source's pushed-down table filters onto target. OPTIONAL_FILTER is advisory scan pruning; copying or omitting it
// cannot change query results.
static void CopyTableFilters(const LogicalGet &source, LogicalGet &target) {
	for (auto &entry : source.table_filters) {
		if (!ExpressionFilter::IsOptionalFilter(entry.Filter())) {
			target.table_filters.PushFilter(entry.GetIndex(),
			                                ExpressionFilter::GetExpressionFilter(entry.Filter(), "Delta scan").Copy());
		}
	}
	for (auto &filter : source.table_filters.GetMultiColumnFilters()) {
		if (!ExpressionFilter::IsOptionalFilter(*filter)) {
			target.table_filters.PushMultiColumnFilter(
			    ExpressionFilter::GetExpressionFilter(*filter, "Delta scan").Copy());
		}
	}
}

static DeltaGetResult CompactDeltaNode(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> delta_node,
                                       TableIndex output_table_index, ColumnBinding mul_binding) {
	auto input_bindings = delta_node->GetColumnBindings();
	auto input_types = delta_node->types;
	if (input_bindings.empty() || input_bindings.size() != input_types.size()) {
		throw InternalException("Delta compaction requires matching non-empty binding and type vectors");
	}
	idx_t base_col_count = input_bindings.size() - 1;

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(MakeAggregate(context, "sum",
	                                   make_uniq<BoundColumnRefExpression>(input_types[base_col_count], mul_binding),
	                                   openivm::MULTIPLICITY_COL));
	vector<unique_ptr<Expression>> groups;
	// The <= mutant includes the multiplicity aggregate itself as a grouping key.
	for (idx_t i = 0; i < base_col_count; i++) { // mull-ignore: cxx_lt_to_le
		groups.push_back(make_uniq<BoundColumnRefExpression>(input_types[i], input_bindings[i]));
	}
	auto aggregate = MakeGroupedAggregate(binder, std::move(groups), std::move(aggregates), std::move(delta_node));

	auto agg_bindings = aggregate->GetColumnBindings();
	auto agg_types = aggregate->types;
	auto filter_expr = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_NOTEQUAL,
	    make_uniq<BoundColumnRefExpression>(agg_types[base_col_count], agg_bindings[base_col_count]),
	    make_uniq<BoundConstantExpression>(Value::INTEGER(0)));
	auto filter = make_uniq<LogicalFilter>(std::move(filter_expr));
	filter->children.push_back(std::move(aggregate));
	filter->ResolveOperatorTypes();

	auto filter_bindings = filter->GetColumnBindings();
	vector<unique_ptr<Expression>> remap_exprs;
	// The <= mutant emits the aggregate multiplicity once here and once below.
	for (idx_t i = 0; i < base_col_count; i++) { // mull-ignore: cxx_lt_to_le
		remap_exprs.push_back(make_uniq<BoundColumnRefExpression>(agg_types[i], filter_bindings[i]));
	}
	remap_exprs.push_back(BoundCastExpression::AddCastToType(
	    context, make_uniq<BoundColumnRefExpression>(agg_types[base_col_count], filter_bindings[base_col_count]),
	    LogicalType::INTEGER));

	auto remap_proj = make_uniq<LogicalProjection>(output_table_index, std::move(remap_exprs));
	remap_proj->children.push_back(std::move(filter));
	remap_proj->ResolveOperatorTypes();

	ColumnBinding compact_mul_binding {output_table_index, ProjectionIndex(base_col_count)};
	return {std::move(remap_proj), compact_mul_binding};
}

// ============================================================================
// DuckLake delta scan: direct plan construction using native catalog types
// ============================================================================

/// Build a DuckLake delta scan by directly constructing LogicalGet nodes
/// with SCAN_INSERTIONS / SCAN_DELETIONS, avoiding SQL string round-trips.
static DeltaGetResult CreateDuckLakeDeltaNode(ClientContext &context, Connection &con, Binder &binder,
                                              LogicalGet *old_get, const string &view_name, DeltaSourceState &state,
                                              bool honor_source_projection) {
	// Copied DuckDB 2.0 scans can expose their projected bindings before their
	// cached type vector has been resolved. The no-snapshot-change fast path must
	// have the same visible layout as a real change scan, including multiplicity.
	old_get->ResolveOperatorTypes();
	auto source_bindings = old_get->GetColumnBindings();
	if (old_get->types.size() != source_bindings.size()) {
		throw InternalException("DuckLake delta source produced %zu types for %zu bindings", old_get->types.size(),
		                        source_bindings.size());
	}
	auto table_ref = old_get->GetTable();
	string catalog_name = table_ref->ParentCatalog().GetName().GetIdentifierName();
	string schema_name = table_ref->schema.name.GetIdentifierName();
	string table_name = table_ref.get()->name.GetIdentifierName();

	OPENIVM_DEBUG_PRINT("[DuckLake] Creating delta node for '%s.%s'\n", catalog_name.c_str(), table_name.c_str());
	OPENIVM_DEBUG_PRINT("[DuckLake] Source scan columns=%zu projections=", old_get->GetColumnIds().size());
	for (auto projection : old_get->projection_ids) {
		OPENIVM_DEBUG_PRINT(" %zu", static_cast<size_t>(projection.GetIndex()));
	}
	OPENIVM_DEBUG_PRINT("\n");

	// Reuse the compiler metadata connection, including its transaction-local snapshot.
	if (!state.has_last_snapshot) {
		auto snap_result = con.Query("SELECT last_snapshot_id FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                             " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND " +
		                             RefreshMetadata::SourcePredicate(table_name, catalog_name, schema_name));
		if (snap_result->HasError()) {
			throw Exception(ExceptionType::CATALOG, "IVM: could not read DuckLake snapshot metadata for view '" +
			                                            view_name + "': " + snap_result->GetError());
		}
		if (snap_result->RowCount() == 0 || snap_result->Collection().GetValue(0, 0).IsNull()) {
			throw Exception(ExceptionType::CATALOG,
			                "IVM: no snapshot ID recorded for DuckLake table '" + table_name + "' in view '" +
			                    view_name + "' (metadata may be missing — try DROP MATERIALIZED VIEW and recreate)");
		}
		state.last_snapshot = snap_result->Collection().GetValue(0, 0).GetValue<int64_t>();
		state.has_last_snapshot = true;
	}
	int64_t last_snap = state.last_snapshot;

	// Get current snapshot from the old_get's existing DuckLake scan info (no SQL needed).
	auto &old_func_info = old_get->function.function_info->Cast<DuckLakeFunctionInfo>();
	int64_t cur_snap = static_cast<int64_t>(old_func_info.snapshot.snapshot_id);

	// DuckLake change scans use an inclusive lower bound, so subtraction would replay already-applied snapshots.
	int64_t start_snap = last_snap + 1; // mull-ignore: cxx_add_to_sub
	OPENIVM_DEBUG_PRINT("[DuckLake] Snapshot range: %ld -> %ld\n", (long)start_snap, (long)cur_snap);

	if (start_snap > cur_snap) {
		vector<LogicalType> empty_types = old_get->types;
		empty_types.push_back(LogicalType::INTEGER);
		vector<ColumnBinding> bindings;
		auto table_index = binder.GenerateTableIndex();
		// >= leaves the empty result without bindings and <= creates one more binding than types.
		for (idx_t i = 0; i < empty_types.size(); i++) { // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
			bindings.emplace_back(table_index, ProjectionIndex(i));
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

	// Keep the source scan's physical layout intact. In particular, DuckDB can retain a
	// filter-only column in column_ids while hiding it through projection_ids. Parent
	// join projection maps index the projected layout, so exposing every physical column
	// here would shift those maps onto the wrong values.
	vector<ColumnIndex> delta_col_ids;
	ColumnIndex virtual_fallback(0);
	for (auto &id : old_get->GetColumnIds()) {
		if (!id.IsVirtualColumn()) {
			virtual_fallback = id;
			break;
		}
	}
	for (auto &id : old_get->GetColumnIds()) {
		// DuckLake change scans do not expose virtual ROWID columns. A physical
		// placeholder preserves binding positions for operators such as COUNT(*) that
		// only need a non-NULL row token; explicit ROWID queries are not incrementalized.
		delta_col_ids.push_back(id.IsVirtualColumn() ? virtual_fallback : id);
	}

	// Helper: create a LogicalGet configured as a DuckLake change scan.
	auto make_change_scan = [&](DuckLakeScanType scan_type, TableIndex table_idx) -> unique_ptr<LogicalGet> {
		unique_ptr<FunctionData> bind_data;
		EntryLookupInfo lookup(CatalogType::TABLE_ENTRY, QualifiedName(Identifier(table_name)), QueryErrorContext());
		auto scan_fn = table_ref->Cast<TableCatalogEntry>().GetScanFunction(context, bind_data, lookup);

		auto &func_info = scan_fn.function_info->Cast<DuckLakeFunctionInfo>();
		func_info.scan_type = scan_type;
		func_info.start_snapshot = make_uniq<DuckLakeSnapshot>(start_snapshot);

		// Capture column metadata before moving scan_fn into the LogicalGet.
		auto col_types = func_info.column_types;
		auto col_names = func_info.column_names;

		auto get = make_uniq<LogicalGet>(table_idx, BoundTableFunction(scan_fn), std::move(bind_data),
		                                 std::move(col_types), StringsToIdentifiers(col_names));
		get->SetColumnIds(vector<ColumnIndex>(delta_col_ids));
		CopyTableFilters(*old_get, *get);
		if (honor_source_projection && !old_get->projection_ids.empty()) {
			get->projection_ids = old_get->projection_ids;
		}

		// Set parameters so LPTS can reconstruct the ducklake_table_insertions/deletions SQL.
		PopulateDuckLakeChangeScanParameters(*get);

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
	if (!SqlUtils::GetBoolSetting(context, "openivm_compact_deltas", true)) {
		OPENIVM_DEBUG_PRINT("[DuckLake] Delta compaction disabled\n");
		return RemapDeltaNode(context, std::move(union_op), old_get->table_index, mul_binding);
	}

	auto compacted = CompactDeltaNode(context, binder, std::move(union_op), old_get->table_index, mul_binding);
	OPENIVM_DEBUG_PRINT("[DuckLake] Delta plan built: %zu output cols, mul_binding: table=%lu col=%lu\n",
	                    compacted.node->types.size(), (unsigned long)compacted.mul_binding.table_index.index,
	                    (unsigned long)compacted.mul_binding.column_index.GetIndex());
	return compacted;
}

// ============================================================================
// Standard DuckDB delta scan (existing logic)
// ============================================================================

DeltaGetResult CreateDeltaGetNode(ClientContext &context, Connection &con, Binder &binder, LogicalGet *old_get,
                                  const string &view_name, DeltaSourceCache *cache, bool honor_source_projection) {
	DeltaSourceCache local_cache;
	auto &sources = cache ? *cache : local_cache;
	// DuckLake tables: use native change tracking via direct plan construction
	auto table_ref = old_get->GetTable();
	if (table_ref.get() && table_ref->ParentCatalog().GetCatalogType() == "ducklake") {
		return CreateDuckLakeDeltaNode(context, con, binder, old_get, view_name, sources[table_ref.get()],
		                               honor_source_projection);
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
	                    view_name.c_str(), (unsigned long)old_get->table_index.index);

	auto *source_table = old_get->GetTable().get();
	if (!source_table) {
		throw Exception(ExceptionType::BINDER,
		                "IVM: cannot create a delta scan for a table function in view '" + view_name + "'");
	}

	// Metadata keys the delta by its source; the physical table may sit in an internal schema.
	string delta_key = SqlUtils::DeltaName(source_table->name.GetIdentifierName());
	auto &source_state = sources[source_table];
	if (!source_state.delta_entry) {
		source_state.delta_entry = RefreshMetadata::FindSourceDelta(
		    context, source_table->catalog.GetName().GetIdentifierName(), source_table->schema.name.GetIdentifierName(),
		    source_table->name.GetIdentifierName());
		if (!source_state.delta_entry) {
			throw Exception(ExceptionType::BINDER, "Table " + delta_key + " does not exist, no deltas to compute!");
		}
	}
	auto &table_entry = *source_state.delta_entry;
	table_name = table_entry.name.GetIdentifierName();
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
		// Equality only reassigns max_oid to itself.
		if (col.Oid() > max_oid) { // mull-ignore: cxx_gt_to_ge
			max_oid = col.Oid();
		}
	}

	vector<LogicalType> return_types(max_oid + 1, LogicalType::ANY);
	vector<Identifier> return_names(max_oid + 1, "");
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
			// Replacement and ALTER can change source ordinals without changing
			// the retained delta layout. Column identity is its name, not its slot.
			auto &column = table_entry.GetColumn(old_get->GetColumnName(id));
			ColumnIndex mapped(column.Oid(), id.GetChildIndexes());
			if (id.HasType()) {
				if (id.IsPushdownExtract()) {
					mapped.SetPushdownExtractType(id.GetType());
				} else {
					mapped.SetType(id.GetType());
				}
			}
			column_ids.push_back(std::move(mapped));
		}
	}
	column_ids.push_back(ColumnIndex(mul_oid));
	column_ids.push_back(ColumnIndex(ts_oid));

	delta_get_node = make_uniq<LogicalGet>(old_get->table_index, BoundTableFunction(scan_function),
	                                       std::move(bind_data), std::move(return_types), std::move(return_names));
	delta_get_node->SetColumnIds(std::move(column_ids));
	CopyTableFilters(*old_get, *delta_get_node);

	// Timestamp filter
	auto &watermark = LoadDeltaWatermark(con, view_name, *source_table, sources);
	if (!watermark.watermark_error.empty()) {
		throw Exception(ExceptionType::EXECUTOR, "IVM: failed to read last_update for view '" + view_name +
		                                             "', table '" + table_name + "': " + watermark.watermark_error);
	}
	if (watermark.last_update.IsNull()) {
		throw Exception(ExceptionType::CATALOG,
		                "IVM: missing last_update for view '" + view_name + "', table '" + table_name + "'");
	}
	auto ts_value = watermark.last_update;
	if (ts_value.type() != LogicalType::TIMESTAMP) {
		ts_value = ts_value.DefaultCastAs(LogicalType::TIMESTAMP);
	}
	auto table_filter =
	    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_GREATERTHANOREQUALTO, ts_value);
	delta_get_node->table_filters.PushFilter(ProjectionIndex(delta_get_node->GetColumnIds().size() - 1),
	                                         std::move(table_filter));

	// A recursive scan rewrite must retain the original binding positions because
	// ancestor expressions still reference those positions. A join leaf instead
	// uses projection maps and must not expose columns needed only by pushed-down
	// filters, or those columns shift multiplicity in the join term.
	delta_get_node->projection_ids.clear();
	const idx_t scanned_source_columns = old_get->GetColumnIds().size();
	if (!honor_source_projection || old_get->projection_ids.empty()) {
		// The <= mutant projects the appended multiplicity column twice.
		for (idx_t i = 0; i < scanned_source_columns; i++) { // mull-ignore: cxx_lt_to_le
			delta_get_node->projection_ids.push_back(ProjectionIndex(i));
		}
	} else {
		delta_get_node->projection_ids.insert(delta_get_node->projection_ids.end(), old_get->projection_ids.begin(),
		                                      old_get->projection_ids.end());
	}
	delta_get_node->projection_ids.push_back(ProjectionIndex(scanned_source_columns)); // mul column

	delta_get_node->ResolveOperatorTypes();
	new_mul_binding = delta_get_node->GetColumnBindings().back();
	OPENIVM_DEBUG_PRINT("[CreateDeltaGet] Delta table: %s, mul_binding: table=%lu col=%lu, columns: %zu\n",
	                    table_name.c_str(), (unsigned long)new_mul_binding.table_index.index,
	                    (unsigned long)new_mul_binding.column_index.GetIndex(), delta_get_node->GetColumnIds().size());
	if (SqlUtils::GetBoolSetting(context, "openivm_compact_deltas", true)) {
		return CompactDeltaNode(context, binder, std::move(delta_get_node), old_get->table_index, new_mul_binding);
	}
	return {std::move(delta_get_node), new_mul_binding};
}

} // namespace duckdb
