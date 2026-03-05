#include "ivm_rule.hpp"
#include "openivm_index_regen.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

DeltaGetResult CreateDeltaGetNode(ClientContext &context, LogicalGet *old_get, const string &view_name) {
	unique_ptr<LogicalGet> delta_get_node;
	ColumnBinding new_mul_binding;
	string table_name;

	optional_ptr<TableCatalogEntry> opt_catalog_entry;
	{
		string delta_table;
		string delta_table_schema;
		string delta_table_catalog;
		if (old_get->GetTable().get() == nullptr) {
			delta_table_schema = "public";
			delta_table_catalog = "p"; // todo
		} else {
			delta_table = "delta_" + old_get->GetTable().get()->name;
			delta_table_schema = old_get->GetTable().get()->schema.name;
			delta_table_catalog = old_get->GetTable().get()->catalog.GetName();
		}
		QueryErrorContext error_context;
		opt_catalog_entry = Catalog::GetEntry<TableCatalogEntry>(
		    context, delta_table_catalog, delta_table_schema, delta_table, OnEntryNotFound::RETURN_NULL, error_context);
		if (opt_catalog_entry == nullptr) {
			throw Exception(ExceptionType::BINDER,
			                "Table " + delta_table + " does not exist, no deltas to compute!");
		}
	}
	auto &table_entry = opt_catalog_entry->Cast<TableCatalogEntry>();
	table_name = table_entry.name;
	unique_ptr<FunctionData> bind_data;
	auto scan_function = table_entry.GetScanFunction(context, bind_data);

	vector<ColumnIndex> column_ids = {};
	idx_t mul_oid = 0, ts_oid = 0, max_oid = 0;
	for (auto &col : table_entry.GetColumns().Logical()) {
		if (col.Name() == "_duckdb_ivm_multiplicity") {
			mul_oid = col.Oid();
		} else if (col.Name() == "_duckdb_ivm_timestamp") {
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
		column_ids.push_back(id);
	}
	column_ids.push_back(ColumnIndex(mul_oid));
	column_ids.push_back(ColumnIndex(ts_oid));

	delta_get_node = make_uniq<LogicalGet>(old_get->table_index, scan_function, std::move(bind_data),
	                                       std::move(return_types), std::move(return_names));
	delta_get_node->SetColumnIds(std::move(column_ids));

	// Timestamp filter
	Connection con(*context.db);
	con.SetAutoCommit(false);
	auto timestamp_query = "select last_update from _duckdb_ivm_delta_tables where view_name = '" + view_name +
	                        "' and table_name = '" + table_name + "';";
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
	return {std::move(delta_get_node), new_mul_binding};
}

unique_ptr<LogicalOperator> FilterDeltaAndProjectOutMul(unique_ptr<LogicalOperator> delta_copy,
                                                        const ColumnBinding &mul_binding, const LogicalType &mul_type,
                                                        idx_t proj_table_index, bool mul_value) {
	auto filter = make_uniq<LogicalFilter>();
	auto mul_ref = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", mul_type, mul_binding);
	auto const_val = make_uniq<BoundConstantExpression>(Value::BOOLEAN(mul_value));
	auto comparison = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, std::move(mul_ref),
	                                                       std::move(const_val));
	filter->expressions.push_back(std::move(comparison));
	filter->children.push_back(std::move(delta_copy));
	filter->ResolveOperatorTypes();

	auto filter_bindings = filter->GetColumnBindings();
	auto filter_types = filter->types;
	vector<unique_ptr<Expression>> proj_expressions;
	for (size_t i = 0; i < filter_bindings.size(); i++) {
		if (filter_bindings[i] != mul_binding) {
			proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(filter_types[i], filter_bindings[i]));
		}
	}
	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_expressions));
	projection->children.push_back(std::move(filter));
	projection->ResolveOperatorTypes();
	return projection;
}

unique_ptr<LogicalOperator> BuildTableOld(ClientContext &context, LogicalGet *original_get, const string &view_name,
                                          Binder &binder) {
	// T_new = copy of original base table scan
	auto r_new = original_get->Copy(context);
	r_new->ResolveOperatorTypes();

	// Create delta GET for this table
	DeltaGetResult delta = CreateDeltaGetNode(context, original_get, view_name);
	auto r_types = r_new->types;
	auto r_col_count = r_new->GetColumnBindings().size();
	auto &delta_mul_binding = delta.mul_binding;
	auto mul_type = LogicalType::BOOLEAN;

	// DR_inserts: filter delta where mul=true, project out mul
	unique_ptr<LogicalOperator> dr_inserts;
	{
		RenumberWrapper res = renumber_and_rebind_subtree(delta.node->Copy(context), binder);
		ColumnBinding dr_mul(res.idx_map[delta_mul_binding.table_index], delta_mul_binding.column_index);
		dr_inserts = FilterDeltaAndProjectOutMul(std::move(res.op), dr_mul, mul_type, binder.GenerateTableIndex(), true);
	}

	// DR_deletes: filter delta where mul=false, project out mul
	unique_ptr<LogicalOperator> dr_deletes;
	{
		RenumberWrapper res = renumber_and_rebind_subtree(delta.node->Copy(context), binder);
		ColumnBinding dr_mul(res.idx_map[delta_mul_binding.table_index], delta_mul_binding.column_index);
		dr_deletes =
		    FilterDeltaAndProjectOutMul(std::move(res.op), dr_mul, mul_type, binder.GenerateTableIndex(), false);
	}

	// T_old = (T_new EXCEPT ALL DR_inserts) UNION ALL DR_deletes
	idx_t except_table_idx = binder.GenerateTableIndex();
	auto r_except = make_uniq<LogicalSetOperation>(except_table_idx, r_col_count, std::move(r_new),
	                                               std::move(dr_inserts), LogicalOperatorType::LOGICAL_EXCEPT, true);
	r_except->types = r_types;

	idx_t union_table_idx = binder.GenerateTableIndex();
	auto r_old = make_uniq<LogicalSetOperation>(union_table_idx, r_col_count, std::move(r_except),
	                                            std::move(dr_deletes), LogicalOperatorType::LOGICAL_UNION, true);
	r_old->types = r_types;
	return r_old;
}

vector<unique_ptr<Expression>> ProjectMultiplicityToEnd(const vector<ColumnBinding> &bindings,
                                                        const vector<LogicalType> &types,
                                                        const ColumnBinding &mul_binding) {
	D_ASSERT(bindings.size() == types.size());
	const size_t col_count = bindings.size();
	auto result = vector<unique_ptr<Expression>>(col_count);

	bool mul_is_seen = false;
	for (idx_t i = 0; i < col_count; ++i) {
		auto binding = bindings[i];
		auto bound_col_ref = make_uniq<BoundColumnRefExpression>(types[i], binding);
		if (binding == mul_binding) {
			mul_is_seen = true;
			result[col_count - 1] = std::move(bound_col_ref);
		} else {
			const size_t insert_at = mul_is_seen ? i - 1 : i;
			result[insert_at] = std::move(bound_col_ref);
		}
	}
	return result;
}

} // namespace duckdb
