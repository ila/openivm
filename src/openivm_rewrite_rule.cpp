#include "openivm_rewrite_rule.hpp"

#include "logical_plan_to_sql.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_insert.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb.hpp"
#include "openivm_index_regen.hpp"
#include "openivm_debug.hpp"

#include <iostream>

namespace {
using duckdb::BoundColumnRefExpression;
using duckdb::ColumnBinding;
using duckdb::Expression;
using duckdb::JoinCondition;
using duckdb::LogicalComparisonJoin;
using duckdb::LogicalOperator;
using duckdb::LogicalType;
using duckdb::make_uniq;
using duckdb::unique_ptr;
using duckdb::vector;

#if OPENIVM_DEBUG
void print_column_bindings(const unique_ptr<LogicalComparisonJoin> &join) {
	const auto join_bindings = join->GetColumnBindings();
	const auto lc_bindings = join->children[0]->GetColumnBindings();
	const auto rc_bindings = join->children[1]->GetColumnBindings();
	const size_t join_cb_count = join_bindings.size();
	const size_t lc_cb_count = lc_bindings.size();
	const size_t rc_cb_count = rc_bindings.size();

	OPENIVM_DEBUG_PRINT("Join CB count: %zu (left child: %zu, right child: %zu)\n", join_cb_count, lc_cb_count,
	                    rc_cb_count);
	for (size_t i = 0; i < lc_cb_count; i++) {
		OPENIVM_DEBUG_PRINT("Left child CB after %zu %s\n", i, join_bindings[i].ToString().c_str());
	}
	for (size_t i = 0; i < rc_cb_count; i++) {
		OPENIVM_DEBUG_PRINT("Right child CB after %zu %s\n", i, join_bindings[i].ToString().c_str());
	}
	for (size_t i = 0; i < join_cb_count; i++) {
		OPENIVM_DEBUG_PRINT("Join CB after %zu %s\n", i, join_bindings[i].ToString().c_str());
	}
}
#endif

unique_ptr<LogicalComparisonJoin> create_empty_join(duckdb::ClientContext &context,
                                                    const unique_ptr<LogicalComparisonJoin> &current_join) {
	unique_ptr<LogicalComparisonJoin> copied_join =
	    duckdb::unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(current_join->Copy(context));
	copied_join->children[0].reset(nullptr);
	copied_join->children[1].reset(nullptr);
	copied_join->expressions.clear();
	copied_join->left_projection_map.clear();
	copied_join->right_projection_map.clear();
	return copied_join;
}

void rebind_bcr_if_needed(BoundColumnRefExpression &bcr, const std::unordered_map<idx_t, idx_t> &idx_map) {
	const idx_t table_index = bcr.binding.table_index;
	if (idx_map.find(table_index) != idx_map.end()) {
		bcr.binding.table_index = idx_map.at(table_index);
	}
}

vector<JoinCondition> rebind_join_conditions(const vector<JoinCondition> &original_conditions,
                                             const std::unordered_map<idx_t, idx_t> &idx_map) {
	vector<JoinCondition> return_vec;
	return_vec.reserve(original_conditions.size());
	for (const JoinCondition &cond : original_conditions) {
		unique_ptr<Expression> i_left = cond.left->Copy();
		unique_ptr<Expression> i_right = cond.right->Copy();
		if (cond.left->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			auto &left_bcr = i_left->Cast<BoundColumnRefExpression>();
			rebind_bcr_if_needed(left_bcr, idx_map);
		}
		if (cond.right->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			auto &right_bcr = i_right->Cast<BoundColumnRefExpression>();
			rebind_bcr_if_needed(right_bcr, idx_map);
		}
		JoinCondition new_condition = JoinCondition();
		new_condition.left = std::move(i_left);
		new_condition.right = std::move(i_right);
		new_condition.comparison = cond.comparison;
		return_vec.emplace_back(std::move(new_condition));
	}
	return return_vec;
}

vector<unique_ptr<Expression>> project_multiplicity_to_end(const vector<ColumnBinding> &bindings,
                                                           const vector<LogicalType> &types,
                                                           const ColumnBinding &mul_binding) {
	assert(bindings.size() == types.size());
	const size_t col_count = bindings.size();
	auto projection_col_refs = vector<unique_ptr<Expression>>(col_count);

	bool mul_is_seen = false;
	for (idx_t i = 0; i < col_count; ++i) {
		auto binding = bindings[i];
		unique_ptr<BoundColumnRefExpression> bound_col_ref = make_uniq<BoundColumnRefExpression>(types[i], binding);
		if (binding == mul_binding) {
			mul_is_seen = true;
			projection_col_refs[col_count - 1] = std::move(bound_col_ref);
		} else {
			const size_t insert_at = mul_is_seen ? i - 1 : i;
			projection_col_refs[insert_at] = std::move(bound_col_ref);
		}
	}
	return projection_col_refs;
}

vector<unique_ptr<Expression>> project_out_duplicate_mul_column(const vector<ColumnBinding> &bindings,
                                                                const vector<LogicalType> &types,
                                                                const ColumnBinding &redundant_mul_binding) {
	const size_t col_count = bindings.size();
	assert(col_count == types.size());

	auto projection_col_refs = vector<unique_ptr<Expression>>();
	projection_col_refs.reserve(col_count - 1);
	for (idx_t i = 0; i < col_count; ++i) {
		const auto &binding = bindings[i];
		if (binding != redundant_mul_binding) {
			projection_col_refs.emplace_back(make_uniq<BoundColumnRefExpression>(types[i], binding));
		}
	}
	return projection_col_refs;
}

vector<unique_ptr<Expression>> bindings_to_expressions(const vector<ColumnBinding> &bindings,
                                                       const vector<LogicalType> &types) {
	const size_t col_count = bindings.size();
	assert(col_count == types.size());

	auto projection_col_refs = vector<unique_ptr<Expression>>();
	projection_col_refs.reserve(col_count);
	for (idx_t i = 0; i < col_count; ++i) {
		projection_col_refs.emplace_back(make_uniq<BoundColumnRefExpression>(types[i], bindings[i]));
	}
	return projection_col_refs;
}

} // namespace

namespace duckdb {

void IVMRewriteRule::AddInsertNode(ClientContext &context, unique_ptr<LogicalOperator> &plan, string &view_name,
                                   string &view_catalog_name, string &view_schema_name) {
#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("\nAdd the insert node to the plan...\n");
	OPENIVM_DEBUG_PRINT("Plan:\n%s\nParameters:", plan->ToString().c_str());
	for (const auto &i_param : plan->ParamsToString()) {
		OPENIVM_DEBUG_PRINT("%s", i_param.second.c_str());
	}
	OPENIVM_DEBUG_PRINT("\n---end of insert node output---\n");
#endif

	auto table =
	    Catalog::GetEntry<TableCatalogEntry>(context, view_catalog_name, view_schema_name, "delta_" + view_name,
	                                         OnEntryNotFound::THROW_EXCEPTION, QueryErrorContext());
	auto insert_node = make_uniq<LogicalInsert>(*table, 999);

	Value value;
	unique_ptr<BoundConstantExpression> exp;
	for (size_t i = 0; i < plan->expressions.size(); i++) {
		insert_node->expected_types.emplace_back(plan->expressions[i]->return_type);
		value = Value(plan->expressions[i]->return_type);
		exp = make_uniq<BoundConstantExpression>(std::move(value));
		insert_node->bound_defaults.emplace_back(std::move(exp));
	}

	insert_node->children.emplace_back(std::move(plan));
	plan = std::move(insert_node);
}

ModifiedPlan IVMRewriteRule::ModifyPlan(PlanWrapper pw) {
	ClientContext &context = pw.input.context;
	const vector<ColumnBinding> original_bindings = pw.plan->GetColumnBindings();

	unique_ptr<LogicalOperator> left_child, right_child;
	if (pw.plan.get()->type == LogicalOperatorType::LOGICAL_JOIN ||
	    pw.plan.get()->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		left_child = pw.plan->children[0]->Copy(context);
		right_child = pw.plan->children[1]->Copy(context);
		left_child->ResolveOperatorTypes();
		right_child->ResolveOperatorTypes();
	}
	vector<ColumnBinding> child_mul_bindings;
	for (auto &&child : pw.plan->children) {
		auto rec_pw = PlanWrapper(pw.input, child, pw.view, pw.root);
		ModifiedPlan child_plan = ModifyPlan(rec_pw);
		child = std::move(child_plan.op);
		child_mul_bindings.emplace_back(child_plan.mul_binding);
	}
	QueryErrorContext error_context = QueryErrorContext();

	switch (pw.plan->type) {
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
		vector<ColumnBinding> modified_plan_bindings = pw.plan->GetColumnBindings();
		auto types = pw.plan->types;
		types.emplace_back(pw.mul_type);
		OPENIVM_DEBUG_PRINT("Modified plan (join, start):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto &i_param : pw.plan->ParamsToString()) {
			OPENIVM_DEBUG_PRINT("%s", i_param.second.c_str());
		}
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("join detected. join child count: %zu\n", pw.plan->children.size());
		OPENIVM_DEBUG_PRINT("plan left_child child count: %zu\n", left_child->children.size());
		OPENIVM_DEBUG_PRINT("plan right_child child count: %zu\n", right_child->children.size());
#endif
		unique_ptr<LogicalComparisonJoin> plan_as_join =
		    unique_ptr_cast<LogicalOperator, LogicalComparisonJoin>(std::move(pw.plan));
		if (plan_as_join->join_type != JoinType::INNER) {
			throw Exception(ExceptionType::OPTIMIZER,
			                JoinTypeToString(plan_as_join->join_type) + " type not yet supported in OpenIVM");
		}
		const unique_ptr<LogicalOperator> &delta_left = plan_as_join->children[0];
		const unique_ptr<LogicalOperator> &delta_right = plan_as_join->children[1];
		const ColumnBinding org_dl_mul = child_mul_bindings[0];
		const ColumnBinding org_dr_mul = child_mul_bindings[1];

		unique_ptr<LogicalProjection> dl_r_projected;
		// dLR
		{
			RenumberWrapper res = renumber_and_rebind_subtree(delta_left->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dl = std::move(res.op);
			unique_ptr<LogicalOperator> r = right_child->Copy(context);

			unique_ptr<LogicalComparisonJoin> dl_r = create_empty_join(context, plan_as_join);
			dl_r->conditions = rebind_join_conditions(plan_as_join->conditions, res.idx_map);
			dl_r->children[0] = std::move(dl);
			dl_r->children[1] = std::move(r);
			dl_r->ResolveOperatorTypes();
#if OPENIVM_DEBUG
			OPENIVM_DEBUG_PRINT("--- Column bindings of dL JOIN R after modifications (but before projection) ---\n");
			print_column_bindings(dl_r);
#endif
			{
				const vector<ColumnBinding> join_bindings = dl_r->GetColumnBindings();
				const vector<LogicalType> join_types = dl_r->types;
				const ColumnBinding dl_mul_binding = {res.idx_map[org_dl_mul.table_index], org_dl_mul.column_index};
				vector<unique_ptr<Expression>> dl_r_projection_bindings =
				    project_multiplicity_to_end(join_bindings, join_types, dl_mul_binding);
				dl_r_projected = make_uniq<LogicalProjection>(pw.input.optimizer.binder.GenerateTableIndex(),
				                                              std::move(dl_r_projection_bindings));
			}
			dl_r_projected->children.emplace_back(std::move(dl_r));
		}
		// LdR
		unique_ptr<LogicalProjection> l_dr_projected;
		{
			unique_ptr<LogicalOperator> l = left_child->Copy(context);
			RenumberWrapper res = renumber_and_rebind_subtree(delta_right->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dr = std::move(res.op);

			unique_ptr<LogicalComparisonJoin> l_dr = create_empty_join(context, plan_as_join);
			l_dr->conditions = rebind_join_conditions(plan_as_join->conditions, res.idx_map);
			l_dr->children[0] = std::move(l);
			l_dr->children[1] = std::move(dr);
			l_dr->ResolveOperatorTypes();
#if OPENIVM_DEBUG
			OPENIVM_DEBUG_PRINT("--- Column bindings of L JOIN dR after modifications (but before projection) ---\n");
			print_column_bindings(l_dr);
#endif
			{
				const vector<ColumnBinding> join_bindings = l_dr->GetColumnBindings();
				const vector<LogicalType> join_types = l_dr->types;
				vector<unique_ptr<Expression>> l_dr_projection_bindings =
				    bindings_to_expressions(join_bindings, join_types);
				l_dr_projected = make_uniq<LogicalProjection>(pw.input.optimizer.binder.GenerateTableIndex(),
				                                              std::move(l_dr_projection_bindings));
			}
			l_dr_projected->children.emplace_back(std::move(l_dr));
		}
		// dLdR
		unique_ptr<LogicalProjection> dl_dr_projected;
		{
			RenumberWrapper dl_res = renumber_and_rebind_subtree(delta_left->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dl = std::move(dl_res.op);
			RenumberWrapper dr_res = renumber_and_rebind_subtree(delta_right->Copy(context), pw.input.optimizer.binder);
			unique_ptr<LogicalOperator> dr = std::move(dr_res.op);
			unique_ptr<LogicalComparisonJoin> dl_dr = create_empty_join(context, plan_as_join);
			{
				std::unordered_map<old_idx, new_idx> idx_map = dl_res.idx_map;
				for (const auto &pair : dr_res.idx_map) {
					idx_map.insert(pair);
				}
				dl_dr->conditions = rebind_join_conditions(plan_as_join->conditions, idx_map);
			}
			{
				ColumnBinding dl_mul_binding = {dl_res.idx_map[org_dl_mul.table_index], org_dl_mul.column_index};
				ColumnBinding dr_mul_binding = {dr_res.idx_map[org_dr_mul.table_index], org_dr_mul.column_index};
				JoinCondition mul_equal_condition;
				mul_equal_condition.left =
				    make_uniq<BoundColumnRefExpression>("left_mul", pw.mul_type, dl_mul_binding, 0);
				mul_equal_condition.right =
				    make_uniq<BoundColumnRefExpression>("right_mul", pw.mul_type, dr_mul_binding, 0);
				mul_equal_condition.comparison = ExpressionType::COMPARE_EQUAL;
				dl_dr->conditions.emplace_back(std::move(mul_equal_condition));
			}
			dl_dr->children[0] = std::move(dl);
			dl_dr->children[1] = std::move(dr);
			dl_dr->ResolveOperatorTypes();
#if OPENIVM_DEBUG
			OPENIVM_DEBUG_PRINT("--- Column bindings of dL JOIN dR after modifications (but before projection) ---\n");
			print_column_bindings(dl_dr);
#endif
			const vector<ColumnBinding> join_bindings = dl_dr->GetColumnBindings();
			{
				const vector<LogicalType> join_types = dl_dr->types;
				const ColumnBinding dl_mul_binding = {dl_res.idx_map[org_dl_mul.table_index], org_dl_mul.column_index};
				auto dl_dr_projection_bindings =
				    project_out_duplicate_mul_column(join_bindings, join_types, dl_mul_binding);
				dl_dr_projected = make_uniq<LogicalProjection>(pw.input.optimizer.binder.GenerateTableIndex(),
				                                               std::move(dl_dr_projection_bindings));
			}
			dl_dr_projected->children.emplace_back(std::move(dl_dr));
		}
		auto copy_union = make_uniq<LogicalSetOperation>(pw.input.optimizer.binder.GenerateTableIndex(), types.size(),
		                                                 std::move(dl_r_projected), std::move(l_dr_projected),
		                                                 LogicalOperatorType::LOGICAL_UNION, true);
		copy_union->types = types;
		auto upper_u_table_index = pw.input.optimizer.binder.GenerateTableIndex();
		pw.plan = make_uniq<LogicalSetOperation>(upper_u_table_index, types.size(), std::move(copy_union),
		                                         std::move(dl_dr_projected), LogicalOperatorType::LOGICAL_UNION, true);
		pw.plan->types = types;
		OPENIVM_DEBUG_PRINT("Modified plan (join, end):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto &i_param : pw.plan->ParamsToString()) {
			OPENIVM_DEBUG_PRINT("%s", i_param.second.c_str());
		}
		ColumnBinding new_mul_binding;
		{
			auto union_bindings = pw.plan->GetColumnBindings();
			if (union_bindings.size() - original_bindings.size() != 1) {
				throw InternalException(
				    "Union (with multiplicity column) should have exactly 1 more binding than original join!");
			}
			ColumnBindingReplacer replacer;
			vector<ReplacementBinding> &replacement_bindings = replacer.replacement_bindings;
			for (idx_t col_idx = 0; col_idx < original_bindings.size(); ++col_idx) {
				const auto &old_binding = original_bindings[col_idx];
				const auto &new_binding = union_bindings[col_idx];
				replacement_bindings.emplace_back(old_binding, new_binding);
			}
#if OPENIVM_DEBUG
			OPENIVM_DEBUG_PRINT("\n--- Running a ColumnBindingReplacer after the Union ---\n");
			for (const auto &i_binding : replacement_bindings) {
				OPENIVM_DEBUG_PRINT("old binding %s -> ", (i_binding.old_binding.ToString().c_str()));
				OPENIVM_DEBUG_PRINT("new binding %s\n", (i_binding.new_binding.ToString().c_str()));
			}
#endif
			replacer.stop_operator = pw.plan;
			replacer.VisitOperator(*pw.root);
			new_mul_binding = union_bindings[union_bindings.size() - 1];
#if OPENIVM_DEBUG
			OPENIVM_DEBUG_PRINT("The new multiplicity binding shall be %s.\n", (new_mul_binding.ToString().c_str()));
			OPENIVM_DEBUG_PRINT("--- End of ColumnBindingReplacer ---\n");
#endif
		}
		return {std::move(pw.plan), new_mul_binding};
	}
	case LogicalOperatorType::LOGICAL_GET: {
		// we are at the bottom of the tree
		auto old_get = dynamic_cast<LogicalGet *>(pw.plan.get());

		OPENIVM_DEBUG_PRINT("Create replacement get node\n");

		unique_ptr<LogicalGet> delta_get_node;
		ColumnBinding new_mul_binding, timestamp_binding;
		string table_name;
		{
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
				opt_catalog_entry =
				    Catalog::GetEntry<TableCatalogEntry>(context, delta_table_catalog, delta_table_schema, delta_table,
				                                         OnEntryNotFound::RETURN_NULL, error_context);
				if (opt_catalog_entry == nullptr) {
					throw Exception(ExceptionType::BINDER,
					                "Table " + delta_table + " does not exist, no deltas to compute!");
				}
			}
			TableCatalogEntry &table_entry = opt_catalog_entry->Cast<TableCatalogEntry>();
			table_name = table_entry.name;
			unique_ptr<FunctionData> bind_data;
			auto scan_function = table_entry.GetScanFunction(context, bind_data);

			// Define the return names and types.
			// returned_types is indexed by ColumnIndex primary value (physical OID).
			// GetColumnType does returned_types[index.GetPrimaryIndex()], so we must
			// pad returned_types to cover all referenced OIDs.
			vector<ColumnIndex> column_ids = {};

			// Find mul/ts OIDs and max OID from the delta table
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

			// Build returned_types/names padded to max OID
			vector<LogicalType> return_types(max_oid + 1, LogicalType::ANY);
			vector<string> return_names(max_oid + 1, "");
			for (auto &col : table_entry.GetColumns().Logical()) {
				return_types[col.Oid()] = col.Type();
				return_names[col.Oid()] = col.Name();
			}

			// Copy original column_ids (physical OIDs from base table, same in delta)
			for (auto &id : old_get->GetColumnIds()) {
				column_ids.push_back(id);
			}
			// Multiplicity column (actual physical OID)
			column_ids.push_back(ColumnIndex(mul_oid));
			idx_t mul_pos = column_ids.size() - 1;
			new_mul_binding = ColumnBinding(old_get->table_index, mul_pos);

			// Timestamp column (actual physical OID)
			column_ids.push_back(ColumnIndex(ts_oid));
			idx_t ts_pos = column_ids.size() - 1;
			timestamp_binding = ColumnBinding(old_get->table_index, ts_pos);

			// Finally, create the delta GET node.
			delta_get_node = make_uniq<LogicalGet>(old_get->table_index, // Will get renumbered later.
			                                       scan_function, std::move(bind_data), std::move(return_types),
			                                       std::move(return_names));
			delta_get_node->SetColumnIds(std::move(column_ids));
		}
		delta_get_node->table_filters = std::move(old_get->table_filters); // this should be empty
		// Add a filter for the timestamp.
		Connection con(*context.db);
		con.SetAutoCommit(false);
		auto timestamp_query = "select last_update from _duckdb_ivm_delta_tables where view_name = '" + pw.view +
		                       "' and table_name = '" + table_name + "';";
		auto r = con.Query(timestamp_query);
		if (r->HasError()) {
			throw InternalException("Error while querying last_update");
		}
		auto ts_value = r->GetValue(0, 0);
		// Cast to TIMESTAMP if needed (now() returns TIMESTAMPTZ in modern DuckDB)
		if (ts_value.type() != LogicalType::TIMESTAMP) {
			ts_value = ts_value.DefaultCastAs(LogicalType::TIMESTAMP);
		}
		auto table_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, ts_value);
		// Filter key must be the column OID (GetPrimaryIndex value), because CreateTableFilterSet
		// searches column_ids for an entry whose GetPrimaryIndex() == filter_key.
		auto &ts_col_id = delta_get_node->GetColumnIds().back(); // last entry = timestamp
		idx_t ts_filter_key = ts_col_id.GetPrimaryIndex();       // the physical OID
		delta_get_node->table_filters.filters[ts_filter_key] = std::move(table_filter);

		// projection_ids: maps output positions to positions in column_ids.
		// With padded returned_types, we need projection_ids to select the right columns.
		delta_get_node->projection_ids.clear();
		idx_t n_base = old_get->GetColumnIds().size();
		if (!old_get->projection_ids.empty()) {
			// Copy original projection_ids (they index into old column_ids, same positions in new)
			for (auto &pid : old_get->projection_ids) {
				delta_get_node->projection_ids.push_back(pid);
			}
		} else {
			// No projection_ids means all base column_ids are output, in order
			for (idx_t i = 0; i < n_base; i++) {
				delta_get_node->projection_ids.push_back(i);
			}
		}
		// Add multiplicity column (position n_base in column_ids)
		delta_get_node->projection_ids.push_back(n_base);
		idx_t mul_proj_pos = delta_get_node->projection_ids.size() - 1;
		new_mul_binding = ColumnBinding(old_get->table_index, mul_proj_pos);
		// Timestamp column is NOT added to projection_ids (filtered, not output)

		delta_get_node->ResolveOperatorTypes();
		delta_get_node->Verify(pw.input.context);
		return {std::move(delta_get_node), new_mul_binding};
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {

		auto modified_node_logical_agg = dynamic_cast<LogicalAggregate *>(pw.plan.get());
#if OPENIVM_DEBUG
		for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
			OPENIVM_DEBUG_PRINT("aggregate node CB before %zu %s\n", i,
			                    modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
		}
		OPENIVM_DEBUG_PRINT("Aggregate index: %zu Group index: %zu\n", modified_node_logical_agg->aggregate_index,
		                    modified_node_logical_agg->group_index);
#endif

		// The input expression must reference the child's mul column binding as-is
		ColumnBinding input_mul_binding = child_mul_bindings[0];
		auto mult_group_by =
		    make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", pw.mul_type, input_mul_binding);
		// The output binding for this new group key: position = groups.size() (before push)
		ColumnBinding mod_mul_binding;
		mod_mul_binding.column_index = modified_node_logical_agg->groups.size();
		modified_node_logical_agg->groups.emplace_back(std::move(mult_group_by));

		auto mult_group_by_stats = make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(pw.mul_type));
		modified_node_logical_agg->group_stats.emplace_back(std::move(mult_group_by_stats));

		if (modified_node_logical_agg->grouping_sets.empty()) {
			modified_node_logical_agg->grouping_sets = {{0}};
		} else {
			idx_t gr = modified_node_logical_agg->grouping_sets[0].size();
			modified_node_logical_agg->grouping_sets[0].insert(gr);
		}

		mod_mul_binding.table_index = modified_node_logical_agg->group_index;
#if OPENIVM_DEBUG
		for (size_t i = 0; i < modified_node_logical_agg->GetColumnBindings().size(); i++) {
			OPENIVM_DEBUG_PRINT("aggregate node CB after %zu %s\n", i,
			                    modified_node_logical_agg->GetColumnBindings()[i].ToString().c_str());
		}
		OPENIVM_DEBUG_PRINT("Modified plan (aggregate/group by):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto &i_param : pw.plan->ParamsToString()) {
			OPENIVM_DEBUG_PRINT("%s", i_param.second.c_str());
		}
		OPENIVM_DEBUG_PRINT("\n---end of modified plan (aggregate/group by)---\n");
#endif
		pw.plan->Verify(pw.input.context);
		return {std::move(pw.plan), mod_mul_binding};
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		OPENIVM_DEBUG_PRINT("\nIn logical projection case \n Add the multiplicity column to the second node...\n");
		OPENIVM_DEBUG_PRINT("Modified plan (projection, start):\n%s\nParameters:", pw.plan->ToString().c_str());
		for (const auto &i_param : pw.plan->ParamsToString()) {
			OPENIVM_DEBUG_PRINT("%s", i_param.second.c_str());
		}
		OPENIVM_DEBUG_PRINT("\n---end of modified plan (projection)---\n");
		const auto bindings = pw.plan->GetColumnBindings();
		for (size_t i = 0; i < bindings.size(); i++) {
			OPENIVM_DEBUG_PRINT("Top node CB before %zu %s\n", i, bindings[i].ToString().c_str());
		}
		auto projection_node = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(pw.plan));

		auto mul_expression =
		    make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", pw.mul_type, child_mul_bindings[0]);
		OPENIVM_DEBUG_PRINT("Add multiplicity column to expression\n");
		projection_node->expressions.emplace_back(std::move(mul_expression));

		// Skip ToString() for now — it triggers ResolveTypes which conflicts with our modified GET node
		OPENIVM_DEBUG_PRINT("Modified plan (of projection_node): [skipped toString]\n");
		const auto new_bindings = projection_node->GetColumnBindings();
		for (size_t i = 0; i < new_bindings.size(); i++) {
			OPENIVM_DEBUG_PRINT("Top node CB %zu %s\n", i, new_bindings[i].ToString().c_str());
		}
		auto new_mul_binding = new_bindings[new_bindings.size() - 1];
		projection_node->Verify(pw.input.context);
		return {std::move(projection_node), new_mul_binding};
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		if (pw.plan->expressions.empty()) {
			pw.plan->children[0]->Verify(pw.input.context);
			return {std::move(pw.plan->children[0]), child_mul_bindings[0]};
		}

		unique_ptr<LogicalFilter> plan_as_filter = unique_ptr_cast<LogicalOperator, LogicalFilter>(std::move(pw.plan));
		plan_as_filter->ResolveOperatorTypes();
		if (!plan_as_filter->projection_map.empty()) {
#if OPENIVM_DEBUG
			auto filter_binds_before = plan_as_filter->GetColumnBindings();
			OPENIVM_DEBUG_PRINT("LOGICAL_FILTER projection_map size before adding mul CB: %zu\n",
			                    filter_binds_before.size());
#endif
			auto child_binds = plan_as_filter->children[0]->GetColumnBindings();
			ColumnBinding mul_binding = child_mul_bindings[0];
			idx_t mul_index = child_binds.size();
			bool mul_found = false;
			while (mul_found == false && mul_index > 0) {
				--mul_index;
				if (child_binds[mul_index] == mul_binding) {
					mul_found = true;
				};
			}
			if (!mul_found) {
				throw InternalException("Filter's child does not have multiplicity column!");
			}
			plan_as_filter->projection_map.emplace_back(mul_index);
#if OPENIVM_DEBUG
			auto filter_binds_after = plan_as_filter->GetColumnBindings();
			OPENIVM_DEBUG_PRINT("LOGICAL_FILTER projection_map size after adding mul CB: %zu\n",
			                    filter_binds_after.size());
#endif
		}
#if OPENIVM_DEBUG
		else {
			OPENIVM_DEBUG_PRINT("LOGICAL_FILTER has no projection_map; do not modify anything.\n");
		}
#endif
		return {std::move(plan_as_filter), child_mul_bindings[0]};
	}
	default:
		throw NotImplementedException("Operator type %s not supported", LogicalOperatorToString(pw.plan->type));
	}
	pw.plan->Verify(pw.input.context);
	return {std::move(pw.plan), child_mul_bindings[0]};
}

void IVMRewriteRule::IVMRewriteRuleFunction(OptimizerExtensionInput &input, duckdb::unique_ptr<LogicalOperator> &plan) {
	if (plan->children.empty()) {
		return;
	}

	auto child = plan.get();
	while (!child->children.empty()) {
		child = child->children[0].get();
	}
	if (child->GetName().substr(0, 5) != "DOIVM") {
		return;
	}

#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("Activating the rewrite rule\n");
#endif

	auto child_get = dynamic_cast<LogicalGet *>(child);
	auto view = child_get->named_parameters["view_name"].ToString();
	auto view_catalog = child_get->named_parameters["view_catalog_name"].ToString();
	auto view_schema = child_get->named_parameters["view_schema_name"].ToString();

	Connection con(*input.context.db);

	con.BeginTransaction();
	const bool verify_column_lifetime = false;
	if (verify_column_lifetime) {
		for (size_t i = 0; i < 30; ++i) {
			input.optimizer.binder.GenerateTableIndex();
		}
		con.Query("SET disabled_optimizers='compressed_materialization, statistics_propagation, expression_rewriter, "
		          "filter_pushdown';");
	} else {
		con.Query("SET disabled_optimizers='compressed_materialization, column_lifetime, statistics_propagation, "
		          "expression_rewriter, filter_pushdown';");
	}
	con.Commit();

	auto v = con.Query("select sql_string from _duckdb_ivm_views where view_name = '" + view + "';");
	if (v->HasError()) {
		throw InternalException("Error while querying view definition");
	}
	string view_query = v->GetValue(0, 0).ToString();

	Parser parser;
	Planner planner(input.context);

	parser.ParseQuery(view_query);
	auto statement = parser.statements[0].get();

	planner.CreatePlan(statement->Copy());
#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("Unoptimized plan: \n%s\n", planner.plan->ToString().c_str());
#endif
	Optimizer optimizer(*planner.binder, input.context);
	auto optimized_plan = optimizer.Optimize(std::move(planner.plan));

	// Reset disabled_optimizers to avoid polluting the session
	con.Query("RESET disabled_optimizers;");

#if OPENIVM_DEBUG
	OPENIVM_DEBUG_PRINT("Optimized plan: \n%s\n", optimized_plan->ToString().c_str());
#endif

	if (optimized_plan->children.empty()) {
		throw NotImplementedException("Plan contains single node, this is not supported");
	}

#if OPENIVM_DEBUG
	std::cout << "Running ModifyPlan..." << '\n';
#endif
	auto root = optimized_plan.get();
	auto start_pw = PlanWrapper(input, optimized_plan, view, root);
	ModifiedPlan modified_plan = ModifyPlan(start_pw);
#if OPENIVM_DEBUG
	std::cout << "Running AddInsertNode..." << '\n';
#endif
	AddInsertNode(input.context, modified_plan.op, view, view_catalog, view_schema);
#if OPENIVM_DEBUG
	std::cout << "\nFINAL PLAN:\n" << modified_plan.op->ToString() << '\n';
#endif
	plan = std::move(modified_plan.op);
	return;
}
} // namespace duckdb
