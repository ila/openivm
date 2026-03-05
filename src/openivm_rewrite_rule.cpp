#include "openivm_rewrite_rule.hpp"

#include "logical_plan_to_sql.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
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
using duckdb::ColumnIndex;
using duckdb::Connection;
using duckdb::ConstantFilter;
using duckdb::Expression;
using duckdb::ExpressionType;
using duckdb::JoinCondition;
using duckdb::LogicalComparisonJoin;
using duckdb::LogicalGet;
using duckdb::LogicalOperator;
using duckdb::LogicalProjection;
using duckdb::LogicalType;
using duckdb::make_uniq;
using duckdb::unique_ptr;
using duckdb::vector;

//==============================================================================
// General helpers
//==============================================================================

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

/// Rebind join conditions in ALL join nodes throughout a subtree.
void rebind_all_conditions_in_tree(unique_ptr<LogicalOperator> &node,
                                   const std::unordered_map<idx_t, idx_t> &idx_map) {
	if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node.get());
		join->conditions = rebind_join_conditions(join->conditions, idx_map);
	}
	for (auto &child : node->children) {
		rebind_all_conditions_in_tree(child, idx_map);
	}
}

/// Call ResolveOperatorTypes bottom-up on a join subtree.
void resolve_types_bottom_up(unique_ptr<LogicalOperator> &node) {
	for (auto &child : node->children) {
		resolve_types_bottom_up(child);
	}
	node->ResolveOperatorTypes();
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

/// Filter a delta subtree by multiplicity (true=inserts, false=deletes) and project out the mul column.
unique_ptr<LogicalOperator> filter_delta_and_project_out_mul(unique_ptr<LogicalOperator> delta_copy,
                                                             const ColumnBinding &mul_binding,
                                                             const duckdb::LogicalType &mul_type,
                                                             idx_t proj_table_index, bool mul_value) {
	auto filter = make_uniq<duckdb::LogicalFilter>();
	auto mul_ref = make_uniq<BoundColumnRefExpression>("_duckdb_ivm_multiplicity", mul_type, mul_binding);
	auto const_val = make_uniq<duckdb::BoundConstantExpression>(duckdb::Value::BOOLEAN(mul_value));
	auto comparison = make_uniq<duckdb::BoundComparisonExpression>(duckdb::ExpressionType::COMPARE_EQUAL,
	                                                               std::move(mul_ref), std::move(const_val));
	filter->expressions.push_back(std::move(comparison));
	filter->children.push_back(std::move(delta_copy));
	filter->ResolveOperatorTypes();

	auto filter_bindings = filter->GetColumnBindings();
	auto filter_types = filter->types;
	vector<unique_ptr<duckdb::Expression>> proj_expressions;
	for (size_t i = 0; i < filter_bindings.size(); i++) {
		if (filter_bindings[i] != mul_binding) {
			proj_expressions.push_back(make_uniq<BoundColumnRefExpression>(filter_types[i], filter_bindings[i]));
		}
	}
	auto projection = make_uniq<duckdb::LogicalProjection>(proj_table_index, std::move(proj_expressions));
	projection->children.push_back(std::move(filter));
	projection->ResolveOperatorTypes();
	return projection;
}

//==============================================================================
// Delta GET creation (extracted from the old LOGICAL_GET case)
//==============================================================================

struct DeltaGetResult {
	unique_ptr<LogicalOperator> node;
	ColumnBinding mul_binding;
};

/// Create a delta GET node for a given base table GET node.
/// The delta GET scans the delta table, adds multiplicity column, filters by timestamp.
DeltaGetResult create_delta_get_node(duckdb::ClientContext &context, LogicalGet *old_get, const std::string &view_name) {
	unique_ptr<LogicalGet> delta_get_node;
	ColumnBinding new_mul_binding;
	std::string table_name;

	duckdb::optional_ptr<duckdb::TableCatalogEntry> opt_catalog_entry;
	{
		std::string delta_table;
		std::string delta_table_schema;
		std::string delta_table_catalog;
		if (old_get->GetTable().get() == nullptr) {
			delta_table_schema = "public";
			delta_table_catalog = "p"; // todo
		} else {
			delta_table = "delta_" + old_get->GetTable().get()->name;
			delta_table_schema = old_get->GetTable().get()->schema.name;
			delta_table_catalog = old_get->GetTable().get()->catalog.GetName();
		}
		duckdb::QueryErrorContext error_context;
		opt_catalog_entry = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
		    context, delta_table_catalog, delta_table_schema, delta_table, duckdb::OnEntryNotFound::RETURN_NULL,
		    error_context);
		if (opt_catalog_entry == nullptr) {
			throw duckdb::Exception(duckdb::ExceptionType::BINDER,
			                        "Table " + delta_table + " does not exist, no deltas to compute!");
		}
	}
	auto &table_entry = opt_catalog_entry->Cast<duckdb::TableCatalogEntry>();
	table_name = table_entry.name;
	unique_ptr<duckdb::FunctionData> bind_data;
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
	vector<std::string> return_names(max_oid + 1, "");
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
		throw duckdb::InternalException("Error while querying last_update");
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

//==============================================================================
// R_old construction: (T_new EXCEPT ALL ΔT_inserts) UNION ALL ΔT_deletes
//==============================================================================

/// Build R_old from a base table GET and its delta GET.
unique_ptr<LogicalOperator> build_r_old_from_delta(duckdb::ClientContext &context, unique_ptr<LogicalOperator> r_new,
                                                   const unique_ptr<LogicalOperator> &delta_get,
                                                   const ColumnBinding &delta_mul_binding,
                                                   const duckdb::LogicalType &mul_type, duckdb::Binder &binder) {
	auto r_types = r_new->types;
	auto r_col_count = r_new->GetColumnBindings().size();

	// DR_inserts: filter delta where mul=true, project out mul
	unique_ptr<LogicalOperator> dr_inserts;
	{
		duckdb::RenumberWrapper res = duckdb::renumber_and_rebind_subtree(delta_get->Copy(context), binder);
		ColumnBinding dr_mul(res.idx_map[delta_mul_binding.table_index], delta_mul_binding.column_index);
		dr_inserts =
		    filter_delta_and_project_out_mul(std::move(res.op), dr_mul, mul_type, binder.GenerateTableIndex(), true);
	}

	// DR_deletes: filter delta where mul=false, project out mul
	unique_ptr<LogicalOperator> dr_deletes;
	{
		duckdb::RenumberWrapper res = duckdb::renumber_and_rebind_subtree(delta_get->Copy(context), binder);
		ColumnBinding dr_mul(res.idx_map[delta_mul_binding.table_index], delta_mul_binding.column_index);
		dr_deletes =
		    filter_delta_and_project_out_mul(std::move(res.op), dr_mul, mul_type, binder.GenerateTableIndex(), false);
	}

	// R_old = (R_new EXCEPT ALL DR_inserts) UNION ALL DR_deletes
	idx_t except_table_idx = binder.GenerateTableIndex();
	auto r_except = make_uniq<duckdb::LogicalSetOperation>(
	    except_table_idx, r_col_count, std::move(r_new), std::move(dr_inserts),
	    duckdb::LogicalOperatorType::LOGICAL_EXCEPT, true);
	r_except->types = r_types;

	idx_t union_table_idx = binder.GenerateTableIndex();
	auto r_old = make_uniq<duckdb::LogicalSetOperation>(union_table_idx, r_col_count, std::move(r_except),
	                                                    std::move(dr_deletes),
	                                                    duckdb::LogicalOperatorType::LOGICAL_UNION, true);
	r_old->types = r_types;
	return r_old;
}

/// Build R_old for a base table by creating its delta GET internally.
unique_ptr<LogicalOperator> build_r_old_for_table(duckdb::ClientContext &context, LogicalGet *original_get,
                                                  const std::string &view_name, duckdb::Binder &binder) {
	// T_new = copy of original base table scan
	auto r_new = original_get->Copy(context);
	r_new->ResolveOperatorTypes();

	// Create delta GET for this table
	DeltaGetResult delta = create_delta_get_node(context, original_get, view_name);

	return build_r_old_from_delta(context, std::move(r_new), delta.node, delta.mul_binding, LogicalType::BOOLEAN,
	                              binder);
}

//==============================================================================
// Join subtree helpers
//==============================================================================

struct JoinLeafInfo {
	vector<size_t> path; // navigation path from join root (0=left child, 1=right child)
	LogicalGet *get;     // pointer to the GET node in the ORIGINAL tree
};

/// Collect all leaf GET nodes from a join subtree in left-to-right order.
void collect_join_leaves(LogicalOperator *node, vector<size_t> path, vector<JoinLeafInfo> &leaves) {
	if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		path.push_back(0);
		collect_join_leaves(node->children[0].get(), path, leaves);
		path.pop_back();
		path.push_back(1);
		collect_join_leaves(node->children[1].get(), path, leaves);
		path.pop_back();
	} else if (node->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
		leaves.push_back({path, dynamic_cast<LogicalGet *>(node)});
	} else {
		throw duckdb::NotImplementedException("Unexpected node type in join subtree: " +
		                                      duckdb::LogicalOperatorToString(node->type));
	}
}

/// Navigate to a node at the given path in a tree, returning a reference to the unique_ptr holding it.
unique_ptr<LogicalOperator> &get_node_at_path(unique_ptr<LogicalOperator> &root, const vector<size_t> &path) {
	unique_ptr<LogicalOperator> *current = &root;
	for (size_t step : path) {
		current = &((*current)->children[step]);
	}
	return *current;
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

/// Flat N-term join incrementalization.
///
/// For a join over N base tables T1, T2, ..., TN, produces N terms:
///   Term i:  T1_new ⋈ ... ⋈ T(i-1)_new ⋈ ΔTi ⋈ T(i+1)_old ⋈ ... ⋈ TN_old
///
/// Tables before the delta table use T_new (current base table).
/// Tables after the delta table use T_old = (T_new EXCEPT ALL ΔT_inserts) UNION ALL ΔT_deletes.
/// The N terms are combined with UNION ALL.
///
/// This avoids recursive rewriting of nested joins, which causes binding corruption.
ModifiedPlan IVMRewriteRule::HandleJoinSubtree(PlanWrapper pw) {
	ClientContext &context = pw.input.context;
	Binder &binder = pw.input.optimizer.binder;
	const vector<ColumnBinding> original_bindings = pw.plan->GetColumnBindings();

	// Verify all joins are INNER
	std::function<void(LogicalOperator *)> verify_inner_joins = [&](LogicalOperator *node) {
		if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
			if (join->join_type != JoinType::INNER) {
				throw Exception(ExceptionType::OPTIMIZER,
				                JoinTypeToString(join->join_type) + " type not yet supported in OpenIVM");
			}
			for (auto &child : node->children) {
				verify_inner_joins(child.get());
			}
		}
	};
	verify_inner_joins(pw.plan.get());

	// 1. Collect all leaf GET nodes from the join subtree
	vector<JoinLeafInfo> leaves;
	collect_join_leaves(pw.plan.get(), {}, leaves);
	size_t N = leaves.size();
	OPENIVM_DEBUG_PRINT("HandleJoinSubtree: %zu leaves found\n", N);

	// Output type: original join columns + multiplicity
	auto types = pw.plan->types;
	types.emplace_back(pw.mul_type);

	// 2. Build N terms
	vector<unique_ptr<LogicalOperator>> terms;

	for (size_t i = 0; i < N; i++) {
		OPENIVM_DEBUG_PRINT("Building term %zu (delta on table_index %llu)\n", i, leaves[i].get->table_index);

		// Copy the original join subtree
		auto term = pw.plan->Copy(context);

		// Replace leaf i with its delta GET (has mul column)
		DeltaGetResult delta_i = create_delta_get_node(context, leaves[i].get, pw.view);
		ColumnBinding mul_binding = delta_i.mul_binding;
		get_node_at_path(term, leaves[i].path) = std::move(delta_i.node);

		// Replace leaves j > i with R_old (no mul column, same column count as original)
		std::unordered_map<idx_t, idx_t> idx_map;
		for (size_t j = i + 1; j < N; j++) {
			auto r_old = build_r_old_for_table(context, leaves[j].get, pw.view, binder);
			idx_t r_old_table_idx = r_old->GetColumnBindings()[0].table_index;
			idx_map[leaves[j].get->table_index] = r_old_table_idx;
			get_node_at_path(term, leaves[j].path) = std::move(r_old);
		}

		// Rebind join conditions throughout the tree for R_old table index changes
		if (!idx_map.empty()) {
			rebind_all_conditions_in_tree(term, idx_map);
		}

		// Resolve types bottom-up after leaf replacement
		resolve_types_bottom_up(term);

		// Add projection: original columns + mul at end
		auto term_bindings = term->GetColumnBindings();
		auto term_types = term->types;
		auto proj_exprs = project_multiplicity_to_end(term_bindings, term_types, mul_binding);
		auto projection =
		    make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));
	}

	// 3. UNION ALL all terms
	auto result = std::move(terms[0]);
	for (size_t i = 1; i < N; i++) {
		auto union_table_index = binder.GenerateTableIndex();
		result = make_uniq<LogicalSetOperation>(union_table_index, types.size(), std::move(result),
		                                        std::move(terms[i]), LogicalOperatorType::LOGICAL_UNION, true);
		result->types = types;
	}

	// 4. Update column bindings in parent
	ColumnBinding new_mul_binding;
	{
		auto union_bindings = result->GetColumnBindings();
		if (union_bindings.size() - original_bindings.size() != 1) {
			throw InternalException(
			    "Union (with multiplicity column) should have exactly 1 more binding than original join!");
		}
		ColumnBindingReplacer replacer;
		vector<ReplacementBinding> &replacement_bindings = replacer.replacement_bindings;
		for (idx_t col_idx = 0; col_idx < original_bindings.size(); ++col_idx) {
			replacement_bindings.emplace_back(original_bindings[col_idx], union_bindings[col_idx]);
		}
		replacer.stop_operator = result;
		replacer.VisitOperator(*pw.root);
		new_mul_binding = union_bindings.back();
	}

	pw.plan = std::move(result);
	return {std::move(pw.plan), new_mul_binding};
}

ModifiedPlan IVMRewriteRule::ModifyPlan(PlanWrapper pw) {
	ClientContext &context = pw.input.context;
	const vector<ColumnBinding> original_bindings = pw.plan->GetColumnBindings();

	// For joins: handle the entire join subtree without recursing into children.
	if (pw.plan->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    pw.plan->type == LogicalOperatorType::LOGICAL_JOIN) {
		return HandleJoinSubtree(pw);
	}

	// For non-join nodes: recurse into children normally.
	vector<ColumnBinding> child_mul_bindings;
	for (auto &&child : pw.plan->children) {
		auto rec_pw = PlanWrapper(pw.input, child, pw.view, pw.root);
		ModifiedPlan child_plan = ModifyPlan(rec_pw);
		child = std::move(child_plan.op);
		child_mul_bindings.emplace_back(child_plan.mul_binding);
	}

	switch (pw.plan->type) {
	case LogicalOperatorType::LOGICAL_GET: {
		OPENIVM_DEBUG_PRINT("Create replacement get node\n");
		auto old_get = dynamic_cast<LogicalGet *>(pw.plan.get());
		DeltaGetResult result = create_delta_get_node(context, old_get, pw.view);
		result.node->Verify(pw.input.context);
		return {std::move(result.node), result.mul_binding};
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
