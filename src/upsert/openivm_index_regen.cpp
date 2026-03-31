#include "upsert/openivm_index_regen.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_set_operation.hpp>

namespace duckdb {

RenumberWrapper renumber_table_indices(unique_ptr<LogicalOperator> plan, Binder &binder) {
	std::unordered_map<old_idx, new_idx> table_reassign;
	std::vector<ColumnBinding> current_bindings = plan->GetColumnBindings();
	std::vector<unique_ptr<LogicalOperator>> rec_children;
	for (auto &child : plan->children) {
		RenumberWrapper child_wrap = renumber_table_indices(std::move(child), binder);
		table_reassign.insert(child_wrap.idx_map.cbegin(), child_wrap.idx_map.cend());
		current_bindings.insert(current_bindings.end(), child_wrap.column_bindings.cbegin(),
		                        child_wrap.column_bindings.cend());
		rec_children.emplace_back(std::move(child_wrap.op));
	}

	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		unique_ptr<LogicalAggregate> agg_ptr = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(plan));
		{
			const idx_t old_gr_idx = agg_ptr->group_index;
			const idx_t new_gr_idx = binder.GenerateTableIndex();
			agg_ptr->group_index = new_gr_idx;
			table_reassign[old_gr_idx] = new_gr_idx;
		}
		{
			const idx_t old_ag_idx = agg_ptr->aggregate_index;
			const idx_t new_ag_idx = binder.GenerateTableIndex();
			agg_ptr->aggregate_index = new_ag_idx;
			table_reassign[old_ag_idx] = new_ag_idx;
		}
		{
			const idx_t old_gs_idx = agg_ptr->groupings_index;
			if (old_gs_idx != DConstants::INVALID_INDEX) {
				const idx_t new_gs_idx = binder.GenerateTableIndex();
				agg_ptr->groupings_index = new_gs_idx;
				table_reassign[old_gs_idx] = new_gs_idx;
			}
		}
		agg_ptr->children = std::move(rec_children);
		return {std::move(agg_ptr), table_reassign, current_bindings};
	}
	case LogicalOperatorType::LOGICAL_GET: {
		unique_ptr<LogicalGet> get_ptr = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(plan));
		const idx_t current_idx = get_ptr->table_index;
		const idx_t new_idx = binder.GenerateTableIndex();
		get_ptr->table_index = new_idx;
		table_reassign[current_idx] = new_idx;
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("Index regen LOGICAL_GET: Change %zu -> %zu\n", current_idx, new_idx);
#endif
		return {std::move(get_ptr), table_reassign, current_bindings};
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		unique_ptr<LogicalProjection> proj_ptr = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(plan));
		const idx_t current_idx = proj_ptr->table_index;
		const idx_t new_idx = binder.GenerateTableIndex();
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("Index regen LOGICAL_PROJECTION: Change %zu -> %zu\n", current_idx, new_idx);
#endif
		proj_ptr->table_index = new_idx;
		table_reassign[current_idx] = new_idx;
		proj_ptr->children = std::move(rec_children);
		return {std::move(proj_ptr), table_reassign, current_bindings};
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		auto &setop = plan->Cast<LogicalSetOperation>();
		const idx_t current_idx = setop.table_index;
		const idx_t new_idx = binder.GenerateTableIndex();
		setop.table_index = new_idx;
		table_reassign[current_idx] = new_idx;
		plan->children = std::move(rec_children);
		return {std::move(plan), table_reassign, current_bindings};
	}
	default: {
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("table indices of type %s ignored.\n", LogicalOperatorToString(plan->type).c_str());
#endif
		break;
	}
	}
	plan->children = std::move(rec_children);
	return {std::move(plan), table_reassign, current_bindings};
}

// Walk every expression in the operator tree and collect all referenced ColumnBindings.
static uint64_t HashBinding(const ColumnBinding &b) {
	// Mix both indices to avoid collisions for large table indices
	return std::hash<idx_t>()(b.table_index) ^ (std::hash<idx_t>()(b.column_index) * 0x9e3779b97f4a7c15ULL);
}

static void CollectAllBindings(LogicalOperator &op, std::unordered_set<uint64_t> &seen,
                                std::vector<ColumnBinding> &out) {
	std::function<void(Expression &)> CollectExpr = [&](Expression &e) {
		if (e.type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bcr = e.Cast<BoundColumnRefExpression>();
			uint64_t key = HashBinding(bcr.binding);
			if (seen.insert(key).second) {
				out.push_back(bcr.binding);
			}
		}
		ExpressionIterator::EnumerateChildren(e, [&](Expression &child) { CollectExpr(child); });
	};
	// GetColumnBindings
	for (auto &cb : op.GetColumnBindings()) {
		uint64_t key = HashBinding(cb);
		if (seen.insert(key).second) {
			out.push_back(cb);
		}
	}
	// Standard expressions
	for (auto &expr : op.expressions) {
		CollectExpr(*expr);
	}
	// COMPARISON_JOIN conditions (stored separately from expressions)
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		for (auto &cond : join.conditions) {
			CollectExpr(*cond.left);
			CollectExpr(*cond.right);
		}
	}
	for (auto &child : op.children) {
		CollectAllBindings(*child, seen, out);
	}
}

// Replace bindings in COMPARISON_JOIN conditions (not visited by ColumnBindingReplacer).
static void RebindJoinConditions(LogicalOperator &op, const std::unordered_map<old_idx, new_idx> &table_mapping) {
	std::function<void(Expression &)> RebindExpr = [&](Expression &e) {
		if (e.type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bcr = e.Cast<BoundColumnRefExpression>();
			auto it = table_mapping.find(bcr.binding.table_index);
			if (it != table_mapping.end()) {
				bcr.binding.table_index = it->second;
			}
		}
		ExpressionIterator::EnumerateChildren(e, [&](Expression &child) { RebindExpr(child); });
	};
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		// Rebind conditions that ColumnBindingReplacer doesn't visit
		for (auto &cond : join.conditions) {
			RebindExpr(*cond.left);
			RebindExpr(*cond.right);
		}
	}
	for (auto &child : op.children) {
		RebindJoinConditions(*child, table_mapping);
	}
}

ColumnBindingReplacer vec_to_replacer(const std::vector<ColumnBinding> &bindings,
                                      const std::unordered_map<old_idx, new_idx> &table_mapping) {
	ColumnBindingReplacer replacer;
	std::unordered_set<uint64_t> seen;
	for (const ColumnBinding &cb : bindings) {
		if (table_mapping.find(cb.table_index) == table_mapping.end()) {
			continue;
		}
		uint64_t key = HashBinding(cb);
		if (!seen.insert(key).second) {
			continue;
		}
		new_idx new_t = table_mapping.at(cb.table_index);
		replacer.replacement_bindings.emplace_back(cb, ColumnBinding(new_t, cb.column_index));
	}
	return replacer;
}

RenumberWrapper renumber_and_rebind_subtree(unique_ptr<LogicalOperator> plan, Binder &binder) {
	// Collect ALL bindings BEFORE renumbering (we need the old table indices)
	std::unordered_set<uint64_t> seen;
	std::vector<ColumnBinding> all_bindings;
	CollectAllBindings(*plan, seen, all_bindings);

	RenumberWrapper res = renumber_table_indices(std::move(plan), binder);

	// Merge in any bindings from the renumber pass
	for (auto &cb : res.column_bindings) {
		uint64_t key = HashBinding(cb);
		if (seen.insert(key).second) {
			all_bindings.push_back(cb);
		}
	}

	ColumnBindingReplacer replacer = vec_to_replacer(all_bindings, res.idx_map);
	replacer.VisitOperator(*res.op);

	// Also rebind COMPARISON_JOIN conditions (not visited by ColumnBindingReplacer)
	RebindJoinConditions(*res.op, res.idx_map);
	return res;
}

} // namespace duckdb
