#include "openivm_index_regen.hpp"
#include "openivm_debug.hpp"
#include "duckdb/planner/binder.hpp"

#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>

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
			const idx_t old_ag_idx = agg_ptr->group_index;
			const idx_t new_ag_idx = binder.GenerateTableIndex();
			agg_ptr->group_index = new_ag_idx;
			table_reassign[old_ag_idx] = new_ag_idx;
		}
		{
			const idx_t old_gs_idx = agg_ptr->groupings_index;
			if (old_gs_idx != DConstants::INVALID_INDEX) {
				const idx_t new_gs_idx = binder.GenerateTableIndex();
				agg_ptr->group_index = new_gs_idx;
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

ColumnBindingReplacer vec_to_replacer(const std::vector<ColumnBinding> &bindings,
                                      const std::unordered_map<old_idx, new_idx> &table_mapping) {
	std::unordered_map<old_idx, std::unordered_set<col_idx>> to_replace;
	for (const ColumnBinding col_binding : bindings) {
		idx_t table_index = col_binding.table_index;
		if (table_mapping.find(table_index) != table_mapping.end()) {
			to_replace[table_index].insert(col_binding.column_index);
		}
	}
	ColumnBindingReplacer replacer;
	for (const auto &pair : to_replace) {
		const old_idx old_t = pair.first;
		const new_idx new_t = table_mapping.at(old_t);
		for (const col_idx col : pair.second) {
			const auto old_binding = ColumnBinding(old_t, col);
			const auto new_binding = ColumnBinding(new_t, col);
			replacer.replacement_bindings.emplace_back(old_binding, new_binding);
		}
	}
	return replacer;
}

RenumberWrapper renumber_and_rebind_subtree(unique_ptr<LogicalOperator> plan, Binder &binder) {
	RenumberWrapper res = renumber_table_indices(std::move(plan), binder);
	ColumnBindingReplacer replacer = vec_to_replacer(res.column_bindings, res.idx_map);
	replacer.VisitOperator(*res.op);
	return res;
}

} // namespace duckdb
