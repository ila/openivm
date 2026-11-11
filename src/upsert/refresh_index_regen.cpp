#include "upsert/refresh_index_regen.hpp"
#include "core/openivm_debug.hpp"
#include "delta/delta_helpers.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

#include <duckdb/planner/operator/logical_aggregate.hpp>
#include <duckdb/planner/operator/logical_empty_result.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_projection.hpp>
#include <duckdb/planner/operator/logical_comparison_join.hpp>
#include <duckdb/planner/operator/logical_delim_get.hpp>
#include <duckdb/planner/operator/logical_dependent_join.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_set_operation.hpp>
#include <duckdb/planner/operator/logical_unnest.hpp>

namespace duckdb {

static uint64_t HashBinding(const ColumnBinding &binding) {
	return std::hash<idx_t>()(binding.table_index.index) ^
	       (std::hash<idx_t>()(binding.column_index.GetIndex()) * 0x9e3779b97f4a7c15ULL);
}

static void CollectExpressionBindings(Expression &expression, std::vector<ColumnBinding> &bindings) {
	if (expression.GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		bindings.push_back(expression.Cast<BoundColumnRefExpression>().Binding());
	}
	ExpressionIterator::EnumerateChildren(expression, [&](unique_ptr<Expression> &child) {
		if (child) {
			CollectExpressionBindings(*child, bindings);
		}
	});
}

static void CollectOperatorExpressionBindings(LogicalOperator &op, std::vector<ColumnBinding> &bindings) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expression) {
		if (expression && *expression) {
			CollectExpressionBindings(**expression, bindings);
		}
	});
	if (op.type != LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		return;
	}
	auto &join = op.Cast<LogicalDependentJoin>();
	if (join.condition) {
		CollectExpressionBindings(*join.condition, bindings);
	}
	for (auto &column : join.correlated_columns) {
		bindings.push_back(column.binding);
	}
}

static new_idx GetOrCreateTableIndex(old_idx old_index, Binder &binder,
                                     std::unordered_map<old_idx, new_idx> &table_reassign) {
	auto entry = table_reassign.find(old_index);
	if (entry != table_reassign.end()) {
		OPENIVM_DEBUG_PRINT("Index regen: Reuse %llu -> %llu\n", static_cast<unsigned long long>(old_index.index),
		                    static_cast<unsigned long long>(entry->second.index));
		return entry->second;
	}
	auto fresh_index = binder.GenerateTableIndex();
	table_reassign.emplace(old_index, fresh_index);
	return fresh_index;
}

static RenumberWrapper renumber_table_indices(unique_ptr<LogicalOperator> plan, Binder &binder,
                                              std::unordered_map<old_idx, new_idx> &table_reassign) {
	std::vector<ColumnBinding> current_bindings = plan->GetColumnBindings();
	CollectOperatorExpressionBindings(*plan, current_bindings);
	bool contains_delim_operator = plan->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	                               plan->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
	                               plan->type == LogicalOperatorType::LOGICAL_DELIM_GET;
	std::vector<unique_ptr<LogicalOperator>> rec_children;
	for (auto &child : plan->children) {
		RenumberWrapper child_wrap = renumber_table_indices(std::move(child), binder, table_reassign);
		current_bindings.insert(current_bindings.end(), child_wrap.column_bindings.cbegin(),
		                        child_wrap.column_bindings.cend());
		contains_delim_operator = contains_delim_operator || child_wrap.contains_delim_operator;
		rec_children.emplace_back(std::move(child_wrap.op));
	}

	switch (plan->type) {
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		unique_ptr<LogicalAggregate> agg_ptr = unique_ptr_cast<LogicalOperator, LogicalAggregate>(std::move(plan));
		{
			const auto old_gr_idx = agg_ptr->group_index;
			const auto new_gr_idx = GetOrCreateTableIndex(old_gr_idx, binder, table_reassign);
			agg_ptr->group_index = new_gr_idx;
		}
		{
			const auto old_ag_idx = agg_ptr->aggregate_index;
			const auto new_ag_idx = GetOrCreateTableIndex(old_ag_idx, binder, table_reassign);
			agg_ptr->aggregate_index = new_ag_idx;
		}
		{
			const auto old_gs_idx = agg_ptr->groupings_index;
			if (old_gs_idx.IsValid()) {
				const auto new_gs_idx = GetOrCreateTableIndex(old_gs_idx, binder, table_reassign);
				agg_ptr->groupings_index = new_gs_idx;
			}
		}
		agg_ptr->children = std::move(rec_children);
		return {std::move(agg_ptr), table_reassign, current_bindings, contains_delim_operator};
	}
	case LogicalOperatorType::LOGICAL_GET: {
		unique_ptr<LogicalGet> get_ptr = unique_ptr_cast<LogicalOperator, LogicalGet>(std::move(plan));
		// Copy serializes DuckLake's bound scan but drops its SQL parameter cache.
		// Restore it from the bound snapshot while already visiting copied scans.
		PopulateDuckLakeChangeScanParameters(*get_ptr);
		const auto current_idx = get_ptr->table_index;
		const auto new_idx = GetOrCreateTableIndex(current_idx, binder, table_reassign);
		get_ptr->table_index = new_idx;
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("Index regen LOGICAL_GET: Change %llu -> %llu\n",
		                    static_cast<unsigned long long>(current_idx.index),
		                    static_cast<unsigned long long>(new_idx.index));
#endif
		get_ptr->children = std::move(rec_children);
		return {std::move(get_ptr), table_reassign, current_bindings, contains_delim_operator};
	}
	case LogicalOperatorType::LOGICAL_DELIM_GET: {
		unique_ptr<LogicalDelimGet> delim_get_ptr = unique_ptr_cast<LogicalOperator, LogicalDelimGet>(std::move(plan));
		const auto current_idx = delim_get_ptr->table_index;
		const auto new_idx = GetOrCreateTableIndex(current_idx, binder, table_reassign);
		delim_get_ptr->table_index = new_idx;
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("Index regen LOGICAL_DELIM_GET: Change %llu -> %llu\n",
		                    static_cast<unsigned long long>(current_idx.index),
		                    static_cast<unsigned long long>(new_idx.index));
#endif
		return {std::move(delim_get_ptr), table_reassign, current_bindings, contains_delim_operator};
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		unique_ptr<LogicalProjection> proj_ptr = unique_ptr_cast<LogicalOperator, LogicalProjection>(std::move(plan));
		const auto current_idx = proj_ptr->table_index;
		const auto new_idx = GetOrCreateTableIndex(current_idx, binder, table_reassign);
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("Index regen LOGICAL_PROJECTION: Change %llu -> %llu\n",
		                    static_cast<unsigned long long>(current_idx.index),
		                    static_cast<unsigned long long>(new_idx.index));
#endif
		proj_ptr->table_index = new_idx;
		proj_ptr->children = std::move(rec_children);
		return {std::move(proj_ptr), table_reassign, current_bindings, contains_delim_operator};
	}
	case LogicalOperatorType::LOGICAL_UNNEST: {
		auto &unnest = plan->Cast<LogicalUnnest>();
		const auto current_idx = unnest.unnest_index;
		const auto new_idx = GetOrCreateTableIndex(current_idx, binder, table_reassign);
		unnest.unnest_index = new_idx;
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("Index regen LOGICAL_UNNEST: Change %llu -> %llu\n",
		                    static_cast<unsigned long long>(current_idx.index),
		                    static_cast<unsigned long long>(new_idx.index));
#endif
		plan->children = std::move(rec_children);
		return {std::move(plan), table_reassign, current_bindings, contains_delim_operator};
	}
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		auto &setop = plan->Cast<LogicalSetOperation>();
		const auto current_idx = setop.table_index;
		const auto new_idx = GetOrCreateTableIndex(current_idx, binder, table_reassign);
		setop.table_index = new_idx;
		plan->children = std::move(rec_children);
		return {std::move(plan), table_reassign, current_bindings, contains_delim_operator};
	}
	case LogicalOperatorType::LOGICAL_EMPTY_RESULT: {
		// LogicalEmptyResult stores a `bindings` vector directly (no single
		// table_index member). Remap each distinct old table_index to a fresh
		// index so its bindings don't collide with a renumbered LogicalGet
		// elsewhere in the plan (e.g. inclusion-exclusion terms where the
		// same source table appears as both full and delta).
		auto &empty = plan->Cast<LogicalEmptyResult>();
		for (auto &cb : empty.bindings) {
			auto old_table = cb.table_index;
			auto existing = table_reassign.find(old_table);
			auto new_t = GetOrCreateTableIndex(old_table, binder, table_reassign);
			if (existing == table_reassign.end()) {
#if OPENIVM_DEBUG
				OPENIVM_DEBUG_PRINT("Index regen LOGICAL_EMPTY_RESULT: Change %llu -> %llu\n",
				                    static_cast<unsigned long long>(old_table.index),
				                    static_cast<unsigned long long>(new_t.index));
#endif
			}
			cb.table_index = new_t;
		}
		plan->children = std::move(rec_children);
		return {std::move(plan), table_reassign, current_bindings, contains_delim_operator};
	}
	default: {
#if OPENIVM_DEBUG
		OPENIVM_DEBUG_PRINT("table indices of type %s ignored.\n", LogicalOperatorToString(plan->type).c_str());
#endif
		break;
	}
	}
	plan->children = std::move(rec_children);
	return {std::move(plan), table_reassign, current_bindings, contains_delim_operator};
}

static void RebindExpressionsNullSafe(LogicalOperator &op, ColumnBindingReplacer &replacer,
                                      const std::unordered_map<old_idx, new_idx> &table_mapping) {
	LogicalOperatorVisitor::EnumerateExpressions(op, [&](unique_ptr<Expression> *expression) {
		if (expression && *expression) {
			replacer.VisitExpression(expression);
		}
	});
	if (op.type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN) {
		auto &join = op.Cast<LogicalDependentJoin>();
		if (join.condition) {
			replacer.VisitExpression(&join.condition);
		}
		for (auto &column : join.correlated_columns) {
			auto entry = table_mapping.find(column.binding.table_index);
			if (entry != table_mapping.end()) {
				column.binding.table_index = entry->second;
			}
		}
	}
	for (auto &child : op.children) {
		if (child) {
			RebindExpressionsNullSafe(*child, replacer, table_mapping);
		}
	}
}

static ColumnBindingReplacer vec_to_replacer(const std::vector<ColumnBinding> &bindings,
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
		replacer.replacement_bindings.emplace_back(cb,
		                                           ColumnBinding(TableIndex(new_t), ProjectionIndex(cb.column_index)));
	}
	return replacer;
}

RenumberWrapper renumber_and_rebind_subtree(unique_ptr<LogicalOperator> plan, Binder &binder) {
	std::unordered_map<old_idx, new_idx> table_reassign;
	RenumberWrapper res = renumber_table_indices(std::move(plan), binder, table_reassign);
	ColumnBindingReplacer replacer = vec_to_replacer(res.column_bindings, res.idx_map);
	if (!res.contains_delim_operator) {
		replacer.VisitOperator(*res.op);
	} else {
		// Some DELIM/DEPENDENT join expression slots are nullable until delimiter
		// rewriting. DuckDB's generic visitor dereferences them unconditionally.
		RebindExpressionsNullSafe(*res.op, replacer, res.idx_map);
	}
	return res;
}

} // namespace duckdb
