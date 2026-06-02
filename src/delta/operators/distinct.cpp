#include "delta/delta_operator.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

namespace duckdb {

DeltaPlanFragment CompileDistinctDelta(DeltaOperatorInput input) {
	auto *distinct_node = dynamic_cast<LogicalDistinct *>(input.plan.get());
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::DISTINCT_COUNT_AGGREGATE);
	OPENIVM_DEBUG_PRINT("[DeltaDistinct] Rewriting DISTINCT node, %zu targets\n",
	                    distinct_node->distinct_targets.size());

	// Save the DISTINCT's output bindings before rewrite — these are what parent operators
	// currently reference via BoundColumnRefExpression. After we replace DISTINCT with
	// AGGREGATE, the aggregate uses fresh group_index/aggregate_index, so we must remap.
	const vector<ColumnBinding> original_bindings = input.plan->GetColumnBindings();

	// 1. Recurse into child first (gets delta with multiplicity)
	auto child_mul = input.CompileChild(input.plan->children[0], input.root);
	auto rewritten_child = std::move(child_mul.op);
	ColumnBinding input_mul_binding = child_mul.mul_binding;

	Binder &binder = input.context.input.optimizer.binder;
	auto child_bindings = rewritten_child->GetColumnBindings();
	auto child_types = rewritten_child->types;

	// 2. Build a LOGICAL_AGGREGATE_AND_GROUP_BY that replaces the DISTINCT node.
	// Group-by keys = all child columns EXCEPT multiplicity
	// Aggregate = COUNT(*) as openivm_distinct_count
	// Multiplicity is added as a group-by key (same pattern as aggregate delta)
	idx_t group_index = binder.GenerateTableIndex();
	idx_t aggregate_index = binder.GenerateTableIndex();

	// Create COUNT(*) aggregate expression
	auto count_star_func = CountStarFun::GetFunction();
	vector<unique_ptr<Expression>> count_args;
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_star_func), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = openivm::DISTINCT_COUNT_COL;

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(count_expr));

	auto agg_node = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));

	// Add all child columns as group-by keys (except multiplicity)
	GroupingSet grouping_set;
	const idx_t child_output_count = MinValue<idx_t>(child_bindings.size(), child_types.size());
	for (idx_t i = 0; i < child_output_count; i++) {
		if (child_bindings[i] == input_mul_binding) {
			continue; // skip multiplicity — added separately below
		}
		auto group_expr = make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]);
		grouping_set.insert(agg_node->groups.size());
		agg_node->groups.push_back(std::move(group_expr));
		agg_node->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(child_types[i])));
	}

	// Add multiplicity as the last group-by key (same pattern as aggregate delta)
	ColumnBinding mod_mul_binding;
	mod_mul_binding.column_index = agg_node->groups.size();
	mod_mul_binding.table_index = group_index;

	auto mul_expr = make_uniq<BoundColumnRefExpression>(openivm::MULTIPLICITY_COL, input.mul_type, input_mul_binding);
	grouping_set.insert(agg_node->groups.size());
	agg_node->groups.push_back(std::move(mul_expr));
	agg_node->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(input.mul_type)));

	agg_node->grouping_sets.push_back(std::move(grouping_set));

	agg_node->children.push_back(std::move(rewritten_child));
	agg_node->ResolveOperatorTypes();

	// Remap parent references: each original DISTINCT output binding (child's i-th binding)
	// is replaced with the new aggregate's group output binding (group_index, group_position).
	// Parent projections/filters holding BCRs to the old bindings will now resolve to the
	// aggregate's new group columns.
	//
	// Stop the walk at the old DISTINCT node itself (still in the tree — the caller will
	// swap it out for our new aggregate after we return). This prevents the replacer from
	// descending into the already-rewritten UNION subtree below, where the same bindings
	// are still valid (they are the UNION's genuine output bindings).
	ColumnBindingReplacer replacer;
	idx_t group_position = 0;
	const idx_t remap_count = MinValue<idx_t>(original_bindings.size(), child_output_count);
	for (idx_t i = 0; i < remap_count; i++) {
		if (child_bindings[i] == input_mul_binding) {
			continue; // multiplicity is not in the parent's original bindings
		}
		ColumnBinding new_binding(group_index, group_position);
		replacer.replacement_bindings.emplace_back(original_bindings[i], new_binding);
		group_position++;
	}
	replacer.stop_operator = input.plan.get();
	replacer.VisitOperator(*input.root);

	OPENIVM_DEBUG_PRINT("[DeltaDistinct] Done, replaced with AGGREGATE (%zu groups + 1 count), mul_binding: "
	                    "table=%lu col=%lu\n",
	                    agg_node->groups.size() - 1, (unsigned long)mod_mul_binding.table_index,
	                    (unsigned long)mod_mul_binding.column_index);

	return {std::move(agg_node), mod_mul_binding};
}

} // namespace duckdb
