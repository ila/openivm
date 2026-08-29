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
	auto &distinct_node = input.plan->Cast<LogicalDistinct>();
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::DISTINCT_COUNT_AGGREGATE);
	OPENIVM_DEBUG_PRINT("[DeltaDistinct] Rewriting DISTINCT node, %zu targets\n",
	                    distinct_node.distinct_targets.size());

	// Save the DISTINCT's output bindings before rewrite — these are what parent operators
	// currently reference via BoundColumnRefExpression. After we replace DISTINCT with
	// AGGREGATE, the aggregate uses fresh group_index/aggregate_index, so we must remap.
	const vector<ColumnBinding> original_bindings = input.plan->GetColumnBindings();

	auto child_mul = input.CompileChild(input.plan->children[0], input.root);
	auto rewritten_child = std::move(child_mul.op);
	ColumnBinding input_mul_binding = child_mul.mul_binding;

	Binder &binder = input.context.input.optimizer.binder;
	auto child_bindings = rewritten_child->GetColumnBindings();
	auto child_types = rewritten_child->types;
	// DuckDB plans expose one type per binding; this guard documents and diagnoses a broken internal plan.
	if (child_bindings.size() != child_types.size()) { // mull-ignore: cxx_ne_to_eq
		throw InternalException("DeltaDistinct: rewritten child binding/type count mismatch");
	}
	// CompileChild's contract adds exactly one multiplicity output to the original DISTINCT columns.
	if (child_bindings.size() != original_bindings.size() + 1) { // mull-ignore: cxx_ne_to_eq,cxx_add_to_sub
		throw InternalException("DeltaDistinct: expected one multiplicity column in rewritten child");
	}

	// Group-by keys = all child columns EXCEPT multiplicity
	// Aggregate = COUNT(*) as openivm_distinct_count
	// Multiplicity is added as a group-by key (same pattern as aggregate delta)
	idx_t group_index = binder.GenerateTableIndex();
	idx_t aggregate_index = binder.GenerateTableIndex();

	auto count_star_func = CountStarFun::GetFunction();
	vector<unique_ptr<Expression>> count_args;
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_star_func), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = openivm::DISTINCT_COUNT_COL;

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(count_expr));

	auto agg_node = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));

	GroupingSet grouping_set;
	const idx_t child_output_count = child_bindings.size();
	// The validated binding/type vectors define this exact traversal; changing the bound is invalid planner state.
	for (idx_t i = 0; i < child_output_count; // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
	     i++) {
		if (child_bindings[i] == input_mul_binding) {
			continue; // skip multiplicity — added separately below
		}
		auto group_expr = make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]);
		grouping_set.insert(agg_node->groups.size());
		agg_node->groups.push_back(std::move(group_expr));
		agg_node->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(child_types[i])));
	}

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
	idx_t original_position = 0;
	// Every non-multiplicity child output corresponds to exactly one original DISTINCT output.
	for (idx_t i = 0; i < child_output_count; // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
	     i++) {
		if (child_bindings[i] == input_mul_binding) {
			continue; // multiplicity is not in the parent's original bindings
		}
		ColumnBinding new_binding(group_index, group_position);
		replacer.replacement_bindings.emplace_back(original_bindings[original_position], new_binding);
		original_position++;
		group_position++;
	}
	// The cardinality contract above guarantees this; retain the check to diagnose future compiler regressions.
	if (original_position != original_bindings.size()) { // mull-ignore: cxx_ne_to_eq
		throw InternalException("DeltaDistinct: rewritten child did not expose every original output binding");
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
