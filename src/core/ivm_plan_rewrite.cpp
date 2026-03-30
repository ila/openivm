#include "core/ivm_plan_rewrite.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

/// Bind an aggregate function by name from the catalog.
static AggregateFunction BindAggregateByName(ClientContext &context, const string &name,
                                             const vector<LogicalType> &arg_types) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
	FunctionBinder binder(context);
	ErrorData error;
	auto best = binder.BindFunction(entry.name, entry.functions, arg_types, error);
	if (!best.IsValid()) {
		throw InternalException("IVMPlanRewrite: failed to bind aggregate '%s'", name);
	}
	return entry.functions.GetFunctionByOffset(best.GetIndex());
}

/// Replace LOGICAL_DISTINCT with LOGICAL_AGGREGATE + COUNT(*).
static void RewriteDistinct(ClientContext &context, unique_ptr<LogicalOperator> &node) {
	if (node->type != LogicalOperatorType::LOGICAL_DISTINCT) {
		for (auto &child : node->children) {
			RewriteDistinct(context, child);
		}
		return;
	}

	auto &distinct = node->Cast<LogicalDistinct>();
	if (node->children.empty()) {
		OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] DISTINCT has no children — skipping\n");
		return;
	}
	auto &child = node->children[0];
	child->ResolveOperatorTypes();
	auto child_bindings = child->GetColumnBindings();
	auto child_types = child->types;

	// COUNT(*) aggregate
	auto count_star = CountStarFun::GetFunction();
	vector<unique_ptr<Expression>> count_args;
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_star), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = "_ivm_distinct_count";

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(count_expr));

	auto table_indices = distinct.GetTableIndex();
	idx_t group_index = table_indices.empty() ? 9990 : table_indices.front();
	idx_t aggregate_index = group_index + 1;

	auto agg_node = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));

	GroupingSet grouping_set;
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		auto group_expr = make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]);
		grouping_set.insert(agg_node->groups.size());
		agg_node->groups.push_back(std::move(group_expr));
		agg_node->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(child_types[i])));
	}
	agg_node->grouping_sets.push_back(std::move(grouping_set));

	agg_node->children.push_back(std::move(child));
	agg_node->ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] DISTINCT → AGGREGATE + COUNT(*), %zu groups\n", agg_node->groups.size());
	node = std::move(agg_node);
}

/// Decompose AVG(x) into SUM(x) + COUNT(x) in LOGICAL_AGGREGATE nodes.
/// The upsert compiler detects _ivm_sum_/_ivm_count_ columns and handles the ratio.
static void RewriteAvg(ClientContext &context, unique_ptr<LogicalOperator> &node) {
	for (auto &child : node->children) {
		RewriteAvg(context, child);
	}

	if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}

	auto &agg = node->Cast<LogicalAggregate>();
	bool has_avg = false;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
			auto &bound = expr->Cast<BoundAggregateExpression>();
			if (bound.function.name == "avg") {
				has_avg = true;
				break;
			}
		}
	}
	if (!has_avg) {
		return;
	}

	vector<unique_ptr<Expression>> new_exprs;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
			auto &bound = expr->Cast<BoundAggregateExpression>();
			if (bound.function.name == "avg" && !bound.children.empty()) {
				string alias = bound.alias.empty() ? ("avg_" + bound.children[0]->GetName()) : bound.alias;
				auto arg_type = bound.children[0]->return_type;

				// SUM(x) as _ivm_sum_<alias>
				auto sum_func = BindAggregateByName(context, "sum", {arg_type});
				vector<unique_ptr<Expression>> sum_args;
				sum_args.push_back(bound.children[0]->Copy());
				auto sum_expr = make_uniq<BoundAggregateExpression>(std::move(sum_func), std::move(sum_args), nullptr,
				                                                    nullptr, AggregateType::NON_DISTINCT);
				sum_expr->alias = "_ivm_sum_" + alias;
				new_exprs.push_back(std::move(sum_expr));

				// COUNT(x) as _ivm_count_<alias>
				auto count_func = BindAggregateByName(context, "count", {arg_type});
				vector<unique_ptr<Expression>> count_args;
				count_args.push_back(bound.children[0]->Copy());
				auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(count_args),
				                                                      nullptr, nullptr, AggregateType::NON_DISTINCT);
				count_expr->alias = "_ivm_count_" + alias;
				new_exprs.push_back(std::move(count_expr));

				// The ratio (SUM/COUNT = AVG) is computed by the upsert compiler
				// via the _ivm_sum_/_ivm_count_ naming convention.
				// A proper projection (SUM::DOUBLE / NULLIF(COUNT, 0) AS alias) would be added here
				// but requires binding division/NULLIF — deferred to upsert compiler.
				continue;
			}
		}
		new_exprs.push_back(std::move(expr));
	}
	agg.expressions = std::move(new_exprs);
	agg.ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] AVG → SUM + COUNT decomposition\n");
}

/// Add _ivm_left_key projection for LEFT/RIGHT JOINs at the top of the plan.
static void RewriteLeftJoinKey(unique_ptr<LogicalOperator> &plan) {
	// Find the first LEFT/RIGHT JOIN and extract the preserved-side key binding
	bool found = false;
	ColumnBinding key_binding;
	LogicalType key_type;

	std::function<void(LogicalOperator *)> find = [&](LogicalOperator *n) {
		if (found) {
			return;
		}
		if (n->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = n->Cast<LogicalComparisonJoin>();
			if ((join.join_type == JoinType::LEFT || join.join_type == JoinType::RIGHT) && !join.conditions.empty()) {
				found = true;
				auto &preserved = join.join_type == JoinType::LEFT ? join.conditions[0].left : join.conditions[0].right;
				if (preserved->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					auto &ref = preserved->Cast<BoundColumnRefExpression>();
					key_binding = ref.binding;
					key_type = ref.return_type;
				}
			}
		}
		for (auto &child : n->children) {
			find(child.get());
		}
	};
	find(plan.get());

	if (!found) {
		return;
	}

	// Ensure types are resolved before accessing them
	plan->ResolveOperatorTypes();
	// Add projection: all existing columns + _ivm_left_key
	auto top_bindings = plan->GetColumnBindings();
	auto top_types = plan->types;

	vector<unique_ptr<Expression>> proj_exprs;
	for (idx_t i = 0; i < top_bindings.size(); i++) {
		proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(top_types[i], top_bindings[i]));
	}

	// The key_binding references a column inside the join tree. We need to check if it's
	// accessible from the top. If the join key was projected through, it's in top_bindings.
	// If not, we need to find it. For safety, search top_bindings for the key.
	bool key_in_output = false;
	for (idx_t i = 0; i < top_bindings.size(); i++) {
		if (top_bindings[i] == key_binding) {
			key_in_output = true;
			key_type = top_types[i];
			break;
		}
	}

	if (!key_in_output) {
		// The join key is not in the output projection — it was projected away.
		// We can't add it without propagating through intermediate projections.
		// For now, fall back — the string-level AddLeftJoinKey would handle this.
		OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] LEFT JOIN key not in output — skipping _ivm_left_key\n");
		return;
	}

	auto key_ref = make_uniq<BoundColumnRefExpression>("_ivm_left_key", key_type, key_binding);
	proj_exprs.push_back(std::move(key_ref));

	// Use a table index that won't conflict (high number)
	idx_t proj_table_index = 9999;
	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_exprs));
	projection->children.push_back(std::move(plan));
	projection->ResolveOperatorTypes();
	plan = std::move(projection);

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Added _ivm_left_key projection\n");
}

void IVMPlanRewrite(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Starting\n");
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Step 1: RewriteDistinct\n");
	RewriteDistinct(context, plan);
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Step 2: RewriteAvg\n");
	RewriteAvg(context, plan);
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Step 3: RewriteLeftJoinKey\n");
	RewriteLeftJoinKey(plan);
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Done\n");
}

} // namespace duckdb
