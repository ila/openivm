#include "core/ivm_plan_rewrite.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
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
static void RewriteDistinct(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &node) {
	if (node->type != LogicalOperatorType::LOGICAL_DISTINCT) {
		for (auto &child : node->children) {
			RewriteDistinct(context, binder, child);
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
	count_expr->alias = ivm::DISTINCT_COUNT_COL;

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(count_expr));

	idx_t group_index = binder.GenerateTableIndex();
	idx_t aggregate_index = binder.GenerateTableIndex();

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

/// Decompose AVG(x) into SUM(x), COUNT(x), and SUM/COUNT ratio.
/// Walks top-down: finds AVG in aggregate, replaces it, propagates new bindings
/// upward through projections, and adds the ratio expression at each projection.
static void RewriteAvg(ClientContext &context, unique_ptr<LogicalOperator> &plan, Optimizer &opt) {
	// Recurse into children first (bottom-up ensures aggregates are processed before parents)
	for (auto &child : plan->children) {
		RewriteAvg(context, child, opt);
	}

	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	// Check if the child is an aggregate with AVG
	if (plan->children.empty() || plan->children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = plan->children[0]->Cast<LogicalAggregate>();
	bool has_avg = false;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE &&
		    expr->Cast<BoundAggregateExpression>().function.name == "avg") {
			has_avg = true;
			break;
		}
	}
	if (!has_avg) {
		return;
	}

	auto &proj = plan->Cast<LogicalProjection>();
	size_t group_count = agg.groups.size();

	// Step 1: Replace AVG(x) with SUM(x) + COUNT(x) in the aggregate.
	// Track which old expression index maps to the new SUM/COUNT indices.
	struct AvgDecomp {
		string alias;
		idx_t sum_idx; // index in new_exprs
		idx_t count_idx;
		idx_t old_idx; // index in original expressions
	};
	vector<AvgDecomp> decomps;
	vector<unique_ptr<Expression>> new_exprs;
	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &expr = agg.expressions[i];
		if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
			auto &bound = expr->Cast<BoundAggregateExpression>();
			if (bound.function.name == "avg" && !bound.children.empty()) {
				string alias = bound.alias.empty() ? ("avg_" + bound.children[0]->GetName()) : bound.alias;
				auto arg_type = bound.children[0]->return_type;

				AvgDecomp d;
				d.alias = alias;
				d.old_idx = i;

				auto sum_func = BindAggregateByName(context, "sum", {arg_type});
				vector<unique_ptr<Expression>> sum_args;
				sum_args.push_back(bound.children[0]->Copy());
				auto sum_expr = make_uniq<BoundAggregateExpression>(std::move(sum_func), std::move(sum_args), nullptr,
				                                                    nullptr, AggregateType::NON_DISTINCT);
				sum_expr->alias = "_ivm_sum_" + alias;
				d.sum_idx = new_exprs.size();
				new_exprs.push_back(std::move(sum_expr));

				auto count_func = BindAggregateByName(context, "count", {arg_type});
				vector<unique_ptr<Expression>> count_args;
				count_args.push_back(bound.children[0]->Copy());
				auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(count_args),
				                                                      nullptr, nullptr, AggregateType::NON_DISTINCT);
				count_expr->alias = "_ivm_count_" + alias;
				d.count_idx = new_exprs.size();
				new_exprs.push_back(std::move(count_expr));

				decomps.push_back(std::move(d));
				continue;
			}
		}
		new_exprs.push_back(std::move(expr));
	}
	agg.expressions = std::move(new_exprs);
	agg.ResolveOperatorTypes();

	// Step 2: Update the parent projection.
	// The old projection referenced AVG's binding. Now we need to:
	// - Replace the AVG column ref with a SUM/COUNT ratio expression
	// - Add SUM and COUNT as extra passthrough columns (for the upsert MERGE)
	auto agg_bindings = plan->children[0]->GetColumnBindings();
	auto agg_types = plan->children[0]->types;

	for (auto &d : decomps) {
		// Old AVG binding was at (aggregate_index, old_idx)
		ColumnBinding old_avg_binding(agg.aggregate_index, d.old_idx);

		// New SUM and COUNT bindings
		ColumnBinding sum_binding = agg_bindings[group_count + d.sum_idx];
		ColumnBinding count_binding = agg_bindings[group_count + d.count_idx];
		LogicalType sum_type = agg_types[group_count + d.sum_idx];
		LogicalType count_type = agg_types[group_count + d.count_idx];

		// Find the projection expression that referenced the old AVG and replace it with SUM/COUNT ratio
		for (idx_t pi = 0; pi < proj.expressions.size(); pi++) {
			if (proj.expressions[pi]->type == ExpressionType::BOUND_COLUMN_REF) {
				auto &ref = proj.expressions[pi]->Cast<BoundColumnRefExpression>();
				if (ref.binding == old_avg_binding) {
					// Replace with SUM / COUNT
					auto sum_ref = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
					auto count_ref = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
					auto ratio = opt.BindScalarFunction("/", std::move(sum_ref), std::move(count_ref));
					ratio->alias = d.alias;
					proj.expressions[pi] = std::move(ratio);
					break;
				}
			}
		}

		// Add SUM and COUNT as extra passthrough columns (hidden, for upsert MERGE)
		auto sum_passthrough = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
		sum_passthrough->alias = "_ivm_sum_" + d.alias;
		proj.expressions.push_back(std::move(sum_passthrough));

		auto count_passthrough = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
		count_passthrough->alias = "_ivm_count_" + d.alias;
		proj.expressions.push_back(std::move(count_passthrough));
	}
	proj.ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] AVG → SUM/COUNT ratio + hidden columns, %zu decompositions\n",
	                    decomps.size());
}

/// Add _ivm_left_key projection for LEFT/RIGHT JOINs at the top of the plan.
static void RewriteLeftJoinKey(Binder &binder, unique_ptr<LogicalOperator> &plan) {
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

	// Skip _ivm_left_key for plans with AGGREGATE above the LEFT JOIN.
	// These use group-recompute (not partial LEFT JOIN recompute), so the key isn't needed
	// and can't pass through the AGGREGATE node anyway.
	bool has_aggregate = false;
	std::function<void(LogicalOperator *)> check_agg = [&](LogicalOperator *n) {
		if (n->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			has_aggregate = true;
		}
		for (auto &child : n->children) {
			if (!has_aggregate) {
				check_agg(child.get());
			}
		}
	};
	check_agg(plan.get());
	if (has_aggregate) {
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
		// Key was projected away. Propagate it through intermediate operators
		// by adding passthrough expressions (same approach as PAC's PropagateSingleBinding).

		// Step 1: Find path from plan root to the join that has the key
		struct PathEntry {
			LogicalOperator *op;
			idx_t child_idx;
		};
		vector<PathEntry> path;
		std::function<bool(LogicalOperator *, bool)> find_path = [&](LogicalOperator *n, bool is_root) -> bool {
			// Check if this node outputs the key binding
			auto binds = n->GetColumnBindings();
			for (auto &b : binds) {
				if (b == key_binding) {
					return true;
				}
			}
			for (idx_t ci = 0; ci < n->children.size(); ci++) {
				if (find_path(n->children[ci].get(), false)) {
					if (!is_root) {
						path.push_back({n, ci});
					}
					return true;
				}
			}
			return false;
		};
		find_path(plan.get(), false);
		// Reverse: path is currently top-down, we need bottom-up
		std::reverse(path.begin(), path.end());

		// Step 2: Propagate binding through each operator on the path
		ColumnBinding current = key_binding;
		for (auto &entry : path) {
			if (entry.op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto &proj = entry.op->Cast<LogicalProjection>();
				// Check if already passed through
				bool found = false;
				for (idx_t i = 0; i < proj.expressions.size(); i++) {
					if (proj.expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
						auto &ref = proj.expressions[i]->Cast<BoundColumnRefExpression>();
						if (ref.binding == current) {
							current = ColumnBinding(proj.table_index, i);
							found = true;
							break;
						}
					}
				}
				if (!found) {
					auto col_ref = make_uniq<BoundColumnRefExpression>(key_type, current);
					proj.expressions.push_back(std::move(col_ref));
					current = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
				}
				proj.ResolveOperatorTypes();
			} else if (entry.op->type == LogicalOperatorType::LOGICAL_FILTER) {
				auto &filter = entry.op->Cast<LogicalFilter>();
				if (!filter.projection_map.empty()) {
					bool in_map = false;
					for (auto &idx : filter.projection_map) {
						if (idx == current.column_index) {
							in_map = true;
							break;
						}
					}
					if (!in_map) {
						filter.projection_map.push_back(current.column_index);
					}
				}
				filter.ResolveOperatorTypes();
			}
			// JOINs pass bindings through — no action needed
		}
		key_binding = current;

		// After propagation, refresh top bindings and rebuild proj_exprs.
		top_bindings = plan->GetColumnBindings();
		top_types = plan->types;
		proj_exprs.clear();
		for (idx_t i = 0; i < top_bindings.size(); i++) {
			proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(top_types[i], top_bindings[i]));
		}
	}

	// Always add _ivm_left_key as a separate extra column.
	proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(ivm::LEFT_KEY_COL, key_type, key_binding));

	// Use a table index that won't conflict (high number)
	idx_t proj_table_index = binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_exprs));
	projection->children.push_back(std::move(plan));
	projection->ResolveOperatorTypes();
	plan = std::move(projection);

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Added _ivm_left_key projection\n");
}

void IVMPlanRewrite(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                    vector<string> &planner_names) {
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Starting\n");
	bool had_distinct = plan->type == LogicalOperatorType::LOGICAL_DISTINCT ||
	                    (plan->type == LogicalOperatorType::LOGICAL_PROJECTION && !plan->children.empty() &&
	                     plan->children[0]->type == LogicalOperatorType::LOGICAL_DISTINCT);
	RewriteDistinct(context, binder, plan);
	if (had_distinct) {
		planner_names.push_back(ivm::DISTINCT_COUNT_COL);
	}
	{
		Optimizer opt(binder, context);
		RewriteAvg(context, plan, opt);
	}
	RewriteLeftJoinKey(binder, plan);
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Done\n");
}

// ============================================================================
// StripHavingFilter: remove HAVING filter, return predicate using output aliases
// ============================================================================

/// Convert a FILTER condition to SQL using output column aliases.
static string HavingExprToSQL(const Expression &expr, const unordered_map<uint64_t, string> &binding_to_alias) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &col = expr.Cast<BoundColumnRefExpression>();
		uint64_t key = (uint64_t)col.binding.table_index ^ ((uint64_t)col.binding.column_index * 0x9e3779b97f4a7c15ULL);
		auto it = binding_to_alias.find(key);
		return (it != binding_to_alias.end()) ? it->second : col.ToString();
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comp = expr.Cast<BoundComparisonExpression>();
		return "(" + HavingExprToSQL(*comp.left, binding_to_alias) + " " + ExpressionTypeToOperator(comp.type) + " " +
		       HavingExprToSQL(*comp.right, binding_to_alias) + ")";
	}
	case ExpressionClass::BOUND_CONSTANT: {
		return expr.Cast<BoundConstantExpression>().value.ToString();
	}
	case ExpressionClass::BOUND_CAST: {
		return HavingExprToSQL(*expr.Cast<BoundCastExpression>().child, binding_to_alias);
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conj = expr.Cast<BoundConjunctionExpression>();
		string op = (conj.type == ExpressionType::CONJUNCTION_AND) ? " AND " : " OR ";
		string result;
		for (idx_t i = 0; i < conj.children.size(); i++) {
			if (i > 0) {
				result += op;
			}
			result += "(" + HavingExprToSQL(*conj.children[i], binding_to_alias) + ")";
		}
		return result;
	}
	default:
		return expr.ToString();
	}
}

string StripHavingFilter(unique_ptr<LogicalOperator> &plan, const vector<string> &output_names) {
	// Find PROJECTION → FILTER → AGGREGATE pattern.
	LogicalOperator *parent = nullptr;
	LogicalOperator *filter_node = nullptr;

	std::function<bool(LogicalOperator *, LogicalOperator *)> find_filter;
	find_filter = [&](LogicalOperator *node, LogicalOperator *par) -> bool {
		if (node->type == LogicalOperatorType::LOGICAL_FILTER && !node->children.empty() &&
		    node->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			parent = par;
			filter_node = node;
			return true;
		}
		for (auto &child : node->children) {
			if (find_filter(child.get(), node)) {
				return true;
			}
		}
		return false;
	};

	if (!find_filter(plan.get(), nullptr)) {
		return "";
	}

	// Build binding → alias map from the PROJECTION above the FILTER.
	unordered_map<uint64_t, string> binding_to_alias;
	if (parent && parent->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = parent->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size() && i < output_names.size(); i++) {
			if (proj.expressions[i]->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col = proj.expressions[i]->Cast<BoundColumnRefExpression>();
				uint64_t key =
				    (uint64_t)col.binding.table_index ^ ((uint64_t)col.binding.column_index * 0x9e3779b97f4a7c15ULL);
				binding_to_alias[key] = output_names[i];
			}
		}
	}

	// Extract HAVING predicate as SQL.
	auto &filter = filter_node->Cast<LogicalFilter>();
	string having_sql;
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		if (i > 0) {
			having_sql += " AND ";
		}
		having_sql += HavingExprToSQL(*filter.expressions[i], binding_to_alias);
	}

	// Remove the FILTER node from the plan.
	if (parent) {
		for (auto &child : parent->children) {
			if (child.get() == filter_node) {
				child = std::move(filter_node->children[0]);
				break;
			}
		}
	} else {
		plan = std::move(filter_node->children[0]);
	}

	OPENIVM_DEBUG_PRINT("[StripHavingFilter] Extracted HAVING predicate: %s\n", having_sql.c_str());
	return having_sql;
}

} // namespace duckdb
