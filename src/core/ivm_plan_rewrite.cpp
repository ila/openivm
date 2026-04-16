#include "core/ivm_plan_rewrite.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
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

static bool IsStddevOrVariance(const string &name) {
	return name == "stddev" || name == "stddev_samp" || name == "stddev_pop" || name == "variance" ||
	       name == "var_samp" || name == "var_pop";
}

static bool IsPopulationVariant(const string &name) {
	return name == "stddev_pop" || name == "var_pop";
}

static bool IsStddevVariant(const string &name) {
	return name == "stddev" || name == "stddev_samp" || name == "stddev_pop";
}

/// Pick the SUM_SQ hidden column prefix based on the original function.
/// Encodes both stddev-vs-variance (sqrt) and sample-vs-population (denominator).
static const char *SumSqPrefix(const string &func_name) {
	if (func_name == "stddev_pop") {
		return ivm::SUM_SQP_COL_PREFIX;
	}
	if (func_name == "var_pop") {
		return ivm::VAR_SQP_COL_PREFIX;
	}
	if (func_name == "variance" || func_name == "var_samp") {
		return ivm::VAR_SQ_COL_PREFIX;
	}
	return ivm::SUM_SQ_COL_PREFIX; // stddev, stddev_samp
}

/// Decompose AVG and STDDEV/VARIANCE aggregates into incrementalizable components.
/// Handles both in a SINGLE PASS to keep aggregate expression indices consistent.
/// - AVG(x)      → SUM(x), COUNT(x) + SUM/COUNT ratio in projection
/// - STDDEV(x)   → SUM(x), SUM(x*x), COUNT(x) + variance formula in projection
/// - VARIANCE(x) → same, without sqrt
static void RewriteDerivedAggregates(ClientContext &context, unique_ptr<LogicalOperator> &plan, Optimizer &opt) {
	for (auto &child : plan->children) {
		RewriteDerivedAggregates(context, child, opt);
	}

	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	if (plan->children.empty() || plan->children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = plan->children[0]->Cast<LogicalAggregate>();

	// Check if there's anything to decompose
	bool has_derived = false;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
			auto &name = expr->Cast<BoundAggregateExpression>().function.name;
			if (name == "avg" || IsStddevOrVariance(name)) {
				has_derived = true;
				break;
			}
		}
	}
	if (!has_derived) {
		return;
	}

	auto &proj = plan->Cast<LogicalProjection>();
	size_t group_count = agg.groups.size();

	// Unified decomposition record for both AVG and STDDEV/VARIANCE
	enum class DecompKind { AVG, STDDEV };
	struct Decomp {
		DecompKind kind;
		string alias;      // internal alias for hidden column suffixes
		string user_alias; // user-facing alias (from projection expression), set in step 2
		string func_name;  // original function name
		idx_t sum_idx;
		idx_t sum_sq_idx; // only for STDDEV
		idx_t count_idx;
		idx_t old_idx; // index in ORIGINAL expressions array
	};
	vector<Decomp> decomps;
	vector<unique_ptr<Expression>> new_exprs;

	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &expr = agg.expressions[i];
		if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
			new_exprs.push_back(std::move(expr));
			continue;
		}
		auto &bound = expr->Cast<BoundAggregateExpression>();
		if (bound.children.empty()) {
			new_exprs.push_back(std::move(expr));
			continue;
		}

		bool is_avg = (bound.function.name == "avg");
		bool is_stddev = IsStddevOrVariance(bound.function.name);
		if (!is_avg && !is_stddev) {
			new_exprs.push_back(std::move(expr));
			continue;
		}

		string alias = bound.alias.empty() ? (bound.function.name + "_" + bound.children[0]->GetName()) : bound.alias;
		auto arg_type = bound.children[0]->return_type;

		Decomp d;
		d.kind = is_avg ? DecompKind::AVG : DecompKind::STDDEV;
		d.alias = alias;
		d.func_name = bound.function.name;
		d.old_idx = i;

		// SUM(x) — both AVG and STDDEV need this
		auto sum_func = BindAggregateByName(context, "sum", {arg_type});
		vector<unique_ptr<Expression>> sum_args;
		sum_args.push_back(bound.children[0]->Copy());
		auto sum_expr = make_uniq<BoundAggregateExpression>(std::move(sum_func), std::move(sum_args), nullptr, nullptr,
		                                                    AggregateType::NON_DISTINCT);
		sum_expr->alias = string(ivm::SUM_COL_PREFIX) + alias;
		d.sum_idx = new_exprs.size();
		new_exprs.push_back(std::move(sum_expr));

		// SUM(x * x) — STDDEV/VARIANCE only
		if (is_stddev) {
			auto sq_arg = opt.BindScalarFunction("*", bound.children[0]->Copy(), bound.children[0]->Copy());
			auto sum_sq_func = BindAggregateByName(context, "sum", {sq_arg->return_type});
			vector<unique_ptr<Expression>> sum_sq_args;
			sum_sq_args.push_back(std::move(sq_arg));
			auto sum_sq_expr = make_uniq<BoundAggregateExpression>(std::move(sum_sq_func), std::move(sum_sq_args),
			                                                       nullptr, nullptr, AggregateType::NON_DISTINCT);
			sum_sq_expr->alias = string(SumSqPrefix(bound.function.name)) + alias;
			d.sum_sq_idx = new_exprs.size();
			new_exprs.push_back(std::move(sum_sq_expr));
		}

		// COUNT(x) — both AVG and STDDEV need this
		auto count_func = BindAggregateByName(context, "count", {arg_type});
		vector<unique_ptr<Expression>> count_args;
		count_args.push_back(bound.children[0]->Copy());
		auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(count_args), nullptr,
		                                                      nullptr, AggregateType::NON_DISTINCT);
		count_expr->alias = string(ivm::COUNT_COL_PREFIX) + alias;
		d.count_idx = new_exprs.size();
		new_exprs.push_back(std::move(count_expr));

		decomps.push_back(std::move(d));
	}
	agg.expressions = std::move(new_exprs);
	agg.ResolveOperatorTypes();

	// Step 2: Update projection — replace derived column refs with formulas
	auto agg_bindings = plan->children[0]->GetColumnBindings();
	auto agg_types = plan->children[0]->types;

	for (auto &d : decomps) {
		ColumnBinding old_binding(agg.aggregate_index, d.old_idx);
		ColumnBinding sum_binding = agg_bindings[group_count + d.sum_idx];
		ColumnBinding count_binding = agg_bindings[group_count + d.count_idx];
		LogicalType sum_type = agg_types[group_count + d.sum_idx];
		LogicalType count_type = agg_types[group_count + d.count_idx];

		// Find and replace the projection expression for this aggregate.
		// Capture the user's alias from the original projection expression (e.g., "my_avg" from
		// "avg(val) AS my_avg") — this is the column name in the data table. We use it for
		// hidden column naming so DetectDerivedAggColumns can match them in the upsert compiler.
		for (idx_t pi = 0; pi < proj.expressions.size(); pi++) {
			if (proj.expressions[pi]->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto &ref = proj.expressions[pi]->Cast<BoundColumnRefExpression>();
			if (ref.binding != old_binding) {
				continue;
			}

			// Capture user's alias BEFORE replacing the expression
			d.user_alias = ref.alias.empty() ? d.alias : ref.alias;

			unique_ptr<Expression> result;
			if (d.kind == DecompKind::AVG) {
				auto sum_ref = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
				auto count_ref = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
				result = opt.BindScalarFunction("/", std::move(sum_ref), std::move(count_ref));
			} else {
				ColumnBinding sum_sq_binding = agg_bindings[group_count + d.sum_sq_idx];
				LogicalType sum_sq_type = agg_types[group_count + d.sum_sq_idx];

				auto s1 = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
				auto s2 = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
				auto sq = make_uniq<BoundColumnRefExpression>(sum_sq_type, sum_sq_binding);
				auto n = make_uniq<BoundColumnRefExpression>(count_type, count_binding);

				auto sum_sq_over_n =
				    opt.BindScalarFunction("/", opt.BindScalarFunction("*", std::move(s1), std::move(s2)),
				                           make_uniq<BoundColumnRefExpression>(count_type, count_binding));
				auto numerator = opt.BindScalarFunction("-", std::move(sq), std::move(sum_sq_over_n));

				unique_ptr<Expression> denom;
				if (IsPopulationVariant(d.func_name)) {
					denom = std::move(n);
				} else {
					denom =
					    opt.BindScalarFunction("-", std::move(n), make_uniq<BoundConstantExpression>(Value::BIGINT(1)));
				}
				auto var_expr = opt.BindScalarFunction("/", std::move(numerator), std::move(denom));
				auto formula = IsStddevVariant(d.func_name) ? opt.BindScalarFunction("sqrt", std::move(var_expr))
				                                            : std::move(var_expr);

				// Wrap in CASE WHEN count > threshold THEN formula ELSE NULL END
				// to produce NULL instead of nan for groups with insufficient rows.
				// Sample (stddev/variance): need count > 1; Population: need count > 0.
				int64_t threshold = IsPopulationVariant(d.func_name) ? 0 : 1;
				auto count_check = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
				auto when_expr =
				    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(count_check),
				                                         make_uniq<BoundConstantExpression>(Value::BIGINT(threshold)));
				auto else_expr = make_uniq<BoundConstantExpression>(Value(formula->return_type));
				result = make_uniq<BoundCaseExpression>(std::move(when_expr), std::move(formula), std::move(else_expr));
			}
			result->alias = d.user_alias;
			proj.expressions[pi] = std::move(result);
			break;
		}

		// Add hidden columns as passthroughs. Use user_alias so that
		// DetectDerivedAggColumns in the upsert compiler can match them.
		string col_suffix = d.user_alias.empty() ? d.alias : d.user_alias;
		auto sum_pt = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
		sum_pt->alias = string(ivm::SUM_COL_PREFIX) + col_suffix;
		proj.expressions.push_back(std::move(sum_pt));

		if (d.kind == DecompKind::STDDEV) {
			ColumnBinding sum_sq_binding = agg_bindings[group_count + d.sum_sq_idx];
			LogicalType sum_sq_type = agg_types[group_count + d.sum_sq_idx];
			auto sum_sq_pt = make_uniq<BoundColumnRefExpression>(sum_sq_type, sum_sq_binding);
			sum_sq_pt->alias = string(SumSqPrefix(d.func_name)) + col_suffix;
			proj.expressions.push_back(std::move(sum_sq_pt));
		}

		auto count_pt = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
		count_pt->alias = string(ivm::COUNT_COL_PREFIX) + col_suffix;
		proj.expressions.push_back(std::move(count_pt));
	}
	proj.ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Derived aggregates → hidden columns, %zu decompositions\n", decomps.size());
}

/// Add _ivm_left_key (and _ivm_right_key for FULL OUTER) projection at the top of the plan.
static void RewriteLeftJoinKey(Binder &binder, unique_ptr<LogicalOperator> &plan) {
	// Find the first LEFT/RIGHT/OUTER JOIN and extract key bindings
	bool found = false;
	bool is_full_outer = false;
	ColumnBinding key_binding;
	LogicalType key_type;
	ColumnBinding right_key_binding;
	LogicalType right_key_type;

	std::function<void(LogicalOperator *)> find = [&](LogicalOperator *n) {
		if (found) {
			return;
		}
		if (n->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = n->Cast<LogicalComparisonJoin>();
			if ((join.join_type == JoinType::LEFT || join.join_type == JoinType::RIGHT ||
			     join.join_type == JoinType::OUTER) &&
			    !join.conditions.empty()) {
				found = true;
				is_full_outer = (join.join_type == JoinType::OUTER);
				// For LEFT/OUTER: preserved side is left; for RIGHT: preserved side is right
				auto &preserved =
				    (join.join_type == JoinType::RIGHT) ? join.conditions[0].right : join.conditions[0].left;
				if (preserved->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					auto &ref = preserved->Cast<BoundColumnRefExpression>();
					key_binding = ref.binding;
					key_type = ref.return_type;
				}
				// For FULL OUTER: also extract the right-side key
				if (is_full_outer) {
					auto &right_key = join.conditions[0].right;
					if (right_key->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
						auto &ref = right_key->Cast<BoundColumnRefExpression>();
						right_key_binding = ref.binding;
						right_key_type = ref.return_type;
					}
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

	// For FULL OUTER: also add _ivm_right_key in the same projection.
	if (is_full_outer) {
		// The right key binding may also need propagation. Check if it's in the current top output.
		bool right_key_in_output = false;
		for (idx_t i = 0; i < top_bindings.size(); i++) {
			if (top_bindings[i] == right_key_binding) {
				right_key_in_output = true;
				right_key_type = top_types[i];
				break;
			}
		}
		if (!right_key_in_output) {
			// Propagate right key through top projection (it may have been projected away)
			if (plan->type == LogicalOperatorType::LOGICAL_PROJECTION) {
				auto &proj = plan->Cast<LogicalProjection>();
				bool already_in = false;
				for (idx_t i = 0; i < proj.expressions.size(); i++) {
					if (proj.expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
						auto &ref = proj.expressions[i]->Cast<BoundColumnRefExpression>();
						if (ref.binding == right_key_binding) {
							right_key_binding = ColumnBinding(proj.table_index, i);
							already_in = true;
							break;
						}
					}
				}
				if (!already_in) {
					// Need to add passthrough in intermediate projections
					// For simplicity, if the right key comes from a child output, just reference it
					auto child_binds = proj.children[0]->GetColumnBindings();
					for (auto &cb : child_binds) {
						if (cb == right_key_binding) {
							// Key is in child output — add a passthrough ref
							auto ref = make_uniq<BoundColumnRefExpression>(right_key_type, right_key_binding);
							proj.expressions.push_back(std::move(ref));
							right_key_binding = ColumnBinding(proj.table_index, proj.expressions.size() - 1);
							break;
						}
					}
				}
			}
		}
		proj_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(ivm::RIGHT_KEY_COL, right_key_type, right_key_binding));
	}

	// Use a table index that won't conflict (high number)
	idx_t proj_table_index = binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_exprs));
	projection->children.push_back(std::move(plan));
	projection->ResolveOperatorTypes();
	plan = std::move(projection);

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Added _ivm_left_key%s projection\n",
	                    is_full_outer ? " + _ivm_right_key" : "");
}

/// For LEFT/OUTER JOIN aggregate views: add COUNT(null_side_key) AS _ivm_match_count.
/// For FULL OUTER JOINs, also add COUNT(left_key) AS _ivm_right_match_count.
/// These hidden aggregates track how many rows match from each side (Larson & Zhou / Zhang & Larson).
/// When match_count=0, aggregate columns from that side should be NULL.
static void RewriteLeftJoinMatchCount(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan) {
	// Find the LEFT/RIGHT/OUTER JOIN and extract null-supplying side bindings
	bool found = false;
	bool is_full_outer = false;
	ColumnBinding null_side_binding;
	LogicalType null_side_type;
	ColumnBinding left_side_binding;
	LogicalType left_side_type;

	std::function<void(LogicalOperator *)> find = [&](LogicalOperator *n) {
		if (found) {
			return;
		}
		if (n->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = n->Cast<LogicalComparisonJoin>();
			if ((join.join_type == JoinType::LEFT || join.join_type == JoinType::RIGHT ||
			     join.join_type == JoinType::OUTER) &&
			    !join.conditions.empty()) {
				found = true;
				is_full_outer = (join.join_type == JoinType::OUTER);
				// Null-supplying side (right for LEFT/OUTER, left for RIGHT)
				auto &null_side =
				    (join.join_type == JoinType::RIGHT) ? join.conditions[0].left : join.conditions[0].right;
				if (null_side->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					auto &ref = null_side->Cast<BoundColumnRefExpression>();
					null_side_binding = ref.binding;
					null_side_type = ref.return_type;
				}
				// For FULL OUTER: also extract the left-side key (both sides are null-supplying)
				if (is_full_outer) {
					auto &left_key = join.conditions[0].left;
					if (left_key->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
						auto &ref = left_key->Cast<BoundColumnRefExpression>();
						left_side_binding = ref.binding;
						left_side_type = ref.return_type;
					}
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

	// Only applies to aggregate plans (PROJECTION → AGGREGATE → ...).
	// For SIMPLE_PROJECTION outer JOINs, match count isn't needed (partial recompute via keys).
	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	LogicalOperator *agg_search = plan->children.empty() ? nullptr : plan->children[0].get();
	// Walk through possible intermediate projections to find the aggregate
	while (agg_search && agg_search->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		agg_search = agg_search->children.empty() ? nullptr : agg_search->children[0].get();
	}
	if (!agg_search || agg_search->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = agg_search->Cast<LogicalAggregate>();

	// Add COUNT(null_side_key) as _ivm_match_count (tracks right-side matches for LEFT/OUTER).
	auto count_func = BindAggregateByName(context, "count", {null_side_type});
	vector<unique_ptr<Expression>> count_args;
	count_args.push_back(make_uniq<BoundColumnRefExpression>(null_side_type, null_side_binding));
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = string(ivm::MATCH_COUNT_COL);
	idx_t match_count_idx = agg.expressions.size();
	agg.expressions.push_back(std::move(count_expr));

	// For FULL OUTER: add COUNT(left_key) as _ivm_right_match_count (tracks left-side matches).
	idx_t right_match_count_idx = 0;
	if (is_full_outer) {
		auto right_count_func = BindAggregateByName(context, "count", {left_side_type});
		vector<unique_ptr<Expression>> right_count_args;
		right_count_args.push_back(make_uniq<BoundColumnRefExpression>(left_side_type, left_side_binding));
		auto right_count_expr = make_uniq<BoundAggregateExpression>(
		    std::move(right_count_func), std::move(right_count_args), nullptr, nullptr, AggregateType::NON_DISTINCT);
		right_count_expr->alias = string(ivm::RIGHT_MATCH_COUNT_COL);
		right_match_count_idx = agg.expressions.size();
		agg.expressions.push_back(std::move(right_count_expr));
	}

	agg.ResolveOperatorTypes();

	// Add passthrough in the top projection
	auto &proj = plan->Cast<LogicalProjection>();
	auto agg_bindings = agg_search->GetColumnBindings();
	auto agg_types = agg_search->types;
	idx_t group_count = agg.groups.size();

	ColumnBinding match_binding = agg_bindings[group_count + match_count_idx];
	LogicalType match_type = agg_types[group_count + match_count_idx];
	auto match_pt = make_uniq<BoundColumnRefExpression>(match_type, match_binding);
	match_pt->alias = string(ivm::MATCH_COUNT_COL);
	proj.expressions.push_back(std::move(match_pt));

	if (is_full_outer) {
		ColumnBinding right_match_binding = agg_bindings[group_count + right_match_count_idx];
		LogicalType right_match_type = agg_types[group_count + right_match_count_idx];
		auto right_match_pt = make_uniq<BoundColumnRefExpression>(right_match_type, right_match_binding);
		right_match_pt->alias = string(ivm::RIGHT_MATCH_COUNT_COL);
		proj.expressions.push_back(std::move(right_match_pt));
	}

	proj.ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Added _ivm_match_count%s for outer join aggregate\n",
	                    is_full_outer ? " + _ivm_right_match_count" : "");
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
		RewriteDerivedAggregates(context, plan, opt);
	}
	RewriteLeftJoinKey(binder, plan);
	RewriteLeftJoinMatchCount(context, binder, plan);
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
