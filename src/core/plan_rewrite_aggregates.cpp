#include "core/plan_rewrite.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/plan_rewrite_internal.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <cctype>
#include <functional>
#include <map>

namespace duckdb {
AggregateFunction BindAggregateByName(ClientContext &context, const string &name,
                                      const vector<LogicalType> &arg_types) {
	auto &catalog = Catalog::GetSystemCatalog(context);
	auto &entry = catalog.GetEntry<AggregateFunctionCatalogEntry>(context, DEFAULT_SCHEMA, name);
	FunctionBinder binder(context);
	ErrorData error;
	auto best = binder.BindFunction(entry.name, entry.functions, arg_types, error);
	if (!best.IsValid()) {
		throw InternalException("PlanRewrite: failed to bind aggregate '%s'", name);
	}
	return entry.functions.GetFunctionByOffset(best.GetIndex());
}

unique_ptr<BoundAggregateExpression> MakeAggregate(ClientContext &context, const string &name,
                                                   unique_ptr<Expression> child, const string &alias) {
	auto function = BindAggregateByName(context, name, {child->return_type});
	vector<unique_ptr<Expression>> children;
	children.push_back(std::move(child));
	auto result = make_uniq<BoundAggregateExpression>(std::move(function), std::move(children), nullptr, nullptr,
	                                                  AggregateType::NON_DISTINCT);
	result->alias = alias;
	return result;
}

unique_ptr<BoundAggregateExpression> MakeCountStar(const string &alias) {
	auto result = make_uniq<BoundAggregateExpression>(CountStarFun::GetFunction(), vector<unique_ptr<Expression>>(),
	                                                  nullptr, nullptr, AggregateType::NON_DISTINCT);
	result->alias = alias;
	return result;
}

unique_ptr<LogicalAggregate> MakeGroupedAggregate(Binder &binder, vector<unique_ptr<Expression>> groups,
                                                  vector<unique_ptr<Expression>> aggregates,
                                                  unique_ptr<LogicalOperator> child) {
	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	auto result = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	GroupingSet grouping_set;
	for (auto &group : groups) {
		grouping_set.insert(result->groups.size());
		result->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(group->return_type)));
		result->groups.push_back(std::move(group));
	}
	result->grouping_sets.push_back(std::move(grouping_set));
	result->children.push_back(std::move(child));
	result->ResolveOperatorTypes();
	return result;
}

/// Keep [A-Za-z0-9_] and collapse other character runs into one '_' (no trailing '_'); `fallback` if nothing remains.
static string SanitizeAliasSuffix(const string &raw, const string &fallback) {
	string clean;
	bool last_under = false;
	for (unsigned char c : raw) {
		if (std::isalnum(c) || c == '_') {
			clean += static_cast<char>(c);
			last_under = (c == '_');
		} else if (!last_under && !clean.empty()) {
			clean += '_';
			last_under = true;
		}
	}
	if (!clean.empty() && clean.back() == '_') {
		clean.pop_back();
	}
	return clean.empty() ? fallback : clean;
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
static const char *SumSqPrefix(const string &func_name) {
	if (func_name == "stddev_pop") {
		return openivm::SUM_SQP_COL_PREFIX;
	}
	if (func_name == "var_pop") {
		return openivm::VAR_SQP_COL_PREFIX;
	}
	if (func_name == "variance" || func_name == "var_samp") {
		return openivm::VAR_SQ_COL_PREFIX;
	}
	return openivm::SUM_SQ_COL_PREFIX; // stddev, stddev_samp
}
void RewriteAggregateFilters(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		RewriteAggregateFilters(context, child);
	}
	if (plan->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = plan->Cast<LogicalAggregate>();
	bool any_filter = false;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE &&
		    expr->Cast<BoundAggregateExpression>().filter) {
			any_filter = true;
			break;
		}
	}
	if (!any_filter) {
		return;
	}
	bool rewritten = false;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound = expr->Cast<BoundAggregateExpression>();
		if (!bound.filter) {
			continue;
		}
		if (bound.function.name == "list") {
			continue;
		}
		auto filter_expr = std::move(bound.filter); // nulls bound.filter
		if (bound.children.empty()) {
			auto then_expr = make_uniq<BoundConstantExpression>(Value::BIGINT(1));
			auto else_expr = make_uniq<BoundConstantExpression>(Value(LogicalType::BIGINT));
			auto case_expr =
			    make_uniq<BoundCaseExpression>(std::move(filter_expr), std::move(then_expr), std::move(else_expr));
			expr = MakeAggregate(context, "count", std::move(case_expr), bound.alias);
			rewritten = true;
		} else {
			auto arg_type = bound.children[0]->return_type;
			auto else_expr = make_uniq<BoundConstantExpression>(Value(arg_type)); // NULL of same type
			bound.children[0] = make_uniq<BoundCaseExpression>(std::move(filter_expr), std::move(bound.children[0]),
			                                                   std::move(else_expr));
			rewritten = true;
		}
	}
	if (!rewritten) {
		return;
	}
	agg.ResolveOperatorTypes();
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Rewrote FILTER aggregates to CASE expressions\n");
}

LogicalOperator *FindProjectionAggregateInput(unique_ptr<LogicalOperator> &plan, bool allow_having_filter) {
	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.empty()) {
		return nullptr;
	}
	auto *input = plan->children[0].get();
	if (input->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return input;
	}
	if (allow_having_filter && input->type == LogicalOperatorType::LOGICAL_FILTER && !input->children.empty() &&
	    input->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return input->children[0].get();
	}
	return nullptr;
}

void InjectSumNonNullCounts(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}

	// Find the linear output path down to the aggregate. ORDER BY/LIMIT/TOP_N/DISTINCT
	// and MATERIALIZED_CTE are pass-through wrappers; projections may rename or
	// cast aggregate outputs and therefore need to be followed explicitly.
	auto spine = FindOutputSpine(plan.get(), [](const LogicalOperator &op) {
		return op.type == LogicalOperatorType::LOGICAL_ORDER_BY || op.type == LogicalOperatorType::LOGICAL_LIMIT ||
		       op.type == LogicalOperatorType::LOGICAL_TOP_N || op.type == LogicalOperatorType::LOGICAL_DISTINCT ||
		       op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE;
	});
	auto *agg_search = SpineAggregate(spine);
	auto &projections = spine.projections;
	if (!agg_search || projections.empty()) {
		return;
	}

	auto &agg = agg_search->Cast<LogicalAggregate>();
	auto &output_projection = *projections.front();
	const idx_t original_aggregate_count = agg.expressions.size();

	struct SumCountOutput {
		idx_t projection_index;
		idx_t aggregate_index;
	};
	vector<SumCountOutput> outputs;
	for (idx_t projection_index = 0; projection_index < output_projection.expressions.size(); projection_index++) {
		Expression *resolved = output_projection.expressions[projection_index].get();
		while (resolved) {
			while (resolved->expression_class == ExpressionClass::BOUND_CAST) {
				resolved = resolved->Cast<BoundCastExpression>().child.get();
			}
			if (resolved->type != ExpressionType::BOUND_COLUMN_REF) {
				break;
			}
			auto &ref = resolved->Cast<BoundColumnRefExpression>();
			if (ref.binding.table_index == agg.aggregate_index) {
				break;
			}
			LogicalProjection *referenced_projection = nullptr;
			for (idx_t projection_depth = 1; projection_depth < projections.size(); projection_depth++) {
				if (projections[projection_depth]->table_index == ref.binding.table_index) {
					referenced_projection = projections[projection_depth];
					break;
				}
			}
			if (!referenced_projection || ref.binding.column_index >= referenced_projection->expressions.size()) {
				resolved = nullptr;
				break;
			}
			resolved = referenced_projection->expressions[ref.binding.column_index].get();
		}
		if (!resolved || resolved->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &ref = resolved->Cast<BoundColumnRefExpression>();
		if (ref.binding.table_index != agg.aggregate_index || ref.binding.column_index >= original_aggregate_count) {
			continue;
		}
		auto &aggregate_expr = agg.expressions[ref.binding.column_index];
		if (aggregate_expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &sum = aggregate_expr->Cast<BoundAggregateExpression>();
		if (sum.function.name != "sum" || sum.IsDistinct() || sum.children.size() != 1) {
			continue;
		}
		if (sum.alias.find(string(openivm::SUM_COL_PREFIX)) == 0 ||
		    sum.alias.find(string(openivm::VAR_SQ_COL_PREFIX)) == 0 ||
		    sum.alias.find(string(openivm::VAR_SQP_COL_PREFIX)) == 0) {
			continue;
		}

		outputs.push_back({projection_index, agg.expressions.size()});
		agg.expressions.push_back(MakeAggregate(context, "count", sum.children[0]->Copy(),
		                                        string(openivm::SUM_COUNT_COL_PREFIX) + to_string(projection_index)));
	}
	if (outputs.empty()) {
		return;
	}

	agg.ResolveOperatorTypes();
	auto aggregate_bindings = agg_search->GetColumnBindings();
	auto aggregate_types = agg_search->types;
	for (auto &output : outputs) {
		idx_t output_index = agg.groups.size() + output.aggregate_index;
		auto alias = string(openivm::SUM_COUNT_COL_PREFIX) + to_string(output.projection_index);
		PropagateHiddenBindingThroughProjectionPath(projections, aggregate_bindings[output_index],
		                                            aggregate_types[output_index], alias);
	}
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Injected %zu SUM non-NULL counts\n", outputs.size());
}

void RewriteDerivedAggregates(ClientContext &context, unique_ptr<LogicalOperator> &plan, Optimizer &opt, bool is_top) {
	for (auto &child : plan->children) {
		RewriteDerivedAggregates(context, child, opt, false);
	}
	auto *agg_search = FindProjectionAggregateInput(plan, is_top);
	if (!agg_search) {
		return;
	}
	auto &agg = agg_search->Cast<LogicalAggregate>();
	bool has_derived = false;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
			auto &bound_agg = expr->Cast<BoundAggregateExpression>();
			auto &name = bound_agg.function.name;
			// DISTINCT aggregates are not decomposable into SUM/COUNT; they stay intact
			// and the checker routes the view to group-recompute.
			if ((name == "avg" || IsStddevOrVariance(name)) && !bound_agg.IsDistinct()) {
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
	std::map<idx_t, idx_t> nondecomposed_remap;

	for (idx_t i = 0; i < agg.expressions.size(); i++) {
		auto &expr = agg.expressions[i];
		if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
			nondecomposed_remap[i] = new_exprs.size();
			new_exprs.push_back(std::move(expr));
			continue;
		}
		auto &bound = expr->Cast<BoundAggregateExpression>();
		if (bound.children.empty()) {
			nondecomposed_remap[i] = new_exprs.size();
			new_exprs.push_back(std::move(expr));
			continue;
		}

		bool is_avg = (bound.function.name == "avg");
		bool is_stddev = IsStddevOrVariance(bound.function.name);
		if ((!is_avg && !is_stddev) || bound.IsDistinct()) {
			nondecomposed_remap[i] = new_exprs.size();
			new_exprs.push_back(std::move(expr));
			continue;
		}

		string alias = bound.alias.empty() ? (bound.function.name + "_" + bound.children[0]->GetName()) : bound.alias;
		alias = SanitizeAliasSuffix(alias, alias);

		Decomp d;
		d.kind = is_avg ? DecompKind::AVG : DecompKind::STDDEV;
		d.alias = alias;
		d.func_name = bound.function.name;
		d.old_idx = i;
		d.sum_idx = new_exprs.size();
		new_exprs.push_back(
		    MakeAggregate(context, "sum", bound.children[0]->Copy(), string(openivm::SUM_COL_PREFIX) + alias));
		if (is_stddev) {
			auto sq_arg = opt.BindScalarFunction("*", bound.children[0]->Copy(), bound.children[0]->Copy());
			d.sum_sq_idx = new_exprs.size();
			new_exprs.push_back(
			    MakeAggregate(context, "sum", std::move(sq_arg), string(SumSqPrefix(bound.function.name)) + alias));
		}
		d.count_idx = new_exprs.size();
		new_exprs.push_back(
		    MakeAggregate(context, "count", bound.children[0]->Copy(), string(openivm::COUNT_COL_PREFIX) + alias));

		decomps.push_back(std::move(d));
	}
	agg.expressions = std::move(new_exprs);
	agg.ResolveOperatorTypes();
	auto agg_bindings = agg_search->GetColumnBindings();
	auto agg_types = agg_search->types;
	const idx_t original_proj_size = proj.expressions.size();
	auto build_replacement = [&](const Decomp &d) -> unique_ptr<Expression> {
		ColumnBinding sum_binding = agg_bindings[group_count + d.sum_idx];
		ColumnBinding count_binding = agg_bindings[group_count + d.count_idx];
		LogicalType sum_type = agg_types[group_count + d.sum_idx];
		LogicalType count_type = agg_types[group_count + d.count_idx];
		if (d.kind == DecompKind::AVG) {
			auto sum_ref = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
			auto count_ref = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
			return opt.BindScalarFunction("/", std::move(sum_ref), std::move(count_ref));
		}
		ColumnBinding sum_sq_binding = agg_bindings[group_count + d.sum_sq_idx];
		LogicalType sum_sq_type = agg_types[group_count + d.sum_sq_idx];
		auto s1 = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
		auto s2 = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
		auto sq = make_uniq<BoundColumnRefExpression>(sum_sq_type, sum_sq_binding);
		auto n = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
		auto sum_sq_over_n = opt.BindScalarFunction("/", opt.BindScalarFunction("*", std::move(s1), std::move(s2)),
		                                            make_uniq<BoundColumnRefExpression>(count_type, count_binding));
		auto numerator = opt.BindScalarFunction("-", std::move(sq), std::move(sum_sq_over_n));
		unique_ptr<Expression> denom;
		if (IsPopulationVariant(d.func_name)) {
			denom = std::move(n);
		} else {
			denom = opt.BindScalarFunction("-", std::move(n), make_uniq<BoundConstantExpression>(Value::BIGINT(1)));
		}
		auto var_expr = opt.BindScalarFunction("/", std::move(numerator), std::move(denom));
		auto formula =
		    IsStddevVariant(d.func_name) ? opt.BindScalarFunction("sqrt", std::move(var_expr)) : std::move(var_expr);
		int64_t threshold = IsPopulationVariant(d.func_name) ? 0 : 1;
		auto count_check = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
		auto when_expr =
		    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(count_check),
		                                         make_uniq<BoundConstantExpression>(Value::BIGINT(threshold)));
		auto else_expr = make_uniq<BoundConstantExpression>(Value(formula->return_type));
		return make_uniq<BoundCaseExpression>(std::move(when_expr), std::move(formula), std::move(else_expr));
	};
	for (auto &d : decomps) {
		ColumnBinding old_binding(agg.aggregate_index, d.old_idx);
		d.user_alias = d.alias; // fallback if no direct ref
		for (idx_t pi = 0; pi < original_proj_size; pi++) {
			if (proj.expressions[pi]->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}
			auto &ref = proj.expressions[pi]->Cast<BoundColumnRefExpression>();
			if (ref.binding == old_binding) {
				d.user_alias = ref.alias.empty() ? d.alias : ref.alias;
				break;
			}
		}
	}
	column_binding_map_t<const Decomp *> old_binding_to_decomp;
	for (auto &d : decomps) {
		old_binding_to_decomp[ColumnBinding(agg.aggregate_index, d.old_idx)] = &d;
	}
	std::function<void(unique_ptr<Expression> &)> walk = [&](unique_ptr<Expression> &expr) {
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bcr = expr->Cast<BoundColumnRefExpression>();
			if (bcr.binding.table_index == agg.aggregate_index) {
				auto dit = old_binding_to_decomp.find(bcr.binding);
				if (dit != old_binding_to_decomp.end()) {
					auto replacement = build_replacement(*dit->second);
					replacement->alias = bcr.alias;
					expr = std::move(replacement);
					return; // do NOT descend into the replacement
				}
				auto rit = nondecomposed_remap.find(bcr.binding.column_index);
				if (rit != nondecomposed_remap.end() && rit->second != bcr.binding.column_index) {
					bcr.binding.column_index = rit->second;
				}
			}
		}
		ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) { walk(child); });
	};
	for (idx_t pi = 0; pi < original_proj_size; pi++) {
		walk(proj.expressions[pi]);
	}
	if (!plan->children.empty() && plan->children[0]->type == LogicalOperatorType::LOGICAL_FILTER) {
		auto &having = plan->children[0]->Cast<LogicalFilter>();
		for (auto &expression : having.expressions) {
			walk(expression);
		}
	}
	for (auto &d : decomps) {
		ColumnBinding sum_binding = agg_bindings[group_count + d.sum_idx];
		ColumnBinding count_binding = agg_bindings[group_count + d.count_idx];
		LogicalType sum_type = agg_types[group_count + d.sum_idx];
		LogicalType count_type = agg_types[group_count + d.count_idx];
		string col_suffix = SanitizeAliasSuffix(d.user_alias.empty() ? d.alias : d.user_alias, d.alias);
		auto sum_pt = make_uniq<BoundColumnRefExpression>(sum_type, sum_binding);
		sum_pt->alias = string(openivm::SUM_COL_PREFIX) + col_suffix;
		proj.expressions.push_back(std::move(sum_pt));

		if (d.kind == DecompKind::STDDEV) {
			ColumnBinding sum_sq_binding = agg_bindings[group_count + d.sum_sq_idx];
			LogicalType sum_sq_type = agg_types[group_count + d.sum_sq_idx];
			auto sum_sq_pt = make_uniq<BoundColumnRefExpression>(sum_sq_type, sum_sq_binding);
			sum_sq_pt->alias = string(SumSqPrefix(d.func_name)) + col_suffix;
			proj.expressions.push_back(std::move(sum_sq_pt));
		}

		auto count_pt = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
		count_pt->alias = string(openivm::COUNT_COL_PREFIX) + col_suffix;
		proj.expressions.push_back(std::move(count_pt));
	}
	proj.ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[PlanRewrite] Derived aggregates → hidden columns, %zu decompositions\n", decomps.size());
}

} // namespace duckdb
