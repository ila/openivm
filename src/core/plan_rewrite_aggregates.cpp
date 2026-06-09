#include "core/plan_rewrite.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/plan_rewrite_internal.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
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
			string saved_alias = std::move(bound.alias);
			auto then_expr = make_uniq<BoundConstantExpression>(Value::BIGINT(1));
			auto else_expr = make_uniq<BoundConstantExpression>(Value(LogicalType::BIGINT));
			auto case_expr =
			    make_uniq<BoundCaseExpression>(std::move(filter_expr), std::move(then_expr), std::move(else_expr));
			auto count_func = BindAggregateByName(context, "count", {LogicalType::BIGINT});
			vector<unique_ptr<Expression>> count_args;
			count_args.push_back(std::move(case_expr));
			auto new_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(count_args), nullptr,
			                                                    nullptr, AggregateType::NON_DISTINCT);
			new_expr->alias = std::move(saved_alias);
			expr = std::move(new_expr);
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
void RewriteDerivedAggregates(ClientContext &context, unique_ptr<LogicalOperator> &plan, Optimizer &opt, bool is_top) {
	// AVG/STDDEV decomposition adds hidden SUM/COUNT/SUM-OF-SQUARES columns to the aggregate it
	// rewrites. Inside a set-operation branch that asymmetrically widens one branch relative to its
	// siblings, producing an arity-mismatched UNION/EXCEPT/INTERSECT that is not valid SQL (LPTS then
	// cannot serialize it). Such views are maintained by recompute/full-refresh anyway, so the
	// decomposition serves no purpose here — stop at the set-op boundary and leave the branches intact.
	// Decomposition of aggregates *above* a set operation is unaffected (handled before we recurse in).
	if (plan->type == LogicalOperatorType::LOGICAL_UNION || plan->type == LogicalOperatorType::LOGICAL_EXCEPT ||
	    plan->type == LogicalOperatorType::LOGICAL_INTERSECT) {
		return;
	}
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
		if (!is_avg && !is_stddev) {
			nondecomposed_remap[i] = new_exprs.size();
			new_exprs.push_back(std::move(expr));
			continue;
		}

		string alias = bound.alias.empty() ? (bound.function.name + "_" + bound.children[0]->GetName()) : bound.alias;
		{
			string clean;
			bool last_under = false;
			for (unsigned char c : alias) {
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
			if (!clean.empty()) {
				alias = std::move(clean);
			}
		}
		auto arg_type = bound.children[0]->return_type;

		Decomp d;
		d.kind = is_avg ? DecompKind::AVG : DecompKind::STDDEV;
		d.alias = alias;
		d.func_name = bound.function.name;
		d.old_idx = i;
		auto sum_func = BindAggregateByName(context, "sum", {arg_type});
		vector<unique_ptr<Expression>> sum_args;
		sum_args.push_back(bound.children[0]->Copy());
		auto sum_expr = make_uniq<BoundAggregateExpression>(std::move(sum_func), std::move(sum_args), nullptr, nullptr,
		                                                    AggregateType::NON_DISTINCT);
		sum_expr->alias = string(openivm::SUM_COL_PREFIX) + alias;
		d.sum_idx = new_exprs.size();
		new_exprs.push_back(std::move(sum_expr));
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
		auto count_func = BindAggregateByName(context, "count", {arg_type});
		vector<unique_ptr<Expression>> count_args;
		count_args.push_back(bound.children[0]->Copy());
		auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(count_args), nullptr,
		                                                      nullptr, AggregateType::NON_DISTINCT);
		count_expr->alias = string(openivm::COUNT_COL_PREFIX) + alias;
		d.count_idx = new_exprs.size();
		new_exprs.push_back(std::move(count_expr));

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
	std::map<uint64_t, const Decomp *> old_binding_to_decomp;
	for (auto &d : decomps) {
		uint64_t key = (uint64_t)agg.aggregate_index ^ ((uint64_t)d.old_idx * 0x9e3779b97f4a7c15ULL);
		old_binding_to_decomp[key] = &d;
	}
	std::function<void(unique_ptr<Expression> &)> walk = [&](unique_ptr<Expression> &expr) {
		if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
			auto &bcr = expr->Cast<BoundColumnRefExpression>();
			if (bcr.binding.table_index == agg.aggregate_index) {
				uint64_t key =
				    (uint64_t)bcr.binding.table_index ^ ((uint64_t)bcr.binding.column_index * 0x9e3779b97f4a7c15ULL);
				auto dit = old_binding_to_decomp.find(key);
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
	for (auto &d : decomps) {
		ColumnBinding sum_binding = agg_bindings[group_count + d.sum_idx];
		ColumnBinding count_binding = agg_bindings[group_count + d.count_idx];
		LogicalType sum_type = agg_types[group_count + d.sum_idx];
		LogicalType count_type = agg_types[group_count + d.count_idx];
		string col_suffix;
		{
			const string &raw = d.user_alias.empty() ? d.alias : d.user_alias;
			bool last_under = false;
			for (unsigned char c : raw) {
				if (std::isalnum(c) || c == '_') {
					col_suffix += static_cast<char>(c);
					last_under = (c == '_');
				} else if (!last_under && !col_suffix.empty()) {
					col_suffix += '_';
					last_under = true;
				}
			}
			if (!col_suffix.empty() && col_suffix.back() == '_') {
				col_suffix.pop_back();
			}
			if (col_suffix.empty()) {
				col_suffix = d.alias; // fallback: already sanitized above
			}
		}
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
