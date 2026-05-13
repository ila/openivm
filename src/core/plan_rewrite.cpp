#include "core/plan_rewrite.hpp"

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
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "upsert/refresh_index_regen.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

#include <functional>
#include <set>

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

/// Inline every `LOGICAL_CTE_REF` in the plan with a fresh deep copy of its CTE
/// definition, then collapse `LOGICAL_MATERIALIZED_CTE` wrapper nodes. This makes
/// IVM see N independent leaves for an N-way self-join through a CTE, instead of
/// one materialized intermediate referenced N times — without it, `IvmJoinRule`'s
/// inclusion-exclusion can't generate the cross-terms (Δt ⋈ t_now and t_now ⋈ Δt)
/// needed to produce new pairs from a single base-table delta.
///
/// Inlining strategy: for each CTE_REF, deep-copy the definition subtree and
/// renumber its bindings to fresh table indices via `renumber_and_rebind_subtree`.
/// Wrap the renumbered subtree in a passthrough projection at the CTE_REF's
/// original `table_index` so parent operators' BCRs (which point to
/// `(ref.table_index, i)`) keep resolving correctly with no upstream rebinding.
///
/// We process MATERIALIZED_CTE nodes bottom-up: at each one, fully inline the
/// definition into the consumer (children[1]), then replace the MATERIALIZED_CTE
/// node with the modified consumer. Bottom-up traversal ensures that nested
/// CTEs inside the definition or consumer are resolved before this CTE is, so
/// no captured pointer ever dangles.
///
/// CTEInlining (already invoked in `parser.cpp`) usually handles this for
/// single-reference and small CTEs, but multi-reference CTEs that don't get
/// inlined by the optimizer end up here. Recursive CTEs are not supported and
/// are left untouched (caller catches the unsupported-operator error downstream).
static void InlineCteRefs(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	// Process this node's children first (bottom-up). Each child may itself be
	// a MATERIALIZED_CTE — handle each independently.
	for (auto &c : plan->children) {
		InlineCteRefs(context, binder, c);
	}
	if (plan->type != LogicalOperatorType::LOGICAL_MATERIALIZED_CTE || plan->children.size() != 2) {
		return;
	}
	auto &cte = plan->Cast<LogicalMaterializedCTE>();
	idx_t cte_index = cte.table_index;
	// Definition is now fully inlined (recursion above ran on cte.children[0] too).
	// Walk the consumer (cte.children[1]) and replace every CTE_REF with this
	// cte_index with a fresh deep-copied + renumbered + passthrough-projected copy
	// of the definition.
	std::function<void(unique_ptr<LogicalOperator> &)> visit = [&](unique_ptr<LogicalOperator> &node) {
		if (!node) {
			return;
		}
		if (node->type == LogicalOperatorType::LOGICAL_CTE_REF) {
			auto &ref = node->Cast<LogicalCTERef>();
			if (ref.cte_index != cte_index) {
				return; // belongs to another (outer) CTE — handle on its way up
			}
			auto deep_copy = plan->children[0]->Copy(context);
			auto renumbered = renumber_and_rebind_subtree(std::move(deep_copy), binder);
			// `types` only gets populated by ResolveOperatorTypes — call it once on the
			// renumbered subtree before reading types for the wrapper projection.
			renumbered.op->ResolveOperatorTypes();
			// Wrap in a passthrough projection at ref.table_index so parent BCRs
			// (ref.table_index, i) keep resolving with no upstream rebind.
			auto subtree_bindings = renumbered.op->GetColumnBindings();
			auto subtree_types = renumbered.op->types;
			vector<unique_ptr<Expression>> proj_exprs;
			idx_t cols = std::min(ref.chunk_types.size(), subtree_bindings.size());
			for (idx_t i = 0; i < cols; i++) {
				auto col_ref = make_uniq<BoundColumnRefExpression>(subtree_types[i], subtree_bindings[i]);
				if (i < ref.bound_columns.size() && !ref.bound_columns[i].empty()) {
					col_ref->alias = ref.bound_columns[i];
				}
				proj_exprs.push_back(std::move(col_ref));
			}
			auto wrapper = make_uniq<LogicalProjection>(ref.table_index, std::move(proj_exprs));
			wrapper->children.push_back(std::move(renumbered.op));
			wrapper->ResolveOperatorTypes();
			OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Inlined CTE_REF cte_index=%lu (table_index=%lu, %zu cols)\n",
			                    (unsigned long)ref.cte_index, (unsigned long)ref.table_index, (size_t)cols);
			node = std::move(wrapper);
			return;
		}
		for (auto &c : node->children) {
			visit(c);
		}
	};
	visit(plan->children[1]);
	// Replace this MATERIALIZED_CTE node with its (now fully inlined) consumer.
	plan = std::move(plan->children[1]);
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Collapsed MATERIALIZED_CTE wrapper (cte_index=%lu)\n",
	                    (unsigned long)cte_index);
}

/// @public — also called from parser.cpp on the full CREATE plan before AnalyzePlan.
/// Normalize AGG(x) FILTER (WHERE p) → AGG(CASE WHEN p THEN x END) before the
/// compatibility checker sees it. This is correct for NULL-ignoring aggregates and
/// lets AVG/STDDEV FILTER queries decompose before maintenance compilation.
/// LIST is intentionally skipped because it preserves NULL elements.
///   COUNT(*) FILTER (WHERE p) → COUNT(CASE WHEN p THEN 1 END)  (count_star → count)
///   AGG(x)   FILTER (WHERE p) → AGG(CASE WHEN p THEN x END)    (in-place child wrap)
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
			// COUNT(*) FILTER (WHERE p) → COUNT(CASE WHEN p THEN 1 END)
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
			// AGG(x) FILTER (WHERE p) → AGG(CASE WHEN p THEN x END)
			auto arg_type = bound.children[0]->return_type;
			auto else_expr = make_uniq<BoundConstantExpression>(Value(arg_type)); // NULL of same type
			bound.children[0] = make_uniq<BoundCaseExpression>(std::move(filter_expr), std::move(bound.children[0]),
			                                                   std::move(else_expr));
			// bound.filter already nullptr from the move above
			rewritten = true;
		}
	}
	if (!rewritten) {
		return;
	}
	agg.ResolveOperatorTypes();
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Rewrote FILTER aggregates to CASE expressions\n");
}

static LogicalOperator *FindProjectionAggregateInput(unique_ptr<LogicalOperator> &plan, bool allow_having_filter) {
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

/// Decompose AVG and STDDEV/VARIANCE aggregates into incrementalizable components.
/// Handles both in a SINGLE PASS to keep aggregate expression indices consistent.
/// - AVG(x)      → SUM(x), COUNT(x) + SUM/COUNT ratio in projection
/// - STDDEV(x)   → SUM(x), SUM(x*x), COUNT(x) + variance formula in projection
/// - VARIANCE(x) → same, without sqrt
static void RewriteDerivedAggregates(ClientContext &context, unique_ptr<LogicalOperator> &plan, Optimizer &opt,
                                     bool is_top = true) {
	// Only recurse when we're NOT the top-level call: the top call's own walk below
	// handles the PROJECTION → [FILTER] → AGGREGATE pattern. Recursing first and
	// letting inner PROJECTIONs self-decompose breaks LIMIT/ORDER-wrapped HAVING
	// queries: the inner PROJECTION → FILTER → AGG matches the pattern but the
	// HAVING SQL extraction later finds a BCR whose meaning has shifted after
	// decomposition, producing invalid `avg(col)` predicates against a column-less
	// data table (see benchmark query_0431).
	for (auto &child : plan->children) {
		RewriteDerivedAggregates(context, child, opt, false);
	}

	// Walk down through at most one LOGICAL_FILTER (HAVING) to reach the aggregate.
	// `SELECT ... GROUP BY ... HAVING AVG(x) > 0` sits PROJECTION → FILTER → AGGREGATE,
	// and FILTER is pass-through (no column rebind), so `proj.expressions`' BCRs still
	// point at the aggregate's bindings — the substitution below works unchanged. This
	// walk only fires at the top level; HAVING sitting below a LIMIT/ORDER wrapper is
	// not safe to decompose because the extracted HAVING SQL won't be rewritten.
	//
	// We also do NOT walk through intermediate LOGICAL_PROJECTION nodes (e.g. those
	// from CTE expansion): they REBIND outputs to fresh column indices, so the top
	// projection's BCRs reference the middle projection's bindings, not the agg's.
	// CTE queries are handled upstream via CTE inlining instead.
	auto *agg_search = FindProjectionAggregateInput(plan, is_top);
	if (!agg_search) {
		return;
	}
	auto &agg = agg_search->Cast<LogicalAggregate>();

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
	// Track the new position of each non-decomposed aggregate so we can remap
	// projection BCRs that still point to the ORIGINAL position. A bare COUNT(*)
	// at original index 4 can move to new index 12 after four STDDEV/VARIANCE
	// decompositions each insert 3 hidden columns ahead of it.
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
		// Sanitize alias for use as a SQL identifier. DuckDB auto-sets alias to the expression
		// string (e.g. "avg(C_BALANCE)") for unaliased aggregates. Parens and other non-identifier
		// characters break column names like _ivm_sum_avg(C_BALANCE) in SELECT * EXCLUDE (...).
		// Use same algorithm as parser.cpp output_names sanitization so that helper column
		// suffixes (e.g. "avg_C_BALANCE") match the sanitized visible column name.
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

	// Step 2: Update projection — replace derived column refs with formulas.
	// Use agg_search (the aggregate we walked down to), not plan->children[0], because
	// HAVING/CTE queries interpose a FILTER/PROJECTION layer between plan and the agg.
	auto agg_bindings = agg_search->GetColumnBindings();
	auto agg_types = agg_search->types;
	const idx_t original_proj_size = proj.expressions.size();

	// Build a per-decomp replacement factory. We need a builder (not a one-shot
	// expression) because each match must produce a fresh copy — expressions are
	// unique_ptr-owned and can't be duplicated implicitly.
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

	// Preserve direct-BCR user aliases before any substitution.
	// `SELECT AVG(x) AS avg_bal FROM ...` has proj.expressions[i] = BCR with alias
	// "avg_bal". We capture that alias and re-attach it to the formula after
	// substitution so the MV column keeps the user's name.
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

	// Single-pass substitution across ALL decomps.
	//
	// We walk each projection expression once. For each BCR, check whether it
	// matches ANY decomp's old_binding and, if so, replace with that decomp's
	// formula and RETURN (don't descend into the replacement). The return is
	// critical: after decomposition, the first decomp's formula contains BCRs
	// at the NEW column positions (e.g. AVG → SUM/COUNT at agg[0]/agg[1]).
	// Position 1 may happen to be another decomp's old_idx (e.g. STDDEV was
	// originally at agg[1]). If we descended into the replacement, we'd
	// mis-substitute the internal COUNT_AVG BCR with the STDDEV formula.
	//
	// Looping across decomps separately (the previous design) couldn't avoid
	// this because later decomps walked the projection tree that earlier
	// decomps had already rewritten. Doing a single pass with a decomp lookup
	// at each BCR site guarantees each BCR is visited exactly once.
	std::map<uint64_t, const Decomp *> old_binding_to_decomp;
	for (auto &d : decomps) {
		uint64_t key = (uint64_t)agg.aggregate_index ^ ((uint64_t)d.old_idx * 0x9e3779b97f4a7c15ULL);
		old_binding_to_decomp[key] = &d;
	}
	// Combined single-pass walk: for each BCR in the ORIGINAL projection tree,
	//   1. If (agg_index, column_index) matches a decomp's old_binding, replace
	//      with that decomp's formula and RETURN (don't descend into the fresh
	//      formula — its internal BCRs already use NEW column positions).
	//   2. Else if the column_index is in nondecomposed_remap, rewrite it to the
	//      post-decomposition position (e.g. MAX moved from old 3 → new 4).
	//   3. Else leave alone.
	// The RETURN on substitute is what prevents a remapped non-decomp position
	// (e.g. new 4 for MAX) from later matching a decomp's old-idx (e.g. STDDEV's
	// old idx 4) — we never visit the same BCR twice, and BCRs we insert are
	// skipped by the return.
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

	// Add hidden columns as passthroughs. Use user_alias so that
	// DetectDerivedAggColumns in the upsert compiler can match them.
	// Sanitize with same algorithm as parser.cpp output_names loop so that the
	// key derived by DetectDerivedAggColumns matches the sanitized visible column name.
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
				col_suffix = d.alias; // fallback: already sanitized at line 188
			}
		}
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

/// Inject a hidden COUNT(*) (alias `_ivm_count_star`) into AGGREGATE_GROUP
/// aggregates that don't already have a reliable total-row-count aggregate.
///
/// Why: the post-MERGE cleanup in CompileAggregateGroups needs a per-group
/// cardinality column to delete rows whose group has dropped to zero tuples.
/// Views with only SUM/MIN/MAX have no such column; views with only FILTERED
/// counts (COUNT(*) FILTER (WHERE p) → COUNT(CASE WHEN p THEN 1 END)) also
/// lack a reliable indicator — filtered counts reach 0 when no rows match the
/// predicate, even though the group still exists. We can't use SUM=0 either
/// (CASE expressions can legitimately yield 0).
///
/// The column is prefixed `_ivm_` so `column_hider` auto-excludes it from
/// the user-facing VIEW; `CompileAggregateGroups` already recognizes it
/// via ivm::COUNT_STAR_COL.
static void InjectGroupCountStar(unique_ptr<LogicalOperator> &plan) {
	// Only inject at the top of the plan — the AGGREGATE_GROUP compile path only
	// runs when the MV root is PROJECTION → [FILTER] → AGGREGATE. Inner aggregates
	// under a UNION/INTERSECT/EXCEPT or subquery are handled by different compile
	// paths (often FULL_REFRESH) that would be broken by extra columns.
	auto *agg_search = FindProjectionAggregateInput(plan, true);
	if (!agg_search) {
		return;
	}
	auto &agg = agg_search->Cast<LogicalAggregate>();
	// Only inject for GROUP BY (SIMPLE_AGGREGATE has a different compile path).
	if (agg.groups.empty()) {
		return;
	}
	// Skip if a reliable group-size count is already present:
	//   - A true COUNT(*): function name "count_star", no children, no filter.
	//     COUNT(col) / COUNT(CASE WHEN p THEN x END) have function name "count"
	//     (with children) and are unreliable: they return 0 when no rows match
	//     the condition even though the group is non-empty. Do NOT skip for those.
	//   - _ivm_count_star or _ivm_distinct_count already injected by an earlier
	//     pass (e.g. DISTINCT rewrite).
	bool has_argminmax = false;
	for (auto &expr : agg.expressions) {
		if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound = expr->Cast<BoundAggregateExpression>();
		// True COUNT(*): no-arg, no filter — always equals the group cardinality.
		if (bound.function.name == "count_star" && bound.children.empty() && !bound.filter) {
			return;
		}
		// Already-injected reliable hidden count.
		if (bound.alias == ivm::COUNT_STAR_COL || bound.alias == ivm::DISTINCT_COUNT_COL) {
			return;
		}
		if (bound.function.name == "arg_min" || bound.function.name == "arg_max") {
			has_argminmax = true;
		}
	}
	// ARG_MIN/ARG_MAX always use group-recompute (LPTS can't round-trip the two-arg form,
	// so view_query_sql is the original SQL without _ivm_count_star). Skip injection so
	// the data table schema matches. Checked after the loop so count_star / distinct_count
	// in the same view still short-circuit correctly regardless of expression order.
	if (has_argminmax) {
		return;
	}
	auto count_star_func = CountStarFun::GetFunction();
	vector<unique_ptr<Expression>> count_args;
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_star_func), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = ivm::COUNT_STAR_COL;
	auto count_type = count_expr->return_type;
	idx_t new_agg_idx = agg.expressions.size();
	agg.expressions.push_back(std::move(count_expr));
	agg.ResolveOperatorTypes();

	ColumnBinding count_binding(agg.aggregate_index, new_agg_idx);
	auto count_pt = make_uniq<BoundColumnRefExpression>(count_type, count_binding);
	count_pt->alias = ivm::COUNT_STAR_COL;
	auto &proj = plan->Cast<LogicalProjection>();
	proj.expressions.push_back(std::move(count_pt));
	proj.ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Injected _ivm_count_star for AGGREGATE_GROUP\n");
}

/// Returns true if `alias` is one of the reserved IVM-hidden-column prefixes
/// added by DecomposeAvgStddev. Used to decide whether to propagate a child
/// projection's expression up through a pass-through parent projection.
static bool IsHiddenAggregateAlias(const string &alias) {
	return alias.find(ivm::SUM_COL_PREFIX) == 0 || alias.find(ivm::COUNT_COL_PREFIX) == 0 ||
	       alias.find(ivm::SUM_SQ_COL_PREFIX) == 0 || alias.find(ivm::SUM_SQP_COL_PREFIX) == 0;
}

/// Propagate hidden aggregate columns (_ivm_sum_*, _ivm_count_*, …) added by
/// DecomposeAvgStddev up through any chain of pass-through PROJECTIONs that
/// sits between the decomposed projection and the plan root.
///
/// Why: CTE inlining + projection stacking can leave a plan like
///   PROJECTION (user SELECT: ROUND(avg_bal, 2), cnt, …)   <- top
///     PROJECTION (pass-through BCRs)                       <- middle
///       PROJECTION (CTE body, holds _ivm_sum_* from decomp) <- inner
///         AGGREGATE
/// Without propagation, middle and top strip the hidden columns, so the MV
/// data table stores only the final AVG and the MERGE computes `v.avg + d.avg`
/// — wrong for non-summable aggregates. Propagation lets CompileAggregateGroups
/// see the hidden SUM/COUNT columns and maintain them separately.
static void PropagateHiddenAggregateColumns(unique_ptr<LogicalOperator> &plan) {
	for (auto &child : plan->children) {
		PropagateHiddenAggregateColumns(child);
	}
	if (plan->type != LogicalOperatorType::LOGICAL_PROJECTION || plan->children.empty() ||
	    plan->children[0]->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return;
	}
	auto &proj = plan->Cast<LogicalProjection>();
	auto &child_proj = plan->children[0]->Cast<LogicalProjection>();

	std::set<string> already_present;
	for (auto &expr : proj.expressions) {
		if (IsHiddenAggregateAlias(expr->alias)) {
			already_present.insert(expr->alias);
		}
	}

	plan->children[0]->ResolveOperatorTypes();
	auto child_types = plan->children[0]->types;
	auto child_bindings = plan->children[0]->GetColumnBindings();

	bool added = false;
	for (idx_t i = 0; i < child_proj.expressions.size(); i++) {
		const string &child_alias = child_proj.expressions[i]->alias;
		if (!IsHiddenAggregateAlias(child_alias) || already_present.count(child_alias)) {
			continue;
		}
		if (i >= child_bindings.size() || i >= child_types.size()) {
			continue;
		}
		auto bcr = make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]);
		bcr->alias = child_alias;
		proj.expressions.push_back(std::move(bcr));
		added = true;
		OPENIVM_DEBUG_PRINT("[PropagateHidden] Added '%s' to parent projection (table_index=%llu)\n",
		                    child_alias.c_str(), (unsigned long long)proj.table_index);
	}
	if (added) {
		proj.ResolveOperatorTypes();
	}
}

struct OuterJoinBindings {
	bool found = false;
	bool is_full_outer = false;
	ColumnBinding preserved_key_binding;
	LogicalType preserved_key_type;
	ColumnBinding right_key_binding;
	LogicalType right_key_type;
	ColumnBinding null_side_binding;
	LogicalType null_side_type;
	ColumnBinding left_side_binding;
	LogicalType left_side_type;
};

static bool IsOuterJoin(JoinType join_type) {
	return join_type == JoinType::LEFT || join_type == JoinType::RIGHT || join_type == JoinType::OUTER;
}

static void ReadColumnRefBinding(const unique_ptr<Expression> &expr, ColumnBinding &binding, LogicalType &type) {
	if (expr->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return;
	}
	auto &ref = expr->Cast<BoundColumnRefExpression>();
	binding = ref.binding;
	type = ref.return_type;
}

static OuterJoinBindings FindFirstOuterJoinBindings(LogicalOperator *plan) {
	OuterJoinBindings bindings;

	std::function<bool(LogicalOperator *)> find = [&](LogicalOperator *node) {
		if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = node->Cast<LogicalComparisonJoin>();
			if (IsOuterJoin(join.join_type) && !join.conditions.empty()) {
				auto &condition = join.conditions[0];
				bindings.found = true;
				bindings.is_full_outer = join.join_type == JoinType::OUTER;

				auto &preserved_key = join.join_type == JoinType::RIGHT ? condition.right : condition.left;
				ReadColumnRefBinding(preserved_key, bindings.preserved_key_binding, bindings.preserved_key_type);
				ReadColumnRefBinding(condition.right, bindings.right_key_binding, bindings.right_key_type);

				auto &null_side = join.join_type == JoinType::RIGHT ? condition.left : condition.right;
				ReadColumnRefBinding(null_side, bindings.null_side_binding, bindings.null_side_type);
				ReadColumnRefBinding(condition.left, bindings.left_side_binding, bindings.left_side_type);
				return true;
			}
		}
		for (auto &child : node->children) {
			if (find(child.get())) {
				return true;
			}
		}
		return false;
	};
	find(plan);
	return bindings;
}

static bool PlanContainsOperator(LogicalOperator *plan, LogicalOperatorType type) {
	if (plan->type == type) {
		return true;
	}
	for (auto &child : plan->children) {
		if (PlanContainsOperator(child.get(), type)) {
			return true;
		}
	}
	return false;
}

static void PropagateBindingThroughOperatorPath(unique_ptr<LogicalOperator> &plan, ColumnBinding &binding,
                                                LogicalType &type) {
	plan->ResolveOperatorTypes();
	auto top_bindings = plan->GetColumnBindings();
	auto top_types = plan->types;
	for (idx_t i = 0; i < top_bindings.size(); i++) {
		if (top_bindings[i] == binding) {
			type = top_types[i];
			return;
		}
	}

	struct PathEntry {
		LogicalOperator *op;
		idx_t child_idx;
	};
	vector<PathEntry> path;
	std::function<bool(LogicalOperator *, bool)> find_path = [&](LogicalOperator *node, bool is_root) -> bool {
		auto bindings = node->GetColumnBindings();
		for (auto &candidate : bindings) {
			if (candidate == binding) {
				return true;
			}
		}
		for (idx_t child_idx = 0; child_idx < node->children.size(); child_idx++) {
			if (find_path(node->children[child_idx].get(), false)) {
				if (!is_root) {
					path.push_back({node, child_idx});
				}
				return true;
			}
		}
		return false;
	};
	find_path(plan.get(), false);

	ColumnBinding current = binding;
	for (auto &entry : path) {
		if (entry.op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = entry.op->Cast<LogicalProjection>();
			bool found = false;
			for (idx_t i = 0; i < projection.expressions.size(); i++) {
				if (projection.expressions[i]->type != ExpressionType::BOUND_COLUMN_REF) {
					continue;
				}
				auto &ref = projection.expressions[i]->Cast<BoundColumnRefExpression>();
				if (ref.binding == current) {
					current = ColumnBinding(projection.table_index, i);
					found = true;
					break;
				}
			}
			if (!found) {
				auto col_ref = make_uniq<BoundColumnRefExpression>(type, current);
				projection.expressions.push_back(std::move(col_ref));
				current = ColumnBinding(projection.table_index, projection.expressions.size() - 1);
			}
			projection.ResolveOperatorTypes();
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
		// JOINs and other transparent nodes already pass child bindings through.
	}
	binding = current;
}

/// Add _ivm_left_key (and _ivm_right_key for FULL OUTER) projection at the top of the plan.
static void RewriteLeftJoinKey(Binder &binder, unique_ptr<LogicalOperator> &plan, const OuterJoinBindings &outer_join) {
	bool is_full_outer = outer_join.is_full_outer;
	auto key_binding = outer_join.preserved_key_binding;
	auto key_type = outer_join.preserved_key_type;
	auto right_key_binding = outer_join.right_key_binding;
	auto right_key_type = outer_join.right_key_type;

	// Outer-join key bindings originate inside the join subtree. User projections,
	// filters with projection maps, and CTE/projection stacks can hide either key
	// before the top of the plan. Push both keys through the operator path first,
	// then build one final projection that exposes user columns plus hidden keys.
	PropagateBindingThroughOperatorPath(plan, key_binding, key_type);
	if (is_full_outer) {
		PropagateBindingThroughOperatorPath(plan, right_key_binding, right_key_type);
	}

	auto top_bindings = plan->GetColumnBindings();
	auto top_types = plan->types;

	vector<unique_ptr<Expression>> proj_exprs;
	for (idx_t i = 0; i < top_bindings.size(); i++) {
		proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(top_types[i], top_bindings[i]));
	}

	// Always add _ivm_left_key as a separate extra column.
	proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(ivm::LEFT_KEY_COL, key_type, key_binding));

	// For FULL OUTER: also add _ivm_right_key in the same projection.
	if (is_full_outer) {
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
static void RewriteLeftJoinMatchCount(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                                      const OuterJoinBindings &outer_join) {
	bool is_full_outer = outer_join.is_full_outer;
	auto null_side_binding = outer_join.null_side_binding;
	auto null_side_type = outer_join.null_side_type;
	auto left_side_binding = outer_join.left_side_binding;
	auto left_side_type = outer_join.left_side_type;

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

static void RewriteOuterJoinSupport(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan) {
	auto outer_join = FindFirstOuterJoinBindings(plan.get());
	if (!outer_join.found) {
		return;
	}

	if (PlanContainsOperator(plan.get(), LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY)) {
		RewriteLeftJoinMatchCount(context, binder, plan, outer_join);
		return;
	}

	RewriteLeftJoinKey(binder, plan, outer_join);
}

struct PlanRewriteContext {
	ClientContext &context;
	Binder &binder;
	unique_ptr<LogicalOperator> &plan;
	vector<string> &planner_names;
};

using PlanRewritePass = void (*)(PlanRewriteContext &);

struct PlanRewritePassEntry {
	const char *name;
	PlanRewritePass rewrite;
};

static bool HasTopLevelDistinct(const unique_ptr<LogicalOperator> &plan) {
	return plan->type == LogicalOperatorType::LOGICAL_DISTINCT ||
	       (plan->type == LogicalOperatorType::LOGICAL_PROJECTION && !plan->children.empty() &&
	        plan->children[0]->type == LogicalOperatorType::LOGICAL_DISTINCT);
}

static void RunRewritePass(const PlanRewritePassEntry &pass, PlanRewriteContext &rewrite_context) {
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Pass start: %s\n", pass.name);
	pass.rewrite(rewrite_context);
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Pass done: %s\n", pass.name);
}

static void RewritePassInlineCteRefs(PlanRewriteContext &rewrite_context) {
	InlineCteRefs(rewrite_context.context, rewrite_context.binder, rewrite_context.plan);
}

static void RewritePassAggregateFilters(PlanRewriteContext &rewrite_context) {
	RewriteAggregateFilters(rewrite_context.context, rewrite_context.plan);
}

static void RewritePassDistinct(PlanRewriteContext &rewrite_context) {
	bool had_distinct = HasTopLevelDistinct(rewrite_context.plan);
	RewriteDistinct(rewrite_context.context, rewrite_context.binder, rewrite_context.plan);
	if (had_distinct) {
		rewrite_context.planner_names.push_back(ivm::DISTINCT_COUNT_COL);
	}
}

static void RewritePassDerivedAggregates(PlanRewriteContext &rewrite_context) {
	Optimizer opt(rewrite_context.binder, rewrite_context.context);
	RewriteDerivedAggregates(rewrite_context.context, rewrite_context.plan, opt);
}

static void RewritePassGroupCountStar(PlanRewriteContext &rewrite_context) {
	InjectGroupCountStar(rewrite_context.plan);
}

static void RewritePassHiddenAggregatePropagation(PlanRewriteContext &rewrite_context) {
	PropagateHiddenAggregateColumns(rewrite_context.plan);
}

static void RewritePassOuterJoinSupport(PlanRewriteContext &rewrite_context) {
	RewriteOuterJoinSupport(rewrite_context.context, rewrite_context.binder, rewrite_context.plan);
}

static void RunRewritePipeline(PlanRewriteContext &rewrite_context) {
	const PlanRewritePassEntry passes[] = {
	    {"inline_cte_refs", RewritePassInlineCteRefs},
	    {"aggregate_filters", RewritePassAggregateFilters},
	    {"distinct", RewritePassDistinct},
	    {"derived_aggregates", RewritePassDerivedAggregates},
	    {"group_count_star", RewritePassGroupCountStar},
	    {"hidden_aggregate_propagation", RewritePassHiddenAggregatePropagation},
	    {"outer_join_support", RewritePassOuterJoinSupport},
	};

	for (const auto &pass : passes) {
		RunRewritePass(pass, rewrite_context);
	}
}

void IVMPlanRewrite(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                    vector<string> &planner_names) {
	OPENIVM_DEBUG_PRINT("[IVMPlanRewrite] Starting\n");
	PlanRewriteContext rewrite_context {context, binder, plan, planner_names};
	RunRewritePipeline(rewrite_context);
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
	case ExpressionClass::BOUND_OPERATOR: {
		// Covers IS NULL / IS NOT NULL / IN-list / NOT etc. Recurse on children so
		// aggregate references inside still get rewritten to their output alias.
		auto &op = expr.Cast<BoundOperatorExpression>();
		string suffix;
		if (expr.type == ExpressionType::OPERATOR_IS_NULL) {
			suffix = " IS NULL";
		} else if (expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
			suffix = " IS NOT NULL";
		}
		if (!suffix.empty() && op.children.size() == 1) {
			return "(" + HavingExprToSQL(*op.children[0], binding_to_alias) + ")" + suffix;
		}
		if (expr.type == ExpressionType::OPERATOR_NOT && op.children.size() == 1) {
			return "(NOT (" + HavingExprToSQL(*op.children[0], binding_to_alias) + "))";
		}
		return expr.ToString();
	}
	case ExpressionClass::BOUND_AGGREGATE: {
		// HAVING can reference aggregate results directly (e.g. `HAVING AVG(x) > 100`).
		// If the aggregate's binding isn't in the output-alias map, fall back to its
		// printed form — but that references base columns the data table doesn't have.
		// Best effort: check if any child is a plain BCR registered in the map.
		auto &agg = expr.Cast<BoundAggregateExpression>();
		if (agg.children.size() == 1 && agg.children[0]->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
			auto &col = agg.children[0]->Cast<BoundColumnRefExpression>();
			uint64_t key =
			    (uint64_t)col.binding.table_index ^ ((uint64_t)col.binding.column_index * 0x9e3779b97f4a7c15ULL);
			auto it = binding_to_alias.find(key);
			if (it != binding_to_alias.end()) {
				return it->second;
			}
		}
		return expr.ToString();
	}
	default:
		return expr.ToString();
	}
}

/// Collect every BOUND_COLUMN_REF binding referenced by the expression tree.
static void CollectFilterBindings(Expression &expr, std::set<pair<idx_t, idx_t>> &out) {
	if (expr.expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col = expr.Cast<BoundColumnRefExpression>();
		out.insert({col.binding.table_index, col.binding.column_index});
		return;
	}
	ExpressionIterator::EnumerateChildren(expr, [&](Expression &child) { CollectFilterBindings(child, out); });
}

string StripHavingFilter(unique_ptr<LogicalOperator> &plan, vector<string> &output_names) {
	// Find PROJECTION → FILTER → AGGREGATE pattern. Only descend through transparent
	// operators (PROJECTION, FILTER, ORDER, LIMIT, DISTINCT) and — for MATERIALIZED_CTE —
	// the outer query (children[1]) only. Stripping a FILTER that's nested inside a
	// MATERIALIZED_CTE body, a UNION branch, or below a JOIN would extract a predicate
	// whose bindings and output_names don't line up with the view's top-level columns
	// (e.g. the CTE's internal SUM output would be aliased to the outer's I_NAME column,
	// producing a view WHERE clause like `(I_NAME > 100)` on a VARCHAR).
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
		// Materialized CTE: only descend into the outer query (children[1]); a FILTER
		// in the CTE body is the CTE's own HAVING and must stay where it is.
		if (node->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			if (node->children.size() >= 2) {
				return find_filter(node->children[1].get(), node);
			}
			return false;
		}
		// Only descend through transparent operators. Stop at JOIN, UNION, GET, AGGREGATE, etc.
		// AGGREGATE is intentionally excluded: HAVING is always a FILTER *above* its AGGREGATE,
		// never inside it. Descending into AGGREGATE children would find a nested HAVING from
		// an inner subquery or a DISTINCT-rewrite-introduced outer AGGREGATE, and erroneously
		// expose _ivm_having_N columns that are invisible in the outer output.
		if (node->type != LogicalOperatorType::LOGICAL_PROJECTION &&
		    node->type != LogicalOperatorType::LOGICAL_FILTER && node->type != LogicalOperatorType::LOGICAL_ORDER_BY &&
		    node->type != LogicalOperatorType::LOGICAL_LIMIT && node->type != LogicalOperatorType::LOGICAL_DISTINCT) {
			return false;
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
	LogicalProjection *proj_ptr = nullptr;
	if (parent && parent->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		proj_ptr = &parent->Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj_ptr->expressions.size() && i < output_names.size(); i++) {
			if (proj_ptr->expressions[i]->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
				auto &col = proj_ptr->expressions[i]->Cast<BoundColumnRefExpression>();
				uint64_t key =
				    (uint64_t)col.binding.table_index ^ ((uint64_t)col.binding.column_index * 0x9e3779b97f4a7c15ULL);
				binding_to_alias[key] = output_names[i];
			}
		}
	}

	// Expose aggregate outputs referenced by the HAVING predicate but missing from
	// the SELECT list (e.g. HAVING COUNT(*) > N when COUNT(*) isn't in SELECT, or
	// HAVING SUM(COALESCE(x, 0)) > ... when only SUM(x) is selected). Without this
	// the predicate SQL falls back to the raw aggregate text, which re-references
	// base-table columns that aren't in the data table.
	auto &filter = filter_node->Cast<LogicalFilter>();
	if (proj_ptr && !filter_node->children.empty() &&
	    filter_node->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg_child = filter_node->children[0]->Cast<LogicalAggregate>();
		std::set<pair<idx_t, idx_t>> filter_bindings;
		for (auto &expr : filter.expressions) {
			CollectFilterBindings(*expr, filter_bindings);
		}
		idx_t next_hidden = 0;
		for (auto &b : filter_bindings) {
			uint64_t key = (uint64_t)b.first ^ ((uint64_t)b.second * 0x9e3779b97f4a7c15ULL);
			if (binding_to_alias.find(key) != binding_to_alias.end()) {
				continue;
			}
			// Only expose aggregate-output bindings. Group bindings should already be in
			// the projection; a raw column ref here would indicate an unexpected plan.
			if (b.first != agg_child.aggregate_index) {
				continue;
			}
			if (b.second >= agg_child.expressions.size()) {
				continue;
			}
			string hidden_name = "_ivm_having_" + std::to_string(next_hidden++);
			auto col_type = agg_child.expressions[b.second]->return_type;
			auto hidden_expr =
			    make_uniq<BoundColumnRefExpression>(hidden_name, col_type, ColumnBinding(b.first, b.second));
			hidden_expr->alias = hidden_name;
			proj_ptr->expressions.push_back(std::move(hidden_expr));
			output_names.push_back(hidden_name);
			binding_to_alias[key] = hidden_name;
		}
	}

	// Extract HAVING predicate as SQL.
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
