#include "core/plan_rewrite.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/plan_rewrite_internal.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/optimizer/deliminator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "rules/column_hider.hpp"
#include "upsert/refresh_index_regen.hpp"

#include <functional>
#include <set>
#include <unordered_set>

namespace duckdb {

/// Replace LOGICAL_DISTINCT with LOGICAL_AGGREGATE + COUNT(*).
/// Returns whether a DISTINCT state aggregate was added at the top level.
static bool RewriteDistinct(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &node,
                            bool is_top_level) {
	if (node->type != LogicalOperatorType::LOGICAL_DISTINCT) {
		bool added_top_level_state = false;
		for (idx_t child_idx = 0; child_idx < node->children.size(); child_idx++) {
			bool child_is_top_level =
			    is_top_level && node->type == LogicalOperatorType::LOGICAL_PROJECTION && child_idx == 0;
			added_top_level_state = RewriteDistinct(context, binder, node->children[child_idx], child_is_top_level) ||
			                        added_top_level_state;
		}
		return added_top_level_state;
	}

	auto &distinct = node->Cast<LogicalDistinct>();
	if (node->children.empty()) {
		OPENIVM_DEBUG_PRINT("[PlanRewrite] DISTINCT has no children — skipping\n");
		return false;
	}
	if (ProducesAtMostOneRow(*node->children[0])) {
		OPENIVM_DEBUG_PRINT("[PlanRewrite] Removed redundant DISTINCT over scalar aggregate\n");
		node = std::move(node->children[0]);
		RewriteDistinct(context, binder, node, false);
		return false;
	}
	if (IsRedundantDistinctOverGroupKeys(*node)) {
		OPENIVM_DEBUG_PRINT("[PlanRewrite] Removed redundant DISTINCT over aggregate group keys\n");
		node = std::move(node->children[0]);
		RewriteDistinct(context, binder, node, false);
		return false;
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
	count_expr->alias = openivm::DISTINCT_COUNT_COL;

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

	OPENIVM_DEBUG_PRINT("[PlanRewrite] DISTINCT → AGGREGATE + COUNT(*), %zu groups\n", agg_node->groups.size());
	node = std::move(agg_node);
	return is_top_level;
}

static bool IsSemiAntiJoinType(JoinType join_type) {
	return join_type == JoinType::SEMI || join_type == JoinType::ANTI || join_type == JoinType::RIGHT_SEMI ||
	       join_type == JoinType::RIGHT_ANTI;
}

static idx_t SemiAntiProbeChildIndex(JoinType join_type) {
	if (join_type == JoinType::SEMI || join_type == JoinType::ANTI) {
		return 1;
	}
	if (join_type == JoinType::RIGHT_SEMI || join_type == JoinType::RIGHT_ANTI) {
		return 0;
	}
	return DConstants::INVALID_INDEX;
}

static unordered_set<idx_t> OutputTableIndexes(LogicalOperator &op) {
	unordered_set<idx_t> result;
	for (auto &binding : op.GetColumnBindings()) {
		result.insert(binding.table_index);
	}
	return result;
}

static unordered_set<idx_t> ExpressionTableIndexes(const Expression &expr) {
	unordered_set<idx_t> result;
	LogicalJoin::GetExpressionBindings(expr, result);
	return result;
}

static bool ExpressionUsesOnly(const Expression &expr, const unordered_set<idx_t> &table_indexes,
                               bool allow_no_bindings = false) {
	auto bindings = ExpressionTableIndexes(expr);
	if (bindings.empty()) {
		return allow_no_bindings;
	}
	for (auto &binding : bindings) {
		if (!table_indexes.count(binding)) {
			return false;
		}
	}
	return true;
}

struct SemiAntiKeyReplacement {
	unique_ptr<Expression> delim_expr;
	unique_ptr<Expression> source_expr;
	bool null_reject_source = false;
};

static bool TryCollectSemiAntiKeyReplacement(ExpressionType comparison, const Expression &left, const Expression &right,
                                             const unordered_set<idx_t> &source_bindings,
                                             const unordered_set<idx_t> &delim_bindings,
                                             vector<SemiAntiKeyReplacement> &replacements) {
	if (comparison != ExpressionType::COMPARE_EQUAL && comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		return false;
	}
	bool left_source = ExpressionUsesOnly(left, source_bindings);
	bool right_source = ExpressionUsesOnly(right, source_bindings);
	bool left_delim = ExpressionUsesOnly(left, delim_bindings);
	bool right_delim = ExpressionUsesOnly(right, delim_bindings);

	SemiAntiKeyReplacement replacement;
	if (left_source && right_delim) {
		replacement.source_expr = left.Copy();
		replacement.delim_expr = right.Copy();
	} else if (right_source && left_delim) {
		replacement.source_expr = right.Copy();
		replacement.delim_expr = left.Copy();
	} else {
		return false;
	}
	replacement.null_reject_source = comparison == ExpressionType::COMPARE_EQUAL;
	replacements.push_back(std::move(replacement));
	return true;
}

static unique_ptr<Expression> BuildIsNotNull(unique_ptr<Expression> child) {
	auto is_not_null = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_IS_NOT_NULL, LogicalType::BOOLEAN);
	is_not_null->children.push_back(std::move(child));
	return std::move(is_not_null);
}

static bool IsEmptyJoinPredicate(const unique_ptr<Expression> &predicate) {
	if (!predicate) {
		return true;
	}
	if (predicate->type != ExpressionType::VALUE_CONSTANT) {
		return false;
	}
	auto &constant = predicate->Cast<BoundConstantExpression>();
	return !constant.value.IsNull() && constant.value.type().id() == LogicalTypeId::BOOLEAN &&
	       constant.value.GetValue<bool>();
}

// DuckDB decorrelates simple equality EXISTS/NOT EXISTS into a SEMI/ANTI join
// whose existence-probe side is:
//   project(delim_key) over filter(source_key = delim_key)
//     over cross(source, distinct(preserved_keys)).
// That is correct but unnecessarily expands the RHS by the left key domain. When
// the correlated predicates are equality/null-safe equality predicates, the probe
// side only needs to expose the distinct matching source keys. Plain '='
// additionally rejects NULL source keys to preserve SQL's UNKNOWN semantics
// before the outer SEMI/ANTI join uses DuckDB's null-safe join condition.
static bool TryRewriteSemiAntiProbeKeyProjection(unique_ptr<LogicalOperator> &node) {
	if (!node || node->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &semi_anti = node->Cast<LogicalComparisonJoin>();
	if (!IsSemiAntiJoinType(semi_anti.join_type) || semi_anti.children.size() != 2) {
		return false;
	}
	const idx_t probe_child_idx = SemiAntiProbeChildIndex(semi_anti.join_type);
	if (probe_child_idx == DConstants::INVALID_INDEX || probe_child_idx >= semi_anti.children.size()) {
		return false;
	}
	auto &probe_child = semi_anti.children[probe_child_idx];
	if (!probe_child || probe_child->type != LogicalOperatorType::LOGICAL_PROJECTION ||
	    probe_child->children.size() != 1) {
		return false;
	}
	auto &old_projection = probe_child->Cast<LogicalProjection>();
	if (!old_projection.children[0] || old_projection.children[0]->type != LogicalOperatorType::LOGICAL_FILTER ||
	    old_projection.children[0]->children.size() != 1) {
		return false;
	}
	auto &filter = old_projection.children[0]->Cast<LogicalFilter>();
	if (!filter.children[0] || filter.children[0]->children.size() != 2) {
		return false;
	}
	if (!filter.projection_map.empty()) {
		return false;
	}
	auto &inner = *filter.children[0];
	if (inner.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	    inner.type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		return false;
	}

	idx_t delim_child_idx = DConstants::INVALID_INDEX;
	if (inner.children[0]->type == LogicalOperatorType::LOGICAL_DISTINCT) {
		delim_child_idx = 0;
	} else if (inner.children[1]->type == LogicalOperatorType::LOGICAL_DISTINCT) {
		delim_child_idx = 1;
	} else {
		return false;
	}
	const idx_t source_child_idx = 1 - delim_child_idx;
	auto source_bindings = OutputTableIndexes(*inner.children[source_child_idx]);
	auto delim_bindings = OutputTableIndexes(*inner.children[delim_child_idx]);

	vector<SemiAntiKeyReplacement> replacements;
	if (inner.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &inner_join = inner.Cast<LogicalComparisonJoin>();
		if (inner_join.join_type != JoinType::INNER || !IsEmptyJoinPredicate(inner_join.predicate)) {
			return false;
		}
		for (auto &condition : inner_join.conditions) {
			if (!condition.left || !condition.right ||
			    !TryCollectSemiAntiKeyReplacement(condition.comparison, *condition.left, *condition.right,
			                                      source_bindings, delim_bindings, replacements)) {
				return false;
			}
		}
	}

	vector<unique_ptr<Expression>> source_filters;
	for (auto &expr : filter.expressions) {
		if (!expr) {
			return false;
		}
		if (expr->type == ExpressionType::COMPARE_EQUAL || expr->type == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			auto &comparison = expr->Cast<BoundComparisonExpression>();
			if (TryCollectSemiAntiKeyReplacement(comparison.type, *comparison.left, *comparison.right, source_bindings,
			                                     delim_bindings, replacements)) {
				continue;
			}
		}
		if (!ExpressionUsesOnly(*expr, source_bindings, true)) {
			return false;
		}
		source_filters.push_back(expr->Copy());
	}
	if (replacements.empty()) {
		return false;
	}

	vector<bool> used_replacements(replacements.size(), false);
	vector<unique_ptr<Expression>> new_projection_exprs;
	new_projection_exprs.reserve(old_projection.expressions.size());
	for (auto &expr : old_projection.expressions) {
		if (!expr) {
			return false;
		}
		bool replaced = false;
		for (idx_t i = 0; i < replacements.size(); i++) {
			if (Expression::Equals(*expr, *replacements[i].delim_expr)) {
				used_replacements[i] = true;
				new_projection_exprs.push_back(replacements[i].source_expr->Copy());
				replaced = true;
				break;
			}
		}
		if (replaced) {
			continue;
		}
		if (ExpressionUsesOnly(*expr, source_bindings, true)) {
			new_projection_exprs.push_back(expr->Copy());
			continue;
		}
		return false;
	}
	for (idx_t i = 0; i < replacements.size(); i++) {
		if (!used_replacements[i]) {
			return false;
		}
		if (replacements[i].null_reject_source) {
			source_filters.push_back(BuildIsNotNull(replacements[i].source_expr->Copy()));
		}
	}

	auto source = std::move(inner.children[source_child_idx]);
	if (!source_filters.empty()) {
		auto source_filter = make_uniq<LogicalFilter>();
		source_filter->expressions = std::move(source_filters);
		source_filter->children.push_back(std::move(source));
		source_filter->ResolveOperatorTypes();
		source = std::move(source_filter);
	}

	auto projection = make_uniq<LogicalProjection>(old_projection.table_index, std::move(new_projection_exprs));
	projection->children.push_back(std::move(source));
	projection->ResolveOperatorTypes();

	vector<unique_ptr<Expression>> distinct_targets;
	auto projection_bindings = projection->GetColumnBindings();
	for (idx_t i = 0; i < projection_bindings.size(); i++) {
		distinct_targets.push_back(make_uniq<BoundColumnRefExpression>(projection->types[i], projection_bindings[i]));
	}
	auto distinct = make_uniq<LogicalDistinct>(std::move(distinct_targets), DistinctType::DISTINCT);
	distinct->children.push_back(std::move(projection));
	distinct->ResolveOperatorTypes();

	semi_anti.children[probe_child_idx] = std::move(distinct);
	semi_anti.ResolveOperatorTypes();
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Rewrote SEMI/ANTI probe side to distinct source keys\n");
	return true;
}

static bool RewriteSemiAntiProbeKeyProjections(unique_ptr<LogicalOperator> &node) {
	if (!node) {
		return false;
	}
	bool rewritten = false;
	for (auto &child : node->children) {
		rewritten = RewriteSemiAntiProbeKeyProjections(child) || rewritten;
	}
	return TryRewriteSemiAntiProbeKeyProjection(node) || rewritten;
}

static bool IsMarkColumnRef(const Expression &expr, idx_t mark_index) {
	if (expr.type != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col_ref = expr.Cast<BoundColumnRefExpression>();
	return col_ref.binding.table_index == mark_index && col_ref.binding.column_index == 0;
}

static bool IsNotMarkColumnRef(const Expression &expr, idx_t mark_index) {
	if (expr.type != ExpressionType::OPERATOR_NOT) {
		return false;
	}
	auto &op = expr.Cast<BoundOperatorExpression>();
	return op.children.size() == 1 && op.children[0] && IsMarkColumnRef(*op.children[0], mark_index);
}

static bool MarkJoinCanBecomeSemiAnti(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN && op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK || join.conditions.empty() || join.predicate || !join.convert_mark_to_semi) {
		return false;
	}
	for (auto &condition : join.conditions) {
		// Correlated EXISTS/NOT EXISTS MARK joins use null-safe delimiter comparisons.
		// That guarantees the mark is TRUE/FALSE, not NULL, so filtering on mark is
		// equivalent to SEMI/ANTI. Other MARK joins are left untouched.
		if (condition.comparison != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return false;
		}
	}
	return true;
}

static void CollectConvertibleMarkIndexes(LogicalOperator &op, unordered_set<idx_t> &mark_indexes) {
	if (MarkJoinCanBecomeSemiAnti(op)) {
		auto &join = op.Cast<LogicalComparisonJoin>();
		mark_indexes.insert(join.mark_index);
	}
	for (auto &child : op.children) {
		if (child) {
			CollectConvertibleMarkIndexes(*child, mark_indexes);
		}
	}
}

static bool IsConvertibleMarkPredicate(const Expression &expr, const unordered_set<idx_t> &mark_indexes) {
	for (auto &mark_index : mark_indexes) {
		if (IsMarkColumnRef(expr, mark_index) || IsNotMarkColumnRef(expr, mark_index)) {
			return true;
		}
	}
	return false;
}

static bool RewriteMarkJoinFilters(unique_ptr<LogicalOperator> &node);

static bool TryPushMarkFilterThroughSemiAnti(unique_ptr<LogicalOperator> &node) {
	if (!node || node->type != LogicalOperatorType::LOGICAL_FILTER || node->children.size() != 1) {
		return false;
	}
	auto &filter = node->Cast<LogicalFilter>();
	if (!filter.projection_map.empty()) {
		return false;
	}
	auto &child = node->children[0];
	if (!child || (child->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
	               child->type != LogicalOperatorType::LOGICAL_DELIM_JOIN)) {
		return false;
	}
	auto &join = child->Cast<LogicalComparisonJoin>();
	const idx_t probe_child_idx = SemiAntiProbeChildIndex(join.join_type);
	if (probe_child_idx == DConstants::INVALID_INDEX || join.children.size() != 2) {
		return false;
	}
	const idx_t preserved_child_idx = 1 - probe_child_idx;

	unordered_set<idx_t> mark_indexes;
	CollectConvertibleMarkIndexes(*join.children[preserved_child_idx], mark_indexes);
	if (mark_indexes.empty()) {
		return false;
	}

	vector<unique_ptr<Expression>> pushed_expressions;
	vector<unique_ptr<Expression>> remaining_expressions;
	for (auto &expr : filter.expressions) {
		if (expr && IsConvertibleMarkPredicate(*expr, mark_indexes)) {
			pushed_expressions.push_back(std::move(expr));
		} else {
			remaining_expressions.push_back(std::move(expr));
		}
	}
	if (pushed_expressions.empty()) {
		filter.expressions = std::move(remaining_expressions);
		return false;
	}

	// SEMI/ANTI only returns the preserved side, so marker predicates on that side
	// can commute below the join. This lets stacked correlated subqueries become a
	// chain of SEMI/ANTI joins instead of leaving one marker column above another.
	auto pushed_filter = make_uniq<LogicalFilter>();
	pushed_filter->expressions = std::move(pushed_expressions);
	pushed_filter->children.push_back(std::move(join.children[preserved_child_idx]));
	pushed_filter->ResolveOperatorTypes();
	join.children[preserved_child_idx] = std::move(pushed_filter);
	RewriteMarkJoinFilters(join.children[preserved_child_idx]);
	join.ResolveOperatorTypes();

	if (remaining_expressions.empty()) {
		node = std::move(child);
	} else {
		filter.expressions = std::move(remaining_expressions);
		filter.ResolveOperatorTypes();
	}
	return true;
}

static bool TryRewriteMarkFilter(unique_ptr<LogicalOperator> &node) {
	if (!node || node->type != LogicalOperatorType::LOGICAL_FILTER || node->children.size() != 1) {
		return false;
	}
	auto &filter = node->Cast<LogicalFilter>();
	if (!filter.projection_map.empty()) {
		return false;
	}
	auto &child = node->children[0];
	if (!child || !MarkJoinCanBecomeSemiAnti(*child)) {
		return TryPushMarkFilterThroughSemiAnti(node);
	}
	auto &join = child->Cast<LogicalComparisonJoin>();

	JoinType replacement_join_type = JoinType::INVALID;
	idx_t mark_filter_idx = DConstants::INVALID_INDEX;
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		auto &expr = filter.expressions[i];
		if (!expr) {
			continue;
		}
		if (IsMarkColumnRef(*expr, join.mark_index)) {
			replacement_join_type = JoinType::SEMI;
			mark_filter_idx = i;
			break;
		}
		if (IsNotMarkColumnRef(*expr, join.mark_index)) {
			replacement_join_type = JoinType::ANTI;
			mark_filter_idx = i;
			break;
		}
	}
	if (mark_filter_idx == DConstants::INVALID_INDEX) {
		return false;
	}

	filter.expressions.erase_at(mark_filter_idx);
	join.join_type = replacement_join_type;
	join.mark_types.clear();
	child->ResolveOperatorTypes();

	if (filter.expressions.empty()) {
		node = std::move(child);
	} else {
		node->ResolveOperatorTypes();
		TryPushMarkFilterThroughSemiAnti(node);
	}
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Rewrote MARK join filter to %s join\n",
	                    replacement_join_type == JoinType::SEMI ? "SEMI" : "ANTI");
	return true;
}

static bool RewriteMarkJoinFilters(unique_ptr<LogicalOperator> &node) {
	if (!node) {
		return false;
	}
	bool rewritten = false;
	for (auto &child : node->children) {
		rewritten = RewriteMarkJoinFilters(child) || rewritten;
	}
	return TryRewriteMarkFilter(node) || rewritten;
}

/// Inline every `LOGICAL_CTE_REF` in the plan with a fresh deep copy of its CTE
/// definition, then collapse `LOGICAL_MATERIALIZED_CTE` wrapper nodes. This
/// makes IVM see N independent leaves for an N-way self-join through a CTE,
/// instead of one materialized intermediate referenced N times — without it,
/// join delta compilation's inclusion-exclusion can't generate the cross-terms
/// (Δt ⋈ t_now and t_now ⋈ Δt) needed to produce new pairs from a single
/// base-table delta.
///
/// Inlining strategy: for each CTE_REF, deep-copy the definition subtree and
/// renumber its bindings to fresh table indices via
/// `renumber_and_rebind_subtree`. Wrap the renumbered subtree in a passthrough
/// projection at the CTE_REF's original `table_index` so parent operators' BCRs
/// (which point to
/// `(ref.table_index, i)`) keep resolving correctly with no upstream rebinding.
///
/// We process MATERIALIZED_CTE nodes bottom-up: at each one, fully inline the
/// definition into the consumer (children[1]), then replace the
/// MATERIALIZED_CTE node with the modified consumer. Bottom-up traversal
/// ensures that nested CTEs inside the definition or consumer are resolved
/// before this CTE is, so no captured pointer ever dangles.
///
/// CTEInlining (already invoked in `parser.cpp`) usually handles this for
/// single-reference and small CTEs, but multi-reference CTEs that don't get
/// inlined by the optimizer end up here. Recursive CTEs are not supported and
/// are left untouched (caller catches the unsupported-operator error
/// downstream).
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
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Inlined CTE_REF cte_index=%lu "
			                    "(table_index=%lu, %zu cols)\n",
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
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Collapsed MATERIALIZED_CTE wrapper (cte_index=%lu)\n", (unsigned long)cte_index);
}

// ===========================================================================
// Constant scalar-subquery folding
//
// An uncorrelated scalar subquery in a projection (e.g. `(SELECT k FROM params)`
// where `params` is a one-row constants CTE) plans, in the unoptimized logical
// plan OpenIVM classifies, as a CROSS_PRODUCT whose subquery side is a single-row
// guard: PROJECTION(CASE WHEN count>1 THEN error() ELSE value END) over an
// ungrouped AGGREGATE(first(arg), count_star()) over a constant subtree. The
// ungrouped `first()` aggregate is unsupported by the incremental checker, so the
// whole view degrades to FULL_REFRESH. When the subquery resolves to a constant,
// folding it to a literal is value-preserving and lets the view incrementalize.
//
// Hard restriction: only fold provably-constant single-row subtrees (PROJECTION /
// ungrouped AGGREGATE(first|count_star) / DUMMY_SCAN, all expressions foldable and
// non-volatile). A subquery that scans base tables changes with deltas and must
// never be folded.
// ===========================================================================

// Replace every BoundColumnRef matching `target` in `expr` with a constant whose
// foldable child references (those in `consts`) have been substituted first.
static void SubstituteConstants(unique_ptr<Expression> &expr, const column_binding_map_t<Value> &consts) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		auto it = consts.find(bcr.binding);
		if (it != consts.end()) {
			expr = make_uniq<BoundConstantExpression>(it->second.DefaultCastAs(bcr.return_type));
		}
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { SubstituteConstants(child, consts); });
}

// Fold a (constant) expression to a Value. CASE is evaluated lazily: only the
// taken branch is folded, so an untaken branch that is itself non-foldable — the
// scalar-subquery guard's `error(...)` THEN branch — is never inspected.
static bool TryFoldConstExpr(ClientContext &context, Expression &expr, Value &result) {
	if (expr.type == ExpressionType::CASE_EXPR) {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		for (auto &check : case_expr.case_checks) {
			Value cond;
			if (!TryFoldConstExpr(context, *check.when_expr, cond)) {
				return false;
			}
			if (!cond.IsNull() && BooleanValue::Get(cond)) {
				return TryFoldConstExpr(context, *check.then_expr, result);
			}
		}
		return TryFoldConstExpr(context, *case_expr.else_expr, result);
	}
	if (expr.IsVolatile() || !expr.IsFoldable()) {
		return false;
	}
	return ExpressionExecutor::TryEvaluateScalar(context, expr, result);
}

// Fold `expr` to a Value after substituting any `consts` child column refs. Returns
// false if the result is not a provably-constant, non-volatile scalar.
static bool SubstituteAndFold(ClientContext &context, const Expression &expr, const column_binding_map_t<Value> &consts,
                              Value &result) {
	auto copy = expr.Copy();
	SubstituteConstants(copy, consts);
	return TryFoldConstExpr(context, *copy, result);
}

// Evaluate a constant-only single-row subtree, mapping each of `node`'s output
// column bindings to its Value. Returns false (leaving the plan untouched) on
// anything not provably constant.
static bool TryEvalConstantSubtree(ClientContext &context, LogicalOperator &node, column_binding_map_t<Value> &out,
                                   const unordered_map<idx_t, LogicalOperator *> &cte_defs) {
	switch (node.type) {
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		return true; // one row, no columns
	case LogicalOperatorType::LOGICAL_CTE_REF: {
		// The full CREATE plan still has materialized CTEs (DuckDB CTEInlining keeps
		// multi-referenced ones). Resolve the ref against its constant definition.
		auto &ref = node.Cast<LogicalCTERef>();
		auto it = cte_defs.find(ref.cte_index);
		if (it == cte_defs.end()) {
			return false;
		}
		column_binding_map_t<Value> def_out;
		if (!TryEvalConstantSubtree(context, *it->second, def_out, cte_defs)) {
			return false;
		}
		it->second->ResolveOperatorTypes();
		auto def_bindings = it->second->GetColumnBindings();
		idx_t cols = std::min(ref.chunk_types.size(), def_bindings.size());
		for (idx_t i = 0; i < cols; i++) {
			auto vit = def_out.find(def_bindings[i]);
			if (vit == def_out.end()) {
				return false;
			}
			out[ColumnBinding(ref.table_index, i)] = vit->second;
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_PROJECTION: {
		if (node.children.size() != 1) {
			return false;
		}
		column_binding_map_t<Value> child;
		if (!TryEvalConstantSubtree(context, *node.children[0], child, cte_defs)) {
			return false;
		}
		auto &proj = node.Cast<LogicalProjection>();
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			Value v;
			if (!SubstituteAndFold(context, *proj.expressions[i], child, v)) {
				return false;
			}
			out[ColumnBinding(proj.table_index, i)] = std::move(v);
		}
		return true;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto &agg = node.Cast<LogicalAggregate>();
		// Only the ungrouped scalar-subquery guard (no GROUP BY, no grouping sets).
		if (!agg.groups.empty() || agg.grouping_sets.size() > 1 || node.children.size() != 1) {
			return false;
		}
		column_binding_map_t<Value> child;
		if (!TryEvalConstantSubtree(context, *node.children[0], child, cte_defs)) {
			return false;
		}
		for (idx_t i = 0; i < agg.expressions.size(); i++) {
			if (agg.expressions[i]->expression_class != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto &bound_agg = agg.expressions[i]->Cast<BoundAggregateExpression>();
			if (bound_agg.filter) {
				return false;
			}
			Value v;
			if (bound_agg.function.name == "count_star" && bound_agg.children.empty()) {
				v = Value::BIGINT(1); // constant subtree is exactly one row
			} else if (bound_agg.function.name == "first" && bound_agg.children.size() == 1) {
				if (!SubstituteAndFold(context, *bound_agg.children[0], child, v)) {
					return false;
				}
			} else {
				return false; // any other aggregate: not a constant scalar guard
			}
			out[ColumnBinding(agg.aggregate_index, i)] = std::move(v);
		}
		return true;
	}
	default:
		return false;
	}
}

// Replaces a single column binding with a constant everywhere in a plan.
class ConstantBindingReplacer : public LogicalOperatorVisitor {
public:
	ConstantBindingReplacer(ColumnBinding target, Value value) : target(target), value(std::move(value)) {
	}
	using LogicalOperatorVisitor::VisitReplace;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		if (expr.binding == target) {
			return make_uniq<BoundConstantExpression>(value.DefaultCastAs(expr.return_type));
		}
		return nullptr;
	}

private:
	ColumnBinding target;
	Value value;
};

// Find the first CROSS_PRODUCT (bottom-up) with a provably-constant single-row
// side, fold that side's output columns to constants plan-wide, and drop the
// cross product. Returns true if a fold was applied.
static bool FoldConstantScalarSubqueriesOnce(ClientContext &context, unique_ptr<LogicalOperator> &root,
                                             unique_ptr<LogicalOperator> &node,
                                             const unordered_map<idx_t, LogicalOperator *> &cte_defs) {
	for (auto &c : node->children) {
		if (FoldConstantScalarSubqueriesOnce(context, root, c, cte_defs)) {
			return true;
		}
	}
	if (node->type != LogicalOperatorType::LOGICAL_CROSS_PRODUCT || node->children.size() != 2) {
		return false;
	}
	for (idx_t side = 0; side < 2; side++) {
		column_binding_map_t<Value> values;
		if (!TryEvalConstantSubtree(context, *node->children[side], values, cte_defs)) {
			continue;
		}
		node->children[side]->ResolveOperatorTypes();
		auto bindings = node->children[side]->GetColumnBindings();
		if (bindings.empty()) {
			continue; // nothing to project away (bare DUMMY_SCAN) — not a scalar value
		}
		bool all_constant = true;
		for (auto &b : bindings) {
			if (values.find(b) == values.end()) {
				all_constant = false;
				break;
			}
		}
		if (!all_constant) {
			continue;
		}
		for (auto &b : bindings) {
			ConstantBindingReplacer replacer(b, values[b]);
			replacer.VisitOperator(*root);
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Folded constant scalar subquery "
			                    "binding (%lu.%lu) = %s\n",
			                    (unsigned long)b.table_index, (unsigned long)b.column_index,
			                    values[b].ToString().c_str());
		}
		node = std::move(node->children[1 - side]); // keep the real input, drop the cross product
		return true;
	}
	return false;
}

void FoldConstantScalarSubqueries(ClientContext &context, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	// Map cte_index -> definition subtree so scalar subqueries that bottom out in a
	// CTE_REF (the full CREATE plan keeps multi-referenced CTEs materialized) can still
	// be resolved to a constant.
	unordered_map<idx_t, LogicalOperator *> cte_defs;
	std::function<void(LogicalOperator &)> collect = [&](LogicalOperator &n) {
		if (n.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && n.children.size() == 2) {
			cte_defs[n.Cast<LogicalMaterializedCTE>().table_index] = n.children[0].get();
		}
		for (auto &c : n.children) {
			collect(*c);
		}
	};
	collect(*plan);
	// Loop to a fixpoint: stacked scalar subqueries appear as nested CROSS_PRODUCTs.
	while (FoldConstantScalarSubqueriesOnce(context, plan, plan, cte_defs)) {
	}
	plan->ResolveOperatorTypes();
}

static ColumnBinding AppendProjectionPassthrough(LogicalProjection &proj, const ColumnBinding &binding,
                                                 const LogicalType &type, const string &alias) {
	auto passthrough = make_uniq<BoundColumnRefExpression>(type, binding);
	passthrough->alias = alias;
	proj.expressions.push_back(std::move(passthrough));
	proj.ResolveOperatorTypes();
	auto bindings = proj.GetColumnBindings();
	if (bindings.empty()) {
		throw InternalException("OpenIVM: projection produced no bindings after appending hidden passthrough");
	}
	return bindings.back();
}

static void PropagateHiddenBindingThroughProjectionPath(vector<LogicalProjection *> &projection_path,
                                                        ColumnBinding binding, LogicalType type, const string &alias) {
	for (auto it = projection_path.rbegin(); it != projection_path.rend(); ++it) {
		binding = AppendProjectionPassthrough(**it, binding, type, alias);
		type = (*it)->types.back();
	}
}

/// Inject a hidden COUNT(*) (alias `openivm_count_star`) into AGGREGATE_GROUP
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
/// The column is prefixed `openivm_` so `column_hider` auto-excludes it from
/// the user-facing VIEW; `CompileAggregateGroups` already recognizes it
/// via openivm::COUNT_STAR_COL.
static void InjectGroupCountStar(unique_ptr<LogicalOperator> &plan) {
	// Only inject into the aggregate on the top-level output path. ORDER BY/LIMIT/TOP_N
	// preserve the projection schema; inner aggregates under joins or set operations
	// are handled by other refresh strategies.
	vector<LogicalProjection *> projection_path;
	LogicalOperator *node = plan.get();
	LogicalOperator *agg_search = nullptr;
	while (node) {
		switch (node->type) {
		case LogicalOperatorType::LOGICAL_ORDER_BY:
		case LogicalOperatorType::LOGICAL_LIMIT:
		case LogicalOperatorType::LOGICAL_TOP_N:
			node = node->children.size() == 1 ? node->children[0].get() : nullptr;
			continue;
		case LogicalOperatorType::LOGICAL_PROJECTION:
			projection_path.push_back(&node->Cast<LogicalProjection>());
			node = node->children.size() == 1 ? node->children[0].get() : nullptr;
			continue;
		case LogicalOperatorType::LOGICAL_FILTER:
			if (node->children.size() == 1 &&
			    node->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
				agg_search = node->children[0].get();
			}
			node = nullptr;
			continue;
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
			agg_search = node;
			node = nullptr;
			continue;
		default:
			node = nullptr;
			continue;
		}
	}
	if (!agg_search || projection_path.empty()) {
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
	//   - openivm_count_star or openivm_distinct_count already injected by an earlier
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
		if (bound.alias == openivm::COUNT_STAR_COL || bound.alias == openivm::DISTINCT_COUNT_COL) {
			return;
		}
		if (bound.function.name == "arg_min" || bound.function.name == "arg_max") {
			has_argminmax = true;
		}
	}
	// ARG_MIN/ARG_MAX always use group-recompute. Skip hidden count injection so the
	// data table schema remains the user-visible aggregate output. Checked after the
	// loop so count_star / distinct_count in the same view still short-circuit
	// correctly regardless of expression order.
	if (has_argminmax) {
		return;
	}
	auto count_star_func = CountStarFun::GetFunction();
	vector<unique_ptr<Expression>> count_args;
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_star_func), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = openivm::COUNT_STAR_COL;
	auto count_type = count_expr->return_type;
	idx_t new_agg_idx = agg.expressions.size();
	agg.expressions.push_back(std::move(count_expr));
	agg.ResolveOperatorTypes();

	ColumnBinding count_binding(agg.aggregate_index, new_agg_idx);
	PropagateHiddenBindingThroughProjectionPath(projection_path, count_binding, count_type, openivm::COUNT_STAR_COL);
	plan->ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[PlanRewrite] Injected openivm_count_star for AGGREGATE_GROUP\n");
}

/// Returns true if `alias` is one of the reserved IVM-hidden-column prefixes
/// added by DecomposeAvgStddev. Used to decide whether to propagate a child
/// projection's expression up through a pass-through parent projection.
static bool IsHiddenAggregateAlias(const string &alias) {
	return alias.find(openivm::SUM_COL_PREFIX) == 0 || alias.find(openivm::COUNT_COL_PREFIX) == 0 ||
	       alias.find(openivm::SUM_SQ_COL_PREFIX) == 0 || alias.find(openivm::SUM_SQP_COL_PREFIX) == 0 ||
	       alias.find(openivm::SUM_COUNT_COL_PREFIX) == 0;
}

/// Propagate hidden aggregate columns (openivm_sum_*, openivm_count_*, …) added
/// by DecomposeAvgStddev up through any chain of pass-through PROJECTIONs that
/// sits between the decomposed projection and the plan root.
///
/// Why: CTE inlining + projection stacking can leave a plan like
///   PROJECTION (user SELECT: ROUND(avg_bal, 2), cnt, …)   <- top
///     PROJECTION (pass-through BCRs)                       <- middle
///       PROJECTION (CTE body, holds openivm_sum_* from decomp) <- inner
///         AGGREGATE
/// Without propagation, middle and top strip the hidden columns, so the MV
/// data table stores only the final AVG and the MERGE computes `v.avg + d.avg`
/// — wrong for non-summable aggregates. Propagation lets CompileAggregateGroups
/// see the hidden SUM/COUNT columns and maintain them separately.
void PropagateHiddenAggregateColumns(unique_ptr<LogicalOperator> &plan) {
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
		OPENIVM_DEBUG_PRINT("[PropagateHidden] Added '%s' to parent projection "
		                    "(table_index=%llu)\n",
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

/// Add openivm_left_key (and openivm_right_key for FULL OUTER) projection at
/// the top of the plan.
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

	// Always add openivm_left_key as a separate extra column.
	proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(openivm::LEFT_KEY_COL, key_type, key_binding));

	// For FULL OUTER: also add openivm_right_key in the same projection.
	if (is_full_outer) {
		proj_exprs.push_back(
		    make_uniq<BoundColumnRefExpression>(openivm::RIGHT_KEY_COL, right_key_type, right_key_binding));
	}

	// Use a table index that won't conflict (high number)
	idx_t proj_table_index = binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(proj_table_index, std::move(proj_exprs));
	projection->children.push_back(std::move(plan));
	projection->ResolveOperatorTypes();
	plan = std::move(projection);

	OPENIVM_DEBUG_PRINT("[PlanRewrite] Added openivm_left_key%s projection\n",
	                    is_full_outer ? " + openivm_right_key" : "");
}

/// For LEFT/OUTER JOIN aggregate views: add COUNT(null_side_key) AS
/// openivm_match_count. For FULL OUTER JOINs, also add COUNT(left_key) AS
/// openivm_right_match_count. These hidden aggregates track how many rows match
/// from each side (Larson & Zhou / Zhang & Larson). When match_count=0,
/// aggregate columns from that side should be NULL.
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
	vector<LogicalProjection *> projection_path;
	auto &root_proj = plan->Cast<LogicalProjection>();
	projection_path.push_back(&root_proj);

	LogicalOperator *agg_search = plan->children.empty() ? nullptr : plan->children[0].get();
	// Walk through possible intermediate projections to find the aggregate
	while (agg_search && agg_search->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		projection_path.push_back(&agg_search->Cast<LogicalProjection>());
		agg_search = agg_search->children.empty() ? nullptr : agg_search->children[0].get();
	}
	if (!agg_search || agg_search->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto &agg = agg_search->Cast<LogicalAggregate>();

	// Add COUNT(null_side_key) as openivm_match_count (tracks right-side matches for LEFT/OUTER).
	auto count_func = BindAggregateByName(context, "count", {null_side_type});
	vector<unique_ptr<Expression>> count_args;
	count_args.push_back(make_uniq<BoundColumnRefExpression>(null_side_type, null_side_binding));
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), std::move(count_args), nullptr,
	                                                      nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = string(openivm::MATCH_COUNT_COL);
	idx_t match_count_idx = agg.expressions.size();
	agg.expressions.push_back(std::move(count_expr));

	// For FULL OUTER: add COUNT(left_key) as openivm_right_match_count (tracks left-side matches).
	idx_t right_match_count_idx = 0;
	if (is_full_outer) {
		auto right_count_func = BindAggregateByName(context, "count", {left_side_type});
		vector<unique_ptr<Expression>> right_count_args;
		right_count_args.push_back(make_uniq<BoundColumnRefExpression>(left_side_type, left_side_binding));
		auto right_count_expr = make_uniq<BoundAggregateExpression>(
		    std::move(right_count_func), std::move(right_count_args), nullptr, nullptr, AggregateType::NON_DISTINCT);
		right_count_expr->alias = string(openivm::RIGHT_MATCH_COUNT_COL);
		right_match_count_idx = agg.expressions.size();
		agg.expressions.push_back(std::move(right_count_expr));
	}

	agg.ResolveOperatorTypes();

	// Add passthrough through every projection between the aggregate and the root.
	// PIVOT and other planner rewrites can introduce several projection layers; adding
	// the hidden binding only to the top projection leaves LPTS with a reference to a
	// column that no child CTE emits.
	auto agg_bindings = agg_search->GetColumnBindings();
	const auto &agg_types = agg_search->types;
	idx_t group_count = agg.groups.size();

	ColumnBinding match_binding = agg_bindings[group_count + match_count_idx];
	const LogicalType &match_type = agg_types[group_count + match_count_idx];
	PropagateHiddenBindingThroughProjectionPath(projection_path, match_binding, match_type, openivm::MATCH_COUNT_COL);

	if (is_full_outer) {
		ColumnBinding right_match_binding = agg_bindings[group_count + right_match_count_idx];
		const LogicalType &right_match_type = agg_types[group_count + right_match_count_idx];
		PropagateHiddenBindingThroughProjectionPath(projection_path, right_match_binding, right_match_type,
		                                            openivm::RIGHT_MATCH_COUNT_COL);
	}

	OPENIVM_DEBUG_PRINT("[PlanRewrite] Added openivm_match_count%s for outer join aggregate\n",
	                    is_full_outer ? " + openivm_right_match_count" : "");
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
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Pass start: %s\n", pass.name);
	pass.rewrite(rewrite_context);
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Pass done: %s\n", pass.name);
}

static void RewritePassInlineCteRefs(PlanRewriteContext &rewrite_context) {
	InlineCteRefs(rewrite_context.context, rewrite_context.binder, rewrite_context.plan);
}

static void RewritePassFoldConstantScalarSubqueries(PlanRewriteContext &rewrite_context) {
	FoldConstantScalarSubqueries(rewrite_context.context, rewrite_context.plan);
}

static void RewritePassAggregateFilters(PlanRewriteContext &rewrite_context) {
	RewriteAggregateFilters(rewrite_context.context, rewrite_context.plan);
}

static void RewritePassDistinct(PlanRewriteContext &rewrite_context) {
	bool added_top_level_state = RewriteDistinct(rewrite_context.context, rewrite_context.binder, rewrite_context.plan,
	                                             HasTopLevelDistinct(rewrite_context.plan));
	if (added_top_level_state) {
		rewrite_context.planner_names.push_back(openivm::DISTINCT_COUNT_COL);
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

static void RewritePassSemiAntiSubqueries(PlanRewriteContext &rewrite_context) {
	if (RewriteMarkJoinFilters(rewrite_context.plan)) {
		Deliminator deliminator;
		rewrite_context.plan = deliminator.Optimize(std::move(rewrite_context.plan));
	}
	RewriteSafeSemiAntiDelimGets(rewrite_context.context, rewrite_context.plan);
	RewriteSemiAntiProbeKeyProjections(rewrite_context.plan);
}

static void RunRewritePipeline(PlanRewriteContext &rewrite_context) {
	const PlanRewritePassEntry passes[] = {
	    {"inline_cte_refs", RewritePassInlineCteRefs},
	    {"fold_constant_scalar_subqueries", RewritePassFoldConstantScalarSubqueries},
	    {"aggregate_filters", RewritePassAggregateFilters},
	    {"distinct", RewritePassDistinct},
	    {"derived_aggregates", RewritePassDerivedAggregates},
	    {"group_count_star", RewritePassGroupCountStar},
	    {"hidden_aggregate_propagation", RewritePassHiddenAggregatePropagation},
	    {"outer_join_support", RewritePassOuterJoinSupport},
	    {"semi_anti_subqueries", RewritePassSemiAntiSubqueries},
	};

	for (const auto &pass : passes) {
		RunRewritePass(pass, rewrite_context);
	}
}

void PlanRewrite(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                 vector<string> &planner_names) {
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Starting\n");
	PlanRewriteContext rewrite_context {context, binder, plan, planner_names};
	RunRewritePipeline(rewrite_context);
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Done\n");
}

static uint64_t DerivedOutputBindingKey(const ColumnBinding &binding) {
	return (uint64_t)binding.table_index ^ ((uint64_t)binding.column_index * 0x9e3779b97f4a7c15ULL);
}

using DerivedOutputProjectionMap = unordered_map<idx_t, const LogicalProjection *>;

static void CollectDerivedOutputProjections(const LogicalOperator &plan, DerivedOutputProjectionMap &projections) {
	if (plan.type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &projection = plan.Cast<LogicalProjection>();
		projections[projection.table_index] = &projection;
	}
	for (auto &child : plan.children) {
		CollectDerivedOutputProjections(*child, projections);
	}
}

static const Expression *ResolveDerivedOutputPassThrough(const Expression &expr,
                                                         const DerivedOutputProjectionMap &projections,
                                                         vector<uint64_t> *binding_path = nullptr) {
	auto current = &expr;
	unordered_set<uint64_t> visited;
	while (current->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column = current->Cast<BoundColumnRefExpression>();
		auto key = DerivedOutputBindingKey(column.binding);
		if (!visited.insert(key).second) {
			return nullptr;
		}
		if (binding_path) {
			binding_path->push_back(key);
		}
		auto projection = projections.find(column.binding.table_index);
		if (projection == projections.end()) {
			return current;
		}
		if (column.binding.column_index >= projection->second->expressions.size()) {
			return nullptr;
		}
		current = projection->second->expressions[column.binding.column_index].get();
	}
	return current;
}

static string QuoteDerivedOutputIdentifier(const string &identifier) {
	string result = "\"";
	for (auto character : identifier) {
		result += character;
		if (character == '"') {
			result += '"';
		}
	}
	return result + "\"";
}

static bool RenderDerivedOutputExpression(const Expression &expr,
                                          const unordered_map<uint64_t, string> &binding_to_column,
                                          const DerivedOutputProjectionMap &projections,
                                          unordered_set<uint64_t> &active_bindings, string &sql) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &column = expr.Cast<BoundColumnRefExpression>();
		auto key = DerivedOutputBindingKey(column.binding);
		auto entry = binding_to_column.find(key);
		if (entry != binding_to_column.end()) {
			// Always quote references in the persisted expression. The refresh
			// compiler substitutes quoted identifiers with the post-MERGE state
			// expression; leaving ordinary names unquoted would make them bind
			// ambiguously between the target and delta aliases.
			sql = QuoteDerivedOutputIdentifier(entry->second);
			return true;
		}
		auto projection = projections.find(column.binding.table_index);
		if (projection == projections.end() || column.binding.column_index >= projection->second->expressions.size() ||
		    !active_bindings.insert(key).second) {
			return false;
		}
		bool rendered = RenderDerivedOutputExpression(*projection->second->expressions[column.binding.column_index],
		                                              binding_to_column, projections, active_bindings, sql);
		active_bindings.erase(key);
		return rendered;
	}
	case ExpressionClass::BOUND_CONSTANT:
		sql = expr.Cast<BoundConstantExpression>().value.ToSQLString();
		return true;
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		sql = "CASE";
		for (auto &check : case_expr.case_checks) {
			string when_sql;
			string then_sql;
			if (!RenderDerivedOutputExpression(*check.when_expr, binding_to_column, projections, active_bindings,
			                                   when_sql) ||
			    !RenderDerivedOutputExpression(*check.then_expr, binding_to_column, projections, active_bindings,
			                                   then_sql)) {
				return false;
			}
			sql += " WHEN " + when_sql + " THEN " + then_sql;
		}
		string else_sql;
		if (!RenderDerivedOutputExpression(*case_expr.else_expr, binding_to_column, projections, active_bindings,
		                                   else_sql)) {
			return false;
		}
		sql += " ELSE " + else_sql + " END";
		return true;
	}
	case ExpressionClass::BOUND_COMPARISON: {
		auto &comparison = expr.Cast<BoundComparisonExpression>();
		string left_sql;
		string right_sql;
		if (!RenderDerivedOutputExpression(*comparison.left, binding_to_column, projections, active_bindings,
		                                   left_sql) ||
		    !RenderDerivedOutputExpression(*comparison.right, binding_to_column, projections, active_bindings,
		                                   right_sql)) {
			return false;
		}
		sql = "(" + left_sql + " " + ExpressionTypeToOperator(comparison.type) + " " + right_sql + ")";
		return true;
	}
	case ExpressionClass::BOUND_CONJUNCTION: {
		auto &conjunction = expr.Cast<BoundConjunctionExpression>();
		string op = conjunction.type == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
		sql = "(";
		for (idx_t i = 0; i < conjunction.children.size(); i++) {
			string child_sql;
			if (!RenderDerivedOutputExpression(*conjunction.children[i], binding_to_column, projections,
			                                   active_bindings, child_sql)) {
				return false;
			}
			if (i > 0) {
				sql += op;
			}
			sql += child_sql;
		}
		sql += ")";
		return true;
	}
	case ExpressionClass::BOUND_OPERATOR: {
		auto &op = expr.Cast<BoundOperatorExpression>();
		if (op.children.size() != 1) {
			return false;
		}
		string child_sql;
		if (!RenderDerivedOutputExpression(*op.children[0], binding_to_column, projections, active_bindings,
		                                   child_sql)) {
			return false;
		}
		if (expr.type == ExpressionType::OPERATOR_IS_NULL) {
			sql = "(" + child_sql + " IS NULL)";
			return true;
		}
		if (expr.type == ExpressionType::OPERATOR_IS_NOT_NULL) {
			sql = "(" + child_sql + " IS NOT NULL)";
			return true;
		}
		if (expr.type == ExpressionType::OPERATOR_NOT) {
			sql = "(NOT " + child_sql + ")";
			return true;
		}
		return false;
	}
	case ExpressionClass::BOUND_CAST: {
		auto &cast = expr.Cast<BoundCastExpression>();
		string child_sql;
		if (!RenderDerivedOutputExpression(*cast.child, binding_to_column, projections, active_bindings, child_sql)) {
			return false;
		}
		sql = string(cast.try_cast ? "TRY_CAST(" : "CAST(") + child_sql + " AS " + cast.return_type.ToString() + ")";
		return true;
	}
	default:
		return false;
	}
}

DerivedAggregateOutputInfo ExtractDerivedAggregateOutputs(const LogicalOperator &plan,
                                                          const vector<string> &output_names) {
	DerivedAggregateOutputInfo info;
	if (plan.type != LogicalOperatorType::LOGICAL_PROJECTION || plan.children.empty() ||
	    !PlanContainsOperator(plan.children[0].get(), LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY)) {
		info.complete = true;
		return info;
	}
	auto &projection = plan.Cast<LogicalProjection>();
	DerivedOutputProjectionMap projections;
	CollectDerivedOutputProjections(*plan.children[0], projections);
	unordered_map<uint64_t, string> binding_to_column;
	vector<const Expression *> resolved_expressions;
	resolved_expressions.reserve(projection.expressions.size());
	for (idx_t i = 0; i < projection.expressions.size() && i < output_names.size(); i++) {
		vector<uint64_t> binding_path;
		auto resolved = ResolveDerivedOutputPassThrough(*projection.expressions[i], projections, &binding_path);
		if (!resolved) {
			return info;
		}
		resolved_expressions.push_back(resolved);
		if (resolved->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
			for (auto key : binding_path) {
				binding_to_column[key] = output_names[i];
			}
		}
	}
	for (idx_t i = 0; i < resolved_expressions.size(); i++) {
		auto &expression = *resolved_expressions[i];
		if (expression.expression_class == ExpressionClass::BOUND_COLUMN_REF ||
		    IncrementalTableNames::IsInternalColumn(output_names[i])) {
			continue;
		}
		string expression_sql;
		unordered_set<uint64_t> active_bindings;
		if (RenderDerivedOutputExpression(expression, binding_to_column, projections, active_bindings,
		                                  expression_sql)) {
			info.outputs.push_back({output_names[i], std::move(expression_sql)});
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Derived aggregate output '%s' = %s\n", output_names[i].c_str(),
			                    info.outputs.back().expression_sql.c_str());
		} else {
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Could not render derived aggregate output '%s'\n",
			                    output_names[i].c_str());
			return info;
		}
	}
	info.complete = true;
	return info;
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
	case ExpressionClass::BOUND_CASE: {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		string result = "CASE";
		for (auto &check : case_expr.case_checks) {
			result += " WHEN " + HavingExprToSQL(*check.when_expr, binding_to_alias) + " THEN " +
			          HavingExprToSQL(*check.then_expr, binding_to_alias);
		}
		return result + " ELSE " + HavingExprToSQL(*case_expr.else_expr, binding_to_alias) + " END";
	}
	case ExpressionClass::BOUND_FUNCTION: {
		auto &function = expr.Cast<BoundFunctionExpression>();
		vector<string> children;
		children.reserve(function.children.size());
		for (auto &child : function.children) {
			children.push_back(HavingExprToSQL(*child, binding_to_alias));
		}
		const auto &name = function.function.name;
		if (children.size() == 2 && (name == "+" || name == "-" || name == "*" || name == "/" || name == "%")) {
			return "(" + children[0] + " " + name + " " + children[1] + ")";
		}
		string result = name + "(";
		for (idx_t i = 0; i < children.size(); i++) {
			if (i > 0) {
				result += ", ";
			}
			result += children[i];
		}
		return result + ")";
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
		// expose openivm_having_N columns that are invisible in the outer output.
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

	// Collect the projection path from the view output down to the projection directly
	// above HAVING. A visible alias is safe only when every projection on this path
	// passes the aggregate output through unchanged.
	vector<LogicalProjection *> proj_chain;
	for (LogicalOperator *node = plan.get(); node && node != filter_node;) {
		if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			proj_chain.push_back(&node->Cast<LogicalProjection>());
		}
		if (node->children.empty()) {
			break;
		}
		node = (node->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE && node->children.size() >= 2)
		           ? node->children[1].get()
		           : node->children[0].get();
	}

	// Build binding → alias map from output expressions that are pure pass-throughs.
	// If an upper projection transforms an aggregate (for example COALESCE(sum, 0)),
	// HAVING must use a hidden raw aggregate column instead of the transformed alias.
	unordered_map<uint64_t, string> binding_to_alias;
	LogicalProjection *proj_ptr = nullptr;
	if (parent && parent->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		proj_ptr = &parent->Cast<LogicalProjection>();
		if (!proj_chain.empty() && proj_chain.back() == proj_ptr) {
			DerivedOutputProjectionMap projections;
			CollectDerivedOutputProjections(*plan, projections);
			auto *output_projection = proj_chain.front();
			for (idx_t i = 0; i < output_projection->expressions.size() && i < output_names.size(); i++) {
				auto *resolved = ResolveDerivedOutputPassThrough(*output_projection->expressions[i], projections);
				if (resolved && resolved->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					auto &column = resolved->Cast<BoundColumnRefExpression>();
					binding_to_alias[DerivedOutputBindingKey(column.binding)] = output_names[i];
				}
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
		bool added_hidden_having_column = false;
		idx_t hidden_start_idx = proj_ptr->expressions.size();
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
			string hidden_name = "openivm_having_" + std::to_string(next_hidden++);
			auto col_type = agg_child.expressions[b.second]->return_type;
			auto hidden_expr =
			    make_uniq<BoundColumnRefExpression>(hidden_name, col_type, ColumnBinding(b.first, b.second));
			hidden_expr->alias = hidden_name;
			proj_ptr->expressions.push_back(std::move(hidden_expr));
			output_names.push_back(hidden_name);
			binding_to_alias[key] = hidden_name;
			added_hidden_having_column = true;
		}
		if (added_hidden_having_column) {
			proj_ptr->ResolveOperatorTypes();
			// proj_ptr (the projection directly above the FILTER) is not necessarily the
			// view's output projection: e.g. `rollup AS (... GROUP BY ... HAVING ...)
			// SELECT ..., COALESCE(agg, 0) ... FROM rollup` plans as an outer projection
			// on top of the HAVING aggregate. Propagate the new hidden columns up through
			// every projection between proj_ptr and the root so they reach the data table
			// the user view's HAVING WHERE clause reads from. Transparent operators
			// (ORDER/LIMIT/DISTINCT/FILTER) pass column bindings through unchanged, so only
			// projections need extending.
			// proj_chain.back() == proj_ptr (it carries the freshly added hidden columns).
			// Walk upward (toward the root) re-exposing each hidden column at every layer.
			if (!proj_chain.empty() && proj_chain.back() == proj_ptr) {
				struct CarriedColumn {
					ColumnBinding binding;
					LogicalType type;
					string name;
				};
				vector<CarriedColumn> carried;
				for (idx_t i = hidden_start_idx; i < proj_ptr->expressions.size(); i++) {
					carried.push_back({ColumnBinding(proj_ptr->table_index, i), proj_ptr->expressions[i]->return_type,
					                   proj_ptr->expressions[i]->GetName()});
				}
				for (idx_t level = proj_chain.size() - 1; level-- > 0;) {
					auto *upper = proj_chain[level];
					vector<CarriedColumn> next_carried;
					for (auto &c : carried) {
						auto ref = make_uniq<BoundColumnRefExpression>(c.name, c.type, c.binding);
						ref->alias = c.name;
						next_carried.push_back(
						    {ColumnBinding(upper->table_index, upper->expressions.size()), c.type, c.name});
						upper->expressions.push_back(std::move(ref));
					}
					upper->ResolveOperatorTypes();
					carried = std::move(next_carried);
				}
			}
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
	if (plan) {
		plan->ResolveOperatorTypes();
	}

	OPENIVM_DEBUG_PRINT("[StripHavingFilter] Extracted HAVING predicate: %s\n", having_sql.c_str());
	return having_sql;
}

} // namespace duckdb
