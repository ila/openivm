#include "duckdb/function/window/rows_functions.hpp"
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
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/column_binding_replacer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
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
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
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
#include "duckdb/planner/operator/logical_set_operation.hpp"
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
		// A projection can drop DISTINCT keys or map them to duplicate output rows.
		// Its child's count is internal state, not an output of the outer query.
		for (auto &child : node->children) {
			RewriteDistinct(context, binder, child, false);
		}
		return false;
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
	if (child->type == LogicalOperatorType::LOGICAL_UNION) {
		// DuckDB represents UNION DISTINCT with both a DISTINCT wrapper and
		// a set-operation flag. The counting aggregate now owns deduplication;
		// retaining the flag also deduplicates hidden per-source markers and
		// prevents the UNION ALL delta rule from compiling this input.
		child->Cast<LogicalSetOperation>().setop_all = true;
	}
	child->ResolveOperatorTypes();
	auto child_bindings = child->GetColumnBindings();
	auto child_types = child->types;

	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(MakeCountStar(openivm::DISTINCT_COUNT_COL));
	vector<unique_ptr<Expression>> groups;
	for (idx_t i = 0; i < child_bindings.size(); i++) {
		groups.push_back(make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]));
	}
	auto agg_node = MakeGroupedAggregate(binder, std::move(groups), std::move(aggregates), std::move(child));

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

static unordered_set<TableIndex> OutputTableIndexes(LogicalOperator &op) {
	unordered_set<TableIndex> result;
	for (auto &binding : op.GetColumnBindings()) {
		result.insert(binding.table_index);
	}
	return result;
}

static unordered_set<TableIndex> ExpressionTableIndexes(const Expression &expr) {
	unordered_set<TableIndex> result;
	LogicalJoin::GetExpressionBindings(expr, result);
	return result;
}

static bool ExpressionUsesOnly(const Expression &expr, const unordered_set<TableIndex> &table_indexes,
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
                                             const unordered_set<TableIndex> &source_bindings,
                                             const unordered_set<TableIndex> &delim_bindings,
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
	is_not_null->GetChildrenMutable().push_back(std::move(child));
	return std::move(is_not_null);
}

static bool IsDistinctCorrelationDomain(const LogicalOperator &op) {
	auto *node = &op;
	while (node->type == LogicalOperatorType::LOGICAL_PROJECTION && node->children.size() == 1) {
		node = node->children[0].get();
	}
	if (node->type == LogicalOperatorType::LOGICAL_DISTINCT) {
		return true;
	}
	if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	auto &aggregate = node->Cast<LogicalAggregate>();
	return aggregate.expressions.empty() && !aggregate.groups.empty();
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
	auto replace_probe = [&](unique_ptr<LogicalOperator> source, vector<unique_ptr<Expression>> source_filters,
	                         vector<unique_ptr<Expression>> projection_expressions) {
		if (!source_filters.empty()) {
			auto source_filter = make_uniq<LogicalFilter>();
			source_filter->expressions = std::move(source_filters);
			source_filter->children.push_back(std::move(source));
			source_filter->ResolveOperatorTypes();
			source = std::move(source_filter);
		}

		auto projection = make_uniq<LogicalProjection>(old_projection.table_index, std::move(projection_expressions));
		projection->children.push_back(std::move(source));
		projection->ResolveOperatorTypes();

		vector<unique_ptr<Expression>> distinct_targets;
		auto projection_bindings = projection->GetColumnBindings();
		for (idx_t i = 0; i < projection_bindings.size(); i++) {
			distinct_targets.push_back(
			    make_uniq<BoundColumnRefExpression>(projection->types[i], projection_bindings[i]));
		}
		auto distinct = make_uniq<LogicalDistinct>(std::move(distinct_targets), DistinctType::DISTINCT);
		distinct->children.push_back(std::move(projection));
		distinct->ResolveOperatorTypes();

		semi_anti.children[probe_child_idx] = std::move(distinct);
		semi_anti.ResolveOperatorTypes();
		OPENIVM_DEBUG_PRINT("[PlanRewrite] Rewrote SEMI/ANTI probe side to distinct source keys\n");
		return true;
	};

	// DuckDB 2.0 can push the correlated predicate into a SEMI or INNER join directly:
	// projection(source keys) -> JOIN(source, distinct(correlation domain)). The
	// domain restriction is redundant for the enclosing SEMI/ANTI join; keys not
	// present on the preserved side cannot affect its result.
	if (old_projection.children[0] &&
	    old_projection.children[0]->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &domain_join = old_projection.children[0]->Cast<LogicalComparisonJoin>();
		if ((domain_join.join_type == JoinType::SEMI || domain_join.join_type == JoinType::INNER) &&
		    !domain_join.HasArbitraryConditions() && domain_join.children.size() == 2 &&
		    IsDistinctCorrelationDomain(*domain_join.children[1])) {
			auto source_bindings = OutputTableIndexes(*domain_join.children[0]);
			auto domain_bindings = OutputTableIndexes(*domain_join.children[1]);
			vector<SemiAntiKeyReplacement> replacements;
			for (auto &condition : domain_join.conditions) {
				if (!condition.LeftReference() || !condition.RightReference() ||
				    !TryCollectSemiAntiKeyReplacement(condition.GetComparisonType(), *condition.LeftReference(),
				                                      *condition.RightReference(), source_bindings, domain_bindings,
				                                      replacements)) {
					return false;
				}
			}
			if (replacements.empty()) {
				return false;
			}
			vector<bool> used_replacements(replacements.size(), false);
			vector<unique_ptr<Expression>> projection_expressions;
			for (auto &expression : old_projection.expressions) {
				if (!expression) {
					return false;
				}
				bool replaced = false;
				for (idx_t i = 0; i < replacements.size(); i++) {
					if (Expression::Equals(*expression, *replacements[i].delim_expr)) {
						used_replacements[i] = true;
						projection_expressions.push_back(replacements[i].source_expr->Copy());
						replaced = true;
						break;
					}
				}
				if (replaced) {
					continue;
				}
				if (!ExpressionUsesOnly(*expression, source_bindings, true)) {
					return false;
				}
				projection_expressions.push_back(expression->Copy());
			}
			vector<unique_ptr<Expression>> source_filters;
			for (idx_t i = 0; i < replacements.size(); i++) {
				if (!used_replacements[i]) {
					return false;
				}
				if (replacements[i].null_reject_source) {
					source_filters.push_back(BuildIsNotNull(replacements[i].source_expr->Copy()));
				}
			}
			return replace_probe(std::move(domain_join.children[0]), std::move(source_filters),
			                     std::move(projection_expressions));
		}
	}
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
	if (IsDistinctCorrelationDomain(*inner.children[0])) {
		delim_child_idx = 0;
	} else if (IsDistinctCorrelationDomain(*inner.children[1])) {
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
		if (inner_join.join_type != JoinType::INNER || inner_join.HasArbitraryConditions()) {
			return false;
		}
		for (auto &condition : inner_join.conditions) {
			if (!condition.LeftReference() || !condition.RightReference() ||
			    !TryCollectSemiAntiKeyReplacement(condition.GetComparisonType(), *condition.LeftReference(),
			                                      *condition.RightReference(), source_bindings, delim_bindings,
			                                      replacements)) {
				return false;
			}
		}
	}

	vector<unique_ptr<Expression>> source_filters;
	for (auto &expr : filter.expressions) {
		if (!expr) {
			return false;
		}
		if (expr->GetExpressionType() == ExpressionType::COMPARE_EQUAL ||
		    expr->GetExpressionType() == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			auto &comparison = expr->Cast<BoundFunctionExpression>();
			if (TryCollectSemiAntiKeyReplacement(
			        comparison.GetExpressionType(), BoundComparisonExpression::Left(comparison),
			        BoundComparisonExpression::Right(comparison), source_bindings, delim_bindings, replacements)) {
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

	return replace_probe(std::move(inner.children[source_child_idx]), std::move(source_filters),
	                     std::move(new_projection_exprs));
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

static bool IsMarkColumnRef(const Expression &expr, TableIndex mark_index) {
	if (expr.GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col_ref = expr.Cast<BoundColumnRefExpression>();
	return col_ref.Binding().table_index == mark_index && col_ref.Binding().column_index == 0;
}

static bool IsNotMarkColumnRef(const Expression &expr, TableIndex mark_index) {
	if (expr.GetExpressionType() != ExpressionType::OPERATOR_NOT) {
		return false;
	}
	auto &op = expr.Cast<BoundOperatorExpression>();
	return op.GetChildren().size() == 1 && op.GetChildren()[0] && IsMarkColumnRef(*op.GetChildren()[0], mark_index);
}

static bool MarkJoinCanBecomeSemiAnti(LogicalOperator &op) {
	if (op.type != LogicalOperatorType::LOGICAL_DELIM_JOIN && op.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return false;
	}
	auto &join = op.Cast<LogicalComparisonJoin>();
	if (join.join_type != JoinType::MARK || join.conditions.empty() || join.HasArbitraryConditions() ||
	    !join.convert_mark_to_semi) {
		return false;
	}
	for (auto &condition : join.conditions) {
		// Correlated EXISTS/NOT EXISTS MARK joins use null-safe delimiter comparisons.
		// That guarantees the mark is TRUE/FALSE, not NULL, so filtering on mark is
		// equivalent to SEMI/ANTI. Other MARK joins are left untouched.
		if (condition.GetComparisonType() != ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return false;
		}
	}
	return true;
}

static void CollectConvertibleMarkIndexes(LogicalOperator &op, unordered_set<TableIndex> &mark_indexes) {
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

static bool IsConvertibleMarkPredicate(const Expression &expr, const unordered_set<TableIndex> &mark_indexes) {
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

	unordered_set<TableIndex> mark_indexes;
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
void InlineMaterializedCteRefs(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan) {
	if (!plan) {
		return;
	}
	// Process this node's children first (bottom-up). Each child may itself be
	// a MATERIALIZED_CTE — handle each independently.
	for (auto &c : plan->children) {
		InlineMaterializedCteRefs(context, binder, c);
	}
	if (plan->type != LogicalOperatorType::LOGICAL_MATERIALIZED_CTE || plan->children.size() != 2) {
		return;
	}
	auto &cte = plan->Cast<LogicalMaterializedCTE>();
	auto cte_index = cte.table_index;
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
					col_ref->SetAlias(Identifier(ref.bound_columns[i]));
				}
				proj_exprs.push_back(std::move(col_ref));
			}
			auto wrapper = make_uniq<LogicalProjection>(ref.table_index, std::move(proj_exprs));
			wrapper->children.push_back(std::move(renumbered.op));
			wrapper->ResolveOperatorTypes();
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Inlined CTE_REF cte_index=%lu "
			                    "(table_index=%lu, %zu cols)\n",
			                    (unsigned long)ref.cte_index.index, (unsigned long)ref.table_index.index, (size_t)cols);
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
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Collapsed MATERIALIZED_CTE wrapper (cte_index=%lu)\n",
	                    (unsigned long)cte_index.index);
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
	if (expr->GetExpressionType() == ExpressionType::BOUND_COLUMN_REF) {
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		auto it = consts.find(bcr.Binding());
		if (it != consts.end()) {
			expr = make_uniq<BoundConstantExpression>(it->second.DefaultCastAs(bcr.GetReturnType()));
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
	if (expr.GetExpressionType() == ExpressionType::CASE_EXPR) {
		auto &case_expr = expr.Cast<BoundCaseExpression>();
		for (auto &check : case_expr.CaseChecks()) {
			Value cond;
			if (!TryFoldConstExpr(context, *check.when_expr, cond)) {
				return false;
			}
			if (!cond.IsNull() && BooleanValue::Get(cond)) {
				return TryFoldConstExpr(context, *check.then_expr, result);
			}
		}
		return TryFoldConstExpr(context, *case_expr.ElseExpression(), result);
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
                                   const unordered_map<TableIndex, LogicalOperator *> &cte_defs) {
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
			out[ColumnBinding(TableIndex(ref.table_index), ProjectionIndex(i))] = vit->second;
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
			out[ColumnBinding(TableIndex(proj.table_index), ProjectionIndex(i))] = std::move(v);
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
			if (agg.expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
				return false;
			}
			auto &bound_agg = agg.expressions[i]->Cast<BoundAggregateExpression>();
			if (bound_agg.GetFilter()) {
				return false;
			}
			Value v;
			if (bound_agg.Function().GetName().GetIdentifierName() == "count_star" && bound_agg.GetChildren().empty()) {
				v = Value::BIGINT(1); // constant subtree is exactly one row
			} else if (bound_agg.Function().GetName().GetIdentifierName() == "first" &&
			           bound_agg.GetChildren().size() == 1) {
				if (!SubstituteAndFold(context, *bound_agg.GetChildren()[0], child, v)) {
					return false;
				}
			} else {
				return false; // any other aggregate: not a constant scalar guard
			}
			out[ColumnBinding(TableIndex(agg.aggregate_index), ProjectionIndex(i))] = std::move(v);
		}
		return true;
	}
	default:
		return false;
	}
}

// Replaces a set of column bindings with constants everywhere in a plan.
class ConstantBindingReplacer : public LogicalOperatorVisitor {
public:
	explicit ConstantBindingReplacer(const column_binding_map_t<Value> &values) : values(values) {
	}
	using LogicalOperatorVisitor::VisitReplace;
	unique_ptr<Expression> VisitReplace(BoundColumnRefExpression &expr, unique_ptr<Expression> *expr_ptr) override {
		auto entry = values.find(expr.Binding());
		if (entry != values.end()) {
			return make_uniq<BoundConstantExpression>(entry->second.DefaultCastAs(expr.GetReturnType()));
		}
		return nullptr;
	}

private:
	const column_binding_map_t<Value> &values;
};

// Find the first CROSS_PRODUCT (bottom-up) with a provably-constant single-row
// side, fold that side's output columns to constants plan-wide, and drop the
// cross product. Returns true if a fold was applied.
static bool FoldConstantScalarSubqueriesOnce(ClientContext &context, unique_ptr<LogicalOperator> &root,
                                             unique_ptr<LogicalOperator> &node,
                                             const unordered_map<TableIndex, LogicalOperator *> &cte_defs) {
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
		ConstantBindingReplacer replacer(values);
		replacer.VisitOperator(*root);
		for (auto &b : bindings) {
			auto value = values.find(b);
			D_ASSERT(value != values.end());
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Folded constant scalar subquery "
			                    "binding (%lu.%lu) = %s\n",
			                    (unsigned long)b.table_index.index, (unsigned long)b.column_index.GetIndex(),
			                    value->second.ToString().c_str());
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
	unordered_map<TableIndex, LogicalOperator *> cte_defs;
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
	passthrough->SetAlias(Identifier(alias));
	proj.expressions.push_back(std::move(passthrough));
	// Projection types depend only on the bound expressions, not a recursive
	// resolution of its entire child plan for every appended helper column.
	proj.types.clear();
	for (auto &expression : proj.expressions) {
		proj.types.push_back(expression->GetReturnType());
	}
	return ColumnBinding(TableIndex(proj.table_index), ProjectionIndex(proj.expressions.size() - 1));
}

static ColumnBinding ExposeBindingInProjection(LogicalProjection &proj, const ColumnBinding &binding,
                                               const LogicalType &type, const string &alias, bool reuse_existing,
                                               bool &appended) {
	if (reuse_existing) {
		for (idx_t i = 0; i < proj.expressions.size(); i++) {
			auto &expression = *proj.expressions[i];
			if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    expression.Cast<BoundColumnRefExpression>().Binding() == binding) {
				return ColumnBinding(TableIndex(proj.table_index), ProjectionIndex(i));
			}
		}
	}
	appended = true;
	return AppendProjectionPassthrough(proj, binding, type, alias);
}

bool PropagateHiddenBindingThroughProjectionPath(vector<LogicalProjection *> &projection_path, ColumnBinding binding,
                                                 LogicalType type, const string &alias, bool reuse_existing) {
	bool appended = false;
	for (auto it = projection_path.rbegin(); it != projection_path.rend(); ++it) {
		binding = ExposeBindingInProjection(**it, binding, type, alias, reuse_existing, appended);
		type = (*it)->expressions[binding.column_index]->GetReturnType();
	}
	return appended;
}

OutputSpine FindOutputSpine(LogicalOperator *root, const std::function<bool(const LogicalOperator &)> &pass_through) {
	OutputSpine spine;
	auto *node = root;
	while (node) {
		LogicalOperator *next = nullptr;
		if (node->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
			if (pass_through(*node) && node->children.size() >= 2) {
				next = node->children[1].get();
			}
		} else if ((node->type == LogicalOperatorType::LOGICAL_PROJECTION || pass_through(*node)) &&
		           node->children.size() == 1) {
			next = node->children[0].get();
		}
		if (!next) {
			break;
		}
		if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			spine.projections.push_back(&node->Cast<LogicalProjection>());
		}
		spine.parent = node;
		node = next;
	}
	spine.bottom = node;
	return spine;
}

LogicalAggregate *SpineAggregate(const OutputSpine &spine) {
	auto *node = spine.bottom;
	if (node && node->type == LogicalOperatorType::LOGICAL_FILTER && node->children.size() == 1) {
		node = node->children[0].get();
	}
	if (!node || node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return nullptr;
	}
	return &node->Cast<LogicalAggregate>();
}

void InjectHiddenGroupKeys(unique_ptr<LogicalOperator> &plan) {
	auto spine = FindOutputSpine(plan.get(), [](const LogicalOperator &op) {
		return op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE || op.type == LogicalOperatorType::LOGICAL_FILTER ||
		       op.type == LogicalOperatorType::LOGICAL_ORDER_BY || op.type == LogicalOperatorType::LOGICAL_LIMIT ||
		       op.type == LogicalOperatorType::LOGICAL_TOP_N;
	});
	if (!spine.bottom || spine.bottom->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY ||
	    spine.projections.empty()) {
		return;
	}
	auto &aggregate = spine.bottom->Cast<LogicalAggregate>();
	for (idx_t i = 0; i < aggregate.groups.size(); i++) {
		PropagateHiddenBindingThroughProjectionPath(
		    spine.projections, ColumnBinding(TableIndex(aggregate.group_index), ProjectionIndex(i)),
		    aggregate.groups[i]->GetReturnType(), "openivm_group_key_" + to_string(i), true);
	}
	plan->ResolveOperatorTypes();
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
	auto spine = FindOutputSpine(plan.get(), [](const LogicalOperator &op) {
		return op.type == LogicalOperatorType::LOGICAL_ORDER_BY || op.type == LogicalOperatorType::LOGICAL_LIMIT ||
		       op.type == LogicalOperatorType::LOGICAL_TOP_N;
	});
	auto *agg_ptr = SpineAggregate(spine);
	if (!agg_ptr || spine.projections.empty()) {
		return;
	}
	auto &agg = *agg_ptr;
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
		if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
			continue;
		}
		auto &bound = expr->Cast<BoundAggregateExpression>();
		// True COUNT(*): no-arg, no filter — always equals the group cardinality.
		if (bound.Function().GetName().GetIdentifierName() == "count_star" && bound.GetChildren().empty() &&
		    !bound.GetFilter()) {
			return;
		}
		// Already-injected reliable hidden count.
		if (bound.GetAlias().GetIdentifierName() == openivm::COUNT_STAR_COL ||
		    bound.GetAlias().GetIdentifierName() == openivm::DISTINCT_COUNT_COL) {
			return;
		}
		if (bound.Function().GetName().GetIdentifierName() == "arg_min" ||
		    bound.Function().GetName().GetIdentifierName() == "arg_max") {
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
	auto count_expr = MakeCountStar(openivm::COUNT_STAR_COL);
	auto count_type = count_expr->GetReturnType();
	idx_t new_agg_idx = agg.expressions.size();
	agg.expressions.push_back(std::move(count_expr));
	agg.ResolveOperatorTypes();

	ColumnBinding count_binding {TableIndex(agg.aggregate_index), ProjectionIndex(new_agg_idx)};
	PropagateHiddenBindingThroughProjectionPath(spine.projections, count_binding, count_type, openivm::COUNT_STAR_COL);
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
		if (IsHiddenAggregateAlias(expr->GetAlias().GetIdentifierName())) {
			already_present.insert(expr->GetAlias().GetIdentifierName());
		}
	}

	plan->children[0]->ResolveOperatorTypes();
	auto child_types = plan->children[0]->types;
	auto child_bindings = plan->children[0]->GetColumnBindings();

	bool added = false;
	for (idx_t i = 0; i < child_proj.expressions.size(); i++) {
		const string &child_alias = child_proj.expressions[i]->GetAlias().GetIdentifierName();
		if (!IsHiddenAggregateAlias(child_alias) || already_present.count(child_alias)) {
			continue;
		}
		if (i >= child_bindings.size() || i >= child_types.size()) {
			continue;
		}
		auto bcr = make_uniq<BoundColumnRefExpression>(child_types[i], child_bindings[i]);
		bcr->SetAlias(Identifier(child_alias));
		proj.expressions.push_back(std::move(bcr));
		added = true;
		OPENIVM_DEBUG_PRINT("[PropagateHidden] Added '%s' to parent projection "
		                    "(table_index=%llu)\n",
		                    child_alias.c_str(), (unsigned long long)proj.table_index.index);
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
	if (expr->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return;
	}
	auto &ref = expr->Cast<BoundColumnRefExpression>();
	binding = ref.Binding();
	type = ref.GetReturnType();
}

// Projection aliases form a DAG. Cache by operator as well as binding so a
// result inside a preserved subtree cannot leak across a nullable join boundary.
using ProjectionKeyBindingCache = unordered_map<LogicalOperator *, column_binding_map_t<bool>>;

static bool IsPreservedKeyExpression(LogicalOperator &plan, Expression &expression, ProjectionKeyBindingCache &cache);

static bool IsPreservedKeyBinding(LogicalOperator &plan, const ColumnBinding &binding,
                                  ProjectionKeyBindingCache &cache) {
	auto &bindings = cache[&plan];
	auto cached = bindings.find(binding);
	if (cached != bindings.end()) {
		return cached->second;
	}

	bool preserved = false;
	if (plan.type == LogicalOperatorType::LOGICAL_PROJECTION &&
	    binding.table_index == plan.Cast<LogicalProjection>().table_index) {
		auto &projection = plan.Cast<LogicalProjection>();
		preserved =
		    IsPreservedKeyExpression(*projection.children[0], *projection.expressions[binding.column_index], cache);
	} else if (plan.type == LogicalOperatorType::LOGICAL_GET) {
		auto columns = plan.GetColumnBindings();
		preserved = std::find(columns.begin(), columns.end(), binding) != columns.end();
	} else {
		auto *join = dynamic_cast<LogicalJoin *>(&plan);
		for (idx_t child_idx = 0; child_idx < plan.children.size(); child_idx++) {
			if (join && ((join->join_type == JoinType::LEFT && child_idx != 0) ||
			             (join->join_type == JoinType::RIGHT && child_idx != 1) || join->join_type == JoinType::OUTER ||
			             (join->join_type == JoinType::SINGLE && child_idx != 0))) {
				continue;
			}
			if (IsPreservedKeyBinding(*plan.children[child_idx], binding, cache)) {
				preserved = true;
				break;
			}
		}
	}
	bindings.emplace(binding, preserved);
	return preserved;
}

static bool IsPreservedKeyExpression(LogicalOperator &plan, Expression &expression, ProjectionKeyBindingCache &cache) {
	if (!expression.IsConsistent()) {
		return false;
	}
	switch (expression.GetExpressionClass()) {
	case ExpressionClass::BOUND_COLUMN_REF: {
		auto &column = expression.Cast<BoundColumnRefExpression>();
		return column.Depth() == 0 && IsPreservedKeyBinding(plan, column.Binding(), cache);
	}
	case ExpressionClass::BOUND_CONSTANT:
		return true;
	case ExpressionClass::BOUND_FUNCTION:
	case ExpressionClass::BOUND_OPERATOR:
	case ExpressionClass::BOUND_CASE:
	case ExpressionClass::BOUND_CONJUNCTION: {
		bool preserved = true;
		ExpressionIterator::EnumerateChildren(expression, [&](Expression &child) {
			preserved = preserved && IsPreservedKeyExpression(plan, child, cache);
		});
		return preserved;
	}
	default:
		return false;
	}
}

static LogicalComparisonJoin *FindProjectionKeyJoin(LogicalOperator &plan, ProjectionKeyBindingCache &cache) {
	if (plan.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = plan.Cast<LogicalComparisonJoin>();
		if (IsOuterJoin(join.join_type) && !join.conditions.empty()) {
			if (join.join_type == JoinType::OUTER) {
				return &join;
			}
			auto preserved_child = join.join_type == JoinType::RIGHT ? 1 : 0;
			auto &key = preserved_child == 0 ? join.conditions[0].LeftReference() : join.conditions[0].RightReference();
			if (key->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
			    IsPreservedKeyBinding(*join.children[preserved_child], key->Cast<BoundColumnRefExpression>().Binding(),
			                          cache)) {
				return &join;
			}
			// Retain a usable outer key to keep its selective refresh scope. Only
			// descend when that key cannot be propagated or depends on nullable inputs.
			return FindProjectionKeyJoin(*join.children[preserved_child], cache);
		}
	}
	for (auto &child : plan.children) {
		if (auto join = FindProjectionKeyJoin(*child, cache)) {
			return join;
		}
	}
	return nullptr;
}

LogicalComparisonJoin *FindProjectionKeyJoin(LogicalOperator &plan) {
	ProjectionKeyBindingCache cache;
	return FindProjectionKeyJoin(plan, cache);
}

static OuterJoinBindings FindFirstOuterJoinBindings(LogicalOperator *plan, bool has_aggregate) {
	OuterJoinBindings bindings;
	if (!has_aggregate) {
		// A downstream lookup key can change when an earlier optional row is
		// removed. Both old and replacement rows must share the refresh scope.
		plan = FindProjectionKeyJoin(*plan);
		if (!plan) {
			return bindings;
		}
	}

	std::function<bool(LogicalOperator *)> find = [&](LogicalOperator *node) {
		if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = node->Cast<LogicalComparisonJoin>();
			if (IsOuterJoin(join.join_type) && !join.conditions.empty()) {
				auto &condition = join.conditions[0];
				bindings.found = true;
				bindings.is_full_outer = join.join_type == JoinType::OUTER;

				auto &preserved_key =
				    join.join_type == JoinType::RIGHT ? condition.RightReference() : condition.LeftReference();
				ReadColumnRefBinding(preserved_key, bindings.preserved_key_binding, bindings.preserved_key_type);
				ReadColumnRefBinding(condition.RightReference(), bindings.right_key_binding, bindings.right_key_type);

				auto &null_side =
				    join.join_type == JoinType::RIGHT ? condition.LeftReference() : condition.RightReference();
				ReadColumnRefBinding(null_side, bindings.null_side_binding, bindings.null_side_type);
				ReadColumnRefBinding(condition.LeftReference(), bindings.left_side_binding, bindings.left_side_type);
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

static void PropagateBindingThroughOperatorPath(ClientContext &context, Binder &binder,
                                                unique_ptr<LogicalOperator> &plan, ColumnBinding &binding,
                                                LogicalType &type, const string &alias = "") {
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
			bool appended = false;
			current =
			    ExposeBindingInProjection(entry.op->Cast<LogicalProjection>(), current, type, alias, true, appended);
		} else if (entry.op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
		           StringUtil::StartsWith(alias, openivm::FULL_SCOPE_PREFIX)) {
			auto &aggregate = entry.op->Cast<LogicalAggregate>();
			// Presence is an existential property of the group. Grouping by an
			// arm's marker would split duplicates contributed by different arms.
			D_ASSERT(type == LogicalType::BOOLEAN);
			auto present = MakeAggregate(context, "bool_or", make_uniq<BoundColumnRefExpression>(type, current), alias);
			current = ColumnBinding(aggregate.aggregate_index, ProjectionIndex(aggregate.expressions.size()));
			aggregate.expressions.push_back(std::move(present));
			aggregate.ResolveOperatorTypes();
		} else if (entry.op->type == LogicalOperatorType::LOGICAL_UNION) {
			auto &setop = entry.op->Cast<LogicalSetOperation>();
			auto bindings = setop.children[entry.child_idx]->GetColumnBindings();
			auto position = std::find(bindings.begin(), bindings.end(), current);
			D_ASSERT(position != bindings.end());
			auto column = idx_t(position - bindings.begin());
			if (column >= setop.column_count) {
				// A new hidden value belongs only to this arm. Other arms must
				// carry NULL, rather than inherit the arm's presence marker.
				for (idx_t side = 0; side < setop.children.size(); side++) {
					auto &child = setop.children[side];
					child->ResolveOperatorTypes();
					auto child_bindings = child->GetColumnBindings();
					vector<unique_ptr<Expression>> expressions;
					for (idx_t i = 0; i < setop.column_count; i++) {
						expressions.push_back(make_uniq<BoundColumnRefExpression>(child->types[i], child_bindings[i]));
					}
					unique_ptr<Expression> value;
					if (side == entry.child_idx) {
						value = make_uniq<BoundColumnRefExpression>(Identifier(alias), type, current);
					} else {
						value = make_uniq<BoundConstantExpression>(Value(type));
						value->SetAlias(Identifier(alias));
					}
					expressions.push_back(std::move(value));
					auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
					projection->children.push_back(std::move(child));
					child = std::move(projection);
				}
				column = setop.column_count++;
			}
			current = ColumnBinding(setop.table_index, ProjectionIndex(column));
			setop.ResolveOperatorTypes();
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
		} else if (entry.op->type == LogicalOperatorType::LOGICAL_UNION ||
		           entry.op->type == LogicalOperatorType::LOGICAL_EXCEPT ||
		           entry.op->type == LogicalOperatorType::LOGICAL_INTERSECT) {
			// Set operations expose a fresh table index. A binding from either input
			// remains visible only through its positional set-operation output; carrying
			// the branch-local binding above this boundary produces invalid SQL when the
			// hidden outer-join key is projected at the root.
			auto child_bindings = entry.op->children[entry.child_idx]->GetColumnBindings();
			idx_t output_idx = DConstants::INVALID_INDEX;
			for (idx_t i = 0; i < child_bindings.size(); i++) {
				if (child_bindings[i] == current) {
					output_idx = i;
					break;
				}
			}
			if (output_idx == DConstants::INVALID_INDEX) {
				throw InternalException("OpenIVM could not propagate hidden binding through set-operation input");
			}
			entry.op->ResolveOperatorTypes();
			auto output_bindings = entry.op->GetColumnBindings();
			if (output_idx >= output_bindings.size() || output_idx >= entry.op->types.size()) {
				throw InternalException("OpenIVM set-operation output is narrower than its hidden binding input");
			}
			current = output_bindings[output_idx];
			type = entry.op->types[output_idx];
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Remapped hidden binding through set operation at column %llu\n",
			                    (unsigned long long)output_idx);
		}
		// JOINs and other transparent nodes already pass child bindings through.
	}
	binding = current;
}

/// Add openivm_left_key (and openivm_right_key for FULL OUTER) projection at
/// the top of the plan.
static void RewriteLeftJoinKey(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                               const OuterJoinBindings &outer_join) {
	bool is_full_outer = outer_join.is_full_outer;
	auto key_binding = outer_join.preserved_key_binding;
	auto key_type = outer_join.preserved_key_type;
	auto right_key_binding = outer_join.right_key_binding;
	auto right_key_type = outer_join.right_key_type;

	// Outer-join key bindings originate inside the join subtree. User projections,
	// filters with projection maps, and CTE/projection stacks can hide either key
	// before the top of the plan. Push both keys through the operator path first,
	// then build one final projection that exposes user columns plus hidden keys.
	PropagateBindingThroughOperatorPath(context, binder, plan, key_binding, key_type);
	if (is_full_outer) {
		PropagateBindingThroughOperatorPath(context, binder, plan, right_key_binding, right_key_type);
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
	auto proj_table_index = binder.GenerateTableIndex();
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

	// Only applies to aggregate plans (PROJECTION → [PROJECTION ...] → AGGREGATE → ...).
	// For SIMPLE_PROJECTION outer JOINs, match count isn't needed (partial recompute via keys).
	auto spine = FindOutputSpine(plan.get(), [](const LogicalOperator &) { return false; });
	auto &projection_path = spine.projections;
	if (projection_path.empty() || !spine.bottom ||
	    spine.bottom->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return;
	}
	auto *agg_search = spine.bottom;
	auto &agg = agg_search->Cast<LogicalAggregate>();

	// Add COUNT(null_side_key) as openivm_match_count (tracks right-side matches for LEFT/OUTER).
	idx_t match_count_idx = agg.expressions.size();
	agg.expressions.push_back(MakeAggregate(context, "count",
	                                        make_uniq<BoundColumnRefExpression>(null_side_type, null_side_binding),
	                                        openivm::MATCH_COUNT_COL));

	// For FULL OUTER: add COUNT(left_key) as openivm_right_match_count (tracks left-side matches).
	idx_t right_match_count_idx = 0;
	if (is_full_outer) {
		right_match_count_idx = agg.expressions.size();
		agg.expressions.push_back(MakeAggregate(context, "count",
		                                        make_uniq<BoundColumnRefExpression>(left_side_type, left_side_binding),
		                                        openivm::RIGHT_MATCH_COUNT_COL));
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

static ColumnBinding AddScopePresence(Binder &binder, unique_ptr<LogicalOperator> &plan,
                                      unique_ptr<LogicalOperator> &child, const string &name,
                                      const Expression *key = nullptr) {
	child->ResolveOperatorTypes();
	auto bindings = child->GetColumnBindings();
	vector<unique_ptr<Expression>> expressions;
	for (idx_t i = 0; i < bindings.size(); i++) {
		expressions.push_back(make_uniq<BoundColumnRefExpression>(child->types[i], bindings[i]));
	}
	auto present = make_uniq<BoundConstantExpression>(Value::BOOLEAN(true));
	present->SetAlias(Identifier(name));
	expressions.push_back(std::move(present));
	if (key) {
		auto copy = key->Copy();
		copy->SetAlias(Identifier(name + "_key"));
		expressions.push_back(std::move(copy));
	}
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(expressions));
	projection->children.push_back(std::move(child));
	ColumnBinding presence(projection->table_index, ProjectionIndex(bindings.size()));
	ColumnBindingReplacer replacer;
	for (idx_t i = 0; i < bindings.size(); i++) {
		replacer.replacement_bindings.emplace_back(bindings[i],
		                                           ColumnBinding(projection->table_index, ProjectionIndex(i)));
	}
	replacer.stop_operator = projection.get();
	child = std::move(projection);
	replacer.VisitOperator(*plan);
	return presence;
}

static void RewriteOuterJoinSupport(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                                    bool has_aggregate) {
	auto outer_join = FindFirstOuterJoinBindings(plan.get(), has_aggregate);
	if (!outer_join.found) {
		return;
	}

	if (has_aggregate) {
		RewriteLeftJoinMatchCount(context, binder, plan, outer_join);
		return;
	}

	RewriteLeftJoinKey(context, binder, plan, outer_join);

	// Nested outer joins can change an ancestor's computed key when they start
	// NULL-padding. Presence columns let key discovery retain only rows from
	// the affected input, excluding unrelated rows padded by ancestor joins.
	auto *selected = FindProjectionKeyJoin(*plan);
	if (!selected || selected->join_type != JoinType::OUTER) {
		return;
	}
	vector<LogicalComparisonJoin *> nested_outer;
	std::function<void(LogicalOperator &)> collect = [&](LogicalOperator &node) {
		if (node.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto &join = node.Cast<LogicalComparisonJoin>();
			if (&join != selected && IsOuterJoin(join.join_type)) {
				nested_outer.push_back(&join);
			}
		}
		for (auto &child : node.children) {
			collect(*child);
		}
	};
	collect(*selected);
	if (!nested_outer.empty()) {
		idx_t source_index = 0;
		for (auto &input : selected->children) {
			vector<std::pair<ColumnBinding, string>> sources;
			std::function<void(unique_ptr<LogicalOperator> &, bool)> mark_inputs =
			    [&](unique_ptr<LogicalOperator> &node, bool in_scope) {
				    if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
				        IsOuterJoin(node->Cast<LogicalComparisonJoin>().join_type)) {
					    in_scope = true;
				    }
				    if (in_scope && node->type == LogicalOperatorType::LOGICAL_GET) {
					    auto name = string(openivm::FULL_SCOPE_PREFIX) + "source_" + to_string(source_index++);
					    sources.emplace_back(AddScopePresence(binder, plan, node, name), name);
					    return;
				    }
				    for (auto &child : node->children) {
					    mark_inputs(child, in_scope);
				    }
			    };
			mark_inputs(input, false);
			for (auto &source : sources) {
				LogicalType type = LogicalType::BOOLEAN;
				// Source participation is needed inside the scoped input only;
				// it is not an extra persisted output of the materialized view.
				PropagateBindingThroughOperatorPath(context, binder, input, source.first, type, source.second);
			}
		}
	}
	for (idx_t index = 0; index < nested_outer.size(); index++) {
		for (idx_t side = 0; side < 2; side++) {
			auto &child = nested_outer[index]->children[side];
			string name = string(openivm::FULL_SCOPE_PREFIX) + to_string(index) + (side == 0 ? "_left" : "_right");
			auto &condition = nested_outer[index]->conditions[0];
			auto presence = AddScopePresence(
			    binder, plan, child, name, (side == 0 ? condition.LeftReference() : condition.RightReference()).get());
			LogicalType type = LogicalType::BOOLEAN;
			PropagateBindingThroughOperatorPath(context, binder, plan, presence, type, name);
			auto &root = plan->Cast<LogicalProjection>();
			D_ASSERT(presence.table_index == root.table_index);
			root.expressions[presence.column_index]->SetAlias(Identifier(name));
		}
	}
	if (!nested_outer.empty()) {
		OPENIVM_DEBUG_PRINT("[PlanRewrite] Added presence columns for %zu nested outer JOIN scopes\n",
		                    nested_outer.size());
	}
}

struct PlanRewriteContext {
	ClientContext &context;
	Binder &binder;
	unique_ptr<LogicalOperator> &plan;
	vector<string> &planner_names;
	const PlanRewriteNeeds &needs;
};

using PlanRewritePass = void (*)(PlanRewriteContext &);

struct PlanRewritePassEntry {
	const char *name;
	PlanRewritePass rewrite;
	bool PlanRewriteNeeds::*required;
};

bool IsRunningWindowCandidate(const BoundWindowExpression &window) {
	if (!window.AggregateFunction() || window.Partitions().size() != 1 || window.OrderBy().size() != 1 ||
	    window.OrderBy()[0].type != OrderType::ASCENDING || window.Filter() || window.Distinct() ||
	    !window.ArgOrders().empty() || window.WindowExclude() != WindowExcludeMode::NO_OTHER ||
	    window.WindowStart() != WindowBoundary::UNBOUNDED_PRECEDING ||
	    (window.WindowEnd() != WindowBoundary::CURRENT_ROW_ROWS &&
	     window.WindowEnd() != WindowBoundary::CURRENT_ROW_RANGE) ||
	    window.OrderBy()[0].expression->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF ||
	    window.Partitions()[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &name = window.AggregateFunction()->GetName().GetIdentifierName();
	return name == "sum" || name == "min" || name == "max" || name == "count" || name == "count_star" || name == "avg";
}

// Seed inputs are maintenance state even when omitted from the public projection.
// ROWS positions share the aggregate's sort, identifying its actual final peer.
static void RewritePassRunningWindowState(PlanRewriteContext &context) {
	vector<LogicalProjection *> projections;
	idx_t position_count = 0, input_count = 0;
	bool changed = false;
	auto expose_input = [&](const Expression &expression) {
		if (expression.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
			return;
		}
		auto alias = string(openivm::RUNNING_INPUT_PREFIX) + to_string(input_count++);
		changed = PropagateHiddenBindingThroughProjectionPath(projections,
		                                                      expression.Cast<BoundColumnRefExpression>().Binding(),
		                                                      expression.GetReturnType(), alias, true) ||
		          changed;
	};
	std::function<bool(LogicalOperator &)> visit = [&](LogicalOperator &node) {
		if (node.children.empty()) {
			return node.type == LogicalOperatorType::LOGICAL_GET;
		}
		if (node.children.size() != 1 ||
		    (node.type != LogicalOperatorType::LOGICAL_PROJECTION && node.type != LogicalOperatorType::LOGICAL_WINDOW &&
		     node.type != LogicalOperatorType::LOGICAL_ORDER_BY && node.type != LogicalOperatorType::LOGICAL_LIMIT &&
		     node.type != LogicalOperatorType::LOGICAL_TOP_N &&
		     node.type != LogicalOperatorType::LOGICAL_CREATE_TABLE)) {
			return false;
		}
		if (node.type == LogicalOperatorType::LOGICAL_WINDOW) {
			auto &first = node.expressions[0]->Cast<BoundWindowExpression>();
			for (auto &item : node.expressions) {
				auto &window = item->Cast<BoundWindowExpression>();
				if (!IsRunningWindowCandidate(window) || !first.PartitionsAreEquivalent(window) ||
				    first.GetSharedOrders(window) != 1) {
					return false;
				}
			}
		}
		if (node.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			projections.push_back(&node.Cast<LogicalProjection>());
		}
		bool eligible = visit(*node.children[0]);
		if (eligible && node.type == LogicalOperatorType::LOGICAL_WINDOW) {
			auto &window = node.Cast<LogicalWindow>();
			bool have_position = false;
			auto expression_count = window.expressions.size();
			for (idx_t i = 0; i < expression_count; i++) {
				auto &expression = window.expressions[i]->Cast<BoundWindowExpression>();
				expose_input(*expression.OrderBy()[0].expression);
				if (expression.AggregateFunction()->GetName().GetIdentifierName() == "avg" &&
				    expression.GetChildren().size() == 1) {
					expose_input(*expression.GetChildren()[0]);
				}
				if (expression.WindowEnd() != WindowBoundary::CURRENT_ROW_ROWS || have_position) {
					continue;
				}
				have_position = true;
				vector<LogicalType> order_types {expression.OrderBy()[0].expression->GetReturnType()};
				vector<LogicalType> argument_order_types;
				FunctionBinder function_binder(context.context);
				auto position = function_binder.BindWindowFunction(
				    RowNumberFun::GetFunction(), vector<unique_ptr<Expression>>(),
				    vector<pair<Identifier, unique_ptr<Expression>>>(), order_types, argument_order_types);
				position->PartitionsMutable().push_back(expression.Partitions()[0]->Copy());
				position->PartitionsStatsMutable().push_back(nullptr);
				position->OrderByMutable().push_back(expression.OrderBy()[0].Copy());
				position->WindowStartMutable() = WindowBoundary::UNBOUNDED_PRECEDING;
				position->WindowEndMutable() = WindowBoundary::CURRENT_ROW_ROWS;
				auto alias = string(openivm::ROWS_POSITION_PREFIX) + to_string(position_count++);
				position->SetAlias(Identifier(alias));
				ColumnBinding binding {TableIndex(window.window_index), ProjectionIndex(window.expressions.size())};
				window.expressions.push_back(std::move(position));
				PropagateHiddenBindingThroughProjectionPath(projections, binding, LogicalType::BIGINT, alias);
				changed = true;
			}
		}
		if (node.type == LogicalOperatorType::LOGICAL_PROJECTION) {
			projections.pop_back();
		}
		return eligible;
	};
	visit(*context.plan);
	if (changed) {
		context.plan->ResolveOperatorTypes();
		OPENIVM_DEBUG_PRINT("[PlanRewrite] Added running-window seed inputs and %llu peer positions\n", position_count);
	}
}

static void RunRewritePipeline(PlanRewriteContext &rewrite_context) {
	const PlanRewritePassEntry passes[] = {
	    {"inline_cte_refs", [](PlanRewriteContext &c) { InlineMaterializedCteRefs(c.context, c.binder, c.plan); },
	     &PlanRewriteNeeds::inline_cte_refs},
	    {"fold_constant_scalar_subqueries",
	     [](PlanRewriteContext &c) { FoldConstantScalarSubqueries(c.context, c.plan); },
	     &PlanRewriteNeeds::fold_constant_scalar_subqueries},
	    {"aggregate_filters", [](PlanRewriteContext &c) { RewriteAggregateFilters(c.context, c.plan); },
	     &PlanRewriteNeeds::aggregate_filters},
	    {"distinct",
	     [](PlanRewriteContext &c) {
		     if (RewriteDistinct(c.context, c.binder, c.plan, true)) {
			     c.planner_names.push_back(openivm::DISTINCT_COUNT_COL);
		     }
	     },
	     &PlanRewriteNeeds::distinct},
	    {"derived_aggregates",
	     [](PlanRewriteContext &c) {
		     Optimizer opt(c.binder, c.context);
		     RewriteDerivedAggregates(c.context, c.plan, opt);
	     },
	     &PlanRewriteNeeds::derived_aggregates},
	    {"group_count_star",
	     [](PlanRewriteContext &c) {
		     InjectHiddenGroupKeys(c.plan);
		     InjectGroupCountStar(c.plan);
	     },
	     &PlanRewriteNeeds::has_aggregate},
	    {"hidden_aggregate_propagation", [](PlanRewriteContext &c) { PropagateHiddenAggregateColumns(c.plan); },
	     &PlanRewriteNeeds::derived_aggregates},
	    {"outer_join_support",
	     [](PlanRewriteContext &c) {
		     if (c.needs.blocking_set_operation) {
			     OPENIVM_DEBUG_PRINT("[PlanRewrite] Skipped outer-join state across blocking set operation\n");
			     return;
		     }
		     RewriteOuterJoinSupport(c.context, c.binder, c.plan, c.needs.has_aggregate);
	     },
	     &PlanRewriteNeeds::outer_join_support},
	    {"semi_anti_subqueries",
	     [](PlanRewriteContext &c) {
		     Optimizer optimizer(c.binder, c.context);
		     FilterPushdown filter_pushdown(optimizer, false,
		                                    FilterPushdown::ProjectionMode::PRESERVE_COMPUTED_EXPRESSIONS);
		     c.plan = filter_pushdown.Rewrite(std::move(c.plan));
		     if (RewriteMarkJoinFilters(c.plan)) {
			     Deliminator deliminator(c.context);
			     c.plan = deliminator.Optimize(std::move(c.plan));
		     }
		     RewriteSafeSemiAntiDelimGets(c.context, c.plan);
		     RewriteSemiAntiProbeKeyProjections(c.plan);
	     },
	     &PlanRewriteNeeds::semi_anti_subqueries},
	    {"running_window_state", RewritePassRunningWindowState, &PlanRewriteNeeds::running_window_state},
	};

	for (const auto &pass : passes) {
		if (!(rewrite_context.needs.*pass.required)) {
			OPENIVM_DEBUG_PRINT("[PlanRewrite] Pass skipped: %s\n", pass.name);
			continue;
		}
		OPENIVM_DEBUG_PRINT("[PlanRewrite] Pass start: %s\n", pass.name);
		pass.rewrite(rewrite_context);
		OPENIVM_DEBUG_PRINT("[PlanRewrite] Pass done: %s\n", pass.name);
	}
}

void PlanRewrite(ClientContext &context, Binder &binder, unique_ptr<LogicalOperator> &plan,
                 vector<string> &planner_names, const PlanRewriteNeeds &needs) {
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Starting\n");
	PlanRewriteContext rewrite_context {context, binder, plan, planner_names, needs};
	RunRewritePipeline(rewrite_context);
	OPENIVM_DEBUG_PRINT("[PlanRewrite] Done\n");
}

using DerivedOutputProjectionMap = unordered_map<TableIndex, const LogicalProjection *>;

static const Expression *ResolveDerivedOutputPassThrough(const Expression &expr,
                                                         const DerivedOutputProjectionMap &projections,
                                                         vector<ColumnBinding> *binding_path = nullptr) {
	auto current = &expr;
	column_binding_set_t visited;
	while (current->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &column = current->Cast<BoundColumnRefExpression>();
		if (!visited.insert(column.Binding()).second) {
			return nullptr;
		}
		if (binding_path) {
			binding_path->push_back(column.Binding());
		}
		auto projection = projections.find(column.Binding().table_index);
		if (projection == projections.end()) {
			return current;
		}
		if (column.Binding().column_index >= projection->second->expressions.size()) {
			return nullptr;
		}
		current = projection->second->expressions[column.Binding().column_index].get();
	}
	return current;
}

namespace {

/// Renders a bound expression as SQL over published output columns. Bindings in `binding_to_column` become quoted
/// column references; other bindings are inlined through `projections`.
struct OutputExpressionRenderer {
	OutputExpressionRenderer(const column_binding_map_t<string> &binding_to_column,
	                         const DerivedOutputProjectionMap &projections, bool allow_functions, bool having = false)
	    : binding_to_column(binding_to_column), projections(projections), allow_functions(allow_functions),
	      having(having) {
	}

	const column_binding_map_t<string> &binding_to_column;
	const DerivedOutputProjectionMap &projections;
	bool allow_functions;
	// HAVING predicates also render IN-lists and aggregate references, and fall back to the expression's own text for
	// anything else that cannot be rendered.
	bool having;
	column_binding_set_t active_bindings;

	bool Render(const Expression &expr, string &sql) {
		if (RenderNode(expr, sql)) {
			return true;
		}
		if (!having) {
			return false;
		}
		sql = "(" + expr.ToString() + ")";
		return true;
	}

	bool RenderAll(const vector<unique_ptr<Expression>> &children, idx_t offset, vector<string> &result) {
		for (idx_t i = offset; i < children.size(); i++) {
			string child_sql;
			if (!Render(*children[i], child_sql)) {
				return false;
			}
			result.push_back(std::move(child_sql));
		}
		return true;
	}

	bool RenderNode(const Expression &expr, string &sql) {
		switch (expr.GetExpressionClass()) {
		case ExpressionClass::BOUND_COLUMN_REF: {
			auto &binding = expr.Cast<BoundColumnRefExpression>().Binding();
			auto entry = binding_to_column.find(binding);
			if (entry != binding_to_column.end()) {
				// Always quote references in the persisted expression. The refresh
				// compiler substitutes quoted identifiers with the post-MERGE state
				// expression; leaving ordinary names unquoted would make them bind
				// ambiguously between the target and delta aliases.
				sql = SQLQuotedIdentifier::ToString(entry->second);
				return true;
			}
			auto projection = projections.find(binding.table_index);
			if (projection == projections.end() || binding.column_index >= projection->second->expressions.size() ||
			    !active_bindings.insert(binding).second) {
				return false;
			}
			bool rendered = Render(*projection->second->expressions[binding.column_index], sql);
			active_bindings.erase(binding);
			return rendered;
		}
		case ExpressionClass::BOUND_FUNCTION: {
			if (BoundComparisonExpression::IsComparison(expr)) {
				auto &comparison = expr.Cast<BoundFunctionExpression>();
				string left_sql;
				string right_sql;
				if (!Render(BoundComparisonExpression::Left(comparison), left_sql) ||
				    !Render(BoundComparisonExpression::Right(comparison), right_sql)) {
					return false;
				}
				sql = "(" + left_sql + " " + ExpressionTypeToOperator(comparison.GetExpressionType()) + " " +
				      right_sql + ")";
				return true;
			}
			if (BoundCastExpression::IsCast(expr)) {
				auto &cast = expr.Cast<BoundFunctionExpression>();
				string child_sql;
				if (!Render(BoundCastExpression::Child(cast), child_sql)) {
					return false;
				}
				sql = string(BoundCastExpression::IsTryCast(cast) ? "TRY_CAST(" : "CAST(") + child_sql + " AS " +
				      cast.GetReturnType().ToString() + ")";
				return true;
			}
			auto &function = expr.Cast<BoundFunctionExpression>();
			vector<string> children;
			if (!allow_functions || !RenderAll(function.GetChildren(), 0, children)) {
				return false;
			}
			const auto &name = function.Function().GetName().GetIdentifierName();
			if (children.size() == 2 && (name == "+" || name == "-" || name == "*" || name == "/" || name == "%")) {
				sql = "(" + children[0] + " " + name + " " + children[1] + ")";
			} else {
				sql = name + "(" + StringUtil::Join(children, ", ") + ")";
			}
			return true;
		}
		case ExpressionClass::BOUND_CONSTANT:
			sql = expr.Cast<BoundConstantExpression>().GetValue().ToSQLString();
			return true;
		case ExpressionClass::BOUND_CASE: {
			auto &case_expr = expr.Cast<BoundCaseExpression>();
			sql = "CASE";
			for (auto &check : case_expr.CaseChecks()) {
				string when_sql;
				string then_sql;
				if (!Render(*check.when_expr, when_sql) || !Render(*check.then_expr, then_sql)) {
					return false;
				}
				sql += " WHEN " + when_sql + " THEN " + then_sql;
			}
			string else_sql;
			if (!Render(*case_expr.ElseExpression(), else_sql)) {
				return false;
			}
			sql += " ELSE " + else_sql + " END";
			return true;
		}
		case ExpressionClass::BOUND_CONJUNCTION: {
			auto &conjunction = expr.Cast<BoundConjunctionExpression>();
			vector<string> children;
			if (!RenderAll(conjunction.GetChildren(), 0, children)) {
				return false;
			}
			auto op = conjunction.GetExpressionType() == ExpressionType::CONJUNCTION_AND ? " AND " : " OR ";
			sql = "(" + StringUtil::Join(children, op) + ")";
			return true;
		}
		case ExpressionClass::BOUND_OPERATOR: {
			auto &op = expr.Cast<BoundOperatorExpression>();
			if (having &&
			    (expr.GetExpressionType() == ExpressionType::COMPARE_IN ||
			     expr.GetExpressionType() == ExpressionType::COMPARE_NOT_IN) &&
			    op.GetChildren().size() >= 2) {
				string left_sql;
				vector<string> list;
				if (!Render(*op.GetChildren()[0], left_sql) || !RenderAll(op.GetChildren(), 1, list)) {
					return false;
				}
				sql = "(" + left_sql +
				      (expr.GetExpressionType() == ExpressionType::COMPARE_IN ? " IN (" : " NOT IN (") +
				      StringUtil::Join(list, ", ") + "))";
				return true;
			}
			string child_sql;
			if (op.GetChildren().size() != 1 || !Render(*op.GetChildren()[0], child_sql)) {
				return false;
			}
			if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NULL) {
				sql = "(" + child_sql + " IS NULL)";
			} else if (expr.GetExpressionType() == ExpressionType::OPERATOR_IS_NOT_NULL) {
				sql = "(" + child_sql + " IS NOT NULL)";
			} else if (expr.GetExpressionType() == ExpressionType::OPERATOR_NOT) {
				sql = "(NOT " + child_sql + ")";
			} else {
				return false;
			}
			return true;
		}
		case ExpressionClass::BOUND_AGGREGATE: {
			// HAVING can reference an aggregate directly (e.g. `HAVING AVG(x) > 100`); use the output column of
			// its single argument when that argument is published.
			auto &aggregate = expr.Cast<BoundAggregateExpression>();
			if (!having || aggregate.GetChildren().size() != 1 ||
			    aggregate.GetChildren()[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
				return false;
			}
			auto entry = binding_to_column.find(aggregate.GetChildren()[0]->Cast<BoundColumnRefExpression>().Binding());
			if (entry == binding_to_column.end()) {
				return false;
			}
			sql = SQLQuotedIdentifier::ToString(entry->second);
			return true;
		}
		default:
			return false;
		}
	}
};

} // namespace

static column_binding_map_t<string> BuildOutputBindingNames(LogicalOperator &plan, const CreateMVPlanFacts &facts,
                                                            const vector<string> &output_names) {
	DerivedOutputProjectionMap projections;
	for (auto *projection : facts.projections) {
		projections[projection->table_index] = projection;
	}
	column_binding_map_t<string> binding_to_column;
	auto bindings = plan.GetColumnBindings();
	for (idx_t i = 0; i < bindings.size() && i < output_names.size(); i++) {
		BoundColumnRefExpression output(LogicalType::SQLNULL, bindings[i]);
		vector<ColumnBinding> path;
		auto resolved = ResolveDerivedOutputPassThrough(output, projections, &path);
		// Every binding on this pass-through path denotes the published value,
		// including when the path ends at a computed expression.
		if (resolved) {
			for (auto key : path) {
				binding_to_column.emplace(key, output_names[i]);
			}
		}
	}
	// DISTINCT normalization replaces pass-through bindings with group outputs.
	// ORDER BY can still reference the corresponding input binding, so expose
	// that equivalence only for group keys present in the published output.
	for (auto *aggregate : facts.aggregates) {
		for (idx_t i = 0; i < aggregate->groups.size(); i++) {
			auto entry = binding_to_column.find(ColumnBinding(TableIndex(aggregate->group_index), ProjectionIndex(i)));
			if (entry == binding_to_column.end()) {
				continue;
			}
			auto name = entry->second;
			vector<ColumnBinding> path;
			auto resolved = ResolveDerivedOutputPassThrough(*aggregate->groups[i], projections, &path);
			if (resolved) {
				for (auto input_key : path) {
					binding_to_column.emplace(input_key, name);
				}
			}
		}
	}
	return binding_to_column;
}

string ResolvePlanOutputColumn(ColumnBinding binding, LogicalOperator &plan, const CreateMVPlanFacts &facts,
                               const vector<string> &output_names) {
	auto names = BuildOutputBindingNames(plan, facts, output_names);
	auto entry = names.find(binding);
	return entry == names.end() ? "" : entry->second;
}

vector<string> ResolvePlanOutputColumns(ColumnBinding binding, LogicalOperator &plan, const CreateMVPlanFacts &facts,
                                        const vector<string> &output_names) {
	DerivedOutputProjectionMap projections;
	for (auto *projection : facts.projections) {
		projections[projection->table_index] = projection;
	}
	vector<string> names;
	auto bindings = plan.GetColumnBindings();
	for (idx_t i = 0; i < bindings.size() && i < output_names.size(); i++) {
		BoundColumnRefExpression output(LogicalType::SQLNULL, bindings[i]);
		vector<ColumnBinding> path;
		if (ResolveDerivedOutputPassThrough(output, projections, &path) &&
		    std::find(path.begin(), path.end(), binding) != path.end()) {
			names.push_back(output_names[i]);
		}
	}
	return names;
}

bool RenderPlanOutputExpression(const Expression &expression, LogicalOperator &plan, const CreateMVPlanFacts &facts,
                                const vector<string> &output_names, string &sql) {
	DerivedOutputProjectionMap projections;
	for (auto *projection : facts.projections) {
		projections[projection->table_index] = projection;
	}
	auto binding_to_column = BuildOutputBindingNames(plan, facts, output_names);
	return OutputExpressionRenderer(binding_to_column, projections, true).Render(expression, sql);
}

DerivedAggregateOutputInfo ExtractDerivedAggregateOutputs(const LogicalOperator &plan, const CreateMVPlanFacts &facts,
                                                          const vector<string> &output_names) {
	DerivedAggregateOutputInfo info;
	if (plan.type != LogicalOperatorType::LOGICAL_PROJECTION || plan.children.empty() || facts.aggregates.empty()) {
		info.complete = true;
		return info;
	}
	auto &projection = plan.Cast<LogicalProjection>();
	DerivedOutputProjectionMap projections;
	for (auto *candidate : facts.projections) {
		if (candidate != &projection) {
			projections[candidate->table_index] = candidate;
		}
	}
	column_binding_map_t<string> binding_to_column;
	vector<const Expression *> resolved_expressions;
	resolved_expressions.reserve(projection.expressions.size());
	for (idx_t i = 0; i < projection.expressions.size() && i < output_names.size(); i++) {
		vector<ColumnBinding> binding_path;
		auto resolved = ResolveDerivedOutputPassThrough(*projection.expressions[i], projections, &binding_path);
		if (!resolved) {
			return info;
		}
		resolved_expressions.push_back(resolved);
		if (resolved->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
			for (auto key : binding_path) {
				binding_to_column.emplace(key, output_names[i]);
			}
		}
	}
	OutputExpressionRenderer renderer(binding_to_column, projections, false);
	for (idx_t i = 0; i < resolved_expressions.size(); i++) {
		auto &expression = *resolved_expressions[i];
		if (IncrementalTableNames::IsInternalColumn(output_names[i])) {
			continue;
		}
		if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
		    binding_to_column.at(expression.Cast<BoundColumnRefExpression>().Binding()) == output_names[i]) {
			continue;
		}
		string expression_sql;
		if (renderer.Render(expression, expression_sql)) {
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

/// Collect every BOUND_COLUMN_REF binding referenced by the expression tree.
static void CollectFilterBindings(Expression &expr, std::set<pair<TableIndex, ProjectionIndex>> &out) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		auto &col = expr.Cast<BoundColumnRefExpression>();
		out.insert({col.Binding().table_index, col.Binding().column_index});
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
	auto spine = FindOutputSpine(plan.get(), [](const LogicalOperator &op) {
		// Only descend through transparent operators. Stop at JOIN, UNION, GET, AGGREGATE, etc.
		// AGGREGATE is intentionally excluded: HAVING is always a FILTER *above* its AGGREGATE,
		// never inside it. Descending into AGGREGATE children would find a nested HAVING from
		// an inner subquery or a DISTINCT-rewrite-introduced outer AGGREGATE, and erroneously
		// expose openivm_having_N columns that are invisible in the outer output.
		// Materialized CTE: only descend into the outer query (children[1]); a FILTER
		// in the CTE body is the CTE's own HAVING and must stay where it is.
		if (op.type == LogicalOperatorType::LOGICAL_FILTER) {
			return op.children.empty() || op.children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY;
		}
		return op.type == LogicalOperatorType::LOGICAL_ORDER_BY || op.type == LogicalOperatorType::LOGICAL_LIMIT ||
		       op.type == LogicalOperatorType::LOGICAL_DISTINCT ||
		       op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE;
	});
	auto *filter_node = spine.bottom;
	auto *parent = spine.parent;
	if (!filter_node || filter_node->type != LogicalOperatorType::LOGICAL_FILTER || filter_node->children.empty() ||
	    filter_node->children[0]->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return "";
	}
	// The spine's projections lead from the view output down to the projection directly
	// above HAVING. A visible alias is safe only when every projection on this path
	// passes the aggregate output through unchanged.
	auto &proj_chain = spine.projections;

	// Build binding → alias map from output expressions that are pure pass-throughs.
	// If an upper projection transforms an aggregate (for example COALESCE(sum, 0)),
	// HAVING must use a hidden raw aggregate column instead of the transformed alias.
	column_binding_map_t<string> binding_to_alias;
	LogicalProjection *proj_ptr = nullptr;
	if (parent && parent->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		// The spine pushes every projection it passes, so the parent is the last one.
		proj_ptr = &parent->Cast<LogicalProjection>();
		DerivedOutputProjectionMap projections;
		for (auto *projection : proj_chain) {
			projections[projection->table_index] = projection;
		}
		auto *output_projection = proj_chain.front();
		for (idx_t i = 0; i < output_projection->expressions.size() && i < output_names.size(); i++) {
			auto *resolved = ResolveDerivedOutputPassThrough(*output_projection->expressions[i], projections);
			if (resolved && resolved->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
				binding_to_alias[resolved->Cast<BoundColumnRefExpression>().Binding()] = output_names[i];
			}
		}
	}

	// Expose aggregate outputs referenced by the HAVING predicate but missing from
	// the SELECT list (e.g. HAVING COUNT(*) > N when COUNT(*) isn't in SELECT, or
	// HAVING SUM(COALESCE(x, 0)) > ... when only SUM(x) is selected). Without this
	// the predicate SQL falls back to the raw aggregate text, which re-references
	// base-table columns that aren't in the data table.
	auto &filter = filter_node->Cast<LogicalFilter>();
	if (proj_ptr) {
		auto &agg_child = filter_node->children[0]->Cast<LogicalAggregate>();
		std::set<pair<TableIndex, ProjectionIndex>> filter_bindings;
		for (auto &expr : filter.expressions) {
			CollectFilterBindings(*expr, filter_bindings);
		}
		idx_t next_hidden = 0;
		bool added_hidden_having_column = false;
		idx_t hidden_start_idx = proj_ptr->expressions.size();
		for (auto &b : filter_bindings) {
			ColumnBinding binding {TableIndex(b.first), ProjectionIndex(b.second)};
			if (binding_to_alias.find(binding) != binding_to_alias.end()) {
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
			auto col_type = agg_child.expressions[b.second]->GetReturnType();
			proj_ptr->expressions.push_back(
			    make_uniq<BoundColumnRefExpression>(Identifier(hidden_name), col_type, binding));
			output_names.push_back(hidden_name);
			binding_to_alias[binding] = hidden_name;
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
			// proj_chain.back() == proj_ptr carries the freshly added hidden columns.
			vector<LogicalProjection *> upper_chain(proj_chain.begin(), proj_chain.end() - 1);
			for (idx_t i = hidden_start_idx; i < proj_ptr->expressions.size(); i++) {
				auto &hidden = *proj_ptr->expressions[i];
				PropagateHiddenBindingThroughProjectionPath(
				    upper_chain, ColumnBinding(TableIndex(proj_ptr->table_index), ProjectionIndex(i)),
				    hidden.GetReturnType(), hidden.GetName().GetIdentifierName());
			}
		}
	}

	// Extract HAVING predicate as SQL.
	DerivedOutputProjectionMap no_projections;
	OutputExpressionRenderer renderer(binding_to_alias, no_projections, true, true);
	string having_sql;
	for (idx_t i = 0; i < filter.expressions.size(); i++) {
		string predicate_sql;
		renderer.Render(*filter.expressions[i], predicate_sql);
		having_sql += (i > 0 ? " AND " : "") + predicate_sql;
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
