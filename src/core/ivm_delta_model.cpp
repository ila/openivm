#include "core/ivm_delta_model.hpp"

#include "core/openivm_constants.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_window.hpp"

#include <unordered_set>

namespace duckdb {

DeltaOperatorSpec GetDeltaOperatorSpec(LogicalOperatorType op_type) {
	switch (op_type) {
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return {op_type,
		        op_type == LogicalOperatorType::LOGICAL_CTE_REF     ? DeltaRewriteKind::CTE_REF
		        : op_type == LogicalOperatorType::LOGICAL_DELIM_GET ? DeltaRewriteKind::UNSUPPORTED
		                                                            : DeltaRewriteKind::CONSTANT_LEAF,
		        Linearity::LINEAR,
		        DeltaApplyKind::INCREMENTAL,
		        /*preserves_insert_only=*/true,
		        /*needs_source_consolidation=*/false,
		        /*needs_delta_consolidation=*/false};
	case LogicalOperatorType::LOGICAL_GET:
		return {op_type,
		        DeltaRewriteKind::SCAN,
		        Linearity::LINEAR,
		        DeltaApplyKind::INCREMENTAL,
		        /*preserves_insert_only=*/true,
		        /*needs_source_consolidation=*/true,
		        /*needs_delta_consolidation=*/false};
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		return {op_type,
		        op_type == LogicalOperatorType::LOGICAL_FILTER       ? DeltaRewriteKind::FILTER
		        : op_type == LogicalOperatorType::LOGICAL_PROJECTION ? DeltaRewriteKind::PROJECTION
		        : op_type == LogicalOperatorType::LOGICAL_UNION      ? DeltaRewriteKind::UNION
		                                                             : DeltaRewriteKind::MATERIALIZED_CTE,
		        Linearity::LINEAR,
		        DeltaApplyKind::INCREMENTAL,
		        /*preserves_insert_only=*/true,
		        /*needs_source_consolidation=*/false,
		        /*needs_delta_consolidation=*/false};
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return {op_type,
		        DeltaRewriteKind::AGGREGATE,
		        Linearity::LINEAR,
		        DeltaApplyKind::INCREMENTAL,
		        /*preserves_insert_only=*/true,
		        /*needs_source_consolidation=*/false,
		        /*needs_delta_consolidation=*/true};
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
	case LogicalOperatorType::LOGICAL_LIMIT:
		return {op_type,
		        DeltaRewriteKind::TOPK,
		        Linearity::NON_LINEAR,
		        DeltaApplyKind::INCREMENTAL,
		        /*preserves_insert_only=*/true,
		        /*needs_source_consolidation=*/false,
		        /*needs_delta_consolidation=*/false};
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
		return {op_type,
		        op_type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
		                op_type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN
		            ? DeltaRewriteKind::DELIM_JOIN
		            : DeltaRewriteKind::JOIN,
		        Linearity::BILINEAR,
		        DeltaApplyKind::INCREMENTAL,
		        /*preserves_insert_only=*/false,
		        /*needs_source_consolidation=*/true,
		        /*needs_delta_consolidation=*/true};
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return {op_type,
		        DeltaRewriteKind::DISTINCT,
		        Linearity::NON_LINEAR,
		        DeltaApplyKind::INCREMENTAL,
		        /*preserves_insert_only=*/false,
		        /*needs_source_consolidation=*/false,
		        /*needs_delta_consolidation=*/true};
	case LogicalOperatorType::LOGICAL_WINDOW:
		return {op_type,
		        DeltaRewriteKind::WINDOW,
		        Linearity::NON_LINEAR,
		        DeltaApplyKind::PARTITION_RECOMPUTE,
		        /*preserves_insert_only=*/false,
		        /*needs_source_consolidation=*/true,
		        /*needs_delta_consolidation=*/true};
	case LogicalOperatorType::LOGICAL_UNNEST:
	default:
		return {op_type,
		        DeltaRewriteKind::UNSUPPORTED,
		        Linearity::NON_LINEAR,
		        DeltaApplyKind::FULL_REFRESH,
		        /*preserves_insert_only=*/false,
		        /*needs_source_consolidation=*/true,
		        /*needs_delta_consolidation=*/true};
	}
}

DeltaRewriteKind GetDeltaRewriteKind(LogicalOperatorType op_type) {
	return GetDeltaOperatorSpec(op_type).rewrite_kind;
}

namespace {

static const unordered_set<string> &SupportedAggregates() {
	static const unordered_set<string> supported = {
	    "count_star", "count",    "sum",      "min",     "max",      "avg",     "list",    "stddev", "stddev_samp",
	    "stddev_pop", "variance", "var_samp", "var_pop", "bool_and", "bool_or", "arg_min", "arg_max"};
	return supported;
}

static bool HasVolatileExpression(vector<unique_ptr<Expression>> &expressions) {
	for (auto &expr : expressions) {
		bool found_volatile = false;
		ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) {
			if (child.expression_class != ExpressionClass::BOUND_FUNCTION) {
				return;
			}
			auto &func = child.Cast<BoundFunctionExpression>();
			if (func.function.name.rfind("__internal_compress", 0) == 0 ||
			    func.function.name.rfind("__internal_decompress", 0) == 0 || func.function.name == "error") {
				return;
			}
			if (func.function.GetStability() != FunctionStability::CONSISTENT) {
				found_volatile = true;
			}
		});
		if (found_volatile) {
			return true;
		}
	}
	return false;
}

static bool HasUnnestExpression(vector<unique_ptr<Expression>> &expressions) {
	for (auto &expr : expressions) {
		bool found_unnest = false;
		ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) {
			if (child.expression_class == ExpressionClass::BOUND_UNNEST) {
				found_unnest = true;
			}
		});
		if (found_unnest) {
			return true;
		}
	}
	return false;
}

static string OrderByColumnName(const Expression &expr) {
	if (expr.type != ExpressionType::BOUND_COLUMN_REF) {
		return string();
	}
	auto &bcr = expr.Cast<BoundColumnRefExpression>();
	if (!bcr.alias.empty()) {
		return bcr.alias;
	}
	return bcr.GetName();
}

static bool ExtractOrderBy(const vector<BoundOrderByNode> &orders, PlanAnalysis &analysis) {
	analysis.top_k_order_columns.clear();
	analysis.top_k_order_desc.clear();
	for (auto &order : orders) {
		string name = OrderByColumnName(*order.expression);
		if (name.empty()) {
			return false;
		}
		analysis.top_k_order_columns.push_back(name);
		analysis.top_k_order_desc.push_back(order.type == OrderType::DESCENDING);
	}
	return true;
}

static bool ChildIsAggregateThroughProjection(LogicalOperator *node) {
	while (node && node->type == LogicalOperatorType::LOGICAL_PROJECTION && !node->children.empty()) {
		node = node->children[0].get();
	}
	return node && node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY;
}

static void AddWindowPartitionColumn(PlanAnalysis &analysis, string col_name, idx_t col_index) {
	for (auto &existing : analysis.window_partition_columns) {
		if (existing == col_name) {
			return;
		}
	}
	analysis.window_partition_columns.push_back(std::move(col_name));
	analysis.window_partition_column_indexes.push_back(col_index);
}

static void AddOperator(DeltaPlanModel &model, LogicalOperatorType op_type) {
	model.operators.push_back(GetDeltaOperatorSpec(op_type));
}

static void AnalyzeDeltaNode(LogicalOperator *node, DeltaPlanModel &model) {
	if (!node) {
		return;
	}

	switch (node->type) {
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_INSERT:
		for (auto &child : node->children) {
			AnalyzeDeltaNode(child.get(), model);
		}
		return;

	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		AddOperator(model, node->type);
		return;

	case LogicalOperatorType::LOGICAL_FILTER:
		if (HasVolatileExpression(node->expressions)) {
			model.analysis.ivm_compatible = false;
		}
		if (!node->children.empty() && ChildIsAggregateThroughProjection(node->children[0].get())) {
			model.analysis.found_having = true;
		}
		AddOperator(model, node->type);
		break;

	case LogicalOperatorType::LOGICAL_PROJECTION:
		model.analysis.found_projection = true;
		if (HasVolatileExpression(node->expressions) || HasUnnestExpression(node->expressions)) {
			model.analysis.ivm_compatible = false;
		}
		AddOperator(model, node->type);
		break;

	case LogicalOperatorType::LOGICAL_UNION:
		if (HasVolatileExpression(node->expressions)) {
			model.analysis.ivm_compatible = false;
		}
		AddOperator(model, node->type);
		break;

	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		model.analysis.found_aggregation = true;
		auto &agg = node->Cast<LogicalAggregate>();
		if (agg.grouping_sets.size() > 1) {
			model.analysis.found_grouping_sets = true;
		}
		for (auto &expr : agg.expressions) {
			if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
				continue;
			}
			auto &bound_agg = expr->Cast<BoundAggregateExpression>();
			const bool scalar_subquery_first =
			    bound_agg.function.name == "first" && model.analysis.group_index != DConstants::INVALID_INDEX;
			if (SupportedAggregates().find(bound_agg.function.name) == SupportedAggregates().end() &&
			    !scalar_subquery_first) {
				model.analysis.ivm_compatible = false;
			}
			if (bound_agg.IsDistinct()) {
				model.analysis.found_count_distinct = true;
			}
			if (bound_agg.filter) {
				if (bound_agg.function.name == "list") {
					model.analysis.found_filtered_list = true;
				} else {
					model.analysis.ivm_compatible = false;
				}
			}
			if (bound_agg.function.name == "min" || bound_agg.function.name == "max" ||
			    bound_agg.function.name == "arg_min" || bound_agg.function.name == "arg_max") {
				model.analysis.found_minmax = true;
			}
			if (bound_agg.function.name == "list") {
				model.analysis.found_list = true;
			}
			if (model.analysis.group_index == DConstants::INVALID_INDEX) {
				model.analysis.aggregate_types.push_back(bound_agg.function.name);
			}
		}
		if (HasVolatileExpression(agg.groups)) {
			model.analysis.ivm_compatible = false;
		}
		if (model.analysis.group_index == DConstants::INVALID_INDEX) {
			model.analysis.group_count = agg.groups.size();
			model.analysis.group_index = agg.group_index;
		} else {
			model.analysis.found_nested_aggregate = true;
		}
		AddOperator(model, node->type);
		break;
	}

	case LogicalOperatorType::LOGICAL_TOP_N: {
		auto &top_n = node->Cast<LogicalTopN>();
		model.analysis.found_top_k = true;
		model.analysis.top_k_limit = top_n.limit;
		model.analysis.top_k_offset = top_n.offset;
		if (!ExtractOrderBy(top_n.orders, model.analysis)) {
			model.analysis.ivm_compatible = false;
		}
		AddOperator(model, node->type);
		break;
	}

	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		auto &order = node->Cast<LogicalOrder>();
		if (model.analysis.top_k_order_columns.empty()) {
			(void)ExtractOrderBy(order.orders, model.analysis);
		}
		AddOperator(model, node->type);
		break;
	}

	case LogicalOperatorType::LOGICAL_LIMIT: {
		model.analysis.found_top_k = true;
		auto &limit = node->Cast<LogicalLimit>();
		if (limit.limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			model.analysis.top_k_limit = limit.limit_val.GetConstantValue();
		}
		if (limit.offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
			model.analysis.top_k_offset = limit.offset_val.GetConstantValue();
		}
		AddOperator(model, node->type);
		break;
	}

	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		model.analysis.found_join = true;
		if (node->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
		    node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			model.analysis.found_delim_join = true;
		}
		auto *join = dynamic_cast<LogicalJoin *>(node);
		if (join) {
			if (join->join_type != JoinType::INNER && join->join_type != JoinType::LEFT &&
			    join->join_type != JoinType::RIGHT && join->join_type != JoinType::OUTER &&
			    join->join_type != JoinType::SEMI && join->join_type != JoinType::ANTI &&
			    join->join_type != JoinType::MARK && join->join_type != JoinType::SINGLE) {
				model.analysis.ivm_compatible = false;
			}
			if (join->join_type == JoinType::SINGLE) {
				model.analysis.found_single_join = true;
			}
			if (join->join_type == JoinType::SEMI || join->join_type == JoinType::ANTI ||
			    join->join_type == JoinType::MARK) {
				model.analysis.found_semi_anti_join = true;
			}
			if (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT ||
			    join->join_type == JoinType::OUTER) {
				model.analysis.found_left_join = true;
			}
			if (join->join_type == JoinType::OUTER) {
				model.analysis.found_full_outer = true;
			}
		}
		AddOperator(model, node->type);
		break;
	}

	case LogicalOperatorType::LOGICAL_DISTINCT: {
		model.analysis.found_distinct = true;
		if (HasVolatileExpression(node->expressions)) {
			model.analysis.ivm_compatible = false;
		}
		auto *distinct_node = dynamic_cast<LogicalDistinct *>(node);
		if (distinct_node && !distinct_node->distinct_targets.empty()) {
			for (auto &target : distinct_node->distinct_targets) {
				model.analysis.aggregate_columns.emplace_back(target->GetName());
			}
		} else if (!node->children.empty()) {
			for (idx_t i = 0; i < node->children[0]->types.size(); i++) {
				model.analysis.aggregate_columns.emplace_back("col" + to_string(i));
			}
		}
		AddOperator(model, node->type);
		break;
	}

	case LogicalOperatorType::LOGICAL_WINDOW: {
		model.analysis.found_window = true;
		auto &window = node->Cast<LogicalWindow>();
		for (auto &expr : window.expressions) {
			if (expr->expression_class != ExpressionClass::BOUND_WINDOW) {
				continue;
			}
			auto &win_expr = expr->Cast<BoundWindowExpression>();
			for (auto &part : win_expr.partitions) {
				string col_name;
				idx_t col_index = DConstants::INVALID_INDEX;
				if (part->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &bcr = part->Cast<BoundColumnRefExpression>();
					col_name = bcr.alias;
					col_index = bcr.binding.column_index;
				}
				if (col_name.empty()) {
					col_name = part->GetName();
				}
				AddWindowPartitionColumn(model.analysis, col_name, col_index);
			}
		}
		AddOperator(model, node->type);
		break;
	}

	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		AddOperator(model, node->type);
		if (node->children.size() >= 2) {
			AnalyzeDeltaNode(node->children[1].get(), model);
			if (!model.analysis.found_aggregation && !model.analysis.found_distinct && !model.analysis.found_join) {
				AnalyzeDeltaNode(node->children[0].get(), model);
			}
		}
		return;

	case LogicalOperatorType::LOGICAL_UNNEST:
		model.analysis.ivm_compatible = false;
		AddOperator(model, node->type);
		return;

	default:
		model.analysis.ivm_compatible = false;
		AddOperator(model, node->type);
		return;
	}

	for (auto &child : node->children) {
		AnalyzeDeltaNode(child.get(), model);
	}
}

static bool HasUnionBeforeAggregate(const LogicalOperator *op, bool seen_agg_above = false) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		seen_agg_above = true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_UNION && !seen_agg_above) {
		return true;
	}
	for (auto &child : op->children) {
		if (HasUnionBeforeAggregate(child.get(), seen_agg_above)) {
			return true;
		}
	}
	return false;
}

static bool HasUnsupportedSetOperation(const LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_INTERSECT || op->type == LogicalOperatorType::LOGICAL_EXCEPT) {
		return true;
	}
	for (auto &child : op->children) {
		if (HasUnsupportedSetOperation(child.get())) {
			return true;
		}
	}
	return false;
}

static bool IsAggregateFunctionUnsupportedByLpts(const string &fn_name) {
	return fn_name == "quantile_cont" || fn_name == "quantile_disc" || fn_name == "percentile_cont" ||
	       fn_name == "percentile_disc" || fn_name == "approx_quantile" || fn_name == "mad" || fn_name == "median" ||
	       fn_name == "mode" ||
	       // Two-argument aggregates whose children LPTS re-aliases to internal
	       // `tN_col` names; the serialized SQL refers to those names against the
	       // original FROM clause and fails binding at CREATE-table time.
	       fn_name == "corr" || fn_name == "covar_pop" || fn_name == "covar_samp" || fn_name == "regr_avgx" ||
	       fn_name == "regr_avgy" || fn_name == "regr_count" || fn_name == "regr_intercept" || fn_name == "regr_r2" ||
	       fn_name == "regr_slope" || fn_name == "regr_sxx" || fn_name == "regr_sxy" || fn_name == "regr_syy" ||
	       fn_name == "arg_min" || fn_name == "arg_max";
}

static bool PlanNeedsOriginalSqlForLpts(LogicalOperator *op) {
	if (!op) {
		return false;
	}
	if (op->type == LogicalOperatorType::LOGICAL_PIVOT) {
		return true;
	}
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto *agg = dynamic_cast<LogicalAggregate *>(op);
		if (agg) {
			for (auto &expr : agg->expressions) {
				if (expr->type != ExpressionType::BOUND_AGGREGATE) {
					continue;
				}
				auto &bound_agg = expr->Cast<BoundAggregateExpression>();
				if (IsAggregateFunctionUnsupportedByLpts(bound_agg.function.name)) {
					return true;
				}
			}
		}
	}
	for (auto &child : op->children) {
		if (PlanNeedsOriginalSqlForLpts(child.get())) {
			return true;
		}
	}
	return false;
}

static bool IsPurePassThroughExpression(const Expression *expr) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		return true;
	}
	if (expr->expression_class == ExpressionClass::BOUND_CAST) {
		auto &cast = expr->Cast<BoundCastExpression>();
		return IsPurePassThroughExpression(cast.child.get());
	}
	return false;
}

static bool ExpressionReferencesNonGroupBinding(Expression *expr, idx_t group_index) {
	if (expr->type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		return bcr.binding.table_index != group_index;
	}
	bool refs_non_group = false;
	ExpressionIterator::EnumerateChildren(*expr, [&](unique_ptr<Expression> &child) {
		if (!refs_non_group && ExpressionReferencesNonGroupBinding(child.get(), group_index)) {
			refs_non_group = true;
		}
	});
	return refs_non_group;
}

static bool OuterJoinAggregateNeedsRecompute(LogicalOperator *op, idx_t group_index) {
	if (op->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op->Cast<LogicalAggregate>();
		for (auto &expr : agg.expressions) {
			if (expr->expression_class != ExpressionClass::BOUND_AGGREGATE) {
				continue;
			}
			auto &bound = expr->Cast<BoundAggregateExpression>();
			for (auto &child : bound.children) {
				if (!IsPurePassThroughExpression(child.get())) {
					return true;
				}
			}
		}
	}
	if (op->type == LogicalOperatorType::LOGICAL_PROJECTION) {
		auto &proj = op->Cast<LogicalProjection>();
		for (auto &expr : proj.expressions) {
			if (!IsPurePassThroughExpression(expr.get()) &&
			    ExpressionReferencesNonGroupBinding(expr.get(), group_index)) {
				return true;
			}
		}
	}
	for (auto &child : op->children) {
		if (OuterJoinAggregateNeedsRecompute(child.get(), group_index)) {
			return true;
		}
	}
	return false;
}

} // namespace

DeltaPlanModel BuildDeltaPlanModel(LogicalOperator *plan) {
	DeltaPlanModel model;
	AnalyzeDeltaNode(plan, model);
	model.analysis.found_union_before_aggregate = model.analysis.found_aggregation && HasUnionBeforeAggregate(plan);
	model.analysis.found_unsupported_set_operation = HasUnsupportedSetOperation(plan);
	model.analysis.needs_original_sql_for_lpts = PlanNeedsOriginalSqlForLpts(plan);
	model.analysis.outer_join_aggregate_needs_recompute =
	    (model.analysis.found_left_join || model.analysis.found_full_outer) && model.analysis.found_aggregation &&
	    OuterJoinAggregateNeedsRecompute(plan, model.analysis.group_index);
	return model;
}

string DeltaPlanModel::DebugString() const {
	string result = analysis.ivm_compatible ? "incremental-compatible" : "full-refresh";
	result += " operators=" + to_string(operators.size());
	result += " aggregation=" + string(analysis.found_aggregation ? "true" : "false");
	result += " projection=" + string(analysis.found_projection ? "true" : "false");
	result += " group_count=" + to_string(analysis.group_count);
	result += " aggregate_types=" + to_string(analysis.aggregate_types.size());
	result += " union_before_agg=" + string(analysis.found_union_before_aggregate ? "true" : "false");
	result += " unsupported_set=" + string(analysis.found_unsupported_set_operation ? "true" : "false");
	result += " lpts_original_sql=" + string(analysis.needs_original_sql_for_lpts ? "true" : "false");
	result += " outer_join_recompute=" + string(analysis.outer_join_aggregate_needs_recompute ? "true" : "false");
	return result;
}

} // namespace duckdb
