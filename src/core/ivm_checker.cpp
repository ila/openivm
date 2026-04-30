#include "core/ivm_checker.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_window.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"
#include "duckdb/planner/operator/logical_order.hpp"

#include <unordered_set>

namespace duckdb {

static const unordered_set<string> &GetSupportedAggregates() {
	static const unordered_set<string> kSet = {
	    "count_star", "count",    "sum",      "min",     "max",      "avg",     "list",    "stddev", "stddev_samp",
	    "stddev_pop", "variance", "var_samp", "var_pop", "bool_and", "bool_or", "arg_min", "arg_max"};
	return kSet;
}

/// Extract a column name from an ORDER BY expression. Returns empty if the expression
/// isn't a column reference (rejecting non-column ORDER BY in Phase 1 keeps the new-top-k
/// SQL emission simple — function/CASE/cast expressions over base columns would need a
/// projection alias).
static string OrderByColumnName(const Expression &expr) {
	if (expr.type == ExpressionType::BOUND_COLUMN_REF) {
		auto &bcr = expr.Cast<BoundColumnRefExpression>();
		if (!bcr.alias.empty()) {
			return bcr.alias;
		}
		return bcr.GetName();
	}
	return string();
}

/// Populate top_k_order_columns / top_k_order_desc from a vector<BoundOrderByNode>.
/// Returns false if any entry is non-column-ref (caller should mark ivm_compatible=false).
static bool ExtractOrderBy(const vector<BoundOrderByNode> &orders, PlanAnalysis &result) {
	result.top_k_order_columns.clear();
	result.top_k_order_desc.clear();
	for (auto &o : orders) {
		string name = OrderByColumnName(*o.expression);
		if (name.empty()) {
			return false;
		}
		result.top_k_order_columns.push_back(name);
		result.top_k_order_desc.push_back(o.type == OrderType::DESCENDING);
	}
	return true;
}

/// Check if any expression in the given list contains a non-deterministic function.
static bool HasVolatileExpression(vector<unique_ptr<Expression>> &expressions) {
	for (auto &expr : expressions) {
		bool found_volatile = false;
		ExpressionIterator::EnumerateExpression(expr, [&](Expression &child) {
			if (child.expression_class == ExpressionClass::BOUND_FUNCTION) {
				auto &func = child.Cast<BoundFunctionExpression>();
				if (func.function.GetStability() != FunctionStability::CONSISTENT) {
					found_volatile = true;
				}
			}
		});
		if (found_volatile) {
			return true;
		}
	}
	return false;
}

/// Single-pass recursive plan analysis: validates IVM compatibility AND extracts metadata.
static void AnalyzeNode(LogicalOperator *node, PlanAnalysis &result) {
	switch (node->type) {
	// Infrastructure nodes — always compatible, no metadata
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		break;

	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		// After the parser runs CTEInlining (with CTE_MATERIALIZE_ALWAYS → DEFAULT), most
		// query-bound CTEs get inlined into the outer plan. Any MATERIALIZED_CTE remaining
		// here couldn't be inlined (recursive, multi-ref with aggregate, etc.). Analyze
		// the outer first; inherit the CTE body's aggregate only when the outer is a pure
		// pass-through, otherwise classify from the outer alone.
		if (node->children.size() >= 2) {
			AnalyzeNode(node->children[1].get(), result);
			if (!result.found_aggregation && !result.found_distinct && !result.found_join) {
				AnalyzeNode(node->children[0].get(), result);
			}
		}
		return;

	case LogicalOperatorType::LOGICAL_FILTER:
		// Check for volatile functions
		if (HasVolatileExpression(node->expressions)) {
			result.ivm_compatible = false;
		}
		// Detect HAVING: a FILTER above an AGGREGATE
		if (!node->children.empty() && node->children[0]->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			result.found_having = true;
		}
		break;

	case LogicalOperatorType::LOGICAL_PROJECTION:
		result.found_projection = true;
		if (HasVolatileExpression(node->expressions)) {
			result.ivm_compatible = false;
		}
		break;

	case LogicalOperatorType::LOGICAL_UNION:
		if (HasVolatileExpression(node->expressions)) {
			result.ivm_compatible = false;
		}
		break;

	case LogicalOperatorType::LOGICAL_DISTINCT: {
		result.found_distinct = true;
		if (HasVolatileExpression(node->expressions)) {
			result.ivm_compatible = false;
		}
		// DISTINCT columns become group-by keys after IVM rewrite
		auto *distinct_node = dynamic_cast<LogicalDistinct *>(node);
		if (distinct_node && !distinct_node->distinct_targets.empty()) {
			for (auto &target : distinct_node->distinct_targets) {
				result.aggregate_columns.emplace_back(target->GetName());
			}
		} else {
			// Plain DISTINCT: all child output columns are keys
			for (idx_t i = 0; i < node->children[0]->types.size(); i++) {
				result.aggregate_columns.emplace_back("col" + to_string(i));
			}
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		result.found_join = true;
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join) {
			if (join->join_type != JoinType::INNER && join->join_type != JoinType::LEFT &&
			    join->join_type != JoinType::RIGHT && join->join_type != JoinType::OUTER) {
				result.ivm_compatible = false;
			}
			if (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT ||
			    join->join_type == JoinType::OUTER) {
				result.found_left_join = true;
			}
			if (join->join_type == JoinType::OUTER) {
				result.found_full_outer = true;
			}
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		result.found_aggregation = true;
		auto *agg = dynamic_cast<LogicalAggregate *>(node);
		if (agg) {
			// GROUPING SETS / ROLLUP / CUBE produce multiple grouping_sets entries.
			// Routed to RECOMPUTE (full DELETE+INSERT with empty-delta skip) in the parser.
			if (agg->grouping_sets.size() > 1) {
				result.found_grouping_sets = true;
			}
			for (auto &expr : agg->expressions) {
				if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
					auto &bound_agg = expr->Cast<BoundAggregateExpression>();
					const auto &supported = GetSupportedAggregates();
					if (supported.find(bound_agg.function.name) == supported.end()) {
						result.ivm_compatible = false;
					}
					// COUNT(DISTINCT x) can't be summed from delta counts, but group-recompute
					// (delete affected groups + re-insert from original query) is correct.
					if (bound_agg.IsDistinct()) {
						result.found_count_distinct = true;
					}
					// FILTER (WHERE predicate) is normalized to CASE WHEN p THEN x END
					// by RewriteAggregateFilters before the checker runs. If a FILTER reaches
					// here it means the rewrite didn't fire (unexpected plan shape) — fall back
					// to full refresh rather than producing wrong incremental results.
					if (bound_agg.filter) {
						result.ivm_compatible = false;
					}
					if (bound_agg.function.name == "min" || bound_agg.function.name == "max" ||
					    bound_agg.function.name == "arg_min" || bound_agg.function.name == "arg_max") {
						result.found_minmax = true;
					}
					// LIST aggregates aren't element-wise summable — different deltas
					// produce lists of different lengths/contents. Mark the view so the
					// upsert compiler uses group-recompute (delete affected groups,
					// re-insert from the original query) rather than the failing
					// list_reduce/lambda path.
					if (bound_agg.function.name == "list") {
						result.found_list = true;
					}
					result.aggregate_types.push_back(bound_agg.function.name);
				}
			}
			if (HasVolatileExpression(agg->groups)) {
				result.ivm_compatible = false;
			}
			// Record group count and group_index — caller walks the plan's projection to extract
			// the final column names for GROUP BY columns (SELECT list order != GROUP BY order).
			result.group_count = agg->groups.size();
			result.group_index = agg->group_index;
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_WINDOW: {
		result.found_window = true;
		auto &window = node->Cast<LogicalWindow>();
		// Extract PARTITION BY column names from ALL window expressions.
		// Different window functions may use different PARTITION BY clauses —
		// collect the union of all partition columns so any change triggers recompute.
		for (auto &expr : window.expressions) {
			if (expr->expression_class == ExpressionClass::BOUND_WINDOW) {
				auto &win_expr = expr->Cast<BoundWindowExpression>();
				for (auto &part : win_expr.partitions) {
					string col_name;
					if (part->type == ExpressionType::BOUND_COLUMN_REF) {
						col_name = part->Cast<BoundColumnRefExpression>().alias;
					}
					if (col_name.empty()) {
						col_name = part->GetName();
					}
					// Avoid duplicates
					bool found = false;
					for (auto &existing : result.window_partition_columns) {
						if (existing == col_name) {
							found = true;
							break;
						}
					}
					if (!found) {
						result.window_partition_columns.push_back(col_name);
					}
				}
			}
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_TOP_N: {
		// TOP_N fuses ORDER BY + LIMIT. Capture both for top-k suffix handling.
		auto &top_n = node->Cast<LogicalTopN>();
		result.found_top_k = true;
		result.top_k_limit = top_n.limit;
		result.top_k_offset = top_n.offset;
		if (!ExtractOrderBy(top_n.orders, result)) {
			result.ivm_compatible = false; // non-column ORDER BY: fall through to FULL_REFRESH
		}
		if (!node->children.empty()) {
			AnalyzeNode(node->children[0].get(), result);
		}
		return;
	}

	case LogicalOperatorType::LOGICAL_ORDER_BY: {
		// ORDER BY alone (without LIMIT) is meaningless on an MV (a table has no inherent
		// order). The IvmTopKRule drops the node from the delta plan; we still capture
		// the order columns so a sibling LIMIT below this node — see plan shapes where
		// LPTS keeps them as separate ORDER_BY → LIMIT nodes — has them available for
		// top-k suffix handling.
		auto &order = node->Cast<LogicalOrder>();
		if (result.top_k_order_columns.empty()) {
			(void)ExtractOrderBy(order.orders, result);
		}
		if (!node->children.empty()) {
			AnalyzeNode(node->children[0].get(), result);
		}
		return;
	}

	case LogicalOperatorType::LOGICAL_LIMIT: {
		// LPTS disables the top_n optimizer so ORDER BY + LIMIT appear as separate nodes.
		// LIMIT is what makes a query top-k; ORDER BY alone does not.
		result.found_top_k = true;
		auto *limit_node = dynamic_cast<LogicalLimit *>(node);
		if (limit_node) {
			if (limit_node->limit_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				result.top_k_limit = limit_node->limit_val.GetConstantValue();
			}
			if (limit_node->offset_val.Type() == LimitNodeType::CONSTANT_VALUE) {
				result.top_k_offset = limit_node->offset_val.GetConstantValue();
			}
		}
		if (!node->children.empty()) {
			AnalyzeNode(node->children[0].get(), result);
		}
		return;
	}

	default:
		// Unsupported operator type
		result.ivm_compatible = false;
		break;
	}

	for (auto &child : node->children) {
		AnalyzeNode(child.get(), result);
	}
}

PlanAnalysis AnalyzePlan(LogicalOperator *plan) {
	PlanAnalysis result;
	AnalyzeNode(plan, result);
	return result;
}

bool ValidateIVMPlan(LogicalOperator *plan) {
	return AnalyzePlan(plan).ivm_compatible;
}

} // namespace duckdb
