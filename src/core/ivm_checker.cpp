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

#include <unordered_set>

namespace duckdb {

static const unordered_set<string> SUPPORTED_AGGREGATES = {
    "count_star", "count",       "sum",        "min",      "max",      "avg",    "list",
    "stddev",     "stddev_samp", "stddev_pop", "variance", "var_samp", "var_pop"};

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
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		break;

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
			// Our delta path groups once; can't produce the cross-grouped rows.
			if (agg->grouping_sets.size() > 1) {
				result.ivm_compatible = false;
			}
			for (auto &expr : agg->expressions) {
				if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
					auto &bound_agg = expr->Cast<BoundAggregateExpression>();
					if (SUPPORTED_AGGREGATES.find(bound_agg.function.name) == SUPPORTED_AGGREGATES.end()) {
						result.ivm_compatible = false;
					}
					// COUNT(DISTINCT x) can't be maintained by summing delta counts — a
					// delta row's value might already be in the MV (no change) or not
					// (+1). Deciding requires auxiliary per-value state.
					if (bound_agg.IsDistinct()) {
						result.ivm_compatible = false;
					}
					// FILTER (WHERE predicate) aggregates aren't captured by the delta
					// path either — the FILTER predicate interacts with delta rows in
					// ways the delta-sum formula can't reproduce.
					if (bound_agg.filter) {
						result.ivm_compatible = false;
					}
					if (bound_agg.function.name == "min" || bound_agg.function.name == "max") {
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
