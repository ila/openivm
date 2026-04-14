#include "core/ivm_checker.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

#include <unordered_set>

namespace duckdb {

static const unordered_set<string> SUPPORTED_AGGREGATES = {"count_star", "count", "sum", "min", "max", "avg", "list"};

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
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join) {
			if (join->join_type != JoinType::INNER && join->join_type != JoinType::LEFT &&
			    join->join_type != JoinType::RIGHT) {
				result.ivm_compatible = false;
			}
			if (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT) {
				result.found_left_join = true;
			}
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		result.found_aggregation = true;
		auto *agg = dynamic_cast<LogicalAggregate *>(node);
		if (agg) {
			for (auto &expr : agg->expressions) {
				if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
					auto &bound_agg = expr->Cast<BoundAggregateExpression>();
					if (SUPPORTED_AGGREGATES.find(bound_agg.function.name) == SUPPORTED_AGGREGATES.end()) {
						result.ivm_compatible = false;
					}
					if (bound_agg.function.name == "min" || bound_agg.function.name == "max") {
						result.found_minmax = true;
					}
					result.aggregate_types.push_back(bound_agg.function.name);
				}
			}
			if (HasVolatileExpression(agg->groups)) {
				result.ivm_compatible = false;
			}
			// Extract group-by column names
			for (auto &group : agg->groups) {
				if (group->type == ExpressionType::BOUND_COLUMN_REF) {
					auto *column = dynamic_cast<BoundColumnRefExpression *>(group.get());
					if (column) {
						result.aggregate_columns.emplace_back(column->alias);
					}
				} else if (group->type == ExpressionType::BOUND_FUNCTION) {
					auto *column = dynamic_cast<BoundFunctionExpression *>(group.get());
					if (column) {
						if (!column->alias.empty()) {
							result.aggregate_columns.emplace_back(column->alias);
						} else {
							auto function = column->GetName();
							function = StringUtil::Replace(function, "\"", "\"\"");
							function = "\"" + function + "\"";
							result.aggregate_columns.emplace_back(function);
						}
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
