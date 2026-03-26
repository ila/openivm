#include "core/ivm_checker.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

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

/// Recursively check if the plan tree is fully IVM-compatible.
static bool CheckNode(LogicalOperator *node) {
	switch (node->type) {
	// Infrastructure nodes from CREATE TABLE AS SELECT
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_GET:
		break;

	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
		// Check for volatile functions in expressions (e.g., RANDOM(), NOW())
		if (HasVolatileExpression(node->expressions)) {
			return false;
		}
		break;

	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN: {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join && join->join_type != JoinType::INNER) {
			return false;
		}
		break;
	}

	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		auto *agg = dynamic_cast<LogicalAggregate *>(node);
		if (agg) {
			for (auto &expr : agg->expressions) {
				if (expr->expression_class == ExpressionClass::BOUND_AGGREGATE) {
					auto *bound_agg = dynamic_cast<BoundAggregateExpression *>(expr.get());
					if (bound_agg &&
					    SUPPORTED_AGGREGATES.find(bound_agg->function.name) == SUPPORTED_AGGREGATES.end()) {
						return false;
					}
				}
			}
			// Check for volatile functions in group-by expressions
			if (HasVolatileExpression(agg->groups)) {
				return false;
			}
		}
		break;
	}

	default:
		// Unsupported operator type
		return false;
	}

	for (auto &child : node->children) {
		if (!CheckNode(child.get())) {
			return false;
		}
	}
	return true;
}

bool ValidateIVMPlan(LogicalOperator *plan) {
	return CheckNode(plan);
}

} // namespace duckdb
