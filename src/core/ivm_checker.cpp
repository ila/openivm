#include "core/ivm_checker.hpp"

#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

#include <unordered_set>

namespace duckdb {

static const unordered_set<string> SUPPORTED_AGGREGATES = {"count_star", "count", "sum", "min", "max", "avg", "list"};

static void ValidateNode(LogicalOperator *node) {
	switch (node->type) {
	// Infrastructure nodes from CREATE TABLE AS SELECT — skip validation, recurse children
	case LogicalOperatorType::LOGICAL_CREATE_TABLE:
	case LogicalOperatorType::LOGICAL_INSERT:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		break;

	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_FILTER:
	case LogicalOperatorType::LOGICAL_PROJECTION:
		break;

	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN: {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join && join->join_type != JoinType::INNER) {
			throw NotImplementedException("IVM does not support %s joins (only INNER JOIN is supported)",
			                              JoinTypeToString(join->join_type));
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
						throw NotImplementedException(
						    "IVM does not support the '%s' aggregate function (supported: SUM, COUNT, MIN, MAX)",
						    bound_agg->function.name);
					}
				}
			}
		}
		break;
	}

	default:
		throw NotImplementedException("IVM does not support %s operators in materialized view definitions",
		                              LogicalOperatorToString(node->type));
	}

	for (auto &child : node->children) {
		ValidateNode(child.get());
	}
}

void ValidateIVMPlan(LogicalOperator *plan) {
	ValidateNode(plan);
}

} // namespace duckdb
