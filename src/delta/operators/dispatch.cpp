#include "delta/delta_operator.hpp"

#include "core/openivm_debug.hpp"

namespace duckdb {

namespace {

static bool IsDelimJoinShape(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_DELIM_JOIN || type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
}

static const char *DeltaOperatorStrategyName(DeltaOperatorStrategy strategy) {
#define OPENIVM_DELTA_OPERATOR_STRATEGY_CASE(name)                                                                     \
	case DeltaOperatorStrategy::name:                                                                                  \
		return #name;
	switch (strategy) { OPENIVM_DELTA_OPERATOR_STRATEGIES(OPENIVM_DELTA_OPERATOR_STRATEGY_CASE) }
#undef OPENIVM_DELTA_OPERATOR_STRATEGY_CASE
	return "UNKNOWN";
}

} // namespace

void LogDeltaOperatorStrategy(const DeltaOperatorInput &input, DeltaOperatorStrategy strategy) {
	// Both branches are debug-only descriptions of the already-selected strategy.
	// mull-ignore-next
	if (input.node) {
		OPENIVM_DEBUG_PRINT("[Delta Operator] strategy=%s kind=%s\n", DeltaOperatorStrategyName(strategy),
		                    DeltaModelNodeKindName(input.node->kind));
		return;
	}
	OPENIVM_DEBUG_PRINT("[Delta Operator] strategy=%s shape=%s\n", DeltaOperatorStrategyName(strategy),
	                    LogicalOperatorToString(input.plan->type).c_str());
}

DeltaPlanFragment CompileDeltaOperatorByKind(const DeltaOperatorInput &input, DeltaModelNodeKind kind) {
	D_ASSERT(!input.node || input.node->plan_node == input.plan.get());
	switch (kind) {
	case DeltaModelNodeKind::SCAN:
		return CompileScanDelta(input);
	case DeltaModelNodeKind::FILTER:
		return CompileFilterDelta(input);
	case DeltaModelNodeKind::PROJECT:
		return CompileProjectionDelta(input);
	case DeltaModelNodeKind::AGGREGATE:
		return CompileAggregateDelta(input);
	case DeltaModelNodeKind::JOIN:
	case DeltaModelNodeKind::SEMI_ANTI:
		return IsDelimJoinShape(input.plan->type) ? CompileDelimJoinDelta(input) : CompileJoinDelta(input);
	case DeltaModelNodeKind::UNION:
		return CompileUnionDelta(input);
	case DeltaModelNodeKind::DISTINCT:
		return CompileDistinctDelta(input);
	case DeltaModelNodeKind::WINDOW:
		return CompileWindowDelta(input);
	case DeltaModelNodeKind::TOP_K:
		return CompileTopKDelta(input);
	case DeltaModelNodeKind::CTE:
		return CompileCteDelta(input);
	case DeltaModelNodeKind::UNNEST:
		return CompileUnnestDelta(input);
	case DeltaModelNodeKind::CONSTANT:
		return input.copied_subtree ? CompileStaticConstantLeaf(input) : CompileConstantZeroDelta(input);
	case DeltaModelNodeKind::ASOF_JOIN:
		return CompileAsofJoinDelta(input);
	case DeltaModelNodeKind::POSITIONAL_JOIN:
		return CompilePositionalJoinDelta(input);
	case DeltaModelNodeKind::SAMPLE:
		return CompileSampleDelta(input);
	default:
		throw NotImplementedException("Delta operator %s (node kind %s) not supported",
		                              LogicalOperatorToString(input.plan->type), DeltaModelNodeKindName(kind));
	}
}

} // namespace duckdb
