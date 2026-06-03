#include "delta/delta_operator.hpp"

#include "core/openivm_debug.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

namespace {

static void ValidateCompileNode(const DeltaModelNode &node, LogicalOperator *op) {
	D_ASSERT(op);
	D_ASSERT(node.plan_node == op);
	D_ASSERT(node.id != DConstants::INVALID_INDEX);
	if (node.kind == DeltaModelNodeKind::SCAN) {
		auto *get = dynamic_cast<LogicalGet *>(op);
		if (get && node.source_table_index != DConstants::INVALID_INDEX) {
			D_ASSERT(get->table_index == node.source_table_index);
		}
	}
	OPENIVM_DEBUG_PRINT("[IR Rewrite] node=%llu kind=%s rule=%s maintenance=%s/%s sources=%zu outputs=%zu hidden=%zu "
	                    "children=%zu domains=%zu lineage=%zu semantics=%zu unsupported=%zu aux=%zu\n",
	                    (unsigned long long)node.id, DeltaModelNodeKindName(node.kind), DeltaRuleKindName(node.rule),
	                    DeltaMaintenanceModeName(node.maintenance.mode),
	                    DeltaMaintenanceStateKindName(node.maintenance.state), node.source_tables.size(),
	                    node.output_columns.size(), node.hidden_columns.size(), node.children.size(),
	                    node.affected_domains.size(), node.lineage_facts.size(), node.update_semantics.size(),
	                    node.unsupported_reasons.size(), node.required_aux_states.size());
}

static bool IsDelimJoinShape(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_DELIM_JOIN || type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN;
}

} // namespace

const char *DeltaOperatorStrategyName(DeltaOperatorStrategy strategy) {
	switch (strategy) {
	case DeltaOperatorStrategy::SCAN_DELTA:
		return "SCAN_DELTA";
	case DeltaOperatorStrategy::FILTER_LINEAR:
		return "FILTER_LINEAR";
	case DeltaOperatorStrategy::FILTER_HAVING_STRIP:
		return "FILTER_HAVING_STRIP";
	case DeltaOperatorStrategy::PROJECTION_APPEND_MULTIPLICITY:
		return "PROJECTION_APPEND_MULTIPLICITY";
	case DeltaOperatorStrategy::AGGREGATE_GROUP_BY_MULTIPLICITY:
		return "AGGREGATE_GROUP_BY_MULTIPLICITY";
	case DeltaOperatorStrategy::JOIN_INCLUSION_EXCLUSION:
		return "JOIN_INCLUSION_EXCLUSION";
	case DeltaOperatorStrategy::JOIN_DUCKLAKE_N_TERM:
		return "JOIN_DUCKLAKE_N_TERM";
	case DeltaOperatorStrategy::DELIM_JOIN_INCLUSION_EXCLUSION:
		return "DELIM_JOIN_INCLUSION_EXCLUSION";
	case DeltaOperatorStrategy::UNION_ALL_LINEAR:
		return "UNION_ALL_LINEAR";
	case DeltaOperatorStrategy::DISTINCT_COUNT_AGGREGATE:
		return "DISTINCT_COUNT_AGGREGATE";
	case DeltaOperatorStrategy::WINDOW_PASSTHROUGH:
		return "WINDOW_PASSTHROUGH";
	case DeltaOperatorStrategy::TOPK_STRIP:
		return "TOPK_STRIP";
	case DeltaOperatorStrategy::CTE_MATERIALIZED:
		return "CTE_MATERIALIZED";
	case DeltaOperatorStrategy::CTE_REF:
		return "CTE_REF";
	case DeltaOperatorStrategy::UNNEST_LINEAR:
		return "UNNEST_LINEAR";
	case DeltaOperatorStrategy::CONSTANT_ZERO_DELTA:
		return "CONSTANT_ZERO_DELTA";
	case DeltaOperatorStrategy::CONSTANT_STATIC:
		return "CONSTANT_STATIC";
	default:
		return "UNKNOWN";
	}
}

void LogDeltaOperatorStrategy(const DeltaOperatorInput &input, DeltaOperatorStrategy strategy) {
	if (input.node) {
		OPENIVM_DEBUG_PRINT("[Delta Operator] strategy=%s node=%llu kind=%s rule=%s maintenance=%s/%s\n",
		                    DeltaOperatorStrategyName(strategy), (unsigned long long)input.node->id,
		                    DeltaModelNodeKindName(input.node->kind), DeltaRuleKindName(input.node->rule),
		                    DeltaMaintenanceModeName(input.node->maintenance.mode),
		                    DeltaMaintenanceStateKindName(input.node->maintenance.state));
		return;
	}
	OPENIVM_DEBUG_PRINT("[Delta Operator] strategy=%s shape=%s\n", DeltaOperatorStrategyName(strategy),
	                    LogicalOperatorToString(input.plan->type).c_str());
}

DeltaPlanFragment CompileDeltaOperatorWithModel(DeltaOperatorInput input, const DeltaModelNode &node) {
	ValidateCompileNode(node, input.plan.get());
	auto node_input = input.WithNode(node);
	switch (node.kind) {
	case DeltaModelNodeKind::SCAN:
		return CompileScanDelta(node_input);
	case DeltaModelNodeKind::FILTER:
		return CompileFilterDelta(node_input);
	case DeltaModelNodeKind::PROJECT:
		return CompileProjectionDelta(node_input);
	case DeltaModelNodeKind::AGGREGATE:
		return CompileAggregateDelta(node_input);
	case DeltaModelNodeKind::JOIN:
	case DeltaModelNodeKind::SEMI_ANTI:
		return IsDelimJoinShape(node_input.plan->type) ? CompileDelimJoinDelta(node_input)
		                                               : CompileJoinDelta(node_input);
	case DeltaModelNodeKind::UNION:
		return CompileUnionDelta(node_input);
	case DeltaModelNodeKind::DISTINCT:
		return CompileDistinctDelta(node_input);
	case DeltaModelNodeKind::WINDOW:
		return CompileWindowDelta(node_input);
	case DeltaModelNodeKind::TOP_K:
		return CompileTopKDelta(node_input);
	case DeltaModelNodeKind::CTE:
		return CompileCteDelta(node_input);
	case DeltaModelNodeKind::UNNEST:
		return CompileUnnestDelta(node_input);
	case DeltaModelNodeKind::CONSTANT:
		return CompileConstantZeroDelta(node_input);
	case DeltaModelNodeKind::OTHER:
		throw NotImplementedException("Delta model node kind %s not supported", DeltaModelNodeKindName(node.kind));
	default:
		throw NotImplementedException("Delta model node kind %s not supported", DeltaModelNodeKindName(node.kind));
	}
}

DeltaPlanFragment CompileCopiedDeltaSubtree(DeltaOperatorInput input) {
	OPENIVM_DEBUG_PRINT("[Copied Delta Subtree] Visiting node: %s\n",
	                    LogicalOperatorToString(input.plan->type).c_str());
	OPENIVM_DEBUG_PRINT("[Copied Delta Subtree] Node detail: %s\n", input.plan->ToString().c_str());

	switch (input.plan->type) {
	case LogicalOperatorType::LOGICAL_GET:
		return CompileScanDelta(input);
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
		return CompileJoinDelta(input);
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
		return CompileDelimJoinDelta(input);
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return CompileProjectionDelta(input);
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return CompileAggregateDelta(input);
	case LogicalOperatorType::LOGICAL_FILTER:
		return CompileFilterDelta(input);
	case LogicalOperatorType::LOGICAL_UNION:
		return CompileUnionDelta(input);
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return CompileDistinctDelta(input);
	case LogicalOperatorType::LOGICAL_WINDOW:
		return CompileWindowDelta(input);
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return CompileTopKDelta(input);
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
	case LogicalOperatorType::LOGICAL_CTE_REF:
		return CompileCteDelta(input);
	case LogicalOperatorType::LOGICAL_UNNEST:
		return CompileUnnestDelta(input);
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		return CompileStaticConstantLeaf(input);
	default:
		throw NotImplementedException("Copied subtree operator type %s not supported",
		                              LogicalOperatorToString(input.plan->type));
	}
}

DeltaPlanFragment CompileNonModelLeaf(DeltaOperatorInput input) {
	switch (input.plan->type) {
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		OPENIVM_DEBUG_PRINT("[Delta Compiler] non-model constant leaf %s\n",
		                    LogicalOperatorToString(input.plan->type).c_str());
		return CompileConstantZeroDelta(input);
	default:
		throw InternalException("Delta compiler missing IR node for operator %s",
		                        LogicalOperatorToString(input.plan->type).c_str());
	}
}

} // namespace duckdb
