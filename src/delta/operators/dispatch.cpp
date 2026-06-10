#include "delta/delta_operator.hpp"

#include "core/ivm_delta_model.hpp"
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

// Every delta fragment must expose exactly one multiplicity column as a real output binding — that is the
// compositional contract the parent operator relies on. Enforce it at every dispatch boundary (a real
// exception, not a D_ASSERT, so the invariant holds in release builds too).
static void ValidateDeltaFragment(const DeltaPlanFragment &fragment, LogicalOperatorType op_type,
                                  DeltaModelNodeKind kind) {
	if (!fragment.op) {
		throw InternalException("Delta compile produced a null fragment for %s (kind %s)",
		                        LogicalOperatorToString(op_type), DeltaModelNodeKindName(kind));
	}
	if (fragment.mul_binding.table_index == DConstants::INVALID_INDEX) {
		throw InternalException("Delta fragment for %s (kind %s) has no multiplicity binding",
		                        LogicalOperatorToString(op_type), DeltaModelNodeKindName(kind));
	}
}

// Single source of truth for delta compilation. Both the model-driven path and the copied-subtree path
// route through here keyed on DeltaModelNodeKind, so there is exactly one operator->compiler table to keep
// in sync. The only state-dependent case is a constant leaf: inside a copied "current-base" join term it
// must emit its static content (multiplicity +1), everywhere else it emits an empty (zero) delta.
static DeltaPlanFragment DispatchDeltaCompile(const DeltaOperatorInput &input, DeltaModelNodeKind kind) {
	auto op_type = input.plan->type;
	DeltaPlanFragment fragment = [&]() -> DeltaPlanFragment {
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
			return IsDelimJoinShape(op_type) ? CompileDelimJoinDelta(input) : CompileJoinDelta(input);
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
		case DeltaModelNodeKind::OTHER:
		default:
			throw NotImplementedException("Delta compiler: operator %s (kind %s) not supported",
			                              LogicalOperatorToString(op_type), DeltaModelNodeKindName(kind));
		}
	}();
	ValidateDeltaFragment(fragment, op_type, kind);
	return fragment;
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
	case DeltaOperatorStrategy::ASOF_AFFECTED_RECOMPUTE:
		return "ASOF_AFFECTED_RECOMPUTE";
	case DeltaOperatorStrategy::POSITIONAL_GLOBAL_RECOMPUTE:
		return "POSITIONAL_GLOBAL_RECOMPUTE";
	case DeltaOperatorStrategy::SAMPLE_GLOBAL_RECOMPUTE:
		return "SAMPLE_GLOBAL_RECOMPUTE";
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

DeltaPlanFragment CompileDeltaOperatorWithModel(const DeltaOperatorInput &input, const DeltaModelNode &node) {
	ValidateCompileNode(node, input.plan.get());
	return DispatchDeltaCompile(input.WithNode(node), node.kind);
}

DeltaPlanFragment CompileCopiedDeltaSubtree(DeltaOperatorInput input) {
	OPENIVM_DEBUG_PRINT("[Copied Delta Subtree] Visiting node: %s\n",
	                    LogicalOperatorToString(input.plan->type).c_str());
	OPENIVM_DEBUG_PRINT("[Copied Delta Subtree] Node detail: %s\n", input.plan->ToString().c_str());
	// Copied subtrees (join inclusion-exclusion terms) carry fresh operator pointers that are not in the
	// model, so derive the kind from the operator type — the same mapping the model itself was built from.
	return DispatchDeltaCompile(input, NodeKindForOperator(*input.plan));
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
