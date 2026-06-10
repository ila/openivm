#ifndef OPENIVM_DELTA_OPERATOR_HPP
#define OPENIVM_DELTA_OPERATOR_HPP

#include "delta/delta_compiler.hpp"
#include "delta/delta_helpers.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

enum class DeltaOperatorStrategy {
	SCAN_DELTA,
	FILTER_LINEAR,
	FILTER_HAVING_STRIP,
	PROJECTION_APPEND_MULTIPLICITY,
	AGGREGATE_GROUP_BY_MULTIPLICITY,
	JOIN_INCLUSION_EXCLUSION,
	JOIN_DUCKLAKE_N_TERM,
	DELIM_JOIN_INCLUSION_EXCLUSION,
	UNION_ALL_LINEAR,
	DISTINCT_COUNT_AGGREGATE,
	WINDOW_PASSTHROUGH,
	TOPK_STRIP,
	CTE_MATERIALIZED,
	CTE_REF,
	UNNEST_LINEAR,
	CONSTANT_ZERO_DELTA,
	CONSTANT_STATIC,
	ASOF_AFFECTED_RECOMPUTE,
	POSITIONAL_GLOBAL_RECOMPUTE,
	SAMPLE_GLOBAL_RECOMPUTE
};

struct DeltaOperatorInput {
	DeltaOperatorInput(DeltaCompileContext &context_, unique_ptr<LogicalOperator> &plan_, LogicalOperator *&root_,
	                   DeltaCompiler *compiler_, bool copied_subtree_, const DeltaModelNode *node_ = nullptr)
	    : context(context_), plan(plan_), root(root_), compiler(compiler_), copied_subtree(copied_subtree_),
	      node(node_) {
	}

	DeltaOperatorInput WithNode(const DeltaModelNode &node_) const {
		auto result = *this;
		result.node = &node_;
		return result;
	}

	DeltaPlanFragment CompileChild(unique_ptr<LogicalOperator> &child, LogicalOperator *&child_root) const;
	DeltaPlanFragment CompileCopiedSubtree(unique_ptr<LogicalOperator> &subtree, LogicalOperator *&subtree_root) const;

	DeltaCompileContext &context;
	unique_ptr<LogicalOperator> &plan;
	LogicalOperator *&root;
	DeltaCompiler *compiler;
	bool copied_subtree;
	const DeltaModelNode *node;
	const LogicalType mul_type = LogicalType::INTEGER;
};

const char *DeltaOperatorStrategyName(DeltaOperatorStrategy strategy);
void LogDeltaOperatorStrategy(const DeltaOperatorInput &input, DeltaOperatorStrategy strategy);

DeltaPlanFragment CompileDeltaOperatorWithModel(const DeltaOperatorInput &input, const DeltaModelNode &node);
DeltaPlanFragment CompileCopiedDeltaSubtree(DeltaOperatorInput input);
DeltaPlanFragment CompileNonModelLeaf(DeltaOperatorInput input);

DeltaPlanFragment CompileScanDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileFilterDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileProjectionDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileAggregateDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileJoinDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileDelimJoinDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileUnionDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileDistinctDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileWindowDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileTopKDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileCteDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileUnnestDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileConstantZeroDelta(DeltaOperatorInput input);
DeltaPlanFragment CompileStaticConstantLeaf(DeltaOperatorInput input);
DeltaPlanFragment CompileAsofJoinDelta(const DeltaOperatorInput &input);
DeltaPlanFragment CompilePositionalJoinDelta(const DeltaOperatorInput &input);
DeltaPlanFragment CompileSampleDelta(const DeltaOperatorInput &input);

} // namespace duckdb

#endif // OPENIVM_DELTA_OPERATOR_HPP
