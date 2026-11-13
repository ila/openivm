#ifndef OPENIVM_DELTA_OPERATOR_HPP
#define OPENIVM_DELTA_OPERATOR_HPP

#include "delta/delta_compiler.hpp"
#include "delta/delta_helpers.hpp"
#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

#define OPENIVM_DELTA_OPERATOR_STRATEGIES(X)                                                                           \
	X(SCAN_DELTA)                                                                                                      \
	X(FILTER_LINEAR)                                                                                                   \
	X(FILTER_HAVING_STRIP)                                                                                             \
	X(PROJECTION_APPEND_MULTIPLICITY)                                                                                  \
	X(AGGREGATE_GROUP_BY_MULTIPLICITY)                                                                                 \
	X(JOIN_INCLUSION_EXCLUSION)                                                                                        \
	X(JOIN_REGULAR_N_TERM)                                                                                             \
	X(JOIN_DUCKLAKE_N_TERM)                                                                                            \
	X(DELIM_JOIN_INCLUSION_EXCLUSION)                                                                                  \
	X(UNION_ALL_LINEAR)                                                                                                \
	X(DISTINCT_COUNT_AGGREGATE)                                                                                        \
	X(WINDOW_PASSTHROUGH)                                                                                              \
	X(TOPK_STRIP)                                                                                                      \
	X(CTE_MATERIALIZED)                                                                                                \
	X(CTE_REF)                                                                                                         \
	X(UNNEST_LINEAR)                                                                                                   \
	X(CONSTANT_ZERO_DELTA)                                                                                             \
	X(CONSTANT_STATIC)                                                                                                 \
	X(ASOF_AFFECTED_RECOMPUTE)                                                                                         \
	X(POSITIONAL_GLOBAL_RECOMPUTE)                                                                                     \
	X(SAMPLE_GLOBAL_RECOMPUTE)

#define OPENIVM_DELTA_OPERATOR_STRATEGY_ENTRY(name) name,
enum class DeltaOperatorStrategy { OPENIVM_DELTA_OPERATOR_STRATEGIES(OPENIVM_DELTA_OPERATOR_STRATEGY_ENTRY) };
#undef OPENIVM_DELTA_OPERATOR_STRATEGY_ENTRY

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

void LogDeltaOperatorStrategy(const DeltaOperatorInput &input, DeltaOperatorStrategy strategy);

// Dispatches to the operator handler for `kind`. A CONSTANT leaf compiles to a static leaf inside a copied subtree and
// to a zero delta otherwise.
DeltaPlanFragment CompileDeltaOperatorByKind(const DeltaOperatorInput &input, DeltaModelNodeKind kind);

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
