#ifndef IVM_DELTA_MODEL_HPP
#define IVM_DELTA_MODEL_HPP

#include "core/ivm_plan_analysis.hpp"

namespace duckdb {

/// DBSP linearity classification of an operator's incremental form.
/// This algebraic taxonomy determines how the delta rule is derived.
enum class Linearity { LINEAR, BILINEAR, NON_LINEAR };

enum class DeltaApplyKind { INCREMENTAL, GROUP_RECOMPUTE, PARTITION_RECOMPUTE, FULL_REFRESH };

enum class DeltaRewriteKind {
	SCAN,
	JOIN,
	DELIM_JOIN,
	PROJECTION,
	AGGREGATE,
	FILTER,
	UNION,
	DISTINCT,
	WINDOW,
	TOPK,
	MATERIALIZED_CTE,
	CTE_REF,
	CONSTANT_LEAF,
	UNSUPPORTED
};

struct DeltaOperatorSpec {
	DeltaOperatorSpec(LogicalOperatorType op_type_, DeltaRewriteKind rewrite_kind_, Linearity linearity_,
	                  DeltaApplyKind apply_kind_, bool preserves_insert_only_, bool needs_source_consolidation_,
	                  bool needs_delta_consolidation_)
	    : op_type(op_type_), rewrite_kind(rewrite_kind_), linearity(linearity_), apply_kind(apply_kind_),
	      preserves_insert_only(preserves_insert_only_), needs_source_consolidation(needs_source_consolidation_),
	      needs_delta_consolidation(needs_delta_consolidation_) {
	}

	LogicalOperatorType op_type;
	DeltaRewriteKind rewrite_kind;
	Linearity linearity;
	DeltaApplyKind apply_kind;
	bool preserves_insert_only = false;
	bool needs_source_consolidation = false;
	bool needs_delta_consolidation = false;
};

DeltaOperatorSpec GetDeltaOperatorSpec(LogicalOperatorType op_type);
DeltaRewriteKind GetDeltaRewriteKind(LogicalOperatorType op_type);

struct DeltaPlanModel {
	PlanAnalysis analysis;
	vector<DeltaOperatorSpec> operators;

	string DebugString() const;
};

DeltaPlanModel BuildDeltaPlanModel(LogicalOperator *plan);

} // namespace duckdb

#endif // IVM_DELTA_MODEL_HPP
