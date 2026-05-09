#ifndef IVM_DELTA_MODEL_HPP
#define IVM_DELTA_MODEL_HPP

#include "core/ivm_plan_analysis.hpp"
#include "rules/rule.hpp"

namespace duckdb {

enum class DeltaApplyKind { INCREMENTAL, GROUP_RECOMPUTE, PARTITION_RECOMPUTE, FULL_REFRESH };

struct DeltaOperatorSpec {
	DeltaOperatorSpec(LogicalOperatorType op_type_, Linearity linearity_, DeltaApplyKind apply_kind_,
	                  bool preserves_insert_only_, bool needs_source_consolidation_, bool needs_delta_consolidation_)
	    : op_type(op_type_), linearity(linearity_), apply_kind(apply_kind_),
	      preserves_insert_only(preserves_insert_only_), needs_source_consolidation(needs_source_consolidation_),
	      needs_delta_consolidation(needs_delta_consolidation_) {
	}

	LogicalOperatorType op_type;
	Linearity linearity;
	DeltaApplyKind apply_kind;
	bool preserves_insert_only = false;
	bool needs_source_consolidation = false;
	bool needs_delta_consolidation = false;
};

struct DeltaPlanModel {
	PlanAnalysis analysis;
	vector<DeltaOperatorSpec> operators;

	string DebugString() const;
};

DeltaPlanModel BuildDeltaPlanModel(LogicalOperator *plan);

} // namespace duckdb

#endif // IVM_DELTA_MODEL_HPP
