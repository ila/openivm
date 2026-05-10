#ifndef IVM_VIEW_CLASSIFIER_HPP
#define IVM_VIEW_CLASSIFIER_HPP

#include "core/ivm_plan_analysis.hpp"
#include "core/openivm_constants.hpp"

namespace duckdb {

enum class DeltaStrategyReason {
	UNION_OVER_AGGREGATE,
	JOIN_KEY_GROUP_FALLBACK,
	DELIM_AGGREGATE_GROUP_FALLBACK,
	SCALAR_DELIM_PROJECTION_GROUP_FALLBACK,
	JOIN_AGGREGATE_PROJECTION_FALLBACK,
	NESTED_AGGREGATE_GROUP_FALLBACK,
	REPEATED_CTE_AGGREGATE_GROUP_FALLBACK
};

struct DeltaViewClassificationInput {
	PlanAnalysis analysis;
	bool has_unsupported_set_operation = false;
	bool has_unsupported_incremental_construct = false;
	bool has_aggregate_columns = false;
	bool distinct_at_top = false;
	bool union_distinct_over_agg = false;
	vector<DeltaStrategyReason> strategy_reasons;
	bool distinct_incremental_supported = false;
	bool semi_anti_recompute_supported = false;
};

struct DeltaViewModelInput {
	PlanAnalysis analysis;
	LogicalOperator *plan = nullptr;
	vector<string> output_names;
	bool has_unsupported_incremental_construct = false;
	bool distinct_incremental_supported = false;
	bool semi_anti_recompute_supported = false;
	bool keep_window_join_partitions = false;
};

struct DeltaViewClassification {
	IVMType type = IVMType::FULL_REFRESH;
	bool warn_unsupported_incremental = false;
	bool warn_unrecognized_pattern = false;
};

struct DeltaViewModel {
	IVMType type = IVMType::FULL_REFRESH;
	vector<DeltaStrategyReason> strategy_reasons;
	vector<string> group_columns;
	vector<string> window_partition_columns;
	vector<string> aggregate_types;
	string full_outer_join_cols;
	bool has_minmax_metadata = false;
	bool distinct_at_top = false;
	bool union_distinct_over_agg = false;
	bool warn_unsupported_incremental = false;
	bool warn_unrecognized_pattern = false;
};

const char *DeltaStrategyReasonName(DeltaStrategyReason reason);
const char *IVMTypeName(IVMType type);
bool IsDecomposedAggregateType(const string &aggregate_type);
bool IsArgMinMaxAggregateType(const string &aggregate_type);
bool IsExtremumAggregateType(const string &aggregate_type);
bool HasGroupRecomputeReason(const vector<DeltaStrategyReason> &reasons);
bool IsDistinctAtTop(const PlanAnalysis &analysis, const vector<string> &output_names);
DeltaViewClassification ClassifyDeltaView(const DeltaViewClassificationInput &input);
DeltaViewModel BuildDeltaViewModel(const DeltaViewClassificationInput &input);
DeltaViewModel BuildDeltaViewModel(DeltaViewModelInput input);

} // namespace duckdb

#endif // IVM_VIEW_CLASSIFIER_HPP
