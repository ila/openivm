#ifndef IVM_VIEW_CLASSIFIER_HPP
#define IVM_VIEW_CLASSIFIER_HPP

#include "core/openivm_constants.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/refresh_metadata.hpp"

namespace duckdb {

enum class DeltaStrategyReason {
	UNION_OVER_AGGREGATE,
	JOIN_KEY_GROUP_FALLBACK,
	DELIM_AGGREGATE_GROUP_FALLBACK,
	SCALAR_DELIM_PROJECTION_GROUP_FALLBACK,
	JOIN_AGGREGATE_PROJECTION_FALLBACK,
	NESTED_AGGREGATE_GROUP_FALLBACK,
	REPEATED_CTE_AGGREGATE_GROUP_FALLBACK,
	OUTER_JOIN_AGGREGATE_RECOMPUTE
};

struct FilteredGroupCountAuxRequirement {
	RefreshMetadata::FilteredGroupCountAuxMeta meta;
	string create_source;
};

struct DeltaViewModelInput {
	const CreateMVPlanFacts *facts = nullptr;
	const vector<string> *output_names = nullptr;
	const RefreshMetadata::DistinctAuxMeta *distinct_aux_candidate = nullptr;
	const FilteredGroupCountAuxRequirement *filtered_group_count_aux_candidate = nullptr;
	const RefreshMetadata::SemiAntiAuxMeta *semi_anti_aux_candidate = nullptr;
	bool has_unsupported_incremental_construct = false;
	bool keep_window_join_partitions = true;
};

struct DeltaViewModel {
	RefreshType type = RefreshType::FULL_REFRESH;
	vector<DeltaStrategyReason> strategy_reasons;
	vector<string> group_columns;
	vector<string> window_partition_columns;
	vector<string> aggregate_types;
	vector<string> lineage_entries;
	string full_outer_join_cols;
	RefreshMetadata::DistinctAuxMeta distinct_aux;
	FilteredGroupCountAuxRequirement filtered_group_count_aux;
	RefreshMetadata::SemiAntiAuxMeta semi_anti_aux;
	bool has_minmax_metadata = false;
	bool distinct_at_top = false;
	bool union_distinct_over_agg = false;
	bool warn_unsupported_incremental = false;
	bool warn_unrecognized_pattern = false;

	bool HasDistinctAux() const {
		return !distinct_aux.aux_table.empty();
	}
	bool HasFilteredGroupCountAux() const {
		return !filtered_group_count_aux.meta.aux_table.empty();
	}
	bool HasSemiAntiAux() const {
		return !semi_anti_aux.aux_table.empty();
	}
};

const char *DeltaStrategyReasonName(DeltaStrategyReason reason);
bool IsDistinctAtTop(const PlanAnalysis &analysis, const vector<string> &output_names);
DeltaViewModel BuildDeltaViewModel(const DeltaViewModelInput &input);
void PopulateDeltaViewModelLineage(DeltaViewModel &model, const CreateMVPlanFacts &facts,
                                   const vector<string> &output_names);

} // namespace duckdb

#endif // IVM_VIEW_CLASSIFIER_HPP
