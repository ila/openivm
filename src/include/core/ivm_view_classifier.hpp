#ifndef IVM_VIEW_CLASSIFIER_HPP
#define IVM_VIEW_CLASSIFIER_HPP

#include "core/openivm_constants.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/refresh_metadata.hpp"

namespace duckdb {

#define OPENIVM_DELTA_STRATEGY_REASONS(X)                                                                              \
	X(UNION_OVER_AGGREGATE)                                                                                            \
	X(UNION_DISTINCT_GROUP_RECOMPUTE)                                                                                  \
	X(JOIN_KEY_GROUP_FALLBACK)                                                                                         \
	X(DELIM_AGGREGATE_GROUP_FALLBACK)                                                                                  \
	X(SCALAR_DELIM_PROJECTION_GROUP_FALLBACK)                                                                          \
	X(JOIN_AGGREGATE_PROJECTION_FALLBACK)                                                                              \
	X(NESTED_AGGREGATE_GROUP_FALLBACK)                                                                                 \
	X(REPEATED_CTE_AGGREGATE_GROUP_FALLBACK)                                                                           \
	X(SEMI_ANTI_AGGREGATE_GROUP_FALLBACK)                                                                              \
	X(OUTER_JOIN_AGGREGATE_RECOMPUTE)                                                                                  \
	X(OUTER_JOIN_PRESERVED_TABLE_FUNCTION_RECOMPUTE)                                                                   \
	X(INNER_DISTINCT_PROJECTION_RECOMPUTE)

#define OPENIVM_DELTA_MODEL_NODE_KINDS(X)                                                                              \
	X(SCAN)                                                                                                            \
	X(FILTER)                                                                                                          \
	X(PROJECT)                                                                                                         \
	X(JOIN)                                                                                                            \
	X(AGGREGATE)                                                                                                       \
	X(WINDOW)                                                                                                          \
	X(DISTINCT)                                                                                                        \
	X(SEMI_ANTI)                                                                                                       \
	X(UNION)                                                                                                           \
	X(TOP_K)                                                                                                           \
	X(UNNEST)                                                                                                          \
	X(ASOF_JOIN)                                                                                                       \
	X(POSITIONAL_JOIN)                                                                                                 \
	X(SAMPLE)                                                                                                          \
	X(CTE)                                                                                                             \
	X(CONSTANT)                                                                                                        \
	X(OTHER)

#define OPENIVM_ENUM_ENTRY(name) name,
enum class DeltaStrategyReason { OPENIVM_DELTA_STRATEGY_REASONS(OPENIVM_ENUM_ENTRY) };
enum class DeltaModelNodeKind { OPENIVM_DELTA_MODEL_NODE_KINDS(OPENIVM_ENUM_ENTRY) };
#undef OPENIVM_ENUM_ENTRY

struct DeltaModelNode {
	DeltaModelNodeKind kind = DeltaModelNodeKind::OTHER;
	const LogicalOperator *plan_node = nullptr;
};

// CREATE-time node counts reported in the refresh profile (`create_compile_classification`).
struct DeltaModelProfileCounts {
	idx_t scan_occurrences = 0;
	idx_t node_semantics = 0;
	idx_t delta_only_nodes = 0;
	idx_t delta_state_nodes = 0;
};

struct FilteredGroupCountAuxRequirement {
	RefreshMetadata::FilteredGroupCountAuxMeta meta;
	string create_source;
};

struct DeltaViewModelInput {
	const CreateMVPlanFacts *facts = nullptr;
	const vector<string> *output_names = nullptr;
	idx_t visible_output_count = DConstants::INVALID_INDEX;
	const RefreshMetadata::DistinctAuxMeta *distinct_aux_candidate = nullptr;
	const RefreshMetadata::CountDistinctAuxMeta *count_distinct_aux_candidate = nullptr;
	const FilteredGroupCountAuxRequirement *filtered_group_count_aux_candidate = nullptr;
	const RefreshMetadata::SemiAntiAuxMeta *semi_anti_aux_candidate = nullptr;
	bool has_unsupported_incremental_construct = false;
	bool stored_query_has_aggregate_filter = false;
	bool stored_query_retains_aggregate_filter = false;
	bool stored_query_has_top_k = false;
	bool has_hidden_minmax_having = false;
	bool has_computed_minmax_aggregate_projection = false;
	bool has_computed_sum_aggregate_projection = false;
	bool has_top_level_redundant_distinct = false;
	bool has_ducklake_source = false;
};

struct DeltaViewModel {
	RefreshType type = RefreshType::FULL_REFRESH;
	vector<DeltaStrategyReason> strategy_reasons;
	vector<string> group_columns;
	vector<string> window_partition_columns;
	vector<string> window_order_columns;
	vector<string> aggregate_types;
	vector<DeltaModelNode> nodes;
	DeltaModelProfileCounts profile_counts;
	vector<RefreshMetadata::WindowPartitionLineageOp> window_lineage_ops;
	RefreshMetadata::ProjectionKeyLineage projection_lineage;
	RefreshMetadata::LeftJoinKeySource left_join_key_source;
	vector<RefreshMetadata::FullOuterScope> full_outer_scopes;
	RefreshMetadata::LeftJoinNullableSources left_join_nullable_sources;
	idx_t max_table_index = 0;
	string full_outer_join_cols;
	GroupRecomputeAffectedMode group_recompute_affected_mode = GroupRecomputeAffectedMode::SOURCE_DELTA;
	RefreshMetadata::DistinctAuxMeta distinct_aux;
	RefreshMetadata::CountDistinctAuxMeta count_distinct_aux;
	FilteredGroupCountAuxRequirement filtered_group_count_aux;
	RefreshMetadata::SemiAntiAuxMeta semi_anti_aux;
	bool has_minmax_metadata = false;
	bool distinct_at_top = false;
	bool union_distinct_over_agg = false;
	bool has_projection_lineage = false;
	bool has_left_join_key_source = false;
	bool has_left_join_nullable = false;
	bool warn_unsupported_incremental = false;
	bool warn_unrecognized_pattern = false;

	bool HasDistinctAux() const {
		return !distinct_aux.aux_table.empty();
	}
	bool HasCountDistinctAux() const {
		return !count_distinct_aux.aux_table.empty();
	}
	bool HasFilteredGroupCountAux() const {
		return !filtered_group_count_aux.meta.aux_table.empty();
	}
	bool HasSemiAntiAux() const {
		return !semi_anti_aux.aux_table.empty();
	}
	idx_t LineageEntryCount() const;
};

const char *DeltaStrategyReasonName(DeltaStrategyReason reason);
const char *DeltaModelNodeKindName(DeltaModelNodeKind kind);
bool IsDistinctAtTop(const CreateMVPlanFacts &facts, const vector<string> &output_names);
DeltaViewModel BuildDeltaViewModel(const DeltaViewModelInput &input);

} // namespace duckdb

#endif // IVM_VIEW_CLASSIFIER_HPP
