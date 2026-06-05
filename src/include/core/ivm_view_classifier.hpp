#ifndef IVM_VIEW_CLASSIFIER_HPP
#define IVM_VIEW_CLASSIFIER_HPP

#include "core/openivm_constants.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/refresh_metadata.hpp"

namespace duckdb {

enum class DeltaStrategyReason {
	UNION_OVER_AGGREGATE,
	UNION_DISTINCT_GROUP_RECOMPUTE,
	JOIN_KEY_GROUP_FALLBACK,
	DELIM_AGGREGATE_GROUP_FALLBACK,
	SCALAR_DELIM_PROJECTION_GROUP_FALLBACK,
	JOIN_AGGREGATE_PROJECTION_FALLBACK,
	NESTED_AGGREGATE_GROUP_FALLBACK,
	REPEATED_CTE_AGGREGATE_GROUP_FALLBACK,
	SEMI_ANTI_AGGREGATE_GROUP_FALLBACK,
	OUTER_JOIN_AGGREGATE_RECOMPUTE,
	ASOF_CURRENT_DIFF_RECOMPUTE,
	SAMPLE_CURRENT_DIFF_RECOMPUTE,
	POSITIONAL_CURRENT_DIFF_RECOMPUTE
};

enum class DeltaModelFeature {
	LINEAR,
	BILINEAR,
	OUTER_JOIN_STATEFUL,
	AGGREGATE_LINEAR,
	AGGREGATE_NON_LINEAR,
	AGGREGATE_HAVING,
	COUNT_DISTINCT_STATEFUL,
	FILTERED_LIST_STATEFUL,
	GROUPING_SETS_STATEFUL,
	DISTINCT_STATEFUL,
	WINDOW_AFFECTED_PARTITION,
	SEMI_ANTI_STATEFUL,
	ASOF_STATEFUL,
	SAMPLE_GLOBAL_RECOMPUTE,
	POSITIONAL_GLOBAL_RECOMPUTE,
	CURRENT_DIFF_RECOMPUTE,
	FULL_ONLY
};

enum class DeltaModelNodeKind {
	SCAN,
	FILTER,
	PROJECT,
	JOIN,
	AGGREGATE,
	WINDOW,
	DISTINCT,
	SEMI_ANTI,
	UNION,
	TOP_K,
	UNNEST,
	ASOF_JOIN,
	POSITIONAL_JOIN,
	SAMPLE,
	CTE,
	CONSTANT,
	OTHER
};

enum class DeltaRuleKind { LINEAR, PRODUCT, STATEFUL, NON_LINEAR, FULL_ONLY };

enum class DeltaMaintenanceMode { DELTA_ONLY, DELTA_WITH_STATE, AFFECTED_DOMAIN_RECOMPUTE, FULL_RECOMPUTE };

enum class DeltaMaintenanceStateKind { NONE, CURRENT_RELATION, MV_DATA, AUX_TABLE };

enum class DeltaUnsupportedReason {
	UNSUPPORTED_SET_OPERATION,
	UNSUPPORTED_PIVOT,
	UNSUPPORTED_FUNCTION,
	UNSUPPORTED_UNNEST,
	UNSUPPORTED_AGGREGATE,
	UNSUPPORTED_FILTERED_AGGREGATE,
	UNSUPPORTED_JOIN_TYPE,
	UNSUPPORTED_ORDER_BY,
	UNSUPPORTED_OPERATOR,
	GROUPING_SETS_NO_KEYS,
	FILTERED_LIST_NO_KEYS,
	SEMI_ANTI_WITH_AGGREGATE,
	SEMI_ANTI_MISSING_AUX,
	UNRECOGNIZED_PATTERN
};

enum class DeltaUpdateSemantics {
	APPEND_ONLY_SAFE,
	DELETE_SENSITIVE,
	UPDATE_SENSITIVE,
	MINMAX_INSERT_ONLY_SAFE,
	PROJECTION_DELETE_SKIP_SAFE,
	AGGREGATE_DELETE_SKIP_SAFE
};

enum class DeltaAuxStateKind { DISTINCT_COUNT, FILTERED_GROUP_COUNT, SEMI_ANTI_MATCH };

enum class DeltaAffectedDomainKind { GROUP, WINDOW_PARTITION, PROJECTION_KEY, SEMI_ANTI_PREDICATE };

enum class DeltaLineageKind { WINDOW_PARTITION, PROJECTION_KEY, SEMI_ANTI_PREDICATE };

struct DeltaNodeMaintenance {
	DeltaMaintenanceMode mode = DeltaMaintenanceMode::FULL_RECOMPUTE;
	DeltaMaintenanceStateKind state = DeltaMaintenanceStateKind::NONE;
};

struct DeltaAffectedDomain {
	DeltaAffectedDomainKind kind = DeltaAffectedDomainKind::GROUP;
	idx_t node_id = DConstants::INVALID_INDEX;
	vector<string> key_columns;
	vector<string> source_tables;
	bool delta_local = false;
	bool needs_base_lookup = false;
};

struct DeltaLineageFact {
	DeltaLineageKind kind = DeltaLineageKind::WINDOW_PARTITION;
	idx_t node_id = DConstants::INVALID_INDEX;
	string source_table;
	idx_t source_occurrence = DConstants::INVALID_INDEX;
	string source_column;
	string output_column;
	string lookup_table;
	string lookup_column;
	string lookup_output_column;
};

struct DeltaModelNode {
	idx_t id = DConstants::INVALID_INDEX;
	DeltaModelNodeKind kind = DeltaModelNodeKind::OTHER;
	DeltaRuleKind rule = DeltaRuleKind::FULL_ONLY;
	const LogicalOperator *plan_node = nullptr;
	vector<idx_t> children;
	string source_table;
	idx_t source_occurrence = DConstants::INVALID_INDEX;
	idx_t source_table_index = DConstants::INVALID_INDEX;
	vector<string> source_tables;
	vector<string> output_columns;
	vector<string> hidden_columns;
	vector<string> affected_key_columns;
	DeltaNodeMaintenance maintenance;
	vector<DeltaUpdateSemantics> update_semantics;
	vector<DeltaUnsupportedReason> unsupported_reasons;
	vector<DeltaAuxStateKind> required_aux_states;
	vector<DeltaAffectedDomain> affected_domains;
	vector<DeltaLineageFact> lineage_facts;
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
	bool stored_query_has_aggregate_filter = false;
	bool stored_query_has_top_k = false;
	bool has_hidden_minmax_having = false;
	bool has_computed_minmax_aggregate_projection = false;
	bool has_ducklake_source = false;
};

struct DeltaViewModel {
	RefreshType type = RefreshType::FULL_REFRESH;
	vector<DeltaStrategyReason> strategy_reasons;
	vector<string> group_columns;
	vector<string> window_partition_columns;
	vector<string> aggregate_types;
	vector<DeltaModelFeature> features;
	vector<DeltaUnsupportedReason> unsupported_reasons;
	vector<DeltaUpdateSemantics> update_semantics;
	vector<DeltaModelNode> nodes;
	vector<DeltaAffectedDomain> affected_domains;
	vector<DeltaLineageFact> lineage_facts;
	vector<RefreshMetadata::WindowPartitionLineageOp> window_lineage_ops;
	RefreshMetadata::ProjectionKeyLineage projection_lineage;
	idx_t root_node = DConstants::INVALID_INDEX;
	string full_outer_join_cols;
	GroupRecomputeAffectedMode group_recompute_affected_mode = GroupRecomputeAffectedMode::SOURCE_DELTA;
	RefreshMetadata::DistinctAuxMeta distinct_aux;
	FilteredGroupCountAuxRequirement filtered_group_count_aux;
	RefreshMetadata::SemiAntiAuxMeta semi_anti_aux;
	bool has_minmax_metadata = false;
	bool distinct_at_top = false;
	bool union_distinct_over_agg = false;
	bool has_projection_lineage = false;
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
	bool HasFeature(DeltaModelFeature feature) const;
	idx_t LineageEntryCount() const;
};

const char *DeltaStrategyReasonName(DeltaStrategyReason reason);
const char *DeltaModelFeatureName(DeltaModelFeature feature);
const char *DeltaModelNodeKindName(DeltaModelNodeKind kind);
const char *DeltaRuleKindName(DeltaRuleKind kind);
const char *DeltaUnsupportedReasonName(DeltaUnsupportedReason reason);
const char *DeltaUpdateSemanticsName(DeltaUpdateSemantics semantics);
const char *DeltaAffectedDomainKindName(DeltaAffectedDomainKind kind);
bool IsDistinctAtTop(const PlanAnalysis &analysis, const vector<string> &output_names);
DeltaViewModel BuildDeltaViewModel(const DeltaViewModelInput &input);

} // namespace duckdb

#endif // IVM_VIEW_CLASSIFIER_HPP
