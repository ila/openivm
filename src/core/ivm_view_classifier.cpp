#include "core/ivm_view_classifier.hpp"

#include "core/ivm_delta_model.hpp"
#include "core/openivm_debug.hpp"
#include "core/vector_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

#include <unordered_set>

namespace duckdb {

namespace {

static void AddVisibleGroupNames(vector<string> &group_columns, const vector<string> &names) {
	for (auto &name : names) {
		if (!IncrementalTableNames::IsInternalColumn(name)) {
			group_columns.push_back(name);
		}
	}
}

static void DeduplicateGroupColumns(vector<string> &group_columns) {
	unordered_set<string> seen_group;
	for (auto &name : group_columns) {
		if (IncrementalTableNames::IsInternalColumn(name)) {
			continue;
		}
		string candidate = name;
		idx_t suffix = 1;
		while (seen_group.count(candidate)) {
			candidate = name + "_" + to_string(suffix++);
		}
		seen_group.insert(candidate);
		name = candidate;
	}
}

static void AddJoinKeyGroupColumns(const CreateMVPlanFacts &facts, const vector<string> &output_names,
                                   vector<string> &aggregate_columns, vector<DeltaStrategyReason> &strategy_reasons) {
	auto *top_proj_ptr = facts.first_projection;
	auto *cjoin = facts.first_comparison_join;
	if (!top_proj_ptr || !cjoin) {
		return;
	}

	unordered_map<idx_t, unordered_set<idx_t>> join_key_cols;
	for (auto &cond : cjoin->conditions) {
		AddJoinKeyColumn(cond.left, join_key_cols);
		AddJoinKeyColumn(cond.right, join_key_cols);
	}

	auto &top_proj = *top_proj_ptr;
	for (idx_t expr_i = 0; expr_i < top_proj.expressions.size(); expr_i++) {
		auto &expr = top_proj.expressions[expr_i];
		if (expr->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		auto it = join_key_cols.find(bcr.binding.table_index);
		if (it == join_key_cols.end() || !it->second.count(bcr.binding.column_index)) {
			continue;
		}

		string col_name;
		if (!expr->alias.empty()) {
			col_name = expr->alias;
		} else if (expr_i < output_names.size() && !output_names[expr_i].empty() &&
		           !IncrementalTableNames::IsInternalColumn(output_names[expr_i])) {
			col_name = output_names[expr_i];
		} else {
			col_name = bcr.GetName();
		}
		if (IncrementalTableNames::IsInternalColumn(col_name)) {
			continue;
		}

		bool exists = false;
		for (auto &existing : aggregate_columns) {
			if (StringUtil::CIEquals(existing, col_name)) {
				exists = true;
				break;
			}
		}
		if (!exists) {
			aggregate_columns.push_back(col_name);
			AddUnique(strategy_reasons, DeltaStrategyReason::JOIN_KEY_GROUP_FALLBACK);
		}
	}
}

static void BuildUnsupportedReasons(DeltaViewModel &model, const CreateMVPlanFacts &facts,
                                    const DeltaViewModelInput &input) {
	const auto &analysis = facts.analysis;
	if (facts.has_unsupported_set_operation) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_SET_OPERATION);
	}
	if (facts.has_pivot) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_PIVOT);
	}
	if (analysis.found_volatile_expression) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_FUNCTION);
	}
	if (analysis.found_non_foldable_unnest) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_UNNEST);
	}
	if (analysis.found_unsupported_aggregate) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_AGGREGATE);
	}
	if (analysis.found_unsupported_filtered_aggregate) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_FILTERED_AGGREGATE);
	}
	if (analysis.found_unsupported_join_type) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_JOIN_TYPE);
	}
	if (analysis.found_unsupported_order_by) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_ORDER_BY);
	}
	if (analysis.found_unsupported_operator) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_OPERATOR);
	}
	if (analysis.found_grouping_sets && model.group_columns.empty()) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::GROUPING_SETS_NO_KEYS);
	}
	if (analysis.found_filtered_list && model.group_columns.empty()) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::FILTERED_LIST_NO_KEYS);
	}
	if (analysis.found_semi_anti_join && analysis.found_aggregation && model.group_columns.empty()) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::SEMI_ANTI_WITH_AGGREGATE);
	}
	if (analysis.found_semi_anti_join && !analysis.found_aggregation && !input.semi_anti_aux_candidate) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::SEMI_ANTI_MISSING_AUX);
	}
	if (!analysis.incremental_compatible && model.unsupported_reasons.empty()) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNSUPPORTED_OPERATOR);
	}
}

static void BuildModelFeatures(DeltaViewModel &model, const PlanAnalysis &analysis, const DeltaViewModelInput &input) {
	if (!model.unsupported_reasons.empty()) {
		AddUnique(model.features, DeltaModelFeature::FULL_ONLY);
	}
	if (analysis.found_projection || (!analysis.found_aggregation && !analysis.found_join && !analysis.found_window)) {
		AddUnique(model.features, DeltaModelFeature::LINEAR);
	}
	if (analysis.found_join && !analysis.found_left_join && !analysis.found_full_outer &&
	    !analysis.found_semi_anti_join) {
		AddUnique(model.features, DeltaModelFeature::BILINEAR);
	}
	if (analysis.found_left_join || analysis.found_full_outer) {
		AddUnique(model.features, DeltaModelFeature::OUTER_JOIN_STATEFUL);
	}
	if (analysis.found_aggregation) {
		if (analysis.found_minmax || analysis.found_count_distinct || analysis.found_list ||
		    analysis.found_filtered_list) {
			AddUnique(model.features, DeltaModelFeature::AGGREGATE_NON_LINEAR);
		} else {
			AddUnique(model.features, DeltaModelFeature::AGGREGATE_LINEAR);
		}
	}
	if (analysis.found_having) {
		AddUnique(model.features, DeltaModelFeature::AGGREGATE_HAVING);
	}
	if (analysis.found_count_distinct) {
		AddUnique(model.features, DeltaModelFeature::COUNT_DISTINCT_STATEFUL);
	}
	if (analysis.found_filtered_list) {
		AddUnique(model.features, DeltaModelFeature::FILTERED_LIST_STATEFUL);
	}
	if (analysis.found_grouping_sets) {
		AddUnique(model.features, DeltaModelFeature::GROUPING_SETS_STATEFUL);
	}
	if (analysis.found_window && !model.window_partition_columns.empty()) {
		AddUnique(model.features, DeltaModelFeature::WINDOW_AFFECTED_PARTITION);
	}
	if (!model.HasFeature(DeltaModelFeature::FULL_ONLY)) {
		if (input.distinct_aux_candidate) {
			AddUnique(model.features, DeltaModelFeature::DISTINCT_STATEFUL);
		}
		if (input.semi_anti_aux_candidate) {
			AddUnique(model.features, DeltaModelFeature::SEMI_ANTI_STATEFUL);
		}
	}
}

static bool IsAggregateRefreshType(RefreshType type) {
	return type == RefreshType::SIMPLE_AGGREGATE || type == RefreshType::AGGREGATE_GROUP ||
	       type == RefreshType::AGGREGATE_HAVING;
}

static void BuildUpdateSemantics(DeltaViewModel &model, const PlanAnalysis &analysis) {
	if (model.type != RefreshType::FULL_REFRESH) {
		AddUnique(model.update_semantics, DeltaUpdateSemantics::DELETE_SENSITIVE);
		AddUnique(model.update_semantics, DeltaUpdateSemantics::UPDATE_SENSITIVE);
	}
	if (model.type == RefreshType::SIMPLE_PROJECTION) {
		AddUnique(model.update_semantics, DeltaUpdateSemantics::APPEND_ONLY_SAFE);
		AddUnique(model.update_semantics, DeltaUpdateSemantics::PROJECTION_DELETE_SKIP_SAFE);
	}
	if (IsAggregateRefreshType(model.type) && !analysis.found_count_distinct && !analysis.found_list &&
	    !analysis.found_filtered_list) {
		AddUnique(model.update_semantics, DeltaUpdateSemantics::APPEND_ONLY_SAFE);
		AddUnique(model.update_semantics, DeltaUpdateSemantics::AGGREGATE_DELETE_SKIP_SAFE);
	}
	if (analysis.found_minmax && !analysis.found_count_distinct && !analysis.found_list) {
		AddUnique(model.update_semantics, DeltaUpdateSemantics::MINMAX_INSERT_ONLY_SAFE);
	}
}

static void BuildGroupColumns(DeltaViewModel &model, const CreateMVPlanFacts &facts,
                              const vector<string> &output_names) {
	const auto &analysis = facts.analysis;
	const auto group_count = analysis.group_count;
	const auto group_index = analysis.group_index;
	const bool has_union_over_agg = analysis.found_aggregation && facts.has_union_before_aggregate;
	model.union_distinct_over_agg = analysis.found_distinct && model.distinct_at_top && has_union_over_agg;

	if (model.union_distinct_over_agg) {
		model.group_columns = DeriveGroupColumnNames(facts, group_index, group_count, output_names);
	} else if (model.distinct_at_top) {
		model.group_columns = analysis.aggregate_columns;
	} else if (analysis.found_distinct && analysis.aggregate_columns.empty()) {
		model.group_columns = analysis.aggregate_columns;
	} else if (group_count > 0 && group_index != DConstants::INVALID_INDEX) {
		model.group_columns = DeriveGroupColumnNames(facts, group_index, group_count, output_names);
	}

	if (analysis.found_delim_join && analysis.found_aggregation && model.group_columns.empty() &&
	    output_names.size() > analysis.aggregate_types.size()) {
		idx_t key_count = output_names.size() - analysis.aggregate_types.size();
		for (idx_t i = 0; i < key_count; i++) {
			if (!output_names[i].empty() && !IncrementalTableNames::IsInternalColumn(output_names[i])) {
				model.group_columns.push_back(output_names[i]);
			}
		}
		if (!model.group_columns.empty()) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::DELIM_AGGREGATE_GROUP_FALLBACK);
			OPENIVM_DEBUG_PRINT("[CREATE MV] DELIM/DEPENDENT aggregate: using %zu visible key columns for "
			                    "GROUP_RECOMPUTE\n",
			                    model.group_columns.size());
		}
	}

	if (analysis.found_delim_join && analysis.found_single_join && !analysis.found_aggregation &&
	    model.group_columns.empty()) {
		auto before = model.group_columns.size();
		AddVisibleGroupNames(model.group_columns, DeriveScalarDelimKeyColumnNames(facts, output_names));
		if (model.group_columns.size() > before) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::SCALAR_DELIM_PROJECTION_GROUP_FALLBACK);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Scalar DELIM/DEPENDENT projection: using %zu visible key columns "
			                    "for GROUP_RECOMPUTE\n",
			                    model.group_columns.size());
		}
	}

	if (analysis.found_nested_aggregate && model.group_columns.empty()) {
		auto before = model.group_columns.size();
		AddVisibleGroupNames(model.group_columns, DeriveAggregateGroupColumnNames(facts, output_names, false));
		if (model.group_columns.size() > before) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::NESTED_AGGREGATE_GROUP_FALLBACK);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Nested aggregate: using %zu visible inner group columns for "
			                    "GROUP_RECOMPUTE\n",
			                    model.group_columns.size());
		}
	}

	if (analysis.found_aggregation && model.group_columns.empty()) {
		auto before = model.group_columns.size();
		AddVisibleGroupNames(model.group_columns, DeriveAggregateGroupColumnNames(facts, output_names, true));
		if (model.group_columns.size() > before) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::REPEATED_CTE_AGGREGATE_GROUP_FALLBACK);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Repeated CTE aggregate under join: using %zu visible group columns "
			                    "for GROUP_RECOMPUTE\n",
			                    model.group_columns.size());
		}
	}

	DeduplicateGroupColumns(model.group_columns);

	if (analysis.found_join && group_count > 0 && !has_union_over_agg) {
		AddJoinKeyGroupColumns(facts, output_names, model.group_columns, model.strategy_reasons);
	}
	if (analysis.found_join && analysis.found_aggregation && !model.group_columns.empty()) {
		ResolveAggregateGroupColumnsThroughJoinKeys(facts, model.group_columns, output_names);
	}

	if (analysis.found_join && analysis.found_aggregation && !model.group_columns.empty()) {
		idx_t expected_linear_outputs = model.group_columns.size() + model.aggregate_types.size();
		if (output_names.size() > expected_linear_outputs) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Join-over-aggregate exposes %zu columns but only %zu are "
			                    "group/aggregate outputs -- using GROUP_RECOMPUTE\n",
			                    output_names.size(), expected_linear_outputs);
		}
	}

	if (has_union_over_agg) {
		AddUnique(model.strategy_reasons, DeltaStrategyReason::UNION_OVER_AGGREGATE);
	}
	if (analysis.found_nested_aggregate) {
		AddUnique(model.strategy_reasons, DeltaStrategyReason::NESTED_AGGREGATE_GROUP_FALLBACK);
	}
	if (analysis.found_semi_anti_join && analysis.found_aggregation && !model.group_columns.empty()) {
		AddUnique(model.strategy_reasons, DeltaStrategyReason::SEMI_ANTI_AGGREGATE_GROUP_FALLBACK);
	}
}

static void SelectRefreshType(DeltaViewModel &model, const PlanAnalysis &analysis,
                              bool has_unsupported_incremental_construct) {
	if (has_unsupported_incremental_construct) {
		model.type = RefreshType::FULL_REFRESH;
	} else if (analysis.found_window) {
		model.type = RefreshType::WINDOW_PARTITION;
	} else if (analysis.found_grouping_sets) {
		model.type = model.group_columns.empty() ? RefreshType::FULL_REFRESH : RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_semi_anti_join && analysis.found_aggregation) {
		model.type = model.group_columns.empty() ? RefreshType::FULL_REFRESH : RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_semi_anti_join && !analysis.found_aggregation) {
		model.type = model.HasFeature(DeltaModelFeature::SEMI_ANTI_STATEFUL) ? RefreshType::SEMI_ANTI_RECOMPUTE
		                                                                     : RefreshType::FULL_REFRESH;
	} else if (!analysis.incremental_compatible) {
		model.type = RefreshType::FULL_REFRESH;
		model.warn_unsupported_incremental = true;
	} else if (analysis.found_filtered_list && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_filtered_list) {
		model.type = RefreshType::FULL_REFRESH;
	} else if (analysis.found_count_distinct && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_distinct && !model.distinct_at_top && analysis.found_aggregation) {
		model.type = model.HasFeature(DeltaModelFeature::DISTINCT_STATEFUL) ? RefreshType::DISTINCT_INCREMENTAL
		                                                                    : RefreshType::GROUP_RECOMPUTE;
	} else if (model.union_distinct_over_agg && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_distinct && model.distinct_at_top && !model.group_columns.empty()) {
		model.type = RefreshType::AGGREGATE_GROUP;
	} else if (analysis.found_having && analysis.found_aggregation && !model.group_columns.empty()) {
		model.type = RefreshType::AGGREGATE_HAVING;
	} else if (!model.strategy_reasons.empty() && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_aggregation && !model.group_columns.empty()) {
		model.type = RefreshType::AGGREGATE_GROUP;
	} else if (analysis.found_aggregation && model.group_columns.empty()) {
		model.type = RefreshType::SIMPLE_AGGREGATE;
	} else if (analysis.found_projection && !analysis.found_aggregation) {
		model.type = RefreshType::SIMPLE_PROJECTION;
	} else {
		model.type = RefreshType::FULL_REFRESH;
		model.warn_unrecognized_pattern = true;
	}
}

static void AttachAuxRequirements(DeltaViewModel &model, const DeltaViewModelInput &input) {
	if (model.type == RefreshType::DISTINCT_INCREMENTAL) {
		D_ASSERT(input.distinct_aux_candidate);
		model.distinct_aux = *input.distinct_aux_candidate;
	}
	if (model.type == RefreshType::SIMPLE_AGGREGATE && input.filtered_group_count_aux_candidate) {
		model.filtered_group_count_aux = *input.filtered_group_count_aux_candidate;
	}
	if (model.type == RefreshType::SEMI_ANTI_RECOMPUTE) {
		D_ASSERT(input.semi_anti_aux_candidate);
		model.semi_anti_aux = *input.semi_anti_aux_candidate;
	}
}

} // namespace

const char *DeltaStrategyReasonName(DeltaStrategyReason reason) {
	switch (reason) {
	case DeltaStrategyReason::UNION_OVER_AGGREGATE:
		return "UNION_OVER_AGGREGATE";
	case DeltaStrategyReason::JOIN_KEY_GROUP_FALLBACK:
		return "JOIN_KEY_GROUP_FALLBACK";
	case DeltaStrategyReason::DELIM_AGGREGATE_GROUP_FALLBACK:
		return "DELIM_AGGREGATE_GROUP_FALLBACK";
	case DeltaStrategyReason::SCALAR_DELIM_PROJECTION_GROUP_FALLBACK:
		return "SCALAR_DELIM_PROJECTION_GROUP_FALLBACK";
	case DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK:
		return "JOIN_AGGREGATE_PROJECTION_FALLBACK";
	case DeltaStrategyReason::NESTED_AGGREGATE_GROUP_FALLBACK:
		return "NESTED_AGGREGATE_GROUP_FALLBACK";
	case DeltaStrategyReason::REPEATED_CTE_AGGREGATE_GROUP_FALLBACK:
		return "REPEATED_CTE_AGGREGATE_GROUP_FALLBACK";
	case DeltaStrategyReason::SEMI_ANTI_AGGREGATE_GROUP_FALLBACK:
		return "SEMI_ANTI_AGGREGATE_GROUP_FALLBACK";
	case DeltaStrategyReason::OUTER_JOIN_AGGREGATE_RECOMPUTE:
		return "OUTER_JOIN_AGGREGATE_RECOMPUTE";
	default:
		return "UNKNOWN";
	}
}

const char *DeltaModelFeatureName(DeltaModelFeature feature) {
	switch (feature) {
	case DeltaModelFeature::LINEAR:
		return "LINEAR";
	case DeltaModelFeature::BILINEAR:
		return "BILINEAR";
	case DeltaModelFeature::OUTER_JOIN_STATEFUL:
		return "OUTER_JOIN_STATEFUL";
	case DeltaModelFeature::AGGREGATE_LINEAR:
		return "AGGREGATE_LINEAR";
	case DeltaModelFeature::AGGREGATE_NON_LINEAR:
		return "AGGREGATE_NON_LINEAR";
	case DeltaModelFeature::AGGREGATE_HAVING:
		return "AGGREGATE_HAVING";
	case DeltaModelFeature::COUNT_DISTINCT_STATEFUL:
		return "COUNT_DISTINCT_STATEFUL";
	case DeltaModelFeature::FILTERED_LIST_STATEFUL:
		return "FILTERED_LIST_STATEFUL";
	case DeltaModelFeature::GROUPING_SETS_STATEFUL:
		return "GROUPING_SETS_STATEFUL";
	case DeltaModelFeature::DISTINCT_STATEFUL:
		return "DISTINCT_STATEFUL";
	case DeltaModelFeature::WINDOW_AFFECTED_PARTITION:
		return "WINDOW_AFFECTED_PARTITION";
	case DeltaModelFeature::SEMI_ANTI_STATEFUL:
		return "SEMI_ANTI_STATEFUL";
	case DeltaModelFeature::FULL_ONLY:
		return "FULL_ONLY";
	default:
		return "UNKNOWN";
	}
}

const char *DeltaModelNodeKindName(DeltaModelNodeKind kind) {
	switch (kind) {
	case DeltaModelNodeKind::SCAN:
		return "SCAN";
	case DeltaModelNodeKind::FILTER:
		return "FILTER";
	case DeltaModelNodeKind::PROJECT:
		return "PROJECT";
	case DeltaModelNodeKind::JOIN:
		return "JOIN";
	case DeltaModelNodeKind::AGGREGATE:
		return "AGGREGATE";
	case DeltaModelNodeKind::WINDOW:
		return "WINDOW";
	case DeltaModelNodeKind::DISTINCT:
		return "DISTINCT";
	case DeltaModelNodeKind::SEMI_ANTI:
		return "SEMI_ANTI";
	case DeltaModelNodeKind::UNION:
		return "UNION";
	case DeltaModelNodeKind::TOP_K:
		return "TOP_K";
	case DeltaModelNodeKind::UNNEST:
		return "UNNEST";
	case DeltaModelNodeKind::CTE:
		return "CTE";
	case DeltaModelNodeKind::CONSTANT:
		return "CONSTANT";
	default:
		return "OTHER";
	}
}

const char *DeltaRuleKindName(DeltaRuleKind kind) {
	switch (kind) {
	case DeltaRuleKind::LINEAR:
		return "LINEAR";
	case DeltaRuleKind::PRODUCT:
		return "PRODUCT";
	case DeltaRuleKind::STATEFUL:
		return "STATEFUL";
	case DeltaRuleKind::NON_LINEAR:
		return "NON_LINEAR";
	case DeltaRuleKind::FULL_ONLY:
		return "FULL_ONLY";
	default:
		return "UNKNOWN";
	}
}

const char *DeltaUnsupportedReasonName(DeltaUnsupportedReason reason) {
	switch (reason) {
	case DeltaUnsupportedReason::UNSUPPORTED_SET_OPERATION:
		return "UNSUPPORTED_SET_OPERATION";
	case DeltaUnsupportedReason::UNSUPPORTED_PIVOT:
		return "UNSUPPORTED_PIVOT";
	case DeltaUnsupportedReason::UNSUPPORTED_FUNCTION:
		return "UNSUPPORTED_FUNCTION";
	case DeltaUnsupportedReason::UNSUPPORTED_UNNEST:
		return "UNSUPPORTED_UNNEST";
	case DeltaUnsupportedReason::UNSUPPORTED_AGGREGATE:
		return "UNSUPPORTED_AGGREGATE";
	case DeltaUnsupportedReason::UNSUPPORTED_FILTERED_AGGREGATE:
		return "UNSUPPORTED_FILTERED_AGGREGATE";
	case DeltaUnsupportedReason::UNSUPPORTED_JOIN_TYPE:
		return "UNSUPPORTED_JOIN_TYPE";
	case DeltaUnsupportedReason::UNSUPPORTED_ORDER_BY:
		return "UNSUPPORTED_ORDER_BY";
	case DeltaUnsupportedReason::UNSUPPORTED_OPERATOR:
		return "UNSUPPORTED_OPERATOR";
	case DeltaUnsupportedReason::GROUPING_SETS_NO_KEYS:
		return "GROUPING_SETS_NO_KEYS";
	case DeltaUnsupportedReason::FILTERED_LIST_NO_KEYS:
		return "FILTERED_LIST_NO_KEYS";
	case DeltaUnsupportedReason::SEMI_ANTI_WITH_AGGREGATE:
		return "SEMI_ANTI_WITH_AGGREGATE";
	case DeltaUnsupportedReason::SEMI_ANTI_MISSING_AUX:
		return "SEMI_ANTI_MISSING_AUX";
	case DeltaUnsupportedReason::UNRECOGNIZED_PATTERN:
		return "UNRECOGNIZED_PATTERN";
	default:
		return "UNKNOWN";
	}
}

const char *DeltaUpdateSemanticsName(DeltaUpdateSemantics semantics) {
	switch (semantics) {
	case DeltaUpdateSemantics::APPEND_ONLY_SAFE:
		return "APPEND_ONLY_SAFE";
	case DeltaUpdateSemantics::DELETE_SENSITIVE:
		return "DELETE_SENSITIVE";
	case DeltaUpdateSemantics::UPDATE_SENSITIVE:
		return "UPDATE_SENSITIVE";
	case DeltaUpdateSemantics::MINMAX_INSERT_ONLY_SAFE:
		return "MINMAX_INSERT_ONLY_SAFE";
	case DeltaUpdateSemantics::PROJECTION_DELETE_SKIP_SAFE:
		return "PROJECTION_DELETE_SKIP_SAFE";
	case DeltaUpdateSemantics::AGGREGATE_DELETE_SKIP_SAFE:
		return "AGGREGATE_DELETE_SKIP_SAFE";
	default:
		return "UNKNOWN";
	}
}

const char *DeltaAffectedDomainKindName(DeltaAffectedDomainKind kind) {
	switch (kind) {
	case DeltaAffectedDomainKind::GROUP:
		return "GROUP";
	case DeltaAffectedDomainKind::WINDOW_PARTITION:
		return "WINDOW_PARTITION";
	case DeltaAffectedDomainKind::PROJECTION_KEY:
		return "PROJECTION_KEY";
	case DeltaAffectedDomainKind::SEMI_ANTI_PREDICATE:
		return "SEMI_ANTI_PREDICATE";
	default:
		return "UNKNOWN";
	}
}

bool DeltaViewModel::HasFeature(DeltaModelFeature feature) const {
	for (auto existing : features) {
		if (existing == feature) {
			return true;
		}
	}
	return false;
}

idx_t DeltaViewModel::LineageEntryCount() const {
	idx_t count = 0;
	if (!window_lineage_ops.empty()) {
		count++;
	}
	if (has_projection_lineage) {
		count++;
	}
	return count;
}

bool IsDistinctAtTop(const PlanAnalysis &analysis, const vector<string> &output_names) {
	if (!analysis.found_distinct || analysis.aggregate_columns.empty() || output_names.empty()) {
		return false;
	}

	unordered_set<string> output_lc;
	for (auto &name : output_names) {
		output_lc.insert(StringUtil::Lower(name));
	}

	for (auto &target : analysis.aggregate_columns) {
		if (!output_lc.count(StringUtil::Lower(target))) {
			return false;
		}
	}
	return true;
}

DeltaViewModel BuildDeltaViewModel(const DeltaViewModelInput &input) {
	D_ASSERT(input.facts);
	D_ASSERT(input.output_names);
	auto &facts = *input.facts;
	auto &analysis = facts.analysis;
	auto &output_names = *input.output_names;

	DeltaViewModel model;
	model.aggregate_types = analysis.aggregate_types;
	model.window_partition_columns = analysis.window_partition_columns;
	ResolveWindowPartitionOutputNames(facts, model.window_partition_columns, output_names);
	if (!input.keep_window_join_partitions) {
		model.window_partition_columns.clear();
	}

	model.has_minmax_metadata = analysis.found_minmax || analysis.found_count_distinct || analysis.found_list;
	model.distinct_at_top = IsDistinctAtTop(analysis, output_names);
	BuildGroupColumns(model, facts, output_names);

	if ((analysis.found_left_join || analysis.found_full_outer) && analysis.found_aggregation &&
	    OuterJoinAggregateNeedsRecompute(facts, analysis.group_index)) {
		model.has_minmax_metadata = true;
		AddUnique(model.strategy_reasons, DeltaStrategyReason::OUTER_JOIN_AGGREGATE_RECOMPUTE);
		OPENIVM_DEBUG_PRINT("[CREATE MV] LEFT/OUTER JOIN aggregate with computed aggregate or projection wrapper -- "
		                    "using group-recompute metadata\n");
	}

	if (analysis.found_full_outer) {
		model.full_outer_join_cols = ExtractFullOuterJoinMetadata(facts);
	}

	BuildUnsupportedReasons(model, facts, input);
	BuildModelFeatures(model, analysis, input);
	SelectRefreshType(model, analysis, input.has_unsupported_incremental_construct);
	if (model.warn_unrecognized_pattern) {
		AddUnique(model.unsupported_reasons, DeltaUnsupportedReason::UNRECOGNIZED_PATTERN);
		AddUnique(model.features, DeltaModelFeature::FULL_ONLY);
	}
	AttachAuxRequirements(model, input);
	BuildUpdateSemantics(model, analysis);
	BuildDeltaModelNodes(model, facts, output_names);
	BuildDeltaModelBaseAffectedDomains(model, facts);
	ValidateDeltaViewModelInvariants(model);
	return model;
}

} // namespace duckdb
