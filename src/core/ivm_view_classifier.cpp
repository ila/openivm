#include "core/ivm_view_classifier.hpp"

#include "core/ivm_delta_model.hpp"
#include "core/openivm_debug.hpp"
#include "core/plan_rewrite.hpp"
#include "core/vector_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

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

static bool ResolvesToAggregateValueBinding(TableIndex table_index, ProjectionIndex column_index,
                                            const CreateMVPlanFacts &facts) {
	for (auto *aggregate : facts.aggregates) {
		if (aggregate && aggregate->group_index == facts.analysis.group_index &&
		    ResolvesToOutputBinding(table_index, column_index, aggregate->aggregate_index,
		                            aggregate->expressions.size(), facts, true)) {
			return true;
		}
	}
	return false;
}

static idx_t GetVisibleOutputCount(const vector<string> &output_names, idx_t visible_output_count) {
	idx_t visible_count = output_names.size();
	if (visible_output_count != DConstants::INVALID_INDEX) {
		visible_count = MinValue<idx_t>(visible_count, visible_output_count);
	}
	return visible_count;
}

static bool HasCaseInsensitiveName(const unordered_set<string> &names, const string &name) {
	return names.count(StringUtil::Lower(name)) > 0;
}

static bool VisibleOutputResolvesToAggregateValue(const CreateMVPlanFacts &facts, const vector<ColumnBinding> &bindings,
                                                  idx_t output_idx) {
	if (facts.root) {
		if (output_idx < bindings.size()) {
			auto &binding = bindings[output_idx];
			return ResolvesToAggregateValueBinding(binding.table_index, binding.column_index, facts);
		}
	}
	if (!facts.first_projection || output_idx >= facts.first_projection->expressions.size()) {
		return false;
	}
	auto *bcr = GetColumnRefThroughCasts(facts.first_projection->expressions[output_idx].get());
	return bcr && ResolvesToAggregateValueBinding(bcr->Binding().table_index, bcr->Binding().column_index, facts);
}

static bool CanInspectVisibleOutputBinding(const CreateMVPlanFacts &facts, const vector<ColumnBinding> &bindings,
                                           idx_t output_idx) {
	if (facts.root && output_idx < bindings.size()) {
		return true;
	}
	return facts.first_projection && output_idx < facts.first_projection->expressions.size();
}

static bool HasVisibleNonGroupNonAggregateOutput(const DeltaViewModel &model, const CreateMVPlanFacts &facts,
                                                 const vector<string> &output_names, idx_t visible_output_count) {
	unordered_set<string> group_outputs;
	for (auto &group_col : model.group_columns) {
		group_outputs.insert(StringUtil::Lower(group_col));
	}
	unordered_set<string> aggregate_outputs;
	for (auto &name : facts.analysis.aggregate_columns) {
		aggregate_outputs.insert(StringUtil::Lower(name));
	}
	auto visible_count = GetVisibleOutputCount(output_names, visible_output_count);
	auto bindings = facts.root ? facts.root->GetColumnBindings() : vector<ColumnBinding>();
	for (idx_t output_idx = 0; output_idx < visible_count; output_idx++) {
		auto &name = output_names[output_idx];
		if (IncrementalTableNames::IsInternalColumn(name) || HasCaseInsensitiveName(group_outputs, name)) {
			continue;
		}
		if (VisibleOutputResolvesToAggregateValue(facts, bindings, output_idx)) {
			continue;
		}
		if (!CanInspectVisibleOutputBinding(facts, bindings, output_idx) &&
		    HasCaseInsensitiveName(aggregate_outputs, name)) {
			continue;
		}
		return true;
	}
	return false;
}

static void AddVisibleNonAggregateOutputNames(vector<string> &group_columns, const CreateMVPlanFacts &facts,
                                              const vector<string> &output_names, idx_t visible_output_count,
                                              const vector<string> &aggregate_columns) {
	unordered_set<string> aggregate_outputs;
	for (auto &name : aggregate_columns) {
		aggregate_outputs.insert(StringUtil::Lower(name));
	}
	idx_t visible_count = GetVisibleOutputCount(output_names, visible_output_count);
	auto bindings = facts.root ? facts.root->GetColumnBindings() : vector<ColumnBinding>();
	for (idx_t output_idx = 0; output_idx < visible_count; output_idx++) {
		auto &name = output_names[output_idx];
		if (IncrementalTableNames::IsInternalColumn(name) ||
		    VisibleOutputResolvesToAggregateValue(facts, bindings, output_idx)) {
			continue;
		}
		if (!CanInspectVisibleOutputBinding(facts, bindings, output_idx) &&
		    HasCaseInsensitiveName(aggregate_outputs, name)) {
			continue;
		}
		group_columns.push_back(name);
	}
}

static bool GroupColumnsAreVisibleOutputs(const vector<string> &group_columns, const vector<string> &output_names,
                                          idx_t visible_output_count) {
	if (group_columns.empty()) {
		return false;
	}
	idx_t visible_count = GetVisibleOutputCount(output_names, visible_output_count);
	unordered_set<string> visible_outputs;
	for (idx_t output_idx = 0; output_idx < visible_count; output_idx++) {
		visible_outputs.insert(StringUtil::Lower(output_names[output_idx]));
	}
	for (auto &group_col : group_columns) {
		if (!visible_outputs.count(StringUtil::Lower(group_col))) {
			return false;
		}
	}
	return true;
}

static void AddJoinKeyGroupColumns(const CreateMVPlanFacts &facts, const vector<string> &output_names,
                                   vector<string> &aggregate_columns, vector<DeltaStrategyReason> &strategy_reasons) {
	auto *top_proj_ptr = facts.first_projection;
	auto *cjoin = facts.comparison_joins.empty() ? nullptr : facts.comparison_joins.front();
	if (!top_proj_ptr || !cjoin) {
		return;
	}

	unordered_map<TableIndex, unordered_set<ProjectionIndex>> join_key_cols;
	for (auto &cond : cjoin->conditions) {
		if (!cond.IsComparison()) {
			continue;
		}
		AddJoinKeyColumn(cond.LeftReference(), join_key_cols);
		AddJoinKeyColumn(cond.RightReference(), join_key_cols);
	}

	auto &top_proj = *top_proj_ptr;
	for (idx_t expr_i = 0; expr_i < top_proj.expressions.size(); expr_i++) {
		auto &expr = top_proj.expressions[expr_i];
		if (expr->GetExpressionType() != ExpressionType::BOUND_COLUMN_REF) {
			continue;
		}
		auto &bcr = expr->Cast<BoundColumnRefExpression>();
		auto it = join_key_cols.find(bcr.Binding().table_index);
		if (it == join_key_cols.end() || !it->second.count(bcr.Binding().column_index)) {
			continue;
		}

		string col_name;
		if (!expr->GetAlias().GetIdentifierName().empty()) {
			col_name = expr->GetAlias().GetIdentifierName();
		} else if (expr_i < output_names.size() && !output_names[expr_i].empty() &&
		           !IncrementalTableNames::IsInternalColumn(output_names[expr_i])) {
			col_name = output_names[expr_i];
		} else {
			col_name = bcr.GetName().GetIdentifierName();
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

static bool HasUnsupportedConstruct(const DeltaViewModel &model, const CreateMVPlanFacts &facts,
                                    const DeltaViewModelInput &input) {
	const auto &analysis = facts.analysis;
	const bool no_group_keys = model.group_columns.empty();
	return facts.has_unsupported_set_operation || facts.has_pivot || analysis.found_volatile_expression ||
	       analysis.found_non_foldable_unnest || analysis.found_unsupported_aggregate ||
	       analysis.found_unsupported_filtered_aggregate || analysis.found_unsupported_join_type ||
	       analysis.found_unsupported_order_by || analysis.found_unsupported_operator ||
	       (analysis.found_grouping_sets && no_group_keys) || (analysis.found_filtered_list && no_group_keys) ||
	       (analysis.found_semi_anti_join && analysis.found_aggregation && no_group_keys) ||
	       (analysis.found_semi_anti_join && !analysis.found_aggregation && !input.semi_anti_aux_candidate) ||
	       !analysis.incremental_compatible;
}

static bool HasStrategyReason(const DeltaViewModel &model, DeltaStrategyReason reason) {
	return std::find(model.strategy_reasons.begin(), model.strategy_reasons.end(), reason) !=
	       model.strategy_reasons.end();
}

// Output path through projections and the wrappers that keep a DISTINCT/aggregate at the top of the view.
static OutputSpine DistinctSpine(const CreateMVPlanFacts &facts) {
	return FindOutputSpine(facts.root, [](const LogicalOperator &op) {
		return op.type == LogicalOperatorType::LOGICAL_CREATE_TABLE || op.type == LogicalOperatorType::LOGICAL_FILTER ||
		       op.type == LogicalOperatorType::LOGICAL_ORDER_BY || op.type == LogicalOperatorType::LOGICAL_LIMIT ||
		       op.type == LogicalOperatorType::LOGICAL_TOP_N;
	});
}

// DISTINCT ON partitions by its targets; payload columns choose a winner within each key.
static vector<string> DistinctOnGroupColumns(const CreateMVPlanFacts &facts, const vector<string> &output_names) {
	auto spine = DistinctSpine(facts);
	if (!spine.bottom || spine.bottom->type != LogicalOperatorType::LOGICAL_DISTINCT) {
		return {};
	}
	auto &distinct = spine.bottom->Cast<LogicalDistinct>();
	if (distinct.distinct_type != DistinctType::DISTINCT_ON) {
		return {};
	}
	auto outputs =
	    spine.projections.empty() ? distinct.GetColumnBindings() : spine.projections.front()->GetColumnBindings();
	vector<string> keys;
	for (auto &target : distinct.distinct_targets) {
		auto binding = target->Cast<BoundColumnRefExpression>().Binding();
		for (auto projection = spine.projections.rbegin(); projection != spine.projections.rend(); ++projection) {
			bool found = false;
			for (idx_t i = 0; i < (*projection)->expressions.size(); i++) {
				auto &expression = *(*projection)->expressions[i];
				if (expression.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
				    expression.Cast<BoundColumnRefExpression>().Binding() == binding) {
					binding = ColumnBinding((*projection)->table_index, ProjectionIndex(i));
					found = true;
					break;
				}
			}
			if (!found) {
				return {};
			}
		}
		auto output = std::find(outputs.begin(), outputs.end(), binding);
		if (output == outputs.end() || idx_t(output - outputs.begin()) >= output_names.size()) {
			return {};
		}
		keys.push_back(output_names[output - outputs.begin()]);
	}
	return keys;
}

static bool HasOuterProjectionOverDistinct(const CreateMVPlanFacts &facts) {
	auto spine = DistinctSpine(facts);
	return !spine.projections.empty() && spine.bottom && spine.bottom->type == LogicalOperatorType::LOGICAL_DISTINCT;
}

static void BuildGroupColumns(DeltaViewModel &model, const CreateMVPlanFacts &facts, const vector<string> &output_names,
                              idx_t visible_output_count) {
	const auto &analysis = facts.analysis;
	const auto group_count = analysis.group_count;
	const auto group_index = analysis.group_index;
	const bool has_union_over_agg = analysis.found_aggregation && facts.has_union_before_aggregate;
	model.union_distinct_over_agg =
	    has_union_over_agg && (analysis.found_union_distinct || (analysis.found_distinct && model.distinct_at_top));

	if (analysis.found_distinct_on) {
		model.group_columns = DistinctOnGroupColumns(facts, output_names);
	} else if (model.union_distinct_over_agg) {
		model.group_columns = DeriveGroupColumnNames(facts, group_index, group_count, output_names);
	} else if (analysis.found_union_distinct && !analysis.found_aggregation) {
		AddVisibleGroupNames(model.group_columns, output_names);
		if (!model.group_columns.empty()) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::UNION_DISTINCT_GROUP_RECOMPUTE);
			OPENIVM_DEBUG_PRINT("[CREATE MV] UNION DISTINCT: using %zu visible output columns for "
			                    "GROUP_RECOMPUTE\n",
			                    model.group_columns.size());
		}
	} else if (model.distinct_at_top) {
		AddVisibleGroupNames(model.group_columns, output_names);
	} else if (analysis.found_distinct && !analysis.found_aggregation && HasOuterProjectionOverDistinct(facts)) {
		AddVisibleGroupNames(model.group_columns, output_names);
		if (!model.group_columns.empty()) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::INNER_DISTINCT_PROJECTION_RECOMPUTE);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Inner DISTINCT below an outer projection -- using "
			                    "GROUP_RECOMPUTE\n");
		}
	} else if (group_count > 0 && group_index.IsValid()) {
		model.group_columns = DeriveGroupColumnNames(facts, group_index, group_count, output_names);
	}

	idx_t actual_visible_outputs = GetVisibleOutputCount(output_names, visible_output_count);
	if (analysis.found_delim_join && analysis.found_aggregation && model.group_columns.empty() &&
	    actual_visible_outputs > analysis.aggregate_types.size()) {
		idx_t key_count = actual_visible_outputs - analysis.aggregate_types.size();
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

	if (!analysis.found_asof_join && analysis.found_join && group_count > 0 && !has_union_over_agg) {
		AddJoinKeyGroupColumns(facts, output_names, model.group_columns, model.strategy_reasons);
	}
	if (!analysis.found_asof_join && analysis.found_join && analysis.found_aggregation &&
	    !model.group_columns.empty()) {
		ResolveAggregateGroupColumnsThroughJoinKeys(facts, model.group_columns, output_names);
	}

	if (analysis.found_join && analysis.found_aggregation &&
	    !GroupColumnsAreVisibleOutputs(model.group_columns, output_names, visible_output_count)) {
		model.group_columns.clear();
		AddVisibleNonAggregateOutputNames(model.group_columns, facts, output_names, visible_output_count,
		                                  analysis.aggregate_columns);
		if (!model.group_columns.empty()) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Join-over-aggregate projection: using %zu visible non-aggregate "
			                    "output columns for GROUP_RECOMPUTE\n",
			                    model.group_columns.size());
		}
		DeduplicateGroupColumns(model.group_columns);
	}

	if (analysis.found_single_join && analysis.found_aggregation && !model.group_columns.empty()) {
		AddUnique(model.strategy_reasons, DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK);
		OPENIVM_DEBUG_PRINT("[CREATE MV] Scalar subquery over grouped aggregate -- using GROUP_RECOMPUTE\n");
	}

	if ((analysis.found_join || analysis.found_delim_join) && analysis.found_aggregation &&
	    !model.group_columns.empty() &&
	    HasVisibleNonGroupNonAggregateOutput(model, facts, output_names, visible_output_count)) {
		AddUnique(model.strategy_reasons, DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK);
		OPENIVM_DEBUG_PRINT("[CREATE MV] Join/delim aggregate exposes non-group, non-aggregate output -- "
		                    "using GROUP_RECOMPUTE\n");
	}

	if (analysis.found_join && analysis.found_aggregation && !model.group_columns.empty()) {
		idx_t expected_linear_outputs = model.group_columns.size() + model.aggregate_types.size();
		if (actual_visible_outputs > expected_linear_outputs) {
			AddUnique(model.strategy_reasons, DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK);
			OPENIVM_DEBUG_PRINT("[CREATE MV] Join-over-aggregate exposes %zu columns but only %zu are "
			                    "group/aggregate outputs -- using GROUP_RECOMPUTE\n",
			                    actual_visible_outputs, expected_linear_outputs);
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

static void SelectRefreshType(DeltaViewModel &model, const PlanAnalysis &analysis, const DeltaViewModelInput &input,
                              bool has_nonredundant_distinct, bool distinct_stateful, bool semi_anti_stateful) {
	auto has_argminmax =
	    std::any_of(analysis.aggregate_types.begin(), analysis.aggregate_types.end(),
	                [](const string &agg_type) { return agg_type == "arg_min" || agg_type == "arg_max"; });
	if (input.has_unsupported_incremental_construct) {
		model.type = RefreshType::FULL_REFRESH;
	} else if (!analysis.incremental_compatible) {
		model.type = RefreshType::FULL_REFRESH;
		model.warn_unsupported_incremental = true;
	} else if (analysis.found_sample) {
		model.type = RefreshType::FULL_REFRESH;
	} else if (analysis.found_positional_join) {
		model.type = RefreshType::FULL_REFRESH;
	} else if (analysis.found_asof_join && analysis.found_window && !model.window_partition_columns.empty()) {
		model.type = RefreshType::WINDOW_PARTITION;
	} else if (analysis.found_asof_join && analysis.found_aggregation && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_asof_join) {
		model.type = RefreshType::FULL_REFRESH;
	} else if (analysis.found_window) {
		model.type = RefreshType::WINDOW_PARTITION;
	} else if (analysis.found_distinct_on && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_grouping_sets) {
		model.type = model.group_columns.empty() ? RefreshType::FULL_REFRESH : RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_semi_anti_join && analysis.found_aggregation) {
		model.type = model.group_columns.empty() ? RefreshType::FULL_REFRESH : RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_semi_anti_join && !analysis.found_aggregation) {
		model.type = semi_anti_stateful ? RefreshType::SEMI_ANTI_RECOMPUTE : RefreshType::FULL_REFRESH;
	} else if (analysis.found_filtered_list && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_filtered_list) {
		model.type = RefreshType::FULL_REFRESH;
	} else if (input.has_computed_sum_aggregate_projection) {
		model.type = model.group_columns.empty() ? RefreshType::FULL_REFRESH : RefreshType::GROUP_RECOMPUTE;
	} else if (input.stored_query_retains_aggregate_filter && analysis.found_aggregation &&
	           !model.group_columns.empty()) {
		// A FILTER separated from its aggregate by a projection cannot be exposed on
		// the user-facing view by StripHavingFilter. Applying that retained predicate
		// to SUM(delta) is incorrect: a negative delta does not imply a negative new
		// group total. Recompute only the affected groups from current base state.
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_distinct && !analysis.found_union_distinct && model.distinct_at_top &&
	           analysis.found_aggregation) {
		// DISTINCT over multiple aggregate result rows is a second non-linear
		// aggregation level. The outer DISTINCT key is an aggregate value, not
		// a source group key that affected-group recompute can recover.
		model.type = RefreshType::FULL_REFRESH;
	} else if (analysis.found_count_distinct && !model.group_columns.empty()) {
		model.type =
		    input.count_distinct_aux_candidate ? RefreshType::COUNT_DISTINCT_INCREMENTAL : RefreshType::GROUP_RECOMPUTE;
	} else if (has_nonredundant_distinct && !model.distinct_at_top && analysis.found_aggregation) {
		model.type = distinct_stateful ? RefreshType::DISTINCT_INCREMENTAL : RefreshType::GROUP_RECOMPUTE;
	} else if (model.union_distinct_over_agg && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_union_distinct && !analysis.found_aggregation && !model.group_columns.empty()) {
		// A descendant UNION DISTINCT does not aggregate the outer projection.
		// FULL JOIN projections need both hidden key scopes to recover old and
		// new NULL-padding keys; source-group recompute loses those transitions.
		model.type = analysis.found_full_outer && !model.distinct_at_top ? RefreshType::SIMPLE_PROJECTION
		                                                                 : RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_distinct && model.distinct_at_top && !model.group_columns.empty()) {
		model.type = RefreshType::AGGREGATE_GROUP;
	} else if (!model.strategy_reasons.empty() && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_having && analysis.found_aggregation &&
	           (input.has_hidden_minmax_having || input.has_computed_minmax_aggregate_projection) &&
	           !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_having && analysis.found_aggregation && has_argminmax && !model.group_columns.empty()) {
		model.type = RefreshType::GROUP_RECOMPUTE;
	} else if (analysis.found_having && analysis.found_aggregation && !model.group_columns.empty()) {
		model.type = RefreshType::AGGREGATE_HAVING;
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

static void SelectGroupRecomputeAffectedMode(DeltaViewModel &model, const DeltaViewModelInput &input) {
	if (model.type != RefreshType::GROUP_RECOMPUTE) {
		model.group_recompute_affected_mode = GroupRecomputeAffectedMode::SOURCE_DELTA;
		return;
	}
	if (input.facts && input.facts->analysis.found_distinct_on) {
		model.group_recompute_affected_mode = GroupRecomputeAffectedMode::SOURCE_DELTA;
		return;
	}
	bool aggregate_filter_join =
	    input.stored_query_has_aggregate_filter && input.facts && input.facts->analysis.found_join;
	if ((input.facts && input.facts->analysis.found_asof_join) || input.stored_query_has_top_k ||
	    aggregate_filter_join || (input.stored_query_has_aggregate_filter && input.has_ducklake_source)) {
		model.group_recompute_affected_mode = GroupRecomputeAffectedMode::CURRENT_DIFF;
	} else if (HasStrategyReason(model, DeltaStrategyReason::JOIN_AGGREGATE_PROJECTION_FALLBACK) ||
	           HasStrategyReason(model, DeltaStrategyReason::OUTER_JOIN_PRESERVED_TABLE_FUNCTION_RECOMPUTE)) {
		model.group_recompute_affected_mode = GroupRecomputeAffectedMode::CURRENT_DIFF;
	} else if (input.stored_query_has_aggregate_filter) {
		model.group_recompute_affected_mode = GroupRecomputeAffectedMode::SOURCE_DELTA_RELAX_AGGREGATE_FILTER;
	} else {
		model.group_recompute_affected_mode = GroupRecomputeAffectedMode::SOURCE_DELTA;
	}
	if (model.group_recompute_affected_mode == GroupRecomputeAffectedMode::CURRENT_DIFF &&
	    !input.stored_query_has_top_k) {
		auto key = DeriveDirectSourceGroupKey(*input.facts, *input.output_names);
		if (!key.empty()) {
			model.group_columns = {key};
			model.group_recompute_affected_mode = GroupRecomputeAffectedMode::DIRECT_SOURCE_KEYS;
			OPENIVM_DEBUG_PRINT("[CREATE MV] Proven source-key-local GROUP_RECOMPUTE on '%s'\n", key.c_str());
		}
	}
}

static void AttachAuxRequirements(DeltaViewModel &model, const DeltaViewModelInput &input) {
	if (model.type == RefreshType::DISTINCT_INCREMENTAL) {
		D_ASSERT(input.distinct_aux_candidate);
		model.distinct_aux = *input.distinct_aux_candidate;
	}
	if (model.type == RefreshType::COUNT_DISTINCT_INCREMENTAL) {
		D_ASSERT(input.count_distinct_aux_candidate);
		model.count_distinct_aux = *input.count_distinct_aux_candidate;
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

#define OPENIVM_ENUM_NAME_CASE(enum_type, name)                                                                        \
	case enum_type::name:                                                                                              \
		return #name;

const char *DeltaStrategyReasonName(DeltaStrategyReason reason) {
#define OPENIVM_STRATEGY_REASON_CASE(name) OPENIVM_ENUM_NAME_CASE(DeltaStrategyReason, name)
	switch (reason) { OPENIVM_DELTA_STRATEGY_REASONS(OPENIVM_STRATEGY_REASON_CASE) }
#undef OPENIVM_STRATEGY_REASON_CASE
	return "UNKNOWN";
}

const char *DeltaModelNodeKindName(DeltaModelNodeKind kind) {
#define OPENIVM_NODE_KIND_CASE(name) OPENIVM_ENUM_NAME_CASE(DeltaModelNodeKind, name)
	switch (kind) { OPENIVM_DELTA_MODEL_NODE_KINDS(OPENIVM_NODE_KIND_CASE) }
#undef OPENIVM_NODE_KIND_CASE
	return "OTHER";
}

#undef OPENIVM_ENUM_NAME_CASE

idx_t DeltaViewModel::LineageEntryCount() const {
	idx_t count = 0;
	if (!window_lineage_ops.empty()) {
		count++;
	}
	if (has_projection_lineage) {
		count++;
	}
	if (has_left_join_key_source) {
		count++;
	}
	if (!full_outer_scopes.empty()) {
		count++;
	}
	return count;
}

bool IsDistinctAtTop(const CreateMVPlanFacts &facts, const vector<string> &output_names) {
	if (!facts.analysis.found_distinct) {
		return false;
	}

	auto spine = DistinctSpine(facts);
	auto *node = spine.bottom;
	if (!spine.projections.empty()) {
		return false;
	}
	if (node && node->type == LogicalOperatorType::LOGICAL_DISTINCT) {
		return true;
	}
	if (!node || node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return false;
	}
	for (auto &name : output_names) {
		if (name == openivm::DISTINCT_COUNT_COL) {
			return true;
		}
	}
	return false;
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
	if (analysis.window_row_key_compatible) {
		model.window_order_columns = analysis.window_order_columns;
		ResolveWindowPartitionOutputNames(facts, model.window_order_columns, output_names);
	}

	model.has_minmax_metadata = analysis.found_minmax || analysis.found_count_distinct || analysis.found_list;
	model.distinct_at_top = IsDistinctAtTop(facts, output_names);
	if (input.has_top_level_redundant_distinct) {
		model.distinct_at_top = false;
	}
	BuildGroupColumns(model, facts, output_names, input.visible_output_count);

	if ((analysis.found_left_join || analysis.found_full_outer) && analysis.found_aggregation &&
	    OuterJoinAggregateNeedsRecompute(facts, analysis.group_index)) {
		model.has_minmax_metadata = true;
		AddUnique(model.strategy_reasons, DeltaStrategyReason::OUTER_JOIN_AGGREGATE_RECOMPUTE);
		OPENIVM_DEBUG_PRINT("[CREATE MV] LEFT/OUTER JOIN aggregate with computed aggregate or projection wrapper -- "
		                    "using group-recompute metadata\n");
	}
	if (analysis.found_aggregation && OuterJoinPreservedSideHasTableFunction(facts)) {
		model.has_minmax_metadata = true;
		AddUnique(model.strategy_reasons, DeltaStrategyReason::OUTER_JOIN_PRESERVED_TABLE_FUNCTION_RECOMPUTE);
		OPENIVM_DEBUG_PRINT("[CREATE MV] LEFT/RIGHT JOIN aggregate with a table function on the preserved side -- "
		                    "using current-diff group recompute\n");
	}

	if (analysis.found_full_outer) {
		model.full_outer_join_cols = ExtractFullOuterJoinMetadata(facts);
	}

	const bool has_unsupported_construct = HasUnsupportedConstruct(model, facts, input);
	const bool has_nonredundant_distinct =
	    analysis.found_distinct && (!input.has_top_level_redundant_distinct || facts.has_descendant_distinct);
	const bool distinct_stateful =
	    !has_unsupported_construct && input.distinct_aux_candidate && has_nonredundant_distinct;
	const bool semi_anti_stateful = !has_unsupported_construct && input.semi_anti_aux_candidate;
	SelectRefreshType(model, analysis, input, has_nonredundant_distinct, distinct_stateful, semi_anti_stateful);
	SelectGroupRecomputeAffectedMode(model, input);
	AttachAuxRequirements(model, input);
	BuildDeltaModelNodes(model, facts, has_unsupported_construct || model.warn_unrecognized_pattern);
	return model;
}

} // namespace duckdb
