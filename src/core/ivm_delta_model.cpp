#include "core/ivm_delta_model.hpp"

#include "core/parser_plan_helpers.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

#include <unordered_set>

namespace duckdb {

namespace {

static bool ContainsStringCI(const vector<string> &values, const string &candidate) {
	for (auto &value : values) {
		if (StringUtil::CIEquals(value, candidate)) {
			return true;
		}
	}
	return false;
}

static void AddStringCI(vector<string> &values, const string &candidate) {
	if (!candidate.empty() && !ContainsStringCI(values, candidate)) {
		values.push_back(candidate);
	}
}

static string WindowPartitionOutputColumn(const string &partition_column) {
	auto separator = partition_column.find('=');
	return separator == string::npos ? partition_column : partition_column.substr(0, separator);
}

static vector<string> ProjectedWindowPartitionColumns(const vector<string> &partition_columns,
                                                      const vector<string> &output_names) {
	vector<string> projected;
	for (auto &partition_column : partition_columns) {
		auto output_column = WindowPartitionOutputColumn(partition_column);
		if (!IncrementalTableNames::IsInternalColumn(output_column) && ContainsStringCI(output_names, output_column)) {
			AddStringCI(projected, partition_column);
		}
	}
	return projected;
}

// Profile counts for a node that has an incremental rule. Linear nodes only need their input delta; stateful nodes
// also read current state (base relation, MV data or an aux table); the rest recompute an affected domain.
// `node_semantics` counts the update semantics each node is safe/sensitive for (append-only, delete-skip, ...).
static void CountProfileNode(DeltaModelProfileCounts &counts, DeltaModelNodeKind kind, const DeltaViewModel &model,
                             const PlanAnalysis &analysis) {
	switch (kind) {
	case DeltaModelNodeKind::SCAN:
	case DeltaModelNodeKind::FILTER:
	case DeltaModelNodeKind::UNION:
	case DeltaModelNodeKind::UNNEST:
	case DeltaModelNodeKind::CTE:
	case DeltaModelNodeKind::CONSTANT:
		counts.delta_only_nodes++;
		break;
	case DeltaModelNodeKind::PROJECT:
		counts.delta_only_nodes++;
		counts.node_semantics += 2; // APPEND_ONLY_SAFE, PROJECTION_DELETE_SKIP_SAFE
		break;
	case DeltaModelNodeKind::JOIN:
		counts.delta_state_nodes++;
		counts.node_semantics += 2; // DELETE_SENSITIVE, UPDATE_SENSITIVE
		break;
	case DeltaModelNodeKind::AGGREGATE: {
		bool non_linear = analysis.found_minmax || analysis.found_count_distinct || analysis.found_list ||
		                  analysis.found_filtered_list;
		if (analysis.found_aggregation && (!non_linear || analysis.found_count_distinct)) {
			counts.node_semantics += 2; // linear: APPEND_ONLY/DELETE_SKIP safe; COUNT DISTINCT: DELETE/UPDATE sensitive
		}
		if (ContainsStringCI(model.aggregate_types, "min") || ContainsStringCI(model.aggregate_types, "max")) {
			counts.node_semantics++; // MINMAX_INSERT_ONLY_SAFE
		}
		if (model.type != RefreshType::GROUP_RECOMPUTE) {
			counts.delta_state_nodes++;
		}
		break;
	}
	case DeltaModelNodeKind::DISTINCT:
	case DeltaModelNodeKind::SEMI_ANTI:
		counts.node_semantics += 2;
		if (kind == DeltaModelNodeKind::DISTINCT ? model.HasDistinctAux() : model.HasSemiAntiAux()) {
			counts.delta_state_nodes++;
		}
		break;
	case DeltaModelNodeKind::ASOF_JOIN:
	case DeltaModelNodeKind::POSITIONAL_JOIN:
	case DeltaModelNodeKind::SAMPLE:
	case DeltaModelNodeKind::WINDOW:
		counts.node_semantics += 2;
		break;
	default:
		break;
	}
}

static bool WindowLineageCoversAllSources(const vector<RefreshMetadata::WindowPartitionLineageOp> &lineage_ops,
                                          const CreateMVPlanFacts &facts) {
	unordered_set<string> covered_sources;
	for (auto &op : lineage_ops) {
		covered_sources.insert(StringUtil::Lower(op.source));
	}
	for (auto &occurrence : facts.source_occurrences) {
		if (!covered_sources.count(StringUtil::Lower(occurrence.table))) {
			return false;
		}
	}
	return !covered_sources.empty();
}

static bool
AsofWindowPartitionReadsRightSideDirectly(const vector<RefreshMetadata::WindowPartitionLineageOp> &lineage_ops,
                                          const CreateMVPlanFacts &facts) {
	vector<string> rhs_sources;
	for (auto &plan_node : facts.plan_nodes_post_order) {
		if (plan_node.plan_node->type != LogicalOperatorType::LOGICAL_ASOF_JOIN || plan_node.child_ids.size() < 2) {
			continue;
		}
		for (auto table_index : facts.plan_nodes_post_order[plan_node.child_ids[1]].subtree_table_indices) {
			auto occurrence = facts.occurrence_by_index.find(table_index);
			if (occurrence != facts.occurrence_by_index.end()) {
				AddStringCI(rhs_sources, occurrence->second.table);
			}
		}
	}
	for (auto &op : lineage_ops) {
		if (op.kind == WindowLineageKind::DIRECT && ContainsStringCI(rhs_sources, op.source)) {
			return true;
		}
	}
	return false;
}

} // namespace

DeltaModelNodeKind NodeKindForOperator(LogicalOperator &op) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET:
		return DeltaModelNodeKind::SCAN;
	case LogicalOperatorType::LOGICAL_FILTER:
		return DeltaModelNodeKind::FILTER;
	case LogicalOperatorType::LOGICAL_PROJECTION:
		return DeltaModelNodeKind::PROJECT;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		return DeltaModelNodeKind::AGGREGATE;
	case LogicalOperatorType::LOGICAL_WINDOW:
		return DeltaModelNodeKind::WINDOW;
	case LogicalOperatorType::LOGICAL_DISTINCT:
		return DeltaModelNodeKind::DISTINCT;
	case LogicalOperatorType::LOGICAL_UNION:
		return DeltaModelNodeKind::UNION;
	case LogicalOperatorType::LOGICAL_TOP_N:
	case LogicalOperatorType::LOGICAL_LIMIT:
	case LogicalOperatorType::LOGICAL_ORDER_BY:
		return DeltaModelNodeKind::TOP_K;
	case LogicalOperatorType::LOGICAL_UNNEST:
		return DeltaModelNodeKind::UNNEST;
	case LogicalOperatorType::LOGICAL_ASOF_JOIN:
		return DeltaModelNodeKind::ASOF_JOIN;
	case LogicalOperatorType::LOGICAL_POSITIONAL_JOIN:
		return DeltaModelNodeKind::POSITIONAL_JOIN;
	case LogicalOperatorType::LOGICAL_SAMPLE:
		return DeltaModelNodeKind::SAMPLE;
	case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
	case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
		return DeltaModelNodeKind::CONSTANT;
	case LogicalOperatorType::LOGICAL_CTE_REF:
	case LogicalOperatorType::LOGICAL_MATERIALIZED_CTE:
		return DeltaModelNodeKind::CTE;
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_ANY_JOIN:
	case LogicalOperatorType::LOGICAL_DEPENDENT_JOIN:
	case LogicalOperatorType::LOGICAL_DELIM_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN:
	case LogicalOperatorType::LOGICAL_CROSS_PRODUCT: {
		auto *join = dynamic_cast<LogicalJoin *>(&op);
		if (join && (join->join_type == JoinType::SEMI || join->join_type == JoinType::ANTI ||
		             join->join_type == JoinType::MARK)) {
			return DeltaModelNodeKind::SEMI_ANTI;
		}
		return DeltaModelNodeKind::JOIN;
	}
	default:
		return DeltaModelNodeKind::OTHER;
	}
}

void BuildDeltaModelNodes(DeltaViewModel &model, const CreateMVPlanFacts &facts, bool full_only) {
	model.nodes.clear();
	model.profile_counts = DeltaModelProfileCounts();
	model.max_table_index = facts.max_table_index;
	for (auto &plan_node : facts.plan_nodes_post_order) {
		auto &op = *plan_node.plan_node;
		DeltaModelNode node;
		node.kind = NodeKindForOperator(op);
		node.plan_node = &op;
		if (node.kind == DeltaModelNodeKind::SCAN &&
		    facts.occurrence_by_index.count(op.Cast<LogicalGet>().table_index)) {
			model.profile_counts.scan_occurrences++;
		}
		if (!full_only) {
			CountProfileNode(model.profile_counts, node.kind, model, facts.analysis);
		}
		model.nodes.push_back(node);
	}
	D_ASSERT(facts.plan_nodes_post_order.empty() || facts.plan_nodes_post_order.back().plan_node == facts.root);
}

void PopulateDeltaViewModelLineage(DeltaViewModel &model, const CreateMVPlanFacts &facts,
                                   const vector<string> &output_names) {
	model.window_lineage_ops.clear();
	model.has_projection_lineage = false;
	model.has_left_join_key_source = false;
	model.has_left_join_nullable = false;
	model.projection_lineage = RefreshMetadata::ProjectionKeyLineage();
	model.left_join_key_source = RefreshMetadata::LeftJoinKeySource();
	model.left_join_nullable_sources = RefreshMetadata::LeftJoinNullableSources();
	const auto &analysis = facts.analysis;
	if (model.type == RefreshType::WINDOW_PARTITION) {
		const auto lineage_partition_columns = model.window_partition_columns;
		vector<RefreshMetadata::WindowPartitionLineageOp> direct_lineage_ops;
		bool has_lineage = BuildWindowPartitionLineageOps(facts, lineage_partition_columns, model.window_lineage_ops,
		                                                  &direct_lineage_ops);
		model.window_partition_columns = ProjectedWindowPartitionColumns(lineage_partition_columns, output_names);
		if (analysis.found_asof_join &&
		    (!has_lineage || AsofWindowPartitionReadsRightSideDirectly(direct_lineage_ops, facts) ||
		     !WindowLineageCoversAllSources(model.window_lineage_ops, facts))) {
			model.type = RefreshType::FULL_REFRESH;
			model.window_lineage_ops.clear();
		}
		return;
	}
	if (model.type != RefreshType::SIMPLE_PROJECTION || analysis.found_full_outer) {
		return;
	}
	if (!analysis.found_left_join) {
		model.has_projection_lineage = BuildProjectionKeyLineage(facts, output_names, model.projection_lineage);
	}
	model.has_left_join_key_source =
	    analysis.found_left_join && BuildLeftJoinKeySource(facts, model.left_join_key_source);
	model.has_left_join_nullable = BuildLeftJoinNullableSources(facts, model.left_join_nullable_sources);
}

string BuildDeltaViewModelLineageJson(const DeltaViewModel &model) {
	vector<string> entries;
	if (!model.window_lineage_ops.empty()) {
		entries.push_back(RefreshMetadata::WindowPartitionLineageToJson(model.window_lineage_ops));
	}
	if (model.has_projection_lineage) {
		entries.push_back(RefreshMetadata::ProjectionKeyLineageToJson(model.projection_lineage));
	}
	if (model.has_left_join_key_source) {
		entries.push_back(RefreshMetadata::LeftJoinKeySourceToJson(model.left_join_key_source));
	}
	if (!model.full_outer_scopes.empty()) {
		entries.push_back(RefreshMetadata::FullOuterScopesToJson(model.full_outer_scopes));
	}
	if (model.has_left_join_nullable) {
		entries.push_back(RefreshMetadata::LeftJoinNullableSourcesToJson(model.left_join_nullable_sources));
	}
	return BuildRefreshLineageJson(entries);
}

string BuildDeltaViewModelProfileDetail(const DeltaViewModel &model) {
	auto &counts = model.profile_counts;
	return "refresh_type=" + string(RefreshTypeName(model.type)) +
	       "; group_cols=" + to_string(model.group_columns.size()) + "; model_nodes=" + to_string(model.nodes.size()) +
	       "; group_recompute_mode=" + string(GroupRecomputeAffectedModeName(model.group_recompute_affected_mode)) +
	       "; scan_occurrences=" + to_string(counts.scan_occurrences) +
	       "; lineage_entries=" + to_string(model.LineageEntryCount()) +
	       "; node_semantics=" + to_string(counts.node_semantics) +
	       "; delta_only_nodes=" + to_string(counts.delta_only_nodes) +
	       "; delta_state_nodes=" + to_string(counts.delta_state_nodes);
}

} // namespace duckdb
