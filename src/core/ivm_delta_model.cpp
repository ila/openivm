#include "core/ivm_delta_model.hpp"

#include "core/parser_plan_helpers.hpp"
#include "core/vector_utils.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

#include <algorithm>
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

static void AddOutputColumnName(const string &name, vector<string> &visible, vector<string> &hidden) {
	if (name.empty()) {
		return;
	}
	if (IncrementalTableNames::IsInternalColumn(name)) {
		AddStringCI(hidden, name);
	} else {
		AddStringCI(visible, name);
	}
}

static void SplitOutputColumns(const vector<string> &input, vector<string> &visible, vector<string> &hidden) {
	for (auto &name : input) {
		AddOutputColumnName(name, visible, hidden);
	}
}

static void AddExpressionNames(const vector<unique_ptr<Expression>> &expressions, vector<string> &visible,
                               vector<string> &hidden) {
	for (auto &expr : expressions) {
		if (!expr) {
			continue;
		}
		AddOutputColumnName(expr->alias.empty() ? expr->GetName() : expr->alias, visible, hidden);
	}
}

static vector<string> SourceTablesFromFacts(const CreateMVPlanFacts &facts) {
	vector<string> tables;
	for (auto &occurrence : facts.source_occurrences) {
		AddStringCI(tables, occurrence.table);
	}
	if (tables.empty()) {
		for (auto &entry : facts.source_table_info) {
			AddStringCI(tables, entry.second.table_name);
		}
		std::sort(tables.begin(), tables.end(), [](const string &left, const string &right) {
			return StringUtil::Lower(left) < StringUtil::Lower(right);
		});
	}
	return tables;
}

static vector<string> SourceTablesFromChildren(const DeltaViewModel &model, const vector<idx_t> &children) {
	vector<string> tables;
	for (auto child_id : children) {
		if (child_id >= model.nodes.size()) {
			continue;
		}
		for (auto &source : model.nodes[child_id].source_tables) {
			AddStringCI(tables, source);
		}
	}
	return tables;
}

static DeltaModelNodeKind NodeKindForOperator(LogicalOperator &op) {
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

static DeltaRuleKind RuleKindForNode(DeltaModelNodeKind kind, LogicalOperator &op, const DeltaViewModel &model,
                                     const PlanAnalysis &analysis) {
	if (!model.unsupported_reasons.empty()) {
		return DeltaRuleKind::FULL_ONLY;
	}
	switch (kind) {
	case DeltaModelNodeKind::SCAN:
	case DeltaModelNodeKind::FILTER:
	case DeltaModelNodeKind::PROJECT:
	case DeltaModelNodeKind::UNION:
	case DeltaModelNodeKind::UNNEST:
	case DeltaModelNodeKind::CTE:
	case DeltaModelNodeKind::CONSTANT:
		return DeltaRuleKind::LINEAR;
	case DeltaModelNodeKind::JOIN: {
		auto *join = dynamic_cast<LogicalJoin *>(&op);
		if (join && (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT ||
		             join->join_type == JoinType::OUTER)) {
			return DeltaRuleKind::STATEFUL;
		}
		return DeltaRuleKind::PRODUCT;
	}
	case DeltaModelNodeKind::ASOF_JOIN:
		return DeltaRuleKind::STATEFUL;
	case DeltaModelNodeKind::POSITIONAL_JOIN:
	case DeltaModelNodeKind::SAMPLE:
		return DeltaRuleKind::NON_LINEAR;
	case DeltaModelNodeKind::AGGREGATE:
		return analysis.found_minmax || analysis.found_count_distinct || analysis.found_list ||
		               analysis.found_filtered_list
		           ? DeltaRuleKind::NON_LINEAR
		           : DeltaRuleKind::STATEFUL;
	case DeltaModelNodeKind::WINDOW:
	case DeltaModelNodeKind::DISTINCT:
	case DeltaModelNodeKind::SEMI_ANTI:
	case DeltaModelNodeKind::TOP_K:
		return DeltaRuleKind::STATEFUL;
	default:
		return DeltaRuleKind::FULL_ONLY;
	}
}

static void AddNodeAuxRequirements(DeltaModelNode &node, const DeltaViewModel &model) {
	if (model.HasDistinctAux() && node.kind == DeltaModelNodeKind::DISTINCT) {
		AddUnique(node.required_aux_states, DeltaAuxStateKind::DISTINCT_COUNT);
	}
	if (model.HasFilteredGroupCountAux() && node.kind == DeltaModelNodeKind::AGGREGATE) {
		AddUnique(node.required_aux_states, DeltaAuxStateKind::FILTERED_GROUP_COUNT);
	}
	if (model.HasSemiAntiAux() && node.kind == DeltaModelNodeKind::SEMI_ANTI) {
		AddUnique(node.required_aux_states, DeltaAuxStateKind::SEMI_ANTI_MATCH);
	}
}

static void AddNodeUnsupportedReasons(DeltaModelNode &node, const DeltaViewModel &model) {
	for (auto reason : model.unsupported_reasons) {
		switch (reason) {
		case DeltaUnsupportedReason::UNSUPPORTED_UNNEST:
			if (node.kind == DeltaModelNodeKind::UNNEST) {
				AddUnique(node.unsupported_reasons, reason);
			}
			break;
		case DeltaUnsupportedReason::UNSUPPORTED_AGGREGATE:
		case DeltaUnsupportedReason::UNSUPPORTED_FILTERED_AGGREGATE:
		case DeltaUnsupportedReason::GROUPING_SETS_NO_KEYS:
		case DeltaUnsupportedReason::FILTERED_LIST_NO_KEYS:
			if (node.kind == DeltaModelNodeKind::AGGREGATE) {
				AddUnique(node.unsupported_reasons, reason);
			}
			break;
		case DeltaUnsupportedReason::UNSUPPORTED_JOIN_TYPE:
			if (node.kind == DeltaModelNodeKind::JOIN || node.kind == DeltaModelNodeKind::SEMI_ANTI) {
				AddUnique(node.unsupported_reasons, reason);
			}
			break;
		case DeltaUnsupportedReason::SEMI_ANTI_WITH_AGGREGATE:
		case DeltaUnsupportedReason::SEMI_ANTI_MISSING_AUX:
			if (node.kind == DeltaModelNodeKind::SEMI_ANTI) {
				AddUnique(node.unsupported_reasons, reason);
			}
			break;
		case DeltaUnsupportedReason::UNSUPPORTED_ORDER_BY:
			if (node.kind == DeltaModelNodeKind::TOP_K) {
				AddUnique(node.unsupported_reasons, reason);
			}
			break;
		default:
			break;
		}
	}
}

static void AddNodeUpdateSemantics(DeltaModelNode &node, const DeltaViewModel &model) {
	if (node.rule == DeltaRuleKind::FULL_ONLY) {
		return;
	}
	if (node.kind == DeltaModelNodeKind::PROJECT) {
		AddUnique(node.update_semantics, DeltaUpdateSemantics::APPEND_ONLY_SAFE);
		AddUnique(node.update_semantics, DeltaUpdateSemantics::PROJECTION_DELETE_SKIP_SAFE);
	}
	if (node.kind == DeltaModelNodeKind::AGGREGATE) {
		if (model.HasFeature(DeltaModelFeature::AGGREGATE_LINEAR)) {
			AddUnique(node.update_semantics, DeltaUpdateSemantics::APPEND_ONLY_SAFE);
			AddUnique(node.update_semantics, DeltaUpdateSemantics::AGGREGATE_DELETE_SKIP_SAFE);
		}
		if (model.HasFeature(DeltaModelFeature::AGGREGATE_NON_LINEAR) &&
		    model.HasFeature(DeltaModelFeature::COUNT_DISTINCT_STATEFUL)) {
			AddUnique(node.update_semantics, DeltaUpdateSemantics::DELETE_SENSITIVE);
			AddUnique(node.update_semantics, DeltaUpdateSemantics::UPDATE_SENSITIVE);
		}
		if (ContainsStringCI(model.aggregate_types, "min") || ContainsStringCI(model.aggregate_types, "max")) {
			AddUnique(node.update_semantics, DeltaUpdateSemantics::MINMAX_INSERT_ONLY_SAFE);
		}
	}
	if (node.kind == DeltaModelNodeKind::JOIN || node.kind == DeltaModelNodeKind::ASOF_JOIN ||
	    node.kind == DeltaModelNodeKind::POSITIONAL_JOIN || node.kind == DeltaModelNodeKind::SAMPLE ||
	    node.kind == DeltaModelNodeKind::WINDOW || node.kind == DeltaModelNodeKind::DISTINCT ||
	    node.kind == DeltaModelNodeKind::SEMI_ANTI) {
		AddUnique(node.update_semantics, DeltaUpdateSemantics::DELETE_SENSITIVE);
		AddUnique(node.update_semantics, DeltaUpdateSemantics::UPDATE_SENSITIVE);
	}
}

static bool NodeHasAuxState(const DeltaModelNode &node) {
	return !node.required_aux_states.empty();
}

static DeltaNodeMaintenance BuildNodeMaintenance(const DeltaModelNode &node, const DeltaViewModel &model) {
	DeltaNodeMaintenance maintenance;
	if (node.rule == DeltaRuleKind::FULL_ONLY || !node.unsupported_reasons.empty()) {
		return maintenance;
	}

	// TODO: Before recursive refresh planning consumes this, decide whether state kinds need to distinguish
	// pre-update base, post-update base, and pre-refresh MV state explicitly.
	switch (node.kind) {
	case DeltaModelNodeKind::SCAN:
	case DeltaModelNodeKind::FILTER:
	case DeltaModelNodeKind::PROJECT:
	case DeltaModelNodeKind::UNION:
	case DeltaModelNodeKind::UNNEST:
	case DeltaModelNodeKind::CTE:
	case DeltaModelNodeKind::CONSTANT:
		maintenance.mode = DeltaMaintenanceMode::DELTA_ONLY;
		break;
	case DeltaModelNodeKind::JOIN:
		maintenance.mode = DeltaMaintenanceMode::DELTA_WITH_STATE;
		maintenance.state = DeltaMaintenanceStateKind::CURRENT_RELATION;
		break;
	case DeltaModelNodeKind::ASOF_JOIN:
	case DeltaModelNodeKind::POSITIONAL_JOIN:
	case DeltaModelNodeKind::SAMPLE:
		maintenance.mode = DeltaMaintenanceMode::AFFECTED_DOMAIN_RECOMPUTE;
		maintenance.state = DeltaMaintenanceStateKind::MV_DATA;
		break;
	case DeltaModelNodeKind::AGGREGATE:
		if (model.type == RefreshType::GROUP_RECOMPUTE) {
			maintenance.mode = DeltaMaintenanceMode::AFFECTED_DOMAIN_RECOMPUTE;
			maintenance.state = DeltaMaintenanceStateKind::MV_DATA;
		} else {
			maintenance.mode = DeltaMaintenanceMode::DELTA_WITH_STATE;
			maintenance.state =
			    NodeHasAuxState(node) ? DeltaMaintenanceStateKind::AUX_TABLE : DeltaMaintenanceStateKind::MV_DATA;
		}
		break;
	case DeltaModelNodeKind::WINDOW:
		maintenance.mode = DeltaMaintenanceMode::AFFECTED_DOMAIN_RECOMPUTE;
		maintenance.state = DeltaMaintenanceStateKind::MV_DATA;
		break;
	case DeltaModelNodeKind::DISTINCT:
	case DeltaModelNodeKind::SEMI_ANTI:
		maintenance.mode = NodeHasAuxState(node) ? DeltaMaintenanceMode::DELTA_WITH_STATE
		                                         : DeltaMaintenanceMode::AFFECTED_DOMAIN_RECOMPUTE;
		maintenance.state =
		    NodeHasAuxState(node) ? DeltaMaintenanceStateKind::AUX_TABLE : DeltaMaintenanceStateKind::MV_DATA;
		break;
	case DeltaModelNodeKind::TOP_K:
		maintenance.mode = DeltaMaintenanceMode::AFFECTED_DOMAIN_RECOMPUTE;
		maintenance.state = DeltaMaintenanceStateKind::MV_DATA;
		break;
	default:
		break;
	}
	return maintenance;
}

static idx_t AddModelNode(DeltaViewModel &model, DeltaModelNode node) {
	node.id = model.nodes.size();
	model.nodes.push_back(std::move(node));
	return model.nodes.back().id;
}

static idx_t BuildModelNodeForPlan(DeltaViewModel &model, LogicalOperator *op, const CreateMVPlanFacts &facts,
                                   const vector<string> &output_names) {
	if (!op) {
		return DConstants::INVALID_INDEX;
	}
	vector<idx_t> children;
	for (auto &child : op->children) {
		auto child_id = BuildModelNodeForPlan(model, child.get(), facts, output_names);
		if (child_id != DConstants::INVALID_INDEX) {
			children.push_back(child_id);
		}
	}

	DeltaModelNode node;
	node.kind = NodeKindForOperator(*op);
	node.rule = RuleKindForNode(node.kind, *op, model, facts.analysis);
	node.plan_node = op;
	node.children = std::move(children);
	node.source_tables = SourceTablesFromChildren(model, node.children);
	AddNodeAuxRequirements(node, model);
	AddNodeUnsupportedReasons(node, model);
	AddNodeUpdateSemantics(node, model);

	if (node.kind == DeltaModelNodeKind::SCAN) {
		auto *get = dynamic_cast<LogicalGet *>(op);
		if (get) {
			auto occurrence_it = facts.occurrence_by_index.find(get->table_index);
			if (occurrence_it != facts.occurrence_by_index.end()) {
				node.source_table = occurrence_it->second.table;
				node.source_occurrence = occurrence_it->second.occurrence;
				node.source_table_index = occurrence_it->second.table_index;
				AddStringCI(node.source_tables, node.source_table);
			}
			SplitOutputColumns(get->names, node.output_columns, node.hidden_columns);
		}
	} else if (node.kind == DeltaModelNodeKind::PROJECT || node.kind == DeltaModelNodeKind::WINDOW ||
	           node.kind == DeltaModelNodeKind::DISTINCT || node.kind == DeltaModelNodeKind::TOP_K) {
		AddExpressionNames(op->expressions, node.output_columns, node.hidden_columns);
	}
	if (node.kind == DeltaModelNodeKind::AGGREGATE) {
		node.affected_key_columns = model.group_columns;
		SplitOutputColumns(model.group_columns, node.output_columns, node.hidden_columns);
		AddExpressionNames(op->expressions, node.output_columns, node.hidden_columns);
	}
	if (node.kind == DeltaModelNodeKind::WINDOW) {
		node.affected_key_columns = model.window_partition_columns;
	}
	if (op == facts.root) {
		SplitOutputColumns(output_names, node.output_columns, node.hidden_columns);
	}
	return AddModelNode(model, std::move(node));
}

static idx_t FindFirstNodeId(const DeltaViewModel &model, DeltaModelNodeKind kind) {
	for (auto &node : model.nodes) {
		if (node.kind == kind) {
			return node.id;
		}
	}
	return DConstants::INVALID_INDEX;
}

static idx_t FindSourceScanNodeId(const DeltaViewModel &model, const string &table, idx_t occurrence) {
	idx_t found = DConstants::INVALID_INDEX;
	for (auto &node : model.nodes) {
		if (node.kind != DeltaModelNodeKind::SCAN || !StringUtil::CIEquals(node.source_table, table)) {
			continue;
		}
		if (occurrence != DConstants::INVALID_INDEX) {
			if (node.source_occurrence == occurrence) {
				return node.id;
			}
			continue;
		}
		if (found != DConstants::INVALID_INDEX) {
			return DConstants::INVALID_INDEX;
		}
		found = node.id;
	}
	return found;
}

static idx_t FindRootOrFirstNodeId(const DeltaViewModel &model, DeltaModelNodeKind kind) {
	if (model.root_node != DConstants::INVALID_INDEX && model.root_node < model.nodes.size() &&
	    model.nodes[model.root_node].kind == kind) {
		return model.root_node;
	}
	return FindFirstNodeId(model, kind);
}

static void AddLineageFact(DeltaViewModel &model, DeltaLineageFact fact) {
	fact.node_id = FindSourceScanNodeId(model, fact.source_table, fact.source_occurrence);
	if (fact.node_id != DConstants::INVALID_INDEX && fact.node_id < model.nodes.size()) {
		model.nodes[fact.node_id].lineage_facts.push_back(fact);
	}
	model.lineage_facts.push_back(std::move(fact));
}

static void AddAffectedDomain(DeltaViewModel &model, DeltaAffectedDomain domain) {
	for (auto &existing : model.affected_domains) {
		if (existing.kind != domain.kind) {
			continue;
		}
		bool same_keys = existing.key_columns.size() == domain.key_columns.size();
		for (idx_t i = 0; same_keys && i < existing.key_columns.size(); i++) {
			same_keys = StringUtil::CIEquals(existing.key_columns[i], domain.key_columns[i]);
		}
		if (same_keys) {
			return;
		}
	}
	if (domain.node_id != DConstants::INVALID_INDEX && domain.node_id < model.nodes.size()) {
		model.nodes[domain.node_id].affected_domains.push_back(domain);
	}
	model.affected_domains.push_back(std::move(domain));
}

static void CountMaintenanceMode(const DeltaModelNode &node, idx_t &delta_only, idx_t &delta_with_state,
                                 idx_t &affected_recompute, idx_t &full_recompute) {
	switch (node.maintenance.mode) {
	case DeltaMaintenanceMode::DELTA_ONLY:
		delta_only++;
		break;
	case DeltaMaintenanceMode::DELTA_WITH_STATE:
		delta_with_state++;
		break;
	case DeltaMaintenanceMode::AFFECTED_DOMAIN_RECOMPUTE:
		affected_recompute++;
		break;
	case DeltaMaintenanceMode::FULL_RECOMPUTE:
		full_recompute++;
		break;
	}
}

static void RefreshDeltaNodeMaintenance(DeltaViewModel &model) {
	for (auto &node : model.nodes) {
		node.maintenance = BuildNodeMaintenance(node, model);
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

static vector<string> AsofRightSourceTables(const DeltaViewModel &model) {
	vector<string> tables;
	for (auto &node : model.nodes) {
		if (node.kind != DeltaModelNodeKind::ASOF_JOIN || node.children.size() < 2) {
			continue;
		}
		auto right_id = node.children[1];
		if (right_id >= model.nodes.size()) {
			continue;
		}
		for (auto &source : model.nodes[right_id].source_tables) {
			AddStringCI(tables, source);
		}
	}
	return tables;
}

static bool
AsofWindowPartitionReadsRightSideDirectly(const vector<RefreshMetadata::WindowPartitionLineageOp> &lineage_ops,
                                          const DeltaViewModel &model) {
	auto rhs_sources = AsofRightSourceTables(model);
	if (rhs_sources.empty()) {
		return false;
	}
	for (auto &op : lineage_ops) {
		if (op.kind == "direct" && ContainsStringCI(rhs_sources, op.source)) {
			return true;
		}
	}
	return false;
}

} // namespace

const char *DeltaMaintenanceModeName(DeltaMaintenanceMode mode) {
	switch (mode) {
	case DeltaMaintenanceMode::DELTA_ONLY:
		return "DELTA_ONLY";
	case DeltaMaintenanceMode::DELTA_WITH_STATE:
		return "DELTA_WITH_STATE";
	case DeltaMaintenanceMode::AFFECTED_DOMAIN_RECOMPUTE:
		return "AFFECTED_DOMAIN_RECOMPUTE";
	case DeltaMaintenanceMode::FULL_RECOMPUTE:
		return "FULL_RECOMPUTE";
	default:
		return "UNKNOWN";
	}
}

const char *DeltaMaintenanceStateKindName(DeltaMaintenanceStateKind state) {
	switch (state) {
	case DeltaMaintenanceStateKind::NONE:
		return "NONE";
	case DeltaMaintenanceStateKind::CURRENT_RELATION:
		return "CURRENT_RELATION";
	case DeltaMaintenanceStateKind::MV_DATA:
		return "MV_DATA";
	case DeltaMaintenanceStateKind::AUX_TABLE:
		return "AUX_TABLE";
	default:
		return "UNKNOWN";
	}
}

void BuildDeltaModelNodes(DeltaViewModel &model, const CreateMVPlanFacts &facts, const vector<string> &output_names) {
	model.nodes.clear();
	model.root_node = BuildModelNodeForPlan(model, facts.root, facts, output_names);
	RefreshDeltaNodeMaintenance(model);
}

void BuildDeltaModelBaseAffectedDomains(DeltaViewModel &model, const CreateMVPlanFacts &facts) {
	if (!model.group_columns.empty()) {
		DeltaAffectedDomain domain;
		domain.kind = DeltaAffectedDomainKind::GROUP;
		domain.node_id = FindRootOrFirstNodeId(model, DeltaModelNodeKind::AGGREGATE);
		domain.key_columns = model.group_columns;
		domain.source_tables = SourceTablesFromFacts(facts);
		domain.delta_local = !facts.analysis.found_join && facts.source_occurrences.size() <= 1;
		domain.needs_base_lookup = facts.analysis.found_join;
		AddAffectedDomain(model, std::move(domain));
	}
	if (model.HasSemiAntiAux()) {
		DeltaAffectedDomain domain;
		domain.kind = DeltaAffectedDomainKind::SEMI_ANTI_PREDICATE;
		domain.node_id = FindFirstNodeId(model, DeltaModelNodeKind::SEMI_ANTI);
		domain.key_columns = model.semi_anti_aux.left_cols;
		AddStringCI(domain.source_tables, model.semi_anti_aux.left_table);
		AddStringCI(domain.source_tables, model.semi_anti_aux.right_table);
		domain.delta_local = false;
		domain.needs_base_lookup = true;
		AddAffectedDomain(model, std::move(domain));
	}
}

void ValidateDeltaViewModelInvariants(const DeltaViewModel &model) {
	D_ASSERT(!model.HasFeature(DeltaModelFeature::DISTINCT_STATEFUL) || model.HasDistinctAux());
	D_ASSERT(!model.HasFeature(DeltaModelFeature::SEMI_ANTI_STATEFUL) || model.HasSemiAntiAux());
	D_ASSERT(!model.HasFeature(DeltaModelFeature::WINDOW_AFFECTED_PARTITION) ||
	         !model.window_partition_columns.empty());
	D_ASSERT(model.type != RefreshType::DISTINCT_INCREMENTAL || model.HasFeature(DeltaModelFeature::DISTINCT_STATEFUL));
	D_ASSERT(model.type != RefreshType::SEMI_ANTI_RECOMPUTE || model.HasFeature(DeltaModelFeature::SEMI_ANTI_STATEFUL));
	D_ASSERT(model.root_node == DConstants::INVALID_INDEX || model.root_node < model.nodes.size());
	for (auto &node : model.nodes) {
		D_ASSERT(node.id < model.nodes.size());
		D_ASSERT(node.plan_node);
		if (node.kind == DeltaModelNodeKind::SCAN && !node.source_table.empty()) {
			D_ASSERT(node.source_occurrence != DConstants::INVALID_INDEX);
			D_ASSERT(node.source_table_index != DConstants::INVALID_INDEX);
		}
		if (node.rule == DeltaRuleKind::FULL_ONLY) {
			D_ASSERT(node.maintenance.mode == DeltaMaintenanceMode::FULL_RECOMPUTE);
		}
		for (auto &domain : node.affected_domains) {
			D_ASSERT(domain.node_id == node.id);
		}
		for (auto &fact : node.lineage_facts) {
			D_ASSERT(fact.node_id == node.id);
		}
	}
	for (auto &domain : model.affected_domains) {
		D_ASSERT(domain.node_id == DConstants::INVALID_INDEX || domain.node_id < model.nodes.size());
	}
	for (auto &fact : model.lineage_facts) {
		D_ASSERT(fact.node_id == DConstants::INVALID_INDEX || fact.node_id < model.nodes.size());
	}
}

void PopulateDeltaViewModelLineage(DeltaViewModel &model, const CreateMVPlanFacts &facts,
                                   const vector<string> &output_names) {
	model.window_lineage_ops.clear();
	model.has_projection_lineage = false;
	model.projection_lineage = RefreshMetadata::ProjectionKeyLineage();
	model.lineage_facts.clear();
	for (auto &node : model.nodes) {
		node.lineage_facts.clear();
	}
	const auto &analysis = facts.analysis;
	if (model.type == RefreshType::WINDOW_PARTITION) {
		vector<RefreshMetadata::WindowPartitionLineageOp> direct_lineage_ops;
		bool has_lineage = BuildWindowPartitionLineageOps(facts, model.window_partition_columns,
		                                                  model.window_lineage_ops, &direct_lineage_ops);
		if (analysis.found_asof_join &&
		    (!has_lineage || AsofWindowPartitionReadsRightSideDirectly(direct_lineage_ops, model) ||
		     !WindowLineageCoversAllSources(model.window_lineage_ops, facts))) {
			model.type = RefreshType::CURRENT_DIFF_RECOMPUTE;
			model.window_lineage_ops.clear();
			AddUnique(model.features, DeltaModelFeature::CURRENT_DIFF_RECOMPUTE);
			AddUnique(model.strategy_reasons, DeltaStrategyReason::ASOF_CURRENT_DIFF_RECOMPUTE);
			ValidateDeltaViewModelInvariants(model);
			return;
		}
		if (has_lineage) {
			DeltaAffectedDomain domain;
			domain.kind = DeltaAffectedDomainKind::WINDOW_PARTITION;
			domain.node_id = FindFirstNodeId(model, DeltaModelNodeKind::WINDOW);
			domain.key_columns = model.window_partition_columns;
			domain.delta_local = true;
			for (auto &op : model.window_lineage_ops) {
				AddStringCI(domain.source_tables, op.source);
				if (op.kind == "lookup") {
					domain.delta_local = false;
					domain.needs_base_lookup = true;
				}
				DeltaLineageFact fact;
				fact.kind = DeltaLineageKind::WINDOW_PARTITION;
				fact.source_table = op.source;
				fact.source_column = op.source_col;
				fact.output_column = op.output_col;
				fact.lookup_table = op.lookup;
				fact.lookup_column = op.lookup_col;
				fact.lookup_output_column = op.lookup_out;
				AddLineageFact(model, std::move(fact));
			}
			AddAffectedDomain(model, std::move(domain));
		}
		ValidateDeltaViewModelInvariants(model);
		return;
	}
	if (model.type == RefreshType::SIMPLE_PROJECTION && !analysis.found_left_join && !analysis.found_full_outer) {
		if (BuildProjectionKeyLineage(facts, output_names, model.projection_lineage)) {
			model.has_projection_lineage = true;
			DeltaAffectedDomain domain;
			domain.kind = DeltaAffectedDomainKind::PROJECTION_KEY;
			domain.node_id = FindRootOrFirstNodeId(model, DeltaModelNodeKind::PROJECT);
			domain.key_columns.push_back(model.projection_lineage.output_col);
			domain.delta_local = true;
			for (auto &arm : model.projection_lineage.arms) {
				AddStringCI(domain.source_tables, arm.source);
				if (!arm.steps.empty()) {
					domain.delta_local = false;
					domain.needs_base_lookup = true;
				}
				DeltaLineageFact fact;
				fact.kind = DeltaLineageKind::PROJECTION_KEY;
				fact.source_table = arm.source;
				fact.source_occurrence = arm.occurrence;
				fact.source_column = arm.source_col;
				fact.output_column = model.projection_lineage.output_col;
				if (!arm.steps.empty()) {
					fact.lookup_table = arm.steps.back().table;
					fact.lookup_column = arm.steps.back().lookup_col;
					fact.lookup_output_column = arm.steps.back().lookup_out;
				}
				AddLineageFact(model, std::move(fact));
			}
			AddAffectedDomain(model, std::move(domain));
		}
	}
	if (model.HasSemiAntiAux()) {
		for (auto &col : model.semi_anti_aux.left_cols) {
			DeltaLineageFact fact;
			fact.kind = DeltaLineageKind::SEMI_ANTI_PREDICATE;
			fact.source_table = model.semi_anti_aux.left_table;
			fact.source_column = col;
			fact.output_column = col;
			AddLineageFact(model, std::move(fact));
		}
	}
	ValidateDeltaViewModelInvariants(model);
}

string BuildDeltaViewModelLineageJson(const DeltaViewModel &model) {
	vector<string> entries;
	if (!model.window_lineage_ops.empty()) {
		entries.push_back(RefreshMetadata::WindowPartitionLineageToJson(model.window_lineage_ops));
	}
	if (model.has_projection_lineage) {
		entries.push_back(RefreshMetadata::ProjectionKeyLineageToJson(model.projection_lineage));
	}
	return BuildRefreshLineageJson(entries);
}

string BuildDeltaViewModelProfileDetail(const DeltaViewModel &model) {
	idx_t scan_occurrences = 0;
	idx_t node_lineage_facts = 0;
	idx_t node_domains = 0;
	idx_t node_semantics = 0;
	idx_t node_unsupported = 0;
	idx_t delta_only_nodes = 0;
	idx_t delta_state_nodes = 0;
	idx_t affected_recompute_nodes = 0;
	idx_t full_recompute_nodes = 0;
	for (auto &node : model.nodes) {
		if (node.kind == DeltaModelNodeKind::SCAN && node.source_occurrence != DConstants::INVALID_INDEX) {
			scan_occurrences++;
		}
		node_lineage_facts += node.lineage_facts.size();
		node_domains += node.affected_domains.size();
		node_semantics += node.update_semantics.size();
		node_unsupported += node.unsupported_reasons.size();
		CountMaintenanceMode(node, delta_only_nodes, delta_state_nodes, affected_recompute_nodes, full_recompute_nodes);
	}
	return "refresh_type=" + string(RefreshTypeName(model.type)) +
	       "; group_cols=" + to_string(model.group_columns.size()) + "; model_nodes=" + to_string(model.nodes.size()) +
	       "; group_recompute_mode=" + string(GroupRecomputeAffectedModeName(model.group_recompute_affected_mode)) +
	       "; scan_occurrences=" + to_string(scan_occurrences) +
	       "; model_domains=" + to_string(model.affected_domains.size()) + "; node_domains=" + to_string(node_domains) +
	       "; lineage_entries=" + to_string(model.LineageEntryCount()) +
	       "; node_lineage=" + to_string(node_lineage_facts) + "; node_semantics=" + to_string(node_semantics) +
	       "; node_unsupported=" + to_string(node_unsupported) + "; delta_only_nodes=" + to_string(delta_only_nodes) +
	       "; delta_state_nodes=" + to_string(delta_state_nodes) +
	       "; affected_recompute_nodes=" + to_string(affected_recompute_nodes) +
	       "; full_recompute_nodes=" + to_string(full_recompute_nodes);
}

} // namespace duckdb
