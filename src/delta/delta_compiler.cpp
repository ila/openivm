#include "delta/delta_compiler.hpp"

#include "core/ivm_delta_model.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_plan_helpers.hpp"
#include "core/refresh_metadata.hpp"
#include "delta/delta_operator.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

namespace {

static string CurrentCatalog(ClientContext &context) {
	auto &sp = ClientData::Get(context).catalog_search_path;
	auto def = sp->GetDefault();
	return def.catalog;
}

static bool AllSourcesAreDuckLake(const CreateMVPlanFacts &facts) {
	if (facts.source_table_info.empty()) {
		return false;
	}
	for (auto &entry : facts.source_table_info) {
		if (facts.ducklake_table_info.find(StringUtil::Lower(entry.second.table_name)) ==
		    facts.ducklake_table_info.end()) {
			return false;
		}
	}
	return true;
}

class ScopedJoinKeyDomainProbeSuppression {
public:
	ScopedJoinKeyDomainProbeSuppression(DeltaCompileAssumptions &assumptions_p, bool suppress)
	    : assumptions(assumptions_p), previous(assumptions_p.suppress_join_key_domain_probe), active(suppress) {
		if (active) {
			assumptions.suppress_join_key_domain_probe = true;
		}
	}

	~ScopedJoinKeyDomainProbeSuppression() {
		if (active) {
			assumptions.suppress_join_key_domain_probe = previous;
		}
	}

private:
	DeltaCompileAssumptions &assumptions;
	bool previous;
	bool active;
};

} // namespace

DeltaCompileContext::DeltaCompileContext(OptimizerExtensionInput &input, Connection &metadata_con, const string &view,
                                         const DeltaViewModel &model, DeltaCompileAssumptions assumptions)
    : input(input), metadata_con(metadata_con), view(view), model(model), assumptions(assumptions) {
}

DeltaCompiler::DeltaCompiler(OptimizerExtensionInput &input, Connection &metadata_con, const string &view,
                             const DeltaViewModel &model, DeltaCompileAssumptions assumptions)
    : DeltaCompiler(DeltaCompileContext(input, metadata_con, view, model, assumptions)) {
}

DeltaCompiler::DeltaCompiler(DeltaCompileContext context) : context(context) {
	for (auto &node : context.model.nodes) {
		if (node.plan_node) {
			node_by_plan[node.plan_node] = node.id;
		}
	}
}

const DeltaModelNode *DeltaCompiler::FindNode(LogicalOperator *op) const {
	if (!op) {
		return nullptr;
	}
	auto entry = node_by_plan.find(op);
	if (entry == node_by_plan.end() || entry->second >= context.model.nodes.size()) {
		return nullptr;
	}
	return &context.model.nodes[entry->second];
}

DeltaPlanFragment DeltaCompiler::CompileInternal(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root,
                                                 bool copied_subtree) {
	if (copied_subtree) {
		OPENIVM_DEBUG_PRINT("[Delta Compiler] copied subtree %s; using copied-subtree compile\n",
		                    LogicalOperatorToString(plan->type).c_str());
		return CompileCopiedDeltaSubtree(DeltaOperatorInput(context, plan, root, this, true));
	}

	auto *node = FindNode(plan.get());
	if (!node) {
		return CompileNonModelLeaf(DeltaOperatorInput(context, plan, root, this, false));
	}

	OPENIVM_DEBUG_PRINT("[Delta Compiler] Visiting node %llu (%s) for operator %s\n", (unsigned long long)node->id,
	                    DeltaModelNodeKindName(node->kind), LogicalOperatorToString(plan->type).c_str());
	return CompileDeltaOperatorWithModel(DeltaOperatorInput(context, plan, root, this, false), *node);
}

DeltaPlanFragment DeltaCompiler::Compile(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root) {
	return CompileInternal(plan, root, false);
}

DeltaPlanFragment DeltaCompiler::CompileCopiedSubtree(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root) {
	return CompileInternal(plan, root, true);
}

DeltaViewModel BuildRefreshDeltaViewModel(OptimizerExtensionInput &input, Connection &metadata_con,
                                          const string &view_name, LogicalOperator *optimized_plan,
                                          const vector<string> &output_names,
                                          DeltaCompileAssumptions *out_assumptions) {
	// TODO: Store/cache the create-time DeltaViewModel and reuse it here. For now refresh planning rebuilds the IR
	// from the optimized view plan so the recursive rewriter can consume one coherent node model.
	auto facts = BuildCreateMVPlanFacts(optimized_plan, CurrentCatalog(input.context));
	auto &analysis = facts.analysis;

	DeltaCompileAssumptions assumptions;
	assumptions.all_sources_are_ducklake = AllSourcesAreDuckLake(facts);
	// Keep joined-window partition metadata. Downstream refresh planning validates whether lineage covers every source
	// before using a partial affected-partition program.
	assumptions.keep_window_join_partitions = true;
	assumptions.has_unsupported_incremental_construct = facts.has_unsupported_set_operation || facts.has_pivot;
	if (out_assumptions) {
		*out_assumptions = assumptions;
	}

	DeltaViewModelInput model_input;
	model_input.facts = &facts;
	model_input.output_names = &output_names;
	model_input.has_unsupported_incremental_construct = assumptions.has_unsupported_incremental_construct;
	model_input.keep_window_join_partitions = assumptions.keep_window_join_partitions;

	RefreshMetadata metadata(metadata_con);
	RefreshMetadata::DistinctAuxMeta distinct_aux;
	if (metadata.GetDistinctAuxMeta(view_name, distinct_aux)) {
		model_input.distinct_aux_candidate = &distinct_aux;
	}
	FilteredGroupCountAuxRequirement filtered_aux;
	if (metadata.GetFilteredGroupCountAuxMeta(view_name, filtered_aux.meta)) {
		filtered_aux.create_source = filtered_aux.meta.source;
		model_input.filtered_group_count_aux_candidate = &filtered_aux;
	}
	RefreshMetadata::SemiAntiAuxMeta semi_anti_aux;
	if (metadata.GetSemiAntiAuxMeta(view_name, semi_anti_aux)) {
		model_input.semi_anti_aux_candidate = &semi_anti_aux;
	}

	auto model = BuildDeltaViewModel(model_input);
	PopulateDeltaViewModelLineage(model, facts, output_names);
	return model;
}

void LogDeltaModelSummary(const DeltaViewModel &model) {
	OPENIVM_DEBUG_PRINT("[IR Rewrite] model type=%s nodes=%zu root=%llu features=%zu strategies=%zu unsupported=%zu "
	                    "domains=%zu lineage=%zu group_cols=%zu window_cols=%zu aggregate_types=%zu "
	                    "update_semantics=%zu window_lineage=%zu projection_lineage=%s full_outer_cols=%s "
	                    "aux(distinct=%s,filtered_group=%s,semi_anti=%s)\n",
	                    RefreshTypeName(model.type), model.nodes.size(), (unsigned long long)model.root_node,
	                    model.features.size(), model.strategy_reasons.size(), model.unsupported_reasons.size(),
	                    model.affected_domains.size(), model.lineage_facts.size(), model.group_columns.size(),
	                    model.window_partition_columns.size(), model.aggregate_types.size(),
	                    model.update_semantics.size(), model.window_lineage_ops.size(),
	                    model.has_projection_lineage ? "true" : "false", model.full_outer_join_cols.c_str(),
	                    model.HasDistinctAux() ? "true" : "false", model.HasFilteredGroupCountAux() ? "true" : "false",
	                    model.HasSemiAntiAux() ? "true" : "false");
}

DeltaPlanFragment DeltaOperatorInput::CompileChild(unique_ptr<LogicalOperator> &child,
                                                   LogicalOperator *&child_root) const {
	if (!compiler) {
		throw InternalException("DeltaOperatorInput::CompileChild requires a DeltaCompiler");
	}
	return copied_subtree ? compiler->CompileCopiedSubtree(child, child_root) : compiler->Compile(child, child_root);
}

DeltaPlanFragment DeltaOperatorInput::CompileCopiedSubtree(unique_ptr<LogicalOperator> &subtree,
                                                           LogicalOperator *&subtree_root,
                                                           bool suppress_join_key_domain_probe) const {
	if (!compiler) {
		throw InternalException("DeltaOperatorInput::CompileCopiedSubtree requires a DeltaCompiler");
	}
	ScopedJoinKeyDomainProbeSuppression suppression(context.assumptions, suppress_join_key_domain_probe);
	return compiler->CompileCopiedSubtree(subtree, subtree_root);
}

} // namespace duckdb
