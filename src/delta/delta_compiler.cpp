#include "delta/delta_compiler.hpp"

#include "core/ivm_delta_model.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_plan_helpers.hpp"
#include "delta/delta_operator.hpp"
#include "duckdb/main/client_data.hpp"

namespace duckdb {

namespace {

static string CurrentCatalog(ClientContext &context) {
	auto &sp = ClientData::Get(context).catalog_search_path;
	auto def = sp->GetDefault();
	return def.GetCatalog().GetIdentifierName();
}

} // namespace

DeltaCompileContext::DeltaCompileContext(OptimizerExtensionInput &input, Connection &metadata_con, const string &view,
                                         const DeltaViewModel &model)
    : input(input), metadata_con(metadata_con), view(view), model(model) {
}

DeltaCompiler::DeltaCompiler(OptimizerExtensionInput &input, Connection &metadata_con, const string &view,
                             const DeltaViewModel &model)
    : DeltaCompiler(DeltaCompileContext(input, metadata_con, view, model)) {
}

DeltaCompiler::DeltaCompiler(DeltaCompileContext context) : context(context) {
	for (idx_t node_id = 0; node_id < context.model.nodes.size(); node_id++) {
		node_by_plan[context.model.nodes[node_id].plan_node] = node_id;
	}
}

const DeltaModelNode *DeltaCompiler::FindNode(LogicalOperator *op) const {
	if (!op) {
		return nullptr;
	}
	auto entry = node_by_plan.find(op);
	if (entry == node_by_plan.end()) {
		return nullptr;
	}
	// Model node ids are vector indexes. Use checked access so malformed internal model metadata fails immediately.
	return &context.model.nodes.at(entry->second);
}

DeltaPlanFragment DeltaCompiler::CompileInternal(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root,
                                                 bool copied_subtree) {
	DeltaOperatorInput op_input(context, plan, root, this, copied_subtree);
	if (copied_subtree) {
		OPENIVM_DEBUG_PRINT("[Delta Compiler] copied subtree %s; using copied-subtree compile\n",
		                    LogicalOperatorToString(plan->type).c_str());
		OPENIVM_DEBUG_PRINT("[Copied Delta Subtree] Node detail: %s\n", plan->ToString().c_str());
		return CompileDeltaOperatorByKind(op_input, NodeKindForOperator(*plan));
	}

	auto *node = FindNode(plan.get());
	if (!node) {
		// Only constant leaves (dummy scans, expression/chunk gets) may lack an IR node; they carry a zero delta.
		if (NodeKindForOperator(*plan) != DeltaModelNodeKind::CONSTANT) {
			throw InternalException("Delta compiler missing IR node for operator %s",
			                        LogicalOperatorToString(plan->type).c_str());
		}
		OPENIVM_DEBUG_PRINT("[Delta Compiler] non-model constant leaf %s\n",
		                    LogicalOperatorToString(plan->type).c_str());
		return CompileDeltaOperatorByKind(op_input, DeltaModelNodeKind::CONSTANT);
	}

	OPENIVM_DEBUG_PRINT("[Delta Compiler] Visiting %s node for operator %s\n", DeltaModelNodeKindName(node->kind),
	                    LogicalOperatorToString(plan->type).c_str());
	return CompileDeltaOperatorByKind(op_input.WithNode(*node), node->kind);
}

DeltaPlanFragment DeltaCompiler::Compile(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root) {
	return CompileInternal(plan, root, false);
}

DeltaPlanFragment DeltaCompiler::CompileCopiedSubtree(unique_ptr<LogicalOperator> &plan, LogicalOperator *&root) {
	return CompileInternal(plan, root, true);
}

DeltaViewModel BuildRefreshDeltaViewModel(OptimizerExtensionInput &input, LogicalOperator *optimized_plan,
                                          const vector<string> &output_names) {
	// TODO: Store/cache the create-time DeltaViewModel and reuse it here. For now refresh planning rebuilds the IR
	// from the optimized view plan so the recursive rewriter can consume one coherent node model.
	auto facts = BuildCreateMVPlanFacts(optimized_plan, CurrentCatalog(input.context));
	DeltaViewModelInput model_input;
	model_input.facts = &facts;
	model_input.output_names = &output_names;
	model_input.has_unsupported_incremental_construct = facts.has_unsupported_set_operation || facts.has_pivot;
	return BuildDeltaViewModel(model_input);
}

void LogDeltaModelSummary(const DeltaViewModel &model) {
	OPENIVM_DEBUG_PRINT("[IR Rewrite] model type=%s nodes=%zu strategies=%zu group_cols=%zu window_cols=%zu "
	                    "aggregate_types=%zu full_outer_cols=%s\n",
	                    RefreshTypeName(model.type), model.nodes.size(), model.strategy_reasons.size(),
	                    model.group_columns.size(), model.window_partition_columns.size(), model.aggregate_types.size(),
	                    model.full_outer_join_cols.c_str());
}

DeltaPlanFragment DeltaOperatorInput::CompileChild(unique_ptr<LogicalOperator> &child,
                                                   LogicalOperator *&child_root) const {
	if (!compiler) {
		throw InternalException("DeltaOperatorInput::CompileChild requires a DeltaCompiler");
	}
	return copied_subtree ? compiler->CompileCopiedSubtree(child, child_root) : compiler->Compile(child, child_root);
}

DeltaPlanFragment DeltaOperatorInput::CompileCopiedSubtree(unique_ptr<LogicalOperator> &subtree,
                                                           LogicalOperator *&subtree_root) const {
	if (!compiler) {
		throw InternalException("DeltaOperatorInput::CompileCopiedSubtree requires a DeltaCompiler");
	}
	return compiler->CompileCopiedSubtree(subtree, subtree_root);
}

} // namespace duckdb
