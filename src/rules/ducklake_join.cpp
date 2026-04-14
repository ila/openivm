#include "rules/ducklake_join.hpp"
#include "rules/join.hpp"
#include "rules/rule.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "upsert/openivm_index_regen.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_scan.hpp"

namespace duckdb {

// ============================================================================
// PinToOldSnapshot: set a DuckLake scan to read the table at last_snapshot_id
// ============================================================================

/// Walk the subtree and pin any DuckLake scan with the given table_index to
/// the old snapshot. LPTS detects the historical snapshot and emits AT VERSION.
static void PinToOldSnapshot(LogicalOperator &op, idx_t table_index, idx_t old_snapshot_id) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.table_index == table_index && get.function.name == "ducklake_scan" && get.function.function_info) {
			auto &func_info = get.function.function_info->Cast<DuckLakeFunctionInfo>();
			func_info.snapshot.snapshot_id = old_snapshot_id;
			OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Pinned table_index=%lu to old snapshot %lu\n",
			                    (unsigned long)table_index, (unsigned long)old_snapshot_id);
		}
	}
	for (auto &child : op.children) {
		PinToOldSnapshot(*child, table_index, old_snapshot_id);
	}
}

// ============================================================================
// BuildDuckLakeJoinTerms: N-term telescoping delta product
// ============================================================================

vector<unique_ptr<LogicalOperator>> BuildDuckLakeJoinTerms(PlanWrapper &pw, ClientContext &context, Binder &binder,
                                                           const vector<JoinLeafInfo> &leaves, bool has_left_join) {
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	// Collect last_snapshot_id for each leaf upfront (one query per table).
	Connection con(*context.db);
	vector<int64_t> old_snapshots(N);
	for (size_t i = 0; i < N; i++) {
		auto *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
		D_ASSERT(get);
		string table_name = get->GetTable().get()->name;
		auto snap_result = con.Query("SELECT last_snapshot_id FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                             " WHERE view_name = '" + OpenIVMUtils::EscapeValue(pw.view) +
		                             "' AND table_name = '" + OpenIVMUtils::EscapeValue(table_name) + "'");
		if (snap_result->HasError() || snap_result->RowCount() == 0 || snap_result->GetValue(0, 0).IsNull()) {
			throw InternalException("No snapshot ID for DuckLake table '%s' in view '%s'", table_name, pw.view);
		}
		old_snapshots[i] = snap_result->GetValue(0, 0).GetValue<int64_t>();
	}

	// Check if empty-delta term skipping is enabled.
	bool skip_empty_enabled = true;
	Value skip_empty_val;
	if (context.TryGetCurrentSetting("ivm_skip_empty_deltas", skip_empty_val) && !skip_empty_val.IsNull()) {
		skip_empty_enabled = skip_empty_val.GetValue<bool>();
	}

	// Get current snapshot ID from the first leaf's DuckLakeFunctionInfo.
	// The plan was just bound for this refresh, so this reflects the current state.
	int64_t current_snapshot = -1;
	{
		auto *first_get = leaves[0].get ? leaves[0].get : FindGetInSubtree(leaves[0].node);
		D_ASSERT(first_get);
		if (first_get->function.name == "ducklake_scan" && first_get->function.function_info) {
			auto &func_info = first_get->function.function_info->Cast<DuckLakeFunctionInfo>();
			current_snapshot = static_cast<int64_t>(func_info.snapshot.snapshot_id);
		}
	}

	OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Building N-term telescoping delta terms (%zu leaves, current_snapshot=%ld)\n",
	                    N, (long)current_snapshot);

	for (size_t i = 0; i < N; i++) {
		// Skip term if this table has no changes since last refresh.
		// Safety: always generate at least one term to avoid empty UNION ALL.
		bool is_last_chance = (i == N - 1) && terms.empty();
		if (skip_empty_enabled && !is_last_chance && current_snapshot >= 0 && old_snapshots[i] == current_snapshot) {
			OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Skipping term %zu: no changes (snapshot %ld == current)\n", i,
			                    (long)old_snapshots[i]);
			continue;
		}

		auto term = pw.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);

		// Re-collect leaves from the copied plan (pointers change after Copy).
		vector<JoinLeafInfo> term_leaves;
		CollectJoinLeaves(term.get(), {}, term_leaves);
		D_ASSERT(term_leaves.size() == N);

		LogicalOperator *term_root = term.get();

		// For LEFT JOINs: demote to INNER when only right-side leaves have deltas.
		if (has_left_join) {
			if (!leaves[i].is_right_of_left_join) {
				// Delta is on left side — keep LEFT JOIN semantics
			} else {
				// Delta is only on the right side — demote to INNER
				DemoteLeftJoins(term.get());
			}
		}

		// Replace leaf[i] with its delta scan.
		ColumnBinding mul_binding;
		if (term_leaves[i].get) {
			// Simple GET leaf — replace directly.
			DeltaGetResult delta_result = CreateDeltaGetNode(context, binder, term_leaves[i].get, pw.view);
			mul_binding = delta_result.mul_binding;
			GetNodeAtPath(term, term_leaves[i].path) = std::move(delta_result.node);
		} else {
			// GET wrapped in projections/filters — rewrite the entire subtree.
			auto &subtree_ref = GetNodeAtPath(term, term_leaves[i].path);
			auto rewritten = IVMRewriteRule::RewritePlan(pw.input, subtree_ref, pw.view, term_root);
			mul_binding = rewritten.mul_binding;
			subtree_ref = std::move(rewritten.op);
		}
		UpdateParentProjectionMap(term, term_leaves[i]);

		// Telescoping: pin leaves j > i to old snapshot (AT VERSION).
		// Leaves j < i stay at current state (already the default).
		for (size_t j = i + 1; j < N; j++) {
			auto &leaf_node = GetNodeAtPath(term, term_leaves[j].path);
			auto *old_get = term_leaves[j].get ? term_leaves[j].get : FindGetInSubtree(leaf_node.get());
			if (old_get && old_get->function.name == "ducklake_scan" && old_get->function.function_info) {
				auto &func_info = old_get->function.function_info->Cast<DuckLakeFunctionInfo>();
				func_info.snapshot.snapshot_id = static_cast<idx_t>(old_snapshots[j]);
				OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Pinned leaf %zu (table_index=%lu) to old snapshot %ld\n", j,
				                    (unsigned long)old_get->table_index, (long)old_snapshots[j]);
			}
		}

		term->ResolveOperatorTypes();

		// Build projection: original columns + multiplicity from the single delta leaf.
		auto term_bindings = term->GetColumnBindings();
		auto term_types = term->types;
		vector<unique_ptr<Expression>> proj_exprs;

		// Emit all columns except the multiplicity binding from the delta.
		uint64_t mul_key =
		    (uint64_t)mul_binding.table_index ^ ((uint64_t)mul_binding.column_index * 0x9e3779b97f4a7c15ULL);
		for (idx_t c = 0; c < term_bindings.size(); c++) {
			uint64_t key = (uint64_t)term_bindings[c].table_index ^
			               ((uint64_t)term_bindings[c].column_index * 0x9e3779b97f4a7c15ULL);
			if (key != mul_key) {
				proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(term_types[c], term_bindings[c]));
			}
		}
		// Append multiplicity as the last column.
		proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(pw.mul_type, mul_binding));

		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));

		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Term %zu: delta on leaf %zu, %zu leaves pinned to old\n", i, i, N - i - 1);
	}

	return terms;
}

} // namespace duckdb
