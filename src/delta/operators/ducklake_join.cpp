#include "delta/operators/ducklake_join.hpp"
#include "delta/operators/join.hpp"
#include "delta/operators/join_key_probe.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "upsert/refresh_index_regen.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_scan.hpp"
#include "upsert/refresh_internal.hpp"

namespace duckdb {

namespace {

static std::chrono::steady_clock::time_point ProfileNow() {
	return std::chrono::steady_clock::now();
}

static void AddCompileProfileStep(ClientContext &context, const string &step_name,
                                  std::chrono::steady_clock::time_point start, const string &detail = string()) {
	auto *profile = RefreshCompileProfileContextSlot::Get(context);
	if (profile) {
		profile->AddStep(step_name, start, detail);
	}
}

static string DuckLakeJoinProfileDetail(size_t index, const string &catalog, const string &schema, const string &table,
                                        int64_t old_snapshot, int64_t current_snapshot) {
	return "term=" + to_string(index) + "; table=" + catalog + "." + schema + "." + table +
	       "; old_snapshot=" + to_string(old_snapshot) + "; current_snapshot=" + to_string(current_snapshot);
}

} // namespace

struct DuckLakeJoinColumnRef {
	size_t leaf_index;
	string column_name;
};

static string DuckLakeQualifiedTable(const string &catalog, const string &schema, const string &table_name,
                                     int64_t snapshot_id) {
	string result = SqlUtils::QuoteIdentifier(catalog) + "." + SqlUtils::QuoteIdentifier(schema) + "." +
	                SqlUtils::QuoteIdentifier(table_name);
	if (snapshot_id >= 0) {
		result += " AT (VERSION => " + to_string(snapshot_id) + ")";
	}
	return result;
}

static bool DuckLakeDeltaKeyHasMatch(Connection &con, const string &catalog, const string &schema,
                                     const string &table_name, const string &delta_column, int64_t old_snapshot,
                                     int64_t current_snapshot, const string &other_catalog, const string &other_schema,
                                     const string &other_table, const string &other_column, int64_t other_snapshot) {
	string delta_col = SqlUtils::QuoteIdentifier(delta_column);
	string other_col = SqlUtils::QuoteIdentifier(other_column);
	string other_relation = DuckLakeQualifiedTable(other_catalog, other_schema, other_table, other_snapshot);
	string old_snap = to_string(old_snapshot);
	string cur_snap = to_string(current_snapshot);
	string sql = "SELECT EXISTS(SELECT 1 FROM ("
	             "SELECT " +
	             delta_col + " AS openivm_key FROM ducklake_table_insertions('" + SqlUtils::EscapeValue(catalog) +
	             "', '" + SqlUtils::EscapeValue(schema) + "', '" + SqlUtils::EscapeValue(table_name) + "', " +
	             old_snap + ", " + cur_snap +
	             ") "
	             "UNION ALL "
	             "SELECT " +
	             delta_col + " AS openivm_key FROM ducklake_table_deletions('" + SqlUtils::EscapeValue(catalog) +
	             "', '" + SqlUtils::EscapeValue(schema) + "', '" + SqlUtils::EscapeValue(table_name) + "', " +
	             old_snap + ", " + cur_snap +
	             ")) openivm_delta_keys "
	             "JOIN (SELECT * FROM " +
	             other_relation + ") openivm_other ON openivm_delta_keys.openivm_key = openivm_other." + other_col +
	             " LIMIT 1)";
	auto result = con.Query(sql);
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Could not probe key-domain intersection: %s\n",
		                    result->HasError() ? result->GetError().c_str() : "no result");
		return true;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

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

vector<unique_ptr<LogicalOperator>> BuildDuckLakeJoinTerms(DeltaOperatorInput input, ClientContext &context,
                                                           Binder &binder, const vector<JoinLeafInfo> &leaves,
                                                           bool has_left_join) {
	auto total_start = ProfileNow();
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	// Collect last_snapshot_id for each leaf upfront (one query per table).
	auto snapshot_metadata_start = ProfileNow();
	Connection con(*context.db);
	vector<int64_t> old_snapshots(N);
	vector<int64_t> current_snapshots(N, -1);
	vector<string> table_catalogs(N);
	vector<string> table_schemas(N);
	vector<string> table_names(N);
	for (size_t i = 0; i < N; i++) {
		auto *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
		D_ASSERT(get);
		auto table_ref = get->GetTable();
		string table_name = table_ref.get()->name;
		table_catalogs[i] = table_ref->ParentCatalog().GetName();
		table_schemas[i] = table_ref->schema.name;
		table_names[i] = table_name;
		auto snap_result = con.Query("SELECT last_snapshot_id FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                             " WHERE view_name = '" + SqlUtils::EscapeValue(input.context.view) +
		                             "' AND table_name = '" + SqlUtils::EscapeValue(table_name) + "'");
		if (snap_result->HasError() || snap_result->RowCount() == 0 || snap_result->GetValue(0, 0).IsNull()) {
			throw Exception(ExceptionType::CATALOG, "IVM: no snapshot ID recorded for DuckLake table '" + table_name +
			                                            "' in view '" + input.context.view + "'");
		}
		old_snapshots[i] = snap_result->GetValue(0, 0).GetValue<int64_t>();
		if (get->function.name == "ducklake_scan" && get->function.function_info) {
			auto &func_info = get->function.function_info->Cast<DuckLakeFunctionInfo>();
			current_snapshots[i] = static_cast<int64_t>(func_info.snapshot.snapshot_id);
		}
	}
	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.snapshot_metadata",
	                      snapshot_metadata_start, "leaves=" + to_string(N));

	// Check if empty-delta term skipping is enabled.
	bool skip_empty_enabled = SqlUtils::GetBoolSetting(context, "openivm_skip_empty_deltas", true);

	// DuckLake snapshot ids are catalog-wide, not global across attached catalogs. A table can have
	// last_snapshot_id != current_snapshot because another table changed. Probe table-level changes before
	// building the term so unchanged tables do not force a full plan copy/rewrite.
	vector<bool> empty_table_delta(N, false);
	if (skip_empty_enabled) {
		auto activity_total_start = ProfileNow();
		RefreshMetadata metadata(con);
		for (size_t i = 0; i < N; i++) {
			auto detail = DuckLakeJoinProfileDetail(i, table_catalogs[i], table_schemas[i], table_names[i],
			                                        old_snapshots[i], current_snapshots[i]);
			if (current_snapshots[i] < 0) {
				AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.activity_probe",
				                      ProfileNow(), detail + "; skipped=no_current_snapshot");
				continue;
			}
			if (old_snapshots[i] == current_snapshots[i]) {
				empty_table_delta[i] = true;
				AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.activity_probe",
				                      ProfileNow(), detail + "; skipped=same_snapshot");
				continue;
			}
			auto activity_probe_start = ProfileNow();
			auto metadata_activity =
			    ProbeDuckLakeSnapshotActivity(metadata, con, input.context.view, table_names[i], table_catalogs[i],
			                                  table_schemas[i], old_snapshots[i], current_snapshots[i]);
			AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.activity_probe",
			                      activity_probe_start,
			                      detail + "; ok=" + string(metadata_activity.ok ? "true" : "false") +
			                          "; has_changes=" + string(metadata_activity.has_changes ? "true" : "false") +
			                          "; has_deletes=" + string(metadata_activity.has_deletes ? "true" : "false"));
			if (metadata_activity.ok) {
				if (!metadata_activity.has_changes) {
					empty_table_delta[i] = true;
				}
				continue;
			}
			string has_changes_sql =
			    "SELECT EXISTS(SELECT 1 FROM ("
			    "(SELECT 1 FROM ducklake_table_insertions('" +
			    SqlUtils::EscapeValue(table_catalogs[i]) + "', '" + SqlUtils::EscapeValue(table_schemas[i]) + "', '" +
			    SqlUtils::EscapeValue(table_names[i]) + "', " + to_string(old_snapshots[i]) + ", " +
			    to_string(current_snapshots[i]) +
			    ") LIMIT 1) "
			    "UNION ALL "
			    "(SELECT 1 FROM ducklake_table_deletions('" +
			    SqlUtils::EscapeValue(table_catalogs[i]) + "', '" + SqlUtils::EscapeValue(table_schemas[i]) + "', '" +
			    SqlUtils::EscapeValue(table_names[i]) + "', " + to_string(old_snapshots[i]) + ", " +
			    to_string(current_snapshots[i]) + ") LIMIT 1)) openivm_delta_probe LIMIT 1)";
			auto fallback_probe_start = ProfileNow();
			auto has_changes = con.Query(has_changes_sql);
			AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.activity_fallback_probe",
			                      fallback_probe_start,
			                      detail + "; error=" + string(has_changes->HasError() ? "true" : "false"));
			if (has_changes->HasError()) {
				OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Could not probe changes for %s.%s.%s: %s\n",
				                    table_catalogs[i].c_str(), table_schemas[i].c_str(), table_names[i].c_str(),
				                    has_changes->GetError().c_str());
				continue;
			}
			if (has_changes->RowCount() > 0 && !has_changes->GetValue(0, 0).IsNull() &&
			    !has_changes->GetValue(0, 0).GetValue<bool>()) {
				empty_table_delta[i] = true;
			}
		}
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.activity_probe_total",
		                      activity_total_start, "leaves=" + to_string(N));
	}

	bool join_key_probe_allowed = !has_left_join && !input.context.assumptions.suppress_join_key_domain_probe;
	vector<vector<DeltaJoinKeyProbe>> key_probes(N);
	if (skip_empty_enabled && join_key_probe_allowed) {
		auto key_probe_prepare_start = ProfileNow();
		unordered_map<uint64_t, DuckLakeJoinColumnRef> column_refs;
		for (size_t i = 0; i < N; i++) {
			auto *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
			if (!get) {
				continue;
			}
			auto bindings = get->GetColumnBindings();
			auto &column_ids = get->GetColumnIds();
			idx_t count = std::min<idx_t>(bindings.size(), column_ids.size());
			for (idx_t col_idx = 0; col_idx < count; col_idx++) {
				if (column_ids[col_idx].IsVirtualColumn()) {
					continue;
				}
				column_refs[DeltaJoinBindingKey(bindings[col_idx])] = {i, get->GetColumnName(column_ids[col_idx])};
			}
			auto leaf_bindings = leaves[i].node->GetColumnBindings();
			idx_t leaf_count = std::min<idx_t>(leaf_bindings.size(), count);
			for (idx_t col_idx = 0; col_idx < leaf_count; col_idx++) {
				if (column_ids[col_idx].IsVirtualColumn()) {
					continue;
				}
				column_refs[DeltaJoinBindingKey(leaf_bindings[col_idx])] = {i, get->GetColumnName(column_ids[col_idx])};
			}
		}
		CollectDeltaJoinKeyProbes(input.plan.get(), column_refs, key_probes);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.key_probe_prepare",
		                      key_probe_prepare_start, "column_refs=" + to_string(column_refs.size()));
	}

	OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Building N-term telescoping delta terms (%zu leaves)\n", N);

	for (size_t i = 0; i < N; i++) {
		auto term_total_start = ProfileNow();
		auto term_detail = DuckLakeJoinProfileDetail(i, table_catalogs[i], table_schemas[i], table_names[i],
		                                             old_snapshots[i], current_snapshots[i]);
		// Skip term if this table has no changes since last refresh.
		if (skip_empty_enabled && empty_table_delta[i]) {
			OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Skipping term %zu: no changes in %s.%s.%s (%ld -> %ld)\n", i,
			                    table_catalogs[i].c_str(), table_schemas[i].c_str(), table_names[i].c_str(),
			                    (long)old_snapshots[i], (long)current_snapshots[i]);
			AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.total",
			                      term_total_start, term_detail + "; emitted=false; skipped=empty_table_delta");
			continue;
		}
		bool key_domain_empty = false;
		if (skip_empty_enabled && join_key_probe_allowed && current_snapshots[i] >= 0 && !key_probes[i].empty()) {
			auto key_domain_start = ProfileNow();
			for (auto &probe : key_probes[i]) {
				size_t other = probe.other_leaf;
				int64_t other_snapshot = other > i ? old_snapshots[other] : -1;
				if (!DuckLakeDeltaKeyHasMatch(con, table_catalogs[i], table_schemas[i], table_names[i],
				                              probe.delta_column, old_snapshots[i], current_snapshots[i],
				                              table_catalogs[other], table_schemas[other], table_names[other],
				                              probe.other_column, other_snapshot)) {
					key_domain_empty = true;
					OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Skipping term %zu: delta key %s.%s has no match in %s.%s\n", i,
					                    table_names[i].c_str(), probe.delta_column.c_str(), table_names[other].c_str(),
					                    probe.other_column.c_str());
					break;
				}
			}
			AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.key_domain_probe",
			                      key_domain_start,
			                      term_detail + "; empty=" + string(key_domain_empty ? "true" : "false"));
		}
		if (key_domain_empty) {
			AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.total",
			                      term_total_start, term_detail + "; emitted=false; skipped=key_domain_empty");
			continue;
		}

		auto copy_start = ProfileNow();
		auto term = input.plan->Copy(context);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.copy", copy_start,
		                      term_detail);
		auto renumber_start = ProfileNow();
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.renumber_rebind",
		                      renumber_start, term_detail);

		// Re-collect leaves from the copied plan (pointers change after Copy).
		auto collect_leaves_start = ProfileNow();
		vector<JoinLeafInfo> term_leaves;
		CollectJoinLeaves(term.get(), {}, term_leaves);
		D_ASSERT(term_leaves.size() == N);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.collect_leaves",
		                      collect_leaves_start, term_detail + "; leaves=" + to_string(term_leaves.size()));

		LogicalOperator *term_root = term.get();

		// For LEFT JOINs: demote to INNER when only right-side leaves have deltas.
		if (has_left_join) {
			auto demote_start = ProfileNow();
			if (!leaves[i].is_right_of_left_join) {
				// Delta is on left side — keep LEFT JOIN semantics
			} else {
				// Delta is only on the right side — demote to INNER
				DemoteLeftJoins(term.get());
			}
			AddCompileProfileStep(
			    context, "generate_refresh_sql.compute_delta.ducklake_join.term.demote_left", demote_start,
			    term_detail + "; right_of_left=" + string(leaves[i].is_right_of_left_join ? "true" : "false"));
		}

		// Replace leaf[i] with its delta scan.
		ColumnBinding mul_binding;
		if (term_leaves[i].get) {
			// Simple GET leaf — replace directly.
			auto delta_leaf_start = ProfileNow();
			DeltaGetResult delta_result = CreateDeltaGetNode(context, binder, term_leaves[i].get, input.context.view);
			mul_binding = delta_result.mul_binding;
			GetNodeAtPath(term, term_leaves[i].path) = std::move(delta_result.node);
			AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.delta_leaf",
			                      delta_leaf_start, term_detail + "; wrapped=false");
		} else {
			// GET wrapped in projections/filters — rewrite the entire subtree.
			auto delta_leaf_start = ProfileNow();
			auto &subtree_ref = GetNodeAtPath(term, term_leaves[i].path);
			auto rewritten = input.CompileCopiedSubtree(subtree_ref, term_root, has_left_join);
			mul_binding = rewritten.mul_binding;
			subtree_ref = std::move(rewritten.op);
			AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.delta_leaf",
			                      delta_leaf_start, term_detail + "; wrapped=true");
		}
		auto projection_map_start = ProfileNow();
		UpdateParentProjectionMap(term, term_leaves[i].path, mul_binding, /*include_delim_parents=*/false);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.update_projection_map",
		                      projection_map_start, term_detail);

		// Telescoping: pin leaves j > i to old snapshot (AT VERSION).
		// Leaves j < i stay at current state (already the default).
		auto pin_snapshots_start = ProfileNow();
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
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.pin_snapshots",
		                      pin_snapshots_start, term_detail + "; pinned=" + to_string(N - i - 1));

		auto resolve_types_start = ProfileNow();
		term->ResolveOperatorTypes();
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.resolve_types",
		                      resolve_types_start, term_detail);

		// Build projection: original columns + multiplicity from the single delta leaf.
		auto projection_start = ProfileNow();
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
		proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(input.mul_type, mul_binding));

		auto output_cols = proj_exprs.size();
		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.projection",
		                      projection_start, term_detail + "; output_cols=" + to_string(output_cols));

		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Term %zu: delta on leaf %zu, %zu leaves pinned to old\n", i, i, N - i - 1);
		AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.term.total", term_total_start,
		                      term_detail + "; emitted=true");
	}

	AddCompileProfileStep(context, "generate_refresh_sql.compute_delta.ducklake_join.total", total_start,
	                      "leaves=" + to_string(N) + "; terms=" + to_string(terms.size()) + "; has_left_join=" +
	                          string(has_left_join ? "true" : "false") + "; key_probe_suppressed=" +
	                          string(input.context.assumptions.suppress_join_key_domain_probe ? "true" : "false"));
	return terms;
}

} // namespace duckdb
