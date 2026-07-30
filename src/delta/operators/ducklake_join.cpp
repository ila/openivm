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
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "storage/ducklake_scan.hpp"
#include "upsert/refresh_internal.hpp"

#include <algorithm>

namespace duckdb {

struct DuckLakeJoinColumnRef {
	size_t leaf_index;
	string column_name;
};

static bool IsJoinOperator(LogicalOperatorType type) {
	return type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT ||
	       type == LogicalOperatorType::LOGICAL_ANY_JOIN;
}

static bool CollectDuckLakeJoinLeaves(LogicalOperator *node, vector<size_t> &path, vector<JoinLeafInfo> &leaves,
                                      bool is_right_of_left, string &fallback_reason) {
	if (IsJoinOperator(node->type)) {
		auto *join = dynamic_cast<LogicalJoin *>(node);
		bool left_is_nullable = join && (join->join_type == JoinType::RIGHT || join->join_type == JoinType::OUTER);
		bool right_is_nullable = join && (join->join_type == JoinType::LEFT || join->join_type == JoinType::OUTER);
		for (size_t child_idx = 0; child_idx < node->children.size(); child_idx++) {
			path.push_back(child_idx);
			bool child_is_nullable = is_right_of_left || (child_idx == 0 ? left_is_nullable : right_is_nullable);
			if (!CollectDuckLakeJoinLeaves(node->children[child_idx].get(), path, leaves, child_is_nullable,
			                               fallback_reason)) {
				return false;
			}
			path.pop_back();
		}
		return true;
	}
	if (node->type == LogicalOperatorType::LOGICAL_PROJECTION || node->type == LogicalOperatorType::LOGICAL_FILTER) {
		if (node->children.size() != 1) {
			fallback_reason = node->GetName() + " does not have exactly one child";
			return false;
		}
		path.push_back(0);
		bool result =
		    CollectDuckLakeJoinLeaves(node->children[0].get(), path, leaves, is_right_of_left, fallback_reason);
		path.pop_back();
		return result;
	}
	if (node->type != LogicalOperatorType::LOGICAL_GET) {
		fallback_reason = "unsupported wrapper " + node->GetName();
		return false;
	}
	auto *get = dynamic_cast<LogicalGet *>(node);
	if (!get || get->function.name != "ducklake_scan" || !get->function.function_info) {
		fallback_reason = "non-DuckLake scan " + node->GetName();
		return false;
	}
	leaves.push_back({path, get, node, is_right_of_left});
	return true;
}

bool TryCollectDuckLakeJoinLeaves(LogicalOperator *node, vector<JoinLeafInfo> &leaves, string &fallback_reason) {
	leaves.clear();
	fallback_reason.clear();
	vector<size_t> path;
	if (!CollectDuckLakeJoinLeaves(node, path, leaves, false, fallback_reason)) {
		leaves.clear();
		return false;
	}
	if (leaves.empty()) {
		fallback_reason = "no DuckLake scans found";
		return false;
	}
	OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Flattened leaf count: %zu\n", leaves.size());
	return true;
}

static void AddDuckLakeLeafColumnRefs(LogicalOperator *root, const JoinLeafInfo &leaf, size_t leaf_index,
                                      unordered_map<uint64_t, DuckLakeJoinColumnRef> &column_refs) {
	vector<LogicalOperator *> ancestors;
	ancestors.reserve(leaf.path.size());
	LogicalOperator *node = root;
	for (auto child_idx : leaf.path) {
		if (child_idx >= node->children.size()) {
			throw InternalException("DuckLakeJoin: leaf path child %llu is out of bounds",
			                        static_cast<idx_t>(child_idx));
		}
		ancestors.push_back(node);
		node = node->children[child_idx].get();
	}

	auto *get = leaf.get;
	if (!get) {
		return;
	}
	unordered_map<uint64_t, string> visible_columns;
	auto bindings = get->GetColumnBindings();
	auto &column_ids = get->GetColumnIds();
	for (idx_t output_idx = 0; output_idx < bindings.size(); output_idx++) {
		idx_t column_id_idx = output_idx;
		if (!get->projection_ids.empty()) {
			if (output_idx >= get->projection_ids.size()) {
				continue;
			}
			column_id_idx = get->projection_ids[output_idx];
		}
		if (column_id_idx >= column_ids.size() || column_ids[column_id_idx].IsVirtualColumn()) {
			continue;
		}
		visible_columns[DeltaJoinBindingKey(bindings[output_idx])] = get->GetColumnName(column_ids[column_id_idx]);
	}

	auto record_visible = [&]() {
		for (auto &entry : visible_columns) {
			column_refs[entry.first] = {leaf_index, entry.second};
		}
	};
	record_visible();

	for (size_t depth = leaf.path.size(); depth-- > 0;) {
		auto *parent = ancestors[depth];
		unordered_map<uint64_t, string> parent_columns;
		if (parent->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = parent->Cast<LogicalProjection>();
			auto parent_bindings = parent->GetColumnBindings();
			idx_t count = std::min<idx_t>(projection.expressions.size(), parent_bindings.size());
			for (idx_t expr_idx = 0; expr_idx < count; expr_idx++) {
				ColumnBinding child_binding;
				if (!TryGetDeltaJoinColumnRef(*projection.expressions[expr_idx], child_binding)) {
					continue;
				}
				auto child_entry = visible_columns.find(DeltaJoinBindingKey(child_binding));
				if (child_entry != visible_columns.end()) {
					parent_columns[DeltaJoinBindingKey(parent_bindings[expr_idx])] = child_entry->second;
				}
			}
		} else {
			for (auto &binding : parent->GetColumnBindings()) {
				auto child_entry = visible_columns.find(DeltaJoinBindingKey(binding));
				if (child_entry != visible_columns.end()) {
					parent_columns[DeltaJoinBindingKey(binding)] = child_entry->second;
				}
			}
		}
		visible_columns = std::move(parent_columns);
		record_visible();
	}
}

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

static bool PathStartsWith(const vector<size_t> &path, const vector<size_t> &prefix) {
	return path.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), path.begin());
}

static void DemoteOuterJoinsForLeaf(LogicalOperator *node, const vector<size_t> &leaf_path, vector<size_t> &path) {
	if (auto *join = dynamic_cast<LogicalJoin *>(node)) {
		bool left_has_delta = false;
		bool right_has_delta = false;
		path.push_back(0);
		left_has_delta = PathStartsWith(leaf_path, path);
		path.pop_back();
		path.push_back(1);
		right_has_delta = PathStartsWith(leaf_path, path);
		path.pop_back();

		if ((join->join_type == JoinType::LEFT && right_has_delta) ||
		    (join->join_type == JoinType::RIGHT && left_has_delta) ||
		    (join->join_type == JoinType::OUTER && (left_has_delta || right_has_delta))) {
			join->join_type = JoinType::INNER;
		}
	}
	for (size_t child_idx = 0; child_idx < node->children.size(); child_idx++) {
		path.push_back(child_idx);
		DemoteOuterJoinsForLeaf(node->children[child_idx].get(), leaf_path, path);
		path.pop_back();
	}
}

static void DemoteOuterJoinsForLeaf(LogicalOperator *node, const vector<size_t> &leaf_path) {
	vector<size_t> path;
	DemoteOuterJoinsForLeaf(node, leaf_path, path);
}

static idx_t FindBindingPosition(LogicalOperator &op, const ColumnBinding &binding, const char *context_label) {
	auto bindings = op.GetColumnBindings();
	for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) {
		if (bindings[binding_idx] == binding) {
			return binding_idx;
		}
	}
	throw InternalException("%s: multiplicity binding %s is not exposed by %s", context_label,
	                        binding.ToString().c_str(), op.GetName().c_str());
}

static ColumnBinding PropagateMultiplicityThroughPath(unique_ptr<LogicalOperator> &term,
                                                      const vector<size_t> &leaf_path, ColumnBinding mul_binding) {
	vector<LogicalOperator *> ancestors;
	ancestors.reserve(leaf_path.size());
	LogicalOperator *node = term.get();
	for (size_t depth = 0; depth < leaf_path.size(); depth++) {
		if (leaf_path[depth] >= node->children.size()) {
			throw InternalException("DuckLakeJoin: leaf path child %llu out of bounds at depth %llu",
			                        static_cast<idx_t>(leaf_path[depth]), static_cast<idx_t>(depth));
		}
		ancestors.push_back(node);
		node = node->children[leaf_path[depth]].get();
	}

	for (size_t depth = leaf_path.size(); depth-- > 0;) {
		auto *parent = ancestors[depth];
		size_t child_side = leaf_path[depth];
		auto &child = *parent->children[child_side];
		idx_t mul_idx = FindBindingPosition(child, mul_binding, "DuckLakeJoin");

		if (parent->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = parent->Cast<LogicalProjection>();
			projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, mul_binding));
			mul_binding = ColumnBinding(projection.table_index, projection.expressions.size() - 1);
			continue;
		}
		if (parent->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = parent->Cast<LogicalFilter>();
			if (!filter.projection_map.empty() && std::find(filter.projection_map.begin(), filter.projection_map.end(),
			                                                mul_idx) == filter.projection_map.end()) {
				filter.projection_map.push_back(mul_idx);
			}
			continue;
		}
		if (auto *join = dynamic_cast<LogicalJoin *>(parent)) {
			auto &projection_map = child_side == 0 ? join->left_projection_map : join->right_projection_map;
			if (!projection_map.empty() &&
			    std::find(projection_map.begin(), projection_map.end(), mul_idx) == projection_map.end()) {
				projection_map.push_back(mul_idx);
			}
			continue;
		}
		throw InternalException("DuckLakeJoin: unsupported ancestor %s in flattened path", parent->GetName());
	}
	return mul_binding;
}

// ============================================================================
// BuildDuckLakeJoinTerms: N-term telescoping delta product
// ============================================================================

vector<unique_ptr<LogicalOperator>> BuildDuckLakeJoinTerms(DeltaOperatorInput input, ClientContext &context,
                                                           Binder &binder, const vector<JoinLeafInfo> &leaves,
                                                           bool has_left_join, bool flattened_leaves) {
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	// Collect last_snapshot_id for all leaves upfront in one metadata query.
	Connection con(*context.db);
	vector<int64_t> old_snapshots(N);
	vector<int64_t> current_snapshots(N, -1);
	vector<string> table_catalogs(N);
	vector<string> table_schemas(N);
	vector<string> table_names(N);
	unordered_map<string, int64_t> stored_snapshots;
	auto snapshot_result = con.Query("SELECT table_name, last_snapshot_id FROM " + string(openivm::DELTA_TABLES_TABLE) +
	                                 " WHERE view_name = '" + SqlUtils::EscapeValue(input.context.view) + "'");
	if (snapshot_result->HasError()) {
		throw Exception(ExceptionType::CATALOG, "IVM: could not read DuckLake snapshot metadata for view '" +
		                                            input.context.view + "': " + snapshot_result->GetError());
	}
	for (idx_t row = 0; row < snapshot_result->RowCount(); row++) {
		if (snapshot_result->GetValue(0, row).IsNull() || snapshot_result->GetValue(1, row).IsNull()) {
			continue;
		}
		stored_snapshots[StringUtil::Lower(snapshot_result->GetValue(0, row).ToString())] =
		    snapshot_result->GetValue(1, row).GetValue<int64_t>();
	}
	for (size_t i = 0; i < N; i++) {
		auto *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
		D_ASSERT(get);
		auto table_ref = get->GetTable();
		string table_name = table_ref.get()->name;
		table_catalogs[i] = table_ref->ParentCatalog().GetName();
		table_schemas[i] = table_ref->schema.name;
		table_names[i] = table_name;
		auto stored_snapshot = stored_snapshots.find(StringUtil::Lower(table_name));
		if (stored_snapshot == stored_snapshots.end()) {
			throw Exception(ExceptionType::CATALOG, "IVM: no snapshot ID recorded for DuckLake table '" + table_name +
			                                            "' in view '" + input.context.view + "'");
		}
		old_snapshots[i] = stored_snapshot->second;
		if (get->function.name == "ducklake_scan" && get->function.function_info) {
			auto &func_info = get->function.function_info->Cast<DuckLakeFunctionInfo>();
			current_snapshots[i] = static_cast<int64_t>(func_info.snapshot.snapshot_id);
		}
	}

	// Check if empty-delta term skipping is enabled.
	bool skip_empty_enabled = SqlUtils::GetBoolSetting(context, "openivm_skip_empty_deltas", true);

	// DuckLake snapshot ids are catalog-wide, not global across attached catalogs. A table can have
	// last_snapshot_id != current_snapshot because another table changed. Probe table-level changes before
	// building the term so unchanged tables do not force a full plan copy/rewrite.
	vector<bool> empty_table_delta(N, false);
	vector<bool> activity_known(N, false);
	if (skip_empty_enabled) {
		auto compile_facts = openivm::CompileFactsContextSlot::Get(context);
		size_t reused_activity_count = 0;
		for (size_t i = 0; i < N; i++) {
			for (auto &entry : compile_facts.delta_shape) {
				if (!StringUtil::CIEquals(SqlUtils::LastIdentifierPart(entry.first), table_names[i])) {
					continue;
				}
				if (StringUtil::CIEquals(entry.second, "UNCHANGED")) {
					empty_table_delta[i] = true;
					activity_known[i] = true;
				} else if (StringUtil::CIEquals(entry.second, "INSERT_ONLY") ||
				           StringUtil::CIEquals(entry.second, "MIXED")) {
					activity_known[i] = true;
				}
				reused_activity_count += activity_known[i] ? 1 : 0;
				break;
			}
		}
		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Reused source activity for %zu/%zu leaves\n", reused_activity_count, N);
		RefreshMetadata metadata(con);
		for (size_t i = 0; i < N; i++) {
			if (activity_known[i]) {
				continue;
			}
			if (current_snapshots[i] < 0) {
				continue;
			}
			if (old_snapshots[i] == current_snapshots[i]) {
				empty_table_delta[i] = true;
				continue;
			}
			auto metadata_activity =
			    ProbeDuckLakeSnapshotActivity(metadata, con, input.context.view, table_names[i], table_catalogs[i],
			                                  table_schemas[i], old_snapshots[i], current_snapshots[i]);
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
			auto has_changes = con.Query(has_changes_sql);
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
	}

	vector<vector<DeltaJoinKeyProbe>> key_probes(N);
	if (skip_empty_enabled && !has_left_join) {
		unordered_map<uint64_t, DuckLakeJoinColumnRef> column_refs;
		for (size_t i = 0; i < N; i++) {
			auto *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
			if (!get) {
				continue;
			}
			AddDuckLakeLeafColumnRefs(input.plan.get(), leaves[i], i, column_refs);
		}
		CollectDeltaJoinKeyProbes(input.plan.get(), column_refs, key_probes);
	}

	OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Building N-term telescoping delta terms (%zu leaves, flattened=%s)\n", N,
	                    flattened_leaves ? "true" : "false");

	for (size_t i = 0; i < N; i++) {
		// Skip term if this table has no changes since last refresh.
		if (skip_empty_enabled && empty_table_delta[i]) {
			OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Skipping term %zu: no changes in %s.%s.%s (%ld -> %ld)\n", i,
			                    table_catalogs[i].c_str(), table_schemas[i].c_str(), table_names[i].c_str(),
			                    (long)old_snapshots[i], (long)current_snapshots[i]);
			continue;
		}
		bool key_domain_empty = false;
		if (skip_empty_enabled && current_snapshots[i] >= 0 && !key_probes[i].empty()) {
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
		}
		if (key_domain_empty) {
			continue;
		}

		auto term = input.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);

		// Re-collect leaves from the copied plan (pointers change after Copy).
		vector<JoinLeafInfo> term_leaves;
		if (flattened_leaves) {
			string fallback_reason;
			if (!TryCollectDuckLakeJoinLeaves(term.get(), term_leaves, fallback_reason)) {
				throw InternalException("DuckLakeJoin: copied plan no longer supports flattening: %s",
				                        fallback_reason.c_str());
			}
		} else {
			CollectJoinLeaves(term.get(), {}, term_leaves);
		}
		D_ASSERT(term_leaves.size() == N);

		LogicalOperator *term_root = term.get();

		// Demote only the outer joins whose NULL-supplying subtree contains this
		// term's delta. Preserved joins elsewhere in a left-deep star must remain
		// outer joins so unmatched rows continue to flow to later dimensions.
		if (has_left_join) {
			if (flattened_leaves) {
				DemoteOuterJoinsForLeaf(term.get(), term_leaves[i].path);
			} else if (leaves[i].is_right_of_left_join) {
				DemoteLeftJoins(term.get());
			}
		}

		// Replace leaf[i] with its delta scan.
		ColumnBinding mul_binding;
		if (flattened_leaves || term_leaves[i].get) {
			// Simple GET leaf — replace directly.
			DeltaGetResult delta_result = CreateDeltaGetNode(context, binder, term_leaves[i].get, input.context.view);
			mul_binding = delta_result.mul_binding;
			GetNodeAtPath(term, term_leaves[i].path) = std::move(delta_result.node);
			if (flattened_leaves) {
				mul_binding = PropagateMultiplicityThroughPath(term, term_leaves[i].path, mul_binding);
			}
		} else {
			// GET wrapped in projections/filters — rewrite the entire subtree.
			auto &subtree_ref = GetNodeAtPath(term, term_leaves[i].path);
			auto rewritten = input.CompileCopiedSubtree(subtree_ref, term_root);
			mul_binding = rewritten.mul_binding;
			subtree_ref = std::move(rewritten.op);
		}
		if (!flattened_leaves) {
			UpdateParentProjectionMap(term, term_leaves[i], mul_binding);
		}

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
		proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(input.mul_type, mul_binding));

		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));

		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Term %zu: delta on leaf %zu, %zu leaves pinned to old\n", i, i, N - i - 1);
	}

	OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Active N-term count: %zu/%zu\n", terms.size(), N);
	return terms;
}

} // namespace duckdb
