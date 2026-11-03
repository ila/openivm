#include "delta/operators/ducklake_join.hpp"
#include "delta/operators/join.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
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

static string DuckLakeQualifiedTable(const string &catalog, const string &schema, const string &table_name,
                                     int64_t snapshot_id) {
	string result = SqlUtils::QuoteIdentifier(catalog) + "." + SqlUtils::QuoteIdentifier(schema) + "." +
	                SqlUtils::QuoteIdentifier(table_name);
	// -1 is the only sentinel; snapshot zero, if supplied by DuckLake, is valid.
	if (snapshot_id >= 0) { // mull-ignore: cxx_ge_to_gt,cxx_ge_to_lt
		result += " AT (VERSION => " + to_string(snapshot_id) + ")";
	}
	return result;
}

// Rows inserted into or deleted from a DuckLake table between two snapshots, projected to select_list.
static string DuckLakeChangesSQL(const string &select_list, const string &catalog, const string &schema,
                                 const string &table_name, int64_t old_snapshot, int64_t current_snapshot) {
	auto rows = [&](const char *function_name) {
		return "SELECT " + select_list + " FROM " +
		       SqlUtils::DuckLakeTableFunction(function_name, catalog, schema, table_name, old_snapshot,
		                                       current_snapshot);
	};
	return rows("ducklake_table_insertions") + " UNION ALL " + rows("ducklake_table_deletions");
}

static idx_t FindBindingPosition(LogicalOperator &op, const ColumnBinding &binding, const char *context_label) {
	auto bindings = op.GetColumnBindings();
	// The <= mutant indexes one past bindings.
	for (idx_t binding_idx = 0; binding_idx < bindings.size(); binding_idx++) { // mull-ignore: cxx_lt_to_le
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
		if (leaf_path[depth] >= node->children.size()) { // mull-ignore: cxx_ge_to_gt
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
		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Propagating multiplicity through %s child=%zu width=%zu mul=%zu\n",
		                    parent->GetName().c_str(), child_side, child.GetColumnBindings().size(),
		                    static_cast<size_t>(mul_idx));
		auto shift_parent_projection = [&](idx_t insertion_idx) {
			ShiftAncestorProjectionMap(ancestors, leaf_path, depth, insertion_idx, 1);
		};

		if (parent->type == LogicalOperatorType::LOGICAL_PROJECTION) {
			auto &projection = parent->Cast<LogicalProjection>();
			projection.expressions.push_back(make_uniq<BoundColumnRefExpression>(LogicalType::INTEGER, mul_binding));
			mul_binding =
			    ColumnBinding(TableIndex(projection.table_index), ProjectionIndex(projection.expressions.size() - 1));
			continue;
		}
		if (parent->type == LogicalOperatorType::LOGICAL_FILTER) {
			auto &filter = parent->Cast<LogicalFilter>();
			if (filter.projection_map.empty()) {
				shift_parent_projection(mul_idx);
			} else {
				auto exposed = filter.GetColumnBindings();
				if (std::find(exposed.begin(), exposed.end(), mul_binding) == exposed.end()) {
					filter.projection_map.push_back(ProjectionIndex(mul_idx));
				}
			}
			filter.ResolveOperatorTypes();
			continue;
		}
		if (parent->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
			// A new left column shifts the right side in any ancestor's positional map.
			idx_t insertion_idx = mul_idx;
			if (child_side == 1) {
				insertion_idx += parent->children[0]->GetColumnBindings().size();
			}
			shift_parent_projection(insertion_idx);
			parent->ResolveOperatorTypes();
			continue;
		}
		if (auto *join = dynamic_cast<LogicalJoin *>(parent)) {
			auto &projection_map = child_side == 0 ? join->left_projection_map : join->right_projection_map;
			OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Join projection map before multiplicity:");
			for (auto projection : projection_map) {
				OPENIVM_DEBUG_PRINT(" %zu", static_cast<size_t>(projection.GetIndex()));
			}
			OPENIVM_DEBUG_PRINT("\n");
			if (projection_map.empty()) {
				idx_t insertion_idx = mul_idx;
				if (child_side == 1) {
					insertion_idx += join->children[0]->GetColumnBindings().size();
				}
				shift_parent_projection(insertion_idx);
			} else {
				// Numeric map positions can alias a different column after a deeper join grows.
				auto exposed = join->GetColumnBindings();
				if (std::find(exposed.begin(), exposed.end(), mul_binding) == exposed.end()) {
					idx_t insertion_idx = projection_map.size();
					if (child_side == 1) {
						insertion_idx += join->left_projection_map.empty()
						                     ? join->children[0]->GetColumnBindings().size()
						                     : join->left_projection_map.size();
					}
					projection_map.push_back(ProjectionIndex(mul_idx));
					shift_parent_projection(insertion_idx);
				}
			}
			join->ResolveOperatorTypes();
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
                                                           bool has_left_join, bool flattened_leaves,
                                                           const vector<vector<DeltaJoinKeyProbe>> &key_probes,
                                                           DeltaSourceCache &source_cache) {
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	// Collect last_snapshot_id for all leaves upfront in one metadata query.
	auto &con = input.context.metadata_con;
	vector<int64_t> old_snapshots(N);
	vector<int64_t> current_snapshots(N, -1);
	vector<string> table_catalogs(N);
	vector<string> table_schemas(N);
	vector<string> table_names(N);
	unordered_map<string, int64_t> stored_snapshots;
	auto snapshot_result = con.Query("SELECT table_name, last_snapshot_id, source_catalog, source_schema FROM " +
	                                 string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                                 SqlUtils::EscapeValue(input.context.view) + "'");
	if (snapshot_result->HasError()) {
		throw Exception(ExceptionType::CATALOG, "IVM: could not read DuckLake snapshot metadata for view '" +
		                                            input.context.view + "': " + snapshot_result->GetError());
	}
	for (idx_t row = 0; row < snapshot_result->RowCount(); row++) {
		if (snapshot_result->Collection().GetValue(0, row).IsNull() ||
		    snapshot_result->Collection().GetValue(1, row).IsNull()) {
			continue;
		}
		auto catalog = snapshot_result->Collection().GetValue(2, row).ToString();
		auto schema = snapshot_result->Collection().GetValue(3, row).ToString();
		auto name = RefreshMetadata::SourceTableName(snapshot_result->Collection().GetValue(0, row).ToString(), catalog,
		                                             schema);
		stored_snapshots[StringUtil::Lower(SqlUtils::FullName(catalog, schema, name))] =
		    snapshot_result->Collection().GetValue(1, row).GetValue<int64_t>();
	}
	for (size_t i = 0; i < N; i++) {
		auto *get = GetLeafScan(leaves[i]);
		D_ASSERT(get);
		auto table_ref = get->GetTable();
		string table_name = table_ref.get()->name.GetIdentifierName();
		table_catalogs[i] = table_ref->ParentCatalog().GetName().GetIdentifierName();
		table_schemas[i] = table_ref->schema.name.GetIdentifierName();
		table_names[i] = table_name;
		auto stored_snapshot = stored_snapshots.find(
		    StringUtil::Lower(SqlUtils::FullName(table_catalogs[i], table_schemas[i], table_name)));
		if (stored_snapshot == stored_snapshots.end()) {
			throw Exception(ExceptionType::CATALOG, "IVM: no snapshot ID recorded for DuckLake table '" + table_name +
			                                            "' in view '" + input.context.view + "'");
		}
		old_snapshots[i] = stored_snapshot->second;
		// CreateDeltaGetNode reuses this snapshot instead of re-reading it for every term.
		auto &source_state = source_cache[table_ref.get()];
		source_state.last_snapshot = old_snapshots[i];
		source_state.has_last_snapshot = true;
		if (get->function.GetName().GetIdentifierName() == "ducklake_scan" && get->function.function_info) {
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
		// Compile facts are an optimization cache; skipping this scan falls back to authoritative metadata probes.
		for (size_t i = 0; i < N; i++) { // mull-ignore: cxx_lt_to_ge
			for (auto &entry : compile_facts.delta_shape) {
				if (!StringUtil::CIEquals(entry.first,
				                          SqlUtils::FullName(table_catalogs[i], table_schemas[i], table_names[i]))) {
					continue;
				}
				if (StringUtil::CIEquals(entry.second, "UNCHANGED")) {
					empty_table_delta[i] = true;
					activity_known[i] = true;
				} else if (StringUtil::CIEquals(entry.second, "INSERT_ONLY") ||
				           StringUtil::CIEquals(entry.second, "MIXED")) {
					activity_known[i] = true;
				}
				// This counter is used only by the debug summary below.
				// mull-ignore-next: cxx_arithmetic
				reused_activity_count += activity_known[i] ? 1 : 0;
				break;
			}
		}
		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Reused source activity for %zu/%zu leaves\n", reused_activity_count, N);
		RefreshMetadata metadata(con);
		// Skipping table-activity probes only retains extra terms; it cannot omit a required delta term.
		for (size_t i = 0; i < N; i++) { // mull-ignore: cxx_lt_to_ge
			if (activity_known[i]) {
				continue;
			}
			// Negative means unavailable; fail open by retaining the term.
			if (current_snapshots[i] < 0) { // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
				continue;
			}
			// Equal snapshots prove emptiness; treating a changed snapshot as unknown only retains an extra term.
			if (old_snapshots[i] == current_snapshots[i]) { // mull-ignore: cxx_eq_to_ne
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
			auto changes = DuckLakeChangesSQL("1", table_catalogs[i], table_schemas[i], table_names[i],
			                                  old_snapshots[i], current_snapshots[i]);
			// Failed probes fail open; the metadata path normally handles this before the SQL fallback.
			if (!DeltaJoinProbeExists(con, "(" + changes + ") openivm_delta_probe", "DuckLakeJoin")) {
				empty_table_delta[i] = true;
			}
		}
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
		// Snapshot zero is valid; excluding it only disables this optional key-domain pruning.
		if (skip_empty_enabled && !has_left_join && current_snapshots[i] >= 0 && // mull-ignore: cxx_ge_to_gt
		    !key_probes[i].empty()) {
			for (auto &probe : key_probes[i]) {
				size_t other = probe.other_leaf;
				// A probe always targets another leaf, so > and >= differ only for an impossible self-probe.
				int64_t other_snapshot = other > i ? old_snapshots[other] : -1; // mull-ignore: cxx_gt_to_ge
				auto changes = DuckLakeChangesSQL(SqlUtils::QuoteIdentifier(probe.delta_column) + " AS openivm_key",
				                                  table_catalogs[i], table_schemas[i], table_names[i], old_snapshots[i],
				                                  current_snapshots[i]);
				auto other_relation = DuckLakeQualifiedTable(table_catalogs[other], table_schemas[other],
				                                             table_names[other], other_snapshot);
				if (!DeltaJoinProbeExists(con,
				                          "(" + changes + ") openivm_delta_keys JOIN (SELECT * FROM " + other_relation +
				                              ") openivm_other ON openivm_delta_keys.openivm_key = openivm_other." +
				                              SqlUtils::QuoteIdentifier(probe.other_column),
				                          "DuckLakeJoin")) {
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

		LogicalOperator *term_root = term.get();

		// Demote only the outer joins whose NULL-supplying subtree contains this
		// term's delta. Preserved joins elsewhere in a left-deep star must remain
		// outer joins so unmatched rows continue to flow to later dimensions.
		if (has_left_join) {
			if (flattened_leaves || input.context.model.type == RefreshType::SIMPLE_PROJECTION) {
				DemoteOuterJoinsForMask(term.get(), leaves, (1ULL << i), input.context.model.type);
			} else if (leaves[i].is_right_of_left_join) {
				DemoteLeftJoins(term.get());
			}
		}

		// Replace leaf[i] with its delta: a bare GET directly, a GET wrapped in projections/filters by rewriting the
		// entire subtree. Flattened leaves are always bare GETs under pass-through wrappers.
		auto &delta_leaf_node = GetNodeAtPath(term, leaves[i].path);
		D_ASSERT(!flattened_leaves || delta_leaf_node->type == LogicalOperatorType::LOGICAL_GET);
		auto original_leaf_width = delta_leaf_node->GetColumnBindings().size();
		auto delta = CompileRegularLeafDelta(input, context, binder, delta_leaf_node, term_root, source_cache);
		delta_leaf_node = std::move(delta.op);
		ColumnBinding mul_binding = delta.mul_binding;
		if (flattened_leaves) {
			mul_binding = PropagateMultiplicityThroughPath(term, leaves[i].path, mul_binding);
		} else {
			UpdateParentProjectionMap(term, leaves[i], mul_binding, original_leaf_width);
		}

		// Telescoping: pin leaves j > i to old snapshot (AT VERSION).
		// Leaves j < i stay at current state (already the default).
		for (size_t j = i + 1; j < N; j++) {
			auto &leaf_node = GetNodeAtPath(term, leaves[j].path);
			auto *old_get = FindGetInSubtree(leaf_node.get());
			if (old_get && old_get->function.GetName().GetIdentifierName() == "ducklake_scan" &&
			    old_get->function.function_info) {
				auto &func_info = old_get->function.function_info->Cast<DuckLakeFunctionInfo>();
				func_info.snapshot.snapshot_id = static_cast<idx_t>(old_snapshots[j]);
				OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Pinned leaf %zu (table_index=%lu) to old snapshot %ld\n", j,
				                    (unsigned long)old_get->table_index.index, (long)old_snapshots[j]);
			}
		}

		terms.push_back(FinalizeJoinTerm(binder, std::move(term), {}, {mul_binding}, input.mul_type, false));

		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Term %zu: delta on leaf %zu, %zu leaves pinned to old\n", i, i, N - i - 1);
	}

	OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Active N-term count: %zu/%zu\n", terms.size(), N);
	return terms;
}

} // namespace duckdb
