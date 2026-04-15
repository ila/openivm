#include "rules/join.hpp"
#include "rules/ducklake_join.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "upsert/openivm_index_regen.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

void CollectJoinLeaves(LogicalOperator *node, vector<size_t> path, vector<JoinLeafInfo> &leaves,
                       bool is_right_of_left) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		bool is_left = false;
		bool is_right = false;
		if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
			is_left = (join && join->join_type == JoinType::LEFT);
			is_right = (join && join->join_type == JoinType::RIGHT);
		}
		path.push_back(0);
		CollectJoinLeaves(node->children[0].get(), path, leaves, is_right_of_left || is_right);
		path.pop_back();
		path.push_back(1);
		CollectJoinLeaves(node->children[1].get(), path, leaves, is_right_of_left || is_left);
		path.pop_back();
	} else if (node->type == LogicalOperatorType::LOGICAL_GET) {
		leaves.push_back({path, dynamic_cast<LogicalGet *>(node), node, is_right_of_left});
	} else {
		leaves.push_back({path, nullptr, node, is_right_of_left});
	}
}

LogicalGet *FindGetInSubtree(LogicalOperator *node) {
	while (node) {
		if (node->type == LogicalOperatorType::LOGICAL_GET) {
			return dynamic_cast<LogicalGet *>(node);
		}
		if (node->children.size() == 1) {
			node = node->children[0].get();
		} else {
			break;
		}
	}
	return nullptr;
}

unique_ptr<LogicalOperator> &GetNodeAtPath(unique_ptr<LogicalOperator> &root, const vector<size_t> &path) {
	unique_ptr<LogicalOperator> *current = &root;
	for (size_t step : path) {
		D_ASSERT(step < (*current)->children.size());
		current = &((*current)->children[step]);
	}
	return *current;
}

/// Verify all joins in the subtree are INNER, LEFT, or RIGHT. Returns true if any LEFT/RIGHT found.
static bool VerifyJoinTypes(LogicalOperator *node) {
	bool has_left = false;
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT) {
			has_left = true;
		} else if (join->join_type != JoinType::INNER) {
			throw Exception(ExceptionType::OPTIMIZER,
			                JoinTypeToString(join->join_type) + " type not yet supported in OpenIVM");
		}
	}
	for (auto &child : node->children) {
		if (VerifyJoinTypes(child.get())) {
			has_left = true;
		}
	}
	return has_left;
}

void DemoteLeftJoins(LogicalOperator *node) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *j = dynamic_cast<LogicalComparisonJoin *>(node);
		if (j && (j->join_type == JoinType::LEFT || j->join_type == JoinType::RIGHT)) {
			j->join_type = JoinType::INNER;
		}
	}
	for (auto &child : node->children) {
		DemoteLeftJoins(child.get());
	}
}

void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const JoinLeafInfo &leaf) {
	if (leaf.path.empty()) {
		return;
	}
	size_t child_side = leaf.path.back();
	unique_ptr<LogicalOperator> *parent = &term;
	for (size_t s = 0; s + 1 < leaf.path.size(); s++) {
		parent = &((*parent)->children[leaf.path[s]]);
	}
	if ((*parent)->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>((*parent).get());
		if (join) {
			auto &proj_map = (child_side == 0) ? join->left_projection_map : join->right_projection_map;
			if (!proj_map.empty()) {
				idx_t mul_idx = leaf.node->GetColumnBindings().size();
				proj_map.push_back(mul_idx);
				OPENIVM_DEBUG_PRINT("[IvmJoinRule] Added mul col %lu to %s proj_map\n", (unsigned long)mul_idx,
				                    child_side == 0 ? "left" : "right");
			}
		}
	}
}

// ============================================================================
// FK-aware term pruning: detect which inclusion-exclusion terms are redundant
// ============================================================================

/// Delta status for join leaves: which have insert-only deltas, which are completely empty.
struct DeltaStatus {
	uint64_t insert_only_mask; // bit i=1: leaf i has no delete rows (insert-only or empty)
	uint64_t empty_mask;       // bit i=1: leaf i has zero pending delta rows
};

/// For each leaf, detect delta status in a single query per table.
/// Returns both insert_only_mask (no deletes) and empty_mask (no rows at all).
static DeltaStatus DetectDeltaStatus(ClientContext &context, const string &view_name,
                                     const vector<JoinLeafInfo> &leaves) {
	DeltaStatus status = {0, 0};
	Connection con(*context.db);
	con.SetAutoCommit(false);

	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
		if (!get) {
			continue;
		}
		auto table_ref = get->GetTable();
		if (table_ref.get() == nullptr) {
			continue;
		}
		string delta_name = OpenIVMUtils::DeltaName(table_ref.get()->name);

		// Get last_update timestamp for this view+table pair
		auto ts_result = con.Query("SELECT last_update FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                           " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
		                           "' AND table_name = '" + OpenIVMUtils::EscapeValue(delta_name) + "'");
		if (ts_result->HasError() || ts_result->RowCount() == 0) {
			continue;
		}
		string last_update = ts_result->GetValue(0, 0).ToString();

		// Single query: get total row count and delete count since last_update
		auto result =
		    con.Query("SELECT COUNT(*), COUNT(*) FILTER (WHERE " + string(ivm::MULTIPLICITY_COL) + " = false) FROM " +
		              OpenIVMUtils::QuoteIdentifier(delta_name) + " WHERE " + string(ivm::TIMESTAMP_COL) + " >= '" +
		              OpenIVMUtils::EscapeValue(last_update) + "'::TIMESTAMP");
		if (result->HasError()) {
			continue;
		}
		int64_t total_count = result->GetValue(0, 0).GetValue<int64_t>();
		int64_t delete_count = result->GetValue(1, 0).GetValue<int64_t>();

		if (total_count == 0) {
			status.empty_mask |= (1ULL << i);
			status.insert_only_mask |= (1ULL << i); // empty is trivially insert-only
			OPENIVM_DEBUG_PRINT("[IvmJoinRule] Leaf %zu (%s) has empty delta\n", i, table_ref.get()->name.c_str());
		} else if (delete_count == 0) {
			status.insert_only_mask |= (1ULL << i);
			OPENIVM_DEBUG_PRINT("[IvmJoinRule] Leaf %zu (%s) has insert-only delta\n", i,
			                    table_ref.get()->name.c_str());
		}
	}
	return status;
}

/// Build a set of FK relationships between join leaves.
/// Returns pairs (fk_leaf_idx, pk_leaf_idx) where leaf fk_leaf_idx has a FK referencing leaf pk_leaf_idx,
/// AND the join condition between them uses the FK/PK columns.
struct FKRelation {
	size_t fk_leaf; // leaf index of the referencing (FK) table
	size_t pk_leaf; // leaf index of the referenced (PK) table
};

static vector<FKRelation> DetectFKRelations(ClientContext &context, const vector<JoinLeafInfo> &leaves,
                                            LogicalOperator *join_root) {
	vector<FKRelation> relations;

	// Build map: table_name -> leaf index (for matching FK targets to leaves)
	unordered_map<string, size_t> table_to_leaf;
	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
		if (!get) {
			continue;
		}
		auto table_ref = get->GetTable();
		if (table_ref.get() == nullptr) {
			continue;
		}
		table_to_leaf[table_ref.get()->name] = i;
	}

	// For each leaf, check its constraints for FK references to other leaves
	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
		if (!get) {
			continue;
		}
		auto table_ref = get->GetTable();
		if (table_ref.get() == nullptr) {
			continue;
		}

		auto &constraints = table_ref->Cast<TableCatalogEntry>().GetConstraints();
		for (auto &constraint : constraints) {
			if (constraint->type != ConstraintType::FOREIGN_KEY) {
				continue;
			}
			auto &fk = constraint->Cast<ForeignKeyConstraint>();
			// FK_TYPE_FOREIGN_KEY_TABLE means this table is the referencing side
			if (fk.info.type != ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE) {
				continue;
			}
			// Check if the referenced table is also a leaf in this join
			auto it = table_to_leaf.find(fk.info.table);
			if (it == table_to_leaf.end()) {
				continue;
			}
			size_t pk_leaf = it->second;
			relations.push_back({i, pk_leaf});
			OPENIVM_DEBUG_PRINT("[IvmJoinRule] FK relation: leaf %zu (%s) -> leaf %zu (%s)\n", i,
			                    table_ref.get()->name.c_str(), pk_leaf, fk.info.table.c_str());
		}
	}
	return relations;
}

/// Compute a bitmask of PK leaves whose delta terms can be skipped entirely.
///
/// For FK relation (fk_leaf -> pk_leaf): when pk_leaf's delta is insert-only, ALL terms
/// that have pk_leaf's bit set produce zero net contribution. This is because the terms
/// with the PK bit cancel algebraically via XOR:
///
///   Term {PK}:        R_current ⋈ ΔS⁺ = (R_old + ΔR) ⋈ ΔS⁺ = R_old⋈ΔS⁺ + ΔR⋈ΔS⁺
///   Term {FK,PK}:     ΔR ⋈ ΔS⁺ with XOR sign (= -1)         = -ΔR⋈ΔS⁺
///   Net:              R_old ⋈ ΔS⁺ = ∅  (FK integrity: no old FK row references new PKs)
///
/// Works regardless of whether ΔR is empty or not — the ΔR⋈ΔS⁺ parts cancel exactly.
static uint64_t ComputeSkipBits(const vector<FKRelation> &fk_relations, uint64_t insert_only_mask) {
	uint64_t skip_bits = 0;
	for (auto &fk : fk_relations) {
		if (insert_only_mask & (1ULL << fk.pk_leaf)) {
			skip_bits |= (1ULL << fk.pk_leaf);
		}
	}
	return skip_bits;
}

// ============================================================================
// BuildInclusionExclusionTerms: create 2^N - 1 delta terms
// ============================================================================
static vector<unique_ptr<LogicalOperator>> BuildInclusionExclusionTerms(PlanWrapper &pw, ClientContext &context,
                                                                        Binder &binder,
                                                                        const vector<JoinLeafInfo> &leaves,
                                                                        bool has_left_join) {
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	// Detect delta status for all leaves (single query per table: total + delete count).
	// Used by both FK pruning and empty-delta skipping.
	DeltaStatus delta_status = DetectDeltaStatus(context, pw.view, leaves);

	// FK-aware pruning: detect insert-only PK leaves whose delta terms cancel algebraically.
	Value fk_pruning_val;
	bool fk_pruning_enabled = true;
	if (context.TryGetCurrentSetting("ivm_fk_pruning", fk_pruning_val) && !fk_pruning_val.IsNull()) {
		fk_pruning_enabled = fk_pruning_val.GetValue<bool>();
	}
	uint64_t skip_bits = 0;
	if (fk_pruning_enabled) {
		auto fk_relations = DetectFKRelations(context, leaves, pw.plan.get());
		if (!fk_relations.empty()) {
			skip_bits = ComputeSkipBits(fk_relations, delta_status.insert_only_mask);
		}
	}

	// Empty-delta skipping: skip terms where any table in the mask has zero delta rows.
	// A join with an empty input always produces zero rows.
	Value skip_empty_val;
	bool skip_empty_enabled = true;
	if (context.TryGetCurrentSetting("ivm_skip_empty_deltas", skip_empty_val) && !skip_empty_val.IsNull()) {
		skip_empty_enabled = skip_empty_val.GetValue<bool>();
	}
	uint64_t empty_mask = skip_empty_enabled ? delta_status.empty_mask : 0;

	uint64_t pruned_count = 0;
	uint64_t total_terms = (1ULL << N) - 1;
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Building inclusion-exclusion terms (%lu total, skip_bits=%lu, empty_mask=%lu)\n",
	                    (unsigned long)total_terms, (unsigned long)skip_bits, (unsigned long)empty_mask);
	for (uint64_t mask = 1; mask < (1ULL << N); mask++) {
		// Safety: always generate at least one term to avoid empty UNION ALL.
		// The last mask (all bits set) is the fallback — even if it produces zero rows.
		bool is_last_chance = (mask == total_terms) && terms.empty();

		// FK pruning: skip any term whose mask overlaps with insert-only PK leaves.
		// All such terms cancel algebraically via XOR (see ComputeSkipBits).
		if (!is_last_chance && skip_bits && (mask & skip_bits)) {
			pruned_count++;
			OPENIVM_DEBUG_PRINT("[IvmJoinRule] Pruned term mask=%lu (FK insert-only PK)\n", (unsigned long)mask);
			continue;
		}
		// Empty-delta skipping: if any table in the mask has zero delta rows,
		// the join term produces zero rows (join with empty input = empty).
		if (!is_last_chance && empty_mask && (mask & empty_mask)) {
			pruned_count++;
			OPENIVM_DEBUG_PRINT("[IvmJoinRule] Skipped term mask=%lu (empty delta)\n", (unsigned long)mask);
			continue;
		}
		auto term = pw.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);
		LogicalOperator *term_root = term.get();
		vector<ColumnBinding> mul_bindings;

		// For LEFT JOINs: demote to INNER when only right-side leaves have deltas
		if (has_left_join) {
			bool has_left_side_delta = false;
			for (size_t i = 0; i < N; i++) {
				if ((mask & (1ULL << i)) && !leaves[i].is_right_of_left_join) {
					has_left_side_delta = true;
					break;
				}
			}
			if (!has_left_side_delta) {
				DemoteLeftJoins(term.get());
			}
		}

		// Replace delta leaves
		for (size_t i = 0; i < N; i++) {
			if (mask & (1ULL << i)) {
				if (leaves[i].get) {
					DeltaGetResult delta_i = CreateDeltaGetNode(context, binder, leaves[i].get, pw.view);
					mul_bindings.push_back(delta_i.mul_binding);
					GetNodeAtPath(term, leaves[i].path) = std::move(delta_i.node);
				} else {
					auto &subtree_ref = GetNodeAtPath(term, leaves[i].path);
					auto rewritten = IVMRewriteRule::RewritePlan(pw.input, subtree_ref, pw.view, term_root);
					mul_bindings.push_back(rewritten.mul_binding);
					subtree_ref = std::move(rewritten.op);
				}
				UpdateParentProjectionMap(term, leaves[i]);
			}
		}

		term->ResolveOperatorTypes();

		// Build projection: original columns + combined multiplicity
		auto term_bindings = term->GetColumnBindings();
		auto term_types = term->types;
		vector<unique_ptr<Expression>> proj_exprs;

		// Filter out multiplicity columns (O(1) lookup via hash set)
		unordered_set<uint64_t> mul_set;
		for (auto &mb : mul_bindings) {
			mul_set.insert((uint64_t)mb.table_index ^ ((uint64_t)mb.column_index * 0x9e3779b97f4a7c15ULL));
		}
		for (idx_t i = 0; i < term_bindings.size(); i++) {
			uint64_t key = (uint64_t)term_bindings[i].table_index ^
			               ((uint64_t)term_bindings[i].column_index * 0x9e3779b97f4a7c15ULL);
			if (!mul_set.count(key)) {
				proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(term_types[i], term_bindings[i]));
			}
		}

		// Combined multiplicity: XOR chain (a != b for booleans)
		unique_ptr<Expression> combined_mul = make_uniq<BoundColumnRefExpression>(pw.mul_type, mul_bindings[0]);
		for (size_t i = 1; i < mul_bindings.size(); i++) {
			auto next = make_uniq<BoundColumnRefExpression>(pw.mul_type, mul_bindings[i]);
			combined_mul = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_NOTEQUAL,
			                                                    std::move(combined_mul), std::move(next));
		}
		proj_exprs.push_back(std::move(combined_mul));

		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));
	}
	if (pruned_count > 0) {
		OPENIVM_DEBUG_PRINT("[IvmJoinRule] FK pruning: %lu/%lu terms pruned, %lu remaining\n",
		                    (unsigned long)pruned_count, (unsigned long)total_terms, (unsigned long)terms.size());
	}
	return terms;
}

// ============================================================================
// AssembleUnionAll: combine terms with UNION ALL + clean projection
// ============================================================================
static unique_ptr<LogicalOperator> AssembleUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                    const vector<LogicalType> &types, Binder &binder) {
	auto result = std::move(terms[0]);
	for (size_t i = 1; i < terms.size(); i++) {
		auto union_table_index = binder.GenerateTableIndex();
		result = make_uniq<LogicalSetOperation>(union_table_index, types.size(), std::move(result), std::move(terms[i]),
		                                        LogicalOperatorType::LOGICAL_UNION, true);
		result->types = types;
	}

	// Clean projection to disambiguate column names for LPTS
	auto union_bindings = result->GetColumnBindings();
	vector<unique_ptr<Expression>> clean_exprs;
	for (idx_t i = 0; i < union_bindings.size(); i++) {
		clean_exprs.push_back(make_uniq<BoundColumnRefExpression>(types[i], union_bindings[i]));
	}
	auto clean_proj = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(clean_exprs));
	clean_proj->children.push_back(std::move(result));
	clean_proj->ResolveOperatorTypes();
	return std::move(clean_proj);
}

// ============================================================================
// ReplaceOutputBindings: map original bindings to new UNION ALL output
// ============================================================================
static ColumnBinding ReplaceOutputBindings(const vector<ColumnBinding> &original_bindings,
                                           unique_ptr<LogicalOperator> &result, LogicalOperator &root) {
	auto union_bindings = result->GetColumnBindings();
	if (union_bindings.size() < 2) {
		throw InternalException("Join rewrite produced too few bindings (%zu)", union_bindings.size());
	}
	ColumnBindingReplacer replacer;
	idx_t map_count = std::min(original_bindings.size(), union_bindings.size() - 1);
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Binding replacement: %zu mappings (original=%zu, union=%zu)\n", map_count,
	                    original_bindings.size(), union_bindings.size());
	for (idx_t col_idx = 0; col_idx < map_count; ++col_idx) {
		replacer.replacement_bindings.emplace_back(original_bindings[col_idx], union_bindings[col_idx]);
	}
	replacer.stop_operator = result;
	replacer.VisitOperator(root);
	return union_bindings.back();
}

// ============================================================================
// IvmJoinRule::Rewrite — main entry point
// ============================================================================
ModifiedPlan IvmJoinRule::Rewrite(PlanWrapper pw) {
	ClientContext &context = pw.input.context;
	Binder &binder = pw.input.optimizer.binder;
	const vector<ColumnBinding> original_bindings = pw.plan->GetColumnBindings();

	// 1. Verify + collect
	bool has_left_join = VerifyJoinTypes(pw.plan.get());
	vector<JoinLeafInfo> leaves;
	CollectJoinLeaves(pw.plan.get(), {}, leaves);
	size_t N = leaves.size();
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Rewriting JOIN node, %zu leaves found\n", N);

	if (N == 0) {
		throw InternalException("IvmJoinRule: no leaves found in join tree");
	}
	if (N > ivm::MAX_JOIN_TABLES) {
		throw NotImplementedException("Inclusion-exclusion IVM not supported for joins with more than 16 tables");
	}

	// 2. Output types
	pw.plan->ResolveOperatorTypes();
	auto types = pw.plan->types;
	D_ASSERT(types.size() == original_bindings.size());
	types.emplace_back(pw.mul_type);

	// 3. Build terms — use DuckLake N-term path when all leaves are DuckLake scans
	// Check if all leaves are DuckLake scans AND N-term telescoping is enabled.
	bool all_ducklake = true;
	Value nterm_val;
	if (context.TryGetCurrentSetting("ivm_ducklake_nterm", nterm_val) && !nterm_val.IsNull() &&
	    !nterm_val.GetValue<bool>()) {
		all_ducklake = false; // forced to inclusion-exclusion
	} else {
		for (size_t i = 0; i < N; i++) {
			auto *get = leaves[i].get ? leaves[i].get : FindGetInSubtree(leaves[i].node);
			if (!get || get->function.name != "ducklake_scan") {
				all_ducklake = false;
				break;
			}
		}
	}

	auto terms = all_ducklake ? BuildDuckLakeJoinTerms(pw, context, binder, leaves, has_left_join)
	                          : BuildInclusionExclusionTerms(pw, context, binder, leaves, has_left_join);

	// 4. UNION ALL
	auto result = AssembleUnionAll(terms, types, binder);

	// 5. Rebind parent references
	ColumnBinding new_mul_binding = ReplaceOutputBindings(original_bindings, result, *pw.root);

	pw.plan = std::move(result);
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Done, %zu terms unioned, mul_binding: table=%lu col=%lu\n", terms.size(),
	                    (unsigned long)new_mul_binding.table_index, (unsigned long)new_mul_binding.column_index);
	return {std::move(pw.plan), new_mul_binding};
}

} // namespace duckdb
