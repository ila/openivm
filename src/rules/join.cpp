#include "rules/join.hpp"
#include "rules/ducklake_join.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "core/ivm_delta_model.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/openivm_utils.hpp"
#include "upsert/openivm_index_regen.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

static uint64_t BindingKey(const ColumnBinding &binding) {
	return (uint64_t)binding.table_index ^ ((uint64_t)binding.column_index * 0x9e3779b97f4a7c15ULL);
}

struct JoinColumnRef {
	size_t leaf_index;
	string table_name;
	string delta_name;
	string column_name;
	string last_update;
};

struct JoinKeyProbe {
	size_t other_leaf;
	string delta_column;
	string other_column;
};

static bool TryGetColumnRef(Expression &expr, ColumnBinding &binding) {
	if (expr.GetExpressionClass() == ExpressionClass::BOUND_CAST) {
		auto &cast = expr.Cast<BoundCastExpression>();
		return TryGetColumnRef(*cast.child, binding);
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	auto &col = expr.Cast<BoundColumnRefExpression>();
	binding = col.binding;
	return true;
}

static void CollectJoinKeyProbes(LogicalOperator *node, const unordered_map<uint64_t, JoinColumnRef> &column_refs,
                                 vector<vector<JoinKeyProbe>> &probes) {
	if (!node) {
		return;
	}
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join && join->join_type == JoinType::INNER) {
			for (auto &cond : join->conditions) {
				if (cond.comparison != ExpressionType::COMPARE_EQUAL) {
					continue;
				}
				ColumnBinding left_binding, right_binding;
				if (!TryGetColumnRef(*cond.left, left_binding) || !TryGetColumnRef(*cond.right, right_binding)) {
					continue;
				}
				auto left_entry = column_refs.find(BindingKey(left_binding));
				auto right_entry = column_refs.find(BindingKey(right_binding));
				if (left_entry == column_refs.end() || right_entry == column_refs.end()) {
					continue;
				}
				auto &left = left_entry->second;
				auto &right = right_entry->second;
				if (left.leaf_index == right.leaf_index) {
					continue;
				}
				probes[left.leaf_index].push_back({right.leaf_index, left.column_name, right.column_name});
				probes[right.leaf_index].push_back({left.leaf_index, right.column_name, left.column_name});
			}
		}
	}
	for (auto &child : node->children) {
		CollectJoinKeyProbes(child.get(), column_refs, probes);
	}
}

static bool DeltaKeyHasBaseMatch(Connection &con, const JoinColumnRef &delta_ref, const string &delta_column,
                                 const JoinColumnRef &other_ref, const string &other_column) {
	string sql = "SELECT EXISTS(SELECT 1 FROM (SELECT " + OpenIVMUtils::QuoteIdentifier(delta_column) +
	             " AS _ivm_key FROM " + OpenIVMUtils::QuoteIdentifier(delta_ref.delta_name) + " WHERE " +
	             string(ivm::TIMESTAMP_COL) + " >= '" + OpenIVMUtils::EscapeValue(delta_ref.last_update) +
	             "'::TIMESTAMP) _ivm_delta_keys JOIN " + OpenIVMUtils::QuoteIdentifier(other_ref.table_name) +
	             " _ivm_other ON _ivm_delta_keys._ivm_key = _ivm_other." + OpenIVMUtils::QuoteIdentifier(other_column) +
	             " LIMIT 1)";
	auto result = con.Query(sql);
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		OPENIVM_DEBUG_PRINT("[IvmJoinRule] Could not probe key-domain intersection: %s\n",
		                    result->HasError() ? result->GetError().c_str() : "no result");
		return true;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

static bool DeltaKeyHasDeltaMatch(Connection &con, const JoinColumnRef &left_ref, const string &left_column,
                                  const JoinColumnRef &right_ref, const string &right_column) {
	string sql = "SELECT EXISTS(SELECT 1 FROM (SELECT " + OpenIVMUtils::QuoteIdentifier(left_column) +
	             " AS _ivm_key FROM " + OpenIVMUtils::QuoteIdentifier(left_ref.delta_name) + " WHERE " +
	             string(ivm::TIMESTAMP_COL) + " >= '" + OpenIVMUtils::EscapeValue(left_ref.last_update) +
	             "'::TIMESTAMP) _ivm_left_delta_keys JOIN (SELECT " + OpenIVMUtils::QuoteIdentifier(right_column) +
	             " AS _ivm_key FROM " + OpenIVMUtils::QuoteIdentifier(right_ref.delta_name) + " WHERE " +
	             string(ivm::TIMESTAMP_COL) + " >= '" + OpenIVMUtils::EscapeValue(right_ref.last_update) +
	             "'::TIMESTAMP) _ivm_right_delta_keys ON _ivm_left_delta_keys._ivm_key = "
	             "_ivm_right_delta_keys._ivm_key LIMIT 1)";
	auto result = con.Query(sql);
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		OPENIVM_DEBUG_PRINT("[IvmJoinRule] Could not probe delta key-domain intersection: %s\n",
		                    result->HasError() ? result->GetError().c_str() : "no result");
		return true;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

static void CollectExistingMultiplicityBindings(LogicalOperator *node, unordered_set<uint64_t> &mul_set) {
	if (!node) {
		return;
	}
	if (node->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = node->Cast<LogicalCTERef>();
		if (!ref.bound_columns.empty() && ref.bound_columns.back() == ivm::MULTIPLICITY_COL) {
			auto bindings = node->GetColumnBindings();
			if (!bindings.empty()) {
				mul_set.insert(BindingKey(bindings.back()));
			}
		}
	}
	for (auto &child : node->children) {
		CollectExistingMultiplicityBindings(child.get(), mul_set);
	}
}

static void FilterInternalMultiplicityColumns(const vector<ColumnBinding> &bindings, const vector<LogicalType> &types,
                                              const unordered_set<uint64_t> &mul_set,
                                              vector<ColumnBinding> &filtered_bindings,
                                              vector<LogicalType> &filtered_types) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (mul_set.count(BindingKey(bindings[i]))) {
			continue;
		}
		filtered_bindings.push_back(bindings[i]);
		filtered_types.push_back(types[i]);
	}
}

void CollectJoinLeaves(LogicalOperator *node, vector<size_t> path, vector<JoinLeafInfo> &leaves,
                       bool is_right_of_left) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT ||
	    node->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		bool is_left = false;
		bool is_right = false;
		bool is_full_outer = false;
		if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		    node->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
			// LogicalAnyJoin inherits from LogicalJoin — join_type lives at that level.
			auto *join = dynamic_cast<LogicalJoin *>(node);
			is_left = (join && join->join_type == JoinType::LEFT);
			is_right = (join && join->join_type == JoinType::RIGHT);
			is_full_outer = (join && join->join_type == JoinType::OUTER);
		}
		path.push_back(0);
		CollectJoinLeaves(node->children[0].get(), path, leaves, is_right_of_left || is_right || is_full_outer);
		path.pop_back();
		path.push_back(1);
		CollectJoinLeaves(node->children[1].get(), path, leaves, is_right_of_left || is_left || is_full_outer);
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

static LogicalGet *GetLeafScan(const JoinLeafInfo &leaf) {
	return leaf.get ? leaf.get : FindGetInSubtree(leaf.node);
}

static bool ResolveLeafBindingToBaseColumn(LogicalOperator *node, const ColumnBinding &binding, string &table_name,
                                           string &column_name) {
	if (!node) {
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_GET) {
		auto *get = dynamic_cast<LogicalGet *>(node);
		if (!get || get->GetTable().get() == nullptr) {
			return false;
		}
		auto bindings = get->GetColumnBindings();
		auto &column_ids = get->GetColumnIds();
		idx_t count = std::min<idx_t>(bindings.size(), column_ids.size());
		for (idx_t col_idx = 0; col_idx < count; col_idx++) {
			if (BindingKey(bindings[col_idx]) != BindingKey(binding) || column_ids[col_idx].IsVirtualColumn()) {
				continue;
			}
			table_name = get->GetTable().get()->name;
			column_name = get->GetColumnName(column_ids[col_idx]);
			return true;
		}
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_PROJECTION && !node->children.empty()) {
		auto &projection = node->Cast<LogicalProjection>();
		auto bindings = node->GetColumnBindings();
		idx_t count = std::min<idx_t>(bindings.size(), projection.expressions.size());
		for (idx_t expr_idx = 0; expr_idx < count; expr_idx++) {
			if (BindingKey(bindings[expr_idx]) != BindingKey(binding)) {
				continue;
			}
			ColumnBinding child_binding;
			if (!TryGetColumnRef(*projection.expressions[expr_idx], child_binding)) {
				return false;
			}
			return ResolveLeafBindingToBaseColumn(node->children[0].get(), child_binding, table_name, column_name);
		}
	}
	if (node->children.size() == 1) {
		auto bindings = node->GetColumnBindings();
		auto child_bindings = node->children[0]->GetColumnBindings();
		idx_t count = std::min<idx_t>(bindings.size(), child_bindings.size());
		for (idx_t col_idx = 0; col_idx < count; col_idx++) {
			if (BindingKey(bindings[col_idx]) == BindingKey(binding)) {
				return ResolveLeafBindingToBaseColumn(node->children[0].get(), child_bindings[col_idx], table_name,
				                                      column_name);
			}
		}
		return ResolveLeafBindingToBaseColumn(node->children[0].get(), binding, table_name, column_name);
	}
	return false;
}

static bool IsConstantLeafSubtree(LogicalOperator *node) {
	if (!node) {
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_GET) {
		auto *get = dynamic_cast<LogicalGet *>(node);
		return get && get->GetTable().get() == nullptr;
	}
	if (node->type == LogicalOperatorType::LOGICAL_CHUNK_GET || node->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN ||
	    node->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET ||
	    node->type == LogicalOperatorType::LOGICAL_UNNEST) {
		return true;
	}
	if (node->children.empty()) {
		return false;
	}
	for (auto &child : node->children) {
		if (!IsConstantLeafSubtree(child.get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<LogicalOperator> &GetNodeAtPath(unique_ptr<LogicalOperator> &root, const vector<size_t> &path) {
	unique_ptr<LogicalOperator> *current = &root;
	for (size_t step : path) {
		D_ASSERT(step < (*current)->children.size());
		current = &((*current)->children[step]);
	}
	return *current;
}

/// Verify all joins in the subtree are supported. Returns true if any LEFT/RIGHT/OUTER found.
/// MARK/SEMI/ANTI joins (from IN-list, EXISTS, etc.) are allowed: LPTS converts MARK→LEFT JOIN,
/// and their constant right-side (VALUES list) has no delta so inclusion-exclusion reduces trivially.
static bool VerifyJoinTypes(LogicalOperator *node) {
	bool has_left = false;
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT ||
		    join->join_type == JoinType::OUTER) {
			has_left = true;
		} else if (join->join_type != JoinType::INNER && join->join_type != JoinType::MARK &&
		           join->join_type != JoinType::SEMI && join->join_type != JoinType::ANTI &&
		           join->join_type != JoinType::RIGHT_SEMI && join->join_type != JoinType::RIGHT_ANTI) {
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
		if (j &&
		    (j->join_type == JoinType::LEFT || j->join_type == JoinType::RIGHT || j->join_type == JoinType::OUTER)) {
			j->join_type = JoinType::INNER;
		}
	}
	for (auto &child : node->children) {
		DemoteLeftJoins(child.get());
	}
}

// Per-LJ demote: walk the join tree and demote only the LJs that need it,
// based on which subtree contains delta-marked leaves in the current mask.
// Demoting indiscriminately (the original DemoteLeftJoins) drops NULL-padded
// rows from intermediate LJs that have no delta in this mask, which breaks
// chained-LJ correctness when only a deeper-right table has changes
// (probe 11: base LJ d1 LJ d2, Δd2 only).
//
// Rules (driven by which side supplies NULLs for that join type):
//   LEFT JOIN  — right side is NULL-supplying; demote iff right subtree has Δ.
//   RIGHT JOIN — left side is NULL-supplying; demote iff left subtree has Δ.
//   FULL OUTER — both sides are NULL-supplying; demote iff EITHER subtree has Δ.
//
// This subsumes the previous global demote conditions without over-demoting in
// chained-LJ shapes. For FULL OUTER aggregate views the upsert MERGE relies on
// the IE delta containing only the matched-via-Δ portion (the unmatched parts
// are tracked separately via _ivm_match_count); failing to demote there would
// leak phantom unmatched rows for groups untouched by the delta.
static bool SubtreeHasDeltaLeaf(const vector<JoinLeafInfo> &leaves, uint64_t mask, const vector<size_t> &prefix) {
	for (size_t i = 0; i < leaves.size(); i++) {
		if (!(mask & (1ULL << i))) {
			continue;
		}
		const auto &lp = leaves[i].path;
		if (lp.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), lp.begin())) {
			return true;
		}
	}
	return false;
}

static void DemoteLeftJoinsForMaskRec(LogicalOperator *node, const vector<JoinLeafInfo> &leaves, uint64_t mask,
                                      vector<size_t> &path) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *j = dynamic_cast<LogicalComparisonJoin *>(node);
		if (j &&
		    (j->join_type == JoinType::LEFT || j->join_type == JoinType::RIGHT || j->join_type == JoinType::OUTER)) {
			bool demote = false;
			path.push_back(0); // left child
			bool left_has_delta = SubtreeHasDeltaLeaf(leaves, mask, path);
			path.pop_back();
			path.push_back(1); // right child
			bool right_has_delta = SubtreeHasDeltaLeaf(leaves, mask, path);
			path.pop_back();
			if (j->join_type == JoinType::LEFT) {
				demote = right_has_delta;
			} else if (j->join_type == JoinType::RIGHT) {
				demote = left_has_delta;
			} else { // OUTER (FULL OUTER)
				demote = left_has_delta || right_has_delta;
			}
			if (demote) {
				j->join_type = JoinType::INNER;
			}
		}
	}
	for (size_t ci = 0; ci < node->children.size(); ci++) {
		path.push_back(ci);
		DemoteLeftJoinsForMaskRec(node->children[ci].get(), leaves, mask, path);
		path.pop_back();
	}
}

static void DemoteLeftJoinsForMask(LogicalOperator *node, const vector<JoinLeafInfo> &leaves, uint64_t mask) {
	vector<size_t> path;
	DemoteLeftJoinsForMaskRec(node, leaves, mask, path);
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
		LogicalGet *get = GetLeafScan(leaves[i]);
		if (!get) {
			// Constant relation leaves (VALUES/CHUNK/DUMMY), often wrapped in projections,
			// have no catalog backing and therefore no delta. Other !get cases may contain
			// real tables inside nested joins, so only prune when the whole leaf subtree is
			// constant.
			if (IsConstantLeafSubtree(leaves[i].node)) {
				status.empty_mask |= (1ULL << i);
				status.insert_only_mask |= (1ULL << i);
				OPENIVM_DEBUG_PRINT("[IvmJoinRule] Leaf %zu (constant values) has empty delta\n", i);
			}
			continue;
		}
		auto table_ref = get->GetTable();
		if (table_ref.get() == nullptr) {
			// Table function (generate_series, range, etc.) has no catalog-backing
			// table, so no delta table exists. Its output is constant across
			// refreshes — the "delta" is always empty. Mark it as empty so the
			// inclusion-exclusion pruner drops every term where this leaf's bit
			// is set: only the term that keeps the table function as its original
			// scan on the "full" side contributes rows, matching the semantics of
			// a non-changing input.
			status.empty_mask |= (1ULL << i);
			status.insert_only_mask |= (1ULL << i);
			OPENIVM_DEBUG_PRINT("[IvmJoinRule] Leaf %zu (table function '%s') has empty delta\n", i,
			                    get->function.name.c_str());
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
		    con.Query("SELECT COUNT(*), COUNT(*) FILTER (WHERE " + string(ivm::MULTIPLICITY_COL) + " < 0) FROM " +
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
		LogicalGet *get = GetLeafScan(leaves[i]);
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
		LogicalGet *get = GetLeafScan(leaves[i]);
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

enum class JoinDeltaPruneReason { NONE, FK_INSERT_ONLY_PK, EMPTY_DELTA, KEY_DOMAIN_EMPTY };

// Everything below separates "term planning" from "term construction".
//
// The join delta itself is still the same OpenIVM inclusion-exclusion formula:
// enumerate every non-empty mask over the join leaves, replace masked leaves by
// their delta scans, keep unmasked leaves as current base scans, then apply the
// Möbius sign in the output multiplicity. Planning exists only to make the skip
// decisions explicit before we copy/rebind a logical plan:
//   - FK insert-only PK pruning removes masks that cancel algebraically.
//   - Empty-delta pruning removes masks that would join against an empty input.
//   - Key-domain pruning removes inner-join masks whose delta keys cannot match.
//
// Keeping these reasons as data is important for the longer-term delta model work:
// the rule should first describe the delta terms it intends to build, and only
// then lower those terms into DuckDB logical operators.
struct JoinDeltaPlanningContext {
	DeltaStatus delta_status;
	bool skip_empty_enabled = true;
	uint64_t skip_bits = 0;
	uint64_t empty_mask = 0;
	uint64_t total_terms = 0;
	vector<JoinColumnRef> leaf_refs;
	vector<vector<JoinKeyProbe>> key_probes;
};

struct JoinDeltaTermPlan {
	uint64_t mask;
	JoinDeltaPruneReason prune_reason;

	bool IsKept() const {
		return prune_reason == JoinDeltaPruneReason::NONE;
	}
};

static const char *JoinDeltaPruneReasonName(JoinDeltaPruneReason reason) {
	switch (reason) {
	case JoinDeltaPruneReason::NONE:
		return "kept";
	case JoinDeltaPruneReason::FK_INSERT_ONLY_PK:
		return "FK insert-only PK";
	case JoinDeltaPruneReason::EMPTY_DELTA:
		return "empty delta";
	case JoinDeltaPruneReason::KEY_DOMAIN_EMPTY:
		return "delta key-domain empty";
	default:
		return "unknown";
	}
}

static bool GetBooleanSetting(ClientContext &context, const string &setting_name, bool default_value) {
	Value setting_value;
	if (context.TryGetCurrentSetting(setting_name, setting_value) && !setting_value.IsNull()) {
		return setting_value.GetValue<bool>();
	}
	return default_value;
}

static void CollectJoinKeyProbeMetadata(PlanWrapper &pw, Connection &con, const vector<JoinLeafInfo> &leaves,
                                        JoinDeltaPlanningContext &planning) {
	unordered_map<uint64_t, JoinColumnRef> column_refs;
	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = GetLeafScan(leaves[i]);
		if (!get) {
			continue;
		}
		auto table_ref = get->GetTable();
		if (table_ref.get() == nullptr) {
			continue;
		}
		string table_name = table_ref.get()->name;
		string delta_name = OpenIVMUtils::DeltaName(table_name);
		auto ts_result = con.Query("SELECT last_update FROM " + string(ivm::DELTA_TABLES_TABLE) +
		                           " WHERE view_name = '" + OpenIVMUtils::EscapeValue(pw.view) +
		                           "' AND table_name = '" + OpenIVMUtils::EscapeValue(delta_name) + "'");
		if (ts_result->HasError() || ts_result->RowCount() == 0 || ts_result->GetValue(0, 0).IsNull()) {
			continue;
		}
		planning.leaf_refs[i].leaf_index = i;
		planning.leaf_refs[i].table_name = table_name;
		planning.leaf_refs[i].delta_name = delta_name;
		planning.leaf_refs[i].last_update = ts_result->GetValue(0, 0).ToString();

		auto leaf_bindings = leaves[i].node->GetColumnBindings();
		for (auto &binding : leaf_bindings) {
			string resolved_table;
			string resolved_column;
			if (!ResolveLeafBindingToBaseColumn(leaves[i].node, binding, resolved_table, resolved_column) ||
			    !StringUtil::CIEquals(resolved_table, table_name)) {
				continue;
			}
			planning.leaf_refs[i].column_name = resolved_column;
			column_refs[BindingKey(binding)] = {i, table_name, delta_name, resolved_column,
			                                    planning.leaf_refs[i].last_update};
		}
	}
	CollectJoinKeyProbes(pw.plan.get(), column_refs, planning.key_probes);
}

static JoinDeltaPlanningContext BuildJoinDeltaPlanningContext(PlanWrapper &pw, ClientContext &context,
                                                              const vector<JoinLeafInfo> &leaves, bool has_left_join) {
	JoinDeltaPlanningContext planning;
	size_t N = leaves.size();
	planning.total_terms = (1ULL << N) - 1;
	planning.leaf_refs.resize(N);
	planning.key_probes.resize(N);

	// Detect delta status for all leaves (single query per table: total + delete count).
	// Used by both FK pruning and empty-delta skipping.
	planning.delta_status = DetectDeltaStatus(context, pw.view, leaves);

	// FK-aware pruning: detect insert-only PK leaves whose delta terms cancel algebraically.
	if (GetBooleanSetting(context, "ivm_fk_pruning", true)) {
		auto fk_relations = DetectFKRelations(context, leaves, pw.plan.get());
		if (!fk_relations.empty()) {
			planning.skip_bits = ComputeSkipBits(fk_relations, planning.delta_status.insert_only_mask);
		}
	}

	// Empty-delta skipping: skip terms where any table in the mask has zero delta rows.
	// A join with an empty input always produces zero rows.
	planning.skip_empty_enabled = GetBooleanSetting(context, "ivm_skip_empty_deltas", true);
	planning.empty_mask = planning.skip_empty_enabled ? planning.delta_status.empty_mask : 0;

	if (planning.skip_empty_enabled && !has_left_join) {
		Connection key_probe_con(*context.db);
		CollectJoinKeyProbeMetadata(pw, key_probe_con, leaves, planning);
	}
	return planning;
}

static bool HasEmptyJoinKeyDomain(Connection &con, const JoinDeltaPlanningContext &planning, uint64_t mask, size_t N) {
	if (!planning.skip_empty_enabled) {
		return false;
	}
	// Key-domain pruning is deliberately best-effort and only runs for inner joins.
	// For each delta-side leaf in the mask, we look at equi-join probes collected
	// from the original join tree. If a delta key has no possible match on the
	// other side, the whole term is empty. When both sides of a probe are in the
	// mask, only the lower-index side performs the check so we do not issue the
	// same delta-vs-delta EXISTS query twice.
	for (size_t i = 0; i < N; i++) {
		if (!(mask & (1ULL << i)) || planning.key_probes[i].empty() || planning.leaf_refs[i].last_update.empty()) {
			continue;
		}
		for (auto &probe : planning.key_probes[i]) {
			if (planning.leaf_refs[probe.other_leaf].last_update.empty()) {
				continue;
			}
			bool has_match;
			if (mask & (1ULL << probe.other_leaf)) {
				if (i > probe.other_leaf) {
					continue;
				}
				has_match = DeltaKeyHasDeltaMatch(con, planning.leaf_refs[i], probe.delta_column,
				                                  planning.leaf_refs[probe.other_leaf], probe.other_column);
			} else {
				has_match = DeltaKeyHasBaseMatch(con, planning.leaf_refs[i], probe.delta_column,
				                                 planning.leaf_refs[probe.other_leaf], probe.other_column);
			}
			if (!has_match) {
				return true;
			}
		}
	}
	return false;
}

static vector<JoinDeltaTermPlan> PlanJoinDeltaTerms(ClientContext &context, const JoinDeltaPlanningContext &planning,
                                                    size_t N, bool has_left_join) {
	vector<JoinDeltaTermPlan> term_plans;
	Connection key_probe_con(*context.db);
	// Pruning order is semantic, not cosmetic:
	//   1. FK pruning is an algebraic cancellation, independent of row counts.
	//   2. Empty-delta pruning is a cheaper structural emptiness check.
	//   3. Key-domain pruning can run SQL EXISTS probes, so it comes last.
	//
	// The first matching reason is recorded so debug output reflects why the
	// physical term was not built. Kept masks proceed unchanged to BuildJoinDeltaTerm.
	for (uint64_t mask = 1; mask < (1ULL << N); mask++) {
		JoinDeltaPruneReason prune_reason = JoinDeltaPruneReason::NONE;
		if (planning.skip_bits && (mask & planning.skip_bits)) {
			prune_reason = JoinDeltaPruneReason::FK_INSERT_ONLY_PK;
		} else if (planning.empty_mask && (mask & planning.empty_mask)) {
			prune_reason = JoinDeltaPruneReason::EMPTY_DELTA;
		} else if (!has_left_join && HasEmptyJoinKeyDomain(key_probe_con, planning, mask, N)) {
			prune_reason = JoinDeltaPruneReason::KEY_DOMAIN_EMPTY;
		}
		term_plans.push_back({mask, prune_reason});
	}
	return term_plans;
}

static unique_ptr<Expression> BuildCombinedMultiplicityExpression(Binder &binder, const LogicalType &mul_type,
                                                                  const vector<ColumnBinding> &mul_bindings) {
	if (mul_bindings.empty()) {
		throw InternalException("IvmJoinRule: join term has no delta multiplicity bindings");
	}
	// Combined multiplicity: (-1)^(k-1) * product(w_i), where k is the
	// number of delta-side leaves in the mask.
	//
	// The product is the ordinary Z-set bilinear join product. The extra Möbius
	// sign is specific to OpenIVM's refresh layout: by the time PRAGMA ivm()
	// runs, base scans read the current table state R_now = R_old + delta R
	// because DML has already been applied to source tables and mirrored into
	// delta tables. Expanding
	//
	//   delta(R join S) = (R_old + dR) join (S_old + dS) - R_old join S_old
	//
	// in terms of current base scans means multi-delta terms are overcounted
	// unless masks with even k are negated:
	//
	//   k=1:  dR join S_now                         positive
	//   k=2:  dR join dS                            negative
	//   k=3:  dR join dS join dT                    positive
	//   k=4:  dR join dS join dT join dU            negative
	//
	// This is algebraically equivalent to the old-state/all-positive DBSP join
	// formula, but it matches the physical state OpenIVM can scan cheaply.
	FunctionBinder fbinder(binder);
	unique_ptr<Expression> product = make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[0]);
	for (size_t i = 1; i < mul_bindings.size(); i++) {
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(product));
		args.push_back(make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[i]));
		ErrorData err;
		product = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), err, true /* is_operator */);
		if (!product) {
			throw InternalException("IvmJoinRule: failed to bind '*' for combined multiplicity: %s", err.RawMessage());
		}
	}
	// Apply Möbius sign: (-1)^(k-1). Only flip when k is even.
	if (mul_bindings.size() % 2 == 0) {
		vector<unique_ptr<Expression>> args;
		args.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(-1)));
		args.push_back(std::move(product));
		ErrorData err;
		product = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), err, true);
		if (!product) {
			throw InternalException("IvmJoinRule: failed to bind '*' for Möbius sign: %s", err.RawMessage());
		}
	}
	return product;
}

static unique_ptr<LogicalOperator> BuildJoinDeltaTerm(PlanWrapper &pw, ClientContext &context, Binder &binder,
                                                      const vector<JoinLeafInfo> &leaves, bool has_left_join,
                                                      const JoinDeltaTermPlan &term_plan) {
	auto term = pw.plan->Copy(context);
	auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
	term = std::move(renumbered.op);
	LogicalOperator *term_root = term.get();
	vector<ColumnBinding> mul_bindings;

	// LEFT JOIN delta rule (per-LJ): demote each LJ to INNER iff its null-supplying
	// subtree contains a delta-marked leaf in this mask.
	//
	// Why per-LJ rather than whole-term: in a chain like (base LJ d1) LJ d2
	// with mask {d2}, demoting all left joins would collapse (base LJ d1) into
	// (base IJ d1), dropping base rows unmatched in d1. Those rows are exactly
	// the existing NULL-padded MV rows that need to flow through so the upsert
	// layer can delete/reinsert affected keys. Per-LJ demotion keeps unrelated
	// outer joins intact while still converting the join that sees the delta on
	// its null-supplying side into the matched part needed by the delta term.
	if (has_left_join) {
		DemoteLeftJoinsForMask(term.get(), leaves, term_plan.mask);
	}

	// Replace delta leaves.
	for (size_t i = 0; i < leaves.size(); i++) {
		if (!(term_plan.mask & (1ULL << i))) {
			continue;
		}
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

	term->ResolveOperatorTypes();

	auto term_bindings = term->GetColumnBindings();
	auto term_types = term->types;
	vector<unique_ptr<Expression>> proj_exprs;

	// Filter out multiplicity columns (O(1) lookup via hash set).
	unordered_set<uint64_t> mul_set;
	CollectExistingMultiplicityBindings(term.get(), mul_set);
	for (auto &mb : mul_bindings) {
		mul_set.insert(BindingKey(mb));
	}
	for (idx_t i = 0; i < term_bindings.size(); i++) {
		if (!mul_set.count(BindingKey(term_bindings[i]))) {
			proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(term_types[i], term_bindings[i]));
		}
	}

	// Combined multiplicity: (-1)^(k-1) * ∏ w_i where k = |mask|.
	// Z-set bilinear product times a Möbius inclusion-exclusion sign.
	proj_exprs.push_back(BuildCombinedMultiplicityExpression(binder, pw.mul_type, mul_bindings));

	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
	projection->children.push_back(std::move(term));
	projection->ResolveOperatorTypes();
	return std::move(projection);
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
	auto planning = BuildJoinDeltaPlanningContext(pw, context, leaves, has_left_join);
	auto term_plans = PlanJoinDeltaTerms(context, planning, N, has_left_join);

	uint64_t pruned_count = 0;
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Building inclusion-exclusion terms (%lu total, skip_bits=%lu, empty_mask=%lu)\n",
	                    (unsigned long)planning.total_terms, (unsigned long)planning.skip_bits,
	                    (unsigned long)planning.empty_mask);
	for (auto &term_plan : term_plans) {
		if (!term_plan.IsKept()) {
			pruned_count++;
			OPENIVM_DEBUG_PRINT("[IvmJoinRule] Skipped term mask=%lu (%s)\n", (unsigned long)term_plan.mask,
			                    JoinDeltaPruneReasonName(term_plan.prune_reason));
			continue;
		}
		terms.push_back(BuildJoinDeltaTerm(pw, context, binder, leaves, has_left_join, term_plan));
	}
	if (pruned_count > 0) {
		OPENIVM_DEBUG_PRINT("[IvmJoinRule] Join term planning: %lu/%lu terms pruned, %lu remaining\n",
		                    (unsigned long)pruned_count, (unsigned long)planning.total_terms,
		                    (unsigned long)terms.size());
	}
	return terms;
}

// ============================================================================
// AssembleUnionAll: combine terms with UNION ALL + clean projection
// ============================================================================
static unique_ptr<LogicalOperator> AssembleUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                    const vector<LogicalType> &types, Binder &binder) {
	if (terms.empty()) {
		vector<ColumnBinding> bindings;
		auto table_index = binder.GenerateTableIndex();
		for (idx_t i = 0; i < types.size(); i++) {
			bindings.emplace_back(table_index, i);
		}
		auto empty = make_uniq<LogicalEmptyResult>(types, std::move(bindings));
		empty->ResolveOperatorTypes();
		return std::move(empty);
	}
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
	auto spec = GetDeltaOperatorSpec(pw.plan->type);
	D_ASSERT(spec.rewrite_kind == DeltaRewriteKind::JOIN);
	D_ASSERT(spec.linearity == Linearity::BILINEAR);
	pw.plan->ResolveOperatorTypes();
	const vector<ColumnBinding> all_original_bindings = pw.plan->GetColumnBindings();
	unordered_set<uint64_t> existing_mul_set;
	CollectExistingMultiplicityBindings(pw.plan.get(), existing_mul_set);
	vector<ColumnBinding> original_bindings;
	vector<LogicalType> output_types;
	FilterInternalMultiplicityColumns(all_original_bindings, pw.plan->types, existing_mul_set, original_bindings,
	                                  output_types);

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
	auto types = output_types;
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
			auto *get = GetLeafScan(leaves[i]);
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
