#include "compile_facts.hpp"
#include "delta/operators/join.hpp"
#include "delta/operators/ducklake_join.hpp"
#include "delta/operators/join_key_probe.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "upsert/refresh_index_regen.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

static idx_t CountBits(uint64_t value) {
	idx_t count = 0;
	while (value) {
		count += value & 1ULL;
		value >>= 1ULL;
	}
	return count;
}

struct JoinColumnRef {
	size_t leaf_index;
	LogicalGet *get;
	string table_name;
	string delta_name;
	string column_name;
	string last_update;
};

static string QualifyColumn(const string &alias, const string &column_name) {
	return alias + "." + SqlUtils::QuoteIdentifier(column_name);
}

static string BuildPushedFilterSQL(LogicalGet &get, const string &alias) {
	string filters;
	for (auto &entry : get.table_filters.filters) {
		if (entry.second->filter_type == TableFilterType::OPTIONAL_FILTER) {
			continue;
		}
		auto col_name = get.GetColumnName(ColumnIndex(entry.first));
		if (!filters.empty()) {
			filters += " AND ";
		}
		filters += "(" + entry.second->ToString(QualifyColumn(alias, col_name)) + ")";
	}
	return filters;
}

static string AppendFilterSQL(const string &predicate) {
	return predicate.empty() ? string() : " AND " + predicate;
}

static bool DeltaKeyHasBaseMatch(Connection &con, const JoinColumnRef &delta_ref, const string &delta_column,
                                 const JoinColumnRef &other_ref, const string &other_column) {
	string delta_filter = delta_ref.get ? BuildPushedFilterSQL(*delta_ref.get, "openivm_delta") : string();
	string other_filter = other_ref.get ? BuildPushedFilterSQL(*other_ref.get, "openivm_other") : string();
	string sql = "SELECT EXISTS(SELECT 1 FROM (SELECT " + QualifyColumn("openivm_delta", delta_column) +
	             " AS openivm_key FROM " + SqlUtils::QuoteIdentifier(delta_ref.delta_name) + " openivm_delta WHERE " +
	             QualifyColumn("openivm_delta", openivm::TIMESTAMP_COL) + " >= '" +
	             SqlUtils::EscapeValue(delta_ref.last_update) + "'::TIMESTAMP" + AppendFilterSQL(delta_filter) +
	             ") openivm_delta_keys JOIN " + SqlUtils::QuoteIdentifier(other_ref.table_name) +
	             " openivm_other ON openivm_delta_keys.openivm_key = " + QualifyColumn("openivm_other", other_column) +
	             (other_filter.empty() ? string() : " WHERE " + other_filter) + " LIMIT 1)";
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Key probe SQL: %s\n", sql.c_str());
	auto result = con.Query(sql);
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		OPENIVM_DEBUG_PRINT("[DeltaJoin] Could not probe key-domain intersection: %s\n",
		                    result->HasError() ? result->GetError().c_str() : "no result");
		return true;
	}
	return result->GetValue(0, 0).GetValue<bool>();
}

static bool DeltaKeyHasDeltaMatch(Connection &con, const JoinColumnRef &left_ref, const string &left_column,
                                  const JoinColumnRef &right_ref, const string &right_column) {
	string left_filter = left_ref.get ? BuildPushedFilterSQL(*left_ref.get, "openivm_left_delta") : string();
	string right_filter = right_ref.get ? BuildPushedFilterSQL(*right_ref.get, "openivm_right_delta") : string();
	string sql = "SELECT EXISTS(SELECT 1 FROM (SELECT " + QualifyColumn("openivm_left_delta", left_column) +
	             " AS openivm_key FROM " + SqlUtils::QuoteIdentifier(left_ref.delta_name) +
	             " openivm_left_delta WHERE " + QualifyColumn("openivm_left_delta", openivm::TIMESTAMP_COL) + " >= '" +
	             SqlUtils::EscapeValue(left_ref.last_update) + "'::TIMESTAMP" + AppendFilterSQL(left_filter) +
	             ") openivm_left_delta_keys JOIN (SELECT " + QualifyColumn("openivm_right_delta", right_column) +
	             " AS openivm_key FROM " + SqlUtils::QuoteIdentifier(right_ref.delta_name) +
	             " openivm_right_delta WHERE " + QualifyColumn("openivm_right_delta", openivm::TIMESTAMP_COL) +
	             " >= '" + SqlUtils::EscapeValue(right_ref.last_update) + "'::TIMESTAMP" +
	             AppendFilterSQL(right_filter) +
	             ") openivm_right_delta_keys ON openivm_left_delta_keys.openivm_key = "
	             "openivm_right_delta_keys.openivm_key LIMIT 1)";
	auto result = con.Query(sql);
	if (result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		OPENIVM_DEBUG_PRINT("[DeltaJoin] Could not probe delta key-domain intersection: %s\n",
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
		if (!ref.bound_columns.empty() && ref.bound_columns.back() == openivm::MULTIPLICITY_COL) {
			auto bindings = node->GetColumnBindings();
			if (!bindings.empty()) {
				mul_set.insert(DeltaJoinBindingKey(bindings.back()));
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
		if (mul_set.count(DeltaJoinBindingKey(bindings[i]))) {
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
		idx_t count = bindings.size();
		for (idx_t col_idx = 0; col_idx < count; col_idx++) {
			if (DeltaJoinBindingKey(bindings[col_idx]) != DeltaJoinBindingKey(binding)) {
				continue;
			}
			idx_t column_id_idx = col_idx;
			if (!get->projection_ids.empty()) {
				if (col_idx >= get->projection_ids.size()) {
					return false;
				}
				column_id_idx = get->projection_ids[col_idx];
			}
			if (column_id_idx >= column_ids.size() || column_ids[column_id_idx].IsVirtualColumn()) {
				return false;
			}
			table_name = get->GetTable().get()->name;
			column_name = get->GetColumnName(column_ids[column_id_idx]);
			return true;
		}
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_PROJECTION && !node->children.empty()) {
		auto &projection = node->Cast<LogicalProjection>();
		auto bindings = node->GetColumnBindings();
		idx_t count = std::min<idx_t>(bindings.size(), projection.expressions.size());
		for (idx_t expr_idx = 0; expr_idx < count; expr_idx++) {
			if (DeltaJoinBindingKey(bindings[expr_idx]) != DeltaJoinBindingKey(binding)) {
				continue;
			}
			ColumnBinding child_binding;
			if (!TryGetDeltaJoinColumnRef(*projection.expressions[expr_idx], child_binding)) {
				return false;
			}
			return ResolveLeafBindingToBaseColumn(node->children[0].get(), child_binding, table_name, column_name);
		}
		return false;
	}
	if (node->children.size() == 1) {
		auto bindings = node->GetColumnBindings();
		auto child_bindings = node->children[0]->GetColumnBindings();
		idx_t count = std::min<idx_t>(bindings.size(), child_bindings.size());
		for (idx_t col_idx = 0; col_idx < count; col_idx++) {
			if (DeltaJoinBindingKey(bindings[col_idx]) == DeltaJoinBindingKey(binding)) {
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

// True if any LOGICAL_GET in the subtree is backed by a real catalog table (i.e. a mutable source).
static bool SubtreeHasMutableBaseGet(LogicalOperator *node) {
	if (!node) {
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_GET) {
		auto *get = dynamic_cast<LogicalGet *>(node);
		return get && get->GetTable().get() != nullptr;
	}
	for (auto &child : node->children) {
		if (SubtreeHasMutableBaseGet(child.get())) {
			return true;
		}
	}
	return false;
}

static bool SubtreeContainsDelim(LogicalOperator *node) {
	if (!node) {
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_DELIM_JOIN ||
	    node->type == LogicalOperatorType::LOGICAL_DEPENDENT_JOIN ||
	    node->type == LogicalOperatorType::LOGICAL_DELIM_GET) {
		return true;
	}
	for (auto &child : node->children) {
		if (SubtreeContainsDelim(child.get())) {
			return true;
		}
	}
	return false;
}

// A join leaf that contains a DELIM_JOIN/DELIM_GET (a correlated keyed expansion such as
// CROSS JOIN LATERAL generate_series(1, w.w_id)) but has NO mutable base table under it is
// base-invariant: its own delta is empty. The row-level delta flows through the correlated source
// (the shared base/CTE) on the *other* side of the enclosing join, so inclusion-exclusion need only
// keep this side full — yielding the correct ΔA⋈B term and avoiding the "no mutable base leaves"
// dead end inside CompileDelimJoinDelta. The `SubtreeContainsDelim` guard excludes a plain CTE_REF
// leaf (which carries the actual delta and must NOT be treated as constant). The leaf root may be a
// projection wrapping the delim join, so both checks recurse.
static bool IsBaseInvariantDelimLeaf(LogicalOperator *node) {
	return node && SubtreeContainsDelim(node) && !SubtreeHasMutableBaseGet(node);
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
// are tracked separately via openivm_match_count); failing to demote there would
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

void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const vector<size_t> &path,
                               const ColumnBinding &mul_binding, bool include_delim_parents) {
	if (path.empty()) {
		return;
	}
	size_t child_side = path.back();
	unique_ptr<LogicalOperator> *parent = &term;
	for (size_t s = 0; s + 1 < path.size(); s++) {
		parent = &((*parent)->children[path[s]]);
	}
	auto *join = dynamic_cast<LogicalComparisonJoin *>((*parent).get());
	if (!join) {
		return;
	}
	// The inclusion-exclusion path only patches plain comparison joins; the delim-join path also patches
	// delim/dependent joins (also LogicalComparisonJoin subclasses).
	if (!include_delim_parents && (*parent)->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		return;
	}
	auto &proj_map = (child_side == 0) ? join->left_projection_map : join->right_projection_map;
	// proj_map is only populated when column_lifetime prunes this join side. When it is, the pruned set
	// drops the multiplicity column, so re-add it. Locate its real index by searching the (already
	// delta-replaced) child's bindings — the leaf's delta rewrite can change arity (e.g. a wrapped
	// projection/filter over a scan), so the multiplicity is not necessarily at the original leaf size.
	if (proj_map.empty()) {
		return;
	}
	auto child_binds = join->children[child_side]->GetColumnBindings();
	idx_t mul_idx = child_binds.size();
	for (idx_t i = child_binds.size(); i-- > 0;) {
		if (child_binds[i] == mul_binding) {
			mul_idx = i;
			break;
		}
	}
	if (mul_idx == child_binds.size()) {
		throw InternalException("IVM: join child does not expose the multiplicity column for projection_map pruning");
	}
	proj_map.push_back(mul_idx);
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Added mul col %lu to %s proj_map\n", (unsigned long)mul_idx,
	                    child_side == 0 ? "left" : "right");
}

// ============================================================================
// FK-aware term pruning: detect which inclusion-exclusion terms are redundant
// ============================================================================

/// Delta status for join leaves: which have insert-only deltas, which are completely empty.
struct DeltaStatus {
	uint64_t insert_only_mask; // bit i=1: leaf i has no delete rows (insert-only or empty)
	uint64_t empty_mask;       // bit i=1: leaf i has zero pending delta rows
	uint64_t constant_mask;    // bit i=1: leaf has no mutable source table
	uint64_t tiny_mask;        // bit i=1: leaf has a tiny non-empty delta
};

/// For each leaf, detect delta status in a single query per table.
/// Returns both insert_only_mask (no deletes) and empty_mask (no rows at all).
static DeltaStatus DetectDeltaStatus(ClientContext &context, const string &view_name,
                                     const vector<JoinLeafInfo> &leaves) {
	DeltaStatus status = {0, 0, 0, 0};
	Connection con(*context.db);
	con.SetAutoCommit(false);
	RefreshMetadata metadata(con);

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
				status.constant_mask |= (1ULL << i);
				OPENIVM_DEBUG_PRINT("[DeltaJoin] Leaf %zu (constant values) has empty delta\n", i);
			} else if (IsBaseInvariantDelimLeaf(leaves[i].node)) {
				// Correlated keyed expansion (e.g. CROSS JOIN LATERAL generate_series(1, w.w_id)) with no
				// mutable base under the delim join — its delta is empty; the row-level delta flows through
				// the correlated source on the other side of this join. Keep it full in every term.
				status.empty_mask |= (1ULL << i);
				status.insert_only_mask |= (1ULL << i);
				status.constant_mask |= (1ULL << i);
				OPENIVM_DEBUG_PRINT("[DeltaJoin] Leaf %zu (base-invariant delim expansion) has empty delta\n", i);
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
			status.constant_mask |= (1ULL << i);
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Leaf %zu (table function '%s') has empty delta\n", i,
			                    get->function.name.c_str());
			continue;
		}
		string delta_name = SqlUtils::DeltaName(table_ref.get()->name);
		// Get last_update timestamp for this view+table pair (empty == no tracked delta row → skip).
		string last_update = metadata.GetLastUpdate(view_name, delta_name);
		if (last_update.empty()) {
			continue;
		}

		// Single query: get pending delta count, delete count, and current base
		// cardinality. The base count lets us define "tiny" as <= max(8 rows,
		// 5% of the source table), avoiding both a hard-coded absolute-only
		// threshold and silly behavior on very small tables.
		auto result =
		    con.Query("SELECT "
		              "(SELECT COUNT(*) FROM " +
		              SqlUtils::QuoteIdentifier(delta_name) + " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" +
		              SqlUtils::EscapeValue(last_update) +
		              "'::TIMESTAMP), "
		              "(SELECT COUNT(*) FROM " +
		              SqlUtils::QuoteIdentifier(delta_name) + " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" +
		              SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP AND " + string(openivm::MULTIPLICITY_COL) +
		              " < 0), "
		              "(SELECT COUNT(*) FROM " +
		              SqlUtils::QuoteIdentifier(table_ref.get()->name) + ")");
		if (result->HasError()) {
			continue;
		}
		int64_t total_count = result->GetValue(0, 0).GetValue<int64_t>();
		int64_t delete_count = result->GetValue(1, 0).GetValue<int64_t>();
		int64_t base_count = result->GetValue(2, 0).GetValue<int64_t>();

		if (total_count == 0) {
			status.empty_mask |= (1ULL << i);
			status.insert_only_mask |= (1ULL << i); // empty is trivially insert-only
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Leaf %zu (%s) has empty delta\n", i, table_ref.get()->name.c_str());
		} else {
			int64_t tiny_limit = std::max<int64_t>(8, (base_count + 19) / 20);
			if (total_count <= tiny_limit) {
				status.tiny_mask |= (1ULL << i);
			}
			if (delete_count == 0) {
				status.insert_only_mask |= (1ULL << i);
				OPENIVM_DEBUG_PRINT("[DeltaJoin] Leaf %zu (%s) has insert-only delta\n", i,
				                    table_ref.get()->name.c_str());
			}
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
			OPENIVM_DEBUG_PRINT("[DeltaJoin] FK relation: leaf %zu (%s) -> leaf %zu (%s)\n", i,
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

unique_ptr<Expression> BuildMultiplicityProduct(Binder &binder, const LogicalType &mul_type,
                                                const vector<ColumnBinding> &mul_bindings) {
	FunctionBinder fbinder(binder);
	unique_ptr<Expression> product = make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[0]);
	for (size_t i = 1; i < mul_bindings.size(); i++) {
		vector<unique_ptr<Expression>> args;
		args.push_back(std::move(product));
		args.push_back(make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[i]));
		ErrorData err;
		product = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), err, true /* is_operator */);
		if (!product) {
			throw InternalException("DeltaJoin: failed to bind '*' for combined multiplicity: %s", err.RawMessage());
		}
	}
	// Apply Möbius inclusion-exclusion sign: (-1)^(k-1). Only flip when k is even.
	if (mul_bindings.size() % 2 == 0) {
		vector<unique_ptr<Expression>> args;
		args.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(-1)));
		args.push_back(std::move(product));
		ErrorData err;
		product = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), err, true);
		if (!product) {
			throw InternalException("DeltaJoin: failed to bind '*' for Möbius sign: %s", err.RawMessage());
		}
	}
	return product;
}

// ============================================================================
// BuildInclusionExclusionTerms: create 2^N - 1 delta terms
// ============================================================================
static vector<unique_ptr<LogicalOperator>> BuildInclusionExclusionTerms(DeltaOperatorInput input,
                                                                        ClientContext &context, Binder &binder,
                                                                        const vector<JoinLeafInfo> &leaves,
                                                                        bool has_left_join) {
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	// Detect delta status for all leaves (single query per table: total + delete count).
	// Used by both FK pruning and empty-delta skipping.
	DeltaStatus delta_status = DetectDeltaStatus(context, input.context.view, leaves);
	uint64_t total_terms = (1ULL << N) - 1;
	uint64_t non_empty_mask = total_terms & ~delta_status.empty_mask & ~delta_status.constant_mask;
	idx_t non_empty_leaf_count = CountBits(non_empty_mask);

	// FK-aware pruning: detect insert-only PK leaves whose delta terms cancel algebraically.
	bool fk_pruning_enabled = SqlUtils::GetBoolSetting(context, "openivm_fk_pruning", true);
	uint64_t skip_bits = 0;
	// FK pruning pays for catalog constraint inspection. When every leaf changed
	// in a small 2/3-way join, the remaining inclusion-exclusion space is tiny and
	// the flag benchmark shows the inspection cost can dominate. Keep it for the
	// main win case: one-sided PK/dimension changes.
	bool fk_pruning_worthwhile = non_empty_leaf_count == 1;
	if (fk_pruning_enabled && fk_pruning_worthwhile) {
		auto fk_relations = DetectFKRelations(context, leaves, input.plan.get());
		if (!fk_relations.empty()) {
			skip_bits = ComputeSkipBits(fk_relations, delta_status.insert_only_mask);
		}
	}

	// Empty-delta skipping: skip terms where any table in the mask has zero delta rows.
	// A join with an empty input always produces zero rows.
	bool skip_empty_enabled = SqlUtils::GetBoolSetting(context, "openivm_skip_empty_deltas", true);
	bool compile_only = openivm::CompileFactsContextSlot::Get(context).compile_only;
	uint64_t empty_mask = 0;
	if (skip_empty_enabled) {
		empty_mask = compile_only ? (delta_status.empty_mask & delta_status.constant_mask) : delta_status.empty_mask;
	}

	Connection key_probe_con(*context.db);
	RefreshMetadata key_probe_metadata(key_probe_con);
	vector<JoinColumnRef> leaf_refs(N);
	vector<vector<DeltaJoinKeyProbe>> key_probes(N);
	// Key-domain pruning can erase the last remaining term when exactly one input
	// changed and its delta keys cannot match the unchanged side. That is the
	// important performance case covered by mv_inner_join. When multiple leaves
	// changed in a small join with many pending delta rows, these probes are extra
	// EXISTS joins on top of work we will still have to do, and the flag benchmark
	// shows that overhead can dominate. Keep probing for single-source changes and
	// for tiny multi-source changes where the probe is cheap.
	bool all_non_empty_deltas_are_tiny = non_empty_mask && ((non_empty_mask & ~delta_status.tiny_mask) == 0);
	bool key_domain_probe_enabled =
	    skip_empty_enabled && !has_left_join && (non_empty_leaf_count == 1 || all_non_empty_deltas_are_tiny);
	if (key_domain_probe_enabled) {
		unordered_map<uint64_t, JoinColumnRef> column_refs;
		for (size_t i = 0; i < N; i++) {
			LogicalGet *get = GetLeafScan(leaves[i]);
			if (!get) {
				continue;
			}
			auto table_ref = get->GetTable();
			if (table_ref.get() == nullptr) {
				continue;
			}
			string table_name = table_ref.get()->name;
			string delta_name = SqlUtils::DeltaName(table_name);
			string last_update = key_probe_metadata.GetLastUpdate(input.context.view, delta_name);
			if (last_update.empty()) {
				continue;
			}
			leaf_refs[i].leaf_index = i;
			leaf_refs[i].get = get;
			leaf_refs[i].table_name = table_name;
			leaf_refs[i].delta_name = delta_name;
			leaf_refs[i].last_update = last_update;

			auto leaf_bindings = leaves[i].node->GetColumnBindings();
			for (auto &binding : leaf_bindings) {
				string resolved_table;
				string resolved_column;
				if (!ResolveLeafBindingToBaseColumn(leaves[i].node, binding, resolved_table, resolved_column) ||
				    !StringUtil::CIEquals(resolved_table, table_name)) {
					continue;
				}
				leaf_refs[i].column_name = resolved_column;
				OPENIVM_DEBUG_PRINT("[DeltaJoin] Column ref leaf=%zu binding=%s -> %s.%s\n", i,
				                    binding.ToString().c_str(), resolved_table.c_str(), resolved_column.c_str());
				column_refs[DeltaJoinBindingKey(binding)] = {
				    i, get, table_name, delta_name, resolved_column, leaf_refs[i].last_update};
			}
		}
		CollectDeltaJoinKeyProbes(input.plan.get(), column_refs, key_probes, "DeltaJoin");
	}

	uint64_t pruned_count = 0;
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Building inclusion-exclusion terms (%lu total, skip_bits=%lu, empty_mask=%lu)\n",
	                    (unsigned long)total_terms, (unsigned long)skip_bits, (unsigned long)empty_mask);
	for (uint64_t mask = 1; mask < (1ULL << N); mask++) {
		// FK pruning: skip any term whose mask overlaps with insert-only PK leaves.
		// All such terms cancel algebraically via XOR (see ComputeSkipBits).
		if (skip_bits && (mask & skip_bits)) {
			pruned_count++;
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Pruned term mask=%lu (FK insert-only PK)\n", (unsigned long)mask);
			continue;
		}
		// Empty-delta skipping: if any table in the mask has zero delta rows,
		// the join term produces zero rows (join with empty input = empty).
		if (empty_mask && (mask & empty_mask)) {
			pruned_count++;
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Skipped term mask=%lu (empty delta)\n", (unsigned long)mask);
			continue;
		}
		bool key_domain_empty = false;
		if (key_domain_probe_enabled) {
			for (size_t i = 0; i < N && !key_domain_empty; i++) {
				if (!(mask & (1ULL << i)) || key_probes[i].empty() || leaf_refs[i].last_update.empty()) {
					continue;
				}
				for (auto &probe : key_probes[i]) {
					if (leaf_refs[probe.other_leaf].last_update.empty()) {
						continue;
					}
					bool has_match;
					if (mask & (1ULL << probe.other_leaf)) {
						if (i > probe.other_leaf) {
							continue;
						}
						has_match = DeltaKeyHasDeltaMatch(key_probe_con, leaf_refs[i], probe.delta_column,
						                                  leaf_refs[probe.other_leaf], probe.other_column);
					} else {
						has_match = DeltaKeyHasBaseMatch(key_probe_con, leaf_refs[i], probe.delta_column,
						                                 leaf_refs[probe.other_leaf], probe.other_column);
					}
					if (!has_match) {
						key_domain_empty = true;
						break;
					}
				}
			}
		}
		if (key_domain_empty) {
			pruned_count++;
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Skipped term mask=%lu (delta key-domain empty)\n", (unsigned long)mask);
			continue;
		}
		auto term = input.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);
		LogicalOperator *term_root = term.get();
		vector<ColumnBinding> mul_bindings;

		// LEFT JOIN delta rule (per-LJ): demote each LJ to INNER iff its right
		// subtree contains a delta-marked leaf in this mask.
		//
		// Why per-LJ rather than whole-term: in a chain like (base LJ d1) LJ d2
		// with mask {Δd2}, demoting *all* LJs collapses (base LJ d1) into
		// (base IJ d1), which drops base rows unmatched in d1 — the very rows
		// whose existing NULL-padded MV entries need to be replaced when Δd2
		// brings in a match. Per-LJ demote keeps the inner LJ intact so those
		// keys still flow through and reach delta_mv (and the partial-recompute
		// DELETE+re-INSERT in the upsert layer fixes them up correctly).
		//
		// This subsumes both original cases (right-only delta, both-sides delta)
		// without over-demoting in chained-LJ shapes.
		if (has_left_join) {
			DemoteLeftJoinsForMask(term.get(), leaves, mask);
		}

		// Replace delta leaves
		for (size_t i = 0; i < N; i++) {
			if (mask & (1ULL << i)) {
				if (leaves[i].get) {
					DeltaGetResult delta_i = CreateDeltaGetNode(context, binder, leaves[i].get, input.context.view);
					mul_bindings.push_back(delta_i.mul_binding);
					GetNodeAtPath(term, leaves[i].path) = std::move(delta_i.node);
				} else {
					auto &subtree_ref = GetNodeAtPath(term, leaves[i].path);
					auto rewritten = input.CompileCopiedSubtree(subtree_ref, term_root);
					mul_bindings.push_back(rewritten.mul_binding);
					subtree_ref = std::move(rewritten.op);
				}
				UpdateParentProjectionMap(term, leaves[i].path, mul_bindings.back(), /*include_delim_parents=*/false);
			}
		}

		term->ResolveOperatorTypes();

		// Build projection: original columns + combined multiplicity
		auto term_bindings = term->GetColumnBindings();
		auto term_types = term->types;
		vector<unique_ptr<Expression>> proj_exprs;

		// Filter out multiplicity columns (O(1) lookup via hash set)
		unordered_set<uint64_t> mul_set;
		CollectExistingMultiplicityBindings(term.get(), mul_set);
		for (auto &mb : mul_bindings) {
			mul_set.insert(DeltaJoinBindingKey(mb));
		}
		for (idx_t i = 0; i < term_bindings.size(); i++) {
			if (!mul_set.count(DeltaJoinBindingKey(term_bindings[i]))) {
				proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(term_types[i], term_bindings[i]));
			}
		}

		// Combined multiplicity: (-1)^(k-1) * ∏ w_i where k = |mask|.
		// Z-set bilinear product times a Möbius inclusion-exclusion sign.
		//
		// The IE sign is required because OpenIVM's "current base" scan reads
		// R_now = R_old + ΔR (deltas have already been applied to the source by
		// the RefreshInsertRule at DML time). Expanding
		//   Δ(R⋈S) = (R_old+ΔR)⋈(S_old+ΔS) − R_old⋈S_old
		// gives an inclusion-exclusion sum: terms with k delta-side leaves carry
		// sign (-1)^(k-1) so the overcounting from "current includes pending"
		// cancels exactly. This is NOT the textbook DBSP delta-join formula
		// (which uses old bases and gives all-positive terms) — it is the
		// algebraically equivalent Möbius form for OpenIVM's data layout.
		//
		// Equivalence to the previous BOOLEAN XOR chain (true=+1, false=-1):
		//   k=1: w_1                 = w_1                       (no sign flip)
		//   k=2: -w_1·w_2            = NOT(w_1 == w_2)            (XOR true,true=false=-1)
		//   k=3: w_1·w_2·w_3         (no sign flip)
		//   k=4: -w_1·w_2·w_3·w_4    (sign flip)
		// — verified algebraically on all sign combinations.
		proj_exprs.push_back(BuildMultiplicityProduct(binder, input.mul_type, mul_bindings));

		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));
	}
	if (pruned_count > 0) {
		OPENIVM_DEBUG_PRINT("[DeltaJoin] FK pruning: %lu/%lu terms pruned, %lu remaining\n",
		                    (unsigned long)pruned_count, (unsigned long)total_terms, (unsigned long)terms.size());
	}
	return terms;
}

DeltaPlanFragment CompileJoinDelta(DeltaOperatorInput input) {
	ClientContext &context = input.context.input.context;
	Binder &binder = input.context.input.optimizer.binder;
	input.plan->ResolveOperatorTypes();
	const vector<ColumnBinding> all_original_bindings = input.plan->GetColumnBindings();
	unordered_set<uint64_t> existing_mul_set;
	CollectExistingMultiplicityBindings(input.plan.get(), existing_mul_set);
	vector<ColumnBinding> original_bindings;
	vector<LogicalType> output_types;
	FilterInternalMultiplicityColumns(all_original_bindings, input.plan->types, existing_mul_set, original_bindings,
	                                  output_types);

	// 1. Verify + collect
	bool has_left_join = VerifyJoinTypes(input.plan.get());
	vector<JoinLeafInfo> leaves;
	CollectJoinLeaves(input.plan.get(), {}, leaves);
	size_t N = leaves.size();
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Rewriting JOIN node, %zu leaves found\n", N);

	if (N == 0) {
		throw InternalException("DeltaJoin: no leaves found in join tree");
	}
	if (N > openivm::MAX_JOIN_TABLES) {
		throw NotImplementedException("Inclusion-exclusion IVM not supported for joins with more than 16 tables");
	}

	// 2. Output types
	auto types = output_types;
	D_ASSERT(types.size() == original_bindings.size());
	types.emplace_back(input.mul_type);

	// 3. Build terms — use DuckLake N-term path when all leaves are DuckLake scans
	// Check if all leaves are DuckLake scans AND N-term telescoping is enabled.
	bool all_ducklake = true;
	if (!SqlUtils::GetBoolSetting(context, "openivm_ducklake_nterm", true)) {
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
	LogDeltaOperatorStrategy(input, all_ducklake ? DeltaOperatorStrategy::JOIN_DUCKLAKE_N_TERM
	                                             : DeltaOperatorStrategy::JOIN_INCLUSION_EXCLUSION);

	auto terms = all_ducklake ? BuildDuckLakeJoinTerms(input, context, binder, leaves, has_left_join)
	                          : BuildInclusionExclusionTerms(input, context, binder, leaves, has_left_join);

	// 4. UNION ALL
	auto result = AssembleJoinUnionAll(terms, types, binder);

	// 5. Rebind parent references
	ColumnBinding new_mul_binding = ReplaceJoinOutputBindings(original_bindings, result, *input.root);

	input.plan = std::move(result);
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Done, %zu terms unioned, mul_binding: table=%lu col=%lu\n", terms.size(),
	                    (unsigned long)new_mul_binding.table_index, (unsigned long)new_mul_binding.column_index);
	return {std::move(input.plan), new_mul_binding};
}

} // namespace duckdb
