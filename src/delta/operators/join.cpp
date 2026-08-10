#include "compile_facts.hpp"
#include "delta/operators/join.hpp"
#include "delta/operators/ducklake_join.hpp"
#include "delta/operators/join_key_probe.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/plan_rewrite_internal.hpp"
#include "core/sql_utils.hpp"
#include "upsert/refresh_index_regen.hpp"
#include "match/constraint_cache.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_join.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

#include <algorithm>

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

// Resolve a binding (possibly on a projection wrapping a leaf's Get, e.g. a "materialized_for_join"
// passthrough) down to its position in target_get's own column bindings. Mirrors
// ResolveLeafBindingToBaseColumn's recursion but returns a POSITION instead of a table/column name.
static bool ResolveKeyToGetPosition(LogicalOperator *node, const ColumnBinding &binding, LogicalGet *target_get,
                                    idx_t &out_pos) {
	if (!node) {
		return false;
	}
	if (node == target_get) {
		auto bindings = node->GetColumnBindings();
		for (idx_t i = 0; i < bindings.size(); i++) {
			if (bindings[i] == binding) {
				out_pos = i;
				return true;
			}
		}
		return false;
	}
	if (node->type == LogicalOperatorType::LOGICAL_PROJECTION && !node->children.empty()) {
		auto &projection = node->Cast<LogicalProjection>();
		auto bindings = node->GetColumnBindings();
		idx_t count = std::min<idx_t>(bindings.size(), projection.expressions.size());
		for (idx_t i = 0; i < count; i++) {
			if (bindings[i] != binding) {
				continue;
			}
			ColumnBinding child_binding;
			if (!TryGetDeltaJoinColumnRef(*projection.expressions[i], child_binding)) {
				return false;
			}
			return ResolveKeyToGetPosition(node->children[0].get(), child_binding, target_get, out_pos);
		}
		return false;
	}
	if (node->children.size() == 1) {
		auto bindings = node->GetColumnBindings();
		auto child_bindings = node->children[0]->GetColumnBindings();
		idx_t count = std::min<idx_t>(bindings.size(), child_bindings.size());
		for (idx_t i = 0; i < count; i++) {
			if (bindings[i] == binding) {
				return ResolveKeyToGetPosition(node->children[0].get(), child_bindings[i], target_get, out_pos);
			}
		}
	}
	return false;
}

struct TransitioningKeySet {
	unique_ptr<LogicalOperator> node;
	ColumnBinding key_binding;
};

struct TransitioningKeyCTEDefinition {
	string name;
	idx_t cte_index;
	unique_ptr<LogicalOperator> node;
	vector<LogicalType> types;
	vector<string> names;
};

// Build the set of DISTINCT key values (from base_get's column at key_pos) whose match-count within
// base_get's own rows ACTUALLY transitions across zero between old (pre-delta) and new (current,
// post-delta) state -- i.e. old_count>0 && new_count==0, or old_count==0 && new_count>0. A key merely
// appearing in the delta is NOT sufficient: for a 1:many relationship (e.g. one customer with many
// orders) a single changed order must not suppress the customer's row when other, unchanged orders
// still match. Returns nullptr if unsupported (caller must then skip the optimization, not guess).
static unique_ptr<TransitioningKeySet> BuildTransitioningKeySetImpl(ClientContext &context, Binder &binder,
                                                                    LogicalGet *base_get, idx_t key_pos,
                                                                    const string &view_name) {
	auto delta_result = CreateDeltaGetNode(context, binder, base_get, view_name);
	auto delta_renumbered = renumber_and_rebind_subtree(std::move(delta_result.node), binder);
	auto delta_bindings = delta_renumbered.op->GetColumnBindings();
	auto delta_types = delta_renumbered.op->types;
	if (delta_bindings.empty() || key_pos >= delta_bindings.size() - 1) {
		return nullptr;
	}
	idx_t mul_pos = delta_bindings.size() - 1; // CreateDeltaGetNode/CompactDeltaNode appends multiplicity last.
	ColumnBinding delta_key_binding = delta_bindings[key_pos];
	ColumnBinding delta_mul_binding = delta_bindings[mul_pos];
	LogicalType key_type = delta_types[key_pos];
	LogicalType mul_type = delta_types[mul_pos];

	// Restrict the current-state count to keys present in this delta before
	// aggregating. Without this join, every inclusion-exclusion term hashes every
	// key in the nullable base table even when only one key changed.
	auto affected_delta = renumber_and_rebind_subtree(delta_renumbered.op->Copy(context), binder);
	auto affected_bindings = affected_delta.op->GetColumnBindings();
	if (key_pos >= affected_bindings.size()) {
		return nullptr;
	}
	auto affected_group_index = binder.GenerateTableIndex();
	auto affected_aggregate_index = binder.GenerateTableIndex();
	auto affected_keys =
	    make_uniq<LogicalAggregate>(affected_group_index, affected_aggregate_index, vector<unique_ptr<Expression>>());
	affected_keys->groups.push_back(make_uniq<BoundColumnRefExpression>(key_type, affected_bindings[key_pos]));
	affected_keys->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(key_type)));
	GroupingSet affected_grouping_set;
	affected_grouping_set.insert(0);
	affected_keys->grouping_sets.push_back(std::move(affected_grouping_set));
	affected_keys->children.push_back(std::move(affected_delta.op));
	affected_keys->ResolveOperatorTypes();
	ColumnBinding affected_key_binding = affected_keys->GetColumnBindings()[0];

	// Fresh scan of the SAME base table (current/post-delta state), filtered to
	// affected keys and grouped by key with COUNT(*).
	auto base_copy_op = base_get->Copy(context);
	auto base_renumbered = renumber_and_rebind_subtree(std::move(base_copy_op), binder);
	auto base_scan_bindings = base_renumbered.op->GetColumnBindings();
	if (key_pos >= base_scan_bindings.size()) {
		return nullptr;
	}
	ColumnBinding base_key_source_binding = base_scan_bindings[key_pos];
	auto affected_condition = make_uniq<BoundComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(key_type, base_key_source_binding),
	    make_uniq<BoundColumnRefExpression>(key_type, affected_key_binding));
	auto affected_base =
	    LogicalComparisonJoin::CreateJoin(context, JoinType::INNER, JoinRefType::REGULAR, std::move(base_renumbered.op),
	                                      std::move(affected_keys), std::move(affected_condition));
	affected_base->ResolveOperatorTypes();

	auto group_index = binder.GenerateTableIndex();
	auto aggregate_index = binder.GenerateTableIndex();
	auto count_func = BindAggregateByName(context, "count_star", {});
	auto count_expr = make_uniq<BoundAggregateExpression>(std::move(count_func), vector<unique_ptr<Expression>>(),
	                                                      nullptr, nullptr, AggregateType::NON_DISTINCT);
	count_expr->alias = "current_count";
	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(std::move(count_expr));
	auto base_agg = make_uniq<LogicalAggregate>(group_index, aggregate_index, std::move(aggregates));
	base_agg->groups.push_back(make_uniq<BoundColumnRefExpression>(key_type, base_key_source_binding));
	base_agg->group_stats.push_back(make_uniq<BaseStatistics>(BaseStatistics::CreateUnknown(key_type)));
	GroupingSet base_grouping_set;
	base_grouping_set.insert(0);
	base_agg->grouping_sets.push_back(std::move(base_grouping_set));
	base_agg->children.push_back(std::move(affected_base));
	base_agg->ResolveOperatorTypes();
	auto base_agg_bindings = base_agg->GetColumnBindings();
	ColumnBinding base_key_binding = base_agg_bindings[0];
	ColumnBinding base_count_binding = base_agg_bindings[1];
	LogicalType count_type = base_agg->types[1];

	// LEFT JOIN: delta (left) LEFT JOIN base_agg (right) ON key. LEFT so a key with new_count=0 (no
	// rows left in base_agg's GROUP BY at all, e.g. all matches deleted) still appears, with a NULL
	// current_count treated as 0 below.
	auto join_cond = make_uniq<BoundComparisonExpression>(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(key_type, delta_key_binding),
	    make_uniq<BoundColumnRefExpression>(key_type, base_key_binding));
	auto joined =
	    LogicalComparisonJoin::CreateJoin(context, JoinType::LEFT, JoinRefType::REGULAR, std::move(delta_renumbered.op),
	                                      std::move(base_agg), std::move(join_cond));
	joined->ResolveOperatorTypes();

	// new_count = COALESCE(current_count, 0); old_count = new_count - net_multiplicity;
	// keep only keys where (old_count>0) != (new_count>0) -- an actual 0<->>0 transition.
	// COALESCE is not a catalog scalar function -- it's a bound operator expression.
	FunctionBinder fbinder(binder);
	auto build_new_count = [&]() -> unique_ptr<Expression> {
		auto coalesce_expr = make_uniq<BoundOperatorExpression>(ExpressionType::OPERATOR_COALESCE, count_type);
		coalesce_expr->children.push_back(make_uniq<BoundColumnRefExpression>(count_type, base_count_binding));
		coalesce_expr->children.push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
		return coalesce_expr;
	};
	auto mul_as_count_type = BoundCastExpression::AddCastToType(
	    context, make_uniq<BoundColumnRefExpression>(mul_type, delta_mul_binding), count_type);
	vector<unique_ptr<Expression>> sub_args;
	sub_args.push_back(build_new_count());
	sub_args.push_back(std::move(mul_as_count_type));
	ErrorData sub_err;
	auto old_count_expr = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "-", std::move(sub_args), sub_err, true);
	if (!old_count_expr) {
		throw InternalException("DeltaJoin: failed to bind '-' for transition check: %s", sub_err.RawMessage());
	}
	// Only the DOWNWARD transition (old>0, new=0) is a phantom-NULL-pad risk here: the "other" side
	// row is LEFT JOINed against base_get's CURRENT (already-merged) state, so a key that just LOST
	// its last match reads as unmatched in "current" even though it was genuinely matched pre-batch --
	// that phantom dangling row must be excluded (the higher-order term supplies the real removal row
	// instead). A key that just GAINED its first match (old=0, new>0) is the opposite: "current"
	// already reflects that real, new match, so this term's row IS the correct contribution and must
	// NOT be excluded -- excluding it would drop the row entirely, since no other term re-adds it.
	auto old_gt_zero =
	    make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_GREATERTHAN, std::move(old_count_expr),
	                                         make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
	auto new_eq_zero = make_uniq<BoundComparisonExpression>(ExpressionType::COMPARE_EQUAL, build_new_count(),
	                                                        make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
	auto transition_expr = make_uniq<BoundConjunctionExpression>(ExpressionType::CONJUNCTION_AND,
	                                                             std::move(old_gt_zero), std::move(new_eq_zero));
	auto filter = make_uniq<LogicalFilter>(std::move(transition_expr));
	filter->children.push_back(std::move(joined));
	filter->ResolveOperatorTypes();

	// Project just the key column.
	vector<unique_ptr<Expression>> proj_exprs;
	proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(key_type, delta_key_binding));
	auto proj_index = binder.GenerateTableIndex();
	auto projection = make_uniq<LogicalProjection>(proj_index, std::move(proj_exprs));
	projection->children.push_back(std::move(filter));
	projection->ResolveOperatorTypes();
	ColumnBinding final_key_binding = projection->GetColumnBindings()[0];

	auto result = make_uniq<TransitioningKeySet>();
	result->node = std::move(projection);
	result->key_binding = final_key_binding;
	return result;
}

static unique_ptr<TransitioningKeySet>
GetTransitioningKeySetRef(ClientContext &context, Binder &binder, LogicalGet *base_get, idx_t key_pos,
                          size_t leaf_index, const string &view_name,
                          map<pair<size_t, idx_t>, idx_t> &transition_cte_indexes,
                          vector<TransitioningKeyCTEDefinition> &transition_ctes) {
	auto cache_key = make_pair(leaf_index, key_pos);
	auto existing = transition_cte_indexes.find(cache_key);
	idx_t definition_index;
	if (existing == transition_cte_indexes.end()) {
		auto transitioning_keys = BuildTransitioningKeySetImpl(context, binder, base_get, key_pos, view_name);
		if (!transitioning_keys) {
			return nullptr;
		}
		D_ASSERT(transitioning_keys->node->types.size() == 1);
		TransitioningKeyCTEDefinition definition;
		definition.cte_index = binder.GenerateTableIndex();
		definition.name = "openivm_transition_keys_" + to_string(definition.cte_index);
		definition.types = transitioning_keys->node->types;
		definition.names = {"openivm_transition_key"};
		definition.node = std::move(transitioning_keys->node);
		definition_index = transition_ctes.size();
		transition_ctes.push_back(std::move(definition));
		transition_cte_indexes[cache_key] = definition_index;
		OPENIVM_DEBUG_PRINT("[DeltaJoin] Materialized transition-key CTE for leaf=%zu key=%zu\n", leaf_index, key_pos);
	} else {
		definition_index = existing->second;
	}
	auto &definition = transition_ctes[definition_index];
	auto ref_table_index = binder.GenerateTableIndex();
	auto ref = make_uniq<LogicalCTERef>(ref_table_index, definition.cte_index, definition.types, definition.names);
	ref->ResolveOperatorTypes();
	auto result = make_uniq<TransitioningKeySet>();
	result->key_binding = ref->GetColumnBindings()[0];
	result->node = std::move(ref);
	return result;
}

// After DemoteLeftJoinsForMask, a LEFT/RIGHT join may remain un-demoted (kept as an outer join)
// because its null-supplying side has no delta leaf in THIS mask — so it reads that side as
// "current" state. If that null-supplying leaf independently has ANY pending delta (from this
// same refresh, just not part of this term's mask), "current" silently mixes old and new state
// for the dangling-tuple decision: a preserved-side row whose matching child rows are ALSO being
// deleted in this same batch gets a spurious extra dangling row here, on top of the correct
// removal already produced by the higher-order term that covers {this leaf, that leaf} together
// (double-count). This is the outer-join analogue of the classic T_old-vs-T_new join delta
// problem — LEFT JOIN's NULL-padding is a non-linear threshold function (unlike inner join's
// bilinear product), so it cannot be decomposed by inclusion-exclusion the way ordinary joins
// can; instead (matching Larson & Zhou / DBSP's semijoin-count treatment), we must exclude keys
// that are themselves transitioning, since those are exclusively owned by the term(s) that
// include this leaf's delta bit. Guard: anti-join the null-supplying leaf's current scan against
// its own delta table on the join key, excluding any key present there. Only applies when the
// null-supplying side is a single, unwrapped base-table leaf directly under the join (bails
// silently otherwise, matching this file's existing unsupported-shape convention).
static void GuardKeptOuterJoinsForMaskRec(ClientContext &context, Binder &binder, LogicalOperator *node,
                                          const vector<JoinLeafInfo> &leaves, uint64_t leaf_has_delta_mask,
                                          const string &view_name, bool portable_anti_guard,
                                          map<pair<size_t, idx_t>, idx_t> &transition_cte_indexes,
                                          vector<TransitioningKeyCTEDefinition> &transition_ctes,
                                          vector<size_t> &path) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *j = dynamic_cast<LogicalComparisonJoin *>(node);
		if (j && (j->join_type == JoinType::LEFT || j->join_type == JoinType::RIGHT) && !j->conditions.empty()) {
			idx_t null_side_child = j->join_type == JoinType::LEFT ? 1 : 0;
			// leaves[]/path matching is ONLY used to find null_leaf_idx (a structural, path-based
			// lookup unaffected by renumbering) for the leaf_has_delta_mask check below. The actual
			// resolution below must use j->children[null_side_child] directly -- that subtree lives
			// in `term`'s OWN freshly-renumbered copy, whereas leaves[i].node/.get are stale pointers
			// into the ORIGINAL (pre-copy) plan and carry different table indices entirely.
			path.push_back(null_side_child);
			size_t null_leaf_idx = SIZE_MAX;
			for (size_t i = 0; i < leaves.size(); i++) {
				if (leaves[i].path == path) {
					null_leaf_idx = i;
					break;
				}
			}
			path.pop_back();
			idx_t other_child = 1 - null_side_child;
			LogicalOperator *current_null_side_node = j->children[null_side_child].get();
			LogicalGet *current_null_side_get = FindGetInSubtree(current_null_side_node);
			if (current_null_side_get && null_leaf_idx != SIZE_MAX && (leaf_has_delta_mask & (1ULL << null_leaf_idx))) {
				auto &cond = j->conditions[0];
				// null_key_expr references the null-supplying side (used to size/position the Δ-scan probe
				// we build below). other_key_expr references the OTHER (mask-driven, preserved) side -- the
				// side that must actually be filtered. A RIGHT/LEFT join always outputs every row of its
				// preserved side (matched or NULL-padded); excluding rows from the null-supplying side's
				// scan cannot prevent a dangling row, since "no match found" is exactly what happens
				// regardless. What must be excluded is the OTHER side's row itself: if its key ALSO has a
				// pending delta on the null-supplying side, this term must neither match nor dangling-pad
				// it -- that key is handled entirely by the higher-order term that includes both leaves.
				auto &null_key_expr = j->join_type == JoinType::LEFT ? cond.right : cond.left;
				auto &other_key_expr = j->join_type == JoinType::LEFT ? cond.left : cond.right;
				BoundColumnRefExpression *key_expr =
				    null_key_expr->expression_class == ExpressionClass::BOUND_COLUMN_REF
				        ? &null_key_expr->Cast<BoundColumnRefExpression>()
				        : nullptr;
				idx_t key_pos = DConstants::INVALID_INDEX;
				if (key_expr) {
					ResolveKeyToGetPosition(current_null_side_node, key_expr->binding, current_null_side_get, key_pos);
				}
				if (key_expr && key_pos != DConstants::INVALID_INDEX &&
				    other_key_expr->expression_class == ExpressionClass::BOUND_COLUMN_REF) {
					// Build the set of keys whose match-count on the null-supplying side ACTUALLY
					// transitions across zero (old>0,new=0 or old=0,new>0). A key merely appearing in
					// the delta is NOT enough to exclude it: for a 1:many relationship (e.g. one
					// customer, many orders) a single changed order must not suppress the customer's
					// row when the customer still has OTHER, unchanged matches. Only a true 0<->>0
					// transition means "this key's presence here is fully owned by the higher-order
					// term" -- matching the same match-count-transition principle as the secondary-delta
					// fix, just applied here to avoid a double-count instead of to add a missing row.
					auto transitioning_keys =
					    GetTransitioningKeySetRef(context, binder, current_null_side_get, key_pos, null_leaf_idx,
					                              view_name, transition_cte_indexes, transition_ctes);
					if (transitioning_keys) {
						auto &other_bcr = other_key_expr->Cast<BoundColumnRefExpression>();
						auto left_expr = make_uniq<BoundColumnRefExpression>(other_bcr.return_type, other_bcr.binding);
						auto right_expr =
						    make_uniq<BoundColumnRefExpression>(other_bcr.return_type, transitioning_keys->key_binding);
						auto anti_condition = make_uniq<BoundComparisonExpression>(
						    ExpressionType::COMPARE_EQUAL, std::move(left_expr), std::move(right_expr));
						auto &other_subtree = j->children[other_child];
						if (portable_anti_guard) {
							auto output_count = other_subtree->GetColumnBindings().size();
							auto mark_join = LogicalComparisonJoin::CreateJoin(
							    context, JoinType::MARK, JoinRefType::REGULAR, std::move(other_subtree),
							    std::move(transitioning_keys->node), std::move(anti_condition));
							auto &mark = mark_join->Cast<LogicalComparisonJoin>();
							mark.mark_index = binder.GenerateTableIndex();
							mark.convert_mark_to_semi = false;
							mark_join->ResolveOperatorTypes();

							auto mark_ref = make_uniq<BoundColumnRefExpression>(LogicalType::BOOLEAN,
							                                                    ColumnBinding(mark.mark_index, 0));
							auto keep_unmatched = make_uniq<BoundComparisonExpression>(
							    ExpressionType::COMPARE_DISTINCT_FROM, std::move(mark_ref),
							    make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
							auto filter = make_uniq<LogicalFilter>(std::move(keep_unmatched));
							for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
								filter->projection_map.push_back(output_idx);
							}
							filter->children.push_back(std::move(mark_join));
							filter->ResolveOperatorTypes();
							other_subtree = std::move(filter);
							OPENIVM_DEBUG_PRINT(
							    "[DeltaJoin] Rendered transition-key exclusion as portable MARK filter\n");
						} else {
							auto anti_join = LogicalComparisonJoin::CreateJoin(
							    context, JoinType::ANTI, JoinRefType::REGULAR, std::move(other_subtree),
							    std::move(transitioning_keys->node), std::move(anti_condition));
							anti_join->ResolveOperatorTypes();
							other_subtree = std::move(anti_join);
						}
						OPENIVM_DEBUG_PRINT("[DeltaJoin] Guarded kept outer join: excluded rows whose key "
						                    "match-count transitions across zero via leaf %zu's delta\n",
						                    null_leaf_idx);
					}
				}
			}
		}
	}
	for (size_t ci = 0; ci < node->children.size(); ci++) {
		path.push_back(ci);
		GuardKeptOuterJoinsForMaskRec(context, binder, node->children[ci].get(), leaves, leaf_has_delta_mask, view_name,
		                              portable_anti_guard, transition_cte_indexes, transition_ctes, path);
		path.pop_back();
	}
}

static void GuardKeptOuterJoinsForMask(ClientContext &context, Binder &binder, LogicalOperator *node,
                                       const vector<JoinLeafInfo> &leaves, uint64_t leaf_has_delta_mask,
                                       const string &view_name, bool portable_anti_guard,
                                       map<pair<size_t, idx_t>, idx_t> &transition_cte_indexes,
                                       vector<TransitioningKeyCTEDefinition> &transition_ctes) {
	vector<size_t> path;
	GuardKeptOuterJoinsForMaskRec(context, binder, node, leaves, leaf_has_delta_mask, view_name, portable_anti_guard,
	                              transition_cte_indexes, transition_ctes, path);
}

void AppendMultiplicityToAncestorProjectionMaps(unique_ptr<LogicalOperator> &term, const vector<size_t> &leaf_path,
                                                const ColumnBinding &mul_binding, const char *context_label,
                                                bool preserve_constant_sibling_child_outputs, idx_t fallback_mul_idx) {
	if (leaf_path.empty()) {
		return;
	}
	vector<LogicalOperator *> ancestors;
	ancestors.reserve(leaf_path.size());
	LogicalOperator *node = term.get();
	for (size_t depth = 0; depth < leaf_path.size(); depth++) {
		if (leaf_path[depth] >= node->children.size()) {
			throw InternalException("%s: leaf path child %llu out of bounds at depth %llu", context_label,
			                        (idx_t)leaf_path[depth], (idx_t)depth);
		}
		ancestors.push_back(node);
		node = node->children[leaf_path[depth]].get();
	}
	for (size_t depth = leaf_path.size(); depth-- > 0;) {
		size_t child_side = leaf_path[depth];
		auto *join = dynamic_cast<LogicalJoin *>(ancestors[depth]);
		if (join && child_side < join->children.size()) {
			auto &proj_map = (child_side == 0) ? join->left_projection_map : join->right_projection_map;
			if (!proj_map.empty()) {
				bool immediate_parent = depth + 1 == leaf_path.size();
				bool preserve_full_child = preserve_constant_sibling_child_outputs && immediate_parent &&
				                           ancestors[depth]->children.size() == 2 &&
				                           IsConstantLeafSubtree(ancestors[depth]->children[1 - child_side].get());
				auto child_bindings = ancestors[depth]->children[child_side]->GetColumnBindings();
				for (auto projected_idx : proj_map) {
					if (projected_idx >= child_bindings.size()) {
						throw InternalException(
						    "%s: projection map index %llu out of bounds for child %llu with %llu bindings",
						    context_label, (idx_t)projected_idx, (idx_t)child_side, (idx_t)child_bindings.size());
					}
				}
				idx_t mul_idx = DConstants::INVALID_INDEX;
				for (idx_t binding_idx = 0; binding_idx < child_bindings.size(); binding_idx++) {
					if (child_bindings[binding_idx] == mul_binding) {
						mul_idx = binding_idx;
						break;
					}
				}
				if (mul_idx == DConstants::INVALID_INDEX && immediate_parent &&
				    fallback_mul_idx < child_bindings.size()) {
					mul_idx = fallback_mul_idx;
				}
				if (mul_idx == DConstants::INVALID_INDEX) {
					continue;
				}
				if (preserve_full_child) {
					idx_t projectable_count = MinValue<idx_t>(mul_idx + 1, child_bindings.size());
					for (idx_t binding_idx = 0; binding_idx < projectable_count; binding_idx++) {
						if (std::find(proj_map.begin(), proj_map.end(), binding_idx) != proj_map.end()) {
							continue;
						}
						proj_map.push_back(binding_idx);
						OPENIVM_DEBUG_PRINT("[%s] Preserved child col %lu in immediate %s proj_map\n", context_label,
						                    (unsigned long)binding_idx, child_side == 0 ? "left" : "right");
					}
				} else if (std::find(proj_map.begin(), proj_map.end(), mul_idx) == proj_map.end()) {
					proj_map.push_back(mul_idx);
					OPENIVM_DEBUG_PRINT("[%s] Added mul col %lu to ancestor %s proj_map\n", context_label,
					                    (unsigned long)mul_idx, child_side == 0 ? "left" : "right");
				}
				join->ResolveOperatorTypes();
			}
		}
	}
}

void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const JoinLeafInfo &leaf,
                               const ColumnBinding &mul_binding) {
	AppendMultiplicityToAncestorProjectionMaps(term, leaf.path, mul_binding, "DeltaJoin");
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
		// Get last_update timestamp for this view+table pair
		auto ts_result = con.Query("SELECT last_update FROM " + string(openivm::DELTA_TABLES_TABLE) +
		                           " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND table_name = '" +
		                           SqlUtils::EscapeValue(delta_name) + "'");
		if (ts_result->HasError() || ts_result->RowCount() == 0) {
			continue;
		}
		string last_update = ts_result->GetValue(0, 0).ToString();

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

static string ShortTableName(const string &name) {
	auto dot = name.find_last_of('.');
	return dot == string::npos ? name : name.substr(dot + 1);
}

static bool TableNameMatches(const string &fact_name, const string &leaf_name) {
	return StringUtil::CIEquals(fact_name, leaf_name) ||
	       StringUtil::CIEquals(ShortTableName(fact_name), ShortTableName(leaf_name));
}

static void InsertLeafAlias(unordered_map<string, size_t> &table_to_leaf, const string &alias, size_t leaf) {
	auto key = StringUtil::Lower(alias);
	auto it = table_to_leaf.find(key);
	if (it != table_to_leaf.end() && it->second != leaf) {
		it->second = size_t(-1);
		return;
	}
	table_to_leaf[key] = leaf;
}

static unordered_map<string, size_t> BuildTableToLeafMap(const vector<JoinLeafInfo> &leaves) {
	unordered_map<string, size_t> table_to_leaf;
	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = GetLeafScan(leaves[i]);
		if (!get || get->GetTable().get() == nullptr) {
			continue;
		}
		auto table_name = get->GetTable().get()->name;
		InsertLeafAlias(table_to_leaf, table_name, i);
		InsertLeafAlias(table_to_leaf, ShortTableName(table_name), i);
	}
	return table_to_leaf;
}

static bool TryFindLeaf(const unordered_map<string, size_t> &table_to_leaf, const string &table_name, size_t &leaf) {
	auto it = table_to_leaf.find(StringUtil::Lower(table_name));
	if (it != table_to_leaf.end() && it->second != size_t(-1)) {
		leaf = it->second;
		return true;
	}
	it = table_to_leaf.find(StringUtil::Lower(ShortTableName(table_name)));
	if (it != table_to_leaf.end() && it->second != size_t(-1)) {
		leaf = it->second;
		return true;
	}
	return false;
}

static bool HasJoinEquality(const vector<vector<DeltaJoinKeyProbe>> &key_probes, size_t child_leaf,
                            const string &child_column, size_t parent_leaf, const string &parent_column) {
	if (child_leaf >= key_probes.size()) {
		return false;
	}
	for (auto &probe : key_probes[child_leaf]) {
		if (probe.other_leaf == parent_leaf && StringUtil::CIEquals(probe.delta_column, child_column) &&
		    StringUtil::CIEquals(probe.other_column, parent_column)) {
			return true;
		}
	}
	return false;
}

static vector<vector<DeltaJoinKeyProbe>> BuildJoinKeyProbes(LogicalOperator *join_root,
                                                            const vector<JoinLeafInfo> &leaves) {
	vector<vector<DeltaJoinKeyProbe>> key_probes(leaves.size());
	unordered_map<uint64_t, JoinColumnRef> column_refs;
	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = GetLeafScan(leaves[i]);
		if (!get || get->GetTable().get() == nullptr) {
			continue;
		}
		auto table_name = get->GetTable().get()->name;
		auto leaf_bindings = leaves[i].node->GetColumnBindings();
		for (auto &binding : leaf_bindings) {
			string resolved_table;
			string resolved_column;
			if (!ResolveLeafBindingToBaseColumn(leaves[i].node, binding, resolved_table, resolved_column) ||
			    !TableNameMatches(resolved_table, table_name)) {
				continue;
			}
			column_refs[DeltaJoinBindingKey(binding)] = {i, get, table_name, string(), resolved_column, string()};
		}
	}
	CollectDeltaJoinKeyProbes(join_root, column_refs, key_probes, "DeltaJoinFK");
	return key_probes;
}

static vector<FKRelation> DetectFKRelations(ClientContext &context, const vector<JoinLeafInfo> &leaves,
                                            LogicalOperator *join_root) {
	vector<FKRelation> relations;

	// Build map: table_name -> leaf index (for matching FK targets to leaves)
	auto table_to_leaf = BuildTableToLeafMap(leaves);
	auto key_probes = BuildJoinKeyProbes(join_root, leaves);

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
			size_t pk_leaf;
			if (!TryFindLeaf(table_to_leaf, fk.info.table, pk_leaf)) {
				continue;
			}
			if (fk.fk_columns.size() != fk.pk_columns.size()) {
				continue;
			}
			bool all_columns_joined = true;
			for (idx_t col_idx = 0; col_idx < fk.fk_columns.size(); col_idx++) {
				if (!HasJoinEquality(key_probes, i, fk.fk_columns[col_idx], pk_leaf, fk.pk_columns[col_idx])) {
					all_columns_joined = false;
					break;
				}
			}
			if (!all_columns_joined) {
				continue;
			}
			relations.push_back({i, pk_leaf});
			OPENIVM_DEBUG_PRINT("[DeltaJoin] FK relation: leaf %zu (%s) -> leaf %zu (%s)\n", i,
			                    table_ref.get()->name.c_str(), pk_leaf, fk.info.table.c_str());
		}

		// Also consult the openivm_constraints_cache for declared RELY_FK / FK
		// relationships (populated via PRAGMA openivm_declare_rely_fk). These are
		// explicit, trusted user declarations the catalog may not carry (e.g.
		// Spark/Delta base tables have no enforced FK constraints).
		openivm::ConstraintCache constraint_cache;
		for (auto &cached : constraint_cache.GetConstraints(context, table_ref.get()->name)) {
			if (!cached.is_trusted ||
			    !(StringUtil::CIEquals(cached.kind, "FK") || StringUtil::CIEquals(cached.kind, "RELY_FK"))) {
				continue;
			}
			size_t pk_leaf;
			if (!TryFindLeaf(table_to_leaf, cached.referenced_table, pk_leaf)) {
				continue;
			}
			if (cached.columns.empty() || cached.columns.size() != cached.referenced_columns.size()) {
				continue;
			}
			bool all_columns_joined = true;
			for (idx_t col_idx = 0; col_idx < cached.columns.size(); col_idx++) {
				if (!HasJoinEquality(key_probes, i, cached.columns[col_idx], pk_leaf,
				                     cached.referenced_columns[col_idx])) {
					all_columns_joined = false;
					break;
				}
			}
			if (!all_columns_joined) {
				continue;
			}
			relations.push_back({i, pk_leaf});
			OPENIVM_DEBUG_PRINT("[DeltaJoin] RELY FK relation: leaf %zu (%s) -> leaf %zu (%s)\n", i,
			                    table_ref.get()->name.c_str(), pk_leaf, cached.referenced_table.c_str());
		}
	}
	return relations;
}

// Cheap pre-check for the FK-pruning gate: does any join leaf carry a trusted
// FK / RELY_FK in the openivm_constraints_cache? A declared RELY_FK is an
// explicit signal that pruning is worthwhile even when multiple leaves changed.
static bool ConstraintCacheHasTrustedFk(ClientContext &context, const vector<JoinLeafInfo> &leaves) {
	openivm::ConstraintCache constraint_cache;
	for (auto &leaf : leaves) {
		LogicalGet *get = GetLeafScan(leaf);
		if (!get || get->GetTable().get() == nullptr) {
			continue;
		}
		for (auto &cached : constraint_cache.GetConstraints(context, get->GetTable().get()->name)) {
			if (cached.is_trusted &&
			    (StringUtil::CIEquals(cached.kind, "FK") || StringUtil::CIEquals(cached.kind, "RELY_FK"))) {
				return true;
			}
		}
	}
	return false;
}

static vector<FKRelation> DetectCompileFactsFKRelations(const openivm::CompileFacts &facts,
                                                        const vector<JoinLeafInfo> &leaves,
                                                        LogicalOperator *join_root) {
	vector<FKRelation> relations;
	if (facts.fk_relations.empty()) {
		return relations;
	}
	auto table_to_leaf = BuildTableToLeafMap(leaves);
	auto key_probes = BuildJoinKeyProbes(join_root, leaves);
	for (auto &fact_fk : facts.fk_relations) {
		size_t child_leaf;
		size_t parent_leaf;
		if (!TryFindLeaf(table_to_leaf, fact_fk.child_table, child_leaf) ||
		    !TryFindLeaf(table_to_leaf, fact_fk.parent_table, parent_leaf) || child_leaf == parent_leaf) {
			continue;
		}
		bool all_columns_joined = true;
		for (idx_t col_idx = 0; col_idx < fact_fk.child_columns.size(); col_idx++) {
			if (!HasJoinEquality(key_probes, child_leaf, fact_fk.child_columns[col_idx], parent_leaf,
			                     fact_fk.parent_columns[col_idx])) {
				all_columns_joined = false;
				break;
			}
		}
		if (!all_columns_joined) {
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Ignoring compile FK %s -> %s; join keys do not match\n",
			                    fact_fk.child_table.c_str(), fact_fk.parent_table.c_str());
			continue;
		}
		relations.push_back({child_leaf, parent_leaf});
		OPENIVM_DEBUG_PRINT("[DeltaJoin] Compile FK relation: leaf %zu (%s) -> leaf %zu (%s)\n", child_leaf,
		                    fact_fk.child_table.c_str(), parent_leaf, fact_fk.parent_table.c_str());
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

static bool DeltaShapeIsInsertOnlyForPruning(const string &shape) {
	return StringUtil::CIEquals(shape, "INSERT_ONLY") || StringUtil::CIEquals(shape, "UNCHANGED");
}

static uint64_t ComputeFactsInsertOnlyMask(const openivm::CompileFacts &facts, const vector<JoinLeafInfo> &leaves) {
	uint64_t mask = 0;
	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = GetLeafScan(leaves[i]);
		if (!get || get->GetTable().get() == nullptr) {
			continue;
		}
		auto table_name = get->GetTable().get()->name;
		if (facts.assume_insert_only) {
			mask |= (1ULL << i);
			continue;
		}
		for (auto &entry : facts.delta_shape) {
			if (TableNameMatches(entry.first, table_name) && DeltaShapeIsInsertOnlyForPruning(entry.second)) {
				mask |= (1ULL << i);
				break;
			}
		}
	}
	return mask;
}

static bool RegularNtermPreservesFKPruning(ClientContext &context, const openivm::CompileFacts &facts,
                                           const vector<JoinLeafInfo> &leaves, LogicalOperator *join_root) {
	if (!SqlUtils::GetBoolSetting(context, "openivm_fk_pruning", true)) {
		return true;
	}
	if (facts.fk_relations.empty()) {
		bool has_cache_fk = ConstraintCacheHasTrustedFk(context, leaves);
		if (has_cache_fk) {
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Keeping inclusion-exclusion for trusted cached FK pruning\n");
		}
		return !has_cache_fk;
	}
	auto fk_relations = DetectCompileFactsFKRelations(facts, leaves, join_root);
	if (fk_relations.empty()) {
		return true;
	}
	auto insert_only_mask = ComputeFactsInsertOnlyMask(facts, leaves);
	auto skip_bits = ComputeSkipBits(fk_relations, insert_only_mask);
	if (skip_bits) {
		OPENIVM_DEBUG_PRINT("[DeltaJoin] Keeping inclusion-exclusion because FK pruning removes masks %lu\n",
		                    (unsigned long)skip_bits);
	}
	return skip_bits == 0;
}

// ============================================================================
// BuildInclusionExclusionTerms: create 2^N - 1 delta terms
// ============================================================================
static vector<unique_ptr<LogicalOperator>>
BuildInclusionExclusionTerms(DeltaOperatorInput input, ClientContext &context, Binder &binder,
                             const vector<JoinLeafInfo> &leaves, bool has_left_join,
                             vector<TransitioningKeyCTEDefinition> &transition_ctes) {
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
	auto compile_facts = openivm::CompileFactsContextSlot::Get(context);
	bool compile_only = compile_facts.compile_only;
	// FK pruning pays for catalog constraint inspection. When every leaf changed
	// in a small 2/3-way join, the remaining inclusion-exclusion space is tiny and
	// the flag benchmark shows the inspection cost can dominate. Keep it for the
	// main win case: one-sided PK/dimension changes, OR when an explicit FK is
	// declared (compile facts, or a trusted RELY_FK in the constraints cache).
	bool has_compile_fk_facts = compile_only && !compile_facts.fk_relations.empty();
	bool has_cache_fk = !has_compile_fk_facts && ConstraintCacheHasTrustedFk(context, leaves);
	bool fk_pruning_worthwhile = has_compile_fk_facts || has_cache_fk || non_empty_leaf_count == 1;
	if (fk_pruning_enabled && fk_pruning_worthwhile) {
		auto fk_relations = has_compile_fk_facts
		                        ? DetectCompileFactsFKRelations(compile_facts, leaves, input.plan.get())
		                        : DetectFKRelations(context, leaves, input.plan.get());
		if (!fk_relations.empty()) {
			uint64_t insert_only_mask = has_compile_fk_facts ? ComputeFactsInsertOnlyMask(compile_facts, leaves)
			                                                 : delta_status.insert_only_mask;
			skip_bits = ComputeSkipBits(fk_relations, insert_only_mask);
		}
	}

	// Empty-delta skipping: skip terms where any table in the mask has zero delta rows.
	// A join with an empty input always produces zero rows.
	bool skip_empty_enabled = SqlUtils::GetBoolSetting(context, "openivm_skip_empty_deltas", true);
	uint64_t empty_mask = 0;
	if (skip_empty_enabled) {
		empty_mask = compile_only ? (delta_status.empty_mask & delta_status.constant_mask) : delta_status.empty_mask;
	}

	Connection key_probe_con(*context.db);
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
			auto ts_result = key_probe_con.Query("SELECT last_update FROM " + string(openivm::DELTA_TABLES_TABLE) +
			                                     " WHERE view_name = '" + SqlUtils::EscapeValue(input.context.view) +
			                                     "' AND table_name = '" + SqlUtils::EscapeValue(delta_name) + "'");
			if (ts_result->HasError() || ts_result->RowCount() == 0 || ts_result->GetValue(0, 0).IsNull()) {
				continue;
			}
			leaf_refs[i].leaf_index = i;
			leaf_refs[i].get = get;
			leaf_refs[i].table_name = table_name;
			leaf_refs[i].delta_name = delta_name;
			leaf_refs[i].last_update = ts_result->GetValue(0, 0).ToString();

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
	map<pair<size_t, idx_t>, idx_t> transition_cte_indexes;
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
					UpdateParentProjectionMap(term, leaves[i], delta_i.mul_binding);
				} else {
					auto &subtree_ref = GetNodeAtPath(term, leaves[i].path);
					auto rewritten = input.CompileCopiedSubtree(subtree_ref, term_root);
					mul_bindings.push_back(rewritten.mul_binding);
					subtree_ref = std::move(rewritten.op);
					UpdateParentProjectionMap(term, leaves[i], rewritten.mul_binding);
				}
			}
		}

		// Guard kept (un-demoted) outer joins AFTER delta leaves are replaced: the mask-driven side
		// (e.g. Δ(P1) via CompileCopiedSubtree) must already be its final compiled form before we wrap
		// it in an anti-join -- doing this earlier corrupts the leaf-replacement step above, which would
		// otherwise try to compute a delta of our anti-join wrapper instead of the original subtree.
		if (has_left_join) {
			uint64_t leaf_has_delta_mask = (~delta_status.empty_mask) & total_terms;
			if (leaf_has_delta_mask) {
				bool portable_anti_guard = compile_facts.target_dialect != SqlDialect::DUCKDB;
				GuardKeptOuterJoinsForMask(context, binder, term.get(), leaves, leaf_has_delta_mask, input.context.view,
				                           portable_anti_guard, transition_cte_indexes, transition_ctes);
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
		FunctionBinder fbinder(binder);
		unique_ptr<Expression> product = make_uniq<BoundColumnRefExpression>(input.mul_type, mul_bindings[0]);
		for (size_t i = 1; i < mul_bindings.size(); i++) {
			vector<unique_ptr<Expression>> args;
			args.push_back(std::move(product));
			args.push_back(make_uniq<BoundColumnRefExpression>(input.mul_type, mul_bindings[i]));
			ErrorData err;
			product = fbinder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), err, true /* is_operator */);
			if (!product) {
				throw InternalException("DeltaJoin: failed to bind '*' for combined multiplicity: %s",
				                        err.RawMessage());
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
				throw InternalException("DeltaJoin: failed to bind '*' for Möbius sign: %s", err.RawMessage());
			}
		}
		proj_exprs.push_back(std::move(product));

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

static bool HasOnlyInnerJoins(LogicalOperator *node) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    node->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto *join = dynamic_cast<LogicalJoin *>(node);
		if (!join || join->join_type != JoinType::INNER) {
			return false;
		}
	}
	for (auto &child : node->children) {
		if (!HasOnlyInnerJoins(child.get())) {
			return false;
		}
	}
	return true;
}

static bool SupportsRegularNtermLeaf(const JoinLeafInfo &leaf) {
	if (leaf.get) {
		return leaf.get->GetTable().get() != nullptr;
	}
	if (leaf.node->type != LogicalOperatorType::LOGICAL_PROJECTION) {
		return false;
	}
	auto bindings = leaf.node->GetColumnBindings();
	if (bindings.empty()) {
		return false;
	}
	auto table_index = bindings[0].table_index;
	for (auto &binding : bindings) {
		if (binding.table_index != table_index) {
			return false;
		}
	}
	return true;
}

static DeltaPlanFragment CreateRegularOldNode(Binder &binder, unique_ptr<LogicalOperator> current_node,
                                              DeltaPlanFragment delta, const LogicalType &mul_type) {
	current_node->ResolveOperatorTypes();
	auto current_bindings = current_node->GetColumnBindings();
	auto current_types = current_node->types;
	if (current_bindings.empty()) {
		throw InternalException("DeltaJoin: regular old-state subtree has no output bindings");
	}
	auto output_table_index = current_bindings[0].table_index;
	for (auto &binding : current_bindings) {
		if (binding.table_index != output_table_index) {
			throw InternalException("DeltaJoin: regular old-state subtree exposes multiple table indexes");
		}
	}
	vector<unique_ptr<Expression>> current_exprs;
	for (idx_t i = 0; i < current_bindings.size(); i++) {
		current_exprs.push_back(make_uniq<BoundColumnRefExpression>(current_types[i], current_bindings[i]));
	}
	current_exprs.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
	auto current_projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(current_exprs));
	current_projection->children.push_back(std::move(current_node));
	current_projection->ResolveOperatorTypes();

	delta.op->ResolveOperatorTypes();
	auto delta_bindings = delta.op->GetColumnBindings();
	auto delta_types = delta.op->types;
	D_ASSERT(delta_bindings.size() == current_bindings.size() + 1);
	vector<unique_ptr<Expression>> delta_exprs;
	for (idx_t i = 0; i < current_bindings.size(); i++) {
		delta_exprs.push_back(make_uniq<BoundColumnRefExpression>(delta_types[i], delta_bindings[i]));
	}
	FunctionBinder function_binder(binder);
	vector<unique_ptr<Expression>> negate_args;
	negate_args.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(-1)));
	negate_args.push_back(make_uniq<BoundColumnRefExpression>(mul_type, delta.mul_binding));
	ErrorData negate_error;
	auto negated = function_binder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(negate_args), negate_error,
	                                                  true /* is_operator */);
	if (!negated) {
		throw InternalException("DeltaJoin: failed to negate old-state delta multiplicity: %s",
		                        negate_error.RawMessage());
	}
	delta_exprs.push_back(std::move(negated));
	auto delta_projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(delta_exprs));
	delta_projection->children.push_back(std::move(delta.op));
	delta_projection->ResolveOperatorTypes();

	auto old_types = current_projection->types;
	auto old_node = make_uniq<LogicalSetOperation>(output_table_index, old_types.size(), std::move(current_projection),
	                                               std::move(delta_projection), LogicalOperatorType::LOGICAL_UNION,
	                                               true /* setop_all */);
	old_node->types = std::move(old_types);
	old_node->ResolveOperatorTypes();
	return {std::move(old_node), ColumnBinding(output_table_index, current_bindings.size())};
}

static DeltaPlanFragment CompileRegularLeafDelta(DeltaOperatorInput input, ClientContext &context, Binder &binder,
                                                 unique_ptr<LogicalOperator> &leaf_node, LogicalOperator *&term_root) {
	if (leaf_node->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = leaf_node->Cast<LogicalGet>();
		auto delta = CreateDeltaGetNode(context, binder, &get, input.context.view);
		return {std::move(delta.node), delta.mul_binding};
	}
	return input.CompileCopiedSubtree(leaf_node, term_root);
}

static vector<unique_ptr<LogicalOperator>> BuildRegularJoinTerms(DeltaOperatorInput input, ClientContext &context,
                                                                 Binder &binder, const vector<JoinLeafInfo> &leaves) {
	vector<unique_ptr<LogicalOperator>> terms;
	// Base scans see post-DML state. Term i uses current state before i, delta i, and reconstructs old state after i as
	// current - delta. These disjoint telescoping terms cover every non-empty delta combination exactly once.
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Building regular N-term telescoping delta (%zu leaves)\n", leaves.size());

	for (size_t delta_leaf = 0; delta_leaf < leaves.size(); delta_leaf++) {
		auto term = input.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);
		vector<JoinLeafInfo> term_leaves;
		LogicalOperator *term_root = term.get();
		CollectJoinLeaves(term.get(), {}, term_leaves);
		D_ASSERT(term_leaves.size() == leaves.size());
		vector<ColumnBinding> mul_bindings;

		for (size_t leaf = 0; leaf < term_leaves.size(); leaf++) {
			auto &leaf_node = GetNodeAtPath(term, term_leaves[leaf].path);
			if (leaf == delta_leaf) {
				auto delta = CompileRegularLeafDelta(input, context, binder, leaf_node, term_root);
				mul_bindings.push_back(delta.mul_binding);
				leaf_node = std::move(delta.op);
				UpdateParentProjectionMap(term, term_leaves[leaf], delta.mul_binding);
				continue;
			}
			if (leaf < delta_leaf) {
				continue;
			}
			auto delta_source = leaf_node->Copy(context);
			auto delta_renumbered = renumber_and_rebind_subtree(std::move(delta_source), binder);
			delta_source = std::move(delta_renumbered.op);
			LogicalOperator *delta_root = delta_source.get();
			auto delta = CompileRegularLeafDelta(input, context, binder, delta_source, delta_root);
			auto old = CreateRegularOldNode(binder, std::move(leaf_node), std::move(delta), input.mul_type);
			mul_bindings.push_back(old.mul_binding);
			leaf_node = std::move(old.op);
			UpdateParentProjectionMap(term, term_leaves[leaf], old.mul_binding);
		}

		term->ResolveOperatorTypes();
		auto term_bindings = term->GetColumnBindings();
		auto term_types = term->types;
		unordered_set<uint64_t> mul_set;
		CollectExistingMultiplicityBindings(term.get(), mul_set);
		for (auto &binding : mul_bindings) {
			mul_set.insert(DeltaJoinBindingKey(binding));
		}
		vector<unique_ptr<Expression>> projection_exprs;
		for (idx_t i = 0; i < term_bindings.size(); i++) {
			if (!mul_set.count(DeltaJoinBindingKey(term_bindings[i]))) {
				projection_exprs.push_back(make_uniq<BoundColumnRefExpression>(term_types[i], term_bindings[i]));
			}
		}

		FunctionBinder function_binder(binder);
		unique_ptr<Expression> product = make_uniq<BoundColumnRefExpression>(input.mul_type, mul_bindings[0]);
		for (size_t i = 1; i < mul_bindings.size(); i++) {
			vector<unique_ptr<Expression>> args;
			args.push_back(std::move(product));
			args.push_back(make_uniq<BoundColumnRefExpression>(input.mul_type, mul_bindings[i]));
			ErrorData error;
			product =
			    function_binder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), error, true /* is_operator */);
			if (!product) {
				throw InternalException("DeltaJoin: failed to bind regular N-term multiplicity: %s",
				                        error.RawMessage());
			}
		}
		projection_exprs.push_back(std::move(product));
		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(projection_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));
	}
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Regular N-term count: %zu\n", terms.size());
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

	// 3. Build terms — use DuckLake N-term path when all leaves are DuckLake scans.
	// The DuckLake collector looks through transparent wrappers so each physical
	// source contributes one term even when a projection wraps a preserved join.
	bool all_ducklake = true;
	bool flattened_ducklake = false;
	vector<JoinLeafInfo> ducklake_leaves;
	string ducklake_fallback_reason;
	if (!SqlUtils::GetBoolSetting(context, "openivm_ducklake_nterm", true)) {
		all_ducklake = false; // forced to inclusion-exclusion
		ducklake_fallback_reason = "openivm_ducklake_nterm is disabled";
	} else {
		if (input.context.model.type == RefreshType::SIMPLE_PROJECTION &&
		    TryCollectDuckLakeJoinLeaves(input.plan.get(), ducklake_leaves, ducklake_fallback_reason)) {
			bool has_wrapped_leaf = ducklake_leaves.size() != leaves.size();
			if (!has_wrapped_leaf) {
				for (auto &leaf : leaves) {
					if (!leaf.get) {
						has_wrapped_leaf = true;
						break;
					}
				}
			}
			if (has_wrapped_leaf) {
				flattened_ducklake = true;
				leaves = std::move(ducklake_leaves);
				N = leaves.size();
			}
		} else if (input.context.model.type != RefreshType::SIMPLE_PROJECTION) {
			ducklake_fallback_reason = "refresh type is outside SIMPLE_PROJECTION scope";
		}
		for (size_t i = 0; i < N; i++) {
			auto *get = GetLeafScan(leaves[i]);
			if (!get || get->function.name != "ducklake_scan") {
				all_ducklake = false;
				break;
			}
		}
	}
	if (!flattened_ducklake && !ducklake_fallback_reason.empty()) {
		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Flattening fallback: %s\n", ducklake_fallback_reason.c_str());
	}
	if (N > openivm::MAX_JOIN_TABLES) {
		throw NotImplementedException("IVM not supported for joins with more than 16 tables");
	}
	auto compile_facts = openivm::CompileFactsContextSlot::Get(context);
	bool regular_nterm = !all_ducklake && compile_facts.compile_only && !has_left_join &&
	                     input.context.model.type == RefreshType::SIMPLE_PROJECTION &&
	                     HasOnlyInnerJoins(input.plan.get()) &&
	                     RegularNtermPreservesFKPruning(context, compile_facts, leaves, input.plan.get()) &&
	                     SqlUtils::GetBoolSetting(context, "openivm_regular_nterm", true);
	if (regular_nterm) {
		for (auto &leaf : leaves) {
			if (!SupportsRegularNtermLeaf(leaf)) {
				regular_nterm = false;
				break;
			}
		}
	}
	LogDeltaOperatorStrategy(input, all_ducklake    ? DeltaOperatorStrategy::JOIN_DUCKLAKE_N_TERM
	                                : regular_nterm ? DeltaOperatorStrategy::JOIN_REGULAR_N_TERM
	                                                : DeltaOperatorStrategy::JOIN_INCLUSION_EXCLUSION);

	vector<TransitioningKeyCTEDefinition> transition_ctes;
	vector<unique_ptr<LogicalOperator>> terms;
	if (all_ducklake) {
		terms = BuildDuckLakeJoinTerms(input, context, binder, leaves, has_left_join, flattened_ducklake);
	} else if (regular_nterm) {
		terms = BuildRegularJoinTerms(input, context, binder, leaves);
	} else {
		terms = BuildInclusionExclusionTerms(input, context, binder, leaves, has_left_join, transition_ctes);
	}

	// 4. UNION ALL
	auto result = AssembleJoinUnionAll(terms, types, binder);
	for (auto definition = transition_ctes.rbegin(); definition != transition_ctes.rend(); definition++) {
		result = make_uniq<LogicalMaterializedCTE>(definition->name, definition->cte_index, definition->types.size(),
		                                           std::move(definition->node), std::move(result),
		                                           CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
		result->ResolveOperatorTypes();
	}
	if (!transition_ctes.empty()) {
		OPENIVM_DEBUG_PRINT("[DeltaJoin] Shared %zu transition-key CTEs across inclusion-exclusion terms\n",
		                    transition_ctes.size());
	}

	// 5. Rebind parent references
	ColumnBinding new_mul_binding = ReplaceJoinOutputBindings(original_bindings, result, *input.root);

	input.plan = std::move(result);
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Done, %zu terms unioned, mul_binding: table=%lu col=%lu\n", terms.size(),
	                    (unsigned long)new_mul_binding.table_index, (unsigned long)new_mul_binding.column_index);
	return {std::move(input.plan), new_mul_binding};
}

} // namespace duckdb
