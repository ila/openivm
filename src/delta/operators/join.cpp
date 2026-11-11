#include "lpts_expression_renderer.hpp"
#include "compile_facts.hpp"
#include "delta/operators/join.hpp"
#include "delta/operators/ducklake_join.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/plan_rewrite_internal.hpp"
#include "core/sql_utils.hpp"
#include "upsert/refresh_index_regen.hpp"
#include "match/constraint_cache.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/common/numeric_utils.hpp"
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
#include <bitset>

namespace duckdb {

// A leaf's scan with its source's watermark, for key-domain probes. An empty last_update disables probing it.
struct JoinProbeSource {
	LogicalGet *get;
	// Qualified physical delta; it may sit in an internal schema rather than beside the source.
	string delta_table_sql;
	string last_update;
};

static string QualifyColumn(const string &alias, const string &column_name) {
	return alias + "." + SqlUtils::QuoteIdentifier(column_name);
}

static string BuildPushedFilterSQL(ClientContext &context, LogicalGet &get, const string &alias) {
	LptsExpressionRenderer renderer(context, SqlDialect::DUCKDB, {});
	return StringUtil::Join(renderer.ScanFiltersToSql(get, alias + "."), " AND ");
}

bool DeltaJoinProbeExists(Connection &con, const string &relation_sql, const char *label) {
	auto sql = "SELECT EXISTS(SELECT 1 FROM " + relation_sql + " LIMIT 1)";
	OPENIVM_DEBUG_PRINT("[%s] Key probe SQL: %s\n", label, sql.c_str());
	auto result = con.Query(sql);
	if (result->HasError() || result->RowCount() == 0 || result->Collection().GetValue(0, 0).IsNull()) {
		OPENIVM_DEBUG_PRINT("[%s] Could not probe key-domain intersection: %s\n", label,
		                    result->HasError() ? result->GetError().c_str() : "no result");
		return true;
	}
	return result->Collection().GetValue(0, 0).GetValue<bool>();
}

// The pending delta keys of ref's source since its watermark, honouring the leaf's pushed-down filters.
static string DeltaKeysSQL(ClientContext &context, const JoinProbeSource &ref, const string &column,
                           const string &alias) {
	auto filter = BuildPushedFilterSQL(context, *ref.get, alias);
	return "(SELECT " + QualifyColumn(alias, column) + " AS openivm_key FROM " + ref.delta_table_sql + " " + alias +
	       " WHERE " + QualifyColumn(alias, openivm::TIMESTAMP_COL) + " >= '" + SqlUtils::EscapeValue(ref.last_update) +
	       "'::TIMESTAMP" + (filter.empty() ? "" : " AND " + filter) + ")";
}

// Whether delta_ref's pending keys meet other_ref's pending delta keys (other_is_delta) or its current base rows.
static bool DeltaKeyHasMatch(Connection &con, const JoinProbeSource &delta_ref, const string &delta_column,
                             const JoinProbeSource &other_ref, const string &other_column, bool other_is_delta) {
	string relation =
	    DeltaKeysSQL(*con.context, delta_ref, delta_column, "openivm_delta") + " openivm_delta_keys JOIN ";
	if (other_is_delta) {
		relation += DeltaKeysSQL(*con.context, other_ref, other_column, "openivm_other") +
		            " openivm_other_keys ON openivm_delta_keys.openivm_key = openivm_other_keys.openivm_key";
	} else {
		auto table = other_ref.get->GetTable();
		auto filter = BuildPushedFilterSQL(*con.context, *other_ref.get, "openivm_other");
		relation +=
		    SqlUtils::FullName(table->ParentCatalog().GetName(), table->schema.name, table->name) +
		    " openivm_other ON openivm_delta_keys.openivm_key = " + QualifyColumn("openivm_other", other_column) +
		    (filter.empty() ? "" : " WHERE " + filter);
	}
	return DeltaJoinProbeExists(con, relation, "DeltaJoin");
}

static void FilterInternalMultiplicityColumns(const vector<ColumnBinding> &bindings, const vector<LogicalType> &types,
                                              const column_binding_set_t &mul_set,
                                              vector<ColumnBinding> &filtered_bindings,
                                              vector<LogicalType> &filtered_types) {
	for (idx_t i = 0; i < bindings.size(); i++) {
		if (mul_set.count(bindings[i])) {
			continue;
		}
		filtered_bindings.push_back(bindings[i]);
		filtered_types.push_back(types[i]);
	}
}

static column_binding_set_t RenumberBindings(const vector<ColumnBinding> &bindings,
                                             const unordered_map<old_idx, new_idx> &idx_map) {
	column_binding_set_t result;
	for (auto binding : bindings) {
		auto remapped = idx_map.find(binding.table_index);
		if (remapped != idx_map.end()) {
			binding.table_index = remapped->second;
		}
		result.insert(binding);
	}
	return result;
}

// The column a join key refers to, looking through casts.
static bool TryGetDeltaJoinColumnRef(Expression &expr, ColumnBinding &binding) {
	if (BoundCastExpression::IsCast(expr)) {
		return TryGetDeltaJoinColumnRef(*BoundCastExpression::ChildMutable(expr.Cast<BoundFunctionExpression>()),
		                                binding);
	}
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) {
		return false;
	}
	binding = expr.Cast<BoundColumnRefExpression>().Binding();
	return true;
}

struct DeltaJoinColumn {
	size_t leaf_index;
	string column_name;
};

static void CollectDeltaJoinKeyProbes(LogicalOperator *node, const column_binding_map_t<DeltaJoinColumn> &columns,
                                      vector<vector<DeltaJoinKeyProbe>> &probes) {
	auto *join =
	    node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ? &node->Cast<LogicalComparisonJoin>() : nullptr;
	if (join && join->join_type == JoinType::INNER) {
		for (auto &cond : join->conditions) {
			ColumnBinding left_binding, right_binding;
			if (!cond.IsComparison() || cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL ||
			    !TryGetDeltaJoinColumnRef(*cond.LeftReference(), left_binding) ||
			    !TryGetDeltaJoinColumnRef(*cond.RightReference(), right_binding)) {
				continue;
			}
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Join key bindings: %s = %s\n", left_binding.ToString().c_str(),
			                    right_binding.ToString().c_str());
			auto left = columns.find(left_binding);
			auto right = columns.find(right_binding);
			if (left == columns.end() || right == columns.end() ||
			    left->second.leaf_index == right->second.leaf_index) {
				continue;
			}
			probes[left->second.leaf_index].push_back(
			    {right->second.leaf_index, left->second.column_name, right->second.column_name});
			probes[right->second.leaf_index].push_back(
			    {left->second.leaf_index, right->second.column_name, left->second.column_name});
		}
	}
	for (auto &child : node->children) {
		CollectDeltaJoinKeyProbes(child.get(), columns, probes);
	}
}

LogicalGet *FindGetInSubtree(LogicalOperator *node, vector<LogicalOperator *> *chain) {
	while (node && node->type != LogicalOperatorType::LOGICAL_GET) {
		if (node->children.size() != 1) {
			return nullptr;
		}
		if (chain) {
			chain->push_back(node);
		}
		node = node->children[0].get();
	}
	return node ? dynamic_cast<LogicalGet *>(node) : nullptr;
}

LogicalGet *GetLeafScan(const JoinLeafInfo &leaf) {
	return leaf.get ? leaf.get : FindGetInSubtree(leaf.node);
}

// Map every binding that re-exposes a column of `get` unchanged to that column's position in get's bindings.
// `chain` lists the operators above get, outermost first. Projections forward column references (looking through
// casts); every other operator forwards only the bindings it re-exposes verbatim.
static column_binding_map_t<idx_t> TraceScanColumns(LogicalGet &get, const vector<LogicalOperator *> &chain) {
	column_binding_map_t<idx_t> visible;
	auto get_bindings = get.GetColumnBindings();
	for (idx_t i = 0; i < get_bindings.size(); i++) {
		visible[get_bindings[i]] = i;
	}
	auto traced = visible;
	for (auto op = chain.rbegin(); op != chain.rend(); op++) {
		bool is_projection = (*op)->type == LogicalOperatorType::LOGICAL_PROJECTION;
		auto bindings = (*op)->GetColumnBindings();
		column_binding_map_t<idx_t> exposed;
		for (idx_t i = 0; i < bindings.size(); i++) {
			auto source = bindings[i];
			if (is_projection &&
			    (i >= (*op)->expressions.size() || !TryGetDeltaJoinColumnRef(*(*op)->expressions[i], source))) {
				continue;
			}
			auto entry = visible.find(source);
			if (entry != visible.end()) {
				exposed[bindings[i]] = entry->second;
			}
		}
		visible = std::move(exposed);
		traced.insert(visible.begin(), visible.end());
	}
	return traced;
}

// The catalog column behind position `pos` of get's bindings; false for virtual or unmapped columns.
static bool ScanColumnName(LogicalGet &get, idx_t pos, string &column_name) {
	auto &column_ids = get.GetColumnIds();
	idx_t column_id_idx = pos;
	if (!get.projection_ids.empty()) {
		column_id_idx = pos < get.projection_ids.size() ? get.projection_ids[pos] : DConstants::INVALID_INDEX;
	}
	if (column_id_idx >= column_ids.size() || column_ids[column_id_idx].IsVirtualColumn()) {
		return false;
	}
	column_name = get.GetColumnName(column_ids[column_id_idx]).GetIdentifierName();
	return true;
}

vector<vector<DeltaJoinKeyProbe>> BuildJoinKeyProbes(LogicalOperator *root, const vector<JoinLeafInfo> &leaves,
                                                     bool trace_wrapped_leaves) {
	column_binding_map_t<DeltaJoinColumn> columns;
	for (size_t i = 0; i < leaves.size(); i++) {
		if (!leaves[i].get && !trace_wrapped_leaves) {
			continue;
		}
		// One pass per leaf: collect the operators from the root down to the leaf's scan, then trace back up once.
		vector<LogicalOperator *> chain;
		auto *node = root;
		for (auto step : leaves[i].path) {
			chain.push_back(node);
			node = node->children[step].get();
		}
		auto *get = FindGetInSubtree(node, &chain);
		if (!get || !get->GetTable()) {
			continue;
		}
		for (auto &entry : TraceScanColumns(*get, chain)) {
			string column_name;
			if (ScanColumnName(*get, entry.second, column_name)) {
				columns[entry.first] = {i, std::move(column_name)};
			}
		}
	}
	vector<vector<DeltaJoinKeyProbe>> probes(leaves.size());
	CollectDeltaJoinKeyProbes(root, columns, probes);
	return probes;
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

struct JoinPlanFacts {
	vector<JoinLeafInfo> leaves;
	vector<JoinLeafInfo> ducklake_leaves;
	vector<ColumnBinding> existing_multiplicity_bindings;
	column_binding_set_t existing_multiplicity_binding_keys;
	string ducklake_fallback_reason;
	bool has_outer_join = false;
	bool only_inner_joins = true;
	bool only_inner_or_one_sided_outer_joins = true;
};

// Collect join leaves and the properties needed to select a compilation strategy. Non-join wrappers remain leaves,
// but their descendants are still inspected for validation and existing multiplicity bindings.
static void CollectJoinPlanFacts(LogicalOperator *node, vector<size_t> &path, bool is_right_of_left, bool collect_leaf,
                                 bool collect_ducklake_leaf, JoinPlanFacts &facts) {
	if (node->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = node->Cast<LogicalCTERef>();
		if (!ref.bound_columns.empty() && ref.bound_columns.back() == openivm::MULTIPLICITY_COL) {
			auto bindings = node->GetColumnBindings();
			if (!bindings.empty() && facts.existing_multiplicity_binding_keys.insert(bindings.back()).second) {
				facts.existing_multiplicity_bindings.push_back(bindings.back());
			}
		}
	}
	bool is_join_tree_node = node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	                         node->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT ||
	                         node->type == LogicalOperatorType::LOGICAL_ANY_JOIN;
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = node->Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
		case JoinType::LEFT:
		case JoinType::RIGHT:
		case JoinType::OUTER:
			facts.has_outer_join = true;
			break;
		case JoinType::INNER:
		case JoinType::MARK:
		case JoinType::SEMI:
		case JoinType::ANTI:
		case JoinType::RIGHT_SEMI:
		case JoinType::RIGHT_ANTI:
			break;
		default:
			throw Exception(ExceptionType::OPTIMIZER,
			                JoinTypeToString(join.join_type) + " type not yet supported in OpenIVM");
		}
	}
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    node->type == LogicalOperatorType::LOGICAL_ANY_JOIN) {
		auto *join = dynamic_cast<LogicalJoin *>(node);
		if (!join || join->join_type != JoinType::INNER) {
			facts.only_inner_joins = false;
		}
		if (!join || (join->join_type != JoinType::INNER && join->join_type != JoinType::LEFT &&
		              join->join_type != JoinType::RIGHT)) {
			facts.only_inner_or_one_sided_outer_joins = false;
		}
	}
	if ((collect_leaf || collect_ducklake_leaf) && is_join_tree_node) {
		bool left_is_nullable = is_right_of_left;
		bool right_is_nullable = is_right_of_left;
		auto *join = dynamic_cast<LogicalJoin *>(node);
		if (join) {
			left_is_nullable =
			    left_is_nullable || join->join_type == JoinType::RIGHT || join->join_type == JoinType::OUTER;
			right_is_nullable =
			    right_is_nullable || join->join_type == JoinType::LEFT || join->join_type == JoinType::OUTER;
		}
		D_ASSERT(node->children.size() == 2);
		path.push_back(0);
		CollectJoinPlanFacts(node->children[0].get(), path, left_is_nullable, collect_leaf, collect_ducklake_leaf,
		                     facts);
		path.pop_back();
		path.push_back(1);
		CollectJoinPlanFacts(node->children[1].get(), path, right_is_nullable, collect_leaf, collect_ducklake_leaf,
		                     facts);
		path.pop_back();
		return;
	}
	if (collect_leaf) {
		auto *get = node->type == LogicalOperatorType::LOGICAL_GET ? dynamic_cast<LogicalGet *>(node) : nullptr;
		facts.leaves.push_back({path, get, node, is_right_of_left});
	}
	if (collect_ducklake_leaf) {
		if (node->type == LogicalOperatorType::LOGICAL_PROJECTION ||
		    node->type == LogicalOperatorType::LOGICAL_FILTER) {
			if (node->children.size() == 1) {
				path.push_back(0);
				CollectJoinPlanFacts(node->children[0].get(), path, is_right_of_left, false, true, facts);
				path.pop_back();
				return;
			}
			if (facts.ducklake_fallback_reason.empty()) {
				facts.ducklake_fallback_reason = node->GetName() + " does not have exactly one child";
			}
		} else if (node->type == LogicalOperatorType::LOGICAL_GET) {
			auto *get = dynamic_cast<LogicalGet *>(node);
			if (get && get->function.GetName().GetIdentifierName() == "ducklake_scan" && get->function.function_info) {
				facts.ducklake_leaves.push_back({path, get, node, is_right_of_left});
				return;
			}
			if (facts.ducklake_fallback_reason.empty()) {
				facts.ducklake_fallback_reason = "non-DuckLake scan " + node->GetName();
			}
		} else if (facts.ducklake_fallback_reason.empty()) {
			facts.ducklake_fallback_reason = "unsupported wrapper " + node->GetName();
		}
	}
	for (idx_t child_idx = 0; child_idx < node->children.size(); child_idx++) {
		path.push_back(child_idx);
		CollectJoinPlanFacts(node->children[child_idx].get(), path, is_right_of_left, false, false, facts);
		path.pop_back();
	}
}

static bool IsNullSupplyingJoin(JoinType join_type) {
	switch (join_type) {
	case JoinType::LEFT:
	case JoinType::RIGHT:
	case JoinType::OUTER:
		return true;
	default:
		return false;
	}
}

void DemoteLeftJoins(LogicalOperator *node) {
	if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *j = dynamic_cast<LogicalComparisonJoin *>(node);
		if (j && IsNullSupplyingJoin(j->join_type)) {
			j->join_type = JoinType::INNER;
		}
	}
	for (auto &child : node->children) {
		DemoteLeftJoins(child.get());
	}
}

// Remove NULL-padding from inputs that do not carry this term's delta. FULL
// projection refreshes also need unmatched delta rows to carry affected keys
// through later joins, so preserve the changed side as LEFT/RIGHT when only one
// side has delta. FULL aggregates keep matched-only terms: their unmatched
// contributions are maintained separately via openivm_match_count.
static bool SubtreeHasDeltaLeaf(const vector<JoinLeafInfo> &leaves, uint64_t mask, const vector<size_t> &prefix) {
	// The <= mutant indexes one past leaves; valid masks cannot make that iteration meaningful.
	// mull-ignore-next: cxx_lt_to_le
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

static void DemoteOuterJoinsForMaskRec(LogicalOperator *node, const vector<JoinLeafInfo> &leaves, uint64_t mask,
                                       RefreshType refresh_type, vector<size_t> &path) {
	auto *join = dynamic_cast<LogicalJoin *>(node);
	if (join && IsNullSupplyingJoin(join->join_type)) {
		path.push_back(0);
		bool left_has_delta = SubtreeHasDeltaLeaf(leaves, mask, path);
		path.back() = 1;
		bool right_has_delta = SubtreeHasDeltaLeaf(leaves, mask, path);
		path.pop_back();
		bool demote = join->join_type == JoinType::LEFT    ? right_has_delta
		              : join->join_type == JoinType::RIGHT ? left_has_delta
		                                                   : left_has_delta || right_has_delta;
		if (join->join_type == JoinType::OUTER && refresh_type == RefreshType::SIMPLE_PROJECTION &&
		    left_has_delta != right_has_delta) {
			join->join_type = left_has_delta ? JoinType::LEFT : JoinType::RIGHT;
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Retained unmatched FULL JOIN delta rows on %s input\n",
			                    left_has_delta ? "left" : "right");
		} else if (demote) {
			join->join_type = JoinType::INNER;
		}
	}
	// Recursing over every child is a tree-walk invariant; reversing the initial bound merely skips the walk.
	for (size_t ci = 0; ci < node->children.size(); ci++) { // mull-ignore: cxx_lt_to_ge
		path.push_back(ci);
		DemoteOuterJoinsForMaskRec(node->children[ci].get(), leaves, mask, refresh_type, path);
		path.pop_back();
	}
}

void DemoteOuterJoinsForMask(LogicalOperator *node, const vector<JoinLeafInfo> &leaves, uint64_t mask,
                             RefreshType refresh_type) {
	vector<size_t> path;
	DemoteOuterJoinsForMaskRec(node, leaves, mask, refresh_type, path);
}

struct TransitioningKeySet {
	unique_ptr<LogicalOperator> node;
	ColumnBinding key_binding;
};

struct TransitioningKeyCTEDefinition {
	string name;
	TableIndex cte_index;
	unique_ptr<LogicalOperator> node;
	vector<LogicalType> types;
	vector<string> names;
};

// Build the set of DISTINCT key values (from base_get's column at key_pos) whose match-count within
// base_get's own rows ACTUALLY transitions across zero between old (pre-delta) and new (current,
// post-delta) state -- i.e. old_count>0 && new_count==0, or old_count==0 && new_count>0. A key merely
// appearing in the delta is NOT sufficient: for a 1:many relationship (e.g. one customer with many
// orders) a single changed order must not suppress the customer's row when other, unchanged orders
// still match.
static unique_ptr<TransitioningKeySet> BuildTransitioningKeySetImpl(ClientContext &context, Connection &con,
                                                                    Binder &binder, LogicalGet *base_get, idx_t key_pos,
                                                                    const string &view_name) {
	auto delta_result = CreateDeltaGetNode(context, con, binder, base_get, view_name);
	auto delta_renumbered = renumber_and_rebind_subtree(std::move(delta_result.node), binder);
	auto delta_bindings = delta_renumbered.op->GetColumnBindings();
	auto delta_types = delta_renumbered.op->types;
	// These mutations admit the appended multiplicity as a data key; valid plans must reject it.
	if (delta_bindings.empty() || key_pos >= delta_bindings.size() - 1) { // mull-ignore: cxx_ge_to_gt,cxx_sub_to_add
		throw InternalException("DeltaJoin: delta key position %llu is outside %llu data bindings", key_pos,
		                        delta_bindings.empty() ? 0 : delta_bindings.size() - 1); // mull-ignore: cxx_sub_to_add
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
	// Equality is the first impossible binding index and is handled by this exception.
	if (key_pos >= affected_bindings.size()) { // mull-ignore: cxx_ge_to_gt
		throw InternalException("DeltaJoin: affected-key position %llu is outside %llu bindings", key_pos,
		                        affected_bindings.size());
	}
	vector<unique_ptr<Expression>> affected_groups;
	affected_groups.push_back(make_uniq<BoundColumnRefExpression>(key_type, affected_bindings[key_pos]));
	auto affected_keys = MakeGroupedAggregate(binder, std::move(affected_groups), {}, std::move(affected_delta.op));
	ColumnBinding affected_key_binding = affected_keys->GetColumnBindings()[0];

	// Fresh scan of the SAME base table (current/post-delta state), filtered to
	// affected keys and grouped by key with COUNT(*).
	auto base_copy_op = base_get->Copy(context);
	auto base_renumbered = renumber_and_rebind_subtree(std::move(base_copy_op), binder);
	auto base_scan_bindings = base_renumbered.op->GetColumnBindings();
	// Equality is the first impossible binding index and is handled by this exception.
	if (key_pos >= base_scan_bindings.size()) { // mull-ignore: cxx_ge_to_gt
		throw InternalException("DeltaJoin: base-key position %llu is outside %llu bindings", key_pos,
		                        base_scan_bindings.size());
	}
	ColumnBinding base_key_source_binding = base_scan_bindings[key_pos];
	auto affected_condition = BoundComparisonExpression::Create(
	    ExpressionType::COMPARE_EQUAL, make_uniq<BoundColumnRefExpression>(key_type, base_key_source_binding),
	    make_uniq<BoundColumnRefExpression>(key_type, affected_key_binding));
	auto affected_base =
	    LogicalComparisonJoin::CreateJoin(context, JoinType::INNER, JoinRefType::REGULAR, std::move(base_renumbered.op),
	                                      std::move(affected_keys), std::move(affected_condition));
	affected_base->ResolveOperatorTypes();

	vector<unique_ptr<Expression>> base_groups;
	base_groups.push_back(make_uniq<BoundColumnRefExpression>(key_type, base_key_source_binding));
	vector<unique_ptr<Expression>> aggregates;
	aggregates.push_back(MakeCountStar("current_count"));
	auto base_agg =
	    MakeGroupedAggregate(binder, std::move(base_groups), std::move(aggregates), std::move(affected_base));
	auto base_agg_bindings = base_agg->GetColumnBindings();
	ColumnBinding base_key_binding = base_agg_bindings[0];
	ColumnBinding base_count_binding = base_agg_bindings[1];
	LogicalType count_type = base_agg->types[1];

	// LEFT JOIN: delta (left) LEFT JOIN base_agg (right) ON key. LEFT so a key with new_count=0 (no
	// rows left in base_agg's GROUP BY at all, e.g. all matches deleted) still appears, with a NULL
	// current_count treated as 0 below.
	auto join_cond = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL,
	                                                   make_uniq<BoundColumnRefExpression>(key_type, delta_key_binding),
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
		coalesce_expr->GetChildrenMutable().push_back(
		    make_uniq<BoundColumnRefExpression>(count_type, base_count_binding));
		coalesce_expr->GetChildrenMutable().push_back(make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
		return std::move(coalesce_expr);
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
	auto old_gt_zero = BoundComparisonExpression::Create(ExpressionType::COMPARE_GREATERTHAN, std::move(old_count_expr),
	                                                     make_uniq<BoundConstantExpression>(Value::BIGINT(0)));
	auto new_eq_zero = BoundComparisonExpression::Create(ExpressionType::COMPARE_EQUAL, build_new_count(),
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
GetTransitioningKeySetRef(ClientContext &context, Connection &con, Binder &binder, LogicalGet *base_get, idx_t key_pos,
                          size_t leaf_index, const string &view_name,
                          map<pair<size_t, idx_t>, idx_t> &transition_cte_indexes,
                          vector<TransitioningKeyCTEDefinition> &transition_ctes) {
	auto cache_key = make_pair(leaf_index, key_pos);
	auto existing = transition_cte_indexes.find(cache_key);
	idx_t definition_index;
	if (existing == transition_cte_indexes.end()) {
		auto transitioning_keys = BuildTransitioningKeySetImpl(context, con, binder, base_get, key_pos, view_name);
		D_ASSERT(transitioning_keys->node->types.size() == 1);
		TransitioningKeyCTEDefinition definition;
		definition.cte_index = binder.GenerateTableIndex();
		definition.name = "openivm_transition_keys_" + to_string(definition.cte_index.index);
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
	auto ref = make_uniq<LogicalCTERef>(ref_table_index, definition.cte_index, definition.types,
	                                    StringsToIdentifiers(definition.names));
	ref->ResolveOperatorTypes();
	auto result = make_uniq<TransitioningKeySet>();
	result->key_binding = ref->GetColumnBindings()[0];
	result->node = std::move(ref);
	return result;
}

// After DemoteOuterJoinsForMask, a LEFT/RIGHT join may remain un-demoted (kept as an outer join)
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
static void
GuardKeptOuterJoinsForMaskRec(ClientContext &context, Connection &con, Binder &binder, LogicalOperator *node,
                              const vector<JoinLeafInfo> &leaves, uint64_t leaf_has_delta_mask, const string &view_name,
                              bool portable_anti_guard, map<pair<size_t, idx_t>, idx_t> &transition_cte_indexes,
                              vector<TransitioningKeyCTEDefinition> &transition_ctes, vector<size_t> &path) {
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
			vector<LogicalOperator *> null_side_chain;
			LogicalGet *current_null_side_get = FindGetInSubtree(j->children[null_side_child].get(), &null_side_chain);
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
				auto &null_key_expr = j->join_type == JoinType::LEFT ? cond.RightReference() : cond.LeftReference();
				auto &other_key_expr = j->join_type == JoinType::LEFT ? cond.LeftReference() : cond.RightReference();
				BoundColumnRefExpression *key_expr =
				    null_key_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF
				        ? &null_key_expr->Cast<BoundColumnRefExpression>()
				        : nullptr;
				idx_t key_pos = DConstants::INVALID_INDEX;
				if (key_expr) {
					auto positions = TraceScanColumns(*current_null_side_get, null_side_chain);
					auto position = positions.find(key_expr->Binding());
					key_pos = position == positions.end() ? key_pos : position->second;
				}
				if (key_expr && key_pos != DConstants::INVALID_INDEX &&
				    other_key_expr->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
					// Build the set of keys whose match-count on the null-supplying side ACTUALLY
					// transitions across zero (old>0,new=0 or old=0,new>0). A key merely appearing in
					// the delta is NOT enough to exclude it: for a 1:many relationship (e.g. one
					// customer, many orders) a single changed order must not suppress the customer's
					// row when the customer still has OTHER, unchanged matches. Only a true 0<->>0
					// transition means "this key's presence here is fully owned by the higher-order
					// term" -- matching the same match-count-transition principle as the secondary-delta
					// fix, just applied here to avoid a double-count instead of to add a missing row.
					auto transitioning_keys =
					    GetTransitioningKeySetRef(context, con, binder, current_null_side_get, key_pos, null_leaf_idx,
					                              view_name, transition_cte_indexes, transition_ctes);
					auto &other_bcr = other_key_expr->Cast<BoundColumnRefExpression>();
					auto left_expr =
					    make_uniq<BoundColumnRefExpression>(other_bcr.GetReturnType(), other_bcr.Binding());
					auto right_expr =
					    make_uniq<BoundColumnRefExpression>(other_bcr.GetReturnType(), transitioning_keys->key_binding);
					auto anti_condition = BoundComparisonExpression::Create(
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

						auto mark_ref = make_uniq<BoundColumnRefExpression>(
						    LogicalType::BOOLEAN, ColumnBinding(TableIndex(mark.mark_index), ProjectionIndex(0)));
						auto keep_unmatched = BoundComparisonExpression::Create(
						    ExpressionType::COMPARE_DISTINCT_FROM, std::move(mark_ref),
						    make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));
						auto filter = make_uniq<LogicalFilter>(std::move(keep_unmatched));
						for (idx_t output_idx = 0; output_idx < output_count; output_idx++) {
							filter->projection_map.push_back(ProjectionIndex(output_idx));
						}
						filter->children.push_back(std::move(mark_join));
						filter->ResolveOperatorTypes();
						other_subtree = std::move(filter);
						OPENIVM_DEBUG_PRINT("[DeltaJoin] Rendered transition-key exclusion as portable MARK filter\n");
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
	// Recursing over every child is required by the validated join tree; the >= mutant only skips descendants.
	for (size_t ci = 0; ci < node->children.size(); ci++) { // mull-ignore: cxx_lt_to_ge
		path.push_back(ci);
		GuardKeptOuterJoinsForMaskRec(context, con, binder, node->children[ci].get(), leaves, leaf_has_delta_mask,
		                              view_name, portable_anti_guard, transition_cte_indexes, transition_ctes, path);
		path.pop_back();
	}
}

static void GuardKeptOuterJoinsForMask(ClientContext &context, Connection &con, Binder &binder, LogicalOperator *node,
                                       const vector<JoinLeafInfo> &leaves, uint64_t leaf_has_delta_mask,
                                       const string &view_name, bool portable_anti_guard,
                                       map<pair<size_t, idx_t>, idx_t> &transition_cte_indexes,
                                       vector<TransitioningKeyCTEDefinition> &transition_ctes) {
	vector<size_t> path;
	GuardKeptOuterJoinsForMaskRec(context, con, binder, node, leaves, leaf_has_delta_mask, view_name,
	                              portable_anti_guard, transition_cte_indexes, transition_ctes, path);
}

void ShiftAncestorProjectionMap(const vector<LogicalOperator *> &ancestors, const vector<size_t> &leaf_path,
                                size_t depth, idx_t insertion_idx, idx_t added) {
	if (added == 0 || depth == 0) {
		return;
	}
	auto *parent = ancestors[depth - 1];
	auto parent_side = leaf_path[depth - 1];
	vector<ProjectionIndex> *projection_map = nullptr;
	if (auto *join = dynamic_cast<LogicalJoin *>(parent)) {
		projection_map = parent_side == 0 ? &join->left_projection_map : &join->right_projection_map;
	} else if (auto *filter = dynamic_cast<LogicalFilter *>(parent)) {
		projection_map = &filter->projection_map;
	}
	if (!projection_map) {
		return;
	}
	for (auto &index : *projection_map) {
		if (index >= insertion_idx) {
			index = ProjectionIndex(index.GetIndex() + added);
		}
	}
}

void AppendMultiplicityToAncestorProjectionMaps(unique_ptr<LogicalOperator> &term, const vector<size_t> &leaf_path,
                                                const ColumnBinding &mul_binding, const char *context_label,
                                                bool preserve_constant_sibling_child_outputs, idx_t fallback_mul_idx,
                                                idx_t original_leaf_width) {
	if (leaf_path.empty()) {
		return;
	}
	vector<LogicalOperator *> ancestors;
	ancestors.reserve(leaf_path.size());
	LogicalOperator *node = term.get();
	for (size_t depth = 0; depth < leaf_path.size(); depth++) {
		// Equality is the first invalid child index and is handled by this exception.
		// mull-ignore-next: cxx_ge_to_gt
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
		// A collected join path contains only child 0 or 1; <= would admit the invalid size() index.
		if (join && child_side < join->children.size()) { // mull-ignore: cxx_lt_to_le
			auto &proj_map = (child_side == 0) ? join->left_projection_map : join->right_projection_map;
			bool immediate_parent = depth + 1 == leaf_path.size();
			// A binary join's sibling is 1-child_side. Addition is either the same index for side 0 or the invalid
			// index 2 for side 1; it cannot describe another valid planner shape.
			bool preserve_full_child =
			    preserve_constant_sibling_child_outputs && immediate_parent && ancestors[depth]->children.size() == 2 &&
			    IsConstantLeafSubtree(ancestors[depth]->children[1 - child_side].get()); // mull-ignore: cxx_sub_to_add
			auto child_bindings = ancestors[depth]->children[child_side]->GetColumnBindings();
			auto shift_parent_projection = [&](idx_t insertion_idx, idx_t added) {
				ShiftAncestorProjectionMap(ancestors, leaf_path, depth, insertion_idx, added);
			};
			if (!proj_map.empty()) {
				idx_t mul_idx = DConstants::INVALID_INDEX;
				auto mul_binding_it = std::find(child_bindings.begin(), child_bindings.end(), mul_binding);
				if (mul_binding_it != child_bindings.end()) {
					mul_idx = idx_t(mul_binding_it - child_bindings.begin());
				}
				// Aggregate delta compilation inserts multiplicity among group bindings. This map still indexes
				// the pre-rewrite child, so shift every selected data column at or after that insertion.
				if (immediate_parent && original_leaf_width != DConstants::INVALID_INDEX &&
				    child_bindings.size() == original_leaf_width + 1 && mul_idx != DConstants::INVALID_INDEX) {
					for (auto &projected_idx : proj_map) {
						if (projected_idx >= mul_idx) {
							projected_idx = ProjectionIndex(projected_idx.GetIndex() + 1);
						}
					}
				}
				for (auto projected_idx : proj_map) {
					// Equality is the first invalid binding index and is handled by this exception.
					// mull-ignore-next: cxx_ge_to_gt
					if (projected_idx >= child_bindings.size()) {
						throw InternalException(
						    "%s: projection map index %llu out of bounds for child %llu with %llu bindings",
						    context_label, (idx_t)projected_idx, (idx_t)child_side, (idx_t)child_bindings.size());
					}
				}
				// CompileChild normally exposes the binding directly. This fallback exists only for an immediate copied
				// wrapper whose validated output position is supplied by the caller.
				switch (mul_idx) {
				case DConstants::INVALID_INDEX:
					if (immediate_parent &&
					    fallback_mul_idx < child_bindings.size()) { // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
						mul_idx = fallback_mul_idx;
					}
					break;
				default:
					break;
				}
				if (mul_idx == DConstants::INVALID_INDEX) {
					continue;
				}
				idx_t old_width = proj_map.size();
				idx_t insertion_idx = old_width;
				if (child_side == 1) {
					auto left_bindings = join->children[0]->GetColumnBindings();
					insertion_idx +=
					    join->left_projection_map.empty() ? left_bindings.size() : join->left_projection_map.size();
				}
				// Appending to this join's own left_projection_map grows its LEFT
				// contribution width, which shifts the absolute position where its RIGHT
				// contribution starts within its own combined GetColumnBindings(). A
				// grandparent ancestor may already have a projection map entry referencing
				// (by that now-stale absolute position) a column from this join's right
				// side; left as-is, that entry would silently start pointing at the
				// newly-inserted column instead, dropping the real column it used to
				// select. Appending to right_projection_map never has this effect: right
				// contributions are always placed last, so a new entry there only ever
				// extends the combined output with a brand-new highest index.
				if (preserve_full_child) {
					idx_t projectable_count = MinValue<idx_t>(mul_idx + 1, child_bindings.size());
					idx_t added = 0;
					for (idx_t binding_idx = 0; binding_idx < projectable_count; binding_idx++) {
						if (std::find(proj_map.begin(), proj_map.end(), binding_idx) != proj_map.end()) {
							continue;
						}
						proj_map.push_back(ProjectionIndex(binding_idx));
						added++;
						OPENIVM_DEBUG_PRINT("[%s] Preserved child col %lu in immediate %s proj_map\n", context_label,
						                    (unsigned long)binding_idx, child_side == 0 ? "left" : "right");
					}
					shift_parent_projection(insertion_idx, added);
				} else {
					// proj_map entries are positions into the child's *current* combined
					// GetColumnBindings(). Testing raw index membership of mul_idx against
					// proj_map can alias onto an unrelated pre-existing entry that now shares
					// the same numeric position after a deeper level's own map grew. Compare
					// by column identity against what this ancestor currently exposes instead
					// of trusting the raw index.
					auto exposed = join->GetColumnBindings();
					if (std::find(exposed.begin(), exposed.end(), mul_binding) == exposed.end()) {
						proj_map.push_back(ProjectionIndex(mul_idx));
						shift_parent_projection(insertion_idx, 1);
						OPENIVM_DEBUG_PRINT("[%s] Added mul col %lu to ancestor %s proj_map\n", context_label,
						                    (unsigned long)mul_idx, child_side == 0 ? "left" : "right");
					}
				}
				join->ResolveOperatorTypes();
				continue;
			}

			auto mul_binding_it = std::find(child_bindings.begin(), child_bindings.end(), mul_binding);
			if (mul_binding_it == child_bindings.end()) {
				continue;
			}
			idx_t insertion_idx = idx_t(mul_binding_it - child_bindings.begin());
			if (child_side == 1) {
				auto left_bindings = join->children[0]->GetColumnBindings();
				insertion_idx +=
				    join->left_projection_map.empty() ? left_bindings.size() : join->left_projection_map.size();
			}
			// An empty map passes the child's bindings through unchanged, so the
			// multiplicity is inserted in-place and shifts every later binding.
			shift_parent_projection(insertion_idx, 1);
			join->ResolveOperatorTypes();
		}
	}
}

void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const JoinLeafInfo &leaf,
                               const ColumnBinding &mul_binding, idx_t original_leaf_width) {
	AppendMultiplicityToAncestorProjectionMaps(term, leaf.path, mul_binding, "DeltaJoin", false,
	                                           DConstants::INVALID_INDEX, original_leaf_width);
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
/// Also records each catalog leaf's watermark and physical delta in probe_sources for key-domain probes.
static DeltaStatus DetectDeltaStatus(Connection &con, const string &view_name, const vector<JoinLeafInfo> &leaves,
                                     DeltaSourceCache &cache, vector<JoinProbeSource> &probe_sources) {
	DeltaStatus status = {0, 0, 0, 0};

	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = GetLeafScan(leaves[i]);
		// Constant leaves have no catalog backing and therefore an always-empty delta: constant relation subtrees
		// (VALUES/CHUNK/DUMMY, often wrapped in projections) and table functions (generate_series, range, ...).
		// Other scan-less leaves may contain real tables inside nested joins, so only whole constant subtrees qualify.
		// Marking the leaf empty makes the pruner drop every term where its bit is set, so only the terms that keep
		// its original scan contribute rows, matching the semantics of a non-changing input.
		if (get ? !get->GetTable() : IsConstantLeafSubtree(leaves[i].node)) {
			status.empty_mask |= (1ULL << i);
			status.insert_only_mask |= (1ULL << i);
			status.constant_mask |= (1ULL << i);
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Leaf %zu (%s) has empty delta\n", i,
			                    get ? get->function.GetName().GetIdentifierName().c_str() : "constant values");
			continue;
		}
		if (!get) {
			continue;
		}
		auto table_ref = get->GetTable();
		// Get last_update timestamp for this view+table pair
		auto &watermark = LoadDeltaWatermark(con, view_name, *table_ref, cache);
		if (!watermark.watermark_error.empty() || watermark.last_update.IsNull()) {
			continue;
		}
		string last_update = watermark.last_update.ToString();
		auto delta_table_sql = RefreshMetadata(con).ResolveDeltaQualifiedName(
		    view_name, watermark.delta_key, table_ref->ParentCatalog().GetName().GetIdentifierName(),
		    table_ref->schema.name.GetIdentifierName());
		probe_sources[i] = {get, delta_table_sql, last_update};

		// Single query: get pending delta count, delete count, and current base
		// cardinality. The base count lets us define "tiny" as <= max(8 rows,
		// 5% of the source table), avoiding both a hard-coded absolute-only
		// threshold and silly behavior on very small tables.
		auto result = con.Query(
		    "SELECT "
		    "(SELECT COUNT(*) FROM " +
		    delta_table_sql + " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" +
		    SqlUtils::EscapeValue(last_update) +
		    "'::TIMESTAMP), "
		    "(SELECT COUNT(*) FROM " +
		    delta_table_sql + " WHERE " + string(openivm::TIMESTAMP_COL) + " >= '" +
		    SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP AND " + string(openivm::MULTIPLICITY_COL) +
		    " < 0), "
		    "(SELECT COUNT(*) FROM " +
		    SqlUtils::FullName(table_ref->ParentCatalog().GetName(), table_ref->schema.name, table_ref->name) + ")");
		if (result->HasError()) {
			continue;
		}
		int64_t total_count = result->Collection().GetValue(0, 0).GetValue<int64_t>();
		int64_t delete_count = result->Collection().GetValue(1, 0).GetValue<int64_t>();
		int64_t base_count = result->Collection().GetValue(2, 0).GetValue<int64_t>();

		if (total_count == 0) {
			status.empty_mask |= (1ULL << i);
			status.insert_only_mask |= (1ULL << i); // empty is trivially insert-only
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Leaf %zu (%s) has empty delta\n", i, table_ref.get()->name.c_str());
		} else {
			// This threshold only decides whether to attempt a semantics-preserving key-domain probe.
			// mull-ignore-next
			int64_t tiny_limit = std::max<int64_t>(8, (base_count + 19) / 20);
			// The inclusive boundary is a performance choice, not a correctness boundary.
			// mull-ignore-next: cxx_boundary
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
	// Skipping this scan can only disable FK pruning; the full inclusion-exclusion plan remains correct.
	// mull-ignore-next: cxx_lt_to_ge
	for (size_t i = 0; i < leaves.size(); i++) {
		LogicalGet *get = GetLeafScan(leaves[i]);
		if (!get || get->GetTable().get() == nullptr) {
			continue;
		}
		auto &table = *get->GetTable();
		InsertLeafAlias(table_to_leaf, SqlUtils::FullName(table.catalog.GetName(), table.schema.name, table.name), i);
		InsertLeafAlias(table_to_leaf,
		                SqlUtils::QuoteIdentifier(table.schema.name) + "." + SqlUtils::QuoteIdentifier(table.name), i);
		InsertLeafAlias(table_to_leaf, SqlUtils::QuoteIdentifier(table.name), i);
	}
	return table_to_leaf;
}

static bool TryFindLeaf(const unordered_map<string, size_t> &table_to_leaf, const string &table_name, size_t &leaf) {
	auto assign_leaf = [&](unordered_map<string, size_t>::const_iterator it) {
		if (it == table_to_leaf.end()) {
			return false;
		}
		switch (it->second) {
		case size_t(-1):
			return false;
		default:
			leaf = it->second;
			return true;
		}
	};
	auto parts = SqlUtils::ParseQualifiedIdentifier(table_name);
	for (auto &part : parts) {
		part = SqlUtils::QuoteIdentifier(part);
	}
	return assign_leaf(table_to_leaf.find(StringUtil::Lower(StringUtil::Join(parts, "."))));
}

static bool HasJoinEquality(const vector<vector<DeltaJoinKeyProbe>> &key_probes, size_t child_leaf,
                            const string &child_column, size_t parent_leaf, const string &parent_column) {
	// Equality is out of range and deliberately returns the conservative no-pruning answer.
	if (child_leaf >= key_probes.size()) { // mull-ignore: cxx_ge_to_gt
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

// Whether every FK column equals its referenced column in an INNER-join key. A missing match only disables pruning;
// inclusion-exclusion remains complete.
static bool AllFKColumnsJoined(const vector<vector<DeltaJoinKeyProbe>> &key_probes, size_t fk_leaf,
                               const vector<string> &fk_columns, size_t pk_leaf, const vector<string> &pk_columns) {
	// mull-ignore-next: cxx_lt_to_ge
	for (idx_t col_idx = 0; col_idx < fk_columns.size(); col_idx++) {
		if (!HasJoinEquality(key_probes, fk_leaf, fk_columns[col_idx], pk_leaf, pk_columns[col_idx])) {
			return false;
		}
	}
	return true;
}

static vector<FKRelation> DetectFKRelations(ClientContext &context, const vector<JoinLeafInfo> &leaves,
                                            const vector<vector<DeltaJoinKeyProbe>> &key_probes,
                                            const vector<openivm::CachedConstraint> &declarations) {
	vector<FKRelation> relations;

	// Build map: table_name -> leaf index (for matching FK targets to leaves)
	auto table_to_leaf = BuildTableToLeafMap(leaves);

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
			if (!TryFindLeaf(table_to_leaf,
			                 SqlUtils::FullName(table_ref->catalog.GetName(), fk.info.schema, fk.info.table),
			                 pk_leaf)) {
				continue;
			}
			// Malformed catalog metadata disables FK pruning; it must not affect correctness.
			if (fk.fk_columns.size() != fk.pk_columns.size()) { // mull-ignore: cxx_ne_to_eq
				continue;
			}
			if (!AllFKColumnsJoined(key_probes, i, IdentifiersToStrings(fk.fk_columns), pk_leaf,
			                        IdentifiersToStrings(fk.pk_columns))) {
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
		for (auto &cached : declarations) {
			if (!StringUtil::CIEquals(cached.table_name, SqlUtils::FullName(table_ref->catalog.GetName(),
			                                                                table_ref->schema.name, table_ref->name))) {
				continue;
			}
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
			if (!AllFKColumnsJoined(key_probes, i, cached.columns, pk_leaf, cached.referenced_columns)) {
				continue;
			}
			relations.push_back({i, pk_leaf});
			OPENIVM_DEBUG_PRINT("[DeltaJoin] RELY FK relation: leaf %zu (%s) -> leaf %zu (%s)\n", i,
			                    table_ref.get()->name.c_str(), pk_leaf, cached.referenced_table.c_str());
		}
	}
	return relations;
}

static vector<FKRelation> DetectCompileFactsFKRelations(const openivm::CompileFacts &facts,
                                                        const vector<JoinLeafInfo> &leaves,
                                                        const vector<vector<DeltaJoinKeyProbe>> &key_probes) {
	vector<FKRelation> relations;
	if (facts.fk_relations.empty()) {
		return relations;
	}
	auto table_to_leaf = BuildTableToLeafMap(leaves);
	for (auto &fact_fk : facts.fk_relations) {
		size_t child_leaf;
		size_t parent_leaf;
		if (!TryFindLeaf(table_to_leaf, fact_fk.child_table, child_leaf) ||
		    !TryFindLeaf(table_to_leaf, fact_fk.parent_table, parent_leaf) || child_leaf == parent_leaf) {
			continue;
		}
		if (!AllFKColumnsJoined(key_probes, child_leaf, fact_fk.child_columns, parent_leaf, fact_fk.parent_columns)) {
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

static bool SourceFactMatches(const string &fact, TableCatalogEntry &table) {
	auto parts = SqlUtils::ParseQualifiedIdentifier(fact);
	return parts.size() <= 3 && StringUtil::CIEquals(parts.back(), table.name.GetIdentifierName()) &&
	       (parts.size() < 2 || StringUtil::CIEquals(parts[parts.size() - 2], table.schema.name.GetIdentifierName())) &&
	       (parts.size() < 3 || StringUtil::CIEquals(parts[0], table.ParentCatalog().GetName().GetIdentifierName()));
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
		if (facts.assume_insert_only) {
			mask |= (1ULL << i);
			continue;
		}
		for (auto &entry : facts.delta_shape) {
			if (SourceFactMatches(entry.first, *get->GetTable()) && DeltaShapeIsInsertOnlyForPruning(entry.second)) {
				mask |= (1ULL << i);
				break;
			}
		}
	}
	return mask;
}

static uint64_t ComputeFactsUnchangedMask(const openivm::CompileFacts &facts, const vector<JoinLeafInfo> &leaves) {
	uint64_t mask = 0;
	for (size_t i = 0; i < leaves.size(); i++) {
		vector<LogicalOperator *> pending = {leaves[i].node};
		bool all_unchanged = true;
		while (!pending.empty() && all_unchanged) {
			auto *node = pending.back();
			pending.pop_back();
			if (node->type == LogicalOperatorType::LOGICAL_GET) {
				auto &get = node->Cast<LogicalGet>();
				if (get.GetTable().get() == nullptr) {
					continue;
				}
				bool table_unchanged = false;
				for (auto &entry : facts.delta_shape) {
					if (SourceFactMatches(entry.first, *get.GetTable()) &&
					    StringUtil::CIEquals(entry.second, "UNCHANGED")) {
						table_unchanged = true;
						break;
					}
				}
				if (!table_unchanged) {
					all_unchanged = false;
				}
			}
			for (auto &child : node->children) {
				pending.push_back(child.get());
			}
		}
		// A supported leaf without a catalog-backed table is an inline constant
		// relation (for example VALUES). It cannot contribute a delta term and is
		// therefore unchanged for every compiled refresh.
		if (all_unchanged) {
			mask |= (1ULL << i);
		}
	}
	return mask;
}

static bool RegularNtermPreservesFKPruning(ClientContext &context, const openivm::CompileFacts &facts,
                                           const vector<JoinLeafInfo> &leaves,
                                           const vector<vector<DeltaJoinKeyProbe>> &key_probes,
                                           const vector<openivm::CachedConstraint> &declarations) {
	if (!SqlUtils::GetBoolSetting(context, "openivm_fk_pruning", true)) {
		return true;
	}
	if (facts.fk_relations.empty()) {
		bool has_cache_fk = !declarations.empty();
		if (has_cache_fk) {
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Keeping inclusion-exclusion for trusted cached FK pruning\n");
		}
		return !has_cache_fk;
	}
	auto fk_relations = DetectCompileFactsFKRelations(facts, leaves, key_probes);
	if (fk_relations.empty()) {
		return true;
	}
	auto insert_only_mask = ComputeFactsInsertOnlyMask(facts, leaves);
	auto skip_bits = ComputeSkipBits(fk_relations, insert_only_mask);
	if (skip_bits) {
		OPENIVM_DEBUG_PRINT("[DeltaJoin] Keeping inclusion-exclusion because FK pruning removes masks %lu\n",
		                    (unsigned long)skip_bits);
	}
	// Both strategies are algebraically equivalent; this condition only avoids the more expensive expansion.
	return skip_bits == 0; // mull-ignore: cxx_eq_to_ne
}

// ============================================================================
// BuildInclusionExclusionTerms: create 2^N - 1 delta terms
// ============================================================================
static vector<unique_ptr<LogicalOperator>> BuildInclusionExclusionTerms(
    DeltaOperatorInput input, ClientContext &context, Binder &binder, const vector<JoinLeafInfo> &leaves,
    bool has_left_join, const vector<ColumnBinding> &existing_multiplicity_bindings,
    vector<TransitioningKeyCTEDefinition> &transition_ctes, const vector<openivm::CachedConstraint> &declarations,
    const vector<vector<DeltaJoinKeyProbe>> &key_probes, DeltaSourceCache &source_cache) {
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	// Detect delta status for all leaves (single query per table: total + delete count).
	// Used by both FK pruning and empty-delta skipping.
	vector<JoinProbeSource> leaf_refs(N);
	DeltaStatus delta_status =
	    DetectDeltaStatus(input.context.metadata_con, input.context.view, leaves, source_cache, leaf_refs);
	uint64_t total_terms = (1ULL << N) - 1;
	uint64_t non_empty_mask = total_terms & ~delta_status.empty_mask & ~delta_status.constant_mask;
	idx_t non_empty_leaf_count = std::bitset<64>(non_empty_mask).count();

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
	bool has_cache_fk = !declarations.empty();
	// The one-leaf cutoff avoids catalog work; FK pruning remains semantically valid on either side of it.
	bool fk_pruning_worthwhile =
	    has_compile_fk_facts || has_cache_fk || non_empty_leaf_count == 1; // mull-ignore: cxx_eq_to_ne
	if (fk_pruning_enabled && fk_pruning_worthwhile) {
		auto fk_relations = has_compile_fk_facts ? DetectCompileFactsFKRelations(compile_facts, leaves, key_probes)
		                                         : DetectFKRelations(context, leaves, key_probes, declarations);
		if (!fk_relations.empty()) {
			uint64_t insert_only_mask = has_compile_fk_facts ? ComputeFactsInsertOnlyMask(compile_facts, leaves)
			                                                 : delta_status.insert_only_mask;
			skip_bits = ComputeSkipBits(fk_relations, insert_only_mask);
		}
	}

	// Empty-delta skipping: skip terms where any table in the mask has zero delta rows.
	// A join with an empty input always produces zero rows.
	bool skip_empty_enabled = SqlUtils::GetBoolSetting(context, "openivm_skip_empty_deltas", true);
	// Compile-only plans also execute against caller-local and future deltas that
	// the metadata connection cannot see. Only constant leaves are known empty;
	// use that same inventory for term pruning and kept-outer-join guards below.
	uint64_t known_empty_mask = compile_only ? delta_status.constant_mask : delta_status.empty_mask;
	uint64_t empty_mask = skip_empty_enabled ? known_empty_mask : 0;

	Connection key_probe_con(*context.db);
	// Key-domain pruning can erase the last remaining term when exactly one input
	// changed and its delta keys cannot match the unchanged side. That is the
	// important performance case covered by mv_inner_join. When multiple leaves
	// changed in a small join with many pending delta rows, these probes are extra
	// EXISTS joins on top of work we will still have to do, and the flag benchmark
	// shows that overhead can dominate. Keep probing for single-source changes and
	// for tiny multi-source changes where the probe is cheap.
	bool all_non_empty_deltas_are_tiny = non_empty_mask && ((non_empty_mask & ~delta_status.tiny_mask) == 0);
	bool key_domain_probe_enabled =
	    skip_empty_enabled && !has_left_join &&
	    (non_empty_leaf_count == 1 || all_non_empty_deltas_are_tiny); // mull-ignore: cxx_eq_to_ne
	if (key_domain_probe_enabled) {
		// Probe committed watermarks in the metadata schema the compiler selected.
		RefreshMetadata::UseSameLocation(*input.context.metadata_con.context, key_probe_con);
	}

	uint64_t pruned_count = 0;
	map<pair<size_t, idx_t>, idx_t> transition_cte_indexes;
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Building inclusion-exclusion terms (%lu total, skip_bits=%lu, empty_mask=%lu, "
	                    "known_empty_mask=%lu)\n",
	                    (unsigned long)total_terms, (unsigned long)skip_bits, (unsigned long)empty_mask,
	                    (unsigned long)known_empty_mask);
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
			// The <= mutant indexes one past key_probes and leaf_refs.
			// mull-ignore-next: cxx_lt_to_le
			for (size_t i = 0; i < N && !key_domain_empty; i++) {
				if (!(mask & (1ULL << i)) || key_probes[i].empty() || leaf_refs[i].last_update.empty()) {
					continue;
				}
				for (auto &probe : key_probes[i]) {
					if (leaf_refs[probe.other_leaf].last_update.empty()) {
						continue;
					}
					bool other_is_delta = mask & (1ULL << probe.other_leaf);
					// A probe always references a different leaf, so > and >= are equivalent here.
					// Reversing the orientation only repeats or omits the same symmetric emptiness probe.
					if (other_is_delta && i > probe.other_leaf) { // mull-ignore: cxx_gt_to_ge,cxx_gt_to_le
						continue;
					}
					if (!DeltaKeyHasMatch(key_probe_con, leaf_refs[i], probe.delta_column, leaf_refs[probe.other_leaf],
					                      probe.other_column, other_is_delta)) {
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
			DemoteOuterJoinsForMask(term.get(), leaves, mask, input.context.model.type);
		}

		// Replace delta leaves
		// The <= mutant indexes one past leaves and the copied join tree.
		// mull-ignore-next: cxx_lt_to_le
		for (size_t i = 0; i < N; i++) {
			if (mask & (1ULL << i)) {
				auto &leaf_node_ref = GetNodeAtPath(term, leaves[i].path);
				auto original_leaf_width = leaf_node_ref->GetColumnBindings().size();
				auto delta_i = CompileRegularLeafDelta(input, context, binder, leaf_node_ref, term_root, source_cache);
				mul_bindings.push_back(delta_i.mul_binding);
				leaf_node_ref = std::move(delta_i.op);
				UpdateParentProjectionMap(term, leaves[i], delta_i.mul_binding, original_leaf_width);
			}
		}

		// Guard kept (un-demoted) outer joins AFTER delta leaves are replaced: the mask-driven side
		// (e.g. Δ(P1) via CompileCopiedSubtree) must already be its final compiled form before we wrap
		// it in an anti-join -- doing this earlier corrupts the leaf-replacement step above, which would
		// otherwise try to compute a delta of our anti-join wrapper instead of the original subtree.
		if (has_left_join) {
			uint64_t leaf_has_delta_mask = (~known_empty_mask) & total_terms;
			if (leaf_has_delta_mask) {
				bool portable_anti_guard = compile_facts.target_dialect != SqlDialect::DUCKDB;
				GuardKeptOuterJoinsForMask(context, input.context.metadata_con, binder, term.get(), leaves,
				                           leaf_has_delta_mask, input.context.view, portable_anti_guard,
				                           transition_cte_indexes, transition_ctes);
			}
		}

		terms.push_back(FinalizeJoinTerm(binder, std::move(term),
		                                 RenumberBindings(existing_multiplicity_bindings, renumbered.idx_map),
		                                 mul_bindings, input.mul_type, true));
	}
	// This condition only suppresses an empty debug message; it cannot affect generated terms.
	// mull-ignore-next
	if (pruned_count > 0) {
		OPENIVM_DEBUG_PRINT("[DeltaJoin] FK pruning: %lu/%lu terms pruned, %lu remaining\n",
		                    (unsigned long)pruned_count, (unsigned long)total_terms, (unsigned long)terms.size());
	}
	return terms;
}

static bool SupportsRegularNtermLeaf(const JoinLeafInfo &leaf) {
	if (leaf.get) {
		// Join leaves created from catalog scans always own a table function; a null pointer is invalid planner state.
		return leaf.get->GetTable().get() != nullptr; // mull-ignore: cxx_ne_to_eq
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
	D_ASSERT(current_types.size() == current_bindings.size());
	idx_t current_type_idx = 0;
	for (auto &binding : current_bindings) {
		current_exprs.push_back(make_uniq<BoundColumnRefExpression>(current_types[current_type_idx++], binding));
	}
	current_exprs.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(1)));
	auto current_projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(current_exprs));
	current_projection->children.push_back(std::move(current_node));
	current_projection->ResolveOperatorTypes();

	delta.op->ResolveOperatorTypes();
	auto delta_bindings = delta.op->GetColumnBindings();
	auto delta_types = delta.op->types;
	D_ASSERT(delta_bindings.size() == current_bindings.size() + 1);
	delta_bindings.pop_back();
	delta_types.pop_back();
	vector<unique_ptr<Expression>> delta_exprs;
	idx_t delta_idx = 0;
	for (auto &delta_binding : delta_bindings) {
		delta_exprs.push_back(make_uniq<BoundColumnRefExpression>(delta_types[delta_idx], delta_binding));
		delta_idx++;
	}
	delta_exprs.push_back(MultiplyIntegers(binder, make_uniq<BoundConstantExpression>(Value::INTEGER(-1)),
	                                       make_uniq<BoundColumnRefExpression>(mul_type, delta.mul_binding)));
	auto delta_projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(delta_exprs));
	delta_projection->children.push_back(std::move(delta.op));
	delta_projection->ResolveOperatorTypes();

	auto old_types = current_projection->types;
	auto old_node = make_uniq<LogicalSetOperation>(output_table_index, old_types.size(), std::move(current_projection),
	                                               std::move(delta_projection), LogicalOperatorType::LOGICAL_UNION,
	                                               true /* setop_all */);
	old_node->types = std::move(old_types);
	old_node->ResolveOperatorTypes();
	return {std::move(old_node),
	        ColumnBinding(TableIndex(output_table_index), ProjectionIndex(current_bindings.size()))};
}

DeltaPlanFragment CompileRegularLeafDelta(const DeltaOperatorInput &input, ClientContext &context, Binder &binder,
                                          unique_ptr<LogicalOperator> &leaf_node, LogicalOperator *&term_root,
                                          DeltaSourceCache &source_cache) {
	// leaf_node must be the term's own renumbered copy, never JoinLeafInfo::get/node from the original plan: those
	// carry pre-renumbering table indexes, and the delta scan reuses the GET's table_index verbatim.
	if (leaf_node->type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = leaf_node->Cast<LogicalGet>();
		auto original_bindings = leaf_node->GetColumnBindings();
		auto delta = CreateDeltaGetNode(context, input.context.metadata_con, binder, &get, input.context.view,
		                                &source_cache, true);
		auto delta_bindings = delta.node->GetColumnBindings();
		if (delta_bindings.size() != original_bindings.size() + 1) {
			throw InternalException("DeltaJoin: projected delta leaf produced %zu bindings for %zu source columns",
			                        delta_bindings.size(), original_bindings.size());
		}
		// DuckDB 2.0 can expose projected GET bindings using their sparse physical
		// positions (for example 1,2,3 when position 0 is filter-only). Delta
		// compaction emits contiguous bindings. Rebind conditions and expressions
		// above the leaf before replacing it; join projection maps are positional and
		// remain the responsibility of UpdateParentProjectionMap.
		ColumnBindingReplacer replacer;
		for (idx_t i = 0; i < original_bindings.size(); i++) {
			replacer.replacement_bindings.emplace_back(original_bindings[i], delta_bindings[i]);
		}
		replacer.stop_operator = leaf_node;
		replacer.VisitOperator(*term_root);
		return {std::move(delta.node), delta.mul_binding};
	}
	return input.CompileCopiedSubtree(leaf_node, term_root);
}

static vector<unique_ptr<LogicalOperator>>
BuildRegularJoinTerms(DeltaOperatorInput input, ClientContext &context, Binder &binder,
                      const vector<JoinLeafInfo> &leaves, uint64_t unchanged_mask, bool has_left_join,
                      const vector<ColumnBinding> &existing_multiplicity_bindings, DeltaSourceCache &source_cache) {
	vector<unique_ptr<LogicalOperator>> terms;
	// Base scans see post-DML state. Term i uses current state before i, delta i, and reconstructs old state after i as
	// current - delta. These disjoint telescoping terms cover every non-empty delta combination exactly once.
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Building regular N-term telescoping delta (%zu leaves)\n", leaves.size());
	bool all_unchanged = unchanged_mask == ((1ULL << leaves.size()) - 1);
	size_t empty_delta_leaf = 0;
	if (all_unchanged) {
		// The <= mutant indexes one past leaves.
		// Choosing another unchanged catalog leaf as the sole empty arm is algebraically equivalent.
		// mull-ignore-next: cxx_lt_to_le,cxx_lt_to_ge
		for (size_t leaf = 0; leaf < leaves.size(); leaf++) {
			auto *get = GetLeafScan(leaves[leaf]);
			if (get && get->GetTable().get() != nullptr) { // mull-ignore: cxx_ne_to_eq
				empty_delta_leaf = leaf;
				break;
			}
		}
	}

	for (size_t delta_leaf = 0; delta_leaf < leaves.size(); delta_leaf++) {
		if ((unchanged_mask & (1ULL << delta_leaf)) && (!all_unchanged || delta_leaf != empty_delta_leaf)) {
			OPENIVM_DEBUG_PRINT("[DeltaJoin] Skipping regular N-term leaf %zu: unchanged or source-independent\n",
			                    delta_leaf);
			continue;
		}
		auto term = input.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);
		LogicalOperator *term_root = term.get();
		// LEFT-JOIN telescoping: demote only the outer join(s) whose NULL-supplying
		// subtree contains this term's single delta leaf, mirroring the DuckLake
		// N-term path (DemoteOuterJoinsForMask). Preserved outer joins elsewhere keep
		// their NULL-padded rows; the upsert layer's key-based partial recompute
		// (BuildLeftJoinProjectionRefresh) fixes NULL<->match transition rows.
		if (has_left_join) {
			DemoteOuterJoinsForMask(term.get(), leaves, (1ULL << delta_leaf), input.context.model.type);
		}

		vector<ColumnBinding> mul_bindings;

		for (size_t leaf = 0; leaf < leaves.size(); leaf++) {
			auto &leaf_node = GetNodeAtPath(term, leaves[leaf].path);
			if (leaf == delta_leaf) {
				auto original_leaf_width = leaf_node->GetColumnBindings().size();
				auto delta = CompileRegularLeafDelta(input, context, binder, leaf_node, term_root, source_cache);
				mul_bindings.push_back(delta.mul_binding);
				leaf_node = std::move(delta.op);
				UpdateParentProjectionMap(term, leaves[leaf], delta.mul_binding, original_leaf_width);
				continue;
			}
			// leaf == delta_leaf continued above, so < and <= are identical here.
			if (leaf < delta_leaf) { // mull-ignore: cxx_lt_to_le
				continue;
			}
			if (unchanged_mask & (1ULL << leaf)) {
				continue;
			}
			auto original_leaf_width = leaf_node->GetColumnBindings().size();
			auto delta_source = leaf_node->Copy(context);
			auto delta_renumbered = renumber_and_rebind_subtree(std::move(delta_source), binder);
			delta_source = std::move(delta_renumbered.op);
			LogicalOperator *delta_root = delta_source.get();
			auto delta = CompileRegularLeafDelta(input, context, binder, delta_source, delta_root, source_cache);
			auto old = CreateRegularOldNode(binder, std::move(leaf_node), std::move(delta), input.mul_type);
			mul_bindings.push_back(old.mul_binding);
			leaf_node = std::move(old.op);
			UpdateParentProjectionMap(term, leaves[leaf], old.mul_binding, original_leaf_width);
		}

		terms.push_back(FinalizeJoinTerm(binder, std::move(term),
		                                 RenumberBindings(existing_multiplicity_bindings, renumbered.idx_map),
		                                 mul_bindings, input.mul_type, false));
	}
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Regular N-term count: %zu\n", terms.size());
	return terms;
}

DeltaPlanFragment CompileJoinDelta(DeltaOperatorInput input) {
	ClientContext &context = input.context.input.context;
	Binder &binder = input.context.input.optimizer.binder;
	input.plan->ResolveOperatorTypes();
	const vector<ColumnBinding> all_original_bindings = input.plan->GetColumnBindings();
	bool ducklake_nterm_enabled = SqlUtils::GetBoolSetting(context, "openivm_ducklake_nterm", true);
	bool collect_ducklake_leaves = ducklake_nterm_enabled && input.context.model.type == RefreshType::SIMPLE_PROJECTION;
	JoinPlanFacts join_facts;
	vector<size_t> join_path;
	CollectJoinPlanFacts(input.plan.get(), join_path, false, true, collect_ducklake_leaves, join_facts);
	vector<ColumnBinding> original_bindings;
	vector<LogicalType> output_types;
	FilterInternalMultiplicityColumns(all_original_bindings, input.plan->types,
	                                  join_facts.existing_multiplicity_binding_keys, original_bindings, output_types);

	// 1. Verify + collect
	bool has_left_join = join_facts.has_outer_join;
	vector<JoinLeafInfo> leaves = std::move(join_facts.leaves);
	size_t N = leaves.size();
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Rewriting JOIN node, %zu leaves found\n", N);

	if (N == 0) {
		throw InternalException("DeltaJoin: no leaves found in join tree");
	}
	// MAX_JOIN_TABLES is inclusive; >= would reject the documented 16-table boundary.
	if (N > openivm::MAX_JOIN_TABLES) { // mull-ignore: cxx_gt_to_ge
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
	string ducklake_fallback_reason;
	if (!ducklake_nterm_enabled) {
		all_ducklake = false; // forced to inclusion-exclusion
		ducklake_fallback_reason = "openivm_ducklake_nterm is disabled";
	} else {
		if (collect_ducklake_leaves && join_facts.ducklake_fallback_reason.empty() &&
		    !join_facts.ducklake_leaves.empty()) {
			// A false positive only replaces the leaf list with an equivalent flattened list.
			bool has_wrapped_leaf = join_facts.ducklake_leaves.size() != leaves.size(); // mull-ignore: cxx_ne_to_eq
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
				leaves = std::move(join_facts.ducklake_leaves);
				N = leaves.size();
			}
		} else if (!collect_ducklake_leaves) {
			// This reason is diagnostic only; it does not select the fallback path.
			ducklake_fallback_reason = "refresh type is outside SIMPLE_PROJECTION scope";
		} else {
			ducklake_fallback_reason = join_facts.ducklake_fallback_reason;
		}
		// The <= mutant indexes one past leaves.
		// mull-ignore-next: cxx_lt_to_le
		for (size_t i = 0; i < N; i++) {
			auto *get = GetLeafScan(leaves[i]);
			if (!get || get->function.GetName().GetIdentifierName() != "ducklake_scan") {
				all_ducklake = false;
				break;
			}
		}
	}
	if (!flattened_ducklake && !ducklake_fallback_reason.empty()) {
		OPENIVM_DEBUG_PRINT("[DuckLakeJoin] Flattening fallback: %s\n", ducklake_fallback_reason.c_str());
	}
	// DuckLake flattening may change N, but the same inclusive boundary applies.
	if (N > openivm::MAX_JOIN_TABLES) { // mull-ignore: cxx_gt_to_ge
		throw NotImplementedException("IVM not supported for joins with more than 16 tables");
	}
	auto compile_facts = openivm::CompileFactsContextSlot::Get(context);
	auto unchanged_mask = ComputeFactsUnchangedMask(compile_facts, leaves);
	vector<openivm::CachedConstraint> declarations;
	if (SqlUtils::GetBoolSetting(context, "openivm_fk_pruning", true) && compile_facts.fk_relations.empty()) {
		case_insensitive_set_t sources;
		for (auto &leaf : leaves) {
			auto get = GetLeafScan(leaf);
			if (get && get->GetTable()) {
				auto &table = *get->GetTable();
				sources.insert(SqlUtils::FullName(table.catalog.GetName(), table.schema.name, table.name));
			}
		}
		declarations = openivm::ReadConstraintDeclarations(context, sources);
	}
	// Equality probes between leaf columns, shared by FK detection and key-domain pruning. The DuckLake path only
	// traces bare scan leaves.
	auto key_probes = BuildJoinKeyProbes(input.plan.get(), leaves, !all_ducklake);
	bool regular_nterm_base = !all_ducklake && compile_facts.compile_only &&
	                          input.context.model.type == RefreshType::SIMPLE_PROJECTION &&
	                          SqlUtils::GetBoolSetting(context, "openivm_regular_nterm", true);
	bool regular_nterm;
	if (has_left_join) {
		// One-sided outer-join telescoping demotes only the join whose NULL-supplying
		// side carries that term's delta (see BuildRegularJoinTerms). FULL OUTER shapes and
		// the inclusion-exclusion FK-pruning path are out of scope; NULL-padded row
		// correctness is completed by BuildLeftJoinProjectionRefresh in the upsert layer.
		regular_nterm = regular_nterm_base && join_facts.only_inner_or_one_sided_outer_joins &&
		                SqlUtils::GetBoolSetting(context, "openivm_regular_nterm_left", true);
	} else {
		regular_nterm = regular_nterm_base && join_facts.only_inner_joins &&
		                RegularNtermPreservesFKPruning(context, compile_facts, leaves, key_probes, declarations);
	}
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
	// Source watermarks and delta tables are read once per compile, not once per term and leaf.
	DeltaSourceCache source_cache;
	if (all_ducklake) {
		terms = BuildDuckLakeJoinTerms(input, context, binder, leaves, has_left_join, flattened_ducklake, key_probes,
		                               source_cache);
	} else if (regular_nterm) {
		terms = BuildRegularJoinTerms(input, context, binder, leaves, unchanged_mask, has_left_join,
		                              join_facts.existing_multiplicity_bindings, source_cache);
	} else {
		terms = BuildInclusionExclusionTerms(input, context, binder, leaves, has_left_join,
		                                     join_facts.existing_multiplicity_bindings, transition_ctes, declarations,
		                                     key_probes, source_cache);
	}

	// 4. UNION ALL
	auto result = AssembleJoinUnionAll(terms, types, binder);
	for (auto definition = transition_ctes.rbegin(); definition != transition_ctes.rend(); definition++) {
		result = make_uniq<LogicalMaterializedCTE>(Identifier(definition->name), definition->cte_index,
		                                           definition->types.size(), std::move(definition->node),
		                                           std::move(result), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
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
	                    (unsigned long)new_mul_binding.table_index.index,
	                    (unsigned long)new_mul_binding.column_index.GetIndex());
	return {std::move(input.plan), new_mul_binding};
}

} // namespace duckdb
