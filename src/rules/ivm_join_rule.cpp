#include "rules/ivm_join_rule.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "upsert/openivm_index_regen.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace {

using duckdb::ColumnBinding;
using duckdb::Expression;
using duckdb::LogicalComparisonJoin;
using duckdb::LogicalGet;
using duckdb::LogicalOperator;
using duckdb::unique_ptr;
using duckdb::vector;

struct JoinLeafInfo {
	vector<size_t> path;
	LogicalGet *get;            // non-null for simple table scans
	LogicalOperator *node;      // always set; for non-GET leaves, rewrite the subtree
	bool is_right_of_left_join; // true if this leaf is on the RIGHT side of a LEFT JOIN
};

void CollectJoinLeaves(LogicalOperator *node, vector<size_t> path, vector<JoinLeafInfo> &leaves,
                        bool is_right_of_left = false) {
	if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    node->type == duckdb::LogicalOperatorType::LOGICAL_CROSS_PRODUCT) {
		bool is_left = false;
		bool is_right = false;
		if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
			is_left = (join && join->join_type == duckdb::JoinType::LEFT);
			is_right = (join && join->join_type == duckdb::JoinType::RIGHT);
		}
		path.push_back(0);
		CollectJoinLeaves(node->children[0].get(), path, leaves, is_right_of_left || is_right);
		path.pop_back();
		path.push_back(1);
		CollectJoinLeaves(node->children[1].get(), path, leaves, is_right_of_left || is_left);
		path.pop_back();
	} else if (node->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
		leaves.push_back({path, dynamic_cast<LogicalGet *>(node), node, is_right_of_left});
	} else {
		leaves.push_back({path, nullptr, node, is_right_of_left});
	}
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
bool VerifyJoinTypes(LogicalOperator *node) {
	bool has_left = false;
	if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
		if (join->join_type == duckdb::JoinType::LEFT || join->join_type == duckdb::JoinType::RIGHT) {
			has_left = true;
		} else if (join->join_type != duckdb::JoinType::INNER) {
			throw duckdb::Exception(duckdb::ExceptionType::OPTIMIZER,
			                        duckdb::JoinTypeToString(join->join_type) + " type not yet supported in OpenIVM");
		}
	}
	for (auto &child : node->children) {
		if (VerifyJoinTypes(child.get())) {
			has_left = true;
		}
	}
	return has_left;
}

/// Demote LEFT/RIGHT JOINs to INNER in a subtree (used when only right-side deltas exist).
void DemoteLeftJoins(LogicalOperator *node) {
	if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *j = dynamic_cast<LogicalComparisonJoin *>(node);
		if (j && (j->join_type == duckdb::JoinType::LEFT || j->join_type == duckdb::JoinType::RIGHT)) {
			j->join_type = duckdb::JoinType::INNER;
		}
	}
	for (auto &child : node->children) {
		DemoteLeftJoins(child.get());
	}
}

/// Update the parent JOIN's projection map to include the new multiplicity column
/// after a subtree replacement. Without this, the mul binding gets silently dropped.
void UpdateParentProjectionMap(unique_ptr<LogicalOperator> &term, const JoinLeafInfo &leaf) {
	if (leaf.path.empty()) {
		return;
	}
	size_t child_side = leaf.path.back();
	unique_ptr<LogicalOperator> *parent = &term;
	for (size_t s = 0; s + 1 < leaf.path.size(); s++) {
		parent = &((*parent)->children[leaf.path[s]]);
	}
	if ((*parent)->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>((*parent).get());
		if (join) {
			auto &proj_map = (child_side == 0) ? join->left_projection_map : join->right_projection_map;
			if (!proj_map.empty()) {
				duckdb::idx_t mul_idx = leaf.node->GetColumnBindings().size();
				proj_map.push_back(mul_idx);
				OPENIVM_DEBUG_PRINT("[IvmJoinRule] Added mul col %lu to %s proj_map\n", (unsigned long)mul_idx,
				                    child_side == 0 ? "left" : "right");
			}
		}
	}
}

} // namespace

namespace duckdb {

// ============================================================================
// BuildInclusionExclusionTerms: create 2^N - 1 delta terms
// ============================================================================
static vector<unique_ptr<LogicalOperator>> BuildInclusionExclusionTerms(PlanWrapper &pw, ClientContext &context,
                                                                         Binder &binder,
                                                                         const vector<JoinLeafInfo> &leaves,
                                                                         bool has_left_join) {
	size_t N = leaves.size();
	vector<unique_ptr<LogicalOperator>> terms;

	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Building %lu inclusion-exclusion terms\n", (unsigned long)((1ULL << N) - 1));
	for (uint64_t mask = 1; mask < (1ULL << N); mask++) {
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
					DeltaGetResult delta_i = CreateDeltaGetNode(context, leaves[i].get, pw.view);
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

	// 3. Build terms
	auto terms = BuildInclusionExclusionTerms(pw, context, binder, leaves, has_left_join);

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
