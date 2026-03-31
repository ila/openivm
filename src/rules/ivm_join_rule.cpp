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

#include <functional>

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

void collect_join_leaves(LogicalOperator *node, vector<size_t> path, vector<JoinLeafInfo> &leaves,
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
		collect_join_leaves(node->children[0].get(), path, leaves, is_right_of_left || is_right);
		path.pop_back();
		path.push_back(1);
		collect_join_leaves(node->children[1].get(), path, leaves, is_right_of_left || is_left);
		path.pop_back();
	} else if (node->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
		leaves.push_back({path, dynamic_cast<LogicalGet *>(node), node, is_right_of_left});
	} else {
		leaves.push_back({path, nullptr, node, is_right_of_left});
	}
}

unique_ptr<LogicalOperator> &get_node_at_path(unique_ptr<LogicalOperator> &root, const vector<size_t> &path) {
	unique_ptr<LogicalOperator> *current = &root;
	for (size_t step : path) {
		current = &((*current)->children[step]);
	}
	return *current;
}

void resolve_types_bottom_up(unique_ptr<LogicalOperator> &node) {
	for (auto &child : node->children) {
		resolve_types_bottom_up(child);
	}
	node->ResolveOperatorTypes();
}


} // namespace

namespace duckdb {

ModifiedPlan IvmJoinRule::Rewrite(PlanWrapper pw) {
	ClientContext &context = pw.input.context;
	Binder &binder = pw.input.optimizer.binder;
	const vector<ColumnBinding> original_bindings = pw.plan->GetColumnBindings();

	// Verify all joins are INNER or LEFT
	bool has_left_join = false;
	std::function<void(LogicalOperator *)> verify_joins = [&](LogicalOperator *node) {
		if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
			if (join->join_type == JoinType::LEFT || join->join_type == JoinType::RIGHT) {
				has_left_join = true;
			} else if (join->join_type != JoinType::INNER) {
				throw Exception(ExceptionType::OPTIMIZER,
				                JoinTypeToString(join->join_type) + " type not yet supported in OpenIVM");
			}
		}
		// CROSS_PRODUCT is an unconditional INNER JOIN — always supported
		for (auto &child : node->children) {
			verify_joins(child.get());
		}
	};
	verify_joins(pw.plan.get());

	// 1. Collect all leaf GET nodes
	vector<JoinLeafInfo> leaves;
	collect_join_leaves(pw.plan.get(), {}, leaves);
	size_t N = leaves.size();
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Rewriting JOIN node, %zu leaves found\n", N);
	for (size_t i = 0; i < N; i++) {
		if (leaves[i].get) {
			OPENIVM_DEBUG_PRINT("[IvmJoinRule]   Leaf %zu: GET table_index=%lu, path_depth=%zu\n", i,
			                    (unsigned long)leaves[i].get->table_index, leaves[i].path.size());
		} else {
			OPENIVM_DEBUG_PRINT("[IvmJoinRule]   Leaf %zu: subtree (%s), path_depth=%zu\n", i,
			                    LogicalOperatorToString(leaves[i].node->type).c_str(), leaves[i].path.size());
		}
	}

	if (N > ivm::MAX_JOIN_TABLES) {
		throw NotImplementedException("Inclusion-exclusion IVM not supported for joins with more than 16 tables");
	}

	// Build output types: original join columns + multiplicity.
	// ResolveOperatorTypes clears and rebuilds correctly (base class clears first).
	pw.plan->ResolveOperatorTypes();
	auto types = pw.plan->types;
	D_ASSERT(types.size() == original_bindings.size());
	types.emplace_back(pw.mul_type);
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] types.size()=%zu, original_bindings.size()=%zu\n", types.size(),
	                    original_bindings.size());

	// 2. Build 2^N - 1 terms (inclusion-exclusion)
	//
	// For each non-empty subset S of {0..N-1}:
	//   - Replace leaf i with delta(T_i) for i in S
	//   - Keep T_new for leaves not in S
	//   - Combined multiplicity = XOR of all delta multiplicities
	//     (this accounts for the (-1)^(|S|-1) sign in inclusion-exclusion)
	vector<unique_ptr<LogicalOperator>> terms;

	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Building %lu inclusion-exclusion terms\n", (unsigned long)((1ULL << N) - 1));
	for (uint64_t mask = 1; mask < (1ULL << N); mask++) {
		auto term = pw.plan->Copy(context);
		auto renumbered = renumber_and_rebind_subtree(std::move(term), binder);
		term = std::move(renumbered.op);
		LogicalOperator *term_root = term.get();
		vector<ColumnBinding> mul_bindings;

		// For LEFT JOINs: determine if ONLY right-side leaves are in this term's mask.
		// If so, change LEFT→INNER to avoid spurious NULL-extended rows from `R LEFT JOIN delta_S`.
		// LEFT JOIN semantics only apply when the LEFT side has delta rows.
		if (has_left_join) {
			bool has_left_side_delta = false;
			for (size_t i = 0; i < N; i++) {
				if ((mask & (1ULL << i)) && !leaves[i].is_right_of_left_join) {
					has_left_side_delta = true;
					break;
				}
			}
			if (!has_left_side_delta) {
				// Only right-side deltas → demote LEFT JOINs to INNER in this term
				std::function<void(LogicalOperator *)> demote_left = [&](LogicalOperator *node) {
					if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
						auto *j = dynamic_cast<LogicalComparisonJoin *>(node);
						if (j && (j->join_type == JoinType::LEFT || j->join_type == JoinType::RIGHT)) {
							j->join_type = JoinType::INNER;
						}
					}
					for (auto &child : node->children) {
						demote_left(child.get());
					}
				};
				demote_left(term.get());
			}
		}

		for (size_t i = 0; i < N; i++) {
			if (mask & (1ULL << i)) {
				if (leaves[i].get) {
					// Simple GET leaf: replace with delta scan
					DeltaGetResult delta_i = CreateDeltaGetNode(context, leaves[i].get, pw.view);
					mul_bindings.push_back(delta_i.mul_binding);
					get_node_at_path(term, leaves[i].path) = std::move(delta_i.node);
				} else {
					// Complex subtree leaf (UNION, etc.): rewrite the entire subtree
					auto &subtree_ref = get_node_at_path(term, leaves[i].path);
					auto rewritten = IVMRewriteRule::RewritePlan(pw.input, subtree_ref, pw.view, term_root);
					mul_bindings.push_back(rewritten.mul_binding);
					subtree_ref = std::move(rewritten.op);
				}
				// The rewritten subtree has 1 extra column (multiplicity) at the end.
				// If the parent is a COMPARISON_JOIN with a projection map, the map won't
				// include this new column — add it so the mul binding flows through the join.
				if (!leaves[i].path.empty()) {
					size_t child_side = leaves[i].path.back();
					unique_ptr<LogicalOperator> *parent = &term;
					for (size_t s = 0; s + 1 < leaves[i].path.size(); s++) {
						parent = &((*parent)->children[leaves[i].path[s]]);
					}
					if ((*parent)->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
						auto *join = dynamic_cast<LogicalComparisonJoin *>((*parent).get());
						if (join) {
							auto &proj_map = (child_side == 0) ? join->left_projection_map
							                                    : join->right_projection_map;
							if (!proj_map.empty()) {
								idx_t mul_idx = leaves[i].node->GetColumnBindings().size();
								proj_map.push_back(mul_idx);
								OPENIVM_DEBUG_PRINT("[IvmJoinRule] Added mul col %lu to %s proj_map\n",
								                    (unsigned long)mul_idx,
								                    child_side == 0 ? "left" : "right");
							}
						}
					}
				}
			}
		}

		resolve_types_bottom_up(term);

		// Build projection: original columns + combined multiplicity
		auto term_bindings = term->GetColumnBindings();
		auto term_types = term->types;

		vector<unique_ptr<Expression>> proj_exprs;

		// Add non-multiplicity columns
		for (idx_t i = 0; i < term_bindings.size(); i++) {
			bool is_mul = false;
			for (auto &mb : mul_bindings) {
				if (term_bindings[i] == mb) {
					is_mul = true;
					break;
				}
			}
			if (!is_mul) {
				proj_exprs.push_back(make_uniq<BoundColumnRefExpression>(term_types[i], term_bindings[i]));
			}
		}

		// Build combined multiplicity: XOR chain of all delta multiplicities
		// XOR(a, b) for booleans = (a != b)
		// This naturally handles the inclusion-exclusion sign:
		//   sign * product(mul_i) in {+1,-1} space = XOR(mul_i) in boolean space
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

	// 3. UNION ALL all terms
	auto result = std::move(terms[0]);
	for (size_t i = 1; i < terms.size(); i++) {
		auto union_table_index = binder.GenerateTableIndex();
		result = make_uniq<LogicalSetOperation>(union_table_index, types.size(), std::move(result), std::move(terms[i]),
		                                        LogicalOperatorType::LOGICAL_UNION, true);
		result->types = types;
	}

	// 3b. Add a clean projection on top to disambiguate column names for LPTS.
	// The UNION ALL of inclusion-exclusion terms can produce duplicate column names
	// (e.g., two "_duckdb_ivm_multiplicity" columns) which confuses name-based SQL generation.
	{
		auto union_bindings = result->GetColumnBindings();
		vector<unique_ptr<Expression>> clean_exprs;
		for (idx_t i = 0; i < union_bindings.size(); i++) {
			clean_exprs.push_back(make_uniq<BoundColumnRefExpression>(types[i], union_bindings[i]));
		}
		auto clean_proj = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(clean_exprs));
		clean_proj->children.push_back(std::move(result));
		clean_proj->ResolveOperatorTypes();
		result = std::move(clean_proj);
	}

	// 4. Update column bindings in parent
	// The result has the original join columns + multiplicity (last column).
	// Map old bindings to new bindings positionally. The plan structure may vary
	// (CTE-inlined plans can have different intermediate column counts), so we
	// only require the result to have at least 2 bindings (columns + mul).
	ColumnBinding new_mul_binding;
	{
		auto union_bindings = result->GetColumnBindings();
		if (union_bindings.size() < 2) {
			throw InternalException("Join rewrite produced too few bindings (%zu)", union_bindings.size());
		}
		ColumnBindingReplacer replacer;
		vector<ReplacementBinding> &replacement_bindings = replacer.replacement_bindings;
		idx_t map_count = std::min(original_bindings.size(), union_bindings.size() - 1);
		OPENIVM_DEBUG_PRINT("[IvmJoinRule] Binding replacement: %zu mappings (original=%zu, union=%zu)\n",
		                    map_count, original_bindings.size(), union_bindings.size());
		for (idx_t col_idx = 0; col_idx < map_count; ++col_idx) {
			OPENIVM_DEBUG_PRINT("[IvmJoinRule]   [%zu] (%lu,%lu) → (%lu,%lu)\n", col_idx,
			                    (unsigned long)original_bindings[col_idx].table_index,
			                    (unsigned long)original_bindings[col_idx].column_index,
			                    (unsigned long)union_bindings[col_idx].table_index,
			                    (unsigned long)union_bindings[col_idx].column_index);
			replacement_bindings.emplace_back(original_bindings[col_idx], union_bindings[col_idx]);
		}
		replacer.stop_operator = result;
		replacer.VisitOperator(*pw.root);
		new_mul_binding = union_bindings.back();
	}

	pw.plan = std::move(result);
	OPENIVM_DEBUG_PRINT("[IvmJoinRule] Done, %zu terms unioned, mul_binding: table=%lu col=%lu\n", terms.size(),
	                    (unsigned long)new_mul_binding.table_index, (unsigned long)new_mul_binding.column_index);
	return {std::move(pw.plan), new_mul_binding};
}

} // namespace duckdb
