#include "ivm_join_rule.hpp"
#include "openivm_debug.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

#include <functional>

namespace {

using duckdb::ColumnBinding;
using duckdb::Expression;
using duckdb::JoinCondition;
using duckdb::LogicalComparisonJoin;
using duckdb::LogicalGet;
using duckdb::LogicalOperator;
using duckdb::unique_ptr;
using duckdb::vector;

struct JoinLeafInfo {
	vector<size_t> path;
	LogicalGet *get;
};

void collect_join_leaves(LogicalOperator *node, vector<size_t> path, vector<JoinLeafInfo> &leaves) {
	if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		path.push_back(0);
		collect_join_leaves(node->children[0].get(), path, leaves);
		path.pop_back();
		path.push_back(1);
		collect_join_leaves(node->children[1].get(), path, leaves);
		path.pop_back();
	} else if (node->type == duckdb::LogicalOperatorType::LOGICAL_GET) {
		leaves.push_back({path, dynamic_cast<LogicalGet *>(node)});
	} else {
		throw duckdb::NotImplementedException("Unexpected node type in join subtree: " +
		                                      duckdb::LogicalOperatorToString(node->type));
	}
}

unique_ptr<LogicalOperator> &get_node_at_path(unique_ptr<LogicalOperator> &root, const vector<size_t> &path) {
	unique_ptr<LogicalOperator> *current = &root;
	for (size_t step : path) {
		current = &((*current)->children[step]);
	}
	return *current;
}

void rebind_bcr_if_needed(duckdb::BoundColumnRefExpression &bcr, const std::unordered_map<idx_t, idx_t> &idx_map) {
	if (idx_map.find(bcr.binding.table_index) != idx_map.end()) {
		bcr.binding.table_index = idx_map.at(bcr.binding.table_index);
	}
}

vector<JoinCondition> rebind_join_conditions(const vector<JoinCondition> &original_conditions,
                                             const std::unordered_map<idx_t, idx_t> &idx_map) {
	vector<JoinCondition> result;
	result.reserve(original_conditions.size());
	for (const JoinCondition &cond : original_conditions) {
		unique_ptr<Expression> left = cond.left->Copy();
		unique_ptr<Expression> right = cond.right->Copy();
		if (cond.left->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			rebind_bcr_if_needed(left->Cast<duckdb::BoundColumnRefExpression>(), idx_map);
		}
		if (cond.right->expression_class == duckdb::ExpressionClass::BOUND_COLUMN_REF) {
			rebind_bcr_if_needed(right->Cast<duckdb::BoundColumnRefExpression>(), idx_map);
		}
		JoinCondition new_cond;
		new_cond.left = std::move(left);
		new_cond.right = std::move(right);
		new_cond.comparison = cond.comparison;
		result.emplace_back(std::move(new_cond));
	}
	return result;
}

void rebind_all_conditions_in_tree(unique_ptr<LogicalOperator> &node,
                                   const std::unordered_map<idx_t, idx_t> &idx_map) {
	if (node->type == duckdb::LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto *join = dynamic_cast<LogicalComparisonJoin *>(node.get());
		join->conditions = rebind_join_conditions(join->conditions, idx_map);
	}
	for (auto &child : node->children) {
		rebind_all_conditions_in_tree(child, idx_map);
	}
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

	// Verify all joins are INNER
	std::function<void(LogicalOperator *)> verify_inner_joins = [&](LogicalOperator *node) {
		if (node->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto *join = dynamic_cast<LogicalComparisonJoin *>(node);
			if (join->join_type != JoinType::INNER) {
				throw Exception(ExceptionType::OPTIMIZER,
				                JoinTypeToString(join->join_type) + " type not yet supported in OpenIVM");
			}
			for (auto &child : node->children) {
				verify_inner_joins(child.get());
			}
		}
	};
	verify_inner_joins(pw.plan.get());

	// 1. Collect all leaf GET nodes
	vector<JoinLeafInfo> leaves;
	collect_join_leaves(pw.plan.get(), {}, leaves);
	size_t N = leaves.size();
	OPENIVM_DEBUG_PRINT("IvmJoinRule: %zu leaves found\n", N);

	// Output type: original columns + multiplicity
	auto types = pw.plan->types;
	types.emplace_back(pw.mul_type);

	// 2. Build N terms
	vector<unique_ptr<LogicalOperator>> terms;

	for (size_t i = 0; i < N; i++) {
		// Copy the original join subtree
		auto term = pw.plan->Copy(context);

		// Replace leaf i with its delta GET
		DeltaGetResult delta_i = CreateDeltaGetNode(context, leaves[i].get, pw.view);
		ColumnBinding mul_binding = delta_i.mul_binding;
		get_node_at_path(term, leaves[i].path) = std::move(delta_i.node);

		// Replace leaves j > i with T_old
		std::unordered_map<idx_t, idx_t> idx_map;
		for (size_t j = i + 1; j < N; j++) {
			auto t_old = BuildTableOld(context, leaves[j].get, pw.view, binder);
			idx_t t_old_table_idx = t_old->GetColumnBindings()[0].table_index;
			idx_map[leaves[j].get->table_index] = t_old_table_idx;
			get_node_at_path(term, leaves[j].path) = std::move(t_old);
		}

		// Rebind join conditions for T_old table index changes
		if (!idx_map.empty()) {
			rebind_all_conditions_in_tree(term, idx_map);
		}

		// Resolve types bottom-up
		resolve_types_bottom_up(term);

		// Add projection: original columns + mul at end
		auto term_bindings = term->GetColumnBindings();
		auto term_types = term->types;
		auto proj_exprs = ProjectMultiplicityToEnd(term_bindings, term_types, mul_binding);
		auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(proj_exprs));
		projection->children.push_back(std::move(term));
		projection->ResolveOperatorTypes();
		terms.push_back(std::move(projection));
	}

	// 3. UNION ALL all terms
	auto result = std::move(terms[0]);
	for (size_t i = 1; i < N; i++) {
		auto union_table_index = binder.GenerateTableIndex();
		result = make_uniq<LogicalSetOperation>(union_table_index, types.size(), std::move(result),
		                                        std::move(terms[i]), LogicalOperatorType::LOGICAL_UNION, true);
		result->types = types;
	}

	// 4. Update column bindings in parent
	ColumnBinding new_mul_binding;
	{
		auto union_bindings = result->GetColumnBindings();
		if (union_bindings.size() - original_bindings.size() != 1) {
			throw InternalException(
			    "Union (with multiplicity column) should have exactly 1 more binding than original join!");
		}
		ColumnBindingReplacer replacer;
		vector<ReplacementBinding> &replacement_bindings = replacer.replacement_bindings;
		for (idx_t col_idx = 0; col_idx < original_bindings.size(); ++col_idx) {
			replacement_bindings.emplace_back(original_bindings[col_idx], union_bindings[col_idx]);
		}
		replacer.stop_operator = result;
		replacer.VisitOperator(*pw.root);
		new_mul_binding = union_bindings.back();
	}

	pw.plan = std::move(result);
	return {std::move(pw.plan), new_mul_binding};
}

} // namespace duckdb
