#include "ivm_join_rule.hpp"
#include "openivm_debug.hpp"
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

	if (N > 16) {
		throw NotImplementedException("Inclusion-exclusion IVM not supported for joins with more than 16 tables");
	}

	// Output type: original columns + multiplicity
	auto types = pw.plan->types;
	types.emplace_back(pw.mul_type);

	// 2. Build 2^N - 1 terms (inclusion-exclusion)
	//
	// For each non-empty subset S of {0..N-1}:
	//   - Replace leaf i with delta(T_i) for i in S
	//   - Keep T_new for leaves not in S
	//   - Combined multiplicity = XOR of all delta multiplicities
	//     (this accounts for the (-1)^(|S|-1) sign in inclusion-exclusion)
	vector<unique_ptr<LogicalOperator>> terms;

	for (uint64_t mask = 1; mask < (1ULL << N); mask++) {
		auto term = pw.plan->Copy(context);
		vector<ColumnBinding> mul_bindings;

		for (size_t i = 0; i < N; i++) {
			if (mask & (1ULL << i)) {
				DeltaGetResult delta_i = CreateDeltaGetNode(context, leaves[i].get, pw.view);
				mul_bindings.push_back(delta_i.mul_binding);
				get_node_at_path(term, leaves[i].path) = std::move(delta_i.node);
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
		unique_ptr<Expression> combined_mul =
		    make_uniq<BoundColumnRefExpression>(pw.mul_type, mul_bindings[0]);
		for (size_t i = 1; i < mul_bindings.size(); i++) {
			auto next = make_uniq<BoundColumnRefExpression>(pw.mul_type, mul_bindings[i]);
			combined_mul = make_uniq<BoundComparisonExpression>(
			    ExpressionType::COMPARE_NOTEQUAL, std::move(combined_mul), std::move(next));
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
