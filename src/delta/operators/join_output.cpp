#include "delta/operators/join.hpp"

#include "core/openivm_debug.hpp"
#include "duckdb/function/function_binder.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_set_operation.hpp"

namespace duckdb {

unique_ptr<Expression> MultiplyIntegers(Binder &binder, unique_ptr<Expression> lhs, unique_ptr<Expression> rhs) {
	vector<unique_ptr<Expression>> args;
	args.push_back(std::move(lhs));
	args.push_back(std::move(rhs));
	ErrorData error;
	FunctionBinder function_binder(binder);
	auto product = function_binder.BindScalarFunction(DEFAULT_SCHEMA, "*", std::move(args), error, true);
	if (!product) {
		throw InternalException("DeltaJoin: failed to bind multiplicity '*': %s", error.RawMessage());
	}
	return product;
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
// Telescoping N-term deltas read disjoint states and therefore omit the sign.
unique_ptr<Expression> BuildJoinMultiplicityProduct(Binder &binder, const LogicalType &mul_type,
                                                    const vector<ColumnBinding> &mul_bindings, bool mobius_sign) {
	if (mul_bindings.empty()) {
		throw InternalException("DeltaJoin: cannot build an empty multiplicity product");
	}
	unique_ptr<Expression> product = make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[0]);
	for (size_t i = 1; i < mul_bindings.size(); i++) {
		product = MultiplyIntegers(binder, std::move(product),
		                           make_uniq<BoundColumnRefExpression>(mul_type, mul_bindings[i]));
	}
	// Apply Möbius sign: (-1)^(k-1). Only flip when k is even.
	if (mobius_sign && mul_bindings.size() % 2 == 0) {
		product = MultiplyIntegers(binder, make_uniq<BoundConstantExpression>(Value::INTEGER(-1)), std::move(product));
	}
	return product;
}

unique_ptr<LogicalOperator> FinalizeJoinTerm(Binder &binder, unique_ptr<LogicalOperator> term,
                                             column_binding_set_t hidden, const vector<ColumnBinding> &mul_bindings,
                                             const LogicalType &mul_type, bool mobius_sign) {
	term->ResolveOperatorTypes();
	hidden.insert(mul_bindings.begin(), mul_bindings.end());
	auto term_bindings = term->GetColumnBindings();
	vector<unique_ptr<Expression>> exprs;
	for (idx_t i = 0; i < term_bindings.size(); i++) {
		if (!hidden.count(term_bindings[i])) {
			exprs.push_back(make_uniq<BoundColumnRefExpression>(term->types[i], term_bindings[i]));
		}
	}
	exprs.push_back(BuildJoinMultiplicityProduct(binder, mul_type, mul_bindings, mobius_sign));
	auto projection = make_uniq<LogicalProjection>(binder.GenerateTableIndex(), std::move(exprs));
	projection->children.push_back(std::move(term));
	projection->ResolveOperatorTypes();
	return std::move(projection);
}

unique_ptr<LogicalOperator> AssembleJoinUnionAll(vector<unique_ptr<LogicalOperator>> &terms,
                                                 const vector<LogicalType> &types, Binder &binder) {
	if (terms.empty()) {
		vector<ColumnBinding> bindings;
		auto table_index = binder.GenerateTableIndex();
		for (idx_t i = 0; i < types.size(); i++) {
			bindings.emplace_back(table_index, ProjectionIndex(i));
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

ColumnBinding ReplaceJoinOutputBindings(const vector<ColumnBinding> &original_bindings,
                                        unique_ptr<LogicalOperator> &result, LogicalOperator &root) {
	auto union_bindings = result->GetColumnBindings();
	if (union_bindings.size() != original_bindings.size() + 1) {
		throw InternalException("Join rewrite produced %zu bindings for %zu original columns and multiplicity",
		                        union_bindings.size(), original_bindings.size());
	}
	ColumnBindingReplacer replacer;
	const idx_t map_count = original_bindings.size();
	OPENIVM_DEBUG_PRINT("[DeltaJoin] Binding replacement: %zu mappings (original=%zu, union=%zu)\n", map_count,
	                    original_bindings.size(), union_bindings.size());
	for (idx_t col_idx = 0; col_idx < map_count; ++col_idx) {
		replacer.replacement_bindings.emplace_back(original_bindings[col_idx], union_bindings[col_idx]);
	}
	replacer.stop_operator = result;
	replacer.VisitOperator(root);
	return union_bindings.back();
}

} // namespace duckdb
