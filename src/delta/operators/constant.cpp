#include "delta/delta_operator.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_empty_result.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

DeltaPlanFragment CompileConstantZeroDelta(DeltaOperatorInput input) {
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::CONSTANT_ZERO_DELTA);
	vector<LogicalType> output_types = input.plan->types;
	output_types.push_back(input.mul_type);

	vector<ColumnBinding> bindings;
	auto table_index = input.context.input.optimizer.binder.GenerateTableIndex();
	// The empty result needs one binding per output type; >= disables construction and <= adds a type-less binding.
	for (idx_t i = 0; i < output_types.size(); // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
	     i++) {                                // mull-ignore: cxx_post_inc_to_post_dec
		bindings.emplace_back(table_index, i);
	}
	if (bindings.empty()) {
		throw InternalException("DeltaConstant: zero-delta result has no multiplicity binding");
	}
	auto mul_binding = bindings.back();
	auto empty = make_uniq<LogicalEmptyResult>(output_types, std::move(bindings));
	empty->ResolveOperatorTypes();

	OPENIVM_DEBUG_PRINT("[Delta Operator] %s constant leaf -- returning empty delta\n",
	                    LogicalOperatorToString(input.plan->type).c_str());
	return {std::move(empty), mul_binding};
}

DeltaPlanFragment CompileStaticConstantLeaf(DeltaOperatorInput input) {
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::CONSTANT_STATIC);
	auto bindings = input.plan->GetColumnBindings();
	// Logical operators expose exactly one type per output binding.
	if (bindings.size() != input.plan->types.size()) { // mull-ignore: cxx_ne_to_eq
		throw InternalException("DeltaConstant: static leaf binding/type count mismatch");
	}
	vector<unique_ptr<Expression>> exprs;
	// reserve only changes allocation capacity, never the resulting expressions.
	exprs.reserve(bindings.size() + 1); // mull-ignore: cxx_add_to_sub
	// The equality check above establishes a single exact bound; >= disables copying and <= indexes past both vectors.
	for (idx_t i = 0; i < bindings.size(); // mull-ignore: cxx_lt_to_ge,cxx_lt_to_le
	     i++) {                            // mull-ignore: cxx_post_inc_to_post_dec
		exprs.push_back(make_uniq<BoundColumnRefExpression>(input.plan->types[i], bindings[i]));
	}
	auto mul_expr = make_uniq<BoundConstantExpression>(Value::INTEGER(1));
	mul_expr->alias = openivm::MULTIPLICITY_COL;
	exprs.push_back(std::move(mul_expr));

	auto projection =
	    make_uniq<LogicalProjection>(input.context.input.optimizer.binder.GenerateTableIndex(), std::move(exprs));
	projection->children.push_back(std::move(input.plan));
	projection->ResolveOperatorTypes();
	auto projection_bindings = projection->GetColumnBindings();
	if (projection_bindings.empty()) {
		throw InternalException("DeltaConstant: static projection has no multiplicity binding");
	}
	auto mul_binding = projection_bindings.back();

	OPENIVM_DEBUG_PRINT("[Delta Operator] %s constant leaf -- appended static multiplicity for copied subtree\n",
	                    LogicalOperatorToString(projection->children[0]->type).c_str());
	return {std::move(projection), mul_binding};
}

} // namespace duckdb
