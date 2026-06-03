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
	for (idx_t i = 0; i < output_types.size(); i++) {
		bindings.emplace_back(table_index, i);
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
	vector<unique_ptr<Expression>> exprs;
	exprs.reserve(bindings.size() + 1);
	for (idx_t i = 0; i < bindings.size() && i < input.plan->types.size(); i++) {
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
	auto mul_binding = projection_bindings.back();

	OPENIVM_DEBUG_PRINT("[Delta Operator] %s constant leaf -- appended static multiplicity for copied subtree\n",
	                    LogicalOperatorToString(projection->children[0]->type).c_str());
	return {std::move(projection), mul_binding};
}

} // namespace duckdb
