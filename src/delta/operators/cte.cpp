#include "delta/delta_operator.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/planner/operator/logical_cte.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"

namespace duckdb {

namespace {

static void UpdateCteRefsWithMul(LogicalOperator *node, idx_t cte_table_index, const LogicalType &mul_type) {
	if (node->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		auto &ref = node->Cast<LogicalCTERef>();
		if (ref.cte_index == cte_table_index &&
		    (ref.bound_columns.empty() || ref.bound_columns.back() != openivm::MULTIPLICITY_COL)) {
			ref.chunk_types.push_back(mul_type);
			ref.bound_columns.push_back(openivm::MULTIPLICITY_COL);
			OPENIVM_DEBUG_PRINT("[CTE]   Updated CTE_REF cte_index=%lu with mul column\n",
			                    (unsigned long)cte_table_index);
		}
	}
	for (auto &child : node->children) {
		UpdateCteRefsWithMul(child.get(), cte_table_index, mul_type);
	}
}

} // namespace

DeltaPlanFragment CompileCteDelta(DeltaOperatorInput input) {
	if (input.plan->type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::CTE_MATERIALIZED);
		// CTE wrapper: children[0] = CTE definition, children[1] = consumer.
		auto &cte = input.plan->Cast<LogicalCTE>();
		idx_t cte_table_index = cte.table_index;

		auto def_mul = input.CompileChild(input.plan->children[0], input.root);
		input.plan->children[0] = std::move(def_mul.op);
		cte.column_count = input.plan->children[0]->GetColumnBindings().size();

		OPENIVM_DEBUG_PRINT("[CTE] Looking for CTE_REFs with cte_index=%lu\n", (unsigned long)cte_table_index);
		UpdateCteRefsWithMul(input.plan.get(), cte_table_index, input.mul_type);

		auto cons_mul = input.CompileChild(input.plan->children[1], input.root);
		input.plan->children[1] = std::move(cons_mul.op);
		input.plan->ResolveOperatorTypes();
		return {std::move(input.plan), cons_mul.mul_binding};
	}
	if (input.plan->type == LogicalOperatorType::LOGICAL_CTE_REF) {
		LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::CTE_REF);
		auto bindings = input.plan->GetColumnBindings();
		ColumnBinding mul_binding = bindings.back();
		return {std::move(input.plan), mul_binding};
	}
	throw NotImplementedException("CTE rewrite expected CTE node, got %s", LogicalOperatorToString(input.plan->type));
}

} // namespace duckdb
