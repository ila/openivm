#include "delta/delta_operator.hpp"

#include "core/openivm_debug.hpp"

namespace duckdb {

DeltaPlanFragment CompileConstantLeafDelta(DeltaOperatorInput input) {
	LogDeltaOperatorStrategy(input, DeltaOperatorStrategy::CONSTANT_LEAF);
	auto bindings = input.plan->GetColumnBindings();
	if (bindings.empty()) {
		throw NotImplementedException("%s has no column bindings", LogicalOperatorToString(input.plan->type));
	}
	OPENIVM_DEBUG_PRINT("[Delta Operator] %s leaf -- returning unchanged (constant, no delta)\n",
	                    LogicalOperatorToString(input.plan->type).c_str());
	return {std::move(input.plan), bindings[0]};
}

} // namespace duckdb
