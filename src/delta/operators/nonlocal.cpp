#include "delta/delta_operator.hpp"

#include "duckdb/common/exception.hpp"

namespace duckdb {

namespace {

static DeltaPlanFragment CompileNonLocalDeltaGuard(const DeltaOperatorInput &input, DeltaOperatorStrategy strategy,
                                                   const char *operator_name, const char *refresh_strategy) {
	LogDeltaOperatorStrategy(input, strategy);
	throw InternalException("%s should be maintained by %s and must not reach ComputeDelta", operator_name,
	                        refresh_strategy);
}

} // namespace

DeltaPlanFragment CompileAsofJoinDelta(const DeltaOperatorInput &input) {
	return CompileNonLocalDeltaGuard(input, DeltaOperatorStrategy::ASOF_AFFECTED_RECOMPUTE, "ASOF_JOIN",
	                                 "WINDOW_PARTITION, GROUP_RECOMPUTE, or CURRENT_DIFF_RECOMPUTE");
}

DeltaPlanFragment CompilePositionalJoinDelta(const DeltaOperatorInput &input) {
	return CompileNonLocalDeltaGuard(input, DeltaOperatorStrategy::POSITIONAL_GLOBAL_RECOMPUTE, "POSITIONAL_JOIN",
	                                 "CURRENT_DIFF_RECOMPUTE");
}

DeltaPlanFragment CompileSampleDelta(const DeltaOperatorInput &input) {
	return CompileNonLocalDeltaGuard(input, DeltaOperatorStrategy::SAMPLE_GLOBAL_RECOMPUTE, "SAMPLE",
	                                 "CURRENT_DIFF_RECOMPUTE");
}

} // namespace duckdb
