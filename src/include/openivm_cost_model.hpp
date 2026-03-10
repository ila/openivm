#ifndef OPENIVM_COST_MODEL_HPP
#define OPENIVM_COST_MODEL_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

struct IVMCostEstimate {
	double ivm_cost;
	double recompute_cost;

	bool ShouldRecompute() const {
		return recompute_cost < ivm_cost;
	}
};

/// Estimate costs of IVM vs full recompute for the given view query plan.
/// Walks the plan tree, collects base table and delta table cardinalities,
/// and computes a cost estimate for both strategies.
IVMCostEstimate EstimateIVMCost(ClientContext &context, LogicalOperator &plan, const string &view_name);

} // namespace duckdb

#endif // OPENIVM_COST_MODEL_HPP
