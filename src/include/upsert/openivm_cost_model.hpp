#ifndef OPENIVM_COST_MODEL_HPP
#define OPENIVM_COST_MODEL_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

struct IVMCostEstimate {
	// Static model components
	double ivm_compute;
	double ivm_upsert;
	double recompute_compute;
	double recompute_replace;

	// Totals (static model)
	double ivm_cost;       // = ivm_compute + ivm_upsert
	double recompute_cost; // = recompute_compute + recompute_replace

	// Calibrated predictions (from regression, or == static totals if uncalibrated)
	double ivm_predicted_ms;
	double recompute_predicted_ms;

	// Whether regression was used (true) or static fallback (false)
	bool calibrated;

	bool ShouldRecompute() const {
		return recompute_predicted_ms < ivm_predicted_ms;
	}
};

/// Estimate costs of IVM vs full recompute for the given view query plan.
/// Walks the plan tree, collects base table and delta table cardinalities,
/// and computes a cost estimate for both strategies. If sufficient execution
/// history exists, applies learned regression to calibrate predictions.
IVMCostEstimate EstimateIVMCost(ClientContext &context, LogicalOperator &plan, const string &view_name);

/// Pragma function: returns the IVM cost estimate for a view as a string.
string IVMCostQuery(ClientContext &context, const FunctionParameters &parameters);

/// Pragma function: returns refresh history for a view.
string IVMCostHistoryQuery(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // OPENIVM_COST_MODEL_HPP
