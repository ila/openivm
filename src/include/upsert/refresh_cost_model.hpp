#ifndef REFRESH_COST_MODEL_HPP
#define REFRESH_COST_MODEL_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

struct RefreshCostEstimate {
	// Static model components
	double incremental_compute;
	double incremental_upsert;
	double recompute_compute;
	double recompute_replace;

	// Totals (static model)
	double incremental_cost; // = incremental_compute + incremental_upsert
	double recompute_cost;   // = recompute_compute + recompute_replace

	// Calibrated predictions (from regression, or == static totals if uncalibrated)
	double incremental_predicted_ms;
	double recompute_predicted_ms;

	// Whether regression was used (true) or static fallback (false)
	bool calibrated;

	// Strategy this view actually uses on refresh — drives both the history label
	// and the meaning of `incremental_compute` / `incremental_upsert`. For most views this is
	// "incremental" (delta-driven IVM); for `RefreshType::GROUP_RECOMPUTE` views
	// (inner DISTINCT under aggregate) it's "group_recompute" and the IVM cost
	// fields hold the affected-groups recompute cost instead. For
	// `RefreshType::WINDOW_PARTITION` it's "window_partition" (partition recompute).
	// Always one of: "incremental", "group_recompute", "window_partition".
	string strategy_label;

	bool ShouldRecompute() const {
		return recompute_predicted_ms < incremental_predicted_ms;
	}
};

/// Estimate costs of IVM vs full recompute for the given view query plan.
/// Walks the plan tree, collects base table and delta table cardinalities,
/// and computes a cost estimate for both strategies. If sufficient execution
/// history exists, applies learned regression to calibrate predictions.
RefreshCostEstimate EstimateIVMCost(ClientContext &context, LogicalOperator &plan, const string &view_name);

/// Pragma function: returns the IVM cost estimate for a view as a string.
string RefreshCostQuery(ClientContext &context, const FunctionParameters &parameters);

/// Pragma function: returns refresh history for a view.
string RefreshCostHistoryQuery(ClientContext &context, const FunctionParameters &parameters);

// =============================================================================
// View-matching extension (gated by `openivm_enable_view_matching`).
// =============================================================================

enum class MatchStrategy : uint8_t {
	BYPASS,               // run query against base, ignore the MV
	USE_MV_AS_IS,         // MV is fresh — just scan
	MV_PLUS_RESIDUAL,     // stale MV + inline delta compensation (Tier 2)
	CASCADE_REFRESH,      // refresh chain through DAG, then scan top MV (Tier 3)
	PARTIAL_MV_PLUS_BASE, // MV covers part of query; UNION ALL with base for rest
	FULL_RECOMPUTE        // throw away MV, recompute from base
};

struct StrategyCostEstimate {
	MatchStrategy strategy;
	double estimated_ms;
	double bypass_baseline_ms;
};

/// For a query that matched `view_name`, score each candidate strategy.
/// Reads pending-delta-row estimate from `openivm_delta_tables` and
/// per-strategy regression from `openivm_refresh_history`. Returns an
/// empty vector if `openivm_enable_view_matching` is off.
vector<StrategyCostEstimate> EstimatePerQuery(ClientContext &context, const string &view_name,
                                              LogicalOperator &query_plan);

} // namespace duckdb

#endif // REFRESH_COST_MODEL_HPP
