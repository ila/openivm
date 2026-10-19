#ifndef REFRESH_COST_MODEL_HPP
#define REFRESH_COST_MODEL_HPP

#include "duckdb.hpp"
#include "duckdb/planner/logical_operator.hpp"

#include <array>

namespace duckdb {

struct DeltaActivityResult;

// ---------------------------------------------------------------------------
// Plan cost features
// ---------------------------------------------------------------------------
//
// A refresh plan described as estimated rows per operator class. Both candidate strategies are
// described in the same terms by the same estimator, so their predictions are directly comparable,
// which the previous two-term model could not claim: it priced the incremental side from delta
// cardinalities and the recompute side from base cardinalities, with separately tuned constants.
//
// Learned weights turn a vector into milliseconds. Measured across a 2,520-case sweep, the
// hand-tuned constants under-predicted BOTH strategies by roughly a factor of two at every scale
// factor, with median q-errors near 2.2 and 2.4. Errors of similar size on both sides cancel while
// the two scale alike and stop cancelling when they do not, which is why decision agreement held at
// 94% at scale factor 1 and fell to 68% at scale factor 100.
enum class PlanFeature : uint8_t {
	SCAN_ROWS,             // rows read by table scans
	FILTER_ROWS,           // rows entering filters
	PROJECT_ROWS,          // rows through projections
	JOIN_BUILD_ROWS,       // build-side input, which is what a hash join materializes
	JOIN_PROBE_ROWS,       // probe-side input
	JOIN_OUTPUT_ROWS,      // rows a join produces, separate because fan-out is the expensive part
	AGGREGATE_INPUT_ROWS,  // rows entering grouping
	AGGREGATE_OUTPUT_ROWS, // groups produced
	ORDER_ROWS,
	WINDOW_ROWS,
	SET_OP_ROWS,
	WRITE_ROWS, // rows the upsert writes; supplied by the caller, not read from the plan
	COUNT
};

constexpr idx_t PLAN_FEATURE_COUNT = static_cast<idx_t>(PlanFeature::COUNT);

// Bumped when the layout above changes. History rows carrying a different value are skipped by the
// fitter rather than silently reinterpreted as a different set of features.
constexpr int32_t PLAN_FEATURE_SCHEMA = 1;

using PlanFeatureVector = std::array<double, PLAN_FEATURE_COUNT>;

inline double &Feature(PlanFeatureVector &v, PlanFeature f) {
	return v[static_cast<idx_t>(f)];
}
inline double Feature(const PlanFeatureVector &v, PlanFeature f) {
	return v[static_cast<idx_t>(f)];
}

/// Describe `plan` as estimated rows per operator class. WRITE_ROWS is left at zero; the caller
/// knows what the upsert writes, which is not visible in the plan that computes the delta.
PlanFeatureVector ExtractPlanFeatures(ClientContext &context, LogicalOperator &plan);

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

	// Per-operator-class row estimates for each candidate, recorded with the refresh so the weights
	// can be refit later against what actually happened. Empty when no plan was available to describe.
	PlanFeatureVector incremental_features;
	PlanFeatureVector recompute_features;
	bool has_features;

	// Strategy this view uses on refresh — drives both the history label and the
	// meaning of `incremental_compute` / `incremental_upsert`. For "incremental"
	// views the fields hold delta-driven IVM cost. For fixed strategy views they
	// hold that strategy's affected-domain cost. Known labels: "incremental",
	// "group_recompute", "window_partition",
	// "distinct_incremental", "semi_anti_recompute", and "full".
	string strategy_label;

	// Non-zero when the strategy was chosen to gather evidence rather than because it looked cheaper:
	// +1 forces recompute, -1 forces incremental. A model that learns only from what it runs cannot
	// observe the road not taken, so a strategy it stops choosing stops accumulating the samples that
	// would show it was right. See the exploration block in EstimateRefreshCost.
	int8_t exploration;

	bool ShouldRecompute() const {
		if (exploration != 0) {
			return exploration > 0;
		}
		return recompute_predicted_ms < incremental_predicted_ms;
	}
};

/// Estimate costs of incremental refresh vs full recompute for the given view query plan.
/// Walks the plan tree, collects base table and delta table cardinalities,
/// and computes a cost estimate for both strategies. If sufficient execution
/// history exists, applies learned regression to calibrate predictions.
///
/// `incremental_plan`, when supplied, is the rewritten delta plan that the refresh will actually
/// execute. Its estimated output cardinality comes from DuckDB's own estimator, which sees the real
/// delta tables, join selectivities and pushed-down filters, so it replaces this model's fanout
/// proxy. Pass nullptr when no such plan exists (adaptive refresh off, or a strategy that produces
/// no delta), and the proxy is used instead.
RefreshCostEstimate EstimateRefreshCost(ClientContext &context, LogicalOperator &plan, const string &view_name,
                                        const DeltaActivityResult *delta_activity = nullptr,
                                        LogicalOperator *incremental_plan = nullptr);

/// Pragma function: returns the refresh cost estimate for a view as a string.
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
