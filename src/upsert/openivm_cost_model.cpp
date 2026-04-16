#include "upsert/openivm_cost_model.hpp"
#include <cmath>
#include <unordered_set>
#include "core/openivm_constants.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_utils.hpp"
#include "core/openivm_debug.hpp"
#include "rules/column_hider.hpp"
#include "storage/ducklake_scan.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/constraints/foreign_key_constraint.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

/// Get the row count of a table by name, returns 0 if table doesn't exist.
static double GetTableRowCount(Connection &con, const string &table_name) {
	auto result = con.Query("SELECT COUNT(*) FROM " + OpenIVMUtils::QuoteIdentifier(table_name) + ";");
	if (result->HasError()) {
		return 0;
	}
	return result->GetValue(0, 0).GetValue<double>();
}

/// Get the number of pending delta rows for a given base delta table and view.
static double GetDeltaRowCount(Connection &con, const string &delta_table_name, const string &view_name) {
	auto ts_string = IVMMetadata(con).GetLastUpdate(view_name, delta_table_name);
	if (ts_string.empty()) {
		return 0;
	}
	auto count_result = con.Query("SELECT COUNT(*) FROM " + OpenIVMUtils::QuoteIdentifier(delta_table_name) +
	                              " WHERE " + string(ivm::TIMESTAMP_COL) + " >= '" + ts_string + "';");
	if (count_result->HasError()) {
		return 0;
	}
	return count_result->GetValue(0, 0).GetValue<double>();
}

struct TableStats {
	string table_name;
	string delta_table_name;
	double base_card;    // |T| (may reflect pushed-down filter selectivity)
	double actual_card;  // actual unfiltered table row count
	double delta_card;   // |ΔT|
	bool has_fk = false; // table has FK referencing another table in the join
};

/// Aggregated plan statistics collected in a single tree walk.
struct PlanStats {
	vector<TableStats> table_stats;
	idx_t join_leaf_count = 0;
	bool has_join = false;
	bool has_aggregate = false;
	bool has_full_outer = false;
	bool all_ducklake = true;        // true until a non-DuckLake leaf is found
	double filter_selectivity = 1.0; // cumulative selectivity from non-pushed-down LOGICAL_FILTER nodes
};

/// Get delta cardinality for a DuckLake table by counting changes between snapshots.
static double GetDuckLakeDeltaRowCount(Connection &con, const string &catalog_name, const string &schema_name,
                                       const string &table_name, const string &view_name) {
	IVMMetadata metadata(con);
	auto last_snap = metadata.GetLastSnapshotId(view_name, table_name);
	auto cur_snap_result = con.Query("SELECT id FROM " + catalog_name + ".current_snapshot()");
	if (cur_snap_result->HasError() || cur_snap_result->RowCount() == 0) {
		return 0;
	}
	auto cur_snap = cur_snap_result->GetValue(0, 0).GetValue<int64_t>();
	if (last_snap == cur_snap) {
		return 0;
	}

	double count = 0;
	auto ins_result =
	    con.Query("SELECT COUNT(*) FROM ducklake_table_insertions('" + OpenIVMUtils::EscapeValue(catalog_name) +
	              "', '" + OpenIVMUtils::EscapeValue(schema_name) + "', '" + OpenIVMUtils::EscapeValue(table_name) +
	              "', " + to_string(last_snap) + ", " + to_string(cur_snap) + ")");
	if (!ins_result->HasError() && ins_result->RowCount() > 0) {
		count += ins_result->GetValue(0, 0).GetValue<double>();
	}
	auto del_result =
	    con.Query("SELECT COUNT(*) FROM ducklake_table_deletions('" + OpenIVMUtils::EscapeValue(catalog_name) + "', '" +
	              OpenIVMUtils::EscapeValue(schema_name) + "', '" + OpenIVMUtils::EscapeValue(table_name) + "', " +
	              to_string(last_snap) + ", " + to_string(cur_snap) + ")");
	if (!del_result->HasError() && del_result->RowCount() > 0) {
		count += del_result->GetValue(0, 0).GetValue<double>();
	}
	return count;
}

/// Walk the plan tree once, collecting table stats, join info, and aggregate presence.
static void CollectPlanStatsRecursive(ClientContext &context, Connection &con, LogicalOperator &op,
                                      const string &view_name, PlanStats &stats) {
	switch (op.type) {
	case LogicalOperatorType::LOGICAL_GET: {
		auto &get = op.Cast<LogicalGet>();
		if (get.GetTable().get() != nullptr) {
			TableStats ts;
			ts.table_name = get.GetTable()->name;
			ts.base_card = static_cast<double>(get.EstimateCardinality(context));
			if (ts.base_card == 0) {
				ts.base_card = 1;
			}

			// Actual (unfiltered) table row count — for filter selectivity estimation.
			// base_card from EstimateCardinality may reflect pushed-down filters, so
			// actual_card / base_card gives us the filter selectivity ratio.
			ts.actual_card = GetTableRowCount(con, ts.table_name);
			if (ts.actual_card == 0) {
				ts.actual_card = 1;
			}

			// DuckLake vs standard delta cardinality
			if (get.function.name == "ducklake_scan" && get.function.function_info) {
				string cat_name = get.GetTable()->ParentCatalog().GetName();
				string schema_name = get.GetTable()->schema.name;
				ts.delta_table_name = ts.table_name; // DuckLake stores bare name
				ts.delta_card = GetDuckLakeDeltaRowCount(con, cat_name, schema_name, ts.table_name, view_name);
			} else {
				stats.all_ducklake = false;
				ts.delta_table_name = OpenIVMUtils::DeltaName(ts.table_name);
				ts.delta_card = GetDeltaRowCount(con, ts.delta_table_name, view_name);
			}

			stats.table_stats.push_back(ts);
		}
		stats.join_leaf_count++;
		break;
	}
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
	case LogicalOperatorType::LOGICAL_JOIN: {
		stats.has_join = true;
		if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
			auto *join = dynamic_cast<LogicalComparisonJoin *>(&op);
			if (join && join->join_type == JoinType::OUTER) {
				stats.has_full_outer = true;
			}
		}
		break;
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		stats.has_aggregate = true;
		break;
	case LogicalOperatorType::LOGICAL_FILTER: {
		// Filters that weren't pushed down into the scan — estimate their selectivity
		// from the cardinality ratio between the filter output and its child input.
		double filter_card = static_cast<double>(op.EstimateCardinality(context));
		double child_card = 0;
		if (!op.children.empty()) {
			child_card = static_cast<double>(op.children[0]->EstimateCardinality(context));
		}
		if (child_card > 0 && filter_card < child_card) {
			stats.filter_selectivity *= (filter_card / child_card);
		}
		break;
	}
	default:
		break;
	}
	for (auto &child : op.children) {
		CollectPlanStatsRecursive(context, con, *child, view_name, stats);
	}
}

// ============================================================================
// Learned cost model: weighted NNLS ridge regression
// ============================================================================

struct RegressionWeights {
	double w_compute;   // >= 0 (NNLS constraint)
	double w_upsert;    // >= 0 (NNLS constraint)
	double w_intercept; // unconstrained
	bool calibrated;    // false if insufficient data
};

/// Solve a 2x2 linear system Ax = b. Returns false if singular.
static bool Solve2x2(double a00, double a01, double a10, double a11, double b0, double b1, double &x0, double &x1) {
	double det = a00 * a11 - a01 * a10;
	if (std::abs(det) < 1e-15) {
		return false;
	}
	x0 = (a11 * b0 - a01 * b1) / det;
	x1 = (a00 * b1 - a10 * b0) / det;
	return true;
}

/// Solve a 3x3 linear system Ax = b. Returns false if singular.
static bool Solve3x3(const double A[3][3], const double b[3], double x[3]) {
	// Gaussian elimination with partial pivoting for a 3x3 system.
	double M[3][4]; // augmented matrix
	for (int i = 0; i < 3; i++) {
		for (int j = 0; j < 3; j++) {
			M[i][j] = A[i][j];
		}
		M[i][3] = b[i];
	}
	for (int col = 0; col < 3; col++) {
		// Partial pivoting
		int max_row = col;
		for (int row = col + 1; row < 3; row++) {
			if (std::abs(M[row][col]) > std::abs(M[max_row][col])) {
				max_row = row;
			}
		}
		if (max_row != col) {
			for (int j = 0; j < 4; j++) {
				std::swap(M[col][j], M[max_row][j]);
			}
		}
		if (std::abs(M[col][col]) < 1e-15) {
			return false;
		}
		// Eliminate below
		for (int row = col + 1; row < 3; row++) {
			double factor = M[row][col] / M[col][col];
			for (int j = col; j < 4; j++) {
				M[row][j] -= factor * M[col][j];
			}
		}
	}
	// Back substitution
	for (int i = 2; i >= 0; i--) {
		x[i] = M[i][3];
		for (int j = i + 1; j < 3; j++) {
			x[i] -= M[i][j] * x[j];
		}
		x[i] /= M[i][i];
	}
	return std::isfinite(x[0]) && std::isfinite(x[1]) && std::isfinite(x[2]);
}

/// Fit weighted NNLS ridge regression from execution history.
/// Returns calibrated weights or uncalibrated fallback.
static RegressionWeights FitRegression(const vector<IVMMetadata::RefreshHistoryEntry> &history, double decay,
                                       double ridge_lambda, idx_t min_samples) {
	RegressionWeights result = {1.0, 1.0, 0.0, false};
	idx_t n = history.size();
	if (n < min_samples) {
		return result; // cold start — use static model
	}

	// Build weighted normal equations: (X'WX + λI) w = X'Wy
	// X columns: [compute_est, upsert_est, 1.0]
	// Decay weights: most recent = 1.0, oldest = decay^(n-1)
	double XtWX[3][3] = {};
	double XtWy[3] = {};
	double weighted_sum_y = 0;
	double weight_sum = 0;

	for (idx_t i = 0; i < n; i++) {
		double w = std::pow(decay, static_cast<double>(n - 1 - i));
		double x[3] = {history[i].compute_est, history[i].upsert_est, 1.0};
		double y = history[i].actual_ms;
		for (int r = 0; r < 3; r++) {
			for (int c = 0; c < 3; c++) {
				XtWX[r][c] += w * x[r] * x[c];
			}
			XtWy[r] += w * x[r] * y;
		}
		weighted_sum_y += w * y;
		weight_sum += w;
	}

	// Add ridge regularization
	for (int i = 0; i < 3; i++) {
		XtWX[i][i] += ridge_lambda;
	}

	double w_vec[3];
	if (!Solve3x3(XtWX, XtWy, w_vec)) {
		// Singular — fall back to weighted mean
		result.w_compute = 0.0;
		result.w_upsert = 0.0;
		result.w_intercept = (weight_sum > 0) ? weighted_sum_y / weight_sum : 0.0;
		result.calibrated = true;
		return result;
	}

	// NNLS: clamp negative slope coefficients, re-fit with reduced features
	if (w_vec[0] < 0 && w_vec[1] < 0) {
		// Both slopes negative — use weighted mean
		result.w_compute = 0.0;
		result.w_upsert = 0.0;
		result.w_intercept = (weight_sum > 0) ? weighted_sum_y / weight_sum : 0.0;
		result.calibrated = true;
		return result;
	}

	if (w_vec[0] < 0) {
		// Remove compute, re-fit with (upsert, intercept)
		double A[2][2] = {};
		double b2[2] = {};
		for (idx_t i = 0; i < n; i++) {
			double w = std::pow(decay, static_cast<double>(n - 1 - i));
			double x[2] = {history[i].upsert_est, 1.0};
			double y = history[i].actual_ms;
			for (int r = 0; r < 2; r++) {
				for (int c = 0; c < 2; c++) {
					A[r][c] += w * x[r] * x[c];
				}
				b2[r] += w * x[r] * y;
			}
		}
		A[0][0] += ridge_lambda;
		A[1][1] += ridge_lambda;
		double w2_0, w2_1;
		if (Solve2x2(A[0][0], A[0][1], A[1][0], A[1][1], b2[0], b2[1], w2_0, w2_1) && w2_0 >= 0) {
			result = {0.0, w2_0, w2_1, true};
		} else {
			result = {0.0, 0.0, (weight_sum > 0) ? weighted_sum_y / weight_sum : 0.0, true};
		}
		return result;
	}

	if (w_vec[1] < 0) {
		// Remove upsert, re-fit with (compute, intercept)
		double A[2][2] = {};
		double b2[2] = {};
		for (idx_t i = 0; i < n; i++) {
			double w = std::pow(decay, static_cast<double>(n - 1 - i));
			double x[2] = {history[i].compute_est, 1.0};
			double y = history[i].actual_ms;
			for (int r = 0; r < 2; r++) {
				for (int c = 0; c < 2; c++) {
					A[r][c] += w * x[r] * x[c];
				}
				b2[r] += w * x[r] * y;
			}
		}
		A[0][0] += ridge_lambda;
		A[1][1] += ridge_lambda;
		double w2_0, w2_1;
		if (Solve2x2(A[0][0], A[0][1], A[1][0], A[1][1], b2[0], b2[1], w2_0, w2_1) && w2_0 >= 0) {
			result = {w2_0, 0.0, w2_1, true};
		} else {
			result = {0.0, 0.0, (weight_sum > 0) ? weighted_sum_y / weight_sum : 0.0, true};
		}
		return result;
	}

	result = {w_vec[0], w_vec[1], w_vec[2], true};
	return result;
}

// ============================================================================

IVMCostEstimate EstimateIVMCost(ClientContext &context, LogicalOperator &plan, const string &view_name) {
	// Single connection for all cardinality queries
	Connection con(*context.db);

	// Read operational flags — cost model reflects what actually happens at refresh time.
	Value nterm_val, fk_val;
	bool nterm_enabled = true;
	bool fk_enabled = true;
	if (context.TryGetCurrentSetting("ivm_ducklake_nterm", nterm_val) && !nterm_val.IsNull()) {
		nterm_enabled = nterm_val.GetValue<bool>();
	}
	if (context.TryGetCurrentSetting("ivm_fk_pruning", fk_val) && !fk_val.IsNull()) {
		fk_enabled = fk_val.GetValue<bool>();
	}

	// 1. Collect all plan statistics in one tree walk
	PlanStats plan_stats;
	CollectPlanStatsRecursive(context, con, plan, view_name, plan_stats);

	auto &table_stats = plan_stats.table_stats;
	size_t N = table_stats.size();
	if (N == 0) {
		// No base tables found — shouldn't happen, but default to IVM
		return {0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, false};
	}

	// 2. Compute basic metrics
	double total_base_scan = 0;
	double delta_fraction_sum = 0;
	for (auto &ts : table_stats) {
		total_base_scan += ts.base_card;
		delta_fraction_sum += ts.delta_card / ts.base_card;
	}

	double mv_card = GetTableRowCount(con, IVMTableNames::DataTableName(view_name));
	if (mv_card == 0) {
		mv_card = 1;
	}

	bool has_join = plan_stats.has_join;
	bool has_aggregate = plan_stats.has_aggregate;

	// 2b. For standard joins, detect FK constraints to estimate term reduction.
	// Count PK-side leaves whose terms can be pruned by FK-aware optimization.
	idx_t fk_pk_leaf_count = 0;
	if (fk_enabled && has_join && !plan_stats.all_ducklake && N > 1) {
		// Build table name -> index map
		unordered_map<string, size_t> table_to_idx;
		string in_list;
		for (size_t i = 0; i < N; i++) {
			table_to_idx[table_stats[i].table_name] = i;
			if (i > 0) {
				in_list += ", ";
			}
			in_list += "'" + OpenIVMUtils::EscapeValue(table_stats[i].table_name) + "'";
		}
		// Batch query: get all FK constraints for join tables in one call.
		unordered_set<size_t> pruned_pk_leaves;
		auto result = con.Query("SELECT table_name, constraint_text FROM duckdb_constraints() "
		                        "WHERE constraint_type = 'FOREIGN KEY' AND table_name IN (" +
		                        in_list + ")");
		if (!result->HasError()) {
			for (idx_t r = 0; r < result->RowCount(); r++) {
				string fk_table = result->GetValue(0, r).ToString();
				string fk_text = result->GetValue(1, r).ToString();
				auto fk_it = table_to_idx.find(fk_table);
				if (fk_it == table_to_idx.end()) {
					continue;
				}
				// Check if any PK table in our join has empty delta.
				// Match by "REFERENCES <table_name>" to avoid substring false positives.
				for (auto &kv : table_to_idx) {
					if (kv.second == fk_it->second) {
						continue;
					}
					string ref_pattern = "REFERENCES " + kv.first;
					if (fk_text.find(ref_pattern) != string::npos && table_stats[kv.second].delta_card == 0) {
						pruned_pk_leaves.insert(kv.second);
					}
				}
			}
		}
		fk_pk_leaf_count = pruned_pk_leaves.size();
	}

	// 3. Estimate IVM cost
	//
	// IVM compute cost:
	//   - For DuckLake joins: N terms (N-term telescoping)
	//   - For standard joins: 2^(N-1) terms (inclusion-exclusion), reduced by FK pruning
	//   - For non-joins: just scan the delta (very cheap)
	//
	// IVM upsert cost:
	//   - Estimated delta result size x merge overhead
	//   - For aggregates: merge cost depends on affected groups
	//   - For projections/filters: targeted insert/delete

	double ivm_compute;
	double estimated_delta_result;

	if (has_join) {
		idx_t join_leaves = plan_stats.join_leaf_count;
		double scan_multiplier;
		if (nterm_enabled && plan_stats.all_ducklake) {
			// DuckLake N-term telescoping: exactly N terms
			scan_multiplier = static_cast<double>(join_leaves);
		} else if (fk_pk_leaf_count > 0) {
			// FK pruning: surviving terms = 2^(N - pruned_pks) - 1
			idx_t effective_leaves = join_leaves - fk_pk_leaf_count;
			scan_multiplier = static_cast<double>((1ULL << effective_leaves) - 1);
			if (scan_multiplier < 1) {
				scan_multiplier = 1;
			}
		} else {
			// Standard inclusion-exclusion: 2^(N-1) average scans per table
			scan_multiplier = static_cast<double>(1ULL << (join_leaves - 1));
		}

		ivm_compute = scan_multiplier * total_base_scan;

		// Estimate delta result: each delta row fans out by MV/actual_card.
		// Use actual_card (unfiltered) instead of base_card to avoid inflated fanout
		// when filters are pushed into the scan (base_card reflects post-filter estimate).
		estimated_delta_result = 0;
		for (auto &ts : table_stats) {
			double fanout = mv_card / ts.actual_card;
			estimated_delta_result += ts.delta_card * fanout;
		}
		// Apply selectivity from any non-pushed-down filter nodes
		estimated_delta_result *= plan_stats.filter_selectivity;
	} else {
		// Unary operators: scan cost is the full delta (filter doesn't reduce scan cost)
		double total_delta = 0;
		double filtered_delta = 0;
		for (auto &ts : table_stats) {
			total_delta += ts.delta_card;
			// Selectivity from pushed-down filters: base_card / actual_card
			double selectivity = std::min(1.0, ts.base_card / ts.actual_card);
			filtered_delta += ts.delta_card * selectivity;
		}
		ivm_compute = total_delta;
		// Apply both pushed-down selectivity and non-pushed-down filter selectivity
		estimated_delta_result = filtered_delta * plan_stats.filter_selectivity;
	}

	double ivm_upsert;
	if (has_aggregate) {
		// Aggregate merge: affected groups ≈ min(delta_result, MV groups)
		// Merge cost is proportional to affected groups, not full MV
		double affected_groups = std::min(estimated_delta_result, mv_card);
		ivm_upsert = affected_groups * 2.0; // read + write per group
		if (plan_stats.has_full_outer) {
			// FULL OUTER: MERGE + targeted unmatched recompute + NULL group recompute
			ivm_upsert *= 3.0;
		}
	} else {
		// Projection/filter: targeted insert/delete
		// EXISTS subquery cost ≈ delta_result × log(MV) for each delete
		ivm_upsert = estimated_delta_result * (1.0 + std::log2(std::max(mv_card, 1.0)));
		if (plan_stats.has_full_outer) {
			// Bidirectional key CTE queries both delta tables
			ivm_upsert *= 1.5;
		}
	}

	double ivm_total = ivm_compute + ivm_upsert;

	// 4. Estimate recompute cost
	//
	// Recompute compute: run the full query once
	//   - Scan all base tables + join/aggregate processing
	// Recompute replace: delete all MV rows + insert new result
	double recompute_compute = total_base_scan + mv_card; // scan + produce result
	double recompute_replace = mv_card * 2.0;             // delete all + insert all
	double recompute_total = recompute_compute + recompute_replace;

	// 5. Learned cost model: calibrate predictions using execution history
	//    Gated by ivm_adaptive_refresh (same gate as the cost model decision).
	double ivm_predicted_ms = ivm_total;
	double recompute_predicted_ms = recompute_total;
	bool calibrated = false;

	Value adaptive_val;
	bool adaptive_on = false;
	if (context.TryGetCurrentSetting("ivm_adaptive_refresh", adaptive_val) && !adaptive_val.IsNull()) {
		adaptive_on = adaptive_val.GetValue<bool>();
	}

	if (adaptive_on) {
		// Read decay setting
		double decay = 0.9;
		Value decay_val;
		if (context.TryGetCurrentSetting("ivm_cost_decay", decay_val) && !decay_val.IsNull()) {
			decay = decay_val.GetValue<double>();
			if (decay < 0.0 || decay > 1.0) {
				decay = 0.9;
			}
		}

		IVMMetadata metadata(con);
		constexpr double RIDGE_LAMBDA = 1e-4;
		constexpr idx_t MIN_SAMPLES = 3;

		auto ivm_history = metadata.GetRefreshHistory(view_name, "incremental");
		auto ivm_reg = FitRegression(ivm_history, decay, RIDGE_LAMBDA, MIN_SAMPLES);
		if (ivm_reg.calibrated) {
			ivm_predicted_ms =
			    std::max(0.0, ivm_reg.w_compute * ivm_compute + ivm_reg.w_upsert * ivm_upsert + ivm_reg.w_intercept);
			calibrated = true;
			OPENIVM_DEBUG_PRINT("[COST MODEL] IVM regression: w_compute=%.4f, w_upsert=%.4f, intercept=%.1f\n",
			                    ivm_reg.w_compute, ivm_reg.w_upsert, ivm_reg.w_intercept);
		}

		auto rc_history = metadata.GetRefreshHistory(view_name, "full");
		auto rc_reg = FitRegression(rc_history, decay, RIDGE_LAMBDA, MIN_SAMPLES);
		if (rc_reg.calibrated) {
			recompute_predicted_ms = std::max(0.0, rc_reg.w_compute * recompute_compute +
			                                           rc_reg.w_upsert * recompute_replace + rc_reg.w_intercept);
			calibrated = true;
			OPENIVM_DEBUG_PRINT("[COST MODEL] Recompute regression: w_compute=%.4f, w_replace=%.4f, intercept=%.1f\n",
			                    rc_reg.w_compute, rc_reg.w_upsert, rc_reg.w_intercept);
		}
	}

	OPENIVM_DEBUG_PRINT(
	    "[COST MODEL] Tables: %zu, Join: %s, Aggregate: %s, FullOuter: %s, DuckLake: %s, FK pruned PKs: %lu\n", N,
	    has_join ? "yes" : "no", has_aggregate ? "yes" : "no", plan_stats.has_full_outer ? "yes" : "no",
	    plan_stats.all_ducklake ? "yes" : "no", (unsigned long)fk_pk_leaf_count);
	OPENIVM_DEBUG_PRINT("[COST MODEL] Base scan total: %.0f, Delta fraction sum: %.4f, Filter selectivity: %.4f\n",
	                    total_base_scan, delta_fraction_sum, plan_stats.filter_selectivity);
	OPENIVM_DEBUG_PRINT("[COST MODEL] MV cardinality: %.0f, Est. delta result: %.0f\n", mv_card,
	                    estimated_delta_result);
	OPENIVM_DEBUG_PRINT("[COST MODEL] IVM cost: %.0f (compute: %.0f, upsert: %.0f)\n", ivm_total, ivm_compute,
	                    ivm_upsert);
	OPENIVM_DEBUG_PRINT("[COST MODEL] Recompute cost: %.0f (compute: %.0f, replace: %.0f)\n", recompute_total,
	                    recompute_compute, recompute_replace);
	if (calibrated) {
		OPENIVM_DEBUG_PRINT("[COST MODEL] Calibrated: IVM=%.0fms, Recompute=%.0fms\n", ivm_predicted_ms,
		                    recompute_predicted_ms);
	}
	OPENIVM_DEBUG_PRINT("[COST MODEL] Decision: %s\n", ivm_predicted_ms < recompute_predicted_ms ? "IVM" : "RECOMPUTE");

	IVMCostEstimate estimate;
	estimate.ivm_compute = ivm_compute;
	estimate.ivm_upsert = ivm_upsert;
	estimate.recompute_compute = recompute_compute;
	estimate.recompute_replace = recompute_replace;
	estimate.ivm_cost = ivm_total;
	estimate.recompute_cost = recompute_total;
	estimate.ivm_predicted_ms = ivm_predicted_ms;
	estimate.recompute_predicted_ms = recompute_predicted_ms;
	estimate.calibrated = calibrated;
	return estimate;
}

string IVMCostQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto view_name = StringValue::Get(parameters.values[0]);

	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);

	// Propagate user session settings to the cost estimation connection.
	// The new connection has defaults, so settings like ivm_adaptive_refresh
	// must be copied from the calling context for calibration to activate.
	for (auto &setting_name : {"ivm_adaptive_refresh", "ivm_cost_decay", "ivm_ducklake_nterm", "ivm_fk_pruning"}) {
		Value v;
		if (context.TryGetCurrentSetting(setting_name, v) && !v.IsNull()) {
			con.Query("SET " + string(setting_name) + " = " + v.ToString());
		}
	}

	con.BeginTransaction();

	IVMMetadata metadata(con);
	auto view_query = metadata.GetViewQuery(view_name);
	if (view_query.empty()) {
		con.Rollback();
		throw ParserException("View '" + view_name + "' not found in IVM metadata");
	}

	auto &con_ctx = *con.context;

	Parser p;
	p.ParseQuery(view_query);
	Planner planner(con_ctx);
	planner.CreatePlan(p.statements[0]->Copy());
	Optimizer optimizer(*planner.binder, con_ctx);
	auto plan = optimizer.Optimize(std::move(planner.plan));

	auto estimate = EstimateIVMCost(con_ctx, *plan, view_name);
	con.Rollback();

	string decision = estimate.ShouldRecompute() ? "full" : "incremental";
	return "SELECT '" + decision + "' AS decision, " + to_string(estimate.ivm_cost) + " AS ivm_cost, " +
	       to_string(estimate.recompute_cost) + " AS recompute_cost, " + to_string(estimate.ivm_predicted_ms) +
	       " AS ivm_predicted_ms, " + to_string(estimate.recompute_predicted_ms) + " AS recompute_predicted_ms, " +
	       (estimate.calibrated ? "true" : "false") + " AS calibrated";
}

string IVMCostHistoryQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto view_name = StringValue::Get(parameters.values[0]);
	return "SELECT view_name, refresh_timestamp, method, ivm_compute_est, ivm_upsert_est,"
	       " recompute_compute_est, recompute_replace_est, actual_duration_ms"
	       " FROM " +
	       string(ivm::HISTORY_TABLE) + " WHERE view_name = '" + OpenIVMUtils::EscapeValue(view_name) +
	       "' ORDER BY refresh_timestamp DESC LIMIT 20";
}

} // namespace duckdb
