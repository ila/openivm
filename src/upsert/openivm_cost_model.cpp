#include "upsert/openivm_cost_model.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_utils.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"

namespace duckdb {

/// Get the row count of a table by name, returns 0 if table doesn't exist.
static double GetTableRowCount(ClientContext &context, const string &table_name) {
	Connection con(*context.db);
	auto result = con.Query("SELECT COUNT(*) FROM " + table_name + ";");
	if (result->HasError()) {
		return 0;
	}
	return result->GetValue(0, 0).GetValue<double>();
}

/// Get the number of pending delta rows for a given base delta table and view.
static double GetDeltaRowCount(ClientContext &context, const string &delta_table_name, const string &view_name) {
	Connection con(*context.db);
	auto ts_string = IVMMetadata(con).GetLastUpdate(view_name, delta_table_name);
	if (ts_string.empty()) {
		return 0;
	}
	auto count_result = con.Query("SELECT COUNT(*) FROM " + delta_table_name + " WHERE " + string(ivm::TIMESTAMP_COL) +
	                              " >= '" + ts_string + "';");
	if (count_result->HasError()) {
		return 0;
	}
	return count_result->GetValue(0, 0).GetValue<double>();
}

struct TableStats {
	string table_name;
	string delta_table_name;
	double base_card;  // |T|
	double delta_card; // |ΔT|
};

/// Collect all base table GET nodes from the plan tree.
static void CollectTableStats(ClientContext &context, LogicalOperator &op, const string &view_name,
                              vector<TableStats> &stats) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		auto &get = op.Cast<LogicalGet>();
		if (get.GetTable().get() != nullptr) {
			TableStats ts;
			ts.table_name = get.GetTable()->name;
			ts.delta_table_name = OpenIVMUtils::DeltaName(ts.table_name);
			ts.base_card = static_cast<double>(get.EstimateCardinality(context));
			if (ts.base_card == 0) {
				ts.base_card = 1; // avoid division by zero
			}
			ts.delta_card = GetDeltaRowCount(context, ts.delta_table_name, view_name);
			stats.push_back(ts);
		}
	}
	for (auto &child : op.children) {
		CollectTableStats(context, *child, view_name, stats);
	}
}

/// Count join nodes in the plan tree.
static idx_t CountJoinLeaves(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_GET) {
		return 1;
	}
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		idx_t count = 0;
		for (auto &child : op.children) {
			count += CountJoinLeaves(*child);
		}
		return count;
	}
	// For other operators (filter, projection, etc.), recurse into children
	idx_t count = 0;
	for (auto &child : op.children) {
		count += CountJoinLeaves(*child);
	}
	return count;
}

/// Check if the plan contains a join operator.
static bool HasJoin(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN || op.type == LogicalOperatorType::LOGICAL_JOIN) {
		return true;
	}
	for (auto &child : op.children) {
		if (HasJoin(*child)) {
			return true;
		}
	}
	return false;
}

/// Check if the plan contains an aggregate operator.
static bool HasAggregate(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		return true;
	}
	for (auto &child : op.children) {
		if (HasAggregate(*child)) {
			return true;
		}
	}
	return false;
}

IVMCostEstimate EstimateIVMCost(ClientContext &context, LogicalOperator &plan, const string &view_name) {
	// 1. Collect table statistics
	vector<TableStats> table_stats;
	CollectTableStats(context, plan, view_name, table_stats);

	size_t N = table_stats.size();
	if (N == 0) {
		// No base tables found — shouldn't happen, but default to IVM
		return {0.0, 1.0};
	}

	// 2. Compute basic metrics
	double total_base_scan = 0;
	double delta_fraction_sum = 0;
	for (auto &ts : table_stats) {
		total_base_scan += ts.base_card;
		delta_fraction_sum += ts.delta_card / ts.base_card;
	}

	double mv_card = GetTableRowCount(context, view_name);
	if (mv_card == 0) {
		mv_card = 1;
	}

	bool has_join = HasJoin(plan);
	bool has_aggregate = HasAggregate(plan);

	// 3. Estimate IVM cost
	//
	// IVM compute cost:
	//   - For joins: 2^(N-1) base table scans per table (inclusion-exclusion terms)
	//   - For non-joins: just scan the delta (very cheap)
	//   - Filter/projection: negligible overhead
	//
	// IVM upsert cost:
	//   - Estimated delta result size × merge overhead
	//   - For aggregates: merge cost depends on affected groups
	//   - For projections/filters: targeted insert/delete

	double ivm_compute;
	double estimated_delta_result;

	if (has_join) {
		idx_t join_leaves = CountJoinLeaves(plan);
		double scan_multiplier = static_cast<double>(1ULL << (join_leaves - 1)); // 2^(N-1)

		ivm_compute = scan_multiplier * total_base_scan;

		// Estimate delta result: each delta row fans out by MV/base_card
		estimated_delta_result = 0;
		for (auto &ts : table_stats) {
			double fanout = mv_card / ts.base_card;
			estimated_delta_result += ts.delta_card * fanout;
		}
	} else {
		// Unary operators: just scan deltas
		double total_delta = 0;
		for (auto &ts : table_stats) {
			total_delta += ts.delta_card;
		}
		ivm_compute = total_delta;
		estimated_delta_result = total_delta;
	}

	double ivm_upsert;
	if (has_aggregate) {
		// Aggregate merge: affected groups ≈ min(delta_result, MV groups)
		// Merge cost is proportional to affected groups, not full MV
		double affected_groups = std::min(estimated_delta_result, mv_card);
		ivm_upsert = affected_groups * 2.0; // read + write per group
	} else {
		// Projection/filter: targeted insert/delete
		// EXISTS subquery cost ≈ delta_result × log(MV) for each delete
		ivm_upsert = estimated_delta_result * (1.0 + std::log2(std::max(mv_card, 1.0)));
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

	OPENIVM_DEBUG_PRINT("[COST MODEL] Tables: %zu, Join: %s, Aggregate: %s\n", N, has_join ? "yes" : "no",
	                    has_aggregate ? "yes" : "no");
	OPENIVM_DEBUG_PRINT("[COST MODEL] Base scan total: %.0f, Delta fraction sum: %.4f\n", total_base_scan,
	                    delta_fraction_sum);
	OPENIVM_DEBUG_PRINT("[COST MODEL] MV cardinality: %.0f, Est. delta result: %.0f\n", mv_card,
	                    estimated_delta_result);
	OPENIVM_DEBUG_PRINT("[COST MODEL] IVM cost: %.0f (compute: %.0f, upsert: %.0f)\n", ivm_total, ivm_compute,
	                    ivm_upsert);
	OPENIVM_DEBUG_PRINT("[COST MODEL] Recompute cost: %.0f (compute: %.0f, replace: %.0f)\n", recompute_total,
	                    recompute_compute, recompute_replace);
	OPENIVM_DEBUG_PRINT("[COST MODEL] Decision: %s\n", ivm_total < recompute_total ? "IVM" : "RECOMPUTE");

	return {ivm_total, recompute_total};
}

string IVMCostQuery(ClientContext &context, const FunctionParameters &parameters) {
	auto view_name = StringValue::Get(parameters.values[0]);

	auto &db = DatabaseInstance::GetDatabase(context);
	Connection con(db);
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
	       to_string(estimate.recompute_cost) + " AS recompute_cost";
}

} // namespace duckdb
