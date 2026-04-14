#include "upsert/openivm_cost_model.hpp"
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
	double base_card;    // |T|
	double delta_card;   // |ΔT|
	bool has_fk = false; // table has FK referencing another table in the join
};

/// Aggregated plan statistics collected in a single tree walk.
struct PlanStats {
	vector<TableStats> table_stats;
	idx_t join_leaf_count = 0;
	bool has_join = false;
	bool has_aggregate = false;
	bool all_ducklake = true; // true until a non-DuckLake leaf is found
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
	case LogicalOperatorType::LOGICAL_JOIN:
		stats.has_join = true;
		break;
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY:
		stats.has_aggregate = true;
		break;
	default:
		break;
	}
	for (auto &child : op.children) {
		CollectPlanStatsRecursive(context, con, *child, view_name, stats);
	}
}

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
		return {0.0, 1.0};
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

	OPENIVM_DEBUG_PRINT("[COST MODEL] Tables: %zu, Join: %s, Aggregate: %s, DuckLake: %s, FK pruned PKs: %lu\n", N,
	                    has_join ? "yes" : "no", has_aggregate ? "yes" : "no", plan_stats.all_ducklake ? "yes" : "no",
	                    (unsigned long)fk_pk_leaf_count);
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
