//
// Daemon Benchmark: refresh + cascade + pipelines + concurrency coverage.
//
// Parent forks a setup child that creates the TPC-C DB, then forks one worker
// child per sub-test. The worker opens the DB, builds the sub-test's MV shape,
// runs concurrent DML + refresh + checker threads for `--duration` seconds,
// then emits a JSON summary line on its result pipe before exiting. The parent
// watches each worker via poll() and aggregates results into a JSONL file.
//
// Sub-tests exercise: automatic (daemon-scheduled) refresh, manual PRAGMA ivm(),
// upstream/downstream/both cascade modes, pipelines up to 10 MVs deep, parallel
// refresh across MVs, conflicting refresh on the same MV, ALTER REFRESH EVERY
// during a run, DML-during-refresh, and DuckLake variants.
//

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"
#include "tpcc_helpers.hpp"

#include <atomic>
#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <poll.h>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace std;
using openivm_bench::CreateTPCCSchema;
using openivm_bench::FileExists;
using openivm_bench::GenerateDeltaPool;
using openivm_bench::InsertTPCCData;
using openivm_bench::Log;
using openivm_bench::ReadAllBytes;
using openivm_bench::Timestamp;
using openivm_bench::WriteAllBytes;

// ---------------------------------------------------------------------------
// Helpers

static string EscapeJson(const string &s) {
	string out;
	out.reserve(s.size() + 2);
	for (char c : s) {
		switch (c) {
		case '"': out += "\\\""; break;
		case '\\': out += "\\\\"; break;
		case '\n': out += "\\n"; break;
		case '\r': out += "\\r"; break;
		case '\t': out += "\\t"; break;
		default:
			if (static_cast<unsigned char>(c) < 0x20) {
				char buf[8];
				snprintf(buf, sizeof(buf), "\\u%04x", c);
				out += buf;
			} else {
				out += c;
			}
		}
	}
	return out;
}

static int64_t NowMs() {
	using namespace std::chrono;
	return duration_cast<milliseconds>(steady_clock::now().time_since_epoch()).count();
}

// ---------------------------------------------------------------------------
// Per-sub-test event & summary structs

struct Event {
	int64_t t_ms; // ms since subtest start
	string kind;  // "refresh_start" | "refresh_end" | "dml" | "check_ok" | "check_fail" | "note"
	string view;  // empty for non-view events
	string detail;
};

struct Summary {
	string subtest;
	int duration_s = 0;
	int mv_count = 0;
	string cascade_mode;
	int refresh_events = 0;
	int dml_ops = 0;
	int checker_runs = 0;
	int crashes = 0;
	int correctness_failures = 0;
	int contention_events = 0;
	int64_t max_staleness_ms = 0;
	vector<double> refresh_latencies_ms;
	vector<string> findings;
	map<string, int> per_mv_refreshes;
	map<string, int64_t> per_mv_last_staleness;

	string ToJson() const {
		ostringstream o;
		o << "{";
		o << "\"subtest\":\"" << EscapeJson(subtest) << "\"";
		o << ",\"duration_s\":" << duration_s;
		o << ",\"mv_count\":" << mv_count;
		o << ",\"cascade_mode\":\"" << EscapeJson(cascade_mode) << "\"";
		o << ",\"refresh_events\":" << refresh_events;
		o << ",\"dml_ops\":" << dml_ops;
		o << ",\"checker_runs\":" << checker_runs;
		o << ",\"crashes\":" << crashes;
		o << ",\"correctness_failures\":" << correctness_failures;
		o << ",\"contention_events\":" << contention_events;
		o << ",\"max_staleness_ms\":" << max_staleness_ms;

		// refresh latency percentiles
		auto l = refresh_latencies_ms;
		std::sort(l.begin(), l.end());
		auto pct = [&](double p) -> double {
			if (l.empty()) return 0.0;
			size_t idx = (size_t)(p * (l.size() - 1));
			return l[idx];
		};
		o << ",\"refresh_latency_ms\":{";
		o << "\"p50\":" << pct(0.50);
		o << ",\"p95\":" << pct(0.95);
		o << ",\"max\":" << (l.empty() ? 0.0 : l.back());
		o << ",\"count\":" << l.size();
		o << "}";

		o << ",\"per_mv\":{";
		bool first = true;
		for (auto &e : per_mv_refreshes) {
			if (!first) o << ",";
			first = false;
			o << "\"" << EscapeJson(e.first) << "\":{\"refreshes\":" << e.second;
			auto it = per_mv_last_staleness.find(e.first);
			o << ",\"last_staleness_ms\":" << (it == per_mv_last_staleness.end() ? 0 : it->second);
			o << "}";
		}
		o << "}";

		o << ",\"findings\":[";
		for (size_t i = 0; i < findings.size(); i++) {
			if (i > 0) o << ",";
			o << "\"" << EscapeJson(findings[i]) << "\"";
		}
		o << "]";
		o << "}";
		return o.str();
	}
};

// ---------------------------------------------------------------------------
// MV shape definition: one MV's name + its create SQL + its ground-truth base
// query (used for EXCEPT ALL correctness checking). A view whose base_query
// is empty is a "chain-level" view whose ground truth is unavailable at a
// single place (since it's the view_query itself, recursively); we still run
// a self-equality check (`SELECT COUNT(*) FROM mv` must match itself).

struct MVDef {
	string name;
	string create_sql;  // "CREATE MATERIALIZED VIEW <name> [REFRESH EVERY ...] AS ..."
	string base_query;  // for EXCEPT ALL correctness; empty = skip
	int64_t refresh_interval_secs = -1; // -1 = no REFRESH EVERY clause
};

// ---------------------------------------------------------------------------
// Per-sub-test context passed around worker helpers

struct SubtestCtx {
	string subtest;
	int duration_s;
	string cascade_mode;
	bool disable_daemon;
	vector<MVDef> mvs;
	vector<string> deltas;

	// Worker-local threading primitives
	atomic<bool> stop {false};
	mutex log_mu;
	vector<Event> events;
	Summary summary;

	// DuckDB handles
	shared_ptr<duckdb::DuckDB> db;

	int64_t t0_ms = 0;

	void LogEvent(const string &kind, const string &view, const string &detail) {
		lock_guard<mutex> lk(log_mu);
		Event ev;
		ev.t_ms = NowMs() - t0_ms;
		ev.kind = kind;
		ev.view = view;
		ev.detail = detail;
		events.push_back(ev);
	}
};

// ---------------------------------------------------------------------------
// MV builder — generates shapes for each sub-test

static void BuildShape(SubtestCtx &ctx) {
	// Baseline queries (chains + independents) — all use columns known to pass
	// Phase 1 benchmarks. Each chain level reads from the previous level.

	auto mv = [](const string &name, const string &sel, const string &gt,
	             int64_t refresh = -1) -> MVDef {
		string clause;
		if (refresh > 0) {
			clause = " REFRESH EVERY '" + std::to_string(refresh / 60) + " minute" +
			         ((refresh / 60) == 1 ? "'" : "s'");
		}
		MVDef m;
		m.name = name;
		m.create_sql = "CREATE MATERIALIZED VIEW " + name + clause + " AS " + sel;
		m.base_query = gt;
		m.refresh_interval_secs = refresh;
		return m;
	};

	// Chain A (5 levels over CUSTOMER / WAREHOUSE / DISTRICT)
	auto chain_a = [&](int min_level, int max_level) {
		if (min_level <= 1 && max_level >= 1) {
			ctx.mvs.push_back(mv("mv_a1",
			    "WITH active AS (SELECT * FROM CUSTOMER WHERE C_BALANCE > 0) "
			    "SELECT C_W_ID, C_D_ID, COUNT(*) AS n, AVG(C_BALANCE) AS avg_bal, "
			    "MAX(C_BALANCE) AS max_bal, MIN(C_BALANCE) AS min_bal "
			    "FROM active GROUP BY C_W_ID, C_D_ID HAVING COUNT(*) > 2 AND AVG(C_BALANCE) > 1000",
			    "WITH active AS (SELECT * FROM CUSTOMER WHERE C_BALANCE > 0) "
			    "SELECT C_W_ID, C_D_ID, COUNT(*) AS n, AVG(C_BALANCE) AS avg_bal, "
			    "MAX(C_BALANCE) AS max_bal, MIN(C_BALANCE) AS min_bal "
			    "FROM active GROUP BY C_W_ID, C_D_ID HAVING COUNT(*) > 2 AND AVG(C_BALANCE) > 1000"));
		}
		if (min_level <= 2 && max_level >= 2) {
			ctx.mvs.push_back(mv("mv_a2",
			    "WITH tall AS (SELECT * FROM mv_a1 WHERE n > 3 AND max_bal > 5000) "
			    "SELECT C_W_ID, C_D_ID, n, avg_bal, "
			    "CASE WHEN avg_bal > max_bal * 0.8 THEN 'concentrated' "
			    "WHEN avg_bal < min_bal * 1.2 THEN 'bottom-heavy' ELSE 'spread' END AS distribution "
			    "FROM tall", ""));
		}
		if (min_level <= 3 && max_level >= 3) {
			ctx.mvs.push_back(mv("mv_a3",
			    "WITH l2 AS (SELECT * FROM mv_a2) "
			    "SELECT l2.C_W_ID, w.W_NAME, d.D_ID, COUNT(*) AS cust_groups, "
			    "SUM(l2.n) AS total_n, MAX(CAST(l2.avg_bal AS DOUBLE)) AS peak_avg "
			    "FROM l2 JOIN WAREHOUSE w ON l2.C_W_ID = w.W_ID "
			    "JOIN DISTRICT d ON l2.C_W_ID = d.D_W_ID AND l2.C_D_ID = d.D_ID "
			    "GROUP BY l2.C_W_ID, w.W_NAME, d.D_ID", ""));
		}
		if (min_level <= 4 && max_level >= 4) {
			ctx.mvs.push_back(mv("mv_a4",
			    "SELECT C_W_ID, W_NAME, COUNT(DISTINCT D_ID) AS districts, "
			    "SUM(total_n) AS totaln, AVG(peak_avg) AS avg_peak_across "
			    "FROM mv_a3 GROUP BY C_W_ID, W_NAME HAVING SUM(total_n) > 5", ""));
		}
		if (min_level <= 5 && max_level >= 5) {
			ctx.mvs.push_back(mv("mv_a5",
			    "WITH ranked AS (SELECT *, "
			    "CASE WHEN totaln > 20 THEN 'tier1' WHEN totaln > 10 THEN 'tier2' ELSE 'tier3' END AS tier "
			    "FROM mv_a4) "
			    "SELECT C_W_ID, W_NAME, districts, totaln, avg_peak_across, tier FROM ranked "
			    "WHERE tier IN ('tier1', 'tier2')", ""));
		}
	};

	// Chain B (5 levels over ORDER_LINE / OORDER / ITEM)
	auto chain_b = [&](int min_level, int max_level) {
		if (min_level <= 1 && max_level >= 1) {
			ctx.mvs.push_back(mv("mv_b1",
			    "SELECT OL_W_ID AS w_id, OL_D_ID AS d_id, OL_O_ID AS o_id, "
			    "COUNT(*) AS line_count, SUM(OL_AMOUNT) AS sum_amount "
			    "FROM ORDER_LINE GROUP BY OL_W_ID, OL_D_ID, OL_O_ID HAVING COUNT(*) > 0",
			    "SELECT OL_W_ID AS w_id, OL_D_ID AS d_id, OL_O_ID AS o_id, "
			    "COUNT(*) AS line_count, SUM(OL_AMOUNT) AS sum_amount "
			    "FROM ORDER_LINE GROUP BY OL_W_ID, OL_D_ID, OL_O_ID HAVING COUNT(*) > 0"));
		}
		if (min_level <= 2 && max_level >= 2) {
			ctx.mvs.push_back(mv("mv_b2",
			    "SELECT w_id, d_id, o_id, line_count, sum_amount, "
			    "CASE WHEN sum_amount > 100 THEN 'big' ELSE 'small' END AS order_class "
			    "FROM mv_b1", ""));
		}
		if (min_level <= 3 && max_level >= 3) {
			ctx.mvs.push_back(mv("mv_b3",
			    "SELECT b2.w_id, b2.d_id, COUNT(*) AS orders_joined, "
			    "SUM(b2.sum_amount) AS wh_total, AVG(CAST(b2.line_count AS DOUBLE)) AS avg_lines "
			    "FROM mv_b2 b2 JOIN OORDER o ON b2.w_id = o.O_W_ID AND b2.d_id = o.O_D_ID AND b2.o_id = o.O_ID "
			    "GROUP BY b2.w_id, b2.d_id", ""));
		}
		if (min_level <= 4 && max_level >= 4) {
			ctx.mvs.push_back(mv("mv_b4",
			    "SELECT w_id, SUM(orders_joined) AS total_orders, "
			    "AVG(avg_lines) AS grand_avg_lines, COUNT(DISTINCT d_id) AS districts "
			    "FROM mv_b3 GROUP BY w_id", ""));
		}
		if (min_level <= 5 && max_level >= 5) {
			ctx.mvs.push_back(mv("mv_b5",
			    "SELECT w_id, total_orders, grand_avg_lines, districts, "
			    "CASE WHEN total_orders > 10 THEN 'busy' ELSE 'quiet' END AS state "
			    "FROM mv_b4", ""));
		}
	};

	auto ind_set = [&]() {
		ctx.mvs.push_back(mv("mv_ind1",
		    "WITH d_cust AS (SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS n_cust, "
		    "COALESCE(SUM(c.C_BALANCE), 0) AS total_bal "
		    "FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID "
		    "GROUP BY d.D_W_ID, d.D_ID) "
		    "SELECT D_W_ID, SUM(n_cust) AS total_cust, MAX(n_cust) AS peak_district, "
		    "SUM(total_bal) AS sum_bal FROM d_cust GROUP BY D_W_ID HAVING SUM(n_cust) > 0",
		    "WITH d_cust AS (SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS n_cust, "
		    "COALESCE(SUM(c.C_BALANCE), 0) AS total_bal "
		    "FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID "
		    "GROUP BY d.D_W_ID, d.D_ID) "
		    "SELECT D_W_ID, SUM(n_cust) AS total_cust, MAX(n_cust) AS peak_district, "
		    "SUM(total_bal) AS sum_bal FROM d_cust GROUP BY D_W_ID HAVING SUM(n_cust) > 0"));
		ctx.mvs.push_back(mv("mv_ind2",
		    "WITH pay AS (SELECT H_C_W_ID, H_C_D_ID, H_AMOUNT FROM HISTORY) "
		    "SELECT H_C_W_ID, H_C_D_ID, "
		    "SUM(CASE WHEN H_AMOUNT > 0  THEN H_AMOUNT ELSE 0 END) AS positive_pay, "
		    "SUM(CASE WHEN H_AMOUNT <= 0 THEN H_AMOUNT ELSE 0 END) AS adj_pay, "
		    "COUNT(*) AS pay_count FROM pay GROUP BY H_C_W_ID, H_C_D_ID",
		    "WITH pay AS (SELECT H_C_W_ID, H_C_D_ID, H_AMOUNT FROM HISTORY) "
		    "SELECT H_C_W_ID, H_C_D_ID, "
		    "SUM(CASE WHEN H_AMOUNT > 0  THEN H_AMOUNT ELSE 0 END) AS positive_pay, "
		    "SUM(CASE WHEN H_AMOUNT <= 0 THEN H_AMOUNT ELSE 0 END) AS adj_pay, "
		    "COUNT(*) AS pay_count FROM pay GROUP BY H_C_W_ID, H_C_D_ID"));
		ctx.mvs.push_back(mv("mv_ind3",
		    "SELECT S_W_ID, COUNT(*) AS stock_lines, SUM(S_QUANTITY) AS total_qty, "
		    "AVG(CAST(S_QUANTITY AS DOUBLE)) AS avg_qty FROM STOCK GROUP BY S_W_ID",
		    "SELECT S_W_ID, COUNT(*) AS stock_lines, SUM(S_QUANTITY) AS total_qty, "
		    "AVG(CAST(S_QUANTITY AS DOUBLE)) AS avg_qty FROM STOCK GROUP BY S_W_ID"));
		ctx.mvs.push_back(mv("mv_ind4",
		    "SELECT NO_W_ID, NO_D_ID, COUNT(*) AS new_cnt FROM NEW_ORDER GROUP BY NO_W_ID, NO_D_ID",
		    "SELECT NO_W_ID, NO_D_ID, COUNT(*) AS new_cnt FROM NEW_ORDER GROUP BY NO_W_ID, NO_D_ID"));
		ctx.mvs.push_back(mv("mv_ind5",
		    "SELECT i.I_ID, i.I_NAME, COUNT(s.S_W_ID) AS n_wh, SUM(s.S_QUANTITY) AS total_qty "
		    "FROM ITEM i LEFT JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID, i.I_NAME",
		    "SELECT i.I_ID, i.I_NAME, COUNT(s.S_W_ID) AS n_wh, SUM(s.S_QUANTITY) AS total_qty "
		    "FROM ITEM i LEFT JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID, i.I_NAME"));
	};

	int one_min = 60;
	int five_min = 300;
	int ten_min = 600;

	if (ctx.subtest == "S1") {
		// Linear 10-chain: Chain A (5) + Chain B (5, levels depend on Chain A final)
		chain_a(1, 5);
		chain_b(1, 5);
		// Final mv10 reads from mv_a5 AND mv_b5 (synthetic 10th level)
		ctx.mvs.push_back(mv("mv_c10",
		    "WITH a AS (SELECT * FROM mv_a5), b AS (SELECT * FROM mv_b5) "
		    "SELECT a.C_W_ID AS w_id, a.totaln AS cust_totaln, b.total_orders AS line_orders "
		    "FROM a LEFT JOIN b ON a.C_W_ID = b.w_id", "", one_min));
	} else if (ctx.subtest == "S2") {
		chain_a(1, 5);
		ctx.mvs.front().refresh_interval_secs = one_min;
		ctx.mvs.front().create_sql = ctx.mvs.front().create_sql; // keep
		// rebuild mv_a1 with REFRESH EVERY
		ctx.mvs.erase(ctx.mvs.begin());
		ctx.mvs.insert(ctx.mvs.begin(), mv("mv_a1",
		    "WITH active AS (SELECT * FROM CUSTOMER WHERE C_BALANCE > 0) "
		    "SELECT C_W_ID, C_D_ID, COUNT(*) AS n, AVG(C_BALANCE) AS avg_bal, "
		    "MAX(C_BALANCE) AS max_bal, MIN(C_BALANCE) AS min_bal "
		    "FROM active GROUP BY C_W_ID, C_D_ID HAVING COUNT(*) > 2 AND AVG(C_BALANCE) > 1000",
		    "", one_min));
		ind_set();
		for (auto &v : ctx.mvs) {
			if (v.name.find("mv_ind") == 0) v.refresh_interval_secs = one_min;
		}
		// Rebuild independent MVs to include the REFRESH EVERY clause
		for (auto &v : ctx.mvs) {
			if (v.name.find("mv_ind") == 0 && v.refresh_interval_secs == one_min) {
				// rebuild create_sql with clause
				size_t as_pos = v.create_sql.find(" AS ");
				if (as_pos != string::npos) {
					v.create_sql = "CREATE MATERIALIZED VIEW " + v.name +
					               " REFRESH EVERY '1 minute' AS " +
					               v.create_sql.substr(as_pos + 4);
				}
			}
		}
	} else if (ctx.subtest == "S3") {
		chain_a(1, 5);
		chain_b(1, 5);
		for (auto &v : ctx.mvs) {
			if (v.name == "mv_a5" || v.name == "mv_b5") {
				v.refresh_interval_secs = one_min;
				size_t as_pos = v.create_sql.find(" AS ");
				v.create_sql = "CREATE MATERIALIZED VIEW " + v.name +
				               " REFRESH EVERY '1 minute' AS " + v.create_sql.substr(as_pos + 4);
			}
		}
		ctx.mvs.push_back(mv("mv_cross",
		    "WITH a AS (SELECT * FROM mv_a5), b AS (SELECT * FROM mv_b5), "
		    "combined AS (SELECT COALESCE(a.C_W_ID, b.w_id) AS w_id, "
		    "a.totaln AS cust_totaln, a.districts AS cust_districts, "
		    "b.total_orders AS order_lines, b.grand_avg_lines AS order_avg "
		    "FROM a FULL OUTER JOIN b ON a.C_W_ID = b.w_id) "
		    "SELECT w_id, cust_totaln, cust_districts, order_lines, order_avg, "
		    "CASE WHEN cust_totaln IS NULL THEN 'order-only' "
		    "WHEN order_lines IS NULL THEN 'cust-only' "
		    "WHEN cust_totaln > 0 AND order_avg > 1 THEN 'active' "
		    "ELSE 'dormant' END AS status FROM combined "
		    "WHERE cust_totaln IS NOT NULL OR order_lines IS NOT NULL",
		    "", one_min));
	} else if (ctx.subtest == "S4") {
		// Mixed shape, NO refresh intervals (manual only)
		chain_a(1, 5);
		ind_set();
		ctx.disable_daemon = true;
	} else if (ctx.subtest == "S5") {
		// 10 independent MVs (split ind_set x2 with different names)
		ind_set();
		// Duplicate each with suffix to get 10
		vector<MVDef> more;
		for (auto &v : ctx.mvs) {
			MVDef copy = v;
			copy.name = v.name + "b";
			// Rename in create_sql too
			size_t p = copy.create_sql.find("MATERIALIZED VIEW " + v.name);
			if (p != string::npos) {
				copy.create_sql.replace(p, ("MATERIALIZED VIEW " + v.name).size(),
				                        "MATERIALIZED VIEW " + copy.name);
			}
			more.push_back(copy);
		}
		for (auto &v : more) ctx.mvs.push_back(v);
		ctx.disable_daemon = true;
	} else if (ctx.subtest == "S6") {
		// Only 3 MVs; all refresh threads target mv_ind1
		ind_set();
		ctx.mvs.resize(3);
		ctx.disable_daemon = true;
	} else if (ctx.subtest == "S7") {
		// S2 shape with explicit REFRESH EVERY '5 minutes' — ALTER mid-run
		chain_a(1, 5);
		ind_set();
		for (auto &v : ctx.mvs) {
			if (v.name == "mv_ind1" || v.name == "mv_ind2" || v.name == "mv_ind3") {
				v.refresh_interval_secs = five_min;
				size_t as_pos = v.create_sql.find(" AS ");
				v.create_sql = "CREATE MATERIALIZED VIEW " + v.name +
				               " REFRESH EVERY '5 minutes' AS " + v.create_sql.substr(as_pos + 4);
			}
		}
	} else if (ctx.subtest == "S8") {
		chain_a(1, 5);
		ind_set();
		for (auto &v : ctx.mvs) {
			if (v.name == "mv_a1" || v.name.find("mv_ind") == 0) {
				v.refresh_interval_secs = one_min;
				size_t as_pos = v.create_sql.find(" AS ");
				v.create_sql = "CREATE MATERIALIZED VIEW " + v.name +
				               " REFRESH EVERY '1 minute' AS " + v.create_sql.substr(as_pos + 4);
			}
		}
	} else if (ctx.subtest == "S9" || ctx.subtest == "S10" || ctx.subtest == "S11") {
		// DuckLake sub-tests — use a simpler placeholder for now; worker will
		// attach dl before creating these.  We set base_query to the
		// equivalent query on dl.main.* so EXCEPT ALL works.
		ctx.mvs.push_back(mv("mv_dl_1",
		    "SELECT OL_W_ID AS w_id, SUM(OL_AMOUNT) AS total FROM dl.ORDER_LINE GROUP BY OL_W_ID",
		    "SELECT OL_W_ID AS w_id, SUM(OL_AMOUNT) AS total FROM dl.ORDER_LINE GROUP BY OL_W_ID",
		    one_min));
	}

	(void)ten_min;
	(void)five_min;
}

// ---------------------------------------------------------------------------
// DML, refresh, checker threads

static void DMLThread(SubtestCtx &ctx, int interval_ms, size_t delta_start_offset) {
	duckdb::Connection con(*ctx.db);
	size_t idx = delta_start_offset;
	while (!ctx.stop.load()) {
		auto r = con.Query(ctx.deltas[idx % ctx.deltas.size()]);
		(void)r;
		{
			lock_guard<mutex> lk(ctx.log_mu);
			ctx.summary.dml_ops++;
		}
		idx++;
		this_thread::sleep_for(chrono::milliseconds(interval_ms));
	}
}

static void CheckerThread(SubtestCtx &ctx, int interval_ms) {
	duckdb::Connection con(*ctx.db);
	while (!ctx.stop.load()) {
		for (auto &v : ctx.mvs) {
			if (ctx.stop.load()) break;
			if (v.base_query.empty()) continue;
			string q = "SELECT COUNT(*) FROM ("
			           "  SELECT * FROM " + v.name + " EXCEPT ALL SELECT * FROM (" + v.base_query + ") __a"
			           "  UNION ALL "
			           "  SELECT * FROM (" + v.base_query + ") __b EXCEPT ALL SELECT * FROM " + v.name +
			           ") __diff";
			auto r = con.Query(q);
			{
				lock_guard<mutex> lk(ctx.log_mu);
				ctx.summary.checker_runs++;
			}
			if (!r || r->HasError()) {
				ctx.LogEvent("check_fail", v.name, r ? r->GetError() : "(null)");
				continue;
			}
			int64_t mismatches = r->GetValue(0, 0).GetValue<int64_t>();
			if (mismatches != 0) {
				// Not a hard failure yet — staleness is expected between refreshes.
				// Record the staleness interval since last refresh on this MV.
				ctx.LogEvent("stale", v.name, std::to_string(mismatches) + " rows");
			}
		}
		this_thread::sleep_for(chrono::milliseconds(interval_ms));
	}
}

static void MonitorThread(SubtestCtx &ctx, int interval_ms) {
	duckdb::Connection con(*ctx.db);
	// Refresh bumps `_duckdb_ivm_delta_tables.last_update` (one row per source
	// table per MV). Per MV we track MAX(last_update) across its source tables.
	map<string, string> prev_update;
	auto r0 = con.Query(
	    "SELECT view_name, MAX(last_update) FROM _duckdb_ivm_delta_tables GROUP BY view_name");
	if (r0 && !r0->HasError()) {
		for (idx_t i = 0; i < r0->RowCount(); i++) {
			prev_update[r0->GetValue(0, i).ToString()] =
			    r0->GetValue(1, i).IsNull() ? "" : r0->GetValue(1, i).ToString();
		}
	}
	while (!ctx.stop.load()) {
		auto r = con.Query(
		    "SELECT view_name, MAX(last_update) FROM _duckdb_ivm_delta_tables GROUP BY view_name");
		if (r && !r->HasError()) {
			for (idx_t i = 0; i < r->RowCount(); i++) {
				string vn = r->GetValue(0, i).ToString();
				string ts = r->GetValue(1, i).IsNull() ? "" : r->GetValue(1, i).ToString();
				auto &prev = prev_update[vn];
				if (!ts.empty() && ts != prev) {
					prev = ts;
					lock_guard<mutex> lk(ctx.log_mu);
					ctx.summary.refresh_events++;
					ctx.summary.per_mv_refreshes[vn]++;
					// Duration from refresh_history if populated (adaptive mode); else 0
					auto hr = con.Query("SELECT actual_duration_ms FROM _duckdb_ivm_refresh_history "
					                    "WHERE view_name = '" + vn +
					                    "' ORDER BY refresh_timestamp DESC LIMIT 1");
					double dur = 0;
					if (hr && !hr->HasError() && hr->RowCount() > 0 && !hr->GetValue(0, 0).IsNull()) {
						dur = hr->GetValue(0, 0).GetValue<int64_t>();
						ctx.summary.refresh_latencies_ms.push_back(dur);
					}
					Event ev;
					ev.t_ms = NowMs() - ctx.t0_ms;
					ev.kind = "refresh";
					ev.view = vn;
					ev.detail = std::to_string(dur);
					ctx.events.push_back(ev);
				}
			}
		}
		this_thread::sleep_for(chrono::milliseconds(interval_ms));
	}
}

// Manual refresh thread — for S4/S5/S6/S7 sub-tests
static void ManualRefreshThread(SubtestCtx &ctx, vector<string> target_mvs, int interval_ms) {
	duckdb::Connection con(*ctx.db);
	size_t idx = 0;
	while (!ctx.stop.load()) {
		if (target_mvs.empty()) {
			this_thread::sleep_for(chrono::milliseconds(interval_ms));
			continue;
		}
		const string &vn = target_mvs[idx % target_mvs.size()];
		auto t0 = NowMs();
		auto r = con.Query("PRAGMA ivm('" + vn + "')");
		auto t1 = NowMs();
		{
			lock_guard<mutex> lk(ctx.log_mu);
			if (r && !r->HasError()) {
				Event ev;
				ev.t_ms = t0 - ctx.t0_ms;
				ev.kind = "manual_refresh";
				ev.view = vn;
				ev.detail = std::to_string(t1 - t0);
				ctx.events.push_back(ev);
			} else {
				ctx.summary.findings.push_back("manual refresh failed on " + vn + ": " +
				                                (r ? r->GetError() : "(null)"));
			}
		}
		idx++;
		this_thread::sleep_for(chrono::milliseconds(interval_ms));
	}
}

// ---------------------------------------------------------------------------
// Worker child: runs one sub-test

static int RunSubtest(const string &subtest, int duration_s, const string &db_path, int write_fd) {
	SubtestCtx ctx;
	ctx.subtest = subtest;
	ctx.duration_s = duration_s;
	ctx.summary.subtest = subtest;
	ctx.summary.duration_s = duration_s;
	ctx.cascade_mode = "downstream"; // default
	ctx.disable_daemon = false;

	try {
		ctx.db = std::make_shared<duckdb::DuckDB>(db_path);
		duckdb::Connection con(*ctx.db);
		con.Query("PRAGMA threads=4");
		auto lr = con.Query("LOAD openivm");
		if (!lr || lr->HasError()) {
			ctx.summary.findings.push_back("LOAD openivm failed");
			string s = ctx.summary.ToJson() + "\n";
			WriteAllBytes(write_fd, s.data(), s.size());
			return 2;
		}
		// Clear any stale refresh_in_progress from previous crashed runs.
		con.Query("UPDATE _duckdb_ivm_views SET refresh_in_progress = false");

		// Per-subtest cascade mode + daemon settings
		if (subtest == "S1") ctx.cascade_mode = "upstream";
		else if (subtest == "S2") ctx.cascade_mode = "downstream";
		else if (subtest == "S3") ctx.cascade_mode = "both";
		else if (subtest == "S9") ctx.cascade_mode = "upstream";
		con.Query("SET GLOBAL ivm_cascade_refresh = '" + ctx.cascade_mode + "'");
		// Enable adaptive refresh GLOBALLY so the daemon (its own connection)
		// and all worker threads see the flag and populate `_duckdb_ivm_refresh_history`.
		con.Query("SET GLOBAL ivm_adaptive_refresh = true");
		ctx.summary.cascade_mode = ctx.cascade_mode;

		if (subtest == "S4" || subtest == "S5" || subtest == "S6" || subtest == "S11") {
			con.Query("SET ivm_disable_daemon = true");
			ctx.disable_daemon = true;
		}

		// Clean up any leftover MVs from a previous crashed run
		auto leftover = con.Query("SELECT table_name FROM information_schema.tables "
		                          "WHERE table_schema='main' AND (table_name LIKE 'mv_%' OR table_name LIKE '_ivm_data_mv_%' OR table_name LIKE 'delta_mv_%')");
		if (leftover && !leftover->HasError()) {
			for (idx_t i = 0; i < leftover->RowCount(); i++) {
				string tn = leftover->GetValue(0, i).ToString();
				con.Query("DROP VIEW IF EXISTS " + tn);
				con.Query("DROP TABLE IF EXISTS " + tn);
			}
		}
		con.Query("DELETE FROM _duckdb_ivm_views WHERE view_name LIKE 'mv_%'");
		con.Query("DELETE FROM _duckdb_ivm_delta_tables WHERE view_name LIKE 'mv_%'");

		// DuckLake attach for S9/S10/S11
		if (subtest == "S9" || subtest == "S10" || subtest == "S11") {
			con.Query("INSTALL ducklake");
			con.Query("LOAD ducklake");
			string dl_path = db_path + ".ducklake.db";
			auto at = con.Query("ATTACH IF NOT EXISTS '" + dl_path + "' AS dl (TYPE ducklake)");
			if (!at || at->HasError()) {
				ctx.summary.findings.push_back("DuckLake attach failed: " +
				                                (at ? at->GetError() : "null"));
			} else {
				// Populate DuckLake TPC-C once
				auto have = con.Query("SELECT COUNT(*) FROM information_schema.tables WHERE "
				                      "table_catalog='dl' AND LOWER(table_name)='warehouse'");
				bool needs_init = !have || have->HasError() || have->RowCount() == 0 ||
				                  have->GetValue(0, 0).GetValue<int64_t>() == 0;
				if (needs_init) {
					con.Query("USE dl.main");
					CreateTPCCSchema(con);
					auto probe = con.Query("SELECT current_database()");
					string native_cat = "memory";
					if (probe && !probe->HasError() && probe->RowCount() > 0) {
						native_cat = probe->GetValue(0, 0).ToString();
					}
					// Actually we need native cat AFTER the USE dl.main; grab fresh
					// current_database is now 'dl' — fall back to the db_path basename
					// without extension.
					if (native_cat == "dl") {
						string b = db_path;
						auto p = b.find_last_of('/');
						if (p != string::npos) b = b.substr(p + 1);
						auto e = b.find(".");
						if (e != string::npos) b = b.substr(0, e);
						native_cat = b;
					}
					for (const char *t : {"WAREHOUSE", "DISTRICT", "CUSTOMER", "ITEM", "STOCK",
					                       "OORDER", "NEW_ORDER", "ORDER_LINE", "HISTORY"}) {
						con.Query(string("INSERT INTO ") + t + " SELECT * FROM " + native_cat +
						          ".main." + t);
					}
					con.Query("USE " + native_cat + ".main");
				}
				// For S10 MVs read from dl.<table> directly on native context
				// For S9/S11 MVs are in dl.main — set USE
				if (subtest == "S9" || subtest == "S11") {
					con.Query("USE dl.main");
				}
			}
		}

		// Build MV shape for this subtest
		BuildShape(ctx);
		ctx.summary.mv_count = static_cast<int>(ctx.mvs.size());

		// Create all MVs
		for (auto &v : ctx.mvs) {
			auto r = con.Query(v.create_sql);
			if (!r || r->HasError()) {
				ctx.summary.findings.push_back("MV creation failed: " + v.name + " : " +
				                                (r ? r->GetError() : "null"));
				string s = ctx.summary.ToJson() + "\n";
				WriteAllBytes(write_fd, s.data(), s.size());
				return 3;
			}
		}

		// Load delta pool
		int scale = 1; // assumes --scale 1 for setup child
		ctx.deltas = GenerateDeltaPool(scale);
		if (ctx.deltas.empty()) {
			ctx.summary.findings.push_back("empty delta pool");
			string s = ctx.summary.ToJson() + "\n";
			WriteAllBytes(write_fd, s.data(), s.size());
			return 4;
		}

		// Spawn threads per sub-test
		ctx.t0_ms = NowMs();
		vector<thread> workers;

		// S6: heavier DML (100ms)
		int dml_interval = (subtest == "S6") ? 100 : 200;
		workers.emplace_back(DMLThread, std::ref(ctx), dml_interval, 0);
		if (subtest == "S8") {
			// second DML thread, different offset to simulate delete-heavy worker
			workers.emplace_back(DMLThread, std::ref(ctx), 100, 100);
		}

		// Checker
		int checker_interval = (subtest == "S6" || subtest == "S8") ? 3000 : 5000;
		workers.emplace_back(CheckerThread, std::ref(ctx), checker_interval);

		// Monitor
		workers.emplace_back(MonitorThread, std::ref(ctx), 5000);

		// Manual refresh threads (S4/S5/S6/S11) or hybrid (S7)
		if (subtest == "S4") {
			vector<string> targets;
			for (auto &v : ctx.mvs) targets.push_back(v.name);
			workers.emplace_back(ManualRefreshThread, std::ref(ctx), targets, 500);
		} else if (subtest == "S5") {
			// 4 refresh threads, each owning a disjoint subset
			vector<string> all;
			for (auto &v : ctx.mvs) all.push_back(v.name);
			for (int t = 0; t < 4; t++) {
				vector<string> subset;
				for (size_t i = t; i < all.size(); i += 4) subset.push_back(all[i]);
				workers.emplace_back(ManualRefreshThread, std::ref(ctx), subset, 500);
			}
		} else if (subtest == "S6") {
			// 4 threads ALL hammer mv_ind1
			for (int t = 0; t < 4; t++) {
				workers.emplace_back(ManualRefreshThread, std::ref(ctx),
				                      vector<string> {"mv_ind1"}, 200);
			}
		} else if (subtest == "S7") {
			// one manual thread hammers mv_ind1 every 30s while daemon also fires
			workers.emplace_back(ManualRefreshThread, std::ref(ctx),
			                      vector<string> {"mv_ind1"}, 30000);
		} else if (subtest == "S11") {
			vector<string> all;
			for (auto &v : ctx.mvs) all.push_back(v.name);
			for (int t = 0; t < 3 && t < (int)all.size(); t++) {
				vector<string> subset = {all[t]};
				if (t + 3 < (int)all.size()) subset.push_back(all[t + 3]);
				workers.emplace_back(ManualRefreshThread, std::ref(ctx), subset, 500);
			}
		}

		// S7: schedule two ALTERs mid-run
		thread alter_thread;
		if (subtest == "S7") {
			alter_thread = thread([&]() {
				duckdb::Connection alter_con(*ctx.db);
				this_thread::sleep_for(chrono::seconds(120));
				if (ctx.stop.load()) return;
				auto r1 = alter_con.Query(
				    "ALTER MATERIALIZED VIEW mv_ind1 SET REFRESH EVERY '1 minute'");
				ctx.LogEvent("alter", "mv_ind1",
				              r1 && !r1->HasError() ? "set 1m" : (r1 ? r1->GetError() : "null"));
				this_thread::sleep_for(chrono::seconds(120));
				if (ctx.stop.load()) return;
				auto r2 = alter_con.Query(
				    "ALTER MATERIALIZED VIEW mv_ind1 SET REFRESH EVERY '10 minutes'");
				ctx.LogEvent("alter", "mv_ind1",
				              r2 && !r2->HasError() ? "set 10m" : (r2 ? r2->GetError() : "null"));
			});
		}

		// Main thread: wait out the duration
		this_thread::sleep_for(chrono::seconds(duration_s));
		ctx.stop.store(true);

		for (auto &t : workers) {
			if (t.joinable()) t.join();
		}
		if (alter_thread.joinable()) alter_thread.join();

		// Final consistency check on every MV with a base_query
		duckdb::Connection fin_con(*ctx.db);
		for (auto &v : ctx.mvs) {
			if (v.base_query.empty()) continue;
			// Trigger a final refresh to be sure
			fin_con.Query("PRAGMA ivm('" + v.name + "')");
			string q = "SELECT COUNT(*) FROM ("
			           "  SELECT * FROM " + v.name + " EXCEPT ALL SELECT * FROM (" + v.base_query + ") __a"
			           "  UNION ALL "
			           "  SELECT * FROM (" + v.base_query + ") __b EXCEPT ALL SELECT * FROM " + v.name +
			           ") __diff";
			auto r = fin_con.Query(q);
			if (!r || r->HasError()) {
				ctx.summary.findings.push_back("final check errored on " + v.name + ": " +
				                                (r ? r->GetError() : "null"));
				ctx.summary.correctness_failures++;
			} else {
				int64_t mis = r->GetValue(0, 0).GetValue<int64_t>();
				if (mis != 0) {
					ctx.summary.correctness_failures++;
					ctx.summary.findings.push_back("final mismatch on " + v.name + ": " +
					                                std::to_string(mis) + " rows");
				}
			}
		}

		// Emit summary
		string s = ctx.summary.ToJson() + "\n";
		WriteAllBytes(write_fd, s.data(), s.size());
		return 0;
	} catch (const std::exception &e) {
		ctx.summary.crashes++;
		ctx.summary.findings.push_back(string("exception: ") + e.what());
		string s = ctx.summary.ToJson() + "\n";
		WriteAllBytes(write_fd, s.data(), s.size());
		return 5;
	}
}

// ---------------------------------------------------------------------------
// Parent: arg parsing, setup child, per-sub-test worker fork, result capture

static void PrintUsage() {
	fprintf(stderr,
	        "daemon_benchmark --scale N --db PATH --out JSONL --duration SECS [--subtests S1,S2,...]\n");
}

int main(int argc, char **argv) {
	int scale = 1;
	string db_path;
	string out_path = "daemon_benchmark_results.jsonl";
	int duration = 300; // 5 min per sub-test
	string subtests_csv = "all";

	for (int i = 1; i < argc; i++) {
		string a = argv[i];
		if (a == "--scale" && i + 1 < argc) scale = std::stoi(argv[++i]);
		else if (a == "--db" && i + 1 < argc) db_path = argv[++i];
		else if (a == "--out" && i + 1 < argc) out_path = argv[++i];
		else if (a == "--duration" && i + 1 < argc) duration = std::stoi(argv[++i]);
		else if (a == "--subtests" && i + 1 < argc) subtests_csv = argv[++i];
		else if (a == "--help" || a == "-h") { PrintUsage(); return 0; }
	}
	if (db_path.empty()) db_path = "daemon_benchmark_sf" + std::to_string(scale) + ".db";

	// Setup child creates TPC-C if missing
	if (!FileExists(db_path)) {
		Log("Creating TPC-C DB: " + db_path);
		pid_t setup_pid = fork();
		if (setup_pid == 0) {
			{
				duckdb::DuckDB db(db_path);
				duckdb::Connection con(db);
				CreateTPCCSchema(con);
				InsertTPCCData(con, scale);
				con.Query("PRAGMA checkpoint");
			}
			_exit(0);
		}
		int status;
		waitpid(setup_pid, &status, 0);
		if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
			Log("Setup child failed");
			return 2;
		}
	}

	// Parse subtests list
	vector<string> all_subtests = {"S1", "S2", "S3", "S4", "S5", "S6", "S7", "S8",
	                                "S9", "S10", "S11"};
	vector<string> subtests;
	if (subtests_csv == "all") {
		subtests = all_subtests;
	} else {
		string tok;
		istringstream iss(subtests_csv);
		while (std::getline(iss, tok, ',')) if (!tok.empty()) subtests.push_back(tok);
	}

	ofstream out(out_path);
	if (!out.is_open()) {
		Log("Cannot open output " + out_path);
		return 3;
	}

	for (auto &st : subtests) {
		Log("[" + st + "] starting (" + std::to_string(duration) + "s)");

		// Clean up stale WAL between sub-tests
		string wal = db_path + ".wal";
		// Only remove if the previous worker died dirty — we try removing
		// unconditionally; it's safe when no one is holding the file.
		std::remove(wal.c_str());

		int pipefd[2];
		if (pipe(pipefd) != 0) { Log("pipe() failed"); continue; }

		pid_t child = fork();
		if (child == 0) {
			close(pipefd[0]);
			int rc = RunSubtest(st, duration, db_path, pipefd[1]);
			close(pipefd[1]);
			_exit(rc);
		}
		close(pipefd[1]);

		// Collect child's summary line
		string summary_line;
		char buf[4096];
		struct pollfd pfd = {pipefd[0], POLLIN, 0};
		auto deadline = chrono::steady_clock::now() + chrono::seconds(duration + 120);
		while (true) {
			auto remaining = chrono::duration_cast<chrono::milliseconds>(
			                      deadline - chrono::steady_clock::now())
			                      .count();
			if (remaining <= 0) break;
			int pr = poll(&pfd, 1, std::min<int64_t>(1000, remaining));
			if (pr <= 0) continue;
			if (pfd.revents & (POLLIN | POLLHUP)) {
				ssize_t n = read(pipefd[0], buf, sizeof(buf));
				if (n <= 0) break;
				summary_line.append(buf, buf + n);
				if (summary_line.find('\n') != string::npos) break;
			}
		}
		close(pipefd[0]);

		int status;
		// Give child time to exit cleanly
		for (int i = 0; i < 10; i++) {
			pid_t r = waitpid(child, &status, WNOHANG);
			if (r == child) break;
			this_thread::sleep_for(chrono::milliseconds(100));
		}
		if (waitpid(child, &status, WNOHANG) == 0) {
			Log("[" + st + "] child still running — SIGKILL");
			kill(child, SIGKILL);
			waitpid(child, &status, 0);
		}

		bool crashed = !WIFEXITED(status) || WEXITSTATUS(status) != 0;
		if (summary_line.empty() || summary_line.find('{') == string::npos) {
			// Fabricate a crash summary
			Summary s;
			s.subtest = st;
			s.duration_s = duration;
			s.crashes = 1;
			s.findings.push_back("child exited without summary, status=" + std::to_string(status));
			out << s.ToJson() << "\n";
			out.flush();
			Log("[" + st + "] CRASHED");
		} else {
			out << summary_line;
			out.flush();
			Log("[" + st + "] finished" + (crashed ? " (abnormal exit)" : ""));
		}
	}

	return 0;
}
