//
// Cost Model Benchmark: compares PRAGMA refresh_cost estimates against measured
// auto / forced-incremental / forced-full refresh times.
//
// Usage:
//   cost_model_benchmark --scale 10 --delta-pcts 0,1,5,10,20,50 --reps 3 --out cost_model_sf10.csv
//

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "tpcc_helpers.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cctype>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unistd.h>
#include <vector>

using namespace std;
using openivm_bench::CreateTPCCSchema;
using openivm_bench::FileExists;
using openivm_bench::InsertTPCCData;
using openivm_bench::Log;

enum class Workload {
	INSERT_ONLY,
	MIXED,
	EMPTY_DELTA,
};

enum class FlagConfig {
	ALL_ON,
	ALL_OFF,
	SKIP_EMPTY_OFF,
};

enum class RefreshMode {
	AUTO,
	INCREMENTAL,
	FULL,
};

// VALIDATED queries are the known-good baseline (run periodically as a regression gate); TODO queries
// exercise operators/shapes still being brought under test. Keep VALIDATED first so it is the value an
// existing brace-initializer (which omits the trailing field) gets by default.
enum class Batch {
	VALIDATED,
	TODO,
};

struct QueryDef {
	string id;
	string description;
	vector<string> setup_sql;
	vector<string> query_settings;
	vector<string> create_mvs;
	vector<string> refresh_mvs;
	vector<string> touched_tables;
	string base_sql;
	vector<Workload> workloads;
	Batch batch;
};

struct CostEstimate {
	string decision;
	double incremental_cost = 0;
	double recompute_cost = 0;
	double incremental_predicted_ms = 0;
	double recompute_predicted_ms = 0;
	bool calibrated = false;
	bool ok = false;
	string error;
};

struct ModeResult {
	bool ok = false;
	bool correct = false;
	double refresh_ms = 0;
	string method;
	int64_t base_rows = 0;
	int64_t mv_rows = 0;
	int64_t dml_statements = 0;
	int64_t delta_rows = 0;
	string error;
	CostEstimate cost;
};

static int64_t kPkBase = 100000;

static int64_t NowMicros() {
	using namespace std::chrono;
	return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

static const char *WorkloadName(Workload w) {
	switch (w) {
	case Workload::INSERT_ONLY:
		return "insert_only";
	case Workload::MIXED:
		return "mixed";
	case Workload::EMPTY_DELTA:
		return "empty_delta";
	}
	return "?";
}

static const char *FlagConfigName(FlagConfig c) {
	switch (c) {
	case FlagConfig::ALL_ON:
		return "all_on";
	case FlagConfig::ALL_OFF:
		return "all_off";
	case FlagConfig::SKIP_EMPTY_OFF:
		return "skip_empty_off";
	}
	return "?";
}

static string CsvQuote(const string &s) {
	string out;
	out.reserve(s.size() + 2);
	out.push_back('"');
	for (char c : s) {
		if (c == '"') {
			out += "\"\"";
		} else if (c == '\n' || c == '\r') {
			out.push_back(' ');
		} else {
			out.push_back(c);
		}
	}
	out.push_back('"');
	return out;
}

// Parse a comma-separated list of (possibly fractional) percentages, e.g. "0,0.01,1,2,5,10,20,50".
// Fractional percentages matter at large scale factors where 0.01% of the base is still many rows.
static vector<double> ParseDoubleList(const string &s) {
	vector<double> out;
	size_t start = 0;
	while (start < s.size()) {
		size_t end = s.find(',', start);
		if (end == string::npos) {
			end = s.size();
		}
		if (end > start) {
			out.push_back(std::stod(s.substr(start, end - start)));
		}
		start = end + 1;
	}
	return out;
}

static bool CopyFile(const string &src, const string &dst) {
	int in_fd = open(src.c_str(), O_RDONLY);
	if (in_fd < 0) {
		return false;
	}
	int out_fd = open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (out_fd < 0) {
		close(in_fd);
		return false;
	}
	char buf[1 << 16];
	ssize_t n;
	while ((n = read(in_fd, buf, sizeof(buf))) > 0) {
		ssize_t written = 0;
		while (written < n) {
			ssize_t w = write(out_fd, buf + written, n - written);
			if (w <= 0) {
				close(in_fd);
				close(out_fd);
				return false;
			}
			written += w;
		}
	}
	close(in_fd);
	close(out_fd);
	return n == 0;
}

struct TempDb {
	string path;
	string wal;

	bool keep = false;

	// `case_tag` identifies the case and `variant` the file's role within it. They are separated by a
	// dot deliberately: DuckDB derives a database's catalog name from the filename up to the FIRST
	// dot, so every file of one case resolves to the same catalog. That matters because a
	// materialized view's metadata records the catalog it was created in, and a refresh run against a
	// copy under a different catalog name fails with "Catalog ... does not exist".
	//
	// The previous naming satisfied this by accident: the delta percentage was formatted into the
	// filename, and the resulting "1.000000" truncated every variant to the same catalog. Removing
	// the percentage from the name broke every refresh at once.
	TempDb(const string &case_tag, const string &variant, bool keep_p = false) : keep(keep_p) {
		path = "/tmp/cost_model_bench_" + to_string(getpid()) + "_" + case_tag + "." + variant + ".db";
		wal = path + ".wal";
		std::remove(path.c_str());
		std::remove(wal.c_str());
	}

	~TempDb() {
		if (keep) {
			return; // promoted to the case's live state; the caller owns it now
		}
		std::remove(path.c_str());
		std::remove(wal.c_str());
	}
};

// Build one multi-row INSERT (a single VALUES list) instead of n per-row statements. The tuple
// values are identical to the old per-row generation, so the captured delta is unchanged; this just
// collapses n con.Query() calls into one, which matters a lot at scale. The VALUES/constant path in
// the delta-capture rule handles the multi-row list.
static vector<string> GenerateInserts(const string &table, int n, int scale, int64_t pk_offset) {
	vector<string> tuples;
	tuples.reserve(n);
	for (int i = 0; i < n; i++) {
		int64_t pk = kPkBase + pk_offset + i;
		int w = 1 + (i % std::max(scale, 1));
		int d = 1 + (i % 10);
		int c = 1 + (i % 30);
		if (table == "CUSTOMER") {
			tuples.push_back("(" + to_string(w) + ", " + to_string(d) + ", " + to_string(pk) +
			                 ", 0.05, 'GC', 'Last" + to_string(pk) + "', 'First" + to_string(pk) + "', 50000.00, " +
			                 to_string(100 + (i % 500)) +
			                 ".00, 0.0, 0, 0, 'S1', 'S2', 'City', 'ST', '12345', '1234567890', NOW(), 'M', 'data')");
		} else if (table == "WAREHOUSE") {
			tuples.push_back("(" + to_string(pk) + ", 0.00, 0.05, 'W', 'S1', 'S2', 'City', 'ST', '123456789')");
		} else if (table == "DISTRICT") {
			tuples.push_back("(" + to_string(w) + ", " + to_string(pk) +
			                 ", 0.00, 0.05, 1, 'D', 'S1', 'S2', 'City', 'ST', '123456789')");
		} else if (table == "OORDER") {
			tuples.push_back("(" + to_string(w) + ", " + to_string(d) + ", " + to_string(pk) + ", " + to_string(c) +
			                 ", NULL, 5, 1, NOW())");
		} else if (table == "ORDER_LINE") {
			tuples.push_back("(" + to_string(w) + ", " + to_string(d) + ", " + to_string(pk) + ", 1, " +
			                 to_string(1 + (i % 100)) + ", NULL, " + to_string(10 + (i % 400)) + ".00, " + to_string(w) +
			                 ", 5.00, 'D')");
		} else if (table == "cm_dist_src" || table == "cm_dist_aux_src") {
			tuples.push_back("(" + to_string(1 + (i % 20)) + ", 'm" + to_string(pk) + "', " + to_string(1 + (i % 64)) +
			                 ")");
		} else if (table == "cm_saj_r") {
			tuples.push_back("(" + to_string(10 + (i % 1000)) + ")");
		} else if (table == "cm_win_src") {
			tuples.push_back("(" + to_string(pk) + ", " + to_string(1 + (i % 20)) + ", " + to_string(i % 1000) + ")");
		} else if (table == "cm_asof_prices") {
			tuples.push_back("('A', TIMESTAMP '2024-01-02 00:00:00' + INTERVAL '" + to_string(i) + " minutes', " +
			                 to_string(100 + (i % 500)) + ")");
		} else if (table == "ed_a") {
			tuples.push_back("(" + to_string(pk) + ", " + to_string(pk * 10) + ")");
		} else if (table == "ed_b") {
			tuples.push_back("(" + to_string(pk) + ", 'b_" + to_string(pk) + "')");
		} else if (table == "ed_c") {
			tuples.push_back("(" + to_string(pk) + ", 'c_" + to_string(pk) + "')");
		}
	}
	if (tuples.empty()) {
		return {};
	}
	string values;
	for (size_t i = 0; i < tuples.size(); i++) {
		if (i > 0) {
			values += ", ";
		}
		values += tuples[i];
	}
	return {"INSERT INTO " + table + " VALUES " + values};
}

// Pick live rows each cycle. Fixed original keys eventually disappear under repeated deletes.
// Updates and deletes use the same ordering so a mixed batch modifies overlapping rows before refresh.
static string LiveRowIds(const string &table, int n) {
	return "SELECT rowid FROM " + table + " ORDER BY rowid LIMIT " + to_string(n);
}

static string UpdateExpression(const string &table) {
	if (table == "CUSTOMER") {
		return "C_BALANCE = -C_BALANCE + 1";
	} else if (table == "WAREHOUSE") {
		return "W_YTD = W_YTD + 1";
	} else if (table == "DISTRICT") {
		return "D_YTD = D_YTD + 1";
	} else if (table == "OORDER") {
		return "O_CARRIER_ID = COALESCE(O_CARRIER_ID, 0) + 1";
	} else if (table == "ORDER_LINE") {
		return "OL_AMOUNT = OL_AMOUNT + 1";
	} else if (table == "cm_dist_src" || table == "cm_dist_aux_src") {
		return "cores = cores + 1";
	} else if (table == "cm_win_src") {
		return "val = val + 1";
	} else if (table == "cm_asof_prices") {
		return "price = price + 1";
	}
	return "";
}

static vector<string> GenerateUpdates(const string &table, int n) {
	auto expression = UpdateExpression(table);
	if (n <= 0 || expression.empty()) {
		return {};
	}
	return {"UPDATE " + table + " SET " + expression + " WHERE rowid IN (" + LiveRowIds(table, n) + ")"};
}

static vector<string> GenerateExistingDeletes(const string &table, int n) {
	if (n <= 0) {
		return {};
	}
	return {"DELETE FROM " + table + " WHERE rowid IN (" + LiveRowIds(table, n) + ")"};
}

static vector<string> BuildWorkload(const string &table, int size, int scale, Workload wl, int64_t pk_offset) {
	if (wl == Workload::EMPTY_DELTA || size <= 0) {
		return {};
	}
	if (wl == Workload::INSERT_ONLY) {
		return GenerateInserts(table, size, scale, pk_offset);
	}

	int n_ins = std::max(1, size / 2);
	int n_upd = std::max(1, (size * 30) / 100);
	int n_del = size - n_ins - n_upd;
	if (UpdateExpression(table).empty()) {
		n_del += n_upd;
		n_upd = 0;
	}
	auto out = GenerateInserts(table, n_ins, scale, pk_offset);
	auto updates = GenerateUpdates(table, n_upd);
	out.insert(out.end(), updates.begin(), updates.end());
	auto deletes = GenerateExistingDeletes(table, n_del);
	out.insert(out.end(), deletes.begin(), deletes.end());
	return out;
}

static void AddQuery(vector<QueryDef> &qs, QueryDef q) {
	qs.push_back(std::move(q));
}

static vector<QueryDef> BuildQueries() {
	vector<QueryDef> qs;

	AddQuery(qs, {"Q01",
	              "SUM+GROUP BY",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY "
	               "C_W_ID"},
	              {"mv_q"},
	              {"CUSTOMER"},
	              "SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY C_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"Q02",
	              "MIN+MAX GROUP BY",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, COUNT(*) AS n, MIN(C_BALANCE) AS mn, "
	               "MAX(C_BALANCE) AS mx FROM CUSTOMER GROUP BY C_W_ID"},
	              {"mv_q"},
	              {"CUSTOMER"},
	              "SELECT C_W_ID, COUNT(*) AS n, MIN(C_BALANCE) AS mn, MAX(C_BALANCE) AS mx FROM CUSTOMER GROUP BY "
	              "C_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"Q03",
	              "GROUP BY + HAVING",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID "
	               "HAVING COUNT(*) > 5"},
	              {"mv_q"},
	              {"CUSTOMER"},
	              "SELECT C_W_ID, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID HAVING COUNT(*) > 5",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"Q04",
	              "projection + filter",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE "
	               "C_BALANCE > 0"},
	              {"mv_q"},
	              {"CUSTOMER"},
	              "SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > 0",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"Q05",
	              "2-way INNER JOIN",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, d.D_ID, d.D_NAME FROM WAREHOUSE w JOIN "
	               "DISTRICT d ON w.W_ID = d.D_W_ID"},
	              {"mv_q"},
	              {"WAREHOUSE", "DISTRICT"},
	              "SELECT w.W_ID, d.D_ID, d.D_NAME FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"Q06",
	              "3-way INNER JOIN",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, d.D_ID, c.C_ID FROM WAREHOUSE w JOIN "
	               "DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID"},
	              {"mv_q"},
	              {"WAREHOUSE", "DISTRICT", "CUSTOMER"},
	              "SELECT w.W_ID, d.D_ID, c.C_ID FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN "
	              "CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"Q07",
	              "LEFT JOIN + COUNT",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, COUNT(d.D_ID) AS nd FROM WAREHOUSE w LEFT "
	               "JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID"},
	              {"mv_q"},
	              {"WAREHOUSE", "DISTRICT"},
	              "SELECT w.W_ID, COUNT(d.D_ID) AS nd FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID "
	              "GROUP BY w.W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"Q08",
	              "JOIN + GROUP BY + SUM",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT o.O_W_ID, SUM(ol.OL_AMOUNT) AS tot FROM OORDER o "
	               "JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = "
	               "ol.OL_O_ID GROUP BY o.O_W_ID"},
	              {"mv_q"},
	              {"OORDER", "ORDER_LINE"},
	              "SELECT o.O_W_ID, SUM(ol.OL_AMOUNT) AS tot FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = "
	              "ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID GROUP BY o.O_W_ID",
	              {Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"P1",
	              "cascade pipeline",
	              {},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_p1_a AS SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) AS s FROM CUSTOMER "
	               "GROUP BY C_W_ID, C_D_ID",
	               "CREATE MATERIALIZED VIEW mv_p1_b AS SELECT C_W_ID, SUM(s) AS s_tot FROM mv_p1_a GROUP BY C_W_ID",
	               "CREATE MATERIALIZED VIEW mv_p1_c AS SELECT SUM(s_tot) AS g FROM mv_p1_b"},
	              {"mv_p1_a", "mv_p1_b", "mv_p1_c"},
	              {"CUSTOMER"},
	              "SELECT SUM(s_tot) AS g FROM (SELECT C_W_ID, SUM(s) AS s_tot FROM (SELECT C_W_ID, C_D_ID, "
	              "SUM(C_BALANCE) AS s FROM CUSTOMER GROUP BY C_W_ID, C_D_ID) a GROUP BY C_W_ID) b",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});

	AddQuery(qs, {"S01",
	              "DISTINCT group recompute",
	              {"CREATE TABLE cm_dist_src (grp INT, machine VARCHAR, cores INT)",
	               "INSERT INTO cm_dist_src VALUES (1, 'm1', 8), (1, 'm1', 8), (1, 'm2', 16), (2, 'm3', 4)"},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT grp, SUM(cores) AS total_cores FROM (SELECT DISTINCT "
	               "grp, machine, cores FROM cm_dist_src WHERE machine IS NOT NULL) d GROUP BY grp"},
	              {"mv_q"},
	              {"cm_dist_src"},
	              "SELECT grp, SUM(cores) AS total_cores FROM (SELECT DISTINCT grp, machine, cores FROM cm_dist_src "
	              "WHERE machine IS NOT NULL) d GROUP BY grp",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"S02",
	              "DISTINCT aux incremental",
	              {"CREATE TABLE cm_dist_aux_src (grp INT, machine VARCHAR, cores INT)",
	               "INSERT INTO cm_dist_aux_src VALUES (1, 'm1', 8), (1, 'm1', 8), (1, 'm2', 16), (2, 'm3', 4)"},
	              {"SET openivm_distinct_aux_state = true"},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT grp, SUM(cores) AS total_cores FROM (SELECT DISTINCT "
	               "grp, machine, cores FROM cm_dist_aux_src WHERE machine IS NOT NULL) d GROUP BY grp"},
	              {"mv_q"},
	              {"cm_dist_aux_src"},
	              "SELECT grp, SUM(cores) AS total_cores FROM (SELECT DISTINCT grp, machine, cores FROM "
	              "cm_dist_aux_src WHERE machine IS NOT NULL) d GROUP BY grp",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"S03",
	              "SEMI JOIN recompute",
	              {"CREATE TABLE cm_saj_l (id INT, x INT)", "CREATE TABLE cm_saj_r (y INT)",
	               "INSERT INTO cm_saj_l SELECT i, i * 10 FROM range(1, 101) t(i)",
	               "INSERT INTO cm_saj_r SELECT i * 10 + 1 FROM range(1, 51) t(i)"},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT l.id, l.x FROM cm_saj_l l SEMI JOIN cm_saj_r r ON "
	               "abs(l.x - r.y) <= 1"},
	              {"mv_q"},
	              {"cm_saj_r"},
	              "SELECT l.id, l.x FROM cm_saj_l l SEMI JOIN cm_saj_r r ON abs(l.x - r.y) <= 1",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"S04",
	              "window partition",
	              {"CREATE TABLE cm_win_src (id INT, grp INT, val INT)",
	               "INSERT INTO cm_win_src SELECT i, i % 20, i FROM range(1, 501) t(i)"},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp "
	               "ORDER BY val, id) AS rn FROM cm_win_src"},
	              {"mv_q"},
	              {"cm_win_src"},
	              "SELECT id, grp, val, ROW_NUMBER() OVER (PARTITION BY grp ORDER BY val, id) AS rn FROM cm_win_src",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"S05",
	              "ASOF current-diff recompute",
	              {"CREATE TABLE cm_asof_trades (symbol VARCHAR, ts TIMESTAMP, qty INT)",
	               "CREATE TABLE cm_asof_prices (symbol VARCHAR, ts TIMESTAMP, price INT)",
	               "INSERT INTO cm_asof_trades SELECT 'A', TIMESTAMP '2024-01-01 10:00:00' + i * INTERVAL '1 "
	               "minute', 1 FROM range(0, 200) t(i)",
	               "INSERT INTO cm_asof_prices SELECT 'A', TIMESTAMP '2024-01-01 09:00:00' + i * INTERVAL '1 "
	               "minute', 100 + i FROM range(0, 200) t(i)"},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT t.symbol, t.ts AS trade_ts, t.qty, p.ts AS price_ts, "
	               "p.price FROM cm_asof_trades t ASOF JOIN cm_asof_prices p ON t.symbol = p.symbol AND t.ts >= p.ts"},
	              {"mv_q"},
	              {"cm_asof_prices"},
	              "SELECT t.symbol, t.ts AS trade_ts, t.qty, p.ts AS price_ts, p.price FROM cm_asof_trades t ASOF "
	              "JOIN cm_asof_prices p ON t.symbol = p.symbol AND t.ts >= p.ts",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}});
	AddQuery(qs, {"S06",
	              "3-way join empty-delta skip",
	              {"CREATE TABLE ed_a (id INT PRIMARY KEY, val INT)",
	               "CREATE TABLE ed_b (id INT PRIMARY KEY, label VARCHAR)",
	               "CREATE TABLE ed_c (id INT PRIMARY KEY, tag VARCHAR)",
	               "INSERT INTO ed_a SELECT i, i * 10 FROM range(1, 501) t(i)",
	               "INSERT INTO ed_b SELECT i, 'b_' || i::VARCHAR FROM range(1, 501) t(i)",
	               "INSERT INTO ed_c SELECT i, 'c_' || i::VARCHAR FROM range(1, 501) t(i)"},
	              {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT a.val, b.label, c.tag FROM ed_a a JOIN ed_b b ON "
	               "a.id = b.id JOIN ed_c c ON a.id = c.id"},
	              {"mv_q"},
	              {"ed_a"},
	              "SELECT a.val, b.label, c.tag FROM ed_a a JOIN ed_b b ON a.id = b.id JOIN ed_c c ON a.id = c.id",
	              {Workload::INSERT_ONLY, Workload::EMPTY_DELTA}});

	// ----- TODO batch: operators/shapes still being brought under test -----
	// AVG/STDDEV use DOUBLE to avoid the documented AVG(DECIMAL) ULP drift that breaks strict EXCEPT ALL.
	// JOIN + AVG: AVG decomposition (SUM/COUNT) maintained over a 2-way join, on a varying measure
	// (OL_AMOUNT), not a constant column.
	AddQuery(qs, {"T01", "JOIN + AVG", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT o.O_W_ID, AVG(ol.OL_AMOUNT::DOUBLE) AS avg_amt FROM OORDER o "
	               "JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID "
	               "GROUP BY o.O_W_ID"},
	              {"mv_q"}, {"OORDER", "ORDER_LINE"},
	              "SELECT o.O_W_ID, AVG(ol.OL_AMOUNT::DOUBLE) AS avg_amt FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = "
	              "ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID GROUP BY o.O_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// ROUND the STDDEV: its SUM/COUNT/sum-of-squares decomposition drifts ~1e-8 from DuckDB's native
	// STDDEV (catastrophic cancellation), which strict EXCEPT ALL would otherwise flag (docs/limitations.md).
	AddQuery(qs, {"T02", "STDDEV aggregate", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, ROUND(STDDEV_SAMP(C_BALANCE::DOUBLE), 2) AS sd FROM "
	               "CUSTOMER GROUP BY C_W_ID"},
	              {"mv_q"}, {"CUSTOMER"},
	              "SELECT C_W_ID, ROUND(STDDEV_SAMP(C_BALANCE::DOUBLE), 2) AS sd FROM CUSTOMER GROUP BY C_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// ARG_MAX orders by (C_BALANCE, C_D_ID, C_ID) rather than C_BALANCE alone. TPC-C gives every
	// customer the same starting balance, so ordering by it alone leaves tens of thousands of tied
	// rows per warehouse and ARG_MAX may return any of them: at scale factor 25 the maximum is tied
	// across 29,700 rows in a single group. Both the view and the base query then return correct but
	// different answers, and the EXCEPT ALL cross-check reports a mismatch that is not one. Appending
	// (C_D_ID, C_ID), which is unique within a warehouse, makes the ordering total and the query
	// single-valued, while still ordering primarily by the column the mixed workload updates.
	AddQuery(qs, {"T03", "ARG_MAX aggregate", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, ARG_MAX(C_ID, (C_BALANCE, C_D_ID, C_ID)) AS top_c "
	               "FROM CUSTOMER GROUP BY C_W_ID"},
	              {"mv_q"}, {"CUSTOMER"},
	              "SELECT C_W_ID, ARG_MAX(C_ID, (C_BALANCE, C_D_ID, C_ID)) AS top_c FROM CUSTOMER GROUP BY C_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	AddQuery(qs, {"T04", "FULL OUTER JOIN projection", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, d.D_ID FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d "
	               "ON w.W_ID = d.D_W_ID"},
	              {"mv_q"}, {"WAREHOUSE", "DISTRICT"},
	              "SELECT w.W_ID, d.D_ID FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d ON w.W_ID = d.D_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	AddQuery(qs, {"T05", "FULL OUTER JOIN aggregate", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, COUNT(d.D_ID) AS nd FROM WAREHOUSE w FULL OUTER JOIN "
	               "DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID"},
	              {"mv_q"}, {"WAREHOUSE", "DISTRICT"},
	              "SELECT w.W_ID, COUNT(d.D_ID) AS nd FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d ON w.W_ID = d.D_W_ID "
	              "GROUP BY w.W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// UNION ALL of two grouped aggregates: each arm maintained incrementally, then unioned.
	AddQuery(qs, {"T06", "UNION ALL of aggregates", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID AS k, SUM(C_BALANCE) AS v FROM CUSTOMER GROUP BY "
	               "C_W_ID UNION ALL SELECT D_W_ID, SUM(D_YTD) FROM DISTRICT GROUP BY D_W_ID"},
	              {"mv_q"}, {"CUSTOMER", "DISTRICT"},
	              "SELECT C_W_ID AS k, SUM(C_BALANCE) AS v FROM CUSTOMER GROUP BY C_W_ID UNION ALL SELECT D_W_ID, "
	              "SUM(D_YTD) FROM DISTRICT GROUP BY D_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	AddQuery(qs, {"T07", "top-level DISTINCT", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT DISTINCT C_W_ID, C_D_ID FROM CUSTOMER"},
	              {"mv_q"}, {"CUSTOMER"}, "SELECT DISTINCT C_W_ID, C_D_ID FROM CUSTOMER",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	AddQuery(qs, {"T08", "ORDER BY + LIMIT (top-k)", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, C_D_ID, C_ID FROM CUSTOMER ORDER BY C_W_ID, C_D_ID, "
	               "C_ID LIMIT 100"},
	              {"mv_q"}, {"CUSTOMER"},
	              "SELECT C_W_ID, C_D_ID, C_ID FROM CUSTOMER ORDER BY C_W_ID, C_D_ID, C_ID LIMIT 100",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// 3-way join + GROUP BY + HAVING: capped at 3 tables (2^3-1 = 7 incl-excl terms; a 4-way's 15 is
	// excessive) but layers an aggregate and a HAVING predicate on top of the join.
	AddQuery(qs, {"T09", "3-way JOIN + GROUP BY + HAVING", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, COUNT(c.C_ID) AS nc FROM WAREHOUSE w JOIN DISTRICT d "
	               "ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY w.W_ID "
	               "HAVING COUNT(c.C_ID) > 100"},
	              {"mv_q"}, {"WAREHOUSE", "DISTRICT", "CUSTOMER"},
	              "SELECT w.W_ID, COUNT(c.C_ID) AS nc FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN "
	              "CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY w.W_ID HAVING COUNT(c.C_ID) > 100",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// JOIN + FILTER aggregate: a FILTER-clause aggregate (normalized at create time) layered over a
	// 2-way join, on the varying OL_AMOUNT measure.
	AddQuery(qs, {"T10", "JOIN + FILTER aggregate", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT o.O_W_ID, SUM(ol.OL_AMOUNT::DOUBLE) FILTER (WHERE "
	               "ol.OL_AMOUNT > 30) AS hi FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = "
	               "ol.OL_D_ID AND o.O_ID = ol.OL_O_ID GROUP BY o.O_W_ID"},
	              {"mv_q"}, {"OORDER", "ORDER_LINE"},
	              "SELECT o.O_W_ID, SUM(ol.OL_AMOUNT::DOUBLE) FILTER (WHERE ol.OL_AMOUNT > 30) AS hi FROM OORDER o JOIN "
	              "ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID GROUP BY "
	              "o.O_W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// Window aggregate over a 2-way join result.
	AddQuery(qs, {"T11", "window SUM over JOIN", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT o.O_W_ID, o.O_D_ID, o.O_ID, SUM(ol.OL_AMOUNT::DOUBLE) OVER "
	               "(PARTITION BY o.O_W_ID) AS wsum FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND "
	               "o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID"},
	              {"mv_q"}, {"OORDER", "ORDER_LINE"},
	              "SELECT o.O_W_ID, o.O_D_ID, o.O_ID, SUM(ol.OL_AMOUNT::DOUBLE) OVER (PARTITION BY o.O_W_ID) AS wsum "
	              "FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = "
	              "ol.OL_O_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// CTE that aggregates, then joins the result to another table.
	AddQuery(qs, {"T12", "CTE aggregate + JOIN", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS WITH wagg AS (SELECT C_W_ID, SUM(C_BALANCE) AS s FROM CUSTOMER "
	               "GROUP BY C_W_ID) SELECT a.C_W_ID, a.s, w.W_NAME FROM wagg a JOIN WAREHOUSE w ON a.C_W_ID = w.W_ID"},
	              {"mv_q"}, {"CUSTOMER", "WAREHOUSE"},
	              "WITH wagg AS (SELECT C_W_ID, SUM(C_BALANCE) AS s FROM CUSTOMER GROUP BY C_W_ID) SELECT a.C_W_ID, a.s, "
	              "w.W_NAME FROM wagg a JOIN WAREHOUSE w ON a.C_W_ID = w.W_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// Multiple aggregates (SUM/COUNT/MAX) over a 2-way join with a multi-column group key.
	AddQuery(qs, {"T13", "JOIN + multi-aggregate", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT o.O_W_ID, o.O_D_ID, SUM(ol.OL_AMOUNT::DOUBLE) AS tot, "
	               "COUNT(*) AS cnt, MAX(ol.OL_AMOUNT) AS mx FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID "
	               "AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID GROUP BY o.O_W_ID, o.O_D_ID"},
	              {"mv_q"}, {"OORDER", "ORDER_LINE"},
	              "SELECT o.O_W_ID, o.O_D_ID, SUM(ol.OL_AMOUNT::DOUBLE) AS tot, COUNT(*) AS cnt, MAX(ol.OL_AMOUNT) AS mx "
	              "FROM OORDER o JOIN ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = "
	              "ol.OL_O_ID GROUP BY o.O_W_ID, o.O_D_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});
	// Large-table 2-way join PROJECTION (no aggregate): directly exercises the join-cost estimate — a
	// small delta on the big ORDER_LINE join should be priced for incremental, not a full recompute.
	AddQuery(qs, {"T14", "large JOIN projection", {}, {},
	              {"CREATE MATERIALIZED VIEW mv_q AS SELECT ol.OL_W_ID, ol.OL_O_ID, ol.OL_NUMBER, ol.OL_AMOUNT, o.O_C_ID "
	               "FROM ORDER_LINE ol JOIN OORDER o ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = "
	               "ol.OL_O_ID"},
	              {"mv_q"}, {"ORDER_LINE", "OORDER"},
	              "SELECT ol.OL_W_ID, ol.OL_O_ID, ol.OL_NUMBER, ol.OL_AMOUNT, o.O_C_ID FROM ORDER_LINE ol JOIN OORDER o "
	              "ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID",
	              {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, Batch::TODO});

	return qs;
}

static void ApplyFlagConfig(duckdb::Connection &con, FlagConfig config) {
	if (config == FlagConfig::ALL_OFF) {
		for (const char *flag : {"openivm_skip_empty_deltas", "openivm_fk_pruning", "openivm_skip_aggregate_delete",
		                         "openivm_skip_projection_delete", "openivm_minmax_incremental", "openivm_having_merge",
		                         "openivm_left_join_merge", "openivm_full_outer_merge", "openivm_ducklake_nterm",
		                         "openivm_distinct_aux_state"}) {
			con.Query(string("SET ") + flag + " = false");
		}
	} else if (config == FlagConfig::SKIP_EMPTY_OFF) {
		con.Query("SET openivm_skip_empty_deltas = false");
	}
}

static int64_t CountRows(duckdb::Connection &con, const string &table) {
	auto result = con.Query("SELECT COUNT(*) FROM " + table);
	if (!result || result->HasError() || result->RowCount() == 0) {
		return 0;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

static vector<int> AllocateDeltas(duckdb::Connection &con, const vector<string> &tables, double pct) {
	vector<int64_t> counts;
	int64_t total_rows = 0;
	for (auto &table : tables) {
		auto count = CountRows(con, table);
		counts.push_back(count);
		total_rows += count;
	}
	vector<int> allocated(tables.size(), 0);
	if (pct <= 0 || total_rows <= 0 || tables.empty()) {
		return allocated;
	}
	int total_delta = static_cast<int>(std::llround(static_cast<double>(total_rows) * pct / 100.0));
	total_delta = std::max(total_delta, 1);
	int assigned = 0;
	for (idx_t i = 0; i < tables.size(); i++) {
		allocated[i] = static_cast<int>(std::floor(static_cast<double>(total_delta) * counts[i] / total_rows));
		assigned += allocated[i];
	}
	for (idx_t i = 0; assigned < total_delta && i < tables.size(); i++, assigned++) {
		allocated[i]++;
	}
	return allocated;
}

// `pk_cursor` is an in/out watermark over the synthetic primary-key space, advanced past whatever
// this call inserts so a later cycle on the same database never reuses a key.
//
// A watermark rather than cycle * constant: the key columns are 32-bit, and a stride wide enough for
// a large cycle exhausts that range within a few dozen cycles. At 1e8 per cycle the keys passed
// INT32_MAX at cycle 22, every insert failed, and the mixed workload's deletes then drained the
// table to empty while still reporting success. Tracking actual usage spends only what is needed.
static int64_t ApplyDML(duckdb::Connection &con, const QueryDef &q, Workload workload, double delta_pct, int scale,
                        int64_t &pk_cursor) {
	if (workload == Workload::EMPTY_DELTA || delta_pct <= 0.0) {
		return 0;
	}
	auto allocations = AllocateDeltas(con, q.touched_tables, delta_pct);
	int64_t issued = 0;
	int64_t pk_offset = pk_cursor;
	for (idx_t i = 0; i < q.touched_tables.size(); i++) {
		// At least one insert, update (where supported), and delete keeps tiny or empty
		// tables participating in repeated mixed batches. The actual delta count is recorded.
		int allocation = workload == Workload::MIXED ? std::max(3, allocations[i]) : allocations[i];
		auto dml = BuildWorkload(q.touched_tables[i], allocation, scale, workload, pk_offset);
		for (auto &sql : dml) {
			auto result = con.Query(sql);
			if (!result || result->HasError()) {
				throw std::runtime_error("DML on " + q.touched_tables[i] + ": " +
				                         (result ? result->GetError() : "null result"));
			}
			issued++;
		}
		pk_offset += allocation + 1000;
	}
	pk_cursor = pk_offset;
	return issued;
}

static string DeltaTableName(const string &table) {
	string name = table;
	auto dot = name.find('.');
	if (dot != string::npos) {
		name = name.substr(dot + 1);
	}
	for (auto &ch : name) {
		ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
	}
	return "openivm_delta_" + name;
}

static int64_t CountPendingDeltaRows(duckdb::Connection &con, const QueryDef &q) {
	int64_t rows = 0;
	for (auto &table : q.touched_tables) {
		rows += CountRows(con, DeltaTableName(table));
	}
	return rows;
}

static void WarmScenario(duckdb::Connection &con, const QueryDef &q) {
	for (auto &table : q.touched_tables) {
		con.Query("SELECT COUNT(*) FROM " + table);
		con.Query("SELECT COUNT(*) FROM " + DeltaTableName(table));
	}
	for (auto &mv : q.refresh_mvs) {
		con.Query("SELECT COUNT(*) FROM " + mv);
	}
	con.Query("SELECT COUNT(*) FROM (" + q.base_sql + ") warm_base");
}

static int64_t ReadCount(duckdb::Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (!result || result->HasError() || result->RowCount() == 0 || result->GetValue(0, 0).IsNull()) {
		return 0;
	}
	return result->GetValue(0, 0).GetValue<int64_t>();
}

static int64_t ReadBaseRows(duckdb::Connection &con, const QueryDef &q) {
	int64_t rows = 0;
	for (auto &table : q.touched_tables) {
		rows += ReadCount(con, "SELECT COUNT(*) FROM " + table);
	}
	return rows;
}

static CostEstimate ReadCost(duckdb::Connection &con, const string &view_name) {
	CostEstimate cost;
	auto result = con.Query("PRAGMA refresh_cost('" + view_name + "')");
	if (!result || result->HasError() || result->RowCount() == 0) {
		cost.error = result ? result->GetError() : "null result";
		return cost;
	}
	cost.decision = result->GetValue(0, 0).ToString();
	cost.incremental_cost = result->GetValue(1, 0).GetValue<double>();
	cost.recompute_cost = result->GetValue(2, 0).GetValue<double>();
	cost.incremental_predicted_ms = result->GetValue(3, 0).GetValue<double>();
	cost.recompute_predicted_ms = result->GetValue(4, 0).GetValue<double>();
	cost.calibrated = result->GetValue(5, 0).GetValue<bool>();
	cost.ok = true;
	return cost;
}

static CostEstimate CombinePipelineCosts(const vector<CostEstimate> &costs) {
	if (costs.size() == 1) {
		return costs[0];
	}
	CostEstimate total;
	total.ok = true;
	for (auto &cost : costs) {
		if (!cost.ok) {
			return cost;
		}
		total.incremental_cost += cost.incremental_cost;
		total.recompute_cost += cost.recompute_cost;
		total.incremental_predicted_ms += cost.incremental_predicted_ms;
		total.recompute_predicted_ms += cost.recompute_predicted_ms;
		total.calibrated = total.calibrated || cost.calibrated;
	}
	total.decision = total.recompute_predicted_ms < total.incremental_predicted_ms ? "full" : "incremental";
	return total;
}

static string CombinePipelineMethods(const vector<string> &methods) {
	bool saw_incremental = false;
	for (auto &method : methods) {
		if (method == "full" || method == "forced_full") {
			return "full";
		}
		if (method != "skipped_or_unrecorded") {
			saw_incremental = true;
		}
	}
	return saw_incremental ? "incremental" : "skipped_or_unrecorded";
}

static bool ValidateMV(duckdb::Connection &con, const string &mv_name, const string &base_sql, string &error) {
	// Parenthesize both operands so a base query ending in ORDER BY/LIMIT (top-k views) is a valid
	// set-operation operand instead of a syntax error.
	string left = "SELECT COUNT(*) FROM ((SELECT * FROM " + mv_name + ") EXCEPT ALL (" + base_sql + ")) x";
	auto l = con.Query(left);
	if (!l || l->HasError()) {
		error = l ? l->GetError() : "left validation returned null";
		return false;
	}
	string right = "SELECT COUNT(*) FROM ((" + base_sql + ") EXCEPT ALL (SELECT * FROM " + mv_name + ")) x";
	auto r = con.Query(right);
	if (!r || r->HasError()) {
		error = r ? r->GetError() : "right validation returned null";
		return false;
	}
	auto missing = l->GetValue(0, 0).GetValue<int64_t>();
	auto extra = r->GetValue(0, 0).GetValue<int64_t>();
	if (missing != 0 || extra != 0) {
		error = "validation mismatch: mv_minus_base=" + to_string(missing) + ", base_minus_mv=" + to_string(extra);
		return false;
	}
	return true;
}

static string LastHistoryMethod(duckdb::Connection &con, const string &view_name) {
	auto result = con.Query("SELECT method FROM openivm_refresh_history WHERE view_name = '" + view_name +
	                        "' ORDER BY refresh_timestamp DESC LIMIT 1");
	if (!result || result->HasError() || result->RowCount() == 0) {
		return "skipped_or_unrecorded";
	}
	return result->GetValue(0, 0).ToString();
}

static void ConfigureMode(duckdb::Connection &con, RefreshMode mode) {
	if (mode == RefreshMode::AUTO) {
		con.Query("SET openivm_refresh_mode = 'auto'");
		con.Query("SET openivm_adaptive_refresh = true");
	} else if (mode == RefreshMode::INCREMENTAL) {
		con.Query("SET openivm_refresh_mode = 'incremental'");
		con.Query("SET openivm_adaptive_refresh = false");
	} else {
		con.Query("SET openivm_refresh_mode = 'full'");
		con.Query("SET openivm_adaptive_refresh = false");
	}
}

// A case's starting state, built once and reused by every refresh mode.
//
// The three modes have to begin from identical state, but they used to reach it by each repeating
// the whole setup: copy the base database, create the materialized view, apply the delta. That work
// dwarfs the refresh it surrounds — at scale factor 50 the timed refresh was 0.4% of the sweep's
// wall clock — and doing it three times tripled the part that was already dominant.
//
// It also did not produce identical state. The generated inserts embed NOW(), so each mode wrote
// different timestamps and the three were not measuring the same delta.
//
// Building it once and restoring from a file copy fixes both. The copy is cheap next to what it
// replaces: 28 MB at scale factor 25 copies in 0.02s, against seconds to minutes for the setup.
// The case's live database, carried across refresh cycles.
//
// A cost model that learns from execution history cannot be measured on a database that has none.
// Every case used to be a fresh copy refreshed exactly once, so history was empty by construction
// and the learned weights could never reach their sample threshold: the sweep only ever exercised
// the uncalibrated fallback. A case is now one database refreshed many times, with the accepted
// result carried forward, so the model sees the regime it is meant to operate in.
struct PreparedCase {
	std::unique_ptr<TempDb> state;
	string case_tag;         // shared by every file of this case, so they share a catalog name
	int64_t pk_cursor = 0;   // advances across cycles so no two reuse a key
	bool ok = false;
	string error;
	int64_t dml_statements = 0;
	int64_t delta_rows = 0;
	int64_t base_rows = 0;
	int64_t mv_rows = 0;
};

static PreparedCase PrepareCase(const string &src_db_path, const QueryDef &q, Workload workload,
                                FlagConfig flag_config, int rep) {
	PreparedCase prepared;
	prepared.case_tag =
	    q.id + "_" + WorkloadName(workload) + "_" + FlagConfigName(flag_config) + "_" + to_string(rep);
	prepared.state.reset(new TempDb(prepared.case_tag, "state"));
	if (!CopyFile(src_db_path, prepared.state->path)) {
		prepared.error = "copy db failed: " + string(strerror(errno));
		return prepared;
	}
	try {
		// Scoped so the database is closed, and therefore checkpointed, before anything copies the
		// file. A snapshot taken while it is open could miss writes still sitting in the WAL.
		duckdb::DuckDB db(prepared.state->path);
		duckdb::Connection con(db);
		auto load = con.Query("LOAD openivm");
		if (!load || load->HasError()) {
			prepared.error = "LOAD openivm: " + (load ? load->GetError() : "null result");
			return prepared;
		}
		ApplyFlagConfig(con, flag_config);
		for (auto &sql : q.setup_sql) {
			auto result = con.Query(sql);
			if (!result || result->HasError()) {
				prepared.error = "setup failed: " + (result ? result->GetError() : "null result");
				return prepared;
			}
		}
		for (auto &sql : q.query_settings) {
			auto result = con.Query(sql);
			if (!result || result->HasError()) {
				prepared.error = "query setting failed: " + (result ? result->GetError() : "null result");
				return prepared;
			}
		}
		for (auto &sql : q.create_mvs) {
			auto result = con.Query(sql);
			if (!result || result->HasError()) {
				prepared.error = "CREATE MV failed: " + (result ? result->GetError() : "null result");
				return prepared;
			}
		}
		prepared.base_rows = ReadBaseRows(con, q);
		prepared.mv_rows = ReadCount(con, "SELECT COUNT(*) FROM " + q.refresh_mvs.back());
	} catch (const std::exception &e) {
		prepared.error = string("exception during setup: ") + e.what();
		return prepared;
	}
	prepared.ok = true;
	return prepared;
}

// Apply one cycle's delta to the live state. Returns false with `error` set on failure.
//
// The cycle index offsets the synthetic key space so no cycle reuses a key an earlier one inserted,
// and it rotates the delta percentage so the recorded history covers a range of change sizes rather
// than repeating one. A model fitted on a single delta size would have nothing to generalise from.
static bool ApplyCycleDelta(PreparedCase &prepared, const QueryDef &q, Workload workload, double delta_pct, int scale,
                            string &error) {
	try {
		duckdb::DuckDB db(prepared.state->path);
		duckdb::Connection con(db);
		auto load = con.Query("LOAD openivm");
		if (!load || load->HasError()) {
			error = "LOAD openivm: " + (load ? load->GetError() : "null result");
			return false;
		}
		for (auto &sql : q.query_settings) {
			con.Query(sql);
		}
		prepared.dml_statements = ApplyDML(con, q, workload, delta_pct, scale, prepared.pk_cursor);
		prepared.delta_rows = CountPendingDeltaRows(con, q);
		if (prepared.dml_statements == 0 && delta_pct > 0) {
			error = "delta applied no statements — the synthetic key space may be exhausted";
			return false;
		}
		if (workload == Workload::MIXED && delta_pct > 0 && prepared.delta_rows <= 0) {
			error = "mixed workload produced no pending delta rows";
			return false;
		}
		prepared.base_rows = ReadBaseRows(con, q);
		prepared.mv_rows = ReadCount(con, "SELECT COUNT(*) FROM " + q.refresh_mvs.back());
	} catch (const std::exception &e) {
		error = string("exception applying delta: ") + e.what();
		return false;
	}
	return true;
}

static ModeResult RunMode(const PreparedCase &prepared, const QueryDef &q, Workload workload, double delta_pct,
                          FlagConfig flag_config, int rep, int cycle, RefreshMode mode, bool read_cost, bool warm,
                          bool do_validate, string *promote_path) {
	ModeResult out;
	out.dml_statements = prepared.dml_statements;
	out.delta_rows = prepared.delta_rows;
	out.base_rows = prepared.base_rows;
	out.mv_rows = prepared.mv_rows;
	// Each mode branches from the same state, so the three are comparable, and only the accepted one
	// is carried forward. Retained past this scope when it is the one to promote.
	TempDb temp(prepared.case_tag, "c" + to_string(cycle) + "m" + to_string(static_cast<int>(mode)),
	            promote_path != nullptr);
	if (!CopyFile(prepared.state->path, temp.path)) {
		out.error = "restore snapshot failed: " + string(strerror(errno));
		return out;
	}
	try {
		duckdb::DuckDB db(temp.path);
		duckdb::Connection con(db);
		auto load = con.Query("LOAD openivm");
		if (!load || load->HasError()) {
			out.error = "LOAD openivm: " + (load ? load->GetError() : "null result");
			return out;
		}
		// Settings live in the session, not in the file, so they are re-applied on the restored copy.
		ApplyFlagConfig(con, flag_config);
		for (auto &sql : q.query_settings) {
			auto result = con.Query(sql);
			if (!result || result->HasError()) {
				out.error = "query setting failed: " + (result ? result->GetError() : "null result");
				return out;
			}
		}
		if (warm) {
			WarmScenario(con, q);
		}
		ConfigureMode(con, mode);
		vector<CostEstimate> stage_costs;
		vector<string> stage_methods;
		int64_t t0 = NowMicros();
		for (auto &mv : q.refresh_mvs) {
			if (read_cost) {
				auto stage_cost = ReadCost(con, mv);
				if (!stage_cost.ok) {
					out.error = "refresh_cost failed for " + mv + ": " + stage_cost.error;
					return out;
				}
				stage_costs.push_back(std::move(stage_cost));
			}
			auto refresh_start = NowMicros();
			auto result = con.Query("PRAGMA refresh('" + mv + "')");
			if (!result || result->HasError()) {
				out.error = "PRAGMA refresh failed: " + (result ? result->GetError() : "null result");
				return out;
			}
			if (mode == RefreshMode::AUTO) {
				stage_methods.push_back(LastHistoryMethod(con, mv));
			}
			int64_t refresh_end = NowMicros();
			out.refresh_ms += (refresh_end - refresh_start) / 1000.0;
		}
		int64_t t1 = NowMicros();
		if (!read_cost) {
			out.refresh_ms = (t1 - t0) / 1000.0;
		} else {
			out.cost = CombinePipelineCosts(stage_costs);
		}
		if (mode == RefreshMode::AUTO) {
			out.method = CombinePipelineMethods(stage_methods);
		} else if (mode == RefreshMode::INCREMENTAL) {
			out.method = "forced_incremental";
		} else {
			out.method = "forced_full";
		}
		if (do_validate) {
			string validation_error;
			out.correct = ValidateMV(con, q.refresh_mvs.back(), q.base_sql, validation_error);
			if (!out.correct) {
				out.error = validation_error;
				return out;
			}
		} else {
			out.correct = true; // validation explicitly disabled by --no-validate
		}
		out.ok = true;
		if (promote_path) {
			*promote_path = temp.path;
		}
		return out;
	} catch (const std::exception &e) {
		out.error = string("exception: ") + e.what();
		return out;
	}
}

// Ratio error between a predicted and a measured duration, the standard accuracy measure for cost
// estimates: 1.0 is exact, 2.0 is off by a factor of two in either direction. Returns 0 when either
// side is non-positive, which marks the sample as undefined rather than perfect.
static double QError(double predicted_ms, double actual_ms) {
	if (predicted_ms <= 0 || actual_ms <= 0) {
		return 0;
	}
	return std::max(predicted_ms, actual_ms) / std::min(predicted_ms, actual_ms);
}

static string BestMethod(double incremental_ms, double full_ms) {
	return incremental_ms <= full_ms ? "incremental" : "full";
}

static string MethodClass(const string &method) {
	if (method == "full" || method == "forced_full") {
		return "full";
	}
	return "incremental";
}

static double ChosenPathMs(const string &auto_method, double incremental_ms, double full_ms) {
	return MethodClass(auto_method) == "full" ? full_ms : incremental_ms;
}

static double RegretRatio(const string &auto_method, double incremental_ms, double full_ms) {
	double best_ms = std::min(incremental_ms, full_ms);
	if (best_ms <= 0) {
		return 0;
	}
	return ChosenPathMs(auto_method, incremental_ms, full_ms) / best_ms;
}

static void PrintUsage() {
	fprintf(stderr, "cost_model_benchmark --scale N --db PATH --out CSV [--reps 3]\n"
	                "                     [--delta-pcts 0,0.01,1,2,5,10,20,50] [--filter Q01,S06,...] [--no-warm]\n"
	                "                     [--configs all_on,all_off,skip_empty_off] [--no-validate]\n"
	                "                     [--batch all|validated|todo] [--cycles 10]\n");
}

int main(int argc, char **argv) {
	int scale = 1;
	string db_path;
	string out_csv = "cost_model_benchmark_results.csv";
	int reps = 3;
	// Refreshes per case, all against one database. The cost model learns from execution history, so
	// a case that refreshes once can only ever exercise the uncalibrated path.
	int cycles = 10;
	// Rotated across cycles rather than forming a dimension of the grid: a model fitted on a single
	// delta size has nothing to generalise from. An empty delta is now just a cycle with no change.
	vector<double> delta_pcts = {0.01, 1, 2, 5, 10, 20, 50};
	set<string> query_filter;
	bool warm = true;
	bool validate = true; // EXCEPT ALL correctness cross-check for every refresh mode
	string batch_sel = "all"; // "all" | "validated" | "todo"
	vector<FlagConfig> configs = {FlagConfig::ALL_ON, FlagConfig::ALL_OFF, FlagConfig::SKIP_EMPTY_OFF};

	for (int i = 1; i < argc; i++) {
		string arg = argv[i];
		auto next = [&](const char *name) -> string {
			if (i + 1 >= argc) {
				fprintf(stderr, "%s requires a value\n", name);
				PrintUsage();
				_exit(2);
			}
			return string(argv[++i]);
		};
		if (arg == "--scale") {
			scale = std::stoi(next("--scale"));
		} else if (arg == "--db") {
			db_path = next("--db");
		} else if (arg == "--out") {
			out_csv = next("--out");
		} else if (arg == "--reps") {
			reps = std::stoi(next("--reps"));
		} else if (arg == "--cycles") {
			cycles = std::stoi(next("--cycles"));
			if (cycles < 1) {
				fprintf(stderr, "--cycles must be >= 1\n");
				return 2;
			}
		} else if (arg == "--delta-pcts") {
			delta_pcts = ParseDoubleList(next("--delta-pcts"));
		} else if (arg == "--filter") {
			string f = next("--filter");
			size_t start = 0;
			while (start < f.size()) {
				size_t end = f.find(',', start);
				if (end == string::npos) {
					end = f.size();
				}
				query_filter.insert(f.substr(start, end - start));
				start = end + 1;
			}
		} else if (arg == "--no-warm") {
			warm = false;
		} else if (arg == "--no-validate") {
			validate = false;
		} else if (arg == "--batch") {
			batch_sel = next("--batch");
			if (batch_sel != "all" && batch_sel != "validated" && batch_sel != "todo") {
				fprintf(stderr, "unknown batch: %s (use all|validated|todo)\n", batch_sel.c_str());
				return 2;
			}
		} else if (arg == "--configs") {
			string f = next("--configs");
			configs.clear();
			size_t start = 0;
			while (start < f.size()) {
				size_t end = f.find(',', start);
				if (end == string::npos) {
					end = f.size();
				}
				string name = f.substr(start, end - start);
				if (name == "all_on") {
					configs.push_back(FlagConfig::ALL_ON);
				} else if (name == "all_off") {
					configs.push_back(FlagConfig::ALL_OFF);
				} else if (name == "skip_empty_off") {
					configs.push_back(FlagConfig::SKIP_EMPTY_OFF);
				} else if (!name.empty()) {
					fprintf(stderr, "unknown config: %s (use all_on,all_off,skip_empty_off)\n", name.c_str());
					return 2;
				}
				start = end + 1;
			}
		} else {
			fprintf(stderr, "unknown arg: %s\n", arg.c_str());
			PrintUsage();
			return 2;
		}
	}
	if (db_path.empty()) {
		db_path = "/tmp/cost_model_bench_tpcc_sf" + to_string(scale) + ".db";
	}
	if (!FileExists(db_path)) {
		Log("Creating TPC-C DB at scale " + to_string(scale) + ": " + db_path);
		duckdb::DuckDB db(db_path);
		duckdb::Connection con(db);
		CreateTPCCSchema(con);
		InsertTPCCData(con, scale, openivm_bench::TPCCScaleProfile::SPEC);
		con.Query("PRAGMA checkpoint");
	}

	auto queries = BuildQueries();

	std::ofstream out(out_csv);
	out << "scale,query_id,description,workload,delta_pct,flag_config,rep,cycle,view_name,"
	       "cost_decision,incremental_cost,recompute_cost,incremental_predicted_ms,recompute_predicted_ms,calibrated,"
	       "auto_method,auto_ms,incremental_ms,full_ms,best_method,regret_ratio,inc_qerror,full_qerror,"
	       "correct,base_rows,mv_rows,"
	       "dml_statements,delta_rows,error\n";

	int total = 0;
	for (auto &q : queries) {
		if (!query_filter.empty() && !query_filter.count(q.id)) {
			continue;
		}
		if (batch_sel == "validated" && q.batch != Batch::VALIDATED) {
			continue;
		}
		if (batch_sel == "todo" && q.batch != Batch::TODO) {
			continue;
		}
		for (auto wl : q.workloads) {
			if (wl == Workload::EMPTY_DELTA) {
				continue; // an empty delta is a cycle with no change, not a case of its own
			}
			total += static_cast<int>(configs.size()) * reps * cycles;
		}
	}
	Log("Total cost-model benchmark rows planned: " + to_string(total));

	int row = 0;
	int errors = 0;
	for (auto &q : queries) {
		if (!query_filter.empty() && !query_filter.count(q.id)) {
			continue;
		}
		if (batch_sel == "validated" && q.batch != Batch::VALIDATED) {
			continue;
		}
		if (batch_sel == "todo" && q.batch != Batch::TODO) {
			continue;
		}
		for (auto wl : q.workloads) {
			if (wl == Workload::EMPTY_DELTA) {
				continue;
			}
			for (auto config : configs) {
				for (int rep = 1; rep <= reps; rep++) {
					// One database per case, refreshed `cycles` times. The cost model learns from
					// execution history, so it can only be measured on a database that accumulates
					// some; a case that refreshes once leaves it permanently uncalibrated.
					auto prepared = PrepareCase(db_path, q, wl, config, rep);
					for (int cycle = 1; cycle <= cycles; cycle++) {
						double pct = delta_pcts[(cycle - 1) % delta_pcts.size()];
						row++;
						Log("[" + to_string(row) + "/" + to_string(total) + "] " + q.id + " wl=" + WorkloadName(wl) +
						    " cycle=" + to_string(cycle) + "/" + to_string(cycles) + " pct=" + to_string(pct) +
						    " flags=" + FlagConfigName(config) + " rep=" + to_string(rep));
						ModeResult auto_result, inc_result, full_result;
						string promoted;
						if (!prepared.ok) {
							auto_result.error = prepared.error;
							inc_result.error = prepared.error;
							full_result.error = prepared.error;
						} else if (!ApplyCycleDelta(prepared, q, wl, pct, scale, prepared.error)) {
							auto_result.error = prepared.error;
							inc_result.error = prepared.error;
							full_result.error = prepared.error;
							prepared.ok = false;
						} else {
							// All three branch from the same state so the comparison is fair; only the
							// automatic one is carried forward, so the history the model learns from is
							// the history of the decisions it actually made.
							auto_result = RunMode(prepared, q, wl, pct, config, rep, cycle, RefreshMode::AUTO, true,
							                      warm, /*do_validate=*/validate, &promoted);
							inc_result = RunMode(prepared, q, wl, pct, config, rep, cycle, RefreshMode::INCREMENTAL,
							                     false, warm, /*do_validate=*/validate, nullptr);
							full_result = RunMode(prepared, q, wl, pct, config, rep, cycle, RefreshMode::FULL, false,
							                      warm, /*do_validate=*/validate, nullptr);
							if (!promoted.empty()) {
								if (auto_result.ok) {
									CopyFile(promoted, prepared.state->path);
								}
								std::remove(promoted.c_str());
								std::remove((promoted + ".wal").c_str());
							}
						}

						bool correct = auto_result.correct && inc_result.correct && full_result.correct;
						bool ok = auto_result.ok && inc_result.ok && full_result.ok;
						if (!ok || !correct) {
							errors++;
						}
						string error = auto_result.error;
						if (!inc_result.error.empty()) {
							if (!error.empty())
								error += " | ";
							error += "incremental: " + inc_result.error;
						}
						if (!full_result.error.empty()) {
							if (!error.empty())
								error += " | ";
							error += "full: " + full_result.error;
						}
						string best = BestMethod(inc_result.refresh_ms, full_result.refresh_ms);
						double chosen_ms =
						    ChosenPathMs(auto_result.method, inc_result.refresh_ms, full_result.refresh_ms);
						double regret = RegretRatio(auto_result.method, inc_result.refresh_ms, full_result.refresh_ms);
						if (!ok || !correct) {
							Log("[ERROR] " + q.id + " wl=" + WorkloadName(wl) + " pct=" + to_string(pct) +
							    " flags=" + FlagConfigName(config) + " rep=" + to_string(rep) + " error=" + error);
						} else if (MethodClass(auto_result.method) != best) {
							std::ostringstream msg;
							msg << "[MISCLASS] " << q.id << " wl=" << WorkloadName(wl) << " pct=" << pct
							    << " flags=" << FlagConfigName(config) << " rep=" << rep
							    << " cost_decision=" << auto_result.cost.decision
							    << " pred_inc_ms=" << auto_result.cost.incremental_predicted_ms
							    << " pred_full_ms=" << auto_result.cost.recompute_predicted_ms
							    << " calibrated=" << (auto_result.cost.calibrated ? "true" : "false")
							    << " auto_method=" << auto_result.method << " best=" << best
							    << " auto_ms=" << std::fixed << std::setprecision(3) << auto_result.refresh_ms
							    << " chosen_path_ms=" << chosen_ms << " incremental_ms=" << inc_result.refresh_ms
							    << " full_ms=" << full_result.refresh_ms << " regret=" << std::setprecision(3)
							    << regret;
							Log(msg.str());
						}
						out << scale << "," << q.id << "," << CsvQuote(q.description) << "," << WorkloadName(wl) << ","
						    << pct << "," << FlagConfigName(config) << "," << rep << "," << cycle << ","
						    << q.refresh_mvs.back() << ","
						    << CsvQuote(auto_result.cost.decision) << "," << std::fixed << std::setprecision(6)
						    << auto_result.cost.incremental_cost << "," << auto_result.cost.recompute_cost << ","
						    << auto_result.cost.incremental_predicted_ms << ","
						    << auto_result.cost.recompute_predicted_ms << ","
						    << (auto_result.cost.calibrated ? "true" : "false") << "," << CsvQuote(auto_result.method)
						    << "," << std::setprecision(3) << auto_result.refresh_ms << "," << inc_result.refresh_ms
						    << "," << full_result.refresh_ms << "," << CsvQuote(best) << "," << std::setprecision(6)
						    << regret << ","
						    << QError(auto_result.cost.incremental_predicted_ms, inc_result.refresh_ms) << ","
						    << QError(auto_result.cost.recompute_predicted_ms, full_result.refresh_ms) << ","
						    << (correct ? "true" : "false") << "," << auto_result.base_rows << ","
						    << auto_result.mv_rows << "," << auto_result.dml_statements << ","
						    << auto_result.delta_rows << "," << CsvQuote(error) << "\n";
						out.flush();
					}
				}
			}
		}
	}

	Log("Rows: " + to_string(row) + ", errors: " + to_string(errors));
	return errors == 0 ? 0 : 1;
}
