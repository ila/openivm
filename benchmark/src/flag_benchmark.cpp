//
// Flag Benchmark (Phase 2): measures the impact of OpenIVM optimization flags
// on materialized view creation + refresh time.
//
// For each combination of (query, workload, delta_size, flag_config, rep):
//   - Fork a child (crash isolation)
//   - Child copies the pre-built TPC-C DB, opens it, applies the flag config,
//     creates the MV(s), warms caches, applies N delta rows, runs PRAGMA refresh,
//     and reports create_ms + refresh_ms to the parent.
// The parent writes one CSV row per run and at the end computes medians per
// (query, workload, delta_size, flag_config) and flags regressions:
// median(refresh_ms[all_on]) must be <= median(refresh_ms[all_off]) * 1.05.
//
// Usage:
//   flag_benchmark --scale 1 --db <path> --out <csv> [--reps 3] [--ducklake]
//                  [--filter Q1,Q2,...] [--keep-db]
//

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"
#include "tpcc_helpers.hpp"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <poll.h>
#include <set>
#include <signal.h>
#include <sstream>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <vector>

using namespace std;
using openivm_bench::CreateTPCCSchema;
using openivm_bench::FileExists;
using openivm_bench::InsertTPCCData;
using openivm_bench::Log;
using openivm_bench::ReadAllBytes;
using openivm_bench::Timestamp;
using openivm_bench::WriteAllBytes;

// ---------------------------------------------------------------------------
// Query + workload definitions

enum class Workload {
	INSERT_ONLY, // N rows inserted
	MIXED,       // INSERTs + UPDATEs + DELETEs
	EMPTY_DELTA, // no DML (N is ignored)
};

static const char *WorkloadName(Workload w) {
	switch (w) {
	case Workload::INSERT_ONLY: return "insert_only";
	case Workload::MIXED: return "mixed";
	case Workload::EMPTY_DELTA: return "empty_delta";
	}
	return "?";
}

struct QueryDef {
	string id;                 // Q01, Q02, ...
	string description;        // human description
	vector<string> create_mvs; // CREATE MATERIALIZED VIEW ...; statements, in order
	vector<string> refresh_mvs;
	vector<string> warm_tables;
	vector<Workload> workloads;
	bool is_ducklake;

	QueryDef(string id_, string desc_, vector<string> create_, vector<string> refresh_, vector<string> warm_,
	         vector<Workload> wls_, bool dl_)
	    : id(std::move(id_)), description(std::move(desc_)), create_mvs(std::move(create_)),
	      refresh_mvs(std::move(refresh_)), warm_tables(std::move(warm_)), workloads(std::move(wls_)),
	      is_ducklake(dl_) {
	}
};

struct FlagConfig {
	string id;        // "all_on", "all_off"
	bool all_on;      // true = defaults, false = flip all optimization flags off
};

// ---------------------------------------------------------------------------
// DML generator: produces deterministic N-row insert-only, mixed, or
// empty-delta workloads for a given TPC-C table. Primary-key offsets are kept
// well above the base-data range so inserts don't collide with pre-populated
// rows at SF=1..10.

static int64_t kPkBase = 100000;

static vector<string> GenerateInserts(const string &table, int n, int scale, int64_t pk_offset) {
	vector<string> out;
	out.reserve(n);
	for (int i = 0; i < n; i++) {
		int64_t pk = kPkBase + pk_offset + i;
		int w = 1 + (i % scale);
		int d = 1 + (i % 10);
		int c = 1 + (i % 30);
		if (table == "CUSTOMER") {
			out.push_back(
			    "INSERT INTO CUSTOMER VALUES (" + to_string(w) + ", " + to_string(d) + ", " + to_string(pk) +
			    ", 0.05, 'GC', 'Last" + to_string(pk) + "', 'First" + to_string(pk) + "', 50000.00, " +
			    to_string(100 + (i % 500)) + ".00, 0.0, 0, 0, 'S1', 'S2', 'City', 'ST', '12345', '1234567890', "
			    "NOW(), 'M', 'data')");
		} else if (table == "WAREHOUSE") {
			out.push_back("INSERT INTO WAREHOUSE VALUES (" + to_string(pk) +
			              ", 0.00, 0.05, 'W', 'S1', 'S2', 'City', 'ST', '123456789')");
		} else if (table == "DISTRICT") {
			out.push_back("INSERT INTO DISTRICT VALUES (" + to_string(w) + ", " + to_string(pk) +
			              ", 0.00, 0.05, 1, 'D', 'S1', 'S2', 'City', 'ST', '123456789')");
		} else if (table == "OORDER") {
			out.push_back("INSERT INTO OORDER VALUES (" + to_string(w) + ", " + to_string(d) + ", " +
			              to_string(pk) + ", " + to_string(c) + ", NULL, 5, 1, NOW())");
		} else if (table == "ORDER_LINE") {
			out.push_back("INSERT INTO ORDER_LINE VALUES (" + to_string(w) + ", " + to_string(d) + ", " +
			              to_string(pk) + ", 1, " + to_string(1 + (i % 100)) + ", NULL, " +
			              to_string(10 + (i % 400)) + ".00, " + to_string(w) + ", 5.00, 'D')");
		} else if (table == "STOCK") {
			out.push_back("INSERT INTO STOCK VALUES (" + to_string(w) + ", " + to_string(pk) + ", " +
			              to_string(50 + (i % 50)) +
			              ", 0.00, 0, 0, 'SD', 'D1', 'D2', 'D3', 'D4', 'D5', 'D6', 'D7', 'D8', 'D9', 'D0')");
		} else if (table == "ITEM") {
			out.push_back("INSERT INTO ITEM VALUES (" + to_string(pk) + ", 'Item" + to_string(pk) + "', " +
			              to_string(10 + (i % 90)) + ".99, 'ID', 1)");
		} else if (table == "HISTORY") {
			out.push_back("INSERT INTO HISTORY VALUES (" + to_string(c) + ", " + to_string(d) + ", " +
			              to_string(w) + ", " + to_string(d) + ", " + to_string(w) +
			              ", '2026-01-01 00:00:00', " + to_string(10 + (i % 500)) + ".00, 'PMT')");
		} else if (table == "NEW_ORDER") {
			out.push_back("INSERT INTO NEW_ORDER VALUES (" + to_string(w) + ", " + to_string(d) + ", " +
			              to_string(pk) + ")");
		}
	}
	return out;
}

static vector<string> GenerateUpdates(const string &table, int n, int scale) {
	vector<string> out;
	out.reserve(n);
	for (int i = 0; i < n; i++) {
		int w = 1 + (i % scale);
		int d = 1 + (i % 10);
		int c = 1 + (i % 30);
		if (table == "CUSTOMER") {
			out.push_back("UPDATE CUSTOMER SET C_BALANCE = " + to_string(-100 - (i % 500)) +
			              ".00 WHERE C_W_ID = " + to_string(w) + " AND C_D_ID = " + to_string(d) +
			              " AND C_ID = " + to_string(c));
		} else if (table == "WAREHOUSE") {
			out.push_back("UPDATE WAREHOUSE SET W_YTD = " + to_string(300000 + i * 100) +
			              ".00 WHERE W_ID = " + to_string(w));
		} else if (table == "DISTRICT") {
			out.push_back("UPDATE DISTRICT SET D_YTD = " + to_string(30000 + i * 10) +
			              ".00 WHERE D_W_ID = " + to_string(w) + " AND D_ID = " + to_string(d));
		} else if (table == "STOCK") {
			out.push_back("UPDATE STOCK SET S_QUANTITY = " + to_string(50 + (i % 100)) +
			              " WHERE S_W_ID = " + to_string(w) + " AND S_I_ID = " + to_string(1 + (i % 100)));
		} else if (table == "ORDER_LINE") {
			out.push_back("UPDATE ORDER_LINE SET OL_AMOUNT = " + to_string(50 + (i % 400)) +
			              ".00 WHERE OL_W_ID = " + to_string(w) + " AND OL_D_ID = " + to_string(d) +
			              " AND OL_O_ID = 1 AND OL_NUMBER = 1");
		} else if (table == "OORDER") {
			out.push_back("UPDATE OORDER SET O_CARRIER_ID = " + to_string(1 + (i % 10)) +
			              " WHERE O_W_ID = " + to_string(w) + " AND O_D_ID = " + to_string(d) +
			              " AND O_ID = " + to_string(1 + (i % 5)));
		} else if (table == "ITEM") {
			out.push_back("UPDATE ITEM SET I_PRICE = " + to_string(20 + (i % 90)) +
			              ".99 WHERE I_ID = " + to_string(1 + (i % 100)));
		}
	}
	return out;
}

// Build a workload of `size` DML statements for a given table + workload type.
static vector<string> BuildWorkload(const string &table, int size, int scale, Workload wl, int64_t pk_offset) {
	if (wl == Workload::EMPTY_DELTA || size == 0) {
		return {};
	}
	if (wl == Workload::INSERT_ONLY) {
		return GenerateInserts(table, size, scale, pk_offset);
	}
	if (wl == Workload::MIXED) {
		// roughly 50% INSERT, 30% UPDATE, 20% DELETE-of-just-inserted
		int n_ins = size / 2;
		int n_upd = (size * 30) / 100;
		int n_del = size - n_ins - n_upd;
		auto inserts = GenerateInserts(table, n_ins, scale, pk_offset);
		auto updates = GenerateUpdates(table, n_upd, scale);
		vector<string> deletes;
		deletes.reserve(n_del);
		for (int i = 0; i < n_del; i++) {
			int64_t pk = kPkBase + pk_offset + i; // deletes a row we just inserted
			if (table == "CUSTOMER") {
				deletes.push_back("DELETE FROM CUSTOMER WHERE C_ID = " + to_string(pk));
			} else if (table == "WAREHOUSE") {
				deletes.push_back("DELETE FROM WAREHOUSE WHERE W_ID = " + to_string(pk));
			} else if (table == "DISTRICT") {
				deletes.push_back("DELETE FROM DISTRICT WHERE D_ID = " + to_string(pk));
			} else if (table == "OORDER") {
				deletes.push_back("DELETE FROM OORDER WHERE O_ID = " + to_string(pk));
			} else if (table == "ORDER_LINE") {
				deletes.push_back("DELETE FROM ORDER_LINE WHERE OL_O_ID = " + to_string(pk));
			} else if (table == "STOCK") {
				deletes.push_back("DELETE FROM STOCK WHERE S_I_ID = " + to_string(pk));
			} else if (table == "ITEM") {
				deletes.push_back("DELETE FROM ITEM WHERE I_ID = " + to_string(pk));
			} else if (table == "NEW_ORDER") {
				deletes.push_back("DELETE FROM NEW_ORDER WHERE NO_O_ID = " + to_string(pk));
			}
		}
		vector<string> out;
		out.reserve(size);
		out.insert(out.end(), inserts.begin(), inserts.end());
		out.insert(out.end(), updates.begin(), updates.end());
		out.insert(out.end(), deletes.begin(), deletes.end());
		return out;
	}
	return {};
}

// ---------------------------------------------------------------------------
// Query catalog
// Each query exercises one or more optimization flags. See per-entry comment.

static void AddQuery(vector<QueryDef> &qs, string id, string desc, vector<string> creates, vector<string> refresh,
                     vector<string> warm, vector<Workload> wls, bool dl) {
	qs.push_back(QueryDef(std::move(id), std::move(desc), std::move(creates), std::move(refresh), std::move(warm),
	                       std::move(wls), dl));
}

static vector<QueryDef> BuildQueries(bool include_ducklake) {
	vector<QueryDef> qs;

	// Q01: SUM+GROUP BY → skip_aggregate_delete (IO), normal path (M)
	AddQuery(qs,
	    "Q01", "SUM+GROUP BY",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY C_W_ID"},
	    {"mv_q"}, {"CUSTOMER"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// Q02: MIN+MAX → minmax_incremental fast path (IO)
	AddQuery(qs,
	    "Q02", "MIN+MAX GROUP BY",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, COUNT(*) AS n, MIN(C_BALANCE) AS mn, MAX(C_BALANCE) AS mx "
	     "FROM CUSTOMER GROUP BY C_W_ID"},
	    {"mv_q"}, {"CUSTOMER"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// Q03: HAVING → having_merge flag
	AddQuery(qs,
	    "Q03", "GROUP BY + HAVING",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID HAVING "
	     "COUNT(*) > 5"},
	    {"mv_q"}, {"CUSTOMER"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// Q04: projection → skip_projection_delete flag
	AddQuery(qs,
	    "Q04", "projection + filter",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > 0"},
	    {"mv_q"}, {"CUSTOMER"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// Q05: 2-way INNER JOIN → skip_empty_deltas, inclusion-exclusion
	AddQuery(qs,
	    "Q05", "2-way INNER JOIN",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, d.D_ID, d.D_NAME FROM WAREHOUSE w JOIN DISTRICT d ON "
	     "w.W_ID = d.D_W_ID"},
	    {"mv_q"}, {"WAREHOUSE", "DISTRICT"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// Q06: 3-way INNER JOIN → fk_pruning reduces 2^3-1=7 terms to fewer
	AddQuery(qs,
	    "Q06", "3-way INNER JOIN",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, d.D_ID, c.C_ID FROM WAREHOUSE w JOIN DISTRICT d ON "
	     "w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID"},
	    {"mv_q"}, {"WAREHOUSE", "DISTRICT", "CUSTOMER"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// Q07: LEFT JOIN + aggregate → group-recompute path
	AddQuery(qs,
	    "Q07", "LEFT JOIN + COUNT",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, COUNT(d.D_ID) AS nd FROM WAREHOUSE w LEFT JOIN DISTRICT "
	     "d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID"},
	    {"mv_q"}, {"WAREHOUSE", "DISTRICT"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// Q08: join + GROUP BY + SUM (FK OORDER→ORDER_LINE) → fk_pruning impact
	AddQuery(qs,
	    "Q08", "JOIN + GROUP BY + SUM",
	    {"CREATE MATERIALIZED VIEW mv_q AS SELECT o.O_W_ID, SUM(ol.OL_AMOUNT) AS tot FROM OORDER o JOIN "
	     "ORDER_LINE ol ON o.O_W_ID = ol.OL_W_ID AND o.O_D_ID = ol.OL_D_ID AND o.O_ID = ol.OL_O_ID GROUP BY "
	     "o.O_W_ID"},
	    {"mv_q"}, {"OORDER", "ORDER_LINE"},
	    {Workload::MIXED, Workload::EMPTY_DELTA}, false);

	// P1: 3-level cascade pipeline — tests cascade refresh ordering
	AddQuery(qs,
	    "P1", "cascade pipeline",
	    {
	        "CREATE MATERIALIZED VIEW mv_p1_a AS SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) AS s FROM CUSTOMER GROUP "
	        "BY C_W_ID, C_D_ID",
	        "CREATE MATERIALIZED VIEW mv_p1_b AS SELECT C_W_ID, SUM(s) AS s_tot FROM mv_p1_a GROUP BY C_W_ID",
	        "CREATE MATERIALIZED VIEW mv_p1_c AS SELECT SUM(s_tot) AS g FROM mv_p1_b",
	    },
	    {"mv_p1_a", "mv_p1_b", "mv_p1_c"}, {"CUSTOMER"},
	    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, false);

	if (include_ducklake) {
		AddQuery(qs,
		    "DL1", "DuckLake aggregate",
		    {"CREATE MATERIALIZED VIEW mv_q AS SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM dl.CUSTOMER GROUP BY "
		     "C_W_ID"},
		    {"mv_q"}, {"dl.CUSTOMER"},
		    {Workload::INSERT_ONLY, Workload::MIXED, Workload::EMPTY_DELTA}, true);

		AddQuery(qs,
		    "DL2", "DuckLake 3-way join",
		    {"CREATE MATERIALIZED VIEW mv_q AS SELECT w.W_ID, d.D_ID, c.C_ID FROM dl.WAREHOUSE w JOIN "
		     "dl.DISTRICT d ON w.W_ID = d.D_W_ID JOIN dl.CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = "
		     "c.C_D_ID"},
		    {"mv_q"}, {"dl.WAREHOUSE", "dl.DISTRICT", "dl.CUSTOMER"},
		    {Workload::MIXED, Workload::EMPTY_DELTA}, true);
	}

	return qs;
}

// ---------------------------------------------------------------------------
// Fork worker: runs one (query, workload, size, flag_config, rep) config

struct RunResult {
	uint32_t ok;               // 1 = measured OK, 0 = error/crash
	double create_ms;          // total CREATE MATERIALIZED VIEW time
	double refresh_ms;         // total PRAGMA refresh time (summed across pipeline MVs)
	int64_t mv_rows;           // SELECT COUNT(*) FROM last MV
	int64_t delta_rows_issued; // number of DML statements executed (post-error-filter)
	char error[512];           // error message (first ~500 chars)
};

static int64_t NowMicros() {
	using namespace std::chrono;
	return duration_cast<microseconds>(steady_clock::now().time_since_epoch()).count();
}

static bool CopyFile(const string &src, const string &dst) {
	int in_fd = open(src.c_str(), O_RDONLY);
	if (in_fd < 0) return false;
	int out_fd = open(dst.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
	if (out_fd < 0) {
		close(in_fd);
		return false;
	}
	char buf[1 << 16];
	ssize_t n;
	while ((n = read(in_fd, buf, sizeof(buf))) > 0) {
		ssize_t w = 0;
		while (w < n) {
			ssize_t k = write(out_fd, buf + w, n - w);
			if (k <= 0) {
				close(in_fd);
				close(out_fd);
				return false;
			}
			w += k;
		}
	}
	close(in_fd);
	close(out_fd);
	return n == 0;
}

static void SetErr(RunResult &r, const string &msg) {
	r.ok = 0;
	size_t n = std::min<size_t>(msg.size(), sizeof(r.error) - 1);
	memcpy(r.error, msg.data(), n);
	r.error[n] = 0;
}

// Wire format parent->child: which config to run (query index + params).
// Length-zero signals child to exit.
struct ConfigRequest {
	uint32_t query_idx;
	uint32_t workload; // Workload cast to uint32
	int32_t delta_size;
	uint32_t all_on; // 1 = all defaults ON, 0 = all optimization flags OFF
	int32_t scale;
};

// RAII cleanup of per-child temp files. The destructor runs no matter how
// RunOneConfig returns — early return on error, exception thrown by DuckDB,
// or normal success — so tmpfs never leaks the 1-3 GB intermediate DB files.
//
// Without this, the previous version only cleaned up on the success path:
// any timeout/error/exception path left /tmp/flag_bench_<pid>.db (often 2+ GB)
// in place. After ~6 large-config failures we filled the 16 GB tmpfs and
// every subsequent child died with ENOSPC on its initial DB copy.
struct TempPaths {
	string db;
	string wal;
	string dl;
	string spill_dir;

	~TempPaths() {
		std::remove(db.c_str());
		std::remove(wal.c_str());
		std::remove(dl.c_str());
		// Remove DuckDB spill directory recursively (only if we set one).
		if (!spill_dir.empty()) {
			// Best-effort: rm -rf via system. Fail silent.
			string cmd = "rm -rf '" + spill_dir + "' 2>/dev/null";
			(void)!system(cmd.c_str());
		}
	}
};

// Run one (query, workload, size, flag_config) measurement inside the persistent
// child. Opens a fresh DuckDB instance on a freshly copied SF DB so no state
// leaks between configs. Returns true on measured result, false on fatal exit
// conditions (caller will still have populated r.error in that case).
static bool RunOneConfig(const QueryDef &q, Workload wl, int delta_size, bool all_on, int scale,
                         const string &src_db_path, RunResult &r) {
	// 1. Fresh DB copy. Pre-clean (in case a previous config's cleanup raced)
	// + register RAII guard for post-cleanup.
	TempPaths tp;
	tp.db = "/tmp/flag_bench_" + to_string(getpid()) + ".db";
	tp.wal = tp.db + ".wal";
	tp.dl = tp.db + ".ducklake.db";
	tp.spill_dir = "/tmp/flag_bench_" + to_string(getpid()) + "_spill";
	std::remove(tp.db.c_str());
	std::remove(tp.wal.c_str());
	std::remove(tp.dl.c_str());
	{
		string cmd = "rm -rf '" + tp.spill_dir + "' 2>/dev/null";
		(void)!system(cmd.c_str());
	}
	if (!CopyFile(src_db_path, tp.db)) {
		SetErr(r, "copy db failed: " + string(strerror(errno)));
		return false;
	}
	const string &my_db = tp.db;
	const string &dl_path = tp.dl;

	duckdb::DuckDB db(my_db);
	duckdb::Connection con(db);
	auto load = con.Query("LOAD openivm");
	if (!load || load->HasError()) {
		SetErr(r, "LOAD openivm: " + (load ? load->GetError() : "null"));
		return false;
	}
	// Contain DuckDB spill files inside our per-child directory so they get
	// cleaned up alongside the DB. Defaults to inside the DB's directory which
	// is fine, but explicit beats implicit + it's easier to monitor /tmp usage.
	con.Query("PRAGMA temp_directory = '" + tp.spill_dir + "'");

	// 2. DuckLake setup if needed
	if (q.is_ducklake) {
		con.Query("INSTALL ducklake");
		con.Query("LOAD ducklake");
		auto at = con.Query("ATTACH IF NOT EXISTS '" + dl_path + "' AS dl (TYPE ducklake)");
		if (!at || at->HasError()) {
			SetErr(r, "attach dl: " + (at ? at->GetError() : "null"));
			return false;
		}
		auto chk = con.Query("SELECT COUNT(*) FROM information_schema.tables WHERE table_catalog='dl' "
		                     "AND LOWER(table_name)='warehouse'");
		bool needs_init = !chk || chk->HasError() || chk->RowCount() == 0 ||
		                  chk->GetValue(0, 0).GetValue<int64_t>() == 0;
		if (needs_init) {
			con.Query("USE dl.main");
			CreateTPCCSchema(con);
			auto probe = con.Query("SELECT current_database()");
			string native_cat = (probe && !probe->HasError() && probe->RowCount() > 0)
			                        ? probe->GetValue(0, 0).ToString()
			                        : "memory";
			if (native_cat == "dl") native_cat = "memory";
			for (const char *t : {"WAREHOUSE", "DISTRICT", "CUSTOMER", "ITEM", "STOCK", "OORDER", "NEW_ORDER",
			                       "ORDER_LINE", "HISTORY"}) {
				con.Query(string("INSERT INTO ") + t + " SELECT * FROM " + native_cat + ".main." + t);
			}
		} else {
			con.Query("USE dl.main");
		}
	}

	// 3. Apply flag config
	if (!all_on) {
		const char *flags[] = {"ivm_skip_empty_deltas",    "ivm_fk_pruning",      "ivm_skip_aggregate_delete",
		                        "ivm_skip_projection_delete", "ivm_minmax_incremental", "ivm_having_merge",
		                        "ivm_ducklake_nterm"};
		for (const char *f : flags) {
			con.Query(string("SET ") + f + " = false");
		}
	}

	// 4. CREATE MATERIALIZED VIEW(s)
	int64_t t0 = NowMicros();
	for (auto &create_sql : q.create_mvs) {
		auto cr = con.Query(create_sql);
		if (!cr || cr->HasError()) {
			SetErr(r, "CREATE MV failed: " + (cr ? cr->GetError() : "null"));
			return false;
		}
	}
	int64_t t1 = NowMicros();
	r.create_ms = (t1 - t0) / 1000.0;

	// 5. Warm caches
	for (auto &tbl : q.warm_tables) {
		con.Query("SELECT * FROM " + tbl);
		if (!q.is_ducklake) {
			string dt = tbl;
			auto pos = dt.find('.');
			if (pos != string::npos) dt = dt.substr(pos + 1);
			for (auto &ch : dt) ch = static_cast<char>(tolower(ch));
			con.Query("SELECT * FROM delta_" + dt);
		}
	}

	// 6. Apply DML workload
	int issued = 0;
	if (wl != Workload::EMPTY_DELTA && delta_size > 0) {
		vector<string> dml;
		if (q.warm_tables.size() == 1) {
			string t = q.warm_tables[0];
			auto p = t.find('.');
			if (p != string::npos) t = t.substr(p + 1);
			dml = BuildWorkload(t, delta_size, scale, wl, /*pk_offset=*/0);
		} else {
			int per = delta_size / static_cast<int>(q.warm_tables.size());
			int64_t off = 0;
			for (auto &t : q.warm_tables) {
				string tb = t;
				auto p = tb.find('.');
				if (p != string::npos) tb = tb.substr(p + 1);
				auto part = BuildWorkload(tb, per, scale, wl, off);
				dml.insert(dml.end(), part.begin(), part.end());
				off += per + 100000;
			}
		}
		for (auto &s : dml) {
			auto w = con.Query(s);
			if (w && !w->HasError()) issued++;
		}
	}
	r.delta_rows_issued = issued;

	// 7. Measure refresh(es)
	int64_t t2 = NowMicros();
	for (auto &mv : q.refresh_mvs) {
		auto rr = con.Query("PRAGMA refresh('" + mv + "')");
		if (!rr || rr->HasError()) {
			SetErr(r, "PRAGMA refresh(" + mv + ") failed: " + (rr ? rr->GetError() : "null"));
			return false;
		}
	}
	int64_t t3 = NowMicros();
	r.refresh_ms = (t3 - t2) / 1000.0;

	// 8. MV row-count sanity
	string last_mv = q.refresh_mvs.back();
	auto cnt = con.Query("SELECT COUNT(*) FROM " + last_mv);
	if (cnt && !cnt->HasError() && cnt->RowCount() > 0) {
		r.mv_rows = cnt->GetValue(0, 0).GetValue<int64_t>();
	}

	r.ok = 1;
	return true;
}

// Persistent child main loop: receives ConfigRequest frames on read_fd, runs
// each measurement in a fresh DuckDB instance (to avoid state leaking between
// configs), and writes RunResult back on write_fd. A zero-length request
// terminates the child cleanly.
static void ChildWorkerMain(int read_fd, int write_fd, const string &src_db_path, bool ducklake) {
	auto queries = BuildQueries(ducklake);
	while (true) {
		ConfigRequest req = {};
		if (!ReadAllBytes(read_fd, &req, sizeof(req))) {
			_exit(0);
		}
		if (req.query_idx == UINT32_MAX) {
			// shutdown signal
			_exit(0);
		}
		RunResult r = {};
		try {
			if (req.query_idx >= queries.size()) {
				SetErr(r, "bad query idx");
			} else {
				RunOneConfig(queries[req.query_idx], static_cast<Workload>(req.workload),
				             static_cast<int>(req.delta_size), req.all_on != 0, req.scale, src_db_path, r);
			}
		} catch (const std::exception &e) {
			SetErr(r, string("exception: ") + e.what());
		} catch (...) {
			SetErr(r, "unknown exception");
		}
		WriteAllBytes(write_fd, &r, sizeof(r));
	}
}

// ForkWorker: persistent fork+pipe pair, mirrors rewriter_benchmark's
// worker. Respawns child if it dies between calls.
struct ForkWorker {
	pid_t child_pid = -1;
	int to_child_fd = -1;
	int from_child_fd = -1;
	string src_db_path;
	bool ducklake = false;

	void Start() {
		Stop();
		int to_c[2], from_c[2];
		if (pipe(to_c) != 0 || pipe(from_c) != 0) {
			throw std::runtime_error("pipe() failed");
		}
		child_pid = fork();
		if (child_pid < 0) {
			close(to_c[0]);
			close(to_c[1]);
			close(from_c[0]);
			close(from_c[1]);
			throw std::runtime_error("fork() failed");
		}
		if (child_pid == 0) {
			close(to_c[1]);
			close(from_c[0]);
			ChildWorkerMain(to_c[0], from_c[1], src_db_path, ducklake);
			_exit(0);
		}
		close(to_c[0]);
		close(from_c[1]);
		to_child_fd = to_c[1];
		from_child_fd = from_c[0];
	}

	void Stop() {
		if (child_pid > 0) {
			ConfigRequest stop = {};
			stop.query_idx = UINT32_MAX;
			WriteAllBytes(to_child_fd, &stop, sizeof(stop));
			int status;
			for (int i = 0; i < 4; i++) {
				int w = waitpid(child_pid, &status, WNOHANG);
				if (w != 0) break;
				usleep(50000);
			}
			int w = waitpid(child_pid, &status, WNOHANG);
			if (w == 0) {
				kill(child_pid, SIGKILL);
				waitpid(child_pid, &status, 0);
			}
			child_pid = -1;
		}
		if (to_child_fd >= 0) {
			close(to_child_fd);
			to_child_fd = -1;
		}
		if (from_child_fd >= 0) {
			close(from_child_fd);
			from_child_fd = -1;
		}
	}

	// Submit one config; block until result or timeout. On timeout or child-
	// crash, respawns the child so the NEXT call works.
	RunResult Submit(const ConfigRequest &req, double timeout_s) {
		RunResult r = {};
		if (child_pid <= 0) {
			Start();
		}
		if (!WriteAllBytes(to_child_fd, &req, sizeof(req))) {
			// child died; reap and report
			int status;
			waitpid(child_pid, &status, 0);
			child_pid = -1;
			close(to_child_fd);
			close(from_child_fd);
			to_child_fd = from_child_fd = -1;
			SetErr(r, "child died (write failed)");
			return r;
		}
		auto deadline = std::chrono::steady_clock::now() +
		                std::chrono::duration_cast<std::chrono::steady_clock::duration>(
		                    std::chrono::duration<double>(timeout_s));
		while (true) {
			auto now = std::chrono::steady_clock::now();
			if (now >= deadline) {
				kill(child_pid, SIGKILL);
				waitpid(child_pid, nullptr, 0);
				child_pid = -1;
				close(to_child_fd);
				close(from_child_fd);
				to_child_fd = from_child_fd = -1;
				SetErr(r, "timeout");
				return r;
			}
			int remaining_ms = static_cast<int>(
			    std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
			int poll_ms = std::min(remaining_ms, 500);
			struct pollfd pfd = {from_child_fd, POLLIN, 0};
			int pr = poll(&pfd, 1, poll_ms);
			if (pr > 0 && (pfd.revents & POLLIN)) {
				if (ReadAllBytes(from_child_fd, &r, sizeof(r))) {
					return r;
				}
				// EOF: child died mid-result
				int status;
				waitpid(child_pid, &status, 0);
				child_pid = -1;
				close(to_child_fd);
				close(from_child_fd);
				to_child_fd = from_child_fd = -1;
				SetErr(r, "child crashed");
				return r;
			}
			// poll timed out (or error); check if child exited
			int status;
			pid_t w = waitpid(child_pid, &status, WNOHANG);
			if (w == child_pid) {
				child_pid = -1;
				close(to_child_fd);
				close(from_child_fd);
				to_child_fd = from_child_fd = -1;
				SetErr(r, "child exited unexpectedly");
				return r;
			}
		}
	}
};

// (Old per-config fork helper removed — replaced by ForkWorker above.)

// ---------------------------------------------------------------------------
// CSV output + median / regression analysis

struct Key {
	string qid;
	string workload;
	int size;
	string flags;
	bool operator<(const Key &o) const {
		if (qid != o.qid) return qid < o.qid;
		if (workload != o.workload) return workload < o.workload;
		if (size != o.size) return size < o.size;
		return flags < o.flags;
	}
};

static double Median(vector<double> v) {
	if (v.empty()) return 0.0;
	std::sort(v.begin(), v.end());
	size_t n = v.size();
	if (n % 2 == 1) return v[n / 2];
	return 0.5 * (v[n / 2 - 1] + v[n / 2]);
}

// ---------------------------------------------------------------------------
// Main

static void PrintUsage() {
	fprintf(stderr,
	        "flag_benchmark --scale N --db PATH --out CSV [--reps 3] [--ducklake] [--filter Q01,Q05,...]\n"
	        "               [--sizes 0,100,1000] [--timeout 120]\n");
}

// Sweep /tmp for orphaned per-child files left by previous crashed runs.
// Each per-child file is named `flag_bench_<pid>.db` (+ .wal, + .ducklake.db,
// + _spill/ directory). Any file whose owner pid is not currently alive
// (no /proc/<pid>) is safe to remove. The source DB (no embedded pid in the
// filename, e.g. `flag_bench_sf1.db`) is preserved.
static int SweepOrphanedTempFiles() {
	int removed = 0;
	DIR *d = opendir("/tmp");
	if (!d) return 0;
	struct dirent *de;
	while ((de = readdir(d))) {
		string name = de->d_name;
		if (name.find("flag_bench_") != 0) continue;
		// Extract pid: format is flag_bench_<pid>.db[.wal|.ducklake.db]
		// or flag_bench_<pid>_spill (directory). Skip names without a numeric pid.
		size_t after_prefix = strlen("flag_bench_");
		if (after_prefix >= name.size()) continue;
		size_t pid_end = after_prefix;
		while (pid_end < name.size() && isdigit(static_cast<unsigned char>(name[pid_end]))) {
			pid_end++;
		}
		if (pid_end == after_prefix) continue; // no digits → not a per-child file (e.g. flag_bench_sf1.db)
		string pid_str = name.substr(after_prefix, pid_end - after_prefix);
		string proc_path = "/proc/" + pid_str;
		struct stat st;
		if (::stat(proc_path.c_str(), &st) == 0) {
			// Owner pid still alive — leave alone.
			continue;
		}
		string full = "/tmp/" + name;
		if (de->d_type == DT_DIR) {
			string cmd = "rm -rf '" + full + "' 2>/dev/null";
			(void)!system(cmd.c_str());
		} else {
			std::remove(full.c_str());
		}
		removed++;
	}
	closedir(d);
	return removed;
}

int main(int argc, char **argv) {
	// Child death closes the write end of the parent->child pipe. Without this,
	// the parent's next `write()` raises SIGPIPE and kills the benchmark (which
	// is what happened to Q10 mixed sz=10000: the 4-way-join + 10k-row refresh
	// exceeds the per-config timeout, the parent SIGKILLs the child, and the
	// next Submit's write to the dead pipe SIGPIPEs the parent itself).
	signal(SIGPIPE, SIG_IGN);

	// Sweep orphaned /tmp/flag_bench_<pid>.db* from previous crashed runs.
	// Each big config can leave 2+ GB behind; at 16 GB tmpfs we hit ENOSPC
	// after a handful of crashes and every subsequent child fails its initial
	// DB copy. Remove files whose owner pid no longer has a /proc entry.
	int swept = SweepOrphanedTempFiles();
	if (swept > 0) {
		Log("Swept " + to_string(swept) + " orphaned /tmp/flag_bench_* file(s) from prior runs");
	}

	int scale = 1;
	string db_path;
	string out_csv = "flag_benchmark_results.csv";
	int reps = 3;
	bool ducklake = false;
	set<string> query_filter;
	vector<int> delta_sizes = {0, 100, 1000};
	double timeout_s = 300.0;

	for (int i = 1; i < argc; i++) {
		string a = argv[i];
		auto next = [&](const char *n) -> string {
			if (i + 1 >= argc) {
				fprintf(stderr, "%s requires a value\n", n);
				PrintUsage();
				_exit(2);
			}
			return string(argv[++i]);
		};
		if (a == "--scale") scale = std::stoi(next("--scale"));
		else if (a == "--db") db_path = next("--db");
		else if (a == "--out") out_csv = next("--out");
		else if (a == "--reps") reps = std::stoi(next("--reps"));
		else if (a == "--timeout") timeout_s = std::stod(next("--timeout"));
		else if (a == "--ducklake") ducklake = true;
		else if (a == "--filter") {
			string f = next("--filter");
			size_t start = 0;
			while (start < f.size()) {
				size_t end = f.find(',', start);
				if (end == string::npos) end = f.size();
				query_filter.insert(f.substr(start, end - start));
				start = end + 1;
			}
		} else if (a == "--sizes") {
			delta_sizes.clear();
			string f = next("--sizes");
			size_t start = 0;
			while (start < f.size()) {
				size_t end = f.find(',', start);
				if (end == string::npos) end = f.size();
				delta_sizes.push_back(std::stoi(f.substr(start, end - start)));
				start = end + 1;
			}
		} else {
			fprintf(stderr, "unknown arg: %s\n", a.c_str());
			PrintUsage();
			return 2;
		}
	}
	if (db_path.empty()) {
		db_path = "/tmp/flag_bench_sf" + to_string(scale) + ".db";
	}

	// 1. Create/populate the source TPC-C DB if missing.
	if (!FileExists(db_path)) {
		Log("Creating TPC-C DB at scale " + to_string(scale) + ": " + db_path);
		pid_t pid = fork();
		if (pid == 0) {
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
		waitpid(pid, &status, 0);
		if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
			Log("DB creation failed");
			return 3;
		}
	}

	// 2. Build query set. Parent and child both call BuildQueries(ducklake) —
	// we pass an index to the child, not the full QueryDef, so both sides must
	// agree on the same vector contents.
	auto queries = BuildQueries(ducklake);

	// 3. CSV header
	std::ofstream out(out_csv);
	out << "scale,query_id,description,workload,delta_size,flag_config,rep,ok,create_ms,refresh_ms,"
	       "mv_rows,delta_rows,error\n";

	vector<FlagConfig> configs = {{"all_on", true}, {"all_off", false}};

	// aggregated for final analysis
	std::map<Key, vector<double>> refresh_samples;
	int total_runs = 0;
	int total_errors = 0;
	int total_crashes = 0;

	// 4. Run the matrix
	int total_plan = 0;
	for (auto &q : queries) {
		if (!query_filter.empty() && !query_filter.count(q.id)) continue;
		for (auto wl : q.workloads) {
			for (int sz : delta_sizes) {
				if (wl == Workload::EMPTY_DELTA && sz != 0) continue; // only sz=0 for empty
				if (wl != Workload::EMPTY_DELTA && sz == 0) continue; // skip non-empty at sz=0
				for (auto &fc : configs) {
					(void)fc;
					total_plan += reps;
				}
			}
		}
	}
	Log("Total runs planned: " + to_string(total_plan));

	// Spin up the persistent child once.
	ForkWorker worker;
	worker.src_db_path = db_path;
	worker.ducklake = ducklake;
	worker.Start();

	int run_idx = 0;
	for (size_t qi = 0; qi < queries.size(); qi++) {
		auto &q = queries[qi];
		if (!query_filter.empty() && !query_filter.count(q.id)) continue;
		for (auto wl : q.workloads) {
			for (int sz : delta_sizes) {
				if (wl == Workload::EMPTY_DELTA && sz != 0) continue;
				if (wl != Workload::EMPTY_DELTA && sz == 0) continue;
				for (auto &fc : configs) {
					for (int rep = 1; rep <= reps; rep++) {
						run_idx++;
						if (run_idx % 20 == 1) {
							Log("[" + to_string(run_idx) + "/" + to_string(total_plan) + "] " + q.id +
							    " wl=" + WorkloadName(wl) + " sz=" + to_string(sz) + " flags=" + fc.id +
							    " rep=" + to_string(rep));
						}
						ConfigRequest req = {};
						req.query_idx = static_cast<uint32_t>(qi);
						req.workload = static_cast<uint32_t>(wl);
						req.delta_size = sz;
						req.all_on = fc.all_on ? 1u : 0u;
						req.scale = scale;
						RunResult r = worker.Submit(req, timeout_s);
						total_runs++;
						if (!r.ok) {
							total_errors++;
							string e = r.error;
							if (e.find("crash") != string::npos || e.find("timeout") != string::npos) {
								total_crashes++;
							}
						} else {
							Key k = {q.id, WorkloadName(wl), sz, fc.id};
							refresh_samples[k].push_back(r.refresh_ms);
						}
						// Escape CSV: description + error may contain commas/quotes
						auto csvq = [](const string &s) {
							string o;
							o.reserve(s.size() + 2);
							o.push_back('"');
							for (char c : s) {
								if (c == '"') o += "\"\"";
								else if (c == '\n') o += ' ';
								else o += c;
							}
							o.push_back('"');
							return o;
						};
						out << scale << "," << q.id << "," << csvq(q.description) << ","
						    << WorkloadName(wl) << "," << sz << "," << fc.id << "," << rep << ","
						    << (r.ok ? 1 : 0) << "," << std::fixed << std::setprecision(3) << r.create_ms
						    << "," << r.refresh_ms << "," << r.mv_rows << "," << r.delta_rows_issued
						    << "," << csvq(string(r.error)) << "\n";
						out.flush();
					}
				}
			}
		}
	}

	// 5. Medians + regression analysis
	Log("Runs: " + to_string(total_runs) + " total, " + to_string(total_errors) + " errors, " +
	    to_string(total_crashes) + " crashes/timeouts");
	Log("");
	Log("=== Regression analysis (refresh_ms, median of " + to_string(reps) + ") ===");
	Log("Expected: median(all_on) <= median(all_off) * 1.05");
	Log("");

	// Group samples by (qid, workload, size) so we can compare the two flag configs side-by-side.
	struct Combo {
		string qid, workload;
		int size;
	};
	auto cmp = [](const Combo &a, const Combo &b) {
		if (a.qid != b.qid) return a.qid < b.qid;
		if (a.workload != b.workload) return a.workload < b.workload;
		return a.size < b.size;
	};
	std::map<Combo, std::map<string, double>, decltype(cmp)> med_by_combo(cmp);
	for (auto &kv : refresh_samples) {
		Combo c = {kv.first.qid, kv.first.workload, kv.first.size};
		med_by_combo[c][kv.first.flags] = Median(kv.second);
	}

	// Regression = all_on is BOTH >5% relatively slower than all_off AND absolutely
	// more than 5ms slower. The absolute floor prevents fixed overhead and scheduler
	// noise on small refreshes from failing the benchmark: a 2ms swing on a 20-40ms
	// refresh is common enough in these forked benchmark runs and is not a useful
	// optimization signal.
	const double kRelTol = 1.05;
	const double kAbsFloorMs = 5.0;
	int n_regressions = 0;
	Log("qid      workload         size     all_on(ms)   all_off(ms)   speedup    status");
	for (auto &kv : med_by_combo) {
		const auto &c = kv.first;
		double on = kv.second.count("all_on") ? kv.second["all_on"] : 0.0;
		double off = kv.second.count("all_off") ? kv.second["all_off"] : 0.0;
		string status = "OK";
		if (on == 0 || off == 0) {
			status = "MISSING";
		} else if (on > off * kRelTol && (on - off) > kAbsFloorMs) {
			status = "REGRESSION";
			n_regressions++;
		}
		double speedup = off > 0 ? off / std::max(on, 0.001) : 0;
		ostringstream line;
		line << std::left << std::setw(9) << c.qid << std::setw(17) << c.workload << std::setw(9) << c.size
		     << std::right << std::setw(12) << std::fixed << std::setprecision(2) << on << std::setw(14) << off
		     << std::setw(10) << std::setprecision(2) << speedup << "x   " << status;
		Log(line.str());
	}
	Log("");
	Log("Regressions: " + to_string(n_regressions));
	Log("Crashes:     " + to_string(total_crashes));
	Log("");
	Log("CSV written to " + out_csv);

	// exit non-zero if anything bad happened, so CI / caller can detect
	worker.Stop();
	if (total_crashes > 0 || n_regressions > 0) return 1;
	return 0;
}
