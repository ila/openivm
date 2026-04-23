//
// Rewriter Benchmark: Test OpenIVM incremental view maintenance across 2000+ TPC-C queries.
// Two modes: --setup (generate DB), default (run benchmark on 5 phases per query).
// Based on PAC's sqlstorm benchmark (fork/pipe isolation, crash handling, stats output).
//

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"
#include "tpcc_helpers.hpp"

#include <chrono>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include <map>
#include <algorithm>
#include <ctime>
#include <iomanip>
#include <cstdio>
#include <cstdlib>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>
#include <poll.h>
#include <signal.h>
#include <limits.h>
#include <random>

using namespace std;

// Thin wrappers around shared helpers so existing in-file call sites don't need to change.
using openivm_bench::Timestamp;
using openivm_bench::Log;
using openivm_bench::WriteAllBytes;
using openivm_bench::ReadAllBytes;
using openivm_bench::FileExists;
using openivm_bench::CreateTPCCSchema;
using openivm_bench::InsertTPCCData;
using openivm_bench::GenerateDeltaPool;

static string FormatNumber(double v) {
	std::ostringstream oss;
	oss << std::setprecision(5) << std::defaultfloat << v;
	string s = oss.str();
	auto pos = s.find('.');
	if (pos != string::npos) {
		while (!s.empty() && s.back() == '0') { s.pop_back(); }
		if (!s.empty() && s.back() == '.') { s.pop_back(); }
	}
	return s;
}

// Normalized verify helper: returns a synthetic column-name list (`c1, c2, ...`) and
// a projection that formats DOUBLE/FLOAT columns with `printf('%.12g', ...)` — 12
// significant digits. The synthetic names let us rename both the MV and the base-
// query results into a common column-name space so EXCEPT ALL can compare by
// position — the base query's output names (e.g. `count(d.D_ID)`) don't match the
// MV's sanitized names (`count_d_d_id`), but positions always agree.
//
// Why %.12g (significant digits) instead of ROUND(x, N) (decimal places):
//   DuckDB's internal AVG / VARIANCE compensation can drift 1 ULP. DOUBLE ULP is
//   ~2^-52 of the magnitude — absolute 1e-15 for values near 1, absolute 1e-9 for
//   values near 1e7, etc. A fixed decimal-place rounding fails for large values
//   (e.g. VARIANCE ≈ 1.76e7 drifts by 4e-9, outside ROUND(x,10)'s 1e-10 window).
//   12 significant digits leaves ~3–4 orders of magnitude headroom relative to
//   DOUBLE's ~16 significant digits regardless of magnitude, while still catching
//   any algebraic error larger than 1e-12 relative.
//
// Non-float types (INTEGER, BIGINT, DECIMAL, VARCHAR, DATE, TIMESTAMP, BOOLEAN,
// LIST, STRUCT, MAP, ARRAY) pass through verbatim so algebraic errors still surface.
//
// If schema lookup fails, column_list is empty and normalized is "*" — caller
// falls back to the strict SELECT * compare.
struct NormalizedVerify {
	string column_list;     // e.g. "c1, c2, c3" — used in WITH cte(<column_list>) AS ...
	string normalized;      // e.g. "c1, ROUND(c2, 10) AS c2, c3"
	bool has_float = false; // any DOUBLE/FLOAT/REAL column present — if false, fall back
	                        // to the strict `SELECT *` verify (avoids reshaping SQL for
	                        // queries that don't need tolerance, e.g. NTILE/ROW_NUMBER
	                        // whose output can subtly shift under CTE materialization).
};

static NormalizedVerify BuildNormalizedVerify(duckdb::Connection &con, const string &rel_name) {
	NormalizedVerify result;
	// Escape single quotes in relation name for the PRAGMA string literal.
	string escaped;
	for (char c : rel_name) {
		escaped += c;
		if (c == '\'') escaped += '\'';
	}
	auto info = con.Query("PRAGMA table_info('" + escaped + "')");
	if (!info || info->HasError() || info->RowCount() == 0) {
		result.normalized = "*";
		return result;
	}
	for (duckdb::idx_t r = 0; r < info->RowCount(); r++) {
		string type = info->GetValue(2, r).ToString();
		string cname = "c" + std::to_string(r);
		if (!result.column_list.empty()) result.column_list += ", ";
		result.column_list += cname;
		if (!result.normalized.empty()) result.normalized += ", ";
		if (type == "DOUBLE" || type == "FLOAT" || type == "REAL") {
			// Format with 12 significant digits (relative precision, not absolute).
			// NULLs stay as NULL — DuckDB's printf('%.12g', NULL) returns NULL, and
			// EXCEPT ALL treats NULL-rows as equal on both sides.
			result.normalized += "printf('%.12g', " + cname + ") AS " + cname;
			result.has_float = true;
		} else {
			result.normalized += cname;
		}
	}
	return result;
}

// Parse is_incremental from the query file's first-line metadata comment.
// Returns 1 if metadata says incremental, 0 if FULL_REFRESH, -1 if missing/unparseable.
static int ParseIsIncrementalFromQueryFile(const string &query_sql) {
	// Look at the first line for "-- {...}" and find "is_incremental": true|false
	size_t nl = query_sql.find('\n');
	string first_line = (nl == string::npos) ? query_sql : query_sql.substr(0, nl);
	if (first_line.rfind("-- ", 0) != 0) return -1;
	size_t pos = first_line.find("\"is_incremental\"");
	if (pos == string::npos) return -1;
	pos = first_line.find(':', pos);
	if (pos == string::npos) return -1;
	// Skip whitespace
	while (pos + 1 < first_line.size() && (first_line[pos + 1] == ' ' || first_line[pos + 1] == '\t')) pos++;
	if (first_line.compare(pos + 1, 4, "true") == 0) return 1;
	if (first_line.compare(pos + 1, 5, "false") == 0) return 0;
	return -1;
}

static string ReadFileToString(const string &path) {
	std::ifstream in(path);
	if (!in.is_open()) { return string(); }
	std::ostringstream ss;
	ss << in.rdbuf();
	return ss.str();
}

static vector<string> CollectQueryFiles(const string &dir) {
	vector<string> files;
	DIR *d = opendir(dir.c_str());
	if (!d) { return files; }
	struct dirent *entry;
	while ((entry = readdir(d)) != nullptr) {
		string name = entry->d_name;
		if (name.size() > 4 && name.substr(name.size() - 4) == ".sql") {
			files.push_back(dir + "/" + name);
		}
	}
	closedir(d);
	// Sort: native `query_*` first, DuckLake `ducklake_*` last. DuckLake queries
	// can trigger INTERNAL errors that invalidate the DB and force the parent to
	// delete the WAL. If DuckLake runs first (alphabetical), that WAL deletion
	// wipes the native TPC-C tables before any native query runs. Reverse order
	// so native runs on a clean DB, then DuckLake runs against the same DB.
	std::sort(files.begin(), files.end(), [](const string &a, const string &b) {
		auto basename = [](const string &p) {
			auto pos = p.find_last_of('/');
			return pos != string::npos ? p.substr(pos + 1) : p;
		};
		string ba = basename(a), bb = basename(b);
		bool a_dl = ba.rfind("ducklake_", 0) == 0;
		bool b_dl = bb.rfind("ducklake_", 0) == 0;
		if (a_dl != b_dl) return !a_dl; // native (non-ducklake) first
		return ba < bb;
	});
	return files;
}

static string QueryName(const string &path) {
	auto pos = path.find_last_of('/');
	string fname = (pos != string::npos) ? path.substr(pos + 1) : path;
	if (fname.size() > 4) { fname = fname.substr(0, fname.size() - 4); }
	return fname;
}

static string FindQueriesDir() {
	vector<string> candidates = {
		"benchmark/queries",
		"queries",
		"../benchmark/queries",
		"../../benchmark/queries",
		"../../../benchmark/queries",
	};

	for (auto &c : candidates) {
		if (FileExists(c)) {
			return c;
		}
	}

	// Try relative to executable
	char exe_path[PATH_MAX];
	ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
	if (len != -1) {
		exe_path[len] = '\0';
		string dir = string(exe_path);
		auto pos = dir.find_last_of('/');
		if (pos != string::npos) {
			dir = dir.substr(0, pos);
		}
		for (int i = 0; i < 6; ++i) {
			string cand = dir + "/benchmark/queries";
			if (FileExists(cand)) {
				return cand;
			}
			auto p2 = dir.find_last_of('/');
			if (p2 == string::npos) break;
			dir = dir.substr(0, p2);
		}
	}

	return "";
}

static string FormatQueryList(const vector<string> &names, size_t max_show = 10) {
	string out;
	for (size_t i = 0; i < names.size(); ++i) {
		if (i > 0) out += ", ";
		if (i >= max_show) {
			out += "... +" + std::to_string(names.size() - i) + " more";
			break;
		}
		out += names[i];
	}
	return out;
}

// ===== Stats Struct =====

struct RewriterStats {
	int validation_ok = 0, validation_fail = 0;
	int mv_creation_ok = 0, mv_creation_fail = 0;
	int incremental = 0, full_refresh = 0;
	int refresh_ok = 0, refresh_fail = 0;
	int correct = 0, incorrect = 0;
	int crashed = 0;
	double total_mv_ms = 0, total_refresh_ms = 0, total_verify_ms = 0;
	std::map<string, int> error_counts;
	std::map<string, vector<string>> error_queries;
	vector<string> crash_queries;
	vector<string> incorrect_queries;

	// Metadata-vs-OpenIVM confusion matrix (only counted among mv_creation_ok).
	// meta_X_actual_Y: metadata says X, OpenIVM actually did Y.
	int meta_true_actual_true = 0;   // correctly predicted incremental
	int meta_true_actual_false = 0;  // predicted incremental but OpenIVM fell back to FULL_REFRESH
	int meta_false_actual_true = 0;  // predicted FULL_REFRESH but OpenIVM handled incrementally (surprising)
	int meta_false_actual_false = 0; // correctly predicted FULL_REFRESH
	int meta_missing = 0;            // query file had no is_incremental metadata
	vector<string> mismatch_true_actual_false_queries;
	vector<string> mismatch_false_actual_true_queries;
};

static void PrintErrorBreakdown(const RewriterStats &stats, int total) {
	if (stats.error_counts.empty()) return;

	int total_errors = 0;
	for (const auto &e : stats.error_counts) {
		total_errors += e.second;
	}

	vector<std::pair<string, int>> sorted_errors(stats.error_counts.begin(), stats.error_counts.end());
	std::sort(sorted_errors.begin(), sorted_errors.end(),
	          [](const std::pair<string, int> &a, const std::pair<string, int> &b) {
		          return a.second > b.second;
	          });

	Log("");
	Log("=== Error Breakdown (" + std::to_string(total_errors) + " total errors across " + std::to_string(total) + " queries) ===");
	for (const auto &e : sorted_errors) {
		double pct = (total > 0) ? (100.0 * e.second / total) : 0.0;
		string msg = e.first;
		if (msg.size() > 80) {
			msg = msg.substr(0, 80) + "...";
		}
		Log("  " + std::to_string(e.second) + "x (" + FormatNumber(pct) + "%) " + msg);

		// Show which queries had this error (first 10)
		auto it = stats.error_queries.find(e.first);
		if (it != stats.error_queries.end() && !it->second.empty()) {
			Log("    queries: " + FormatQueryList(it->second, 10));
		}
	}
}

// ===== ForkWorker (identical to sqlstorm pattern) =====

static bool IsFatalError(const string &error) {
	return error.find("FATAL") != string::npos ||
	       error.find("database has been invalidated") != string::npos;
}

static string TruncateError(const string &error) {
	string msg = error;
	// Strip stack traces
	auto st = msg.find("\n\nStack Trace");
	if (st != string::npos) msg = msg.substr(0, st);
	// Also strip JSON stack traces
	auto js = msg.find("\"stack_trace");
	if (js != string::npos) msg = msg.substr(0, js);
	// Cap length
	if (msg.size() > 200) msg = msg.substr(0, 200) + "...";
	return msg;
}

static void ChildWorkerMain(int read_fd, int write_fd, const string &db_path, const vector<string> &deltas) {
	try {
		duckdb::DuckDB db(db_path);
		duckdb::Connection con(db);
		con.Query("PRAGMA threads=4");

		auto load_result = con.Query("LOAD openivm");
		if (load_result && load_result->HasError()) {
			string err = "LOAD openivm failed: " + load_result->GetError();
			fprintf(stderr, "%s\n", err.c_str());
			_exit(2);
		}

		// Figure out the default (native) catalog name so we can switch back
		// after a ducklake query. When `db_path` is a file like
		// `rewriter_benchmark_sf1.db`, DuckDB names the catalog
		// `rewriter_benchmark_sf1` — not `memory`.
		string native_catalog = "memory";
		{
			auto cur = con.Query("SELECT current_database()");
			if (cur && !cur->HasError() && cur->RowCount() > 0) {
				native_catalog = cur->GetValue(0, 0).ToString();
			}
		}
		string native_use = "USE " + native_catalog + ".main";

		// Attach a DuckLake catalog alongside native tables. Queries whose filename
		// starts with `ducklake_` will be run after `USE dl.main` so their
		// unqualified table references resolve to the DuckLake copy of TPC-C.
		// Fully-qualified `dl.<table>` references also work.
		bool ducklake_ok = false;
		{
			string dl_path = db_path + ".ducklake.db";
			auto inst = con.Query("INSTALL ducklake");
			auto ld = con.Query("LOAD ducklake");
			if ((!inst || !inst->HasError()) && (!ld || !ld->HasError())) {
				string attach_sql = "ATTACH IF NOT EXISTS '" + dl_path + "' AS dl (TYPE ducklake)";
				auto at = con.Query(attach_sql);
				if (at && !at->HasError()) {
					ducklake_ok = true;
					// Populate TPC-C tables in the DuckLake catalog on first attach.
					// Use LOWER() to handle DuckLake's uppercase table names (e.g. 'WAREHOUSE').
					auto have = con.Query("SELECT COUNT(*) FROM information_schema.tables "
					                      "WHERE table_catalog = 'dl' AND LOWER(table_name) = 'warehouse'");
					bool needs_init =
					    !have || have->HasError() || have->RowCount() == 0 || have->GetValue(0, 0).GetValue<int64_t>() == 0;
					if (needs_init) {
						con.Query("USE dl.main");
						CreateTPCCSchema(con);
						// Copy data from the native catalog (captured above into
						// `native_catalog`) into DuckLake so both start identical.
						for (const char *t : {"WAREHOUSE", "DISTRICT", "CUSTOMER", "ITEM", "STOCK", "OORDER",
						                      "NEW_ORDER", "ORDER_LINE", "HISTORY"}) {
							con.Query(string("INSERT INTO ") + t + " SELECT * FROM " + native_catalog + ".main." + t);
						}
						con.Query(native_use);
					} else {
						// Ensure we're always in the native catalog context after DuckLake check.
						con.Query(native_use);
					}
				}
			}
		}

		// Clean up any leftover mv_q* views and orphaned data tables from a previous crashed child.
		// DuckLake MVs (created with USE dl.main) land in dl.main.*, so we use catalog-qualified
		// drops via information_schema which spans all catalogs.
		{
			// Drop views from all catalogs
			auto leftover = con.Query("SELECT table_catalog, table_name FROM information_schema.views "
			                          "WHERE table_name LIKE 'mv_q%' AND table_schema = 'main'");
			if (leftover && !leftover->HasError()) {
				for (idx_t r = 0; r < leftover->RowCount(); r++) {
					string cat = leftover->GetValue(0, r).ToString();
					string vn = leftover->GetValue(1, r).ToString();
					con.Query("DROP VIEW IF EXISTS " + cat + ".main." + vn);
				}
			}
			// Drop orphaned data/delta tables from all catalogs
			auto orphaned =
			    con.Query("SELECT table_catalog, table_name FROM information_schema.tables "
			              "WHERE (table_name LIKE '_ivm_data_mv_q%' OR table_name LIKE 'delta_mv_q%'"
			              "    OR table_name LIKE 'ivm_delta_mv_q%') AND table_schema = 'main'");
			if (orphaned && !orphaned->HasError()) {
				for (idx_t r = 0; r < orphaned->RowCount(); r++) {
					string cat = orphaned->GetValue(0, r).ToString();
					string tn = orphaned->GetValue(1, r).ToString();
					con.Query("DROP TABLE IF EXISTS " + cat + ".main." + tn);
				}
			}
			// Clean metadata tables (always in native catalog, unqualified)
			con.Query("DELETE FROM _duckdb_ivm_views WHERE view_name LIKE 'mv_q%'");
			con.Query("DELETE FROM _duckdb_ivm_delta_tables WHERE view_name LIKE 'mv_q%'");
		}

		int delta_idx = 0;
		int delta_batch_size = 10;
		int query_counter = 0;

		while (true) {
			uint32_t query_len = 0;
			if (!ReadAllBytes(read_fd, &query_len, sizeof(query_len))) break;
			if (query_len == 0) break;

			string query(query_len, '\0');
			if (!ReadAllBytes(read_fd, &query[0], query_len)) break;

			// First line is `-- <query_name>` (injected by the parent via Submit)
			// so the child knows which query it's processing. We use the name to
			// decide catalog context: filenames starting with `ducklake_` run
			// against the DuckLake copy of TPC-C; everything else runs against
			// native tables. Subsequent `-- …` lines (e.g. the `{JSON metadata}`
			// header shipped inside each query file) are stripped next.
			string worker_query_name;
			bool is_ducklake_query = false;
			{
				auto newline_pos = query.find('\n');
				if (newline_pos != string::npos && query.size() >= 2 && query[0] == '-' && query[1] == '-') {
					string header = query.substr(0, newline_pos);
					if (header.size() > 3) {
						worker_query_name = header.substr(3);
						while (!worker_query_name.empty() && (worker_query_name.back() == ' ' ||
						                                       worker_query_name.back() == '\r')) {
							worker_query_name.pop_back();
						}
						is_ducklake_query = worker_query_name.find("ducklake_") != string::npos && ducklake_ok;
					}
					query = query.substr(newline_pos + 1);
				}
			}
			// Strip any remaining leading `-- …` comment lines (JSON metadata, etc.).
			while (query.size() >= 2 && query[0] == '-' && query[1] == '-') {
				auto nl = query.find('\n');
				if (nl == string::npos) {
					query.clear();
					break;
				}
				query = query.substr(nl + 1);
			}

			// Trim leading/trailing whitespace, newlines, and semicolons for composable SQL
			while (!query.empty() && (query[0] == '\n' || query[0] == '\r' || query[0] == ' ')) {
				query = query.substr(1);
			}
			while (!query.empty() && (query.back() == ';' || query.back() == '\n' || query.back() == '\r' || query.back() == ' ')) {
				query.pop_back();
			}

			query_counter++;
			string mv_name = "mv_q" + std::to_string(query_counter);

			// Switch default catalog before anything else. The MV, its data
			// table, delta view, and any deltas applied below all target the
			// catalog active at execution time.
			if (is_ducklake_query) {
				con.Query("USE dl.main");
			} else {
				con.Query(native_use);
			}

			uint8_t phase_reached = 0;  // 1=duckdb_fail, 2=mv_fail, 3=delta_fail, 4=refresh_fail, 5=verify_fail, 6=ok, 99=crash
			uint8_t is_incremental = 0;
			uint8_t is_correct = 0;
			double time_select_ms = 0, time_mv_ms = 0, time_refresh_ms = 0, time_verify_ms = 0;
			string error;

			// Phase 1: DuckDB validation
			auto start = std::chrono::steady_clock::now();
			try {
				auto test_query = "SELECT * FROM (" + query + ") __q LIMIT 0";
				auto test_result = con.Query(test_query);
				if (test_result && test_result->HasError()) {
					error = test_result->GetError();
					phase_reached = IsFatalError(error) ? 99 : 1;
				} else {
					auto end = std::chrono::steady_clock::now();
					time_select_ms = std::chrono::duration<double, std::milli>(end - start).count();
					phase_reached = 2;
				}
			} catch (std::exception &e) {
				error = e.what();
				phase_reached = IsFatalError(error) ? 99 : 1;
			} catch (...) {
				error = "unknown exception";
				phase_reached = 1;
			}

			if (phase_reached == 2) {
				// Phase 2: MV creation
				start = std::chrono::steady_clock::now();
				try {
					string create_mv = "CREATE MATERIALIZED VIEW " + mv_name + " AS " + query;
					auto mv_result = con.Query(create_mv);
					if (mv_result && mv_result->HasError()) {
						error = mv_result->GetError();
						if (IsFatalError(error)) {
							phase_reached = 99;
						} else {
							phase_reached = 2;
						}
					} else {
						auto end = std::chrono::steady_clock::now();
						time_mv_ms = std::chrono::duration<double, std::milli>(end - start).count();
						phase_reached = 3;
					}

					if (phase_reached == 3) {
						// Phase 2b: Check incrementability (type=3 is FULL_REFRESH, rest are incremental)
						auto check_result = con.Query("SELECT type FROM _duckdb_ivm_views WHERE view_name = '" + mv_name + "'");
						if (check_result && !check_result->HasError() && check_result->RowCount() > 0) {
							int64_t ivm_type = check_result->GetValue(0, 0).GetValue<int64_t>();
							is_incremental = (ivm_type != 3) ? 1 : 0;
						}

						// Phase 3: Apply deltas
						for (int d = 0; d < delta_batch_size && (size_t)delta_idx < deltas.size(); d++, delta_idx++) {
							con.Query(deltas[delta_idx]);  // Errors are OK (e.g. duplicate keys)
						}
						if ((size_t)delta_idx >= deltas.size()) delta_idx = 0;
						phase_reached = 4;

						// Phase 4: PRAGMA ivm()
						start = std::chrono::steady_clock::now();
						auto refresh_result = con.Query("PRAGMA ivm('" + mv_name + "')");
						if (refresh_result && refresh_result->HasError()) {
							error = refresh_result->GetError();
							if (IsFatalError(error)) {
								phase_reached = 99;
							} else {
								phase_reached = 4;
							}
						} else {
							auto end_refresh = std::chrono::steady_clock::now();
							time_refresh_ms = std::chrono::duration<double, std::milli>(end_refresh - start).count();
							phase_reached = 5;

							// Phase 5: EXCEPT ALL verification (wrap in subqueries to handle ORDER BY/CTEs).
							// Rename both sides to synthetic column names (c0, c1, ...) so EXCEPT ALL
							// compares by position — the MV's sanitized column names (e.g. count_d_d_id)
							// don't match the base query's unsanitized output names (count(d.D_ID)).
							// Round DOUBLE/FLOAT columns to 10 decimals to tolerate ULP drift on
							// AVG-over-DECIMAL MVs (DuckDB's native AVG uses compensated arithmetic
							// we can't reproduce via SUM/COUNT). See docs/limitations.md.
							start = std::chrono::steady_clock::now();
							auto nv = BuildNormalizedVerify(con, mv_name);
							string verify_query;
							if (nv.column_list.empty() || !nv.has_float) {
								// Strict compare: no DOUBLE/FLOAT columns (so tolerance is
								// unnecessary) or schema lookup failed. Keeps the SQL byte-
								// identical to the historical verify so queries whose output
								// is order-sensitive under CTE materialization (NTILE, etc.)
								// aren't perturbed.
								verify_query = "SELECT COUNT(*) FROM ("
									"SELECT * FROM " + mv_name + " EXCEPT ALL SELECT * FROM (" + query + ") __a"
									" UNION ALL "
									"SELECT * FROM (" + query + ") __b EXCEPT ALL SELECT * FROM " + mv_name +
									") __diff";
							} else {
								verify_query =
									"WITH mv_r(" + nv.column_list + ") AS (SELECT * FROM " + mv_name + "), "
									"gt_r(" + nv.column_list + ") AS (SELECT * FROM (" + query + ") __gt) "
									"SELECT COUNT(*) FROM ("
									"SELECT " + nv.normalized + " FROM mv_r "
									"EXCEPT ALL "
									"SELECT " + nv.normalized + " FROM gt_r "
									"UNION ALL "
									"SELECT " + nv.normalized + " FROM gt_r "
									"EXCEPT ALL "
									"SELECT " + nv.normalized + " FROM mv_r"
									") __diff";
							}
							auto verify_result = con.Query(verify_query);
							if (verify_result && !verify_result->HasError() && verify_result->RowCount() > 0) {
								int64_t mismatch_count = verify_result->GetValue(0, 0).GetValue<int64_t>();
								is_correct = (mismatch_count == 0) ? 1 : 0;
								if (!is_correct && getenv("OPENIVM_DUMP_DIFF")) {
									// Dump the diff rows to stderr for post-mortem analysis.
									// Use two separate queries to avoid UNION ALL column-name alignment issues
									// when mv and gt have mismatched output schemas.
									fprintf(stderr, "[DIFF %s] mismatch_count=%lld\n", mv_name.c_str(),
									        static_cast<long long>(mismatch_count));
									string mv_only_q, gt_only_q;
									if (nv.column_list.empty() || !nv.has_float) {
										mv_only_q = "SELECT * FROM " + mv_name + " EXCEPT ALL SELECT * FROM (" +
										            query + ") __a LIMIT 10";
										gt_only_q = "SELECT * FROM (" + query + ") __b EXCEPT ALL SELECT * FROM " +
										            mv_name + " LIMIT 10";
									} else {
										string ctes = "WITH mv_r(" + nv.column_list + ") AS (SELECT * FROM " + mv_name +
										              "), gt_r(" + nv.column_list + ") AS (SELECT * FROM (" + query +
										              ") __gt) ";
										mv_only_q = ctes + "SELECT " + nv.normalized + " FROM mv_r EXCEPT ALL SELECT " +
										            nv.normalized + " FROM gt_r LIMIT 10";
										gt_only_q = ctes + "SELECT " + nv.normalized + " FROM gt_r EXCEPT ALL SELECT " +
										            nv.normalized + " FROM mv_r LIMIT 10";
									}
									auto dump_side = [&](const string &label, const string &q) {
										auto rows = con.Query(q);
										if (!rows || rows->HasError()) {
											fprintf(stderr, "[DIFF %s]   %s: ERROR %s\n", mv_name.c_str(), label.c_str(),
											        rows ? rows->GetError().c_str() : "(null)");
											return;
										}
										if (rows->RowCount() == 0) {
											return;
										}
										for (idx_t r = 0; r < rows->RowCount(); r++) {
											string row;
											for (idx_t c = 0; c < rows->ColumnCount(); c++) {
												if (c > 0) row += " | ";
												row += rows->GetValue(c, r).ToString();
											}
											fprintf(stderr, "[DIFF %s]   %s: %s\n", mv_name.c_str(), label.c_str(),
											        row.c_str());
										}
									};
									dump_side("mv_only", mv_only_q);
									dump_side("gt_only", gt_only_q);
									fflush(stderr);
								}
							} else if (verify_result && verify_result->HasError()) {
								error = verify_result->GetError();
							}
							auto end_verify = std::chrono::steady_clock::now();
							time_verify_ms = std::chrono::duration<double, std::milli>(end_verify - start).count();
							phase_reached = (is_correct ? 6 : 5);
						}
					}

					// Cleanup — use DROP VIEW (not DROP MATERIALIZED VIEW; DuckDB intercepts
					// that before OpenIVM, so it silently fails to clean metadata).
					con.Query("DROP VIEW IF EXISTS " + mv_name);

				} catch (std::exception &e) {
					error = e.what();
					if (IsFatalError(error)) {
						phase_reached = 99;
					} else if (phase_reached < 2) {
						phase_reached = 2;
					}
				} catch (...) {
					error = "mv creation failed";
					phase_reached = 2;
				}
			}

			// Truncate error before sending
			error = TruncateError(error);

			// Send result back
			WriteAllBytes(write_fd, &phase_reached, sizeof(phase_reached));
			WriteAllBytes(write_fd, &is_incremental, sizeof(is_incremental));
			WriteAllBytes(write_fd, &is_correct, sizeof(is_correct));
			WriteAllBytes(write_fd, &time_select_ms, sizeof(time_select_ms));
			WriteAllBytes(write_fd, &time_mv_ms, sizeof(time_mv_ms));
			WriteAllBytes(write_fd, &time_refresh_ms, sizeof(time_refresh_ms));
			WriteAllBytes(write_fd, &time_verify_ms, sizeof(time_verify_ms));
			uint32_t err_len = static_cast<uint32_t>(error.size());
			WriteAllBytes(write_fd, &err_len, sizeof(err_len));
			if (err_len > 0) WriteAllBytes(write_fd, error.data(), err_len);

			if (phase_reached == 99) { _exit(1); }
		}
	} catch (std::exception &e) {
		fprintf(stderr, "Child fatal: %s\n", e.what());
		_exit(2);
	} catch (...) {
		fprintf(stderr, "Child fatal: unknown exception\n");
		_exit(2);
	}
	_exit(0);
}

struct ForkWorker {
	pid_t child_pid = -1;
	int to_child_fd = -1, from_child_fd = -1;
	string db_path;
	vector<string> deltas;

	uint8_t result_phase = 0;
	uint8_t result_incremental = 0;
	uint8_t result_correct = 0;
	double result_time_select_ms = 0, result_time_mv_ms = 0, result_time_refresh_ms = 0, result_time_verify_ms = 0;
	string result_error;

	void Start() {
		Stop();
		int to_child[2], from_child[2];
		if (pipe(to_child) != 0 || pipe(from_child) != 0) throw std::runtime_error("pipe() failed");

		child_pid = fork();
		if (child_pid < 0) {
			close(to_child[0]); close(to_child[1]);
			close(from_child[0]); close(from_child[1]);
			throw std::runtime_error("fork() failed");
		}

		if (child_pid == 0) {
			close(to_child[1]); close(from_child[0]);
			ChildWorkerMain(to_child[0], from_child[1], db_path, deltas);
			_exit(0);
		}

		close(to_child[0]); close(from_child[1]);
		to_child_fd = to_child[1];
		from_child_fd = from_child[0];
	}

	void Stop() {
		if (child_pid > 0) {
			uint32_t zero = 0;
			WriteAllBytes(to_child_fd, &zero, sizeof(zero));
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
		if (to_child_fd >= 0) { close(to_child_fd); to_child_fd = -1; }
		if (from_child_fd >= 0) { close(from_child_fd); from_child_fd = -1; }
	}

	void Submit(const string &query, double timeout_s, const string &query_name = "") {
		result_phase = 0;
		result_incremental = 0;
		result_correct = 0;
		result_error.clear();

		if (child_pid <= 0) return;

		// Prepend `-- <query_name>\n` so the worker can recognize filename
		// prefixes (e.g. `ducklake_`) and dispatch to the right catalog.
		string framed = query_name.empty() ? query : ("-- " + query_name + "\n" + query);
		uint32_t query_len = static_cast<uint32_t>(framed.size());
		if (!WriteAllBytes(to_child_fd, &query_len, sizeof(query_len)) ||
		    !WriteAllBytes(to_child_fd, framed.data(), query_len)) {
			int status;
			waitpid(child_pid, &status, 0);
			child_pid = -1;
			result_phase = 99;
			return;
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
				result_phase = 2;  // timeout
				result_error = "timeout";
				return;
			}

			int remaining_ms = static_cast<int>(
				std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
			int poll_ms = std::min(remaining_ms, 500);

			struct pollfd pfd;
			pfd.fd = from_child_fd;
			pfd.events = POLLIN;
			pfd.revents = 0;

			int ret = poll(&pfd, 1, poll_ms);

			if (ret > 0 && (pfd.revents & POLLIN)) {
				if (!ReadAllBytes(from_child_fd, &result_phase, sizeof(result_phase)) ||
				    !ReadAllBytes(from_child_fd, &result_incremental, sizeof(result_incremental)) ||
				    !ReadAllBytes(from_child_fd, &result_correct, sizeof(result_correct)) ||
				    !ReadAllBytes(from_child_fd, &result_time_select_ms, sizeof(result_time_select_ms)) ||
				    !ReadAllBytes(from_child_fd, &result_time_mv_ms, sizeof(result_time_mv_ms)) ||
				    !ReadAllBytes(from_child_fd, &result_time_refresh_ms, sizeof(result_time_refresh_ms)) ||
				    !ReadAllBytes(from_child_fd, &result_time_verify_ms, sizeof(result_time_verify_ms))) {
					int status;
					waitpid(child_pid, &status, 0);
					child_pid = -1;
					result_phase = 99;
					return;
				}
				uint32_t err_len = 0;
				if (!ReadAllBytes(from_child_fd, &err_len, sizeof(err_len))) {
					int status;
					waitpid(child_pid, &status, 0);
					child_pid = -1;
					result_phase = 99;
					return;
				}
				if (err_len > 0) {
					result_error.resize(err_len);
					if (!ReadAllBytes(from_child_fd, &result_error[0], err_len)) {
						int status;
						waitpid(child_pid, &status, 0);
						child_pid = -1;
						result_phase = 99;
						return;
					}
				}

				if (result_phase == 99) {
					int status;
					waitpid(child_pid, &status, 0);
					child_pid = -1;
				}
				return;
			}

			if (ret > 0 && (pfd.revents & (POLLHUP | POLLERR))) {
				int status;
				waitpid(child_pid, &status, 0);
				child_pid = -1;
				result_phase = 99;
				if (WIFEXITED(status)) {
					result_error = "child exited with code " + std::to_string(WEXITSTATUS(status));
				} else if (WIFSIGNALED(status)) {
					result_error = "child killed by signal " + std::to_string(WTERMSIG(status));
				} else {
					result_error = "child died unexpectedly";
				}
				return;
			}

			int status;
			int w = waitpid(child_pid, &status, WNOHANG);
			if (w > 0) {
				child_pid = -1;
				result_phase = 99;
				if (WIFEXITED(status)) {
					result_error = "child exited with code " + std::to_string(WEXITSTATUS(status));
				} else if (WIFSIGNALED(status)) {
					result_error = "child killed by signal " + std::to_string(WTERMSIG(status));
				} else {
					result_error = "child died";
				}
				return;
			}
		}
	}

	~ForkWorker() { Stop(); }
};

// ===== Main Benchmark Logic =====

static void PrintIncrementalityMatrix(const RewriterStats &stats) {
	// Confusion matrix: metadata prediction vs OpenIVM's actual classification
	int mt_at = stats.meta_true_actual_true;
	int mt_af = stats.meta_true_actual_false;
	int mf_at = stats.meta_false_actual_true;
	int mf_af = stats.meta_false_actual_false;
	int total = mt_at + mt_af + mf_at + mf_af;
	int correct = mt_at + mf_af;
	int wrong = mt_af + mf_at;

	Log("");
	Log("=== Metadata vs OpenIVM — Incrementalizable Classification ===");
	Log("(over " + std::to_string(total) + " queries where MV creation succeeded)");
	Log("");
	Log("                          | OpenIVM:  INCREMENTAL  |  OpenIVM:  FULL_REFRESH |");
	Log("  ------------------------+------------------------+-------------------------+");
	Log("  Metadata: INCREMENTAL   |  correct:   " + std::to_string(mt_at) + string(10 - std::min<size_t>(10, std::to_string(mt_at).size()), ' ') +
	    "|  mismatch:  " + std::to_string(mt_af) + string(11 - std::min<size_t>(11, std::to_string(mt_af).size()), ' ') + "|");
	Log("  Metadata: FULL_REFRESH  |  mismatch:  " + std::to_string(mf_at) + string(10 - std::min<size_t>(10, std::to_string(mf_at).size()), ' ') +
	    "|  correct:   " + std::to_string(mf_af) + string(11 - std::min<size_t>(11, std::to_string(mf_af).size()), ' ') + "|");
	Log("");
	if (total > 0) {
		Log("  Correctly classified by metadata: " + std::to_string(correct) + " (" +
		    FormatNumber(100.0 * correct / total) + "%)");
		Log("  Mismatched (metadata wrong):      " + std::to_string(wrong) + " (" +
		    FormatNumber(100.0 * wrong / total) + "%)");
	}
	if (stats.meta_missing > 0) {
		Log("  Queries without is_incremental metadata: " + std::to_string(stats.meta_missing));
	}

	if (!stats.mismatch_true_actual_false_queries.empty()) {
		Log("");
		Log("  Metadata=true but OpenIVM=FULL_REFRESH (metadata too optimistic):");
		Log("    " + FormatQueryList(stats.mismatch_true_actual_false_queries, 20));
	}
	if (!stats.mismatch_false_actual_true_queries.empty()) {
		Log("");
		Log("  Metadata=false but OpenIVM=incremental (metadata too pessimistic):");
		Log("    " + FormatQueryList(stats.mismatch_false_actual_true_queries, 20));
	}
}

static void PrintStats(const string &label, const RewriterStats &stats, int total) {
	Log("--- " + label + " ---");
	Log("  Total:   " + std::to_string(total));
	Log("  Validation OK:   " + std::to_string(stats.validation_ok) + " (" + FormatNumber(100.0 * stats.validation_ok / total) + "%)");
	Log("  MV Creation OK:   " + std::to_string(stats.mv_creation_ok) + " (" + FormatNumber(100.0 * stats.mv_creation_ok / total) + "%)");
	Log("  Incremental:  " + std::to_string(stats.incremental) + " (" + FormatNumber(100.0 * stats.incremental / std::max(1, stats.mv_creation_ok)) + "% of created)");
	Log("  Full refresh: " + std::to_string(stats.full_refresh) + " (" + FormatNumber(100.0 * stats.full_refresh / std::max(1, stats.mv_creation_ok)) + "% of created)");
	Log("  Refresh OK:   " + std::to_string(stats.refresh_ok) + " (" + FormatNumber(100.0 * stats.refresh_ok / total) + "%)");
	Log("  Correct:      " + std::to_string(stats.correct) + " (" + FormatNumber(100.0 * stats.correct / std::max(1, stats.refresh_ok)) + "% of refreshed)");
	Log("  Crashed:      " + std::to_string(stats.crashed) + " (" + FormatNumber(100.0 * stats.crashed / total) + "%)");
	if (stats.mv_creation_ok > 0) {
		Log("  Avg MV time:      " + FormatNumber(stats.total_mv_ms / stats.mv_creation_ok) + " ms");
		Log("  Avg refresh time: " + FormatNumber(stats.total_refresh_ms / stats.refresh_ok) + " ms");
		Log("  Avg verify time:  " + FormatNumber(stats.total_verify_ms / stats.refresh_ok) + " ms");
	}
}

static vector<string> RunBenchmark(const string &queries_dir, const string &db_path, int scale_factor, double timeout_s) {
	vector<string> csv_lines;
	csv_lines.push_back("query_name,phase_reached,meta_is_incremental,actual_is_incremental,is_correct,time_select_ms,time_mv_ms,time_refresh_ms,time_verify_ms,error");

	if (!FileExists(db_path)) {
		Log("Creating TPC-C database: " + db_path);
		// Fork a child to create the DB so the file lock is released when it exits
		pid_t setup_pid = fork();
		if (setup_pid == 0) {
			{
				// Nested scope so DuckDB destructors run before _exit, ensuring WAL is flushed
				duckdb::DuckDB db(db_path);
				duckdb::Connection con(db);
				CreateTPCCSchema(con);
				InsertTPCCData(con, scale_factor);
				con.Query("PRAGMA checkpoint");
			}
			_exit(0);
		}
		int status;
		waitpid(setup_pid, &status, 0);
		if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) {
			Log("✗ Database creation failed");
			return csv_lines;
		}
		Log("✓ Database created");
	}

	vector<string> query_files = CollectQueryFiles(queries_dir);
	int total = static_cast<int>(query_files.size());
	Log("Running benchmark on " + std::to_string(total) + " queries...");

	RewriterStats stats;

	ForkWorker worker;
	worker.db_path = db_path;
	worker.deltas = GenerateDeltaPool(scale_factor);
	worker.Start();

	int log_interval = std::max(1, total / 10);
	string last_internal_error_query;

	for (int i = 0; i < total; i++) {
		if (worker.child_pid <= 0) { worker.Start(); }

		string query_file = query_files[i];
		string query_name = QueryName(query_file);
		string query_sql = ReadFileToString(query_file);

		if (query_sql.empty()) {
			stats.validation_fail++;
			csv_lines.push_back(query_name + ",1,0,0,0,0,0,0,,,empty file");
			continue;
		}

		// Parse metadata is_incremental prediction before running
		int meta_incremental = ParseIsIncrementalFromQueryFile(query_sql);

		worker.Submit(query_sql, timeout_s, query_name);

		uint8_t phase = worker.result_phase;
		string error_msg = worker.result_error;

		// If the child died (crash OR SIGKILL'd timeout), delete the WAL before the
		// next child opens the DB. A SIGKILL'd child never flushes its WAL, and
		// DuckDB's WAL replay can then fail with "Calling DatabaseManager::
		// GetDefaultDatabase with no default database set", killing every subsequent
		// query. Timeouts surface as phase=2 with error=="timeout"; actual crashes
		// as phase=99.
		bool child_died = (phase == 99) || (phase == 2 && error_msg == "timeout");
		if (child_died) {
			string wal_path = db_path + ".wal";
			if (FileExists(wal_path)) {
				std::remove(wal_path.c_str());
			}
		}

		// Handle crash: attribute to root-cause query if INTERNAL error preceded it
		if (phase == 99) {
			worker.Stop();

			if (!last_internal_error_query.empty()) {
				// This query is a victim — the crash was caused by the previous INTERNAL error
				Log("  " + query_name + " hit invalidated DB (root cause: " + last_internal_error_query + ")");
				stats.crashed++;
				string root_error = "INTERNAL error that invalidated the database (victim: " + query_name + ")";
				stats.error_counts[root_error]++;
				stats.error_queries[root_error].push_back(last_internal_error_query);
				stats.crash_queries.push_back(last_internal_error_query);
				last_internal_error_query.clear();
				// Retry victim query with fresh child
				i--;
				continue;
			}
			stats.crashed++;
			stats.crash_queries.push_back(query_name);
			string err = error_msg.empty() ? "child died unexpectedly" : error_msg;
			stats.error_counts[err]++;
			stats.error_queries[err].push_back(query_name);
		} else if (phase == 1) {
			stats.validation_fail++;
			stats.error_counts[error_msg]++;
			stats.error_queries[error_msg].push_back(query_name);
			last_internal_error_query.clear();
		} else if (phase == 2) {
			stats.validation_ok++;
			stats.mv_creation_fail++;
			stats.error_counts[error_msg]++;
			stats.error_queries[error_msg].push_back(query_name);
			// Track INTERNAL errors for root-cause attribution
			if (error_msg.find("INTERNAL") != string::npos) {
				last_internal_error_query = query_name;
			} else {
				last_internal_error_query.clear();
			}
		} else if (phase == 3 || phase == 4) {
			stats.validation_ok++;
			stats.mv_creation_ok++;
			if (worker.result_incremental) stats.incremental++;
			else stats.full_refresh++;
			stats.refresh_fail++;
			stats.error_counts[error_msg]++;
			stats.error_queries[error_msg].push_back(query_name);
			if (error_msg.find("INTERNAL") != string::npos) {
				last_internal_error_query = query_name;
			} else {
				last_internal_error_query.clear();
			}
		} else if (phase == 5) {
			stats.validation_ok++;
			stats.mv_creation_ok++;
			if (worker.result_incremental) stats.incremental++;
			else stats.full_refresh++;
			stats.refresh_ok++;
			stats.incorrect++;
			stats.incorrect_queries.push_back(query_name);
			stats.total_mv_ms += worker.result_time_mv_ms;
			stats.total_refresh_ms += worker.result_time_refresh_ms;
			stats.total_verify_ms += worker.result_time_verify_ms;
			if (error_msg.find("INTERNAL") != string::npos) {
				last_internal_error_query = query_name;
			} else {
				last_internal_error_query.clear();
			}
		} else if (phase == 6) {
			stats.validation_ok++;
			stats.mv_creation_ok++;
			if (worker.result_incremental) stats.incremental++;
			else stats.full_refresh++;
			stats.refresh_ok++;
			stats.correct++;
			stats.total_mv_ms += worker.result_time_mv_ms;
			stats.total_refresh_ms += worker.result_time_refresh_ms;
			stats.total_verify_ms += worker.result_time_verify_ms;
			last_internal_error_query.clear();
		}

		// Metadata-vs-OpenIVM confusion matrix: only count queries where MV creation succeeded
		// (so we have a real ivm_type to compare against).
		if (phase == 3 || phase == 4 || phase == 5 || phase == 6) {
			bool actual_incr = (worker.result_incremental != 0);
			if (meta_incremental < 0) {
				stats.meta_missing++;
			} else if (meta_incremental == 1 && actual_incr) {
				stats.meta_true_actual_true++;
			} else if (meta_incremental == 1 && !actual_incr) {
				stats.meta_true_actual_false++;
				stats.mismatch_true_actual_false_queries.push_back(query_name);
			} else if (meta_incremental == 0 && actual_incr) {
				stats.meta_false_actual_true++;
				stats.mismatch_false_actual_true_queries.push_back(query_name);
			} else {
				stats.meta_false_actual_false++;
			}
		}

		// Build CSV line — meta_is_incremental column is blank when metadata is missing
		string meta_str = (meta_incremental < 0) ? "" : std::to_string(meta_incremental);
		std::ostringstream csv_line;
		csv_line << query_name << "," << std::to_string(phase) << ","
		         << meta_str << "," << std::to_string(worker.result_incremental) << ","
		         << std::to_string(worker.result_correct) << ","
		         << FormatNumber(worker.result_time_select_ms) << "," << FormatNumber(worker.result_time_mv_ms) << ","
		         << FormatNumber(worker.result_time_refresh_ms) << "," << FormatNumber(worker.result_time_verify_ms) << ",\"" << error_msg << "\"";
		csv_lines.push_back(csv_line.str());

		if ((i + 1) % log_interval == 0 || i + 1 == total) {
			Log("[" + std::to_string(i + 1) + "/" + std::to_string(total) + "] val=" + std::to_string(stats.validation_ok) +
			    " mv=" + std::to_string(stats.mv_creation_ok) +
			    " incr=" + std::to_string(stats.incremental) +
			    " refresh=" + std::to_string(stats.refresh_ok) +
			    " correct=" + std::to_string(stats.correct) +
			    " crash=" + std::to_string(stats.crashed) +
			    " | meta[T/T=" + std::to_string(stats.meta_true_actual_true) +
			    " T/F=" + std::to_string(stats.meta_true_actual_false) +
			    " F/T=" + std::to_string(stats.meta_false_actual_true) +
			    " F/F=" + std::to_string(stats.meta_false_actual_false) + "]");
		}
	}

	worker.Stop();

	Log("");
	PrintStats("Rewriter Benchmark Results", stats, total);
	PrintIncrementalityMatrix(stats);
	PrintErrorBreakdown(stats, total);

	if (!stats.incorrect_queries.empty()) {
		Log("");
		Log("=== Incorrect Queries (" + std::to_string(stats.incorrect_queries.size()) + ") ===");
		Log("  " + FormatQueryList(stats.incorrect_queries, 20));
	}
	if (!stats.crash_queries.empty()) {
		Log("");
		Log("=== Crash Queries (" + std::to_string(stats.crash_queries.size()) + ") ===");
		Log("  " + FormatQueryList(stats.crash_queries, 20));
	}
	Log("");

	return csv_lines;
}

int main(int argc, char **argv) {
	signal(SIGPIPE, SIG_IGN);

	string queries_dir;
	string db_path;
	string out_file;
	int scale_factor = 3;
	double timeout_s = 30.0;

	for (int i = 1; i < argc; i++) {
		string arg = argv[i];
		if (arg == "--queries" && i + 1 < argc) {
			queries_dir = argv[++i];
		} else if (arg == "--db" && i + 1 < argc) {
			db_path = argv[++i];
		} else if (arg == "--out" && i + 1 < argc) {
			out_file = argv[++i];
		} else if (arg == "--scale" && i + 1 < argc) {
			scale_factor = std::stoi(argv[++i]);
		} else if (arg == "--timeout" && i + 1 < argc) {
			timeout_s = std::stod(argv[++i]);
		}
	}

	// Auto-search for queries directory if not specified
	if (queries_dir.empty()) {
		queries_dir = FindQueriesDir();
	}

	if (db_path.empty()) {
		db_path = "rewriter_benchmark_sf" + std::to_string(scale_factor) + ".db";
	}

	if (out_file.empty()) {
		out_file = "rewriter_benchmark_results.csv";
	}

	try {
		if (queries_dir.empty()) {
			Log("Error: --queries DIR not found or specified");
			return 1;
		}

		auto csv_lines = RunBenchmark(queries_dir, db_path, scale_factor, timeout_s);

		// Write CSV
		std::ofstream csv(out_file);
		for (const auto &line : csv_lines) {
			csv << line << "\n";
		}
		csv.close();
		Log("✓ Results written to " + out_file);

		return 0;
	} catch (std::exception &ex) {
		Log("Fatal error: " + string(ex.what()));
		return 2;
	}
}
