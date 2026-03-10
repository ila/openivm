#include "openivm_benchmark.hpp"

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"

#include <algorithm>
#include <chrono>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace duckdb {

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static string Timestamp() {
	auto now = std::chrono::system_clock::now();
	std::time_t t = std::chrono::system_clock::to_time_t(now);
	char buf[64];
	std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&t));
	return string(buf);
}

static void Log(const string &msg) {
	Printer::Print("[" + Timestamp() + "] " + msg);
}

static string FormatNumber(double v) {
	std::ostringstream oss;
	oss << std::setprecision(15) << std::defaultfloat << v;
	string s = oss.str();
	auto pos = s.find('.');
	if (pos != string::npos) {
		while (!s.empty() && s.back() == '0') {
			s.pop_back();
		}
		if (!s.empty() && s.back() == '.') {
			s.pop_back();
		}
	}
	return s;
}

static double Median(vector<double> v) {
	std::sort(v.begin(), v.end());
	size_t n = v.size();
	if (n % 2 == 1) {
		return v[n / 2];
	}
	return (v[n / 2 - 1] + v[n / 2]) / 2.0;
}

static string S(size_t n) {
	return std::to_string(n);
}

static void Exec(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw std::runtime_error("SQL error: " + result->GetError() + "\n  Query: " + sql);
	}
}

// ---------------------------------------------------------------------------
// Scenario definitions
// ---------------------------------------------------------------------------

struct ScenarioInfo {
	string name;
	string view_name;
	string view_query;
	int view_type; // 0=AGGREGATE_GROUP, 2=SIMPLE_PROJECTION
	vector<string> delta_table_names;
};

// Set up IVM metadata tables
static void SetupMetaTables(Connection &con) {
	Exec(con, "CREATE TABLE IF NOT EXISTS _duckdb_ivm_views "
	          "(view_name VARCHAR PRIMARY KEY, sql_string VARCHAR, type TINYINT, last_update TIMESTAMP)");
	Exec(con, "CREATE TABLE IF NOT EXISTS _duckdb_ivm_delta_tables "
	          "(view_name VARCHAR, table_name VARCHAR, last_update TIMESTAMP, "
	          "PRIMARY KEY(view_name, table_name))");
}

static ScenarioInfo SetupProjection(Connection &con, size_t base_rows, size_t delta_rows) {
	string vn = "mv_proj";
	string vq = "SELECT id, val FROM bench_proj";

	Exec(con, "CREATE TABLE bench_proj (id INT, val INT, data VARCHAR)");
	Exec(con, "INSERT INTO bench_proj SELECT i, (i * 7) % 1000, 'data_' || i::VARCHAR "
	          "FROM generate_series(1, " + S(base_rows) + ") gs(i)");
	Exec(con, "CREATE TABLE " + vn + " AS " + vq);
	Exec(con, "CREATE TABLE delta_" + vn + " AS SELECT *, true AS _duckdb_ivm_multiplicity FROM " + vn + " LIMIT 0");
	Exec(con, "CREATE TABLE delta_bench_proj AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_proj LIMIT 0");
	Exec(con, "INSERT INTO _duckdb_ivm_views VALUES ('" + vn + "', '" + vq + "', 2, now()::timestamp)");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_proj', '1970-01-01')");

	if (delta_rows > 0) {
		Exec(con, "INSERT INTO bench_proj SELECT i, (i * 7) % 1000, 'data_' || i::VARCHAR "
		          "FROM generate_series(" + S(base_rows + 1) + ", " + S(base_rows + delta_rows) + ") gs(i)");
	}
	return {"projection", vn, vq, 2, {"delta_bench_proj"}};
}

static ScenarioInfo SetupFilter(Connection &con, size_t base_rows, size_t delta_rows) {
	string vn = "mv_filt";
	string vq = "SELECT id, val FROM bench_filt WHERE val > 500";

	Exec(con, "CREATE TABLE bench_filt (id INT, val INT, data VARCHAR)");
	Exec(con, "INSERT INTO bench_filt SELECT i, (i * 7) % 1000, 'data_' || i::VARCHAR "
	          "FROM generate_series(1, " + S(base_rows) + ") gs(i)");
	Exec(con, "CREATE TABLE " + vn + " AS " + vq);
	Exec(con, "CREATE TABLE delta_" + vn + " AS SELECT *, true AS _duckdb_ivm_multiplicity FROM " + vn + " LIMIT 0");
	Exec(con, "CREATE TABLE delta_bench_filt AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_filt LIMIT 0");
	Exec(con, "INSERT INTO _duckdb_ivm_views VALUES ('" + vn + "', '" + vq + "', 2, now()::timestamp)");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_filt', '1970-01-01')");

	if (delta_rows > 0) {
		Exec(con, "INSERT INTO bench_filt SELECT i, (i * 7) % 1000, 'data_' || i::VARCHAR "
		          "FROM generate_series(" + S(base_rows + 1) + ", " + S(base_rows + delta_rows) + ") gs(i)");
	}
	return {"filter", vn, vq, 2, {"delta_bench_filt"}};
}

static ScenarioInfo SetupAggregateGrouped(Connection &con, size_t base_rows, size_t delta_rows) {
	string vn = "mv_agg";
	size_t num_groups = std::max(base_rows / 100, (size_t)1);
	string vq = "SELECT grp, sum(val) AS total, count(val) AS cnt FROM bench_agg GROUP BY grp";

	Exec(con, "CREATE TABLE bench_agg (id INT, grp INT, val INT)");
	Exec(con, "INSERT INTO bench_agg SELECT i, i % " + S(num_groups) + ", (i * 7) % 1000 "
	          "FROM generate_series(1, " + S(base_rows) + ") gs(i)");
	Exec(con, "CREATE TABLE " + vn + " AS " + vq);
	Exec(con, "CREATE TABLE delta_" + vn + " AS SELECT *, true AS _duckdb_ivm_multiplicity FROM " + vn + " LIMIT 0");
	Exec(con, "CREATE UNIQUE INDEX " + vn + "_ivm_index ON " + vn + " (grp)");
	Exec(con, "CREATE TABLE delta_bench_agg AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_agg LIMIT 0");
	Exec(con, "INSERT INTO _duckdb_ivm_views VALUES ('" + vn + "', '" + vq + "', 0, now()::timestamp)");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_agg', '1970-01-01')");

	if (delta_rows > 0) {
		Exec(con, "INSERT INTO bench_agg SELECT i, i % " + S(num_groups) + ", (i * 7) % 1000 "
		          "FROM generate_series(" + S(base_rows + 1) + ", " + S(base_rows + delta_rows) + ") gs(i)");
	}
	return {"aggregate_grouped", vn, vq, 0, {"delta_bench_agg"}};
}

static ScenarioInfo SetupJoin2Way(Connection &con, size_t base_rows, size_t delta_rows) {
	string vn = "mv_join2";
	string vq = "SELECT a.id, a.name, b.val FROM bench_a AS a INNER JOIN bench_b AS b ON b.a_id = a.id";

	Exec(con, "CREATE TABLE bench_a (id INT, name VARCHAR)");
	Exec(con, "INSERT INTO bench_a SELECT i, 'name_' || i::VARCHAR "
	          "FROM generate_series(1, " + S(base_rows) + ") gs(i)");
	size_t b_rows = base_rows * 2;
	Exec(con, "CREATE TABLE bench_b (id INT, a_id INT, val INT)");
	Exec(con, "INSERT INTO bench_b SELECT i, (i % " + S(base_rows) + ") + 1, (i * 13) % 1000 "
	          "FROM generate_series(1, " + S(b_rows) + ") gs(i)");
	Exec(con, "CREATE TABLE " + vn + " AS " + vq);
	Exec(con, "CREATE TABLE delta_" + vn + " AS SELECT *, true AS _duckdb_ivm_multiplicity FROM " + vn + " LIMIT 0");
	Exec(con, "CREATE TABLE delta_bench_a AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_a LIMIT 0");
	Exec(con, "CREATE TABLE delta_bench_b AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_b LIMIT 0");
	Exec(con, "INSERT INTO _duckdb_ivm_views VALUES ('" + vn + "', '" + vq + "', 2, now()::timestamp)");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_a', '1970-01-01')");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_b', '1970-01-01')");

	if (delta_rows > 0) {
		Exec(con, "INSERT INTO bench_b SELECT i, (i % " + S(base_rows) + ") + 1, (i * 13) % 1000 "
		          "FROM generate_series(" + S(b_rows + 1) + ", " + S(b_rows + delta_rows) + ") gs(i)");
	}
	return {"join_2way", vn, vq, 2, {"delta_bench_a", "delta_bench_b"}};
}

static ScenarioInfo SetupJoin3Way(Connection &con, size_t base_rows, size_t delta_rows) {
	string vn = "mv_join3";
	string vq = "SELECT a.id, b.val, c.data FROM bench_x3 AS a "
	            "INNER JOIN bench_y3 AS b ON b.a_id = a.id "
	            "INNER JOIN bench_z3 AS c ON c.b_id = b.id";

	Exec(con, "CREATE TABLE bench_x3 (id INT, name VARCHAR)");
	Exec(con, "INSERT INTO bench_x3 SELECT i, 'x_' || i::VARCHAR "
	          "FROM generate_series(1, " + S(base_rows) + ") gs(i)");
	Exec(con, "CREATE TABLE bench_y3 (id INT, a_id INT, val INT)");
	Exec(con, "INSERT INTO bench_y3 SELECT i, (i % " + S(base_rows) + ") + 1, (i * 11) % 1000 "
	          "FROM generate_series(1, " + S(base_rows) + ") gs(i)");
	Exec(con, "CREATE TABLE bench_z3 (id INT, b_id INT, data VARCHAR)");
	Exec(con, "INSERT INTO bench_z3 SELECT i, (i % " + S(base_rows) + ") + 1, 'z_' || i::VARCHAR "
	          "FROM generate_series(1, " + S(base_rows) + ") gs(i)");
	Exec(con, "CREATE TABLE " + vn + " AS " + vq);
	Exec(con, "CREATE TABLE delta_" + vn + " AS SELECT *, true AS _duckdb_ivm_multiplicity FROM " + vn + " LIMIT 0");
	Exec(con, "CREATE TABLE delta_bench_x3 AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_x3 LIMIT 0");
	Exec(con, "CREATE TABLE delta_bench_y3 AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_y3 LIMIT 0");
	Exec(con, "CREATE TABLE delta_bench_z3 AS SELECT *, true AS _duckdb_ivm_multiplicity, "
	          "now()::timestamp AS _duckdb_ivm_timestamp FROM bench_z3 LIMIT 0");
	Exec(con, "INSERT INTO _duckdb_ivm_views VALUES ('" + vn + "', '" + vq + "', 2, now()::timestamp)");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_x3', '1970-01-01')");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_y3', '1970-01-01')");
	Exec(con, "INSERT INTO _duckdb_ivm_delta_tables VALUES ('" + vn + "', 'delta_bench_z3', '1970-01-01')");

	if (delta_rows > 0) {
		Exec(con, "INSERT INTO bench_y3 SELECT i, (i % " + S(base_rows) + ") + 1, (i * 11) % 1000 "
		          "FROM generate_series(" + S(base_rows + 1) + ", " + S(base_rows + delta_rows) + ") gs(i)");
	}
	return {"join_3way", vn, vq, 2, {"delta_bench_x3", "delta_bench_y3", "delta_bench_z3"}};
}

// ---------------------------------------------------------------------------
// Benchmark runner
// ---------------------------------------------------------------------------

struct ScenarioConfig {
	string name;
	size_t base_rows;
	size_t delta_rows;
	double delta_pct;
};

// Run a single benchmark point: fresh DB for IVM, fresh DB for recompute
// Returns (ivm_median_ms, recompute_median_ms)
static std::pair<double, double> RunPoint(const ScenarioConfig &sc, int threads) {
	// Define a lambda that sets up a fresh DB and scenario, returns the ScenarioInfo
	auto make_db_and_setup = [&](DuckDB **out_db, Connection **out_con) -> ScenarioInfo {
		*out_db = new DuckDB(nullptr);
		*out_con = new Connection(**out_db);
		Connection &con = **out_con;
		con.Query("SET threads TO " + std::to_string(threads) + ";");
		auto r = con.Query("LOAD openivm");
		if (r->HasError()) {
			Log("LOAD openivm error: " + r->GetError());
		}
		SetupMetaTables(con);

		if (sc.name == "projection") {
			return SetupProjection(con, sc.base_rows, sc.delta_rows);
		} else if (sc.name == "filter") {
			return SetupFilter(con, sc.base_rows, sc.delta_rows);
		} else if (sc.name == "aggregate_grouped") {
			return SetupAggregateGrouped(con, sc.base_rows, sc.delta_rows);
		} else if (sc.name == "join_2way") {
			return SetupJoin2Way(con, sc.base_rows, sc.delta_rows);
		} else {
			return SetupJoin3Way(con, sc.base_rows, sc.delta_rows);
		}
	};

	// --- Measure IVM (PRAGMA): 5 hot runs, take median ---
	vector<double> ivm_times;
	for (int run = 1; run <= 5; ++run) {
		DuckDB *db_ptr;
		Connection *con_ptr;
		auto info = make_db_and_setup(&db_ptr, &con_ptr);

		auto t0 = std::chrono::steady_clock::now();
		Exec(*con_ptr, "PRAGMA ivm_options('memory', 'main', '" + info.view_name + "')");
		auto t1 = std::chrono::steady_clock::now();
		double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
		ivm_times.push_back(ms);

		delete con_ptr;
		delete db_ptr;

		Log("  IVM " + sc.name + " run " + std::to_string(run) + ": " + FormatNumber(ms) + " ms");
	}

	// --- Measure recompute: 5 hot runs, take median ---
	vector<double> recompute_times;
	for (int run = 1; run <= 5; ++run) {
		DuckDB *db_ptr;
		Connection *con_ptr;
		auto info = make_db_and_setup(&db_ptr, &con_ptr);

		auto t0 = std::chrono::steady_clock::now();
		Exec(*con_ptr, "DELETE FROM " + info.view_name);
		Exec(*con_ptr, "INSERT INTO " + info.view_name + " " + info.view_query);
		Exec(*con_ptr, "UPDATE _duckdb_ivm_delta_tables SET last_update = now() "
		               "WHERE view_name = '" + info.view_name + "'");
		for (auto &dt : info.delta_table_names) {
			Exec(*con_ptr, "DELETE FROM " + dt + " WHERE _duckdb_ivm_timestamp < "
			               "(SELECT min(last_update) FROM _duckdb_ivm_delta_tables "
			               "WHERE table_name = '" + dt + "')");
		}
		auto t1 = std::chrono::steady_clock::now();
		double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
		recompute_times.push_back(ms);

		delete con_ptr;
		delete db_ptr;

		Log("  Recompute " + sc.name + " run " + std::to_string(run) + ": " + FormatNumber(ms) + " ms");
	}

	return {Median(ivm_times), Median(recompute_times)};
}

// ---------------------------------------------------------------------------
// Main benchmark entry point
// ---------------------------------------------------------------------------

int RunIVMBenchmark(const string &out_csv, int threads) {
	try {
		Log("OpenIVM Benchmark — IVM vs Full Recompute");
		Log("Threads: " + std::to_string(threads));

		// Scale points
		vector<size_t> base_row_counts = {1000, 10000, 100000};
		vector<double> delta_percentages = {0.1, 1.0, 5.0, 10.0};
		vector<string> scenarios = {"projection", "filter", "aggregate_grouped", "join_2way", "join_3way"};

		// Build all configurations
		vector<ScenarioConfig> configs;
		for (auto &name : scenarios) {
			for (auto base_rows : base_row_counts) {
				for (auto delta_pct : delta_percentages) {
					size_t delta_rows = std::max((size_t)1, (size_t)(base_rows * delta_pct / 100.0));
					configs.push_back({name, base_rows, delta_rows, delta_pct});
				}
			}
		}

		string actual_out = out_csv;
		if (actual_out.empty()) {
			actual_out = "benchmarks/ivm_benchmark_results.csv";
		}

		std::ofstream csv(actual_out, std::ofstream::out | std::ofstream::trunc);
		if (!csv.is_open()) {
			throw std::runtime_error("Failed to open output CSV: " + actual_out);
		}
		csv << "scenario,base_rows,delta_rows,delta_pct,ivm_median_ms,recompute_median_ms,speedup\n";

		size_t total = configs.size();
		for (size_t i = 0; i < total; ++i) {
			auto &sc = configs[i];
			Log("=== [" + std::to_string(i + 1) + "/" + std::to_string(total) + "] " + sc.name +
			    " base=" + S(sc.base_rows) + " delta=" + S(sc.delta_rows) +
			    " (" + FormatNumber(sc.delta_pct) + "%) ===");

			try {
				auto result = RunPoint(sc, threads);
				double ivm_ms = result.first;
				double recompute_ms = result.second;
				double speedup = (ivm_ms > 0) ? recompute_ms / ivm_ms : 0;

				Log("  => IVM median: " + FormatNumber(ivm_ms) + " ms, Recompute median: " +
				    FormatNumber(recompute_ms) + " ms, Speedup: " + FormatNumber(speedup) + "x");

				csv << sc.name << "," << sc.base_rows << "," << sc.delta_rows << ","
				    << FormatNumber(sc.delta_pct) << "," << FormatNumber(ivm_ms) << ","
				    << FormatNumber(recompute_ms) << "," << FormatNumber(speedup) << "\n";
				csv.flush();
			} catch (const std::exception &e) {
				Log("  ERROR: " + string(e.what()));
				csv << sc.name << "," << sc.base_rows << "," << sc.delta_rows << ","
				    << FormatNumber(sc.delta_pct) << ",-1,-1,0\n";
				csv.flush();
			}
		}

		csv.close();
		Log("Benchmark finished. Results written to " + actual_out);
		return 0;

	} catch (std::exception &ex) {
		Log("Error running benchmark: " + string(ex.what()));
		return 2;
	}
}

} // namespace duckdb

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

static void PrintUsageMain() {
	std::cout << "Usage: openivm_benchmark [out_csv] [--threads=N]\n"
	          << "  out_csv:      Output CSV path (default: benchmarks/ivm_benchmark_results.csv)\n"
	          << "  --threads=N:  Number of DuckDB threads (default: 8)\n";
}

int main(int argc, char **argv) {
	if (argc > 1) {
		std::string arg1 = argv[1];
		if (arg1 == "-h" || arg1 == "--help") {
			PrintUsageMain();
			return 0;
		}
	}

	int threads = 8;
	std::string out_csv;

	for (int i = 1; i < argc; ++i) {
		std::string a = argv[i];
		if (a.rfind("--threads=", 0) == 0) {
			threads = std::stoi(a.substr(10));
		} else {
			out_csv = a;
		}
	}

	return duckdb::RunIVMBenchmark(out_csv, threads);
}
