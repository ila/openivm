// Regression for the benchmark metadata lookups: openivm_views.view_name is an encoded internal key, so
// the benchmarks must resolve the SQL-facing MV name to its key before reading metadata. Exercises the
// exact helpers used by rewriter_benchmark on a file-backed native catalog and on a DuckLake catalog.
#include "duckdb.hpp"
#include "benchmark_metadata.hpp"
#include "core/sql_utils.hpp"
#include <iostream>

using namespace duckdb;

static void Execute(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException(sql + ": " + result->GetError());
	}
}

static void Check(bool condition, const string &message) {
	if (!condition) {
		throw InvalidInputException(message);
	}
}

// The obsolete benchmark lookup: matching the SQL-facing name against the internal key column.
static idx_t LegacyLookupRows(Connection &con, const string &native_catalog, const string &name) {
	auto result = con.Query("SELECT type FROM " + KeywordHelper::WriteOptionallyQuoted(native_catalog) +
	                        ".main.openivm_views WHERE view_name = " + Value(name).ToSQLString());
	Check(!result->HasError(), "legacy lookup failed to run: " + result->GetError());
	return result->RowCount();
}

static void CheckLookup(DuckDB &db, Connection &con, const string &native_catalog, const string &mv, int64_t type,
                        const string &label) {
	Check(LegacyLookupRows(con, native_catalog, mv) == 0,
	      label + ": view_name unexpectedly equals the SQL name; the regression no longer isolates the key lookup");
	auto lookup = openivm_bench::LookupViewType(db, con, native_catalog, mv);
	Check(lookup.found, label + ": metadata lookup failed for " + mv + ": " + lookup.error);
	Check(lookup.key != mv && lookup.key.rfind("__openivm_mv_", 0) == 0,
	      label + ": expected an encoded internal key, got '" + lookup.key + "'");
	Check(lookup.type == type, label + ": wrong refresh type " + to_string(lookup.type));
	Check(!openivm_bench::LookupViewType(db, con, native_catalog, "mv_missing").found,
	      label + ": lookup of a missing view must not succeed");
}

int main(int argc, char **argv) {
	if (argc != 2) {
		return 2;
	}
	try {
		string dir = SqlUtils::EscapeValue(argv[1]);
		DuckDB db(string(argv[1]) + "/native.db");
		Connection con(db);
		Execute(con, "LOAD openivm; LOAD ducklake; SET openivm_files_path='" + dir + "'");
		auto native = con.Query("SELECT current_database()");
		Check(!native->HasError(), native->GetError());
		string native_catalog = native->GetValue(0, 0).ToString();
		Check(native_catalog != "memory", "expected a file-backed native catalog");

		// File-backed native catalog; mv_q1 is a simple projection (type 2), mv_q2 an aggregate (type 0).
		Execute(con, "CREATE TABLE source(id INTEGER, grp VARCHAR, val INTEGER); "
		             "INSERT INTO source VALUES (1, 'a', 10), (2, 'b', 20);");
		Execute(con, "CREATE MATERIALIZED VIEW mv_q1 AS SELECT id, grp FROM source");
		Execute(con, "CREATE MATERIALIZED VIEW mv_q2 AS SELECT grp, SUM(val) AS total FROM source GROUP BY grp");
		CheckLookup(db, con, native_catalog, "mv_q1", 2, "native");
		CheckLookup(db, con, native_catalog, "mv_q2", 0, "native");

		auto leftovers = openivm_bench::ListLeftoverViews(con, native_catalog, "mv_q%");
		Check(leftovers.size() == 2, "native: expected two leftover views, got " + to_string(leftovers.size()));
		for (auto &lv : leftovers) {
			Check(lv.key.rfind("__openivm_mv_", 0) == 0, "native: leftover key is not an internal key");
			Check(lv.catalog == native_catalog && lv.schema == "main", "native: wrong leftover location");
		}
		Check(openivm_bench::ListLeftoverViews(con, native_catalog, "nomatch%").empty(),
		      "native: unrelated pattern must match nothing");

		// DuckLake catalog: the MV lands in lake.main while its metadata stays in the native catalog.
		Execute(con, "ATTACH '" + dir + "/lake.db' AS lake (TYPE ducklake, DATA_PATH '" + dir + "/lake.data')");
		Execute(con, "CREATE TABLE lake.source(id INTEGER, val INTEGER); INSERT INTO lake.source VALUES (1, 5);");
		Execute(con, "USE lake.main");
		Execute(con, "CREATE MATERIALIZED VIEW mv_q3 AS SELECT id, val FROM source");
		CheckLookup(db, con, native_catalog, "mv_q3", 2, "ducklake");
		// The native-catalog views must stay invisible from the DuckLake schema.
		Check(!openivm_bench::LookupViewType(db, con, native_catalog, "mv_q1").found,
		      "ducklake: lookup must be scoped to the connection's catalog and schema");

		leftovers = openivm_bench::ListLeftoverViews(con, native_catalog, "mv_q3");
		Check(leftovers.size() == 1 && leftovers[0].catalog == "lake" && leftovers[0].schema == "main",
		      "ducklake: leftover location must point at the DuckLake catalog");

		std::cout << "PASS" << std::endl;
	} catch (const std::exception &e) {
		std::cerr << e.what() << std::endl;
		return 1;
	}
}
