#include "duckdb.hpp"
#include "core/openivm_constants.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "mutation_gate_test_access.hpp"

#include <chrono>
#include <future>
#include <iostream>
#include <thread>

using namespace duckdb;

static void Execute(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("Concurrency SQL failed: %s; SQL: %s", result->GetError(), sql);
	}
}

static void CheckBag(Connection &con, const string &view, const string &expected) {
	Execute(con, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + expected + ") OR EXISTS (" +
	                 expected + " EXCEPT ALL SELECT * FROM " + view +
	                 ") THEN error('Concurrent refresh bag mismatch') ELSE true END;");
}

// The caller holds an explicit transaction's mutation gate. Release it only
// after the competing SQL has actually reached its blocked acquisition.
static void RunBlockedMutation(Connection &owner, Connection &waiter, const string &sql, const string &release) {
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*owner.context));
	auto competing = std::async(std::launch::async, [&]() { Execute(waiter, sql); });
	try {
		MutationGateTestAccess::WaitForBlockedOwner(*gate);
		Execute(owner, release);
	} catch (const std::exception &) {
		// Unblock the worker before its future is destroyed, including on a failed
		// wait assertion. The process runner also bounds deadlocks in either client.
		owner.Query("ROLLBACK;");
		throw;
	}
	competing.get();
}

static void Configure(Connection &con, const string &path) {
	Execute(con,
	        "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_files_path=" + Value(path).ToSQLString() + ";");
}

static void TestWriterRefresh(const string &path, bool unrelated) {
	std::cerr << "CASE writer commit blocks " << (unrelated ? "unrelated" : "same-source") << " refresh" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), waiter(db);
	Configure(owner, path);
	Execute(owner, "CREATE TABLE a(id INTEGER, v INTEGER); INSERT INTO a VALUES (1,10); "
	               "CREATE TABLE b(id INTEGER, v INTEGER); INSERT INTO b VALUES (1,10),(2,20); "
	               "CREATE MATERIALIZED VIEW mv AS SELECT * FROM " +
	                   string(unrelated ? "b" : "a") + ";");
	if (unrelated) {
		// Track A too: its write must retain the gate even though MV reads B.
		Execute(owner, "CREATE MATERIALIZED VIEW tracked_a AS SELECT * FROM a; INSERT INTO b VALUES (3,30);");
	}
	Execute(owner, "BEGIN; INSERT INTO a VALUES (2,20);");
	RunBlockedMutation(owner, waiter, "PRAGMA refresh('mv');", "COMMIT;");
	CheckBag(waiter, "mv", "SELECT * FROM " + string(unrelated ? "b" : "a"));
}

static void TestWriterExpansion(const string &path, bool rollback) {
	std::cerr << "CASE writer " << (rollback ? "rollback before competing write" : "extends write set") << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), waiter(db);
	Configure(owner, path);
	Execute(owner, "CREATE TABLE a(id INTEGER); CREATE TABLE b(id INTEGER); "
	               "INSERT INTO a VALUES (1); INSERT INTO b VALUES (1); "
	               "CREATE MATERIALIZED VIEW mv AS SELECT a.id a_id,b.id b_id FROM a JOIN b ON true;");
	Execute(owner, "BEGIN; INSERT INTO a VALUES (2);");
	const string query = "SELECT a.id,b.id FROM a JOIN b ON true";
	if (rollback) {
		Execute(waiter, "BEGIN;");
		RunBlockedMutation(owner, waiter, "INSERT INTO b VALUES (2);", "ROLLBACK;");
		Execute(waiter, "PRAGMA refresh('mv');");
		CheckBag(waiter, "mv", query);
		Execute(waiter, "ROLLBACK;");
		CheckBag(owner, "mv", query);
	} else {
		RunBlockedMutation(owner, waiter, "PRAGMA refresh('mv');", "INSERT INTO b VALUES (2); COMMIT;");
		CheckBag(waiter, "mv", query);
	}
}

static void TestRefreshRollback(const string &path) {
	std::cerr << "CASE refresh retains gate until rollback" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), waiter(db);
	Configure(owner, path);
	Execute(owner, "CREATE TABLE source(id INTEGER,v INTEGER); INSERT INTO source VALUES (1,10); "
	               "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source; INSERT INTO source VALUES (2,20); "
	               "BEGIN;");
	Execute(owner, "PRAGMA refresh('mv');");
	CheckBag(owner, "mv", "SELECT * FROM source");
	RunBlockedMutation(owner, waiter, "PRAGMA refresh('mv');", "ROLLBACK;");
	CheckBag(waiter, "mv", "SELECT * FROM source");
}

static void TestParentPublication(const string &path) {
	std::cerr << "CASE child waits for parent refresh publication" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), waiter(db);
	Configure(owner, path);
	Execute(owner, "SET openivm_cascade_refresh='off'; "
	               "CREATE TABLE source(id INTEGER,v INTEGER); INSERT INTO source VALUES (1,10); "
	               "CREATE MATERIALIZED VIEW parent AS SELECT * FROM source; "
	               "CREATE MATERIALIZED VIEW child AS SELECT SUM(v) total FROM parent; "
	               "INSERT INTO source VALUES (2,20); BEGIN;");
	Execute(owner, "PRAGMA refresh('parent');");
	CheckBag(owner, "parent", "SELECT * FROM source");
	RunBlockedMutation(owner, waiter, "PRAGMA refresh('child');", "COMMIT;");
	CheckBag(waiter, "parent", "SELECT * FROM source");
	CheckBag(waiter, "child", "SELECT SUM(v) FROM source");
}

static void TestDaemonHook(const string &path) {
	std::cerr << "CASE daemon hook completion" << std::endl;
	DuckDB db(nullptr);
	Connection con(db);
	Configure(con, path);
	Execute(con, "CREATE TABLE source(id INTEGER); CREATE TABLE hook_log(msg VARCHAR); "
	             "CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) n FROM source; "
	             "INSERT INTO source VALUES (1),(2);");
	Execute(con, "INSERT INTO openivm_refresh_hooks SELECT view_name, "
	             "'INSERT INTO hook_log VALUES(''daemon-hook ran'')', 'after' "
	             "FROM openivm_views WHERE view_sql_name='mv'; "
	             "UPDATE openivm_views SET refresh_interval=60 WHERE view_sql_name='mv'; "
	             "UPDATE openivm_delta_tables SET last_update=" +
	                 string(openivm::UTC_NOW_SQL) + " - INTERVAL '2 minutes';");
	Execute(con, "PRAGMA refresh_start_daemon;");
	auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
	while (true) {
		auto result = con.Query("SELECT count(*) FROM hook_log WHERE msg='daemon-hook ran';");
		if (result->HasError()) {
			throw InvalidInputException(result->GetError());
		}
		auto row = result->Fetch();
		auto count = row->GetValue(0, 0).GetValue<int64_t>();
		if (count > 1) {
			throw InvalidInputException("Scheduled refresh executed its hook more than once");
		}
		if (count == 1) {
			auto status = con.Query("PRAGMA refresh_status('mv');");
			if (status->HasError()) {
				throw InvalidInputException(status->GetError());
			}
			auto state = status->Fetch();
			if (state->GetValue(4, 0).ToString() == "idle") {
				break;
			}
		}
		if (std::chrono::steady_clock::now() >= deadline) {
			throw InvalidInputException("Scheduled refresh did not complete its hook");
		}
		std::this_thread::yield();
	}
	Execute(con, "UPDATE openivm_views SET refresh_interval=NULL WHERE view_sql_name='mv';");
	Execute(con, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Scheduled hook count differs') END FROM hook_log;");
	CheckBag(con, "mv", "SELECT COUNT(*) FROM source");
}

int main(int argc, char **argv) {
	if (argc != 2) {
		return 2;
	}
	try {
		TestWriterExpansion(argv[1], true);
		TestWriterRefresh(argv[1], false);
		TestParentPublication(argv[1]);
		TestWriterRefresh(argv[1], true);
		TestWriterExpansion(argv[1], false);
		TestRefreshRollback(argv[1]);
		TestDaemonHook(argv[1]);
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
