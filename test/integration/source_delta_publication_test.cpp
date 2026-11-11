#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "duckdb.hpp"
#include "core/parser.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/parser_ddl.hpp"
#include "core/published_view.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "rules/column_hider.hpp"

#include <chrono>
#include <future>
#include <iostream>
#include <thread>

namespace duckdb {

// Observe the actual gate wait, rather than assuming a launched worker reached
// an ownership lookup. The deadline only bounds a broken test's wait.
class MutationGateTestAccess {
public:
	static void WaitForBlockedOwner(MutationGate &gate, const string &source = "", bool publication = false) {
		auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
		while (std::chrono::steady_clock::now() < deadline) {
			{
				std::lock_guard<mutex> guard(gate.lock);
				if (source.empty() && gate.waiting_owners != 0) {
					return;
				}
				for (auto &catalog : gate.sources) {
					auto entry = catalog.second.find(StringUtil::Lower(source));
					if (entry != catalog.second.end() &&
					    (publication ? entry->second->waiting_publishers : entry->second->waiting_readers)) {
						return;
					}
				}
			}
			std::this_thread::yield();
		}
		throw InvalidInputException("Competing mutation did not wait on its admission gate: %s", source);
	}
};

} // namespace duckdb

using namespace duckdb;

static ParserExtensionParseResult ParseMaterializedViewStatement(ClientContext &context, const string &sql) {
	Parser parser(context);
	parser.ParseQuery(sql);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::EXTENSION_STATEMENT) {
		throw InvalidInputException("Publication fixture expected one materialized-view statement");
	}
	return ParserExtensionParseResult(std::move(parser.statements[0]->Cast<ExtensionStatement>().parse_data));
}

static void Execute(Connection &con, const string &sql) {
	// Flush before execution so a coordinator timeout identifies the blocked
	// operation even when the query never returns an error or result.
	std::cerr << "START " << SqlUtils::SQLStatementPreview(sql) << std::endl;
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException("Lifecycle SQL failed: %s\nSQL: %s", result->GetError(), sql);
	}
	std::cerr << "DONE " << SqlUtils::SQLStatementPreview(sql) << std::endl;
}

static void CheckBag(Connection &con, const string &actual, const string &expected, const string &message) {
	Execute(con, "SELECT CASE WHEN EXISTS (" + actual + " EXCEPT ALL " + expected + ") OR EXISTS (" + expected +
	                 " EXCEPT ALL " + actual + ") THEN error(" + Value(message).ToSQLString() + ") ELSE true END;");
}

static void ExecuteBatch(Connection &con, const vector<string> &statements, bool batched) {
	if (batched) {
		Execute(con, StringUtil::Join(statements, "; ") + ";");
	} else {
		for (auto &sql : statements) {
			Execute(con, sql + ";");
		}
	}
}

static void RefreshAndCheckBag(Connection &owner, const string &view, const string &query) {
	Execute(owner, "PRAGMA refresh(" + Value(view).ToSQLString() + ");");
	CheckBag(owner, "SELECT * FROM " + view, query, "Publication owner survivor bag mismatch");
}

static void TestInternalSchemaReopen(const string &path) {
	std::cerr << "CASE internal schema reopen with fresh configuration" << std::endl;
	{
		// Startup must be WAL-safe even when core functions are available at LOAD.
		DBConfig config;
		config.options.load_extensions = false;
		DuckDB db(path, &config);
		ExtensionHelper::LoadExtension(db, "core_functions");
		ExtensionHelper::LoadExtension(db, "openivm");
		Connection owner(db);
		Execute(owner, "SET openivm_disable_daemon=true; PRAGMA disable_checkpoint_on_shutdown; "
		               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
		               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".items(i INTEGER); "
		               "INSERT INTO \"Source.Space\".items VALUES (1),(1),(2); "
		               "CREATE MATERIALIZED VIEW \"Source.Space\".original AS SELECT * FROM \"Source.Space\".items;");
	}
	{
		DuckDB db(path);
		Connection owner(db);
		Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true;");
		Execute(owner, "SELECT CASE WHEN current_setting('openivm_internal_schema')='' "
		               "AND current_setting('openivm_metadata_schema')='' "
		               "THEN true ELSE error('Reopen persisted placement settings') END;");
		Execute(owner,
		        "SET search_path='\"Control.Space\"'; "
		        "CREATE MATERIALIZED VIEW \"Source.Space\".after_reopen AS SELECT * FROM \"Source.Space\".items; "
		        "CREATE OR REPLACE MATERIALIZED VIEW \"Source.Space\".original "
		        "AS SELECT * FROM \"Source.Space\".items;");
		Execute(owner, "SELECT CASE WHEN "
		               "(SELECT count(*) FROM \"Control.Space\".openivm_views "
		               "WHERE view_sql_name='after_reopen' AND coalesce(internal_schema,'')='')=1 "
		               "AND (SELECT count(*) FROM \"Control.Space\".openivm_views "
		               "WHERE view_sql_name='original' AND internal_schema='Internal Space')=1 "
		               "AND (SELECT count(*) FROM duckdb_tables() t JOIN \"Control.Space\".openivm_views v "
		               "ON t.table_name='openivm_data_' || v.view_name "
		               "WHERE v.view_sql_name='after_reopen' AND t.schema_name='Source.Space')=1 "
		               "AND (SELECT count(*) FROM duckdb_tables() t JOIN \"Control.Space\".openivm_views v "
		               "ON t.table_name='openivm_data_' || v.view_name "
		               "WHERE v.view_sql_name='original' AND t.schema_name='Internal Space')=1 "
		               "AND (SELECT count(*) FROM \"Control.Space\".openivm_delta_tables "
		               "WHERE source_schema='Source.Space' AND delta_schema='Internal Space')=2 "
		               "AND (SELECT count(DISTINCT delta_table_name) FROM \"Control.Space\".openivm_delta_tables "
		               "WHERE source_schema='Source.Space')=1 "
		               "THEN true ELSE error('Reopen changed internal placement or duplicated a source delta') END;");
		Execute(owner, "INSERT INTO \"Source.Space\".items VALUES (1),(2),(3),(3); "
		               "DELETE FROM \"Source.Space\".items WHERE i=1; "
		               "UPDATE \"Source.Space\".items SET i=4 WHERE i=2; "
		               "INSERT INTO \"Source.Space\".items VALUES (1),(4),(5); "
		               "DELETE FROM \"Source.Space\".items WHERE i=5;");
		for (const auto &view : vector<string> {"original", "after_reopen"}) {
			auto qualified = SqlUtils::QuoteIdentifier("Source.Space") + "." + SqlUtils::QuoteIdentifier(view);
			Execute(owner, "PRAGMA refresh(" + Value(qualified).ToSQLString() + ");");
			CheckBag(owner, "SELECT * FROM " + qualified, "SELECT * FROM \"Source.Space\".items",
			         "Reopened MV bag mismatch");
		}
		Execute(owner, "DROP VIEW \"Source.Space\".original; DROP VIEW \"Source.Space\".after_reopen;");
		Execute(owner, "SELECT CASE WHEN "
		               "NOT EXISTS (SELECT 1 FROM \"Control.Space\".openivm_views) "
		               "AND NOT EXISTS (SELECT 1 FROM \"Control.Space\".openivm_delta_tables) "
		               "AND NOT EXISTS (SELECT 1 FROM duckdb_tables() "
		               "WHERE schema_name IN ('Source.Space','Internal Space') AND starts_with(table_name,'openivm_')) "
		               "THEN true ELSE error('Reopened namespace cleanup leaked') END;");
	}
}

static void TestPublication(bool explicit_transaction) {
	std::cerr << "CASE publication explicit_transaction=" << explicit_transaction << std::endl;
	DuckDB db(nullptr);
	Connection first(db), second(db), observer(db);
	Execute(first, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1), (1), (2);");
	Execute(second, "SET openivm_internal_schema='Internal Space';");
	// Stop exactly after planning has committed the physical source delta,
	// before the first view's ownership metadata is published.
	first.context->transaction.BeginTransaction();
	auto parsed =
	    ParseMaterializedViewStatement(*first.context, "CREATE MATERIALIZED VIEW first_mv AS SELECT * FROM source;");
	auto planned = MaterializedViewParserExtension::PlanFunction(nullptr, *first.context, std::move(parsed.parse_data));
	Execute(observer, "SELECT CASE WHEN (SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space' "
	                  "AND table_name='openivm_delta_6d61696e_source')=1 "
	                  "AND (SELECT count(*) FROM openivm_delta_tables)=0 "
	                  "THEN true ELSE error('Did not reach the source allocation/publication interval') END;");
	// Check exclusion synchronously, without relying on worker scheduling or sleeps.
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*first.context));
	auto second_owner = TransactionalMVLockState::Get(*second.context).GetMutationOwner();
	if (gate->TryLock(second_owner)) {
		gate->Unlock(second_owner);
		throw InvalidInputException("Source allocation did not retain the mutation gate through publication");
	}
	if (explicit_transaction) {
		// Establish a metadata snapshot before first publishes its ownership row.
		// Waiting for the mutation gate cannot advance this transaction's snapshot.
		Execute(second, "BEGIN;");
		Execute(second, "SELECT CASE WHEN count(*)=0 THEN true "
		                "ELSE error('Competing transaction already sees source ownership') END "
		                "FROM openivm_delta_tables;");
	}
	std::promise<void> started;
	auto ready = started.get_future();
	auto competing = std::async(std::launch::async, [&]() {
		started.set_value();
		Execute(second, "CREATE MATERIALIZED VIEW second_mv AS SELECT * FROM source;");
	});
	ready.get();
	try {
		ExecuteStagedDDL(*first.context, planned.parameters);
		first.context->transaction.Commit();
	} catch (const std::exception &) {
		if (first.context->transaction.HasActiveTransaction()) {
			first.context->transaction.Rollback(nullptr);
		}
		throw;
	}
	competing.get();
	if (explicit_transaction) {
		// Also retain transaction-local ownership when reconciling committed rows.
		Execute(second, "CREATE MATERIALIZED VIEW third_mv AS SELECT * FROM source; COMMIT;");
	}
	Execute(observer, "SELECT CASE WHEN (SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space' "
	                  "AND starts_with(table_name, 'openivm_delta_6d61696e_source'))=1 "
	                  "AND (SELECT count(DISTINCT delta_table_name) FROM openivm_delta_tables "
	                  "WHERE source_schema='main' AND table_name='openivm_delta_source')=1 "
	                  "THEN true ELSE error('Concurrent CREATEs allocated different source deltas') END;");
	Execute(observer, "INSERT INTO source VALUES (2),(3); DELETE FROM source WHERE i=1; "
	                  "UPDATE source SET i=4 WHERE i=2;");
	vector<string> views {"first_mv", "second_mv"};
	if (explicit_transaction) {
		views.push_back("third_mv");
	}
	for (const auto &view : views) {
		Execute(observer, "PRAGMA refresh('" + string(view) + "');");
		CheckBag(observer, "SELECT * FROM " + string(view), "SELECT * FROM source", "Concurrent MV bag mismatch");
	}
}

static void TestDMLDuringPublication(bool dedicated_schema, const string &first_operation, bool explicit_transaction,
                                     bool commit, bool publish = true) {
	std::cerr << "CASE DML during publication dedicated_schema=" << dedicated_schema
	          << " first_operation=" << first_operation << " explicit_transaction=" << explicit_transaction
	          << " commit=" << commit << " publish=" << publish << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	const string source = "\"Source.Space\".\"Items;Table\"";
	const string view = "\"Source.Space\".\"MV;View\"";
	const string internal_schema = dedicated_schema ? "Internal Space" : "Source.Space";
	const string delta_name = RefreshMetadata::SourceDeltaTableName("Source.Space", "Items;Table", internal_schema);
	const string delta = SqlUtils::FullName("memory", internal_schema, delta_name);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_profile_refresh=true; "
	                 "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE SCHEMA \"Source.Space\"; CREATE TABLE " + source + "(i INTEGER); INSERT INTO " +
	                     source + " VALUES (1),(1),(2),(3);");
	creator.context->transaction.BeginTransaction();
	auto parsed = ParseMaterializedViewStatement(*creator.context, "CREATE MATERIALIZED VIEW " + view +
	                                                                   " AS SELECT * FROM " + source + ";");
	auto planned =
	    MaterializedViewParserExtension::PlanFunction(nullptr, *creator.context, std::move(parsed.parse_data));
	Execute(observer, "SELECT CASE WHEN (SELECT count(*) FROM duckdb_tables() WHERE schema_name=" +
	                      Value(internal_schema).ToSQLString() + " AND table_name=" + Value(delta_name).ToSQLString() +
	                      ")=1 AND NOT EXISTS (SELECT 1 FROM \"Control.Space\".openivm_delta_tables) "
	                      "THEN true ELSE error('DML fixture missed the allocation/publication interval') END;");
	Execute(writer, "USE memory.\"Control.Space\";");
	if (explicit_transaction) {
		Execute(writer, "BEGIN; SELECT * FROM openivm_delta_tables;");
	}
	const string insert = "INSERT INTO " + source + " VALUES (1),(2),(3),(3),(5); ";
	const string remove = "DELETE FROM " + source + " WHERE i=1 OR i=5; ";
	const string update = "UPDATE " + source + " SET i=4 WHERE i=2; ";
	const string batch = (first_operation == "insert"   ? insert + remove + update
	                      : first_operation == "delete" ? remove + insert + update
	                                                    : update + insert + remove) +
	                     "INSERT INTO " + source + " VALUES (4),(6),(6); DELETE FROM " + source + " WHERE i=6;";
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*creator.context));
	auto competing = std::async(std::launch::async, [&]() { Execute(writer, batch); });
	try {
		MutationGateTestAccess::WaitForBlockedOwner(*gate, SqlUtils::FullName("memory", "Source.Space", "Items;Table"));
		if (publish) {
			ExecuteStagedDDL(*creator.context, planned.parameters);
			creator.context->transaction.Commit();
		} else {
			creator.context->transaction.Rollback(nullptr);
		}
	} catch (const std::exception &) {
		if (creator.context->transaction.HasActiveTransaction()) {
			creator.context->transaction.Rollback(nullptr);
		}
		competing.wait();
		if (writer.context->transaction.HasActiveTransaction()) {
			writer.context->transaction.Rollback(nullptr);
		}
		throw;
	}
	competing.get();
	if (explicit_transaction) {
		if (publish) {
			Execute(writer, "SELECT CASE WHEN count(*) FILTER (WHERE openivm_multiplicity=1)>0 "
			                "AND count(*) FILTER (WHERE openivm_multiplicity=-1)>0 "
			                "THEN true ELSE error('Admitted DML missed signed capture') END FROM " +
			                    delta + ";");
		}
		Execute(writer, commit ? "COMMIT;" : "ROLLBACK;");
	}
	if (!publish) {
		// Failed publication must release admission and reclaim the unowned
		// allocation. The next CREATE loads the writer's committed source rows.
		Execute(creator, "CREATE MATERIALIZED VIEW " + view + " AS SELECT * FROM " + source + ";");
	} else if (!explicit_transaction || commit) {
		Execute(observer, "SELECT CASE WHEN count(*) FILTER (WHERE openivm_multiplicity=1)>0 "
		                  "AND count(*) FILTER (WHERE openivm_multiplicity=-1)>0 "
		                  "THEN true ELSE error('Committed DML missed signed capture') END FROM " +
		                      delta + ";");
	} else {
		Execute(observer, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Rolled-back capture survived') END FROM " +
		                      delta + ";");
	}
	RefreshAndCheckBag(observer, view, "SELECT * FROM " + source);
	Execute(observer, "DROP VIEW " + view + ";");
	Execute(observer, "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM duckdb_tables() WHERE schema_name=" +
	                      Value(internal_schema).ToSQLString() + " AND table_name=" + Value(delta_name).ToSQLString() +
	                      ") THEN true ELSE error('Admitted DML retained the final source delta') END;");
}

static void TestDropDuringPublication(bool explicit_transaction, bool staged_drop, bool schema_drop = false) {
	std::cerr << "CASE drop during publication explicit_transaction=" << explicit_transaction
	          << " staged_drop=" << staged_drop << " schema_drop=" << schema_drop << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), dropper(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; "
	                 "SET openivm_internal_schema='Internal Space'; "
	                 "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2);");
	string dropped_view = "first_mv";
	if (staged_drop) {
		// A view in another native catalog exercises staged cleanup without a
		// DuckLake install: its shared source delta is in the host catalog.
		Execute(creator, "ATTACH ':memory:' AS other;");
		dropped_view = "other.main.first_mv";
	}
	const string dropped_schema = staged_drop ? "other.\"Drop;Space\"" : "memory.\"Drop;Space\"";
	if (schema_drop) {
		Execute(creator, "CREATE SCHEMA " + dropped_schema + ";");
		dropped_view = dropped_schema + ".first_mv";
	}
	Execute(creator, "CREATE MATERIALIZED VIEW " + dropped_view + " AS SELECT * FROM source;");
	creator.context->transaction.BeginTransaction();
	auto parsed =
	    ParseMaterializedViewStatement(*creator.context, "CREATE MATERIALIZED VIEW second_mv AS SELECT * FROM source;");
	auto planned =
	    MaterializedViewParserExtension::PlanFunction(nullptr, *creator.context, std::move(parsed.parse_data));
	if (explicit_transaction) {
		Execute(dropper, "BEGIN;");
	}
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*creator.context));
	auto competing = std::async(std::launch::async, [&]() {
		Execute(dropper,
		        schema_drop ? "DROP SCHEMA " + dropped_schema + " CASCADE;" : "DROP VIEW " + dropped_view + ";");
	});
	try {
		// Old DROP reaches this wait only AFTER deciding to delete the delta;
		// corrected DROP waits BEFORE reading any ownership metadata.
		MutationGateTestAccess::WaitForBlockedOwner(*gate);
		ExecuteStagedDDL(*creator.context, planned.parameters);
		creator.context->transaction.Commit();
	} catch (const std::exception &) {
		if (creator.context->transaction.HasActiveTransaction()) {
			creator.context->transaction.Rollback(nullptr);
		}
		competing.wait();
		if (dropper.context->transaction.HasActiveTransaction()) {
			dropper.context->transaction.Rollback(nullptr);
		}
		throw;
	}
	competing.get();
	auto observer_owner = TransactionalMVLockState::Get(*observer.context).GetMutationOwner();
	if (explicit_transaction) {
		if (gate->TryLock(observer_owner)) {
			gate->Unlock(observer_owner);
			throw InvalidInputException("DROP released the mutation gate before its transaction committed");
		}
		Execute(dropper, "COMMIT;");
	}
	if (!gate->TryLock(observer_owner)) {
		throw InvalidInputException("DROP retained the mutation gate after execution committed");
	}
	gate->Unlock(observer_owner);
	Execute(observer, "SELECT CASE WHEN count(*)=1 THEN true "
	                  "ELSE error('DROP removed the surviving CREATE source delta') END FROM duckdb_tables() "
	                  "WHERE schema_name='Internal Space' AND table_name='openivm_delta_6d61696e_source';");
	Execute(observer, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	                  "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (4),(5); "
	                  "DELETE FROM source WHERE i=5;");
	Execute(observer, "SELECT CASE WHEN count(*)>0 THEN true "
	                  "ELSE error('Surviving source delta did not capture batched DML') END "
	                  "FROM \"Internal Space\".openivm_delta_6d61696e_source;");
	Execute(observer, "PRAGMA refresh('second_mv');");
	CheckBag(observer, "SELECT * FROM second_mv", "SELECT * FROM source", "CREATE versus DROP MV bag mismatch");
}

static void TestSharedMetadataUpgrade(bool explicit_transaction, bool commit) {
	std::cerr << "CASE shared metadata upgrade explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Control.Space\"; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2);");
	auto registry = SqlUtils::FullName("memory", "Control.Space", openivm::VIEWS_TABLE);
	Execute(owner, CreateMetadataTableSQL(openivm::VIEWS_TABLE, registry) + "; ALTER TABLE " + registry +
	                   " DROP COLUMN internal_schema;");
	if (explicit_transaction) {
		// Keep a source snapshot open while OpenIVM probes the legacy registry.
		// Those helper commits must not look like uncaptured source writes.
		Execute(owner, "BEGIN; SELECT * FROM source;");
	}
	Execute(owner, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	if (!commit) {
		Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true "
		               "ELSE error('Rolled-back initialization retained its schema or column') END FROM ("
		               "SELECT 1 FROM duckdb_schemas() WHERE schema_name='Internal Space' UNION ALL "
		               "SELECT 1 FROM information_schema.columns WHERE table_schema='Control.Space' "
		               "AND table_name='openivm_views' AND column_name='internal_schema');");
		// The same old registry must also upgrade after rollback, without manual repair.
		Execute(owner, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	}
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (1),(4),(5); "
	               "DELETE FROM source WHERE i=5;");
	Execute(owner, "PRAGMA refresh('mv');");
	CheckBag(owner, "SELECT * FROM mv", "SELECT * FROM source", "Upgraded registry MV bag mismatch");
	Execute(owner, "DROP VIEW mv;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Upgraded registry cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

static void TestCrossCatalogSharedDelta(bool dedicated_schema, bool commit) {
	std::cerr << "CASE cross-catalog shared delta dedicated_schema=" << dedicated_schema << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema=" +
	                   Value(dedicated_schema ? "Internal Space" : "").ToSQLString() + ";");
	Execute(owner, "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".a(i INTEGER); "
	               "CREATE TABLE \"Source.Space\".b(i INTEGER); "
	               "INSERT INTO \"Source.Space\".a VALUES (1),(1),(2); "
	               "INSERT INTO \"Source.Space\".b VALUES (3),(3),(4); "
	               "CREATE MATERIALIZED VIEW main.sentinel_a AS SELECT * FROM \"Source.Space\".a; "
	               "CREATE MATERIALIZED VIEW main.sentinel_b AS SELECT * FROM \"Source.Space\".b; "
	               "ATTACH ':memory:' AS \"Consumer.Catalog\"; "
	               "CREATE MATERIALIZED VIEW \"Consumer.Catalog\".main.mv AS SELECT * FROM \"Source.Space\".b;");
	// Leave nonempty shared deltas before the new initial loads. Their watermarks
	// must still come from the caller's visible rows when source DDL is omitted.
	for (const auto &source : vector<string> {"a", "b"}) {
		auto source_table = SqlUtils::FullName("memory", "Source.Space", source);
		Execute(owner, "INSERT INTO " + source_table + " VALUES (1),(2),(3),(3); DELETE FROM " + source_table +
		                   " WHERE i=1; UPDATE " + source_table + " SET i=4 WHERE i=2;");
	}
	Execute(owner, "USE memory.\"Control.Space\"; BEGIN; "
	               "CREATE OR REPLACE MATERIALIZED VIEW \"Consumer.Catalog\".main.mv "
	               "AS SELECT * FROM \"Source.Space\".a; "
	               "CREATE MATERIALIZED VIEW \"Consumer.Catalog\".main.new_mv AS SELECT * FROM \"Source.Space\".a;");
	for (const auto &view : vector<string> {"mv", "new_mv"}) {
		CheckBag(owner, "SELECT * FROM \"Consumer.Catalog\".main." + view, "SELECT * FROM \"Source.Space\".a",
		         "Cross-catalog initial load bag mismatch");
	}
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(commit ? "1" : "0") +
	                   " THEN true ELSE error('Cross-catalog CREATE transaction outcome mismatch') END "
	                   "FROM duckdb_views() WHERE database_name='Consumer.Catalog' AND schema_name='main' "
	                   "AND view_name='new_mv';");
	for (const auto &source : vector<string> {"a", "b"}) {
		auto source_table = SqlUtils::FullName("memory", "Source.Space", source);
		Execute(owner, "INSERT INTO " + source_table + " VALUES (4),(5),(5),(6); DELETE FROM " + source_table +
		                   " WHERE i=4; UPDATE " + source_table + " SET i=7 WHERE i=5; INSERT INTO " + source_table +
		                   " VALUES (4),(7); DELETE FROM " + source_table + " WHERE i=6;");
	}
	vector<pair<string, string>> survivors = {
	    {"main.sentinel_a", "a"},
	    {"main.sentinel_b", "b"},
	    {"\"Consumer.Catalog\".main.mv", commit ? "a" : "b"},
	};
	if (commit) {
		survivors.emplace_back("\"Consumer.Catalog\".main.new_mv", "a");
	}
	for (const auto &view : survivors) {
		Execute(owner, "PRAGMA refresh(" + Value(view.first).ToSQLString() + ");");
		CheckBag(owner, "SELECT * FROM " + view.first, "SELECT * FROM \"Source.Space\"." + view.second,
		         "Cross-catalog shared delta refresh bag mismatch");
	}
	for (const auto &view : survivors) {
		Execute(owner, "DROP VIEW " + view.first + ";");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Cross-catalog final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE database_name='memory' "
	               "AND schema_name IN ('Source.Space','Internal Space') AND starts_with(table_name,'openivm_');");
}

static void TestCrossCatalogPublicationConsumers(bool commit, const string &internal_schema, bool unqualified_parent) {
	std::cerr << "CASE cross-catalog publication consumers replacement_commit=" << commit
	          << " internal_schema=" << internal_schema << " unqualified_parent=" << unqualified_parent << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	const string source = "memory.\"Source.Space\".items";
	const string replacement = "memory.\"Other.Source\".items";
	const string parent =
	    SqlUtils::FullName("memory", unqualified_parent ? "Control.Space" : "Parent.Space", "Parent;MV");
	const string parent_target = unqualified_parent ? "\"Parent;MV\"" : parent;
	const string fast = "\"Consumer.Catalog\".\"Child.Space\".\"Fast;MV\"";
	const string slow = "\"Consumer.Catalog\".\"Other.Child\".\"Slow;MV\"";
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema=" +
	                   Value(internal_schema).ToSQLString() +
	                   "; CREATE SCHEMA \"Source.Space\"; CREATE SCHEMA \"Other.Source\"; "
	                   "CREATE SCHEMA \"Parent.Space\"; CREATE SCHEMA \"Control.Space\"; CREATE TABLE " +
	                   source + "(i INTEGER); CREATE TABLE " + replacement + "(i INTEGER); INSERT INTO " + source +
	                   " VALUES (1),(1),(2); INSERT INTO " + replacement + " VALUES (20),(20),(30); " +
	                   (unqualified_parent ? "USE memory.\"Control.Space\"; " : "") + "CREATE MATERIALIZED VIEW " +
	                   parent_target + " AS SELECT * FROM " + source +
	                   "; ATTACH ':memory:' AS \"Consumer.Catalog\"; "
	                   "CREATE SCHEMA \"Consumer.Catalog\".\"Child.Space\"; "
	                   "CREATE SCHEMA \"Consumer.Catalog\".\"Other.Child\"; CREATE MATERIALIZED VIEW " +
	                   fast + " AS SELECT * FROM " + parent + "; CREATE MATERIALIZED VIEW " + slow +
	                   " AS SELECT * FROM " + parent + "; USE \"Consumer.Catalog\".\"Control.Space\";");
	// No consumer is registered in the parent's catalog. Cover both publication
	// programs, keeping one child behind so cleanup must retain its earlier rows.
	Execute(owner, "INSERT INTO " + source + " VALUES (9),(9);");
	RefreshAndCheckBag(owner, parent, "SELECT * FROM " + source);
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + parent);
	Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
	                   " WHERE i=1 OR i=9; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
	                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5;");
	RefreshAndCheckBag(owner, parent, "SELECT * FROM " + source);
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + parent);
	RefreshAndCheckBag(owner, slow, "SELECT * FROM " + parent);
	// Replacement must retain the parent's publication and emit its signed diff
	// for consumers in another metadata catalog. Rollback must retain the old source.
	if (unqualified_parent) {
		Execute(owner, "USE memory.\"Control.Space\";");
	}
	Execute(owner,
	        "BEGIN; CREATE OR REPLACE MATERIALIZED VIEW " + parent_target + " AS SELECT * FROM " + replacement + ";");
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	const auto &active_source = commit ? replacement : source;
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + active_source);
	RefreshAndCheckBag(owner, slow, "SELECT * FROM " + active_source);
	Execute(owner, "INSERT INTO " + active_source + " VALUES (1),(2),(3),(3); DELETE FROM " + active_source +
	                   " WHERE i=1; UPDATE " + active_source + " SET i=4 WHERE i=2; INSERT INTO " + active_source +
	                   " VALUES (1),(4),(5); DELETE FROM " + active_source + " WHERE i=5;");
	RefreshAndCheckBag(owner, parent, "SELECT * FROM " + active_source);
	RefreshAndCheckBag(owner, fast, "SELECT * FROM " + parent);
	RefreshAndCheckBag(owner, slow, "SELECT * FROM " + parent);
	Execute(owner, "DROP VIEW " + fast + "; DROP VIEW " + slow + "; DROP VIEW " + parent + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Cross-catalog publication cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Cross-catalog publication cleanup leaked') END "
	               "FROM duckdb_tables() WHERE starts_with(table_name, 'openivm_data_') "
	               "OR starts_with(table_name, 'openivm_visible_') "
	               "OR (starts_with(table_name, 'openivm_delta_') AND table_name <> 'openivm_delta_tables');");
}

static void TestSchemaCascade(bool explicit_transaction, bool commit, bool cross_catalog) {
	std::cerr << "CASE schema cascade explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << " cross_catalog=" << cross_catalog << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	const string dropped_schema = "memory.\"Drop;Space\"";
	const string own_source = dropped_schema + ".items";
	const string shared_source = "memory.\"Keep.Space\".items";
	// Keep the group output distinct from the counted value's auxiliary column.
	// This exercises namespace cleanup without the baseline COUNT(DISTINCT)
	// duplicate-column alias defect for SELECT i, count(DISTINCT i).
	const string shared_query = "SELECT i AS g, count(DISTINCT i) AS n FROM " + shared_source + " GROUP BY i";
	const string sibling = "memory.\"Keep.Space\".\"Same.MV\"";
	const string new_sibling = "memory.\"Keep.Space\".\"New;MV\"";
	const string child_catalog = cross_catalog ? "Peer.Catalog" : "memory";
	const string child = SqlUtils::FullName(child_catalog, "main", "Child;MV");
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "SET openivm_stateful_auxstate=true; "
	               "CREATE SCHEMA \"Drop;Space\"; CREATE SCHEMA \"Keep.Space\"; "
	               "CREATE TABLE " +
	                   own_source + "(i INTEGER); CREATE TABLE " + shared_source + "(i INTEGER); INSERT INTO " +
	                   own_source + " VALUES (1),(1),(2); INSERT INTO " + shared_source +
	                   " VALUES (3),(3),(4); CREATE MATERIALIZED VIEW " + dropped_schema +
	                   ".\"Same.MV\" AS SELECT * FROM " + own_source + "; CREATE MATERIALIZED VIEW " + dropped_schema +
	                   ".\"Shared;MV\" AS " + shared_query + "; CREATE MATERIALIZED VIEW " + sibling +
	                   " AS SELECT * FROM " + shared_source +
	                   "; CREATE MATERIALIZED VIEW main.source_consumer AS SELECT * FROM " + own_source + ";");
	if (cross_catalog) {
		Execute(owner, "ATTACH ':memory:' AS \"Peer.Catalog\"; CREATE SCHEMA \"Peer.Catalog\".\"Drop;Space\"; "
		               "CREATE TABLE \"Peer.Catalog\".\"Drop;Space\".items(i INTEGER); "
		               "INSERT INTO \"Peer.Catalog\".\"Drop;Space\".items VALUES (8),(8),(9); "
		               "CREATE MATERIALIZED VIEW \"Peer.Catalog\".\"Drop;Space\".\"Same.MV\" "
		               "AS SELECT * FROM \"Peer.Catalog\".\"Drop;Space\".items;");
	}
	Execute(owner, "CREATE MATERIALIZED VIEW " + child + " AS SELECT * FROM " + dropped_schema + ".\"Shared;MV\";");
	const vector<pair<string, string>> removed = {
	    {"memory", "Drop;Space"}, {"memory", "Drop;Space"}, {"memory", "main"}, {child_catalog, "main"}};
	const vector<string> removed_names = {"Same.MV", "Shared;MV", "source_consumer", "Child;MV"};
	string owned_predicate;
	string auxiliary_table;
	RefreshMetadata::CountDistinctAuxMeta auxiliary_meta;
	for (idx_t index = 0; index < removed.size(); index++) {
		RefreshMetadata::UseCatalog(*owner.context, helper, removed[index].first);
		RefreshMetadata metadata(helper);
		auto key = metadata.FindViewKey(removed[index].first, removed[index].second, removed_names[index]);
		if (key.empty()) {
			throw InvalidInputException("Schema CASCADE fixture has no registered MV");
		}
		auto auxiliary = metadata.GetAuxiliaryTables(key);
		if (index == 1 && auxiliary.empty()) {
			throw InvalidInputException("Schema CASCADE fixture must own auxiliary state");
		}
		if (index == 1) {
			if (metadata.GetViewType(key) != RefreshType::COUNT_DISTINCT_INCREMENTAL ||
			    !metadata.GetCountDistinctAuxMeta(key, auxiliary_meta)) {
				throw InvalidInputException("Schema CASCADE fixture must use incremental COUNT(DISTINCT)");
			}
			auto internal = metadata.GetInternalLocation(key);
			auxiliary_table = SqlUtils::FullName(internal.catalog_name, internal.schema_name, auxiliary_meta.aux_table);
		}
		vector<string> tables = {IncrementalTableNames::DataTableName(key), PublishedViewName(key),
		                         SqlUtils::DeltaName(key), SqlUtils::DeltaName(PublishedViewName(key))};
		tables.insert(tables.end(), auxiliary.begin(), auxiliary.end());
		for (auto &table : tables) {
			if (!owned_predicate.empty()) {
				owned_predicate += " OR ";
			}
			owned_predicate += "(database_name=" + Value(removed[index].first).ToSQLString() +
			                   " AND schema_name='Internal Space' AND table_name=" + Value(table).ToSQLString() + ")";
		}
	}
	auto check_auxiliary_state = [&]() {
		if (!RefreshMetadata(helper).TableColumnsMatch(
		        "memory", "Internal Space", auxiliary_meta.aux_table,
		        RefreshMetadata::ExpectedCountDistinctAuxColumns(auxiliary_meta))) {
			throw InvalidInputException("Schema CASCADE changed the auxiliary column identity");
		}
		auto expected =
		    "SELECT i AS g, i, count(*)::BIGINT AS _count FROM " + shared_source + " WHERE i IS NOT NULL GROUP BY i";
		CheckBag(owner, "SELECT * FROM " + auxiliary_table, expected, "Schema CASCADE auxiliary bag mismatch");
	};
	check_auxiliary_state();
	auto owned_query = "SELECT database_name, schema_name, table_name FROM duckdb_tables() WHERE " + owned_predicate;
	auto original = owner.Query(owned_query);
	if (original->HasError() || original->RowCount() == 0) {
		throw InvalidInputException("Schema CASCADE fixture has no maintenance tables");
	}
	const auto original_count = original->RowCount();
	Execute(owner, "USE memory.\"Control.Space\"; DROP SCHEMA IF EXISTS memory.\"Missing;Space\" CASCADE;");
	auto restricted = owner.Query("DROP SCHEMA " + dropped_schema + ";");
	if (!restricted->HasError() || restricted->GetError().find("depend") == string::npos) {
		throw InvalidInputException("Schema RESTRICT must retain the native dependency error");
	}
	Execute(owner, "SELECT CASE WHEN (SELECT count(*) FROM (" + owned_query + "))=" + to_string(original_count) +
	                   " THEN true ELSE error('Schema RESTRICT mutated maintenance objects') END;");
	if (explicit_transaction) {
		// Replacement contributes metadata replay before the schema cleanup.
		Execute(owner, "BEGIN; CREATE OR REPLACE MATERIALIZED VIEW " + dropped_schema +
		                   ".\"Same.MV\" AS SELECT * FROM " + own_source + ";");
	}
	Execute(owner, "SELECT 'before;schema'; /* drop; comment */ DROP SCHEMA " + dropped_schema + " CASCADE; SELECT 1;");
	Execute(owner, "SELECT CASE WHEN NOT EXISTS (" + owned_query +
	                   ") THEN true ELSE error('Schema CASCADE leaked maintenance objects') END;");
	Execute(owner, "SELECT CASE WHEN count(*)=1 AND min(view_schema)='Keep.Space' "
	               "THEN true ELSE error('Schema CASCADE leaked registry rows or removed a sibling') END "
	               "FROM memory.\"Control.Space\".openivm_views;");
	if (cross_catalog) {
		Execute(owner, "SELECT CASE WHEN count(*)=1 AND min(view_schema)='Drop;Space' "
		               "THEN true ELSE error('Schema CASCADE lost cross-catalog registry identity') END "
		               "FROM \"Peer.Catalog\".\"Control.Space\".openivm_views;");
	}
	if (explicit_transaction) {
		// Reuse the same schema and physical MV key before commit. Neither the
		// removed source owner nor its old allocation may leak into this CREATE.
		Execute(owner, "CREATE SCHEMA " + dropped_schema + "; CREATE TABLE " + own_source +
		                   "(i INTEGER); INSERT INTO " + own_source + " VALUES (7),(7),(8); CREATE MATERIALIZED VIEW " +
		                   dropped_schema + ".\"Same.MV\" AS SELECT * FROM " + own_source + "; DROP SCHEMA " +
		                   dropped_schema + " CASCADE;");
	}
	// A later CREATE must replay the schema cleanup without losing the sibling
	// that still owns the shared delta. Rollback must restore all prior owners.
	Execute(owner, "CREATE MATERIALIZED VIEW " + new_sibling + " AS SELECT * FROM " + shared_source + ";");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	const bool retained = explicit_transaction && !commit;
	Execute(owner, "SELECT CASE WHEN (SELECT count(*) FROM (" + owned_query +
	                   "))=" + to_string(retained ? original_count : 0) +
	                   " THEN true ELSE error('Schema CASCADE commit/rollback lost maintenance objects') END;");
	if (retained) {
		check_auxiliary_state();
	}
	vector<pair<string, string>> survivors = {{sibling, "SELECT * FROM " + shared_source}};
	if (!retained) {
		survivors.emplace_back(new_sibling, "SELECT * FROM " + shared_source);
	} else {
		survivors.emplace_back(dropped_schema + ".\"Same.MV\"", "SELECT * FROM " + own_source);
		survivors.emplace_back(dropped_schema + ".\"Shared;MV\"", shared_query);
		survivors.emplace_back("memory.main.source_consumer", "SELECT * FROM " + own_source);
		survivors.emplace_back(child, "SELECT * FROM " + dropped_schema + ".\"Shared;MV\"");
	}
	if (cross_catalog) {
		survivors.emplace_back("\"Peer.Catalog\".\"Drop;Space\".\"Same.MV\"",
		                       "SELECT * FROM \"Peer.Catalog\".\"Drop;Space\".items");
	}
	vector<string> sources = {shared_source};
	if (retained) {
		sources.push_back(own_source);
	}
	if (cross_catalog) {
		sources.push_back("\"Peer.Catalog\".\"Drop;Space\".items");
	}
	for (auto &source : sources) {
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i IN (1,8); UPDATE " + source + " SET i=4 WHERE i IN (2,9); INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5; UPDATE " + source +
		                   " SET i=2 WHERE i=4;");
	}
	for (auto &view : survivors) {
		RefreshAndCheckBag(owner, view.first, view.second);
	}
	if (retained) {
		check_auxiliary_state();
	}
	// A rolled-back schema is removed again through the same cleanup path.
	if (retained) {
		Execute(owner, "DROP SCHEMA " + dropped_schema + " CASCADE;");
	}
	Execute(owner, "DROP VIEW " + sibling + ";");
	if (!retained) {
		Execute(owner, "DROP VIEW " + new_sibling + ";");
	}
	if (cross_catalog) {
		Execute(owner, "DROP SCHEMA \"Peer.Catalog\".\"Drop;Space\" CASCADE;");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Schema final-consumer cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
	for (auto &catalog : cross_catalog ? vector<string> {"memory", "Peer.Catalog"} : vector<string> {"memory"}) {
		const auto views = SqlUtils::FullName(catalog, "Control.Space", openivm::VIEWS_TABLE);
		const auto deltas = SqlUtils::FullName(catalog, "Control.Space", openivm::DELTA_TABLES_TABLE);
		Execute(owner, "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM " + views + ") AND NOT EXISTS (SELECT 1 FROM " +
		                   deltas + ") THEN true ELSE error('Schema final-consumer metadata cleanup leaked') END;");
	}
	Execute(owner,
	        "CREATE SCHEMA \"Current;Space\"; USE memory.\"Current;Space\"; "
	        "CREATE TABLE scratch(i INTEGER); DROP SCHEMA memory.\"Current;Space\" CASCADE; "
	        "SELECT CASE WHEN current_schema()='main' THEN true ELSE error('Schema DROP lost search path reset') END;");
}

static void TestMixedLifecycleBatch(bool explicit_transaction, bool commit) {
	std::cerr << "CASE mixed lifecycle explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal;Space'; "
	               "CREATE SCHEMA \"Public;Space\"; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2);");
	const string view = "\"Public;Space\".\"Mixed;MV\"";
	// DuckDB tokenizes newline/line-comment-concatenated literals as one token.
	// Keep nested block comments outside the continuation, as required by its lexer.
	const string base_query = "SELECT i, 'a'\n'b' AS label, 'c' -- continuation ;\n'd' "
	                          "/* nested /* ; */ comment */ AS joined, 'value;--literal' AS comment_label, "
	                          "$$Dollar;/*Literal*/$$ AS dollar_label, 'materialized view' AS keyword_label, "
	                          "'or replace' AS replace_label, 'refresh every ''1 hour''' AS refresh_label FROM source";
	const string expected_query =
	    "SELECT i, 'ab' AS label, 'cd' AS joined, 'value;--literal' AS comment_label, "
	    "'Dollar;/*Literal*/' AS dollar_label, 'materialized view' AS keyword_label, "
	    "'or replace' AS replace_label, 'refresh every ''1 hour''' AS refresh_label FROM source";
	Execute(owner, "CREATE MATERIALIZED VIEW " + view + " REFRESH EVERY '1 hour' AS " + base_query + ";");
	CheckBag(owner, "SELECT * FROM " + view, expected_query, "Scheduled lifecycle changed literal contents");
	Execute(owner, "CREATE MATERIALIZED VIEW sibling AS SELECT * FROM source;");
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// Native statement binding follows execution order. The second DROP must
	// consume ownership from a CREATE executed in this same batch.
	Execute(owner, "SELECT $$native;literal$$, 'create materialized view in a literal'; "
	               "/* outer ; /* nested ; */ comment */ DROP VIEW " +
	                   view + "; -- delimiter ; inside a comment\n CREATE MATERIALIZED VIEW " + view + " AS " +
	                   base_query + "; INSERT INTO source VALUES (3),(3); DROP VIEW " + view +
	                   "; CREATE /* keyword comment ; */ OR REPLACE MATERIALIZED VIEW " + view + " AS " + base_query +
	                   "; SELECT CASE WHEN count(*)=5 THEN true ELSE error('Mixed batch reordered DML') END FROM " +
	                   view + ";");
	CheckBag(owner, "SELECT * FROM " + view, expected_query, "Mixed lifecycle initial load bag mismatch");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (1),(5),(5); "
	               "DELETE FROM source WHERE i=5; UPDATE source SET i=2 WHERE i=4;");
	for (auto &survivor : vector<pair<string, string>> {{view, expected_query}, {"sibling", "SELECT * FROM source"}}) {
		Execute(owner, "PRAGMA refresh('" + survivor.first + "');");
		CheckBag(owner, "SELECT * FROM " + survivor.first, survivor.second, "Mixed lifecycle survivor bag mismatch");
	}
	Execute(owner, "DROP VIEW " + view + "; DROP VIEW sibling; SELECT 1;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Mixed lifecycle final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal;Space';");
}

static void TestCreateAlterTransaction(bool existing_metadata, bool batched, bool commit) {
	std::cerr << "CASE CREATE/ALTER existing_metadata=" << existing_metadata << " batched=" << batched
	          << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Public;Space\"; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2);");
	if (existing_metadata) {
		Execute(owner, "CREATE MATERIALIZED VIEW main.\"Same;MV\" REFRESH EVERY '1 hour' AS SELECT * FROM source;");
		Execute(owner, "USE memory.\"Control.Space\";");
	}
	const string view = "\"Public;Space\".\"Same;MV\"";
	const string registry = "memory.\"Control.Space\".openivm_views";
	const string target_filter = "view_sql_name='Same;MV' AND view_schema='Public;Space'";
	vector<string> statements = {
	    "BEGIN",
	    "CREATE MATERIALIZED VIEW " + view + " REFRESH EVERY '1 hour' AS SELECT * FROM main.source",
	    "ALTER /* lifecycle ; */ MATERIALIZED VIEW memory.\"pUBLIC;sPACE\".\"sAME;mv\" SET REFRESH MANUAL",
	    "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Transaction-local ALTER MANUAL failed') END FROM " +
	        registry + " WHERE " + target_filter + " AND refresh_interval IS NULL",
	    "ALTER MATERIALIZED VIEW " + view + " SET REFRESH EVERY '2 hours'",
	    "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Transaction-local ALTER EVERY failed') END FROM " +
	        registry + " WHERE " + target_filter + " AND refresh_interval=7200",
	};
	ExecuteBatch(owner, statements, batched);
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(commit ? "1" : "0") +
	                   " THEN true ELSE error('CREATE/ALTER transaction outcome mismatch') END FROM duckdb_views() "
	                   "WHERE schema_name='Public;Space' AND view_name='Same;MV';");
	if (!existing_metadata && !commit) {
		Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('CREATE/ALTER rollback leaked schemas') END "
		               "FROM duckdb_schemas() WHERE schema_name IN ('Control.Space','Internal Space');");
		return;
	}
	if (existing_metadata) {
		Execute(owner, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('ALTER changed a same-named sibling') END "
		               "FROM " +
		                   registry +
		                   " WHERE view_sql_name='Same;MV' AND view_schema='main' AND refresh_interval=3600;");
	}
	if (commit) {
		Execute(owner, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('ALTER interval was not committed') END "
		               "FROM " +
		                   registry + " WHERE " + target_filter + " AND refresh_interval=7200;");
	} else {
		Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('ALTER rollback leaked metadata') END FROM " +
		                   registry + " WHERE " + target_filter + ";");
	}
	Execute(owner, "INSERT INTO main.source VALUES (1),(2),(3),(3); DELETE FROM main.source WHERE i=1; "
	               "UPDATE main.source SET i=4 WHERE i=2; INSERT INTO main.source VALUES (4),(5),(5); "
	               "DELETE FROM main.source WHERE i=5;");
	vector<string> survivors;
	if (commit) {
		survivors.push_back(view);
	}
	if (existing_metadata) {
		survivors.push_back("main.\"Same;MV\"");
	}
	for (auto &survivor : survivors) {
		Execute(owner, "PRAGMA refresh(" + Value(survivor).ToSQLString() + ");");
		CheckBag(owner, "SELECT * FROM " + survivor, "SELECT * FROM main.source", "CREATE/ALTER survivor bag mismatch");
	}
	for (auto &survivor : survivors) {
		Execute(owner, "DROP VIEW " + survivor + ";");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('CREATE/ALTER final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

static void TestMetadataSchemaMigration(bool create_trigger) {
	std::cerr << "CASE metadata migration create_trigger=" << create_trigger << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_internal_schema='Internal Space'; CREATE SCHEMA \"Source.Space\"; "
	               "CREATE TABLE \"Source.Space\".source(i INTEGER); "
	               "INSERT INTO \"Source.Space\".source VALUES (1),(1),(2); "
	               "CREATE TABLE main.hook_log(message VARCHAR); "
	               "CREATE MATERIALIZED VIEW main.mv AS SELECT * FROM \"Source.Space\".source;");
	const auto hook = Value("INSERT INTO main.hook_log VALUES ('migrated')").ToSQLString();
	Execute(owner, "INSERT INTO main.openivm_refresh_hooks SELECT view_name, " + hook +
	                   ", 'after' FROM main.openivm_views WHERE view_sql_name='mv'; "
	                   "CREATE TEMP TABLE maintenance_before AS SELECT schema_name, table_name FROM duckdb_tables() "
	                   "WHERE schema_name='Internal Space';");
	// A deliberately incompatible target forces a late copy failure, after the
	// dependency and view registries were copied and dropped in the helper.
	Execute(owner, "CREATE SCHEMA \"Control.Space\"; "
	               "CREATE TABLE \"Control.Space\".openivm_refresh_hooks "
	               "(view_name VARCHAR PRIMARY KEY, hook_sql INTEGER, mode VARCHAR); "
	               "SET openivm_metadata_schema='Control.Space'; "
	               "INSERT INTO \"Source.Space\".source VALUES (1),(2),(3),(3); "
	               "DELETE FROM \"Source.Space\".source WHERE i=1; "
	               "UPDATE \"Source.Space\".source SET i=4 WHERE i=2;");
	const string trigger = create_trigger
	                           ? "CREATE MATERIALIZED VIEW main.sibling AS SELECT * FROM \"Source.Space\".source;"
	                           : "PRAGMA refresh('main.mv');";
	auto failure = owner.Query(trigger);
	if (!failure->HasError() || failure->GetError().find("could not move its metadata") == string::npos ||
	    failure->GetError().find("Conversion Error") == string::npos) {
		throw InvalidInputException("Metadata migration did not reach the intended late copy failure: %s",
		                            failure->HasError() ? failure->GetError() : string("query succeeded"));
	}
	Execute(owner, "SELECT CASE WHEN "
	               "(SELECT count(*) FROM main.openivm_views WHERE view_sql_name='mv')=1 "
	               "AND (SELECT count(*) FROM main.openivm_delta_tables)=1 "
	               "AND (SELECT count(*) FROM main.openivm_refresh_hooks WHERE hook_sql=" +
	                   hook +
	                   ")=1 "
	                   "AND (SELECT count(*) FROM duckdb_tables() WHERE schema_name='Control.Space')=1 "
	                   "AND NOT EXISTS (SELECT 1 FROM main.hook_log) "
	                   "THEN true ELSE error('Failed migration did not restore metadata') END;");
	// The successful retry must not treat the two transient view registries as
	// competing metadata locations, nor retract or drop a tracked source delta.
	Execute(owner, "DROP TABLE \"Control.Space\".openivm_refresh_hooks;");
	Execute(owner, trigger);
	auto check_bag = [&](const string &view) {
		CheckBag(owner, "SELECT * FROM " + view, "SELECT * FROM \"Source.Space\".source", "Migrated MV bag mismatch");
	};
	check_bag(create_trigger ? "main.sibling" : "main.mv");
	Execute(owner,
	        "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM duckdb_tables() "
	        "WHERE schema_name='main' AND table_name IN "
	        "('openivm_views','openivm_delta_tables','openivm_refresh_hooks','openivm_refresh_history',"
	        "'openivm_refresh_profile','openivm_mv_dependencies','openivm_match_log','openivm_constraints_cache')) "
	        "AND (SELECT count(*) FROM \"Control.Space\".openivm_refresh_hooks WHERE hook_sql=" +
	            hook +
	            ")=1 "
	            "THEN true ELSE error('Migration split metadata or lost the hook') END;");
	vector<string> survivors = {"main.mv"};
	if (create_trigger) {
		survivors.push_back("main.sibling");
	}
	// Exercise moving the same registry back, with pending conflicting writes.
	for (const auto &schema : vector<string> {"Control.Space", "main"}) {
		Execute(owner, "SET openivm_metadata_schema=" + Value(schema).ToSQLString() +
		                   "; "
		                   "INSERT INTO \"Source.Space\".source VALUES (1),(2),(3),(3); "
		                   "DELETE FROM \"Source.Space\".source WHERE i=1; "
		                   "UPDATE \"Source.Space\".source SET i=4 WHERE i=2;");
		for (auto &view : survivors) {
			Execute(owner, "PRAGMA refresh(" + Value(view).ToSQLString() + ");");
			check_bag(view);
		}
	}
	Execute(owner, "SELECT CASE WHEN NOT EXISTS (SELECT * FROM maintenance_before EXCEPT ALL "
	               "SELECT schema_name, table_name FROM duckdb_tables() WHERE schema_name='Internal Space') "
	               "AND EXISTS (SELECT 1 FROM main.hook_log WHERE message='migrated') "
	               "AND (SELECT count(*) FROM duckdb_tables() WHERE schema_name='Control.Space')=0 "
	               "THEN true ELSE error('Metadata migration moved maintenance state or lost the hook') END;");
	for (auto &view : survivors) {
		Execute(owner, "DROP VIEW " + view + ";");
	}
	Execute(owner, "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM main.openivm_views) "
	               "AND NOT EXISTS (SELECT 1 FROM main.openivm_delta_tables) "
	               "AND NOT EXISTS (SELECT 1 FROM duckdb_tables() WHERE schema_name='Internal Space') "
	               "THEN true ELSE error('Migration final-consumer cleanup leaked') END;");
}

static void TestMetadataSchemaSettingTransaction(bool existing_metadata, bool batched, bool commit) {
	std::cerr << "CASE metadata setting transaction existing_metadata=" << existing_metadata << " batched=" << batched
	          << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Public;Space\"; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2);");
	if (existing_metadata) {
		Execute(owner, "CREATE MATERIALIZED VIEW main.sibling AS SELECT * FROM source;");
		Execute(owner, "USE memory.\"Control.Space\";");
	}
	const string first = "memory.\"Public;Space\".\"First;MV\"";
	const string second = "memory.\"Public;Space\".\"Second;MV\"";
	const string registry = "memory.\"Control.Space\".openivm_views";
	vector<string> statements = {"BEGIN"};
	if (existing_metadata) {
		// No replay record exists yet: the caller-visible registry still wins.
		statements.push_back("SET openivm_metadata_schema='Different;Control'");
	}
	statements.push_back("CREATE MATERIALIZED VIEW " + first + " AS SELECT * FROM memory.main.source");
	statements.push_back("SET openivm_metadata_schema=''");
	statements.push_back("ALTER MATERIALIZED VIEW " + first + " SET REFRESH MANUAL");
	statements.push_back("SELECT CASE WHEN count(*)=1 THEN true ELSE error('Cleared setting lost ALTER registry') "
	                     "END FROM " +
	                     registry + " WHERE view_sql_name='First;MV' AND refresh_interval IS NULL");
	statements.push_back("SET openivm_metadata_schema='Different;Control'");
	statements.push_back("CREATE MATERIALIZED VIEW " + second + " AS SELECT * FROM memory.main.source");
	statements.push_back("SET openivm_metadata_schema=''");
	statements.push_back("CREATE OR REPLACE MATERIALIZED VIEW " + first +
	                     " AS SELECT i FROM memory.main.source WHERE i<>1");
	statements.push_back("SET openivm_metadata_schema='Different;Control'");
	statements.push_back("DROP VIEW " + second);
	statements.push_back("SET openivm_metadata_schema=''");
	statements.push_back("DROP VIEW " + first);
	// With no consumers left (in the fresh case), neither cleanup nor the next
	// CREATE may abandon the transaction's original registry and delta ownership.
	statements.push_back("SET openivm_metadata_schema=''");
	statements.push_back("CREATE MATERIALIZED VIEW " + first + " AS SELECT i FROM memory.main.source WHERE i<>1");
	statements.push_back("SET openivm_metadata_schema='Different;Control'");
	statements.push_back("CREATE MATERIALIZED VIEW " + second + " AS SELECT * FROM memory.main.source");
	statements.push_back("ALTER MATERIALIZED VIEW " + first + " SET REFRESH EVERY '2 hours'");
	statements.push_back("SELECT CASE WHEN count(*)=1 THEN true ELSE error('Changed setting lost ALTER registry') "
	                     "END FROM " +
	                     registry + " WHERE view_sql_name='First;MV' AND refresh_interval=7200");
	statements.push_back("SELECT CASE WHEN count(*)=" + string(existing_metadata ? "3" : "2") +
	                     " THEN true ELSE error('Setting change split transaction registry') END FROM " + registry);
	const string no_other_registry =
	    "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Setting change created another registry') END "
	    "FROM duckdb_tables() WHERE database_name='memory' AND table_name='openivm_views' "
	    "AND schema_name<>'Control.Space'";
	statements.push_back(no_other_registry);
	statements.push_back("SELECT CASE WHEN count(*)=0 THEN true ELSE error('Setting change created another schema') "
	                     "END FROM duckdb_schemas() WHERE schema_name='Different;Control'");
	ExecuteBatch(owner, statements, batched);
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	// Avoid a requested autocommit migration obscuring the transaction outcome.
	Execute(owner, "SET openivm_metadata_schema='';");
	Execute(owner, no_other_registry + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(commit ? "2" : "0") +
	                   " THEN true ELSE error('Setting transaction view outcome mismatch') END FROM duckdb_views() "
	                   "WHERE database_name='memory' AND schema_name='Public;Space';");
	vector<pair<string, string>> survivors;
	if (commit) {
		Execute(owner, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Setting transaction lost ALTER commit') "
		               "END FROM " +
		                   registry + " WHERE view_sql_name='First;MV' AND refresh_interval=7200;");
		survivors.emplace_back(first, "SELECT i FROM main.source WHERE i<>1");
		survivors.emplace_back(second, "SELECT * FROM main.source");
	} else if (!existing_metadata) {
		Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Setting rollback leaked schemas') END "
		               "FROM duckdb_schemas() WHERE schema_name IN ('Control.Space','Internal Space');");
		// Commit/rollback must discard the old transaction's recorded location.
		Execute(owner, "SET openivm_metadata_schema='After;Control'; BEGIN;");
		Execute(owner, "CREATE MATERIALIZED VIEW main.after_mv AS SELECT * FROM main.source;");
		Execute(owner, "COMMIT; SET openivm_metadata_schema='';");
		Execute(owner, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Rollback retained stale metadata location') "
		               "END FROM memory.\"After;Control\".openivm_views WHERE view_sql_name='after_mv';");
		survivors.emplace_back("main.after_mv", "SELECT * FROM main.source");
	} else {
		Execute(owner, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Setting rollback changed sibling registry') "
		               "END FROM " +
		                   registry + " WHERE view_sql_name='sibling';");
	}
	if (existing_metadata) {
		survivors.emplace_back("main.sibling", "SELECT * FROM main.source");
	}
	Execute(owner, "INSERT INTO main.source VALUES (1),(2),(3),(3); DELETE FROM main.source WHERE i=1; "
	               "UPDATE main.source SET i=4 WHERE i=2; INSERT INTO main.source VALUES (4),(5),(5); "
	               "DELETE FROM main.source WHERE i=5;");
	for (auto &survivor : survivors) {
		Execute(owner, "PRAGMA refresh(" + Value(survivor.first).ToSQLString() + ");");
		CheckBag(owner, "SELECT * FROM " + survivor.first, survivor.second,
		         "Setting transaction survivor bag mismatch");
	}
	for (auto &survivor : survivors) {
		Execute(owner, "DROP VIEW " + survivor.first + ";");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Setting transaction cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

static void TestFailedCreateSourceDelta(bool dedicated_schema, bool peer_view, bool shared) {
	std::cerr << "CASE failed CREATE dedicated_schema=" << dedicated_schema << " peer_view=" << peer_view
	          << " shared=" << shared << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1000),(1),(1),(2); ATTACH ':memory:' AS peer;");
	if (dedicated_schema) {
		Execute(owner, "SET openivm_internal_schema='Internal Space';");
	}
	if (shared) {
		Execute(owner, "CREATE MATERIALIZED VIEW sibling AS SELECT * FROM main.source;");
	}
	const string view = peer_view ? "peer.main.mv" : "main.mv";
	auto failed =
	    owner.Query("CREATE MATERIALIZED VIEW " + view + " AS SELECT i::TINYINT AS original FROM main.source;");
	if (!failed->HasError() || failed->GetError().find("1000") == string::npos) {
		throw InvalidInputException(
		    "Failed CREATE fixture did not reach the intended initial-load conversion error: %s",
		    failed->HasError() ? failed->GetError() : string("CREATE unexpectedly succeeded"));
	}
	const auto delta_name =
	    dedicated_schema ? RefreshMetadata::InternalSourceDeltaName("main", "source") : SqlUtils::DeltaName("source");
	const string schema = dedicated_schema ? "Internal Space" : "main";
	const string allocations =
	    " FROM duckdb_tables() WHERE database_name='memory' AND schema_name=" + Value(schema).ToSQLString() +
	    " AND starts_with(table_name," + Value(delta_name).ToSQLString() + ")";
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(shared ? "1" : "0") +
	                   " THEN true ELSE error('Failed CREATE leaked or removed a shared source delta') END" +
	                   allocations);
	Execute(owner, "DELETE FROM main.source WHERE i=1000; CREATE MATERIALIZED VIEW " + view +
	                   " AS SELECT i::TINYINT AS original FROM main.source;");
	Execute(owner,
	        "SELECT CASE WHEN count(*)=1 THEN true ELSE error('CREATE retry allocated a second source delta') END" +
	            allocations);
	Execute(owner, "INSERT INTO main.source VALUES (1),(2),(3),(3); DELETE FROM main.source WHERE i=1; "
	               "UPDATE main.source SET i=4 WHERE i=2; INSERT INTO main.source VALUES (5),(5); "
	               "DELETE FROM main.source WHERE i=5;");
	for (auto &survivor : shared ? vector<string> {view, "main.sibling"} : vector<string> {view}) {
		Execute(owner, "PRAGMA refresh(" + Value(survivor).ToSQLString() + ");");
		CheckBag(owner, "SELECT * FROM " + survivor, "SELECT i::TINYINT FROM main.source",
		         "Failed CREATE retry survivor bag mismatch");
	}
	Execute(owner, "DROP VIEW " + view + ";");
	if (shared) {
		Execute(owner, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Retry DROP removed sibling delta') END" +
		                   allocations);
		Execute(owner, "DROP VIEW main.sibling;");
	}
	Execute(owner,
	        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Final DROP leaked failed-CREATE allocation') END" +
	            allocations);
}

int main(int argc, char **argv) {
	if (argc != 2) {
		std::cerr << "Usage: source_delta_publication_test <fresh-database-path>" << std::endl;
		return 2;
	}
	try {
		TestInternalSchemaReopen(argv[1]);
		TestSharedMetadataUpgrade(false, true);
		TestSharedMetadataUpgrade(true, true);
		TestSharedMetadataUpgrade(true, false);
		TestMetadataSchemaMigration(false);
		TestMetadataSchemaMigration(true);
		for (bool commit : {false, true}) {
			for (bool existing : {false, true}) {
				for (bool batched : {false, true}) {
					TestMetadataSchemaSettingTransaction(existing, batched, commit);
					TestCreateAlterTransaction(existing, batched, commit);
				}
			}
			TestCrossCatalogSharedDelta(true, commit);
			TestCrossCatalogPublicationConsumers(commit, "Internal Space", true);
			TestSchemaCascade(true, commit, false);
			TestMixedLifecycleBatch(true, commit);
		}
		TestSchemaCascade(false, true, false);
		TestSchemaCascade(false, true, true);
		TestMixedLifecycleBatch(false, true);
		for (bool peer : {false, true}) {
			TestFailedCreateSourceDelta(true, peer, false);
		}
		TestFailedCreateSourceDelta(true, true, true);
		TestPublication(false);
		TestPublication(true);
		for (bool dedicated : {false, true}) {
			for (const auto &operation : vector<string> {"insert", "delete", "update"}) {
				TestDMLDuringPublication(dedicated, operation, false, true);
			}
			TestDMLDuringPublication(dedicated, "insert", true, true);
			TestDMLDuringPublication(dedicated, "insert", true, false);
			TestDMLDuringPublication(dedicated, "insert", false, true, false);
		}
		TestDropDuringPublication(false, false);
		TestDropDuringPublication(true, false);
		TestDropDuringPublication(false, true);
		TestDropDuringPublication(false, false, true);
		TestDropDuringPublication(true, false, true);
		TestDropDuringPublication(false, true, true);
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
