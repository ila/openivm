#include "duckdb.hpp"
#include "core/parser.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/parser_ddl.hpp"
#include "core/published_view.hpp"
#include "compile_facts.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/binder.hpp"
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

static void RefreshAndCheckBag(Connection &owner, const string &view, const string &query) {
	Execute(owner, "PRAGMA refresh(" + Value(view).ToSQLString() + ");");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + query + ") OR EXISTS (" + query +
	                   " EXCEPT ALL SELECT * FROM " + view +
	                   ") THEN error('Publication owner survivor bag mismatch') ELSE true END;");
}

static void TestQuotedNamespaceNormalization() {
	const string sql = "CREATE MATERIALIZED VIEW \"Peer'Catalog\".\"Schema\"\"Name\".\"View.Name\" "
	                   "AS SELECT 'String''Value' AS \"Column'Name\";";
	const string expected = "create materialized view \"Peer'Catalog\".\"Schema\"\"Name\".\"View.Name\" "
	                        "as select 'String''Value' as \"Column'Name\";";
	if (SqlUtils::SQLToLowercase(sql) != expected) {
		throw InvalidInputException("CREATE normalization changed a quoted namespace or literal");
	}
	// Native literal continuation permits whitespace and line comments, but not
	// block comments. Header normalization must not silently accept invalid bodies.
	Parser native_parser;
	bool native_rejected = false;
	try {
		native_parser.ParseQuery("SELECT 'c' /* nested /* ; */ comment */\n'd' AS joined");
	} catch (const ParserException &) {
		native_rejected = true;
	}
	bool lifecycle_rejected = false;
	try {
		ParseMaterializedViewStatement("CREATE MATERIALIZED VIEW mv AS SELECT 'c' /* nested /* ; */ comment */\n"
		                               "'d' AS joined",
		                               SqlDialect::DUCKDB);
	} catch (const ParserException &) {
		lifecycle_rejected = true;
	}
	if (!native_rejected || !lifecycle_rejected) {
		throw InvalidInputException(
		    "Lifecycle parsing must preserve native rejection of block-comment literal continuation");
	}
}

static void TestQualifiedTableReferences() {
	// These two physical identities flatten to the same text if quoted dots are
	// mistaken for qualification. Only the maintenance table may receive a pin.
	const string data_table = "\"Lake\".\"Internal.Space\".\"maintenance\"";
	const string other_table = "\"Lake.Internal\".\"Space\".\"maintenance\"";
	const string reference = "lake.\"Internal.Space\".maintenance";
	const string query = "SELECT * FROM " + reference + " JOIN " + other_table + " USING(i)";
	const string pinned_table = data_table + " AT (VERSION => 7)";
	const string expected = "SELECT * FROM " + pinned_table + " JOIN " + other_table + " USING(i)";
	if (SqlUtils::FindTableReference(query, data_table) != reference ||
	    SqlUtils::CountTableReferences(query, data_table) != 1 ||
	    SqlUtils::ReplaceTableReferences(query, data_table, pinned_table) != expected ||
	    SqlUtils::ReplaceTableReferences(expected, data_table, pinned_table) != expected) {
		throw InvalidInputException("Publication pinning lost quoted catalog/schema identity");
	}
	const string escaped_table = SqlUtils::FullName("Lake\"Catalog", "Internal Space", "maintenance");
	const string escaped_query = "SELECT * FROM " + escaped_table;
	if (SqlUtils::ReplaceTableReferences(escaped_query, escaped_table, pinned_table) !=
	    "SELECT * FROM " + pinned_table) {
		throw InvalidInputException("Publication pinning did not match escaped identifiers");
	}
	// Unqualified names still match a qualified reference. Catalog callers quote
	// bare names containing punctuation before passing them to the SQL matcher.
	if (SqlUtils::FindTableReference(query, "maintenance") != reference ||
	    SqlUtils::FindTableReference("SELECT * FROM lake.\"Internal Space\".\"Table Name\"",
	                                 SqlUtils::QuoteIdentifier("Table Name")) !=
	        "lake.\"Internal Space\".\"Table Name\"") {
		throw InvalidInputException("Table-reference matching lost unqualified table names");
	}
	const string dotted_query = "SELECT * FROM lake.main.\"Table.Name\"";
	if (SqlUtils::FindTableReference(dotted_query, SqlUtils::QuoteIdentifier("Table.Name")) !=
	        "lake.main.\"Table.Name\"" ||
	    !SqlUtils::FindTableReference(dotted_query, "Table.Name").empty()) {
		throw InvalidInputException("Table-reference matching confused a quoted table name with qualification");
	}
}

static void TestIdentifierComparisons() {
	std::cerr << "CASE ASCII-only identifier comparisons" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	for (const auto &collation : vector<string> {"binary", "nocase"}) {
		Execute(owner, "SET default_collation=" + Value(collation).ToSQLString() + ";");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT 1 FROM (VALUES "
		               "('Items','iTEMS',true), ('Ä','Ä',true), ('Ä','ä',false), "
		               "('ÄItems','ÄiTEMS',true), ('ÄItems','äiTEMS',false), "
		               "('Schema\"Name','sCHEMA\"nAME',true), (NULL,'items',NULL)) v(l,r,expected) WHERE " +
		                   SqlUtils::IdentifierEqualsSQL("l", "r") +
		                   " IS DISTINCT FROM expected) THEN error('Identifier comparison differs from catalog rules') "
		                   "ELSE true END;");
		const auto qualified_key = SqlUtils::FullName("ÄCatalog", "ÄSchema", "openivm_delta_ÄItems");
		for (const auto &key : vector<string> {"openivm_delta_ÄItems", qualified_key}) {
			Execute(owner,
			        "SELECT CASE WHEN EXISTS (SELECT 1 FROM (VALUES "
			        "('openivm_delta_ÄItems','ÄCatalog','ÄSchema',true), "
			        "('OPENIVM_DELTA_ÄITEMS','ÄCATALOG','ÄSCHEMA',true), "
			        "('openivm_delta_äItems','ÄCatalog','ÄSchema',false), "
			        "('openivm_delta_ÄItems','äCatalog','ÄSchema',false), "
			        "('openivm_delta_ÄItems','ÄCatalog','äSchema',false), "
			        "('openivm_delta_ÄItems',NULL,NULL,true), (" +
			            Value(qualified_key).ToSQLString() +
			            ",'ÄCatalog','ÄSchema',true)) v(table_name,source_catalog,source_schema,expected) WHERE (" +
			            RefreshMetadata::SourcePredicate(key, "ÄCatalog", "ÄSchema") +
			            ") IS DISTINCT FROM expected) THEN error('Source predicate merged distinct identifiers') "
			            "ELSE true END;");
		}
	}
}

static void TestTableColumnIdentity() {
	std::cerr << "CASE table-column lookup preserves physical identifier identity" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "ATTACH ':memory:' AS \"ÄCatalog\"; ATTACH ':memory:' AS \"äCatalog\"; "
	               "CREATE SCHEMA \"ÄCatalog\".\"ÄSchema\"; CREATE SCHEMA \"ÄCatalog\".\"äSchema\"; "
	               "CREATE SCHEMA \"äCatalog\".\"ÄSchema\"; "
	               "CREATE TABLE \"ÄCatalog\".\"ÄSchema\".\"ÄItems\"(first INTEGER, second INTEGER); "
	               "CREATE TABLE \"ÄCatalog\".\"ÄSchema\".\"äItems\"(other_table INTEGER); "
	               "CREATE TABLE \"ÄCatalog\".\"äSchema\".\"ÄItems\"(other_schema INTEGER); "
	               "CREATE TABLE \"äCatalog\".\"ÄSchema\".\"ÄItems\"(other_catalog INTEGER);");
	RefreshMetadata metadata(owner);
	for (const auto &collation : vector<string> {"binary", "nocase"}) {
		Execute(owner, "SET default_collation=" + Value(collation).ToSQLString() + ";");
		// ASCII spelling differences bind to the same object; Unicode case and
		// an application collation must not merge distinct physical identities.
		if (metadata.GetTableColumns("ÄcATALOG", "ÄsCHEMA", "ÄiTEMS") != vector<string> {"first", "second"} ||
		    metadata.GetTableColumns("ÄcATALOG", "ÄsCHEMA", "äiTEMS") != vector<string> {"other_table"} ||
		    metadata.GetTableColumns("ÄcATALOG", "äsCHEMA", "ÄiTEMS") != vector<string> {"other_schema"} ||
		    metadata.GetTableColumns("äcATALOG", "ÄsCHEMA", "ÄiTEMS") != vector<string> {"other_catalog"} ||
		    !metadata.GetTableColumns("ÄcATALOG", "ÄsCHEMA", "missing").empty()) {
			throw InvalidInputException("Table-column lookup changed physical identity or column order");
		}
	}
}

// Unlike SQLLogicTest's restart, separate DuckDB instances with no supplied
// DBConfig cannot carry extension settings across the close/reopen boundary.
static void TestInternalSchemaReopen(const string &path) {
	std::cerr << "CASE internal schema reopen with fresh configuration" << std::endl;
	{
		DuckDB db(path);
		Connection owner(db);
		Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
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
			Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + qualified +
			                   " EXCEPT ALL SELECT * FROM \"Source.Space\".items) "
			                   "OR EXISTS (SELECT * FROM \"Source.Space\".items EXCEPT ALL SELECT * FROM " +
			                   qualified + ") THEN error('Reopened MV bag mismatch') ELSE true END;");
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
	auto parsed = ParseMaterializedViewStatement("CREATE MATERIALIZED VIEW first_mv AS SELECT * FROM source;",
	                                             SqlDialect::DUCKDB);
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
		Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM " + string(view) +
		                      " EXCEPT ALL SELECT * FROM source) OR EXISTS (SELECT * FROM source EXCEPT ALL "
		                      "SELECT * FROM " +
		                      string(view) + ") THEN error('Concurrent MV bag mismatch') ELSE true END;");
	}
}

// A writer must not decide that a source is untracked between its physical
// delta allocation and its first committed ownership row. Pin the writer's
// metadata snapshot in that interval, then wait for its actual admission wait.
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
	auto parsed = ParseMaterializedViewStatement(
	    "CREATE MATERIALIZED VIEW " + view + " AS SELECT * FROM " + source + ";", SqlDialect::DUCKDB);
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

static void TestDMLBeforePublication(bool dedicated_schema, bool commit) {
	std::cerr << "CASE DML before publication dedicated_schema=" << dedicated_schema << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	const string delta_schema = dedicated_schema ? "Internal Space" : "main";
	const string unowned_name = RefreshMetadata::SourceDeltaTableName("main", "source", delta_schema);
	const string unowned = SqlUtils::FullName("memory", delta_schema, unowned_name);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; "
	                 "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2); "
	                     "CREATE SCHEMA IF NOT EXISTS " +
	                     SqlUtils::QuoteIdentifier(delta_schema) + "; CREATE TABLE " + unowned +
	                     "(i INTEGER, openivm_multiplicity INTEGER, openivm_timestamp TIMESTAMP); "
	                     "INSERT INTO " +
	                     unowned + " VALUES (99,1,TIMESTAMP '2000-01-01');");
	Execute(writer, "BEGIN; INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	                "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (5),(5); "
	                "DELETE FROM source WHERE i=5;");
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*creator.context));
	auto competing = std::async(std::launch::async,
	                            [&]() { Execute(creator, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;"); });
	try {
		MutationGateTestAccess::WaitForBlockedOwner(*gate, SqlUtils::FullName("memory", "main", "source"), true);
		Execute(writer, commit ? "COMMIT;" : "ROLLBACK;");
	} catch (const std::exception &) {
		if (writer.context->transaction.HasActiveTransaction()) {
			writer.context->transaction.Rollback(nullptr);
		}
		competing.wait();
		throw;
	}
	competing.get();
	Execute(observer, "SELECT CASE WHEN count(*)=1 AND min(i)=99 AND min(openivm_multiplicity)=1 "
	                  "AND min(openivm_timestamp)=TIMESTAMP '2000-01-01' "
	                  "THEN true ELSE error('Unowned namesake adopted as a source delta') END FROM " +
	                      unowned + ";");
	Execute(observer, "SELECT CASE WHEN count(*)=1 AND min(delta_table_name)<>" + Value(unowned_name).ToSQLString() +
	                      " THEN true ELSE error('First CREATE did not allocate an owned source delta') END "
	                      "FROM \"Control.Space\".openivm_delta_tables;");
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	                  "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (5),(5); "
	                  "DELETE FROM source WHERE i=5;");
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
	Execute(observer, "SELECT CASE WHEN count(*)=1 AND min(table_name)=" + Value(unowned_name).ToSQLString() +
	                      " THEN true ELSE error('Final cleanup removed the unowned table or leaked a delta') END "
	                      "FROM duckdb_tables() WHERE schema_name=" +
	                      Value(delta_schema).ToSQLString() + " AND starts_with(table_name,'openivm_delta_');");
}

static void TestDMLWithInvisibleDelta(bool dedicated_schema) {
	std::cerr << "CASE DML with invisible delta dedicated_schema=" << dedicated_schema << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; "
	                 "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2);");
	Execute(writer, "BEGIN; SELECT * FROM source;");
	Execute(creator, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	auto failed = writer.Query("INSERT INTO source VALUES (77);");
	if (!failed->HasError() || failed->GetError().find("retry the transaction") == string::npos) {
		throw InvalidInputException("DML with an invisible committed delta must request a transaction retry");
	}
	Execute(writer, "ROLLBACK;");
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM (VALUES (1),(1),(2))) "
	                  "OR EXISTS (SELECT * FROM (VALUES (1),(1),(2)) EXCEPT ALL SELECT * FROM source) "
	                  "THEN error('Rejected DML changed its source') ELSE true END;");
	Execute(writer, "BEGIN; INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	                "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (5),(5); "
	                "DELETE FROM source WHERE i=5; COMMIT;");
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
}

static string ConflictingSourceDML(const string &source) {
	return "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source + " WHERE i=1; UPDATE " + source +
	       " SET i=4 WHERE i=2; INSERT INTO " + source + " VALUES (5),(5); DELETE FROM " + source + " WHERE i=5;";
}

static string SourceAdmissionDML(const string &operation) {
	if (operation == "insert") {
		return "INSERT INTO source VALUES (99)";
	}
	if (operation == "delete") {
		return "DELETE FROM source WHERE i=1";
	}
	if (operation == "update") {
		return "UPDATE source SET i=99 WHERE i=2";
	}
	return "MERGE INTO source USING (VALUES (2),(99)) v(i) ON source.i=v.i "
	       "WHEN MATCHED THEN UPDATE SET i=100 WHEN NOT MATCHED THEN INSERT VALUES(v.i)";
}

static void TestPlanningOnlySourceAdmission(bool dedicated_schema, bool tracked, const string &operation,
                                            const string &mode, bool explicit_writer) {
	std::cerr << "CASE planning-only source admission dedicated_schema=" << dedicated_schema << " tracked=" << tracked
	          << " operation=" << operation << " mode=" << mode << " explicit_writer=" << explicit_writer << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	                 "SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE TABLE source(i INTEGER); CREATE TABLE other(i INTEGER); "
	                     "INSERT INTO source VALUES (1),(1),(2); INSERT INTO other VALUES (1),(1),(2); "
	                     "CREATE SCHEMA \"Prior Internal\"; CREATE TABLE seed(i INTEGER); "
	                     "CREATE MATERIALIZED VIEW bootstrap AS SELECT * FROM seed; DROP VIEW bootstrap;");
	// Pin only the source snapshot before another connection plans DML. For the
	// tracked case, ownership comes and goes outside this snapshot; use another
	// maintenance schema to avoid an unrelated native catalog tombstone conflict.
	Execute(creator, "BEGIN; SELECT * FROM source;");
	if (tracked) {
		Execute(writer, "SET openivm_internal_schema='Prior Internal'; "
		                "CREATE MATERIALIZED VIEW prior_mv AS SELECT * FROM source;");
	}
	if (explicit_writer) {
		Execute(writer, "BEGIN;");
	}
	const auto dml = SourceAdmissionDML(operation);
	if (mode == "explain") {
		Execute(writer, "EXPLAIN " + dml);
	} else if (mode == "prepare") {
		Execute(writer, "PREPARE planned_dml AS " + dml);
	} else {
		auto prepared = writer.Prepare(dml);
		if (prepared->HasError()) {
			throw InvalidInputException("Planning-only native preparation failed: %s", prepared->GetError());
		}
	}
	// Executing another source in the same transaction must not turn a retained
	// planning-only admission into an executed write for the explained source.
	Execute(writer, ConflictingSourceDML("other"));
	if (explicit_writer) {
		Execute(writer, "COMMIT;");
	}
	if (tracked) {
		RefreshAndCheckBag(writer, "prior_mv", "SELECT * FROM source");
		Execute(writer, "DROP VIEW prior_mv;");
	}
	Execute(creator, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source; COMMIT;");
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM (VALUES (1),(1),(2))) "
	                  "OR EXISTS (SELECT * FROM (VALUES (1),(1),(2)) EXCEPT ALL SELECT * FROM source) "
	                  "THEN error('Planning-only DML changed its source') ELSE true END;");
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, ConflictingSourceDML("source"));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
}

static void TestExecutedSourceAdmission(bool dedicated_schema, const string &operation, const string &mode,
                                        bool commit) {
	std::cerr << "CASE executed source admission dedicated_schema=" << dedicated_schema << " operation=" << operation
	          << " mode=" << mode << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	                 "SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2);");
	const auto dml = SourceAdmissionDML(operation);
	unique_ptr<PreparedStatement> prepared;
	if (mode == "api_execute") {
		// Preparation ends its own transaction. Execution must reacquire admission
		// even if DuckDB reuses the physical plan without another optimizer pass.
		prepared = writer.Prepare(dml);
		if (prepared->HasError()) {
			throw InvalidInputException("Native preparation failed: %s", prepared->GetError());
		}
	} else if (mode == "execute") {
		Execute(writer, "PREPARE planned_dml AS " + dml);
	}
	Execute(creator, "BEGIN; SELECT * FROM source;");
	Execute(writer, "BEGIN;");
	if (mode == "api_execute") {
		auto executed = prepared->Execute();
		if (executed->HasError()) {
			throw InvalidInputException("Native prepared DML execution failed: %s", executed->GetError());
		}
	} else {
		Execute(writer, mode == "execute" ? "EXECUTE planned_dml;" : "EXPLAIN ANALYZE " + dml);
	}
	// A later planning-only statement must not erase an executed write either.
	Execute(writer, "EXPLAIN UPDATE source SET i=77 WHERE i=99;");
	// Isolate the commit marker from this execution: another ordinary source
	// write here would hide a missing EXPLAIN ANALYZE or cached-plan marker.
	Execute(writer, commit ? "COMMIT;" : "ROLLBACK;");
	auto created = creator.Query("CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	if (commit) {
		if (!created->HasError() || created->GetError().find("retry the transaction") == string::npos) {
			throw InvalidInputException("Executed source DML lost its commit marker after planning-only preparation");
		}
		Execute(creator, "ROLLBACK; CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	} else {
		if (created->HasError()) {
			throw InvalidInputException("Rolled-back executed DML prevented first CREATE: %s", created->GetError());
		}
		Execute(creator, "COMMIT;");
	}
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, ConflictingSourceDML("source"));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
}

static void TestFirstCreateWithOldSourceSnapshot(bool dedicated_schema, bool commit, bool pending_writer = false) {
	std::cerr << "CASE first CREATE with old source snapshot dedicated_schema=" << dedicated_schema
	          << " commit=" << commit << " pending_writer=" << pending_writer << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	                 "SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2);");
	Execute(creator, "BEGIN; SELECT * FROM source;");
	Execute(writer, "BEGIN; " + ConflictingSourceDML("source"));
	unique_ptr<MaterializedQueryResult> created;
	if (pending_writer) {
		auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*creator.context));
		auto creating = std::async(std::launch::async, [&]() {
			return creator.Query("CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
		});
		try {
			MutationGateTestAccess::WaitForBlockedOwner(*gate, SqlUtils::FullName("memory", "main", "source"), true);
			Execute(writer, commit ? "COMMIT;" : "ROLLBACK;");
		} catch (const std::exception &) {
			if (writer.context->transaction.HasActiveTransaction()) {
				writer.context->transaction.Rollback(nullptr);
			}
			creating.wait();
			throw;
		}
		created = creating.get();
	} else {
		Execute(writer, commit ? "COMMIT;" : "ROLLBACK;");
		created = creator.Query("CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	}
	if (commit) {
		if (!created->HasError() || created->GetError().find("retry the transaction") == string::npos) {
			throw InvalidInputException("First CREATE silently accepted uncaptured commits newer than its snapshot");
		}
		Execute(creator, "ROLLBACK; CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	} else {
		if (created->HasError()) {
			throw InvalidInputException("Rolled-back untracked DML prevented first CREATE: %s", created->GetError());
		}
		Execute(creator, "COMMIT;");
	}
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, ConflictingSourceDML("source"));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
}

static void TestFirstCreateAfterNonSourceCommits(bool dedicated_schema, bool commit) {
	std::cerr << "CASE first CREATE after non-source commits dedicated_schema=" << dedicated_schema
	          << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	                 "SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE SCHEMA \"Other.Space\"; CREATE TABLE source(i INTEGER); "
	                     "CREATE TABLE \"Other.Space\".source(i INTEGER); "
	                     "INSERT INTO source VALUES (1),(1),(2); "
	                     "INSERT INTO \"Other.Space\".source VALUES (1),(1),(2);");
	// A native source snapshot must survive read-only helper commits and writes
	// to a different qualified source in the same catalog. Neither needs capture
	// for this MV, even though DuckDB's catalog commit counter advances.
	Execute(creator, "BEGIN; SELECT * FROM source;");
	Execute(observer, "SELECT * FROM source;");
	Execute(writer, "BEGIN; " + ConflictingSourceDML("\"Other.Space\".source") + (commit ? " COMMIT;" : " ROLLBACK;"));
	Execute(creator, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source; COMMIT;");
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, ConflictingSourceDML("source"));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
}

static void TestFirstCreateAfterTrackedConsumerDrop(bool dedicated_schema, bool commit) {
	std::cerr << "CASE first CREATE after tracked consumer drop dedicated_schema=" << dedicated_schema
	          << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	                 "SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2); "
	                     "CREATE SCHEMA \"Prior Internal\"; CREATE TABLE seed(i INTEGER); "
	                     "CREATE MATERIALIZED VIEW bootstrap AS SELECT * FROM seed; DROP VIEW bootstrap;");
	Execute(writer, "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Prior Internal';");
	// This snapshot predates all ownership of source, so after final DROP neither
	// its local registry nor a committed probe can reveal the intervening writes.
	// Different maintenance locations avoid a separate native catalog conflict
	// when the older transaction allocates after that DROP's newer tombstone.
	Execute(creator, "BEGIN; SELECT * FROM source;");
	Execute(writer, "CREATE MATERIALIZED VIEW prior_mv AS SELECT * FROM source;");
	Execute(writer, "BEGIN; " + ConflictingSourceDML("source") + (commit ? " COMMIT;" : " ROLLBACK;"));
	RefreshAndCheckBag(writer, "prior_mv", "SELECT * FROM source");
	Execute(writer, "DROP VIEW prior_mv;");
	auto created = creator.Query("CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	if (commit) {
		if (!created->HasError() || created->GetError().find("retry the transaction") == string::npos) {
			throw InvalidInputException("Final-consumer cleanup hid committed source writes from stale CREATE");
		}
		Execute(creator, "ROLLBACK; CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	} else {
		if (created->HasError()) {
			throw InvalidInputException("Final-consumer cleanup rejected unchanged source snapshot: %s",
			                            created->GetError());
		}
		Execute(creator, "COMMIT;");
	}
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, ConflictingSourceDML("source"));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
}

static pair<string, string> CreateIndependentSources(Connection &con, bool dedicated_schema, bool cross_catalog) {
	const string source = "memory.\"Source.Space\".\"Items;Table\"";
	const string independent =
	    cross_catalog ? "peer.\"Source.Space\".\"Items;Table\"" : "memory.\"Other.Space\".\"Items;Table\"";
	Execute(con, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	             "SET openivm_internal_schema=" +
	                 Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                 "; ATTACH ':memory:' AS peer; CREATE SCHEMA \"Source.Space\"; "
	                 "CREATE SCHEMA \"Other.Space\"; CREATE SCHEMA peer.\"Source.Space\"; CREATE TABLE " +
	                 source + "(i INTEGER); CREATE TABLE " + independent + "(i INTEGER); INSERT INTO " + source +
	                 " VALUES (1),(1),(2); INSERT INTO " + independent + " VALUES (1),(1),(2);");
	return {source, independent};
}

static void TestIndependentUntrackedWriters(bool cross_catalog, bool commit) {
	std::cerr << "CASE independent untracked writers cross_catalog=" << cross_catalog << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection first(db), second(db), observer(db);
	auto sources = CreateIndependentSources(first, true, cross_catalog);
	const auto &source = sources.first;
	const auto &independent = sources.second;
	Execute(first, "BEGIN; " + ConflictingSourceDML(source));
	auto competing = std::async(std::launch::async,
	                            [&]() { Execute(second, "BEGIN; " + ConflictingSourceDML(independent) + " COMMIT;"); });
	// Completion must precede release of the first transaction. The deadline
	// only bounds failure when a regression serializes these independent writers.
	bool completed = competing.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
	Execute(first, commit && completed ? "COMMIT;" : "ROLLBACK;");
	competing.get();
	if (!completed) {
		throw InvalidInputException("Independent untracked writer could not commit while another remained open");
	}
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM " + independent +
	                      " EXCEPT ALL SELECT * FROM (VALUES (3),(3),(4),(4))) OR EXISTS "
	                      "(SELECT * FROM (VALUES (3),(3),(4),(4)) EXCEPT ALL SELECT * FROM " +
	                      independent + ") THEN error('Independent writer lost committed rows') ELSE true END;");
	const string expected = commit ? "(VALUES (3),(3),(4),(4))" : "(VALUES (1),(1),(2))";
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM " + source + " EXCEPT ALL SELECT * FROM " + expected +
	                      ") OR EXISTS (SELECT * FROM " + expected + " EXCEPT ALL SELECT * FROM " + source +
	                      ") THEN error('Open untracked writer lost commit/rollback isolation') ELSE true END;");
	Execute(first, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM " + source + ";");
	Execute(observer, ConflictingSourceDML(source) + ConflictingSourceDML(independent));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM " + source);
	Execute(observer, "DROP VIEW mv;");
}

static void TestIndependentWriterDuringPublication(bool dedicated_schema, bool cross_catalog) {
	std::cerr << "CASE independent writer during publication dedicated_schema=" << dedicated_schema
	          << " cross_catalog=" << cross_catalog << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	auto sources = CreateIndependentSources(creator, dedicated_schema, cross_catalog);
	const auto &source = sources.first;
	const auto &independent = sources.second;
	creator.context->transaction.BeginTransaction();
	auto parsed = ParseMaterializedViewStatement("CREATE MATERIALIZED VIEW mv AS SELECT * FROM " + source + ";",
	                                             SqlDialect::DUCKDB);
	auto planned =
	    MaterializedViewParserExtension::PlanFunction(nullptr, *creator.context, std::move(parsed.parse_data));
	auto competing = std::async(std::launch::async,
	                            [&]() { Execute(writer, "BEGIN; " + ConflictingSourceDML(independent) + " COMMIT;"); });
	bool completed = competing.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
	if (!completed) {
		creator.context->transaction.Rollback(nullptr);
		competing.get();
		throw InvalidInputException("First MV publication blocked a writer of an independent qualified source");
	}
	competing.get();
	ExecuteStagedDDL(*creator.context, planned.parameters);
	creator.context->transaction.Commit();
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM " + independent +
	                      " EXCEPT ALL SELECT * FROM (VALUES (3),(3),(4),(4))) OR EXISTS "
	                      "(SELECT * FROM (VALUES (3),(3),(4),(4)) EXCEPT ALL SELECT * FROM " +
	                      independent +
	                      ") THEN error('Independent publication writer lost committed rows') ELSE true END;");
	Execute(observer, ConflictingSourceDML(source));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM " + source);
	Execute(observer, "DROP VIEW mv;");
}

static void TestConcurrentUntrackedAppends() {
	std::cerr << "CASE concurrent untracked appends to one source" << std::endl;
	DuckDB db(nullptr);
	Connection first(db), second(db), observer(db);
	Execute(first, "LOAD openivm; SET openivm_disable_daemon=true; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2); BEGIN; INSERT INTO source VALUES (10),(10),(11); "
	               "UPDATE source SET i=12 WHERE i=11; DELETE FROM source WHERE i=12;");
	auto competing = std::async(std::launch::async, [&]() {
		Execute(second, "BEGIN; INSERT INTO source VALUES (20),(20),(21); "
		                "UPDATE source SET i=22 WHERE i=21; DELETE FROM source WHERE i=22; COMMIT;");
	});
	bool completed = competing.wait_for(std::chrono::seconds(10)) == std::future_status::ready;
	Execute(first, completed ? "COMMIT;" : "ROLLBACK;");
	competing.get();
	if (!completed) {
		throw InvalidInputException("Source admission serialized nonconflicting writers of one untracked source");
	}
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM source EXCEPT ALL "
	                  "SELECT * FROM (VALUES (1),(1),(2),(10),(10),(20),(20))) OR EXISTS "
	                  "(SELECT * FROM (VALUES (1),(1),(2),(10),(10),(20),(20)) EXCEPT ALL SELECT * FROM source) "
	                  "THEN error('Concurrent untracked appends lost rows') ELSE true END;");
	Execute(observer, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source; " + ConflictingSourceDML("source"));
	RefreshAndCheckBag(observer, "mv", "SELECT * FROM source");
	Execute(observer, "DROP VIEW mv;");
}

static void TestSourceAdmissionLockOrder(bool dedicated_schema, bool tracked) {
	std::cerr << "CASE source admission lock order dedicated_schema=" << dedicated_schema << " tracked=" << tracked
	          << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), writer(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	                 "SET openivm_internal_schema=" +
	                     Value(dedicated_schema ? "Internal Space" : "").ToSQLString() +
	                     "; CREATE TABLE a(i INTEGER); CREATE TABLE b(i INTEGER); "
	                     "INSERT INTO a VALUES (1),(1),(2); INSERT INTO b VALUES (1),(1),(2);");
	if (tracked) {
		Execute(creator, "CREATE MATERIALIZED VIEW mv_b AS SELECT * FROM b;");
	}
	Execute(writer, "BEGIN; " + ConflictingSourceDML("a"));
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*creator.context));
	std::future<void> creating;
	ParserExtensionPlanResult planned;
	if (tracked) {
		creating = std::async(std::launch::async,
		                      [&]() { Execute(creator, "CREATE MATERIALIZED VIEW mv_a AS SELECT * FROM a;"); });
		try {
			MutationGateTestAccess::WaitForBlockedOwner(*gate, SqlUtils::FullName("memory", "main", "a"), true);
		} catch (const std::exception &) {
			Execute(writer, "ROLLBACK;");
			creating.wait();
			throw;
		}
	} else {
		creator.context->transaction.BeginTransaction();
		auto parsed =
		    ParseMaterializedViewStatement("CREATE MATERIALIZED VIEW mv_b AS SELECT * FROM b;", SqlDialect::DUCKDB);
		planned =
		    MaterializedViewParserExtension::PlanFunction(nullptr, *creator.context, std::move(parsed.parse_data));
	}
	// A has already been written without capture. Waiting for the publisher of B
	// (or its global gate) could form a lock-order cycle; this statement must fail
	// before changing B, and rollback must unblock publication of A.
	auto result = writer.Query("INSERT INTO b VALUES (77);");
	bool retry = result->HasError() && result->GetError().find("retry the transaction") != string::npos;
	Execute(writer, "ROLLBACK;");
	if (tracked) {
		creating.get();
	} else {
		ExecuteStagedDDL(*creator.context, planned.parameters);
		creator.context->transaction.Commit();
	}
	if (!retry) {
		throw InvalidInputException("Source admission lock-order conflict did not request a transaction retry");
	}
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM a EXCEPT ALL SELECT * FROM (VALUES (1),(1),(2))) "
	                  "OR EXISTS (SELECT * FROM (VALUES (1),(1),(2)) EXCEPT ALL SELECT * FROM a) "
	                  "OR EXISTS (SELECT * FROM b EXCEPT ALL SELECT * FROM (VALUES (1),(1),(2))) "
	                  "OR EXISTS (SELECT * FROM (VALUES (1),(1),(2)) EXCEPT ALL SELECT * FROM b) "
	                  "THEN error('Admission retry committed partial DML') ELSE true END;");
	Execute(observer, ConflictingSourceDML("a") + ConflictingSourceDML("b"));
	RefreshAndCheckBag(observer, "mv_b", "SELECT * FROM b");
	if (tracked) {
		RefreshAndCheckBag(observer, "mv_a", "SELECT * FROM a");
		Execute(observer, "DROP VIEW mv_a;");
	}
	Execute(observer, "DROP VIEW mv_b;");
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
	auto parsed = ParseMaterializedViewStatement("CREATE MATERIALIZED VIEW second_mv AS SELECT * FROM source;",
	                                             SqlDialect::DUCKDB);
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
	Execute(observer, "SELECT CASE WHEN EXISTS (SELECT * FROM second_mv EXCEPT ALL SELECT * FROM source) "
	                  "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM second_mv) "
	                  "THEN error('CREATE versus DROP MV bag mismatch') ELSE true END;");
}

// Lookup must finish after schema enumeration releases its lock, and preserve
// source identity when metadata, source and physical delta have different schemas.
static void TestSourceDeltaLookup() {
	std::cerr << "CASE source delta lookup across schemas" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE main.items(i INTEGER); "
	               "CREATE TABLE \"Source.Space\".items(i INTEGER);");
	Execute(owner, "CREATE MATERIALIZED VIEW main_mv AS SELECT * FROM main.items;");
	Execute(owner, "CREATE MATERIALIZED VIEW quoted_mv AS SELECT * FROM \"Source.Space\".items;");
	owner.context->RunFunctionInTransaction([&]() {
		for (const auto &schema : vector<string> {"main", "Source.Space"}) {
			auto delta = RefreshMetadata::FindSourceDelta(*owner.context, "memory", schema, "items");
			if (!delta || delta->schema.name != "Internal Space" ||
			    delta->name != RefreshMetadata::InternalSourceDeltaName(schema, "items")) {
				throw InvalidInputException("Source delta lookup lost its qualified physical identity");
			}
		}
		if (RefreshMetadata::FindSourceDelta(*owner.context, "memory", "Control.Space", "items")) {
			throw InvalidInputException("Source delta lookup reused another schema's same-named source");
		}
	});
	const vector<pair<string, string>> views = {{"main_mv", "main.items"}, {"quoted_mv", "\"Source.Space\".items"}};
	for (auto &view : views) {
		Execute(owner, "INSERT INTO " + view.second + " VALUES (1),(1),(2),(3); DELETE FROM " + view.second +
		                   " WHERE i=1; UPDATE " + view.second + " SET i=4 WHERE i=2; INSERT INTO " + view.second +
		                   " VALUES (1),(4),(5); DELETE FROM " + view.second + " WHERE i=5;");
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL SELECT * FROM " +
		                   view.second + ") OR EXISTS (SELECT * FROM " + view.second + " EXCEPT ALL SELECT * FROM " +
		                   view.first + ") THEN error('Source delta lookup MV bag mismatch') ELSE true END;");
	}
}

// Exercise initialization as well as discovery: upgrading a committed registry
// must not pin the caller's catalog snapshot before the helper's DDL commits.
// Explicit transactions instead own the upgrade and new schemas until commit.
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
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM mv EXCEPT ALL SELECT * FROM source) "
	               "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM mv) "
	               "THEN error('Upgraded registry MV bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW mv;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Upgraded registry cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Source planning changes the helper's search path without changing its selected
// metadata location. Probe both durable ownership and transaction-local shadows.
static void TestDeltaOwnershipSearchPath(bool replay) {
	std::cerr << "CASE delta ownership search path replay=" << replay << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".source(i INTEGER); "
	               "INSERT INTO \"Source.Space\".source VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv AS SELECT * FROM \"Source.Space\".source; "
	               "ATTACH ':memory:' AS \"Probe.Peer\"; CREATE SCHEMA \"Probe.Peer\".\"Source.Space\";");
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto key = RefreshMetadata(helper).FindViewKey("memory", "main", "mv");
	auto sources = RefreshMetadata(helper).GetDeltaSources(key, "memory", "main");
	if (sources.size() != 1) {
		throw InvalidInputException("Ownership search-path fixture must have one source");
	}
	if (replay) {
		Execute(owner, "BEGIN; CREATE MATERIALIZED VIEW sibling AS SELECT * FROM \"Source.Space\".source;");
		TransactionalMVMetadataState::Get(*owner.context).Apply(helper);
	}
	Execute(helper, "USE \"Probe.Peer\".\"Source.Space\";");
	RefreshMetadata::DeltaSource view_delta;
	view_delta.table_name = SqlUtils::DeltaName(key);
	view_delta.catalog_name = "memory";
	view_delta.schema_name = "Internal Space";
	if (!RefreshMetadata(helper).IsMaterializedViewDelta(view_delta) ||
	    RefreshMetadata(helper).IsMaterializedViewDelta(sources[0])) {
		throw InvalidInputException("Delta owner lookup followed the source search path");
	}
	// Inspect this helper's selected registry directly, as commit-time cleanup does.
	// In the replay case only the shadow contains the uncommitted sibling.
	auto registry = RefreshMetadata::MetadataTable(helper, openivm::DELTA_TABLES_TABLE);
	if (!RefreshMetadata::HasSourceDeltaConsumers(helper, sources[0]) ||
	    RefreshMetadata::HasSourceDeltaConsumers(helper, sources[0], nullptr, {{registry, Value(key).ToSQLString()}}) !=
	        replay) {
		throw InvalidInputException("Delta consumer lookup lost durable or replay ownership");
	}
	Execute(helper, "SELECT CASE WHEN current_database()='Probe.Peer' AND current_schema()='Source.Space' "
	                "THEN true ELSE error('Ownership inspection changed the helper search path') END;");
	if (replay) {
		Execute(owner, "ROLLBACK;");
	}
	Execute(owner, "INSERT INTO \"Source.Space\".source VALUES (1),(2),(3),(3); "
	               "DELETE FROM \"Source.Space\".source WHERE i=1; "
	               "UPDATE \"Source.Space\".source SET i=4 WHERE i=2;");
	Execute(owner, "PRAGMA refresh('mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM mv EXCEPT ALL SELECT * FROM \"Source.Space\".source) "
	               "OR EXISTS (SELECT * FROM \"Source.Space\".source EXCEPT ALL SELECT * FROM mv) "
	               "THEN error('Ownership search-path survivor bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW mv;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Ownership search-path cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Scratch DROP must bind after its CREATE, using the caller's temporary catalog
// even when a persistent same-named table and a separate metadata schema exist.
static void TestDeferredScratchDrop(bool explicit_transaction, bool commit, bool executor_drop) {
	std::cerr << "CASE deferred scratch drop explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << " executor_drop=" << executor_drop << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm;");
	Execute(owner, "SET openivm_disable_daemon=true; SET openivm_metadata_schema='Control.Space'; "
	               "SET openivm_internal_schema='Internal Space'; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2); CREATE TABLE \"Scratch;Name\"(i INTEGER); "
	               "INSERT INTO \"Scratch;Name\" VALUES (99); CREATE VIEW \"Scratch;View\" AS SELECT 99 AS i;");
	Execute(owner, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	Execute(owner, "USE memory.\"Control.Space\";");
	// Round-trip each physical DROP plan, including the direct executor route.
	// Query verification would execute a SELECT with side effects repeatedly;
	// serializer verification alone exercises the actual failing bind boundary.
	ClientConfig::GetConfig(*owner.context).verify_serializer = true;
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	auto drop = executor_drop ? "SELECT * FROM openivm_execute_drop_table('', '', 'Scratch;Name', false, false); "
	                          : "DROP TABLE \"Scratch;Name\"; ";
	Execute(owner, "CREATE TEMP TABLE \"Scratch;Name\" AS SELECT 7 AS i; "
	               "INSERT INTO \"Scratch;Name\" VALUES (8); " +
	                   string(drop) +
	                   "SELECT CASE WHEN count(*)=0 THEN true "
	                   "ELSE error('Deferred scratch DROP lost temporary identity') END "
	                   "FROM duckdb_tables() WHERE temporary AND table_name='Scratch;Name';");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM main.\"Scratch;Name\" EXCEPT ALL SELECT 99) "
	               "OR EXISTS (SELECT 99 EXCEPT ALL SELECT * FROM main.\"Scratch;Name\") "
	               "THEN error('Scratch DROP removed the persistent namesake') ELSE true END;");
	// Exercise the same resolved-target round trip for VIEW, with a persistent
	// namesake outside the active search path just as for TABLE above.
	auto view_drop = executor_drop ? "SELECT * FROM openivm_execute_drop_view('', '', 'Scratch;View', false, false); "
	                               : "DROP VIEW \"Scratch;View\"; ";
	Execute(owner, "CREATE TEMP VIEW \"Scratch;View\" AS SELECT 7 AS i; " + string(view_drop) +
	                   "SELECT CASE WHEN count(*)=0 THEN true "
	                   "ELSE error('Deferred scratch VIEW DROP lost temporary identity') END "
	                   "FROM duckdb_views() WHERE temporary AND view_name='Scratch;View';");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM main.\"Scratch;View\" EXCEPT ALL SELECT 99) "
	               "OR EXISTS (SELECT 99 EXCEPT ALL SELECT * FROM main.\"Scratch;View\") "
	               "THEN error('Scratch VIEW DROP removed the persistent namesake') ELSE true END;");
	Execute(owner, "DROP TABLE IF EXISTS \"Missing;Scratch\"; SELECT 1;");
	Execute(owner, "SELECT * FROM openivm_execute_drop_table('', '', 'Missing;Scratch', false, true);");
	Execute(owner, "SELECT * FROM openivm_execute_drop_view('', '', 'Missing;Scratch', false, true);");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	Execute(owner, "INSERT INTO main.source VALUES (1),(2),(3),(3); DELETE FROM main.source WHERE i=1; "
	               "UPDATE main.source SET i=4 WHERE i=2; INSERT INTO main.source VALUES (1),(4),(5); "
	               "DELETE FROM main.source WHERE i=5;");
	Execute(owner, "PRAGMA refresh('main.mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM main.mv EXCEPT ALL SELECT * FROM main.source) "
	               "OR EXISTS (SELECT * FROM main.source EXCEPT ALL SELECT * FROM main.mv) "
	               "THEN error('Deferred scratch DROP publication bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW main.mv; SELECT 1;");
}

// Fail on a wrong helper identity before acquiring the caller gate: otherwise
// the subsequent replayed ALTER would wait forever instead of reporting it.
static void TestMetadataReplayMutationOwner(bool commit) {
	std::cerr << "CASE metadata replay mutation owner commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db), competitor(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto key = RefreshMetadata(helper).FindViewKey("memory", "main", "mv");
	Execute(owner, "BEGIN;");
	auto &state = TransactionalMVMetadataState::Get(*owner.context);
	state.RegisterSQL(
	    CreateMetadataTableSQL(openivm::REFRESH_HOOKS_TABLE, "memory.\"Control.Space\".openivm_refresh_hooks") +
	        "; ALTER TABLE memory.\"Control.Space\".openivm_refresh_hooks "
	        "ADD COLUMN IF NOT EXISTS replay_probe INTEGER;",
	    key);
	state.Apply(helper);
	auto &owner_locks = TransactionalMVLockState::Get(*owner.context);
	auto &helper_locks = TransactionalMVLockState::Get(*helper.context);
	if (helper_locks.GetMutationOwner() != owner_locks.GetMutationOwner()) {
		throw InvalidInputException("Metadata replay helper did not inherit the caller mutation owner");
	}
	owner_locks.AcquireMutationLock();
	Execute(helper, "BEGIN;");
	// The second Apply must also work while this helper retains its own
	// re-entrant guard through an explicit transaction.
	state.Apply(helper);
	state.Apply(helper);
	auto gate = RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(*owner.context));
	const auto competing_owner = TransactionalMVLockState::Get(*competitor.context).GetMutationOwner();
	if (gate->TryLock(competing_owner)) {
		gate->Unlock(competing_owner);
		throw InvalidInputException("Metadata replay released the caller mutation gate");
	}
	Execute(helper, "SELECT replay_probe FROM temp.main.openivm_refresh_hooks LIMIT 0;");
	Execute(helper, "ROLLBACK;");
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	if (!gate->TryLock(competing_owner)) {
		throw InvalidInputException("Metadata replay retained the mutation gate after transaction end");
	}
	gate->Unlock(competing_owner);
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Replay mutated durable metadata') END "
	               "FROM duckdb_columns() WHERE schema_name='Control.Space' AND table_name='openivm_refresh_hooks' "
	               "AND column_name='replay_probe';");
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (4),(5),(5); "
	               "DELETE FROM source WHERE i=5;");
	Execute(owner, "PRAGMA refresh('mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM mv EXCEPT ALL SELECT * FROM source) "
	               "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM mv) "
	               "THEN error('Metadata replay damaged surviving MV') ELSE true END;");
	Execute(owner, "DROP VIEW mv;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Metadata replay cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Replay on a reused helper must follow catalog/schema identity, including when
// CREATE and CASCADE have both contributed mutations to the same transaction.
static void TestMetadataReplayLocations() {
	std::cerr << "CASE metadata replay locations" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_metadata_schema='Control.Space'; "
	               "SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE a(i INTEGER); CREATE TABLE b(i INTEGER); "
	               "INSERT INTO a VALUES (1),(1),(2); INSERT INTO b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv1 AS SELECT a.i FROM a JOIN b USING(i); "
	               "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM b; "
	               "ATTACH ':memory:' AS peer; CREATE TABLE peer.main.b(i INTEGER); "
	               "INSERT INTO peer.main.b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW peer.main.mv2 AS SELECT * FROM peer.main.b;");
	Execute(owner, "BEGIN; CREATE MATERIALIZED VIEW local_mv AS SELECT * FROM b; DROP TABLE a CASCADE;");
	auto &state = TransactionalMVMetadataState::Get(*owner.context);
	RefreshMetadata::UseCatalog(*owner.context, helper);
	auto primary = RefreshMetadata::CurrentLocation(helper).catalog_name;
	state.Apply(helper);
	Execute(helper, "SELECT CASE WHEN (SELECT count(*) FROM openivm_views WHERE view_sql_name='mv1')=0 "
	                "AND (SELECT count(*) FROM openivm_views WHERE view_sql_name='local_mv')=1 "
	                "AND (SELECT count(*) FROM openivm_views WHERE view_sql_name='mv2')=1 "
	                "THEN true ELSE error('Primary metadata replay lost CREATE or CASCADE ownership') END;");
	RefreshMetadata::UseCatalog(*owner.context, helper, "peer");
	state.Apply(helper);
	Execute(helper, "SELECT CASE WHEN count(*)=1 AND min(view_catalog)='peer' "
	                "THEN true ELSE error('Metadata replay leaked across catalogs') END FROM openivm_views;");
	RefreshMetadata::UseCatalog(*owner.context, helper, primary);
	state.Apply(helper);
	Execute(helper, "SELECT CASE WHEN (SELECT count(*) FROM openivm_views WHERE view_sql_name='mv1')=0 "
	                "AND (SELECT count(*) FROM openivm_views WHERE view_sql_name='local_mv')=1 "
	                "THEN true ELSE error('Metadata replay retained the previous catalog shadow') END;");
	Execute(owner, "ROLLBACK;");
	// Both catalogs' surviving views must still capture conflicting batches.
	for (const auto &source : vector<string> {"a", "b", "peer.main.b"}) {
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i=1; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5;");
	}
	const vector<pair<string, string>> views = {
	    {"mv1", "SELECT a.i FROM a JOIN b USING(i)"},
	    {"mv2", "SELECT * FROM b"},
	    {"peer.main.mv2", "SELECT * FROM peer.main.b"},
	};
	for (const auto &view : views) {
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL " + view.second +
		                   ") OR EXISTS (" + view.second + " EXCEPT ALL SELECT * FROM " + view.first +
		                   ") THEN error('Catalog replay rollback MV bag mismatch') ELSE true END;");
	}
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
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM \"Consumer.Catalog\".main." + view +
		                   " EXCEPT ALL SELECT * FROM \"Source.Space\".a) OR EXISTS "
		                   "(SELECT * FROM \"Source.Space\".a EXCEPT ALL SELECT * FROM \"Consumer.Catalog\".main." +
		                   view + ") THEN error('Cross-catalog initial load bag mismatch') ELSE true END;");
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
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first +
		                   " EXCEPT ALL SELECT * FROM \"Source.Space\"." + view.second +
		                   ") OR EXISTS (SELECT * FROM \"Source.Space\"." + view.second + " EXCEPT ALL SELECT * FROM " +
		                   view.first +
		                   ") THEN error('Cross-catalog shared delta refresh bag mismatch') ELSE true END;");
	}
	for (const auto &view : survivors) {
		Execute(owner, "DROP VIEW " + view.first + ";");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Cross-catalog final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE database_name='memory' "
	               "AND schema_name IN ('Source.Space','Internal Space') AND starts_with(table_name,'openivm_');");
}

static void TestCreateAfterCommittedDeltaRemoval(bool dedicated_schema, bool peer_view, bool republished) {
	std::cerr << "CASE CREATE after committed delta removal dedicated_schema=" << dedicated_schema
	          << " peer_view=" << peer_view << " republished=" << republished << std::endl;
	DuckDB db(nullptr);
	Connection creator(db), dropper(db), observer(db);
	Execute(creator, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	                 "SET openivm_metadata_schema='Control.Space'; CREATE SCHEMA \"Source.Space\"; "
	                 "CREATE TABLE \"Source.Space\".\"Items;Name\"(i INTEGER); "
	                 "INSERT INTO \"Source.Space\".\"Items;Name\" VALUES (1),(1),(2); "
	                 "ATTACH ':memory:' AS peer;");
	const string view_catalog = peer_view ? "peer" : "memory";
	const string view_prefix = SqlUtils::QualifiedPrefix(view_catalog, "View.Space");
	const string original = view_prefix + "\"Owner;Name\"";
	const string retried = view_prefix + "\"Retry;Name\"";
	const string source = "memory.\"Source.Space\".\"Items;Name\"";
	const string delta_schema = dedicated_schema ? "Internal Space" : "Source.Space";
	const string delta_name = RefreshMetadata::SourceDeltaTableName("Source.Space", "Items;Name", delta_schema);
	const string delta = SqlUtils::FullName("memory", delta_schema, delta_name);
	const string physical_delta =
	    " FROM duckdb_tables() WHERE database_name='memory' AND schema_name=" + Value(delta_schema).ToSQLString() +
	    " AND table_name=" + Value(delta_name).ToSQLString();
	const string registry = SqlUtils::FullName(view_catalog, "Control.Space", openivm::DELTA_TABLES_TABLE);
	for (auto *con : {&creator, &dropper}) {
		Execute(*con, "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema=" +
		                  Value(dedicated_schema ? "Internal Space" : "").ToSQLString() + ";");
	}
	Execute(dropper, "CREATE SCHEMA " + SqlUtils::QuoteIdentifier(view_catalog) +
	                     ".\"View.Space\"; "
	                     "CREATE MATERIALIZED VIEW " +
	                     original + " AS SELECT * FROM " + source + ";");
	Execute(creator, "USE " + SqlUtils::QuoteIdentifier(view_catalog) + ".\"Control.Space\"; BEGIN;");
	// Pin both the metadata and source-catalog snapshots before the final DROP.
	// No gate is held by this read-only transaction, so DROP commits first.
	Execute(creator, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Missing initial delta owner') END FROM " +
	                     registry + "; SELECT * FROM " + delta + ";");
	Execute(dropper, "DROP VIEW " + original + ";");
	Execute(creator, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Caller did not retain old ownership snapshot') "
	                 "END FROM " +
	                     registry + "; SELECT * FROM " + delta + ";");
	Execute(observer,
	        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Final DROP retained delta') END" + physical_delta + ";");
	if (republished) {
		// Reuse the exact physical name, so checking names alone cannot detect
		// that the caller would publish the previous allocation's catalog entry.
		Execute(dropper, "CREATE MATERIALIZED VIEW " + original + " AS SELECT * FROM " + source + ";");
		Execute(observer,
		        "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Delta was not recreated at same name') END" +
		            physical_delta + ";");
	}
	auto failed = creator.Query("CREATE MATERIALIZED VIEW " + retried + " AS SELECT * FROM " + source + ";");
	if (!failed->HasError() || failed->GetError().find("retry the transaction") == string::npos ||
	    failed->GetError().find("OpenIVM source delta") == string::npos) {
		throw InvalidInputException("Stale CREATE must reject the removed delta with a transaction retry: %s",
		                            failed->HasError() ? failed->GetError() : "CREATE succeeded");
	}
	Execute(creator, "ROLLBACK;");
	Execute(observer, "SELECT CASE WHEN count(*)=" + string(republished ? "1" : "0") +
	                      " THEN true ELSE error('Failed CREATE published stale ownership') END FROM " + registry +
	                      ";");
	Execute(creator, "CREATE MATERIALIZED VIEW " + retried + " AS SELECT * FROM " + source + ";");
	Execute(observer, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Retry allocated duplicate deltas') END" +
	                      physical_delta + ";");
	Execute(observer, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
	                      " WHERE i=1; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
	                      " VALUES (5),(5); DELETE FROM " + source + " WHERE i=5;");
	Execute(observer, "SELECT CASE WHEN count(*)>0 THEN true ELSE error('Retry delta missed batched DML') END FROM " +
	                      delta + ";");
	for (const auto &view : republished ? vector<string> {original, retried} : vector<string> {retried}) {
		RefreshAndCheckBag(creator, view, "SELECT * FROM " + source);
	}
	Execute(creator, "DROP VIEW " + retried + ";");
	if (republished) {
		Execute(observer, "SELECT CASE WHEN count(*)=1 THEN true ELSE error('Retry DROP removed sibling delta') END" +
		                      physical_delta + ";");
		Execute(dropper, "DROP VIEW " + original + ";");
	}
	Execute(observer, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Retry final DROP leaked delta') END" +
	                      physical_delta + ";");
}

static void TestMetadataQueryText(bool explicit_transaction, bool commit) {
	std::cerr << "CASE metadata qualification preserves query text explicit_transaction=" << explicit_transaction
	          << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Public.Space\"; CREATE TABLE \"Public.Space\".source(i INTEGER); "
	               "INSERT INTO \"Public.Space\".source VALUES (1),(1),(2); USE memory.\"Public.Space\";");
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// EXCEPT retains the original query text for materialization. In particular,
	// these dollar literals must not be scanned for metadata table references.
	vector<string> queries = {"SELECT i, $$FROM openivm_views WHERE true$$ AS label FROM \"Public.Space\".source "
	                          "EXCEPT SELECT -1, 'never'",
	                          "SELECT i, $metadata$FROM openivm_delta_tables; JOIN openivm_views$metadata$ AS label "
	                          "FROM \"Public.Space\".source EXCEPT SELECT -1, 'never'"};
	for (auto &table : MetadataTableNames()) {
		queries.push_back("WITH " + SqlUtils::QuoteIdentifier(table) +
		                  " AS (SELECT i FROM \"Public.Space\".source) SELECT i FROM " +
		                  SqlUtils::QuoteIdentifier(table) + " EXCEPT SELECT -1");
	}
	auto view_name = [&](idx_t index) {
		return SqlUtils::FullName("memory", "Public.Space", "Literal;MV" + to_string(index));
	};
	auto expected_query = [&](idx_t index) {
		return "SELECT * FROM (" + queries[index] + ") expected";
	};
	auto check_initial = [&](idx_t index) {
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view_name(index) + " EXCEPT ALL " +
		                   expected_query(index) + ") OR EXISTS (" + expected_query(index) +
		                   " EXCEPT ALL SELECT * FROM " + view_name(index) +
		                   ") THEN error('Metadata qualification changed initial query results') ELSE true END;");
	};
	for (idx_t i = 0; i < queries.size(); i++) {
		Execute(owner, "CREATE MATERIALIZED VIEW " + view_name(i) + " AS " + queries[i] + ";");
		check_initial(i);
	}
	Execute(owner, "INSERT INTO \"Public.Space\".source VALUES (1),(2),(3),(3),(8); "
	               "DELETE FROM \"Public.Space\".source WHERE i=1; "
	               "UPDATE \"Public.Space\".source SET i=4 WHERE i=2; "
	               "DELETE FROM \"Public.Space\".source WHERE i=8;");
	for (idx_t i = 0; i < queries.size(); i++) {
		RefreshAndCheckBag(owner, view_name(i), expected_query(i));
	}
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	if (!explicit_transaction || commit) {
		for (idx_t i = 0; i < queries.size(); i++) {
			check_initial(i);
			Execute(owner, "DROP VIEW " + view_name(i) + ";");
		}
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Query-text lifecycle leaked views') END "
	               "FROM duckdb_views() WHERE database_name='memory' AND schema_name='Public.Space' "
	               "AND starts_with(view_name, 'Literal;MV');");
}

// Removing the last child must not remove either of its parent's delta companions.
// Native DROP and staged cross-catalog DROP share the replacement cleanup path.
static void TestPublicationOwnerCleanup(bool cross_catalog, bool replace, bool explicit_transaction, bool commit) {
	std::cerr << "CASE publication owner cleanup cross_catalog=" << cross_catalog << " replace=" << replace
	          << " explicit_transaction=" << explicit_transaction << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	const string child_catalog = cross_catalog ? "Consumer.Catalog" : "memory";
	const string parent = "memory.\"Parent.Space\".\"Parent;MV\"";
	const string child = SqlUtils::FullName(child_catalog, "Child.Space", "Child;MV");
	const string local_source = SqlUtils::FullName(child_catalog, "Child.Space", "source");
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Parent.Space\"; CREATE TABLE main.source(i INTEGER); "
	               "INSERT INTO main.source VALUES (1),(1),(2); CREATE MATERIALIZED VIEW " +
	                   parent + " AS SELECT * FROM memory.main.source;");
	if (cross_catalog) {
		Execute(owner, "ATTACH ':memory:' AS \"Consumer.Catalog\";");
	}
	Execute(owner, "CREATE SCHEMA " + SqlUtils::QuoteIdentifier(child_catalog) + ".\"Child.Space\"; CREATE TABLE " +
	                   local_source + "(i INTEGER); INSERT INTO " + local_source +
	                   " VALUES (7),(7),(8); CREATE MATERIALIZED VIEW " + child + " AS SELECT * FROM " + parent + ";");
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto key = RefreshMetadata(helper).FindViewKey("memory", "Parent.Space", "Parent;MV");
	if (key.empty()) {
		throw InvalidInputException("Publication owner fixture is missing parent metadata");
	}
	auto companions = "FROM duckdb_tables() WHERE database_name='memory' AND schema_name='Internal Space' "
	                  "AND table_name IN (" +
	                  Value(SqlUtils::DeltaName(key)).ToSQLString() + "," +
	                  Value(SqlUtils::DeltaName(PublishedViewName(key))).ToSQLString() + ")";
	auto check_companions = [&]() {
		Execute(owner, "SELECT CASE WHEN count(*)=2 THEN true ELSE error('Child cleanup removed parent delta') END " +
		                   companions + ";");
	};
	check_companions();
	// Use a search path that cannot resolve the parent's user-facing schema.
	Execute(owner, "USE " + SqlUtils::QuoteIdentifier(child_catalog) + ".\"Control.Space\";");
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	Execute(owner, replace ? "CREATE OR REPLACE MATERIALIZED VIEW " + child + " AS SELECT * FROM " + local_source + ";"
	                       : "SELECT 'before;drop'; /* quoted child; */ DROP VIEW " + child + "; SELECT 1;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	check_companions();
	for (const auto &source : vector<string> {"memory.main.source", local_source}) {
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i=1; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5;");
	}
	RefreshAndCheckBag(owner, parent, "SELECT * FROM memory.main.source");
	if (replace || !commit) {
		RefreshAndCheckBag(owner, child, "SELECT * FROM " + (replace && commit ? local_source : parent));
		Execute(owner, "DROP VIEW " + child + ";");
		check_companions();
		Execute(owner, "INSERT INTO memory.main.source VALUES (4),(4),(6); DELETE FROM memory.main.source WHERE i=4; "
		               "UPDATE memory.main.source SET i=7 WHERE i=6;");
		RefreshAndCheckBag(owner, parent, "SELECT * FROM memory.main.source");
	}
	Execute(owner, "DROP VIEW " + parent + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Parent DROP retained its companions') END " +
	                   companions + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Publication owner cleanup leaked state') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
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

// Drop the parent first, including descendants whose ownership lives in another
// registry and a grandchild back in the parent's catalog. Equal SQL names and
// shared base sources must not pull unrelated consumers into that closure.
static void TestViewCascade(bool cross_catalog, bool explicit_transaction, bool commit, bool batched_drop) {
	std::cerr << "CASE view cascade cross_catalog=" << cross_catalog << " explicit_transaction=" << explicit_transaction
	          << " commit=" << commit << " batched_drop=" << batched_drop << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	const string child_catalog = cross_catalog ? "Consumer.Catalog" : "memory";
	const string parent = "memory.\"Parent.Space\".\"Same;MV\"";
	const string child = SqlUtils::FullName(child_catalog, "Child.Space", "Child;MV");
	const string grandchild = "memory.\"Child.Space\".\"Grand;MV\"";
	const string source = "memory.main.source";
	const string child_source = SqlUtils::FullName(child_catalog, "Child.Space", "source");
	const string sibling = "memory.\"Keep.Space\".\"Same;MV\"";
	const string child_sibling = SqlUtils::FullName(child_catalog, "Keep.Space", "Same;MV");
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Parent.Space\"; CREATE SCHEMA \"Keep.Space\"; CREATE SCHEMA \"Child.Space\"; "
	               "CREATE TABLE main.source(i INTEGER); INSERT INTO main.source VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW " +
	                   parent + " AS SELECT * FROM " + source + "; CREATE MATERIALIZED VIEW " + sibling +
	                   " AS SELECT * FROM " + source + ";");
	if (cross_catalog) {
		Execute(owner, "ATTACH ':memory:' AS \"Consumer.Catalog\"; "
		               "CREATE SCHEMA \"Consumer.Catalog\".\"Child.Space\"; "
		               "CREATE SCHEMA \"Consumer.Catalog\".\"Keep.Space\";");
	}
	// For the local case use another schema so the two unrelated siblings also
	// retain the same SQL name without sharing a physical identity.
	const string other_sibling = cross_catalog ? child_sibling : "memory.\"Child.Space\".\"Same;MV\"";
	Execute(owner, "CREATE TABLE " + child_source + "(i INTEGER); INSERT INTO " + child_source +
	                   " VALUES (1),(1),(2); CREATE MATERIALIZED VIEW " + child + " AS SELECT p.i FROM " + parent +
	                   " p JOIN " + child_source + " s USING(i); CREATE MATERIALIZED VIEW " + grandchild +
	                   " AS SELECT * FROM " + child + "; CREATE MATERIALIZED VIEW " + other_sibling +
	                   " AS SELECT * FROM " + child_source + ";");
	const vector<string> catalogs =
	    cross_catalog ? vector<string> {"memory", child_catalog} : vector<string> {"memory"};
	const vector<string> removed_names = {"Same;MV", "Child;MV", "Grand;MV"};
	const vector<string> removed_schemas = {"Parent.Space", "Child.Space", "Child.Space"};
	const vector<string> removed_catalogs = {"memory", child_catalog, "memory"};
	string owned_predicate;
	for (idx_t index = 0; index < removed_names.size(); index++) {
		RefreshMetadata::UseCatalog(*owner.context, helper, removed_catalogs[index]);
		RefreshMetadata metadata(helper);
		auto key = metadata.FindViewKey(removed_catalogs[index], removed_schemas[index], removed_names[index]);
		if (key.empty()) {
			throw InvalidInputException("View CASCADE fixture is missing registered ownership");
		}
		for (const auto &name : {IncrementalTableNames::DataTableName(key), PublishedViewName(key),
		                         SqlUtils::DeltaName(key), SqlUtils::DeltaName(PublishedViewName(key))}) {
			if (!owned_predicate.empty()) {
				owned_predicate += " OR ";
			}
			owned_predicate += "(database_name=" + Value(removed_catalogs[index]).ToSQLString() +
			                   " AND schema_name='Internal Space' AND table_name=" + Value(name).ToSQLString() + ")";
		}
	}
	auto owned_query = "SELECT database_name, schema_name, table_name FROM duckdb_tables() WHERE " + owned_predicate;
	auto original = owner.Query(owned_query);
	if (original->HasError() || original->RowCount() == 0) {
		throw InvalidInputException("View CASCADE fixture is missing maintenance tables");
	}
	const auto original_count = original->RowCount();
	Execute(owner, "USE " + SqlUtils::QuoteIdentifier(child_catalog) + ".\"Control.Space\";");
	if (explicit_transaction) {
		// Replay replacement ownership before following the physical descendants.
		Execute(owner, "BEGIN; CREATE OR REPLACE MATERIALIZED VIEW " + parent + " AS SELECT * FROM " + source + ";");
	}
	const string drop = "DROP VIEW " + parent + " CASCADE;";
	const string drop_sql = batched_drop ? "SELECT 'before;drop'; /* parent; first */ " + drop + " SELECT 1;" : drop;
	if (cross_catalog && explicit_transaction) {
		// The grandchild's local DROP precedes the cross-catalog write. Neither
		// that partial program nor replacement may escape the caller's rollback.
		auto result = owner.Query(drop_sql);
		if (!result->HasError() ||
		    result->GetError().find("single transaction can only write to a single attached database") ==
		        string::npos) {
			throw InvalidInputException("Cross-catalog CASCADE bypassed the caller's native transaction boundary");
		}
		Execute(owner, "ROLLBACK;");
	} else {
		Execute(owner, drop_sql);
		Execute(owner, "SELECT CASE WHEN NOT EXISTS (" + owned_query +
		                   ") THEN true ELSE error('Parent CASCADE leaked descendant maintenance tables') END;");
		for (const auto &catalog : catalogs) {
			const auto prefix = SqlUtils::QualifiedPrefix(catalog, "Control.Space");
			Execute(
			    owner,
			    "SELECT CASE WHEN (SELECT count(*) FROM " + prefix +
			        "openivm_views)=" + to_string(cross_catalog ? 1 : 2) + " AND NOT EXISTS (SELECT 1 FROM " + prefix +
			        "openivm_delta_tables WHERE view_name NOT IN (SELECT view_name FROM " + prefix +
			        "openivm_views)) THEN true ELSE error('Parent CASCADE leaked or removed registry ownership') END;");
		}
		for (idx_t index = 0; index < removed_names.size(); index++) {
			Execute(owner,
			        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Parent CASCADE left a public descendant') END "
			        "FROM duckdb_views() WHERE database_name=" +
			            Value(removed_catalogs[index]).ToSQLString() +
			            " AND schema_name=" + Value(removed_schemas[index]).ToSQLString() +
			            " AND view_name=" + Value(removed_names[index]).ToSQLString() + ";");
		}
		if (explicit_transaction) {
			Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
		}
	}
	const bool retained = explicit_transaction && (cross_catalog || !commit);
	Execute(owner, "SELECT CASE WHEN (SELECT count(*) FROM (" + owned_query +
	                   "))=" + to_string(retained ? original_count : 0) +
	                   " THEN true ELSE error('Parent CASCADE commit/rollback lost maintenance tables') END;");
	for (const auto &table : {source, child_source}) {
		Execute(owner, "INSERT INTO " + table + " VALUES (1),(2),(3),(3); DELETE FROM " + table +
		                   " WHERE i=1; UPDATE " + table + " SET i=4 WHERE i=2; INSERT INTO " + table +
		                   " VALUES (1),(4),(5); DELETE FROM " + table + " WHERE i=5; UPDATE " + table +
		                   " SET i=2 WHERE i=4;");
	}
	RefreshAndCheckBag(owner, sibling, "SELECT * FROM " + source);
	RefreshAndCheckBag(owner, other_sibling, "SELECT * FROM " + child_source);
	if (retained) {
		RefreshAndCheckBag(owner, parent, "SELECT * FROM " + source);
		RefreshAndCheckBag(owner, child, "SELECT p.i FROM " + source + " p JOIN " + child_source + " s USING(i)");
		RefreshAndCheckBag(owner, grandchild, "SELECT p.i FROM " + source + " p JOIN " + child_source + " s USING(i)");
		Execute(owner, drop);
	}
	Execute(owner, "DROP VIEW " + sibling + "; DROP VIEW " + other_sibling + ";");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('View CASCADE final-consumer cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
	for (const auto &catalog : catalogs) {
		const auto prefix = SqlUtils::QualifiedPrefix(catalog, "Control.Space");
		Execute(owner, "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM " + prefix +
		                   "openivm_views) AND NOT EXISTS (SELECT 1 FROM " + prefix +
		                   "openivm_delta_tables) THEN true ELSE error('View CASCADE final ownership leaked') END;");
	}
}

static void TestPublicationOwnerReplay(bool commit) {
	std::cerr << "CASE publication owner replay commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Parent.Space\"; CREATE TABLE main.source(i INTEGER); "
	               "INSERT INTO main.source VALUES (1),(1),(2); ATTACH ':memory:' AS \"Consumer.Catalog\"; "
	               "CREATE TABLE \"Consumer.Catalog\".main.source(i INTEGER); "
	               "CREATE MATERIALIZED VIEW \"Consumer.Catalog\".main.sentinel "
	               "AS SELECT * FROM \"Consumer.Catalog\".main.source;");
	RefreshMetadata::UseCatalog(*owner.context, helper, "Consumer.Catalog");
	Execute(owner, "BEGIN; CREATE MATERIALIZED VIEW \"Parent.Space\".\"Parent;MV\" AS SELECT * FROM main.source;");
	auto keys =
	    owner.Query("SELECT view_name FROM memory.\"Control.Space\".openivm_views WHERE view_sql_name='Parent;MV'");
	if (keys->HasError() || keys->RowCount() != 1) {
		throw InvalidInputException("Publication replay fixture is missing transaction-local parent metadata");
	}
	RefreshMetadata::DeltaSource source;
	source.table_name = SqlUtils::DeltaName(PublishedViewName(keys->GetValue(0, 0).ToString()));
	source.catalog_name = "memory";
	source.schema_name = "Internal Space";
	auto &state = TransactionalMVMetadataState::Get(*owner.context);
	if (RefreshMetadata(helper).IsMaterializedViewDelta(source) ||
	    !RefreshMetadata(helper).IsMaterializedViewDelta(source, &state)) {
		throw InvalidInputException("Publication owner lookup missed the transaction's new metadata location");
	}
	// Qualified source keys and a different selected registry must keep the same owner.
	source.table_name = SqlUtils::FullName(source.catalog_name, source.schema_name, source.table_name);
	if (!RefreshMetadata(helper).IsMaterializedViewDelta(source, &state)) {
		throw InvalidInputException("Publication owner replay lost qualified source identity");
	}
	Execute(owner, "CREATE MATERIALIZED VIEW main.\"Child;MV\" AS SELECT * FROM \"Parent.Space\".\"Parent;MV\"; "
	               "DROP VIEW main.\"Child;MV\";");
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	if (RefreshMetadata(helper).IsMaterializedViewDelta(source) != commit) {
		throw InvalidInputException("Publication owner lookup retained rolled-back or lost committed ownership");
	}
	if (!commit) {
		Execute(owner, "CREATE MATERIALIZED VIEW \"Parent.Space\".\"Parent;MV\" AS SELECT * FROM main.source;");
	}
	Execute(owner, "INSERT INTO main.source VALUES (1),(2),(3),(3); DELETE FROM main.source WHERE i=1; "
	               "UPDATE main.source SET i=4 WHERE i=2;");
	RefreshAndCheckBag(owner, "memory.\"Parent.Space\".\"Parent;MV\"", "SELECT * FROM memory.main.source");
	Execute(owner, "BEGIN; DROP VIEW \"Parent.Space\".\"Parent;MV\";");
	if (!RefreshMetadata(helper).IsMaterializedViewDelta(source) ||
	    RefreshMetadata(helper).IsMaterializedViewDelta(source, &state)) {
		throw InvalidInputException("Publication owner replay retained a transaction-local DROP");
	}
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	if (!commit) {
		Execute(owner, "INSERT INTO main.source VALUES (4),(4),(6); DELETE FROM main.source WHERE i=4; "
		               "UPDATE main.source SET i=7 WHERE i=6;");
		RefreshAndCheckBag(owner, "memory.\"Parent.Space\".\"Parent;MV\"", "SELECT * FROM memory.main.source");
		Execute(owner, "DROP VIEW \"Parent.Space\".\"Parent;MV\";");
	}
	if (RefreshMetadata(helper).IsMaterializedViewDelta(source)) {
		throw InvalidInputException("Publication owner lookup retained a dropped parent");
	}
	Execute(helper, "SELECT CASE WHEN current_database()='Consumer.Catalog' AND current_schema()='Control.Space' "
	                "THEN true ELSE error('Publication owner replay changed the selected metadata location') END;");
	Execute(owner, "INSERT INTO \"Consumer.Catalog\".main.source VALUES (1),(2),(3),(3); "
	               "DELETE FROM \"Consumer.Catalog\".main.source WHERE i=1; "
	               "UPDATE \"Consumer.Catalog\".main.source SET i=4 WHERE i=2;");
	RefreshAndCheckBag(owner, "\"Consumer.Catalog\".main.sentinel", "SELECT * FROM \"Consumer.Catalog\".main.source");
	Execute(owner, "DROP VIEW \"Consumer.Catalog\".main.sentinel;");
}

static void CheckSourceWatermarkReplay(Connection &owner, DatabaseInstance &db) {
	Connection helper(db);
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	const string columns = "SELECT d.table_name, d.last_update, d.last_refresh_ts FROM ";
	const string predicate = " v ON d.view_name=v.view_name WHERE v.view_sql_name='mv2' "
	                         "AND d.source_schema='Source.Space'";
	auto durable = owner.Query(columns + RefreshMetadata::MetadataTable(helper, openivm::DELTA_TABLES_TABLE) +
	                           " d JOIN " + RefreshMetadata::MetadataTable(helper, openivm::VIEWS_TABLE) + predicate);
	if (durable->HasError() || durable->RowCount() != 1) {
		throw InvalidInputException("Source watermark fixture must have one caller-visible source row");
	}
	// The replacement's new physical delta is uncommitted. Replay must retain
	// exactly the watermark evaluated by the caller without reading that table.
	TransactionalMVMetadataState::Get(*owner.context).Apply(helper);
	string expected = "SELECT " + durable->GetValue(0, 0).ToSQLString() + " AS table_name, " +
	                  durable->GetValue(1, 0).ToSQLString() + "::TIMESTAMP AS last_update, " +
	                  durable->GetValue(2, 0).ToSQLString() + "::TIMESTAMP AS last_refresh_ts";
	auto replayed = columns + "openivm_delta_tables d JOIN openivm_views" + predicate;
	Execute(helper, "SELECT CASE WHEN EXISTS (" + replayed + " EXCEPT ALL " + expected + ") OR EXISTS (" + expected +
	                    " EXCEPT ALL " + replayed +
	                    ") THEN error('Source watermark replay changed the caller value') ELSE true END;");
}

// Keep an unrelated, later-sorted metadata catalog selected at the end of source
// enumeration. Ownership in memory must include every earlier local mutation,
// regardless of which location Apply() last shadowed on the compiler helper.
static void TestSourceCascadeConsumerLocations(const string &mutation, bool commit, bool batched_drop,
                                               bool explicit_transaction) {
	std::cerr << "CASE source cascade mutation=" << mutation << " commit=" << commit << " batched_drop=" << batched_drop
	          << " explicit_transaction=" << explicit_transaction << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source.Space\"; CREATE TABLE \"Source.Space\".a(i INTEGER); "
	               "CREATE TABLE \"Source.Space\".b(i INTEGER); CREATE TABLE \"Source.Space\".c(i INTEGER); "
	               "INSERT INTO \"Source.Space\".a VALUES (1),(1),(2); "
	               "INSERT INTO \"Source.Space\".b VALUES (1),(1),(2); "
	               "INSERT INTO \"Source.Space\".c VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv1 AS SELECT a.i FROM \"Source.Space\".a a "
	               "JOIN \"Source.Space\".b b USING(i); "
	               "ATTACH ':memory:' AS \"zz Replay Peer\"; "
	               "CREATE TABLE \"zz Replay Peer\".main.b(i INTEGER); "
	               "INSERT INTO \"zz Replay Peer\".main.b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW \"zz Replay Peer\".main.mv1 "
	               "AS SELECT * FROM \"zz Replay Peer\".main.b;");
	// Verify placement before any transaction replay or cleanup. A CREATE that
	// folds the quoted catalog can bind successfully while publishing ownership
	// in memory instead of the peer's native metadata catalog.
	Execute(owner, "SELECT CASE WHEN count(*)=1 AND min(view_catalog)='zz Replay Peer' "
	               "THEN true ELSE error('Quoted peer metadata was published in the wrong catalog') END "
	               "FROM \"zz Replay Peer\".\"Control.Space\".openivm_views WHERE view_sql_name='mv1';");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true "
	               "ELSE error('Peer ownership leaked into the source metadata catalog') END "
	               "FROM memory.\"Control.Space\".openivm_views WHERE lower(view_catalog)='zz replay peer';");
	{
		Connection helper(db);
		RefreshMetadata::UseCatalog(*owner.context, helper, "zz replay peer");
		auto location = RefreshMetadata::CurrentLocation(helper);
		if (location.catalog_name != "zz Replay Peer" || location.schema_name != "Control.Space") {
			throw InvalidInputException("Case-insensitive catalog lookup changed metadata ownership");
		}
	}
	bool added = mutation == "create";
	bool removed = mutation == "drop";
	// Retain a nonempty delta for mv1. A new consumer must replay MAX(delta)+1,
	// not substitute its creation time or the helper's transaction timestamp.
	Execute(owner, "INSERT INTO \"Source.Space\".b VALUES (6),(6);");
	if (!added) {
		Execute(owner, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
	}
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	if (added) {
		Execute(owner, "CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
	} else if (removed) {
		Execute(owner, "DROP VIEW mv2;");
	} else {
		Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".c;");
		if (explicit_transaction) {
			CheckSourceWatermarkReplay(owner, *db.instance);
		}
		if (mutation == "replace") {
			Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
			if (explicit_transaction) {
				CheckSourceWatermarkReplay(owner, *db.instance);
			}
			Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".c;");
		}
		if (mutation == "recreate") {
			Execute(owner, "DROP VIEW mv2; CREATE MATERIALIZED VIEW mv2 AS SELECT * FROM \"Source.Space\".b;");
		}
	}
	if (explicit_transaction && !removed) {
		CheckSourceWatermarkReplay(owner, *db.instance);
	}
	Execute(owner, "DROP TABLE \"Source.Space\".a CASCADE;" + string(batched_drop ? " SELECT 1;" : ""));
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	bool owns_b = !commit || added || mutation == "recreate";
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(owns_b ? "1" : "0") +
	                   " THEN true ELSE error('Source CASCADE lost local ownership or leaked a removed delta') END "
	                   "FROM duckdb_tables() WHERE database_name='memory' AND schema_name='Internal Space' "
	                   "AND table_name='" +
	                   RefreshMetadata::InternalSourceDeltaName("Source.Space", "b") + "';");
	vector<pair<string, string>> surviving = {
	    {"\"zz Replay Peer\".main.mv1", "SELECT * FROM \"zz Replay Peer\".main.b"},
	};
	if (!commit) {
		surviving.push_back({"mv1", "SELECT a.i FROM \"Source.Space\".a a JOIN \"Source.Space\".b b USING(i)"});
	}
	if ((!commit && !added) || (commit && !removed)) {
		surviving.push_back({"mv2", commit && mutation == "replace" ? "SELECT * FROM \"Source.Space\".c"
		                                                            : "SELECT * FROM \"Source.Space\".b"});
	}
	vector<string> sources = {"\"Source.Space\".b", "\"Source.Space\".c", "\"zz Replay Peer\".main.b"};
	if (!commit) {
		sources.push_back("\"Source.Space\".a");
	}
	for (auto &source : sources) {
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i=1; UPDATE " + source + " SET i=4 WHERE i=2; INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5; UPDATE " + source +
		                   " SET i=2 WHERE i=4;");
	}
	for (auto &view : surviving) {
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL " + view.second +
		                   ") OR EXISTS (" + view.second + " EXCEPT ALL SELECT * FROM " + view.first +
		                   ") THEN error('Cross-location source CASCADE survivor bag mismatch') ELSE true END;");
	}
	if (commit && !removed) {
		Execute(owner, "DROP VIEW mv2;");
	}
	if (!commit) {
		Execute(owner, "DROP VIEW mv1;");
		if (!added) {
			Execute(owner, "DROP VIEW mv2;");
		}
	}
	Execute(owner, "DROP VIEW \"zz Replay Peer\".main.mv1;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Final consumer delta cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// SQL lower() merges these sources; DuckDB's catalog keeps them distinct.
static void TestUnicodeSourceCascade(bool explicit_transaction, bool commit, bool cross_catalog, bool batched_drop) {
	std::cerr << "CASE Unicode source cascade explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << " cross_catalog=" << cross_catalog << " batched_drop=" << batched_drop << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Ä\"; CREATE SCHEMA \"ä\"; "
	               "CREATE TABLE \"Ä\".items(i INTEGER); CREATE TABLE \"ä\".items(i INTEGER); "
	               "INSERT INTO \"Ä\".items VALUES (1),(1),(2); INSERT INTO \"ä\".items VALUES (8),(8),(9);");
	// Separate metadata catalogs exercise staged cleanup as well as catalog lookup:
	// these quoted catalog names, like the source schemas, are distinct in DuckDB.
	const string upper_catalog = cross_catalog ? "ÄCatalog" : "memory";
	const string lower_catalog = cross_catalog ? "äCatalog" : "memory";
	if (cross_catalog) {
		Execute(owner, "ATTACH ':memory:' AS \"ÄCatalog\"; ATTACH ':memory:' AS \"äCatalog\"; "
		               "CREATE SCHEMA \"ÄCatalog\".\"Ä\"; CREATE SCHEMA \"äCatalog\".\"ä\";");
	}
	const auto upper_view = SqlUtils::FullName(upper_catalog, "Ä", "mv");
	const auto lower_view = SqlUtils::FullName(lower_catalog, "ä", "mv");
	const auto upper_metadata = SqlUtils::FullName(upper_catalog, "Control.Space", openivm::VIEWS_TABLE);
	const auto lower_metadata = SqlUtils::FullName(lower_catalog, "Control.Space", openivm::VIEWS_TABLE);
	Execute(owner, "CREATE MATERIALIZED VIEW " + upper_view +
	                   " AS SELECT * FROM memory.\"Ä\".items; "
	                   "CREATE MATERIALIZED VIEW " +
	                   lower_view + " AS SELECT * FROM memory.\"ä\".items;");
	Execute(owner, "SELECT CASE WHEN "
	               "(SELECT count(*) FROM " +
	                   upper_metadata + " WHERE view_catalog=" + Value(upper_catalog).ToSQLString() +
	                   " AND view_schema='Ä')=1 AND "
	                   "(SELECT count(*) FROM " +
	                   lower_metadata + " WHERE view_catalog=" + Value(lower_catalog).ToSQLString() +
	                   " AND view_schema='ä')=1 AND "
	                   "(SELECT count(DISTINCT table_name) FROM duckdb_tables() WHERE database_name='memory' "
	                   "AND schema_name='Internal Space' AND table_name IN (" +
	                   Value(RefreshMetadata::InternalSourceDeltaName("Ä", "items")).ToSQLString() + "," +
	                   Value(RefreshMetadata::InternalSourceDeltaName("ä", "items")).ToSQLString() +
	                   "))=2 "
	                   "THEN true ELSE error('Unicode namespaces lost physical placement') END;");
	Execute(owner, "USE " + SqlUtils::QuoteIdentifier(lower_catalog) + ".\"Control.Space\";");
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// Replacing only one same-named view must retain both qualified owners. Its
	// metadata mutations also exercise replay before CASCADE inside BEGIN.
	Execute(owner, "CREATE OR REPLACE MATERIALIZED VIEW " + upper_view + " AS SELECT * FROM memory.\"Ä\".items;");
	Execute(owner, "DROP TABLE memory.\"Ä\".iTEMS CASCADE;" + string(batched_drop ? " /* keep ä */ SELECT 1;" : ""));
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	Execute(owner, "SELECT CASE WHEN (SELECT count(*) FROM " + upper_metadata + " WHERE view_schema='Ä')=" +
	                   string(commit ? "0" : "1") + " AND (SELECT count(*) FROM " + lower_metadata +
	                   " WHERE view_schema='ä')=1 THEN true ELSE error('Unicode CASCADE selected unrelated MV') END;");
	Execute(owner, "SELECT CASE WHEN count(*)=" + string(commit ? "1" : "2") +
	                   " THEN true ELSE error('Unicode CASCADE removed or leaked source delta') END "
	                   "FROM duckdb_tables() WHERE database_name='memory' AND schema_name='Internal Space' "
	                   "AND table_name IN (" +
	                   Value(RefreshMetadata::InternalSourceDeltaName("Ä", "items")).ToSQLString() + "," +
	                   Value(RefreshMetadata::InternalSourceDeltaName("ä", "items")).ToSQLString() + ");");
	vector<pair<string, string>> survivors = {{lower_view, "memory.\"ä\".items"}};
	if (!commit) {
		survivors.emplace_back(upper_view, "memory.\"Ä\".items");
	}
	for (const auto &survivor : survivors) {
		const auto &source = survivor.second;
		Execute(owner, "INSERT INTO " + source + " VALUES (1),(2),(3),(3); DELETE FROM " + source +
		                   " WHERE i IN (1,8); UPDATE " + source + " SET i=4 WHERE i IN (2,9); INSERT INTO " + source +
		                   " VALUES (1),(4),(5); DELETE FROM " + source + " WHERE i=5; UPDATE " + source +
		                   " SET i=2 WHERE i=4;");
		RefreshAndCheckBag(owner, survivor.first, "SELECT * FROM " + source);
		Execute(owner, "DROP VIEW " + survivor.first + ";");
	}
	Execute(owner, "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM " + upper_metadata +
	                   ") "
	                   "AND NOT EXISTS (SELECT 1 FROM " +
	                   lower_metadata +
	                   ") "
	                   "AND NOT EXISTS (SELECT 1 FROM duckdb_tables() WHERE schema_name='Internal Space') "
	                   "THEN true ELSE error('Unicode final-consumer cleanup leaked') END;");
}

// Legacy view keys may repeat across metadata locations. Exclusions must name
// their metadata table even when the excluded key also appears elsewhere.
static void TestStagedCascadeConsumerIdentity() {
	std::cerr << "CASE staged cascade consumer identity" << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE a(i INTEGER); CREATE TABLE b(i INTEGER); INSERT INTO a VALUES (1),(1),(2); "
	               "INSERT INTO b VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv1 AS SELECT a.i FROM a JOIN b USING(i); "
	               "ATTACH ':memory:' AS peer; CREATE MATERIALIZED VIEW peer.main.mv1 AS SELECT * FROM memory.main.b;");
	Connection helper(db);
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto primary_table = RefreshMetadata::MetadataTable(helper, openivm::DELTA_TABLES_TABLE);
	auto primary_key = RefreshMetadata(helper).FindViewKey("memory", "main", "mv1");
	auto sources = RefreshMetadata(helper).GetDeltaSources(primary_key, "memory", "main");
	RefreshMetadata::UseCatalog(*owner.context, helper, "peer");
	auto peer_key = RefreshMetadata(helper).FindViewKey("peer", "main", "mv1");
	// Include the peer key in the primary exclusion list: it must not exclude
	// the peer row. This also exercises legacy short-key collisions without
	// changing persisted physical identities to manufacture a legacy database.
	bool checked_b = false;
	for (auto &source : sources) {
		if (RefreshMetadata::SourceTableName(source.table_name, source.catalog_name, source.schema_name) !=
		    SqlUtils::DeltaName("b")) {
			continue;
		}
		checked_b = true;
		if (!RefreshMetadata::HasSourceDeltaConsumers(
		        helper, source, nullptr,
		        {{primary_table, Value(primary_key).ToSQLString() + "," + Value(peer_key).ToSQLString()}})) {
			throw InvalidInputException("Source ownership exclusions leaked into another metadata catalog");
		}
	}
	if (!checked_b) {
		throw InvalidInputException("Cross-catalog consumer fixture is missing source b ownership");
	}
	Execute(owner, "DROP TABLE a CASCADE; "
	               "INSERT INTO b VALUES (1),(2),(3),(3); DELETE FROM b WHERE i=1; UPDATE b SET i=4 WHERE i=2; "
	               "INSERT INTO b VALUES (1),(4),(5); DELETE FROM b WHERE i=5; UPDATE b SET i=2 WHERE i=4;");
	Execute(owner, "PRAGMA refresh('peer.main.mv1');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM peer.main.mv1 EXCEPT ALL SELECT * FROM b) "
	               "OR EXISTS (SELECT * FROM b EXCEPT ALL SELECT * FROM peer.main.mv1) "
	               "THEN error('Staged CASCADE excluded another catalog consumer') ELSE true END;");
	Execute(owner, "DROP VIEW peer.main.mv1;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Staged final consumer delta cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Native batches prepare each DROP after earlier statements have executed. Cover
// full view cleanup, a committed sibling, final consumers, and rollback without
// relying on optimizer or result-order settings to execute maintenance steps.
static void TestBatchedViewDrop(bool explicit_transaction, bool commit, bool disable_optimizer) {
	std::cerr << "CASE batched view drop explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << " disable_optimizer=" << disable_optimizer << std::endl;
	DuckDB db(nullptr);
	Connection owner(db), helper(db);
	Execute(owner, "LOAD openivm;");
	Execute(owner, "SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "SET openivm_stateful_auxstate=true; SET preserve_insertion_order=false; SET threads=4;");
	Execute(owner, "CREATE SCHEMA \"Source.Space\"; CREATE SCHEMA \"Public.Space\"; CREATE SCHEMA \"Other.Public\"; "
	               "CREATE TABLE \"Source.Space\".items(g INTEGER, x INTEGER); "
	               "INSERT INTO \"Source.Space\".items VALUES (1,10),(1,10),(2,20);");
	Execute(owner, "CREATE MATERIALIZED VIEW \"Public.Space\".\"Same.MV\" "
	               "AS SELECT g, COUNT(DISTINCT x) AS n FROM \"Source.Space\".items GROUP BY g;");
	Execute(owner, "CREATE MATERIALIZED VIEW \"Other.Public\".\"Same.MV\" AS SELECT * FROM \"Source.Space\".items;");
	RefreshMetadata::UseCatalog(*owner.context, helper, "memory");
	auto view_key = RefreshMetadata(helper).FindViewKey("memory", "Public.Space", "Same.MV");
	if (view_key.empty() || RefreshMetadata(helper).GetAuxiliaryTables(view_key).empty()) {
		throw InvalidInputException("Batched view DROP fixture must own auxiliary state");
	}
	auto metadata_table = RefreshMetadata::MetadataTable(helper, openivm::VIEWS_TABLE);
	string owned_names;
	vector<string> owned_tables = {IncrementalTableNames::DataTableName(view_key), PublishedViewName(view_key),
	                               SqlUtils::DeltaName(view_key), SqlUtils::DeltaName(PublishedViewName(view_key))};
	for (auto &name : RefreshMetadata(helper).GetAuxiliaryTables(view_key)) {
		owned_tables.push_back(name);
	}
	for (auto &name : owned_tables) {
		if (!owned_names.empty()) {
			owned_names += ",";
		}
		owned_names += Value(name).ToSQLString();
	}
	auto owned_count = "(SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space' "
	                   "AND table_name IN (" +
	                   owned_names + "))";
	Execute(owner, "SELECT CASE WHEN " + owned_count +
	                   ">=3 THEN true "
	                   "ELSE error('Batched DROP fixture has no backing/publication/auxiliary tables') END;");
	if (disable_optimizer) {
		Execute(owner, "PRAGMA disable_optimizer;");
	}
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// A preceding statement must finish before DROP looks up ownership. Both
	// mutations and trailing queries must remain in their original batch order.
	Execute(owner, string(disable_optimizer ? "SELECT 1; " : "INSERT INTO \"Source.Space\".items VALUES (3,30); ") +
	                   "DROP VIEW \"Public.Space\".\"Same.MV\"; SELECT 1;");
	if (disable_optimizer) {
		Execute(owner, "PRAGMA enable_optimizer;");
	}
	Execute(owner, "SELECT CASE WHEN " + owned_count +
	                   "=0 AND "
	                   "(SELECT count(*) FROM " +
	                   metadata_table + " WHERE view_name=" + Value(view_key).ToSQLString() +
	                   ")=0 THEN true ELSE error('Batched view DROP leaked metadata or maintenance tables') END;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	bool retained = explicit_transaction && !commit;
	Execute(owner, "SELECT CASE WHEN (SELECT count(*) FROM " + metadata_table +
	                   " WHERE view_name=" + Value(view_key).ToSQLString() + ")=" + string(retained ? "1" : "0") +
	                   " THEN true ELSE error('Batched DROP commit/rollback lost registry identity') END;");
	Execute(owner, "INSERT INTO \"Source.Space\".items VALUES (1,10),(2,20),(3,30),(3,30); "
	               "DELETE FROM \"Source.Space\".items WHERE x=10; "
	               "UPDATE \"Source.Space\".items SET x=40 WHERE x=20; "
	               "INSERT INTO \"Source.Space\".items VALUES (1,10),(4,40),(5,50); "
	               "DELETE FROM \"Source.Space\".items WHERE x=50; "
	               "UPDATE \"Source.Space\".items SET x=20 WHERE x=40;");
	vector<pair<string, string>> survivors = {
	    {"\"Other.Public\".\"Same.MV\"", "SELECT * FROM \"Source.Space\".items"},
	};
	if (retained) {
		survivors.push_back(
		    {"\"Public.Space\".\"Same.MV\"", "SELECT g, COUNT(DISTINCT x) FROM \"Source.Space\".items GROUP BY g"});
	}
	for (auto &view : survivors) {
		Execute(owner, "PRAGMA refresh('" + view.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view.first + " EXCEPT ALL " + view.second +
		                   ") OR EXISTS (" + view.second + " EXCEPT ALL SELECT * FROM " + view.first +
		                   ") THEN error('Batched view DROP survivor bag mismatch') ELSE true END;");
	}
	if (retained) {
		Execute(owner, "BEGIN; DROP VIEW \"Public.Space\".\"Same.MV\"; "
		               "DROP VIEW \"Other.Public\".\"Same.MV\"; COMMIT;");
	} else {
		Execute(owner, "DROP VIEW \"Other.Public\".\"Same.MV\"; SELECT 1;");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Batched final consumer cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Schema CASCADE must discard local appends to each removed allocation, even
// when its SQL name is reused before commit. Another schema's same-named table
// and its pending source deltas must survive.
static void TestSchemaDropPendingAppends(bool commit, bool executor_drop) {
	std::cerr << "CASE schema DROP pending appends commit=" << commit << " executor_drop=" << executor_drop
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	const string source = "memory.\"Drop;Space\".items";
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Drop;Space\"; CREATE TABLE " +
	                   source + "(i INTEGER); INSERT INTO " + source +
	                   " VALUES (1),(1),(2); "
	                   "CREATE TABLE main.items(i INTEGER); INSERT INTO main.items VALUES (3),(3),(4); "
	                   "CREATE MATERIALIZED VIEW main.mv AS SELECT * FROM main.items; "
	                   "USE memory.\"Control.Space\"; BEGIN; "
	                   "INSERT INTO main.items VALUES (1),(2),(3),(3); DELETE FROM main.items WHERE i=1; "
	                   "UPDATE main.items SET i=4 WHERE i=2;");
	for (idx_t generation = 0; generation < 2; generation++) {
		Execute(owner, "INSERT INTO " + source + " VALUES (5),(5),(6); DELETE FROM " + source + " WHERE i=5; UPDATE " +
		                   source + " SET i=9 WHERE i=6;");
		Execute(owner,
		        executor_drop
		            ? "SELECT * FROM openivm_execute_drop_schema('memory', '', 'Drop;Space', true, false);"
		            : "SELECT 'before;schema'; /* pending; appends */ DROP SCHEMA memory.\"Drop;Space\" CASCADE;");
		Execute(owner, "CREATE SCHEMA memory.\"Drop;Space\"; CREATE TABLE " + source + "(i INTEGER); INSERT INTO " +
		                   source + " VALUES (7),(7),(8);");
	}
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	const vector<pair<string, string>> expected = {
	    {source, commit ? "SELECT * FROM (VALUES (7),(7),(8))" : "SELECT * FROM (VALUES (1),(1),(2))"},
	    {"main.items",
	     commit ? "SELECT * FROM (VALUES (3),(3),(3),(3),(4),(4))" : "SELECT * FROM (VALUES (3),(3),(4))"}};
	for (auto &table : expected) {
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + table.first + " EXCEPT ALL " + table.second +
		                   ") OR EXISTS (" + table.second + " EXCEPT ALL SELECT * FROM " + table.first +
		                   ") THEN error('Schema reuse lost committed or restored appends') ELSE true END;");
	}
	Execute(owner, "INSERT INTO main.items VALUES (1),(2),(3),(3); DELETE FROM main.items WHERE i=1; "
	               "UPDATE main.items SET i=4 WHERE i=2;");
	RefreshAndCheckBag(owner, "main.mv", "SELECT * FROM main.items");
	Execute(owner,
	        "DROP VIEW main.mv; DROP SCHEMA memory.\"Drop;Space\" CASCADE; "
	        "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Schema append fixture leaked maintenance tables') END "
	        "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

// Schema CASCADE must clean both public MVs and out-of-schema consumers of
// sources, while retaining an independent sibling's shared physical delta.
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
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + auxiliary_table + " EXCEPT ALL " + expected +
		                   ") OR EXISTS (" + expected + " EXCEPT ALL SELECT * FROM " + auxiliary_table +
		                   ") THEN error('Schema CASCADE auxiliary bag mismatch') ELSE true END;");
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

static void TestOwnedNamespaceCascade(const string &schema, bool commit) {
	std::cerr << "CASE owned namespace cascade schema=" << schema << " commit=" << commit << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1),(1),(2); "
	               "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source; "
	               "CREATE MATERIALIZED VIEW sibling AS SELECT * FROM source;");
	auto initial = owner.Query("SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space'");
	if (initial->HasError() || initial->RowCount() != 1) {
		throw InvalidInputException("Owned namespace fixture could not read initial table count");
	}
	auto initial_count = initial->GetValue(0, 0).GetValue<int64_t>();
	Execute(owner, "BEGIN; DROP SCHEMA " + SqlUtils::QuoteIdentifier(schema) + " CASCADE;");
	Execute(owner, "SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM duckdb_views() WHERE view_name IN ('mv','sibling')) "
	               "AND NOT EXISTS (SELECT 1 FROM duckdb_tables() WHERE schema_name='Internal Space') "
	               "THEN true ELSE error('Owned namespace CASCADE left live consumers or tables') END;");
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	Execute(owner, "SELECT CASE WHEN (SELECT count(*) FROM duckdb_views() WHERE view_name IN ('mv','sibling'))=" +
	                   string(commit ? "0" : "2") +
	                   " AND (SELECT count(*) FROM duckdb_tables() WHERE schema_name='Internal Space')=" +
	                   to_string(commit ? 0 : initial_count) +
	                   " THEN true ELSE error('Owned namespace transaction lost ownership') END;");
	if (commit) {
		// Missing control/internal schemas are recreated by the normal lifecycle.
		Execute(owner, "CREATE MATERIALIZED VIEW mv AS SELECT * FROM source;");
	}
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (4),(5); DELETE FROM source WHERE i=5;");
	RefreshAndCheckBag(owner, "mv", "SELECT * FROM source");
	if (!commit) {
		RefreshAndCheckBag(owner, "sibling", "SELECT * FROM source");
		Execute(owner, "DROP VIEW sibling;");
	}
	Execute(owner, "DROP VIEW mv;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Owned namespace final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
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
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + expected_query +
	                   ") OR EXISTS (" + expected_query + " EXCEPT ALL SELECT * FROM " + view +
	                   ") THEN error('Scheduled lifecycle changed literal contents') ELSE true END;");
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
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL " + expected_query +
	                   ") OR EXISTS (" + expected_query + " EXCEPT ALL SELECT * FROM " + view +
	                   ") THEN error('Mixed lifecycle initial load bag mismatch') ELSE true END;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2; INSERT INTO source VALUES (1),(5),(5); "
	               "DELETE FROM source WHERE i=5; UPDATE source SET i=2 WHERE i=4;");
	for (auto &survivor : vector<pair<string, string>> {{view, expected_query}, {"sibling", "SELECT * FROM source"}}) {
		Execute(owner, "PRAGMA refresh('" + survivor.first + "');");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + survivor.first + " EXCEPT ALL " + survivor.second +
		                   ") OR EXISTS (" + survivor.second + " EXCEPT ALL SELECT * FROM " + survivor.first +
		                   ") THEN error('Mixed lifecycle survivor bag mismatch') ELSE true END;");
	}
	Execute(owner, "DROP VIEW " + view + "; DROP VIEW sibling; SELECT 1;");
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Mixed lifecycle final cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal;Space';");
}

static void TestSequentialCreateBatch(bool explicit_transaction, bool commit) {
	std::cerr << "CASE sequential CREATE batch explicit_transaction=" << explicit_transaction << " commit=" << commit
	          << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "PRAGMA enable_profiling='no_output'; PRAGMA enable_verification;");
	const auto metrics = ClientConfig::GetConfig(*owner.context).profiler_settings;
	// BEGIN, schema/table creation, DML, and MV creation are all in one input.
	// No source exists when DuckDB preprocesses it. Each replacement must see
	// the caller's preceding DROP and intervening mutations.
	Execute(owner,
	        string(explicit_transaction ? "BEGIN; " : "") +
	            "CREATE SCHEMA \"Public;Space\"; CREATE TABLE \"Public;Space\".source(i INTEGER); "
	            "INSERT INTO \"Public;Space\".source VALUES (1),(1),(2); "
	            "CREATE MATERIALIZED VIEW \"Public;Space\".mv AS SELECT * FROM \"Public;Space\".source; "
	            "DROP VIEW \"Public;Space\".mv; INSERT INTO \"Public;Space\".source VALUES (3),(3); "
	            "CREATE MATERIALIZED VIEW \"Public;Space\".mv AS SELECT * FROM \"Public;Space\".source; "
	            "CREATE OR REPLACE MATERIALIZED VIEW \"Public;Space\".mv AS SELECT * FROM \"Public;Space\".source;");
	if (ClientConfig::GetConfig(*owner.context).profiler_settings != metrics) {
		throw InvalidInputException("Lifecycle execution did not restore profiling metrics");
	}
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM \"Public;Space\".mv EXCEPT ALL "
	               "SELECT * FROM \"Public;Space\".source) OR EXISTS (SELECT * FROM \"Public;Space\".source "
	               "EXCEPT ALL SELECT * FROM \"Public;Space\".mv) "
	               "THEN error('Sequential CREATE initial load mismatch') ELSE true END;");
	if (explicit_transaction) {
		Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	}
	if (explicit_transaction && !commit) {
		Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('Sequential CREATE rollback leaked objects') "
		               "END FROM duckdb_tables() WHERE schema_name IN ('Public;Space','Internal Space');");
		return;
	}
	Execute(owner, "INSERT INTO \"Public;Space\".source VALUES (1),(2),(3),(3); "
	               "DELETE FROM \"Public;Space\".source WHERE i=1; "
	               "UPDATE \"Public;Space\".source SET i=4 WHERE i=2;");
	Execute(owner, "PRAGMA refresh('\"Public;Space\".mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM \"Public;Space\".mv EXCEPT ALL "
	               "SELECT * FROM \"Public;Space\".source) OR EXISTS (SELECT * FROM \"Public;Space\".source "
	               "EXCEPT ALL SELECT * FROM \"Public;Space\".mv) "
	               "THEN error('Sequential CREATE refresh bag mismatch') ELSE true END;");
	Execute(owner, "DROP VIEW \"Public;Space\".mv; SELECT CASE WHEN count(*)=0 THEN true "
	               "ELSE error('Sequential CREATE final cleanup leaked') END FROM duckdb_tables() "
	               "WHERE schema_name='Internal Space';");
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
	if (batched) {
		Execute(owner, StringUtil::Join(statements, "; ") + ";");
	} else {
		for (auto &sql : statements) {
			Execute(owner, sql + ";");
		}
	}
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
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + survivor +
		                   " EXCEPT ALL SELECT * FROM main.source) OR EXISTS (SELECT * FROM main.source "
		                   "EXCEPT ALL SELECT * FROM " +
		                   survivor + ") THEN error('CREATE/ALTER survivor bag mismatch') ELSE true END;");
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
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view +
		                   " EXCEPT ALL SELECT * FROM \"Source.Space\".source) "
		                   "OR EXISTS (SELECT * FROM \"Source.Space\".source EXCEPT ALL SELECT * FROM " +
		                   view + ") THEN error('Migrated MV bag mismatch') ELSE true END;");
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
	if (batched) {
		Execute(owner, StringUtil::Join(statements, "; ") + ";");
	} else {
		for (auto &statement : statements) {
			Execute(owner, statement + ";");
		}
	}
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
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + survivor.first + " EXCEPT ALL " + survivor.second +
		                   ") OR EXISTS (" + survivor.second + " EXCEPT ALL SELECT * FROM " + survivor.first +
		                   ") THEN error('Setting transaction survivor bag mismatch') ELSE true END;");
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
		throw InvalidInputException("Failed CREATE fixture did not reach the intended initial-load conversion error");
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
		Execute(owner,
		        "SELECT CASE WHEN EXISTS (SELECT * FROM " + survivor +
		            " EXCEPT ALL SELECT i::TINYINT FROM main.source) OR EXISTS (SELECT i::TINYINT FROM main.source "
		            "EXCEPT ALL SELECT * FROM " +
		            survivor + ") THEN error('Failed CREATE retry survivor bag mismatch') ELSE true END;");
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

static void TestAlterTableNamespace(bool existing_metadata, bool batched, bool commit, bool peer_view) {
	std::cerr << "CASE ALTER TABLE namespace existing_metadata=" << existing_metadata << " batched=" << batched
	          << " commit=" << commit << " peer_view=" << peer_view << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; SET openivm_cascade_refresh='off'; "
	               "SET openivm_metadata_schema='Control.Space'; SET openivm_internal_schema='Internal Space'; "
	               "CREATE SCHEMA \"Source;Space\"; CREATE SCHEMA \"Other Source\"; "
	               "CREATE TABLE \"Source;Space\".items(i INTEGER); CREATE TABLE \"Other Source\".items(i INTEGER); "
	               "INSERT INTO \"Source;Space\".items VALUES (1),(1),(2); "
	               "INSERT INTO \"Other Source\".items VALUES (8),(8),(9); ATTACH ':memory:' AS peer;");
	if (existing_metadata) {
		Execute(owner,
		        "CREATE MATERIALIZED VIEW \"Other Source\".\"Same;MV\" AS SELECT i FROM \"Other Source\".items;");
	}
	const string view = peer_view ? "peer.main.\"Same;MV\"" : "\"Source;Space\".\"Same;MV\"";
	const string create =
	    "CREATE MATERIALIZED VIEW " + view + " AS SELECT i AS original FROM memory.\"Source;Space\".items";
	// Cross-native-catalog DDL retains the existing committed-helper/undo route.
	// New metadata in BEGIN is tested in its source's native catalog.
	if (peer_view) {
		Execute(owner, create + ";");
	}
	vector<string> statements = {"BEGIN"};
	if (!peer_view) {
		statements.push_back(create);
	}
	statements.push_back("ALTER TABLE memory.\"Source;Space\".items RENAME COLUMN i TO \"New;Column\"");
	statements.push_back("ALTER TABLE memory.\"Source;Space\".items RENAME COLUMN \"New;Column\" TO \"Final.Column\"");
	statements.push_back("ALTER TABLE memory.\"Source;Space\".items ADD COLUMN scratch INTEGER");
	statements.push_back("ALTER TABLE memory.\"Source;Space\".items DROP COLUMN scratch");
	if (batched) {
		Execute(owner, StringUtil::Join(statements, "; ") + ";");
	} else {
		for (auto &statement : statements) {
			Execute(owner, statement + ";");
		}
	}
	auto rejected_drop = owner.Query("ALTER TABLE memory.\"Source;Space\".items DROP COLUMN \"Final.Column\";");
	if (!rejected_drop->HasError() ||
	    rejected_drop->GetError().find("referenced by materialized view") == string::npos ||
	    rejected_drop->GetError().find("Same;MV") == string::npos) {
		throw InvalidInputException("Caller-visible namespace dependency did not reject DROP COLUMN");
	}
	if (!peer_view) {
		Execute(owner, "INSERT INTO memory.\"Source;Space\".items VALUES (1),(2),(3),(3); "
		               "DELETE FROM memory.\"Source;Space\".items WHERE \"Final.Column\"=1; "
		               "UPDATE memory.\"Source;Space\".items SET \"Final.Column\"=4 WHERE \"Final.Column\"=2;");
		Execute(owner, "PRAGMA refresh(" + Value(view).ToSQLString() + ");");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view +
		                   " EXCEPT ALL SELECT \"Final.Column\" FROM memory.\"Source;Space\".items) OR EXISTS "
		                   "(SELECT \"Final.Column\" FROM memory.\"Source;Space\".items EXCEPT ALL SELECT * FROM " +
		                   view + ") THEN error('Caller-visible ALTER TABLE bag mismatch') ELSE true END;");
	}
	Execute(owner, commit ? "COMMIT;" : "ROLLBACK;");
	if (commit || peer_view) {
		const string column = commit ? "\"Final.Column\"" : "i";
		Execute(owner, "INSERT INTO memory.\"Source;Space\".items VALUES (1),(2),(3),(3); "
		               "DELETE FROM memory.\"Source;Space\".items WHERE " +
		                   column +
		                   "=1; "
		                   "UPDATE memory.\"Source;Space\".items SET " +
		                   column + "=4 WHERE " + column + "=2;");
		Execute(owner, "PRAGMA refresh(" + Value(view).ToSQLString() + ");");
		Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM " + view + " EXCEPT ALL SELECT " + column +
		                   " FROM memory.\"Source;Space\".items) OR EXISTS (SELECT " + column +
		                   " FROM memory.\"Source;Space\".items EXCEPT ALL SELECT * FROM " + view +
		                   ") THEN error('ALTER TABLE transaction outcome bag mismatch') ELSE true END;");
		Execute(owner, "DROP VIEW " + view + ";");
	}
	if (existing_metadata) {
		Execute(owner,
		        "INSERT INTO \"Other Source\".items VALUES (8),(9),(10),(10); "
		        "DELETE FROM \"Other Source\".items WHERE i=8; UPDATE \"Other Source\".items SET i=11 WHERE i=9;");
		Execute(owner, "PRAGMA refresh('\"Other Source\".\"Same;MV\"');");
		Execute(owner,
		        "SELECT CASE WHEN EXISTS (SELECT * FROM \"Other Source\".\"Same;MV\" EXCEPT ALL "
		        "SELECT * FROM \"Other Source\".items) OR EXISTS (SELECT * FROM \"Other Source\".items EXCEPT ALL "
		        "SELECT * FROM \"Other Source\".\"Same;MV\") THEN error('ALTER TABLE changed sibling identity') ELSE "
		        "true END;");
		Execute(owner, "DROP VIEW \"Other Source\".\"Same;MV\";");
	}
	Execute(owner, "SELECT CASE WHEN count(*)=0 THEN true ELSE error('ALTER TABLE namespace cleanup leaked') END "
	               "FROM duckdb_tables() WHERE schema_name='Internal Space';");
}

static void TestLateLifecycleBindFailure(bool explicit_transaction) {
	std::cerr << "CASE late lifecycle bind failure explicit_transaction=" << explicit_transaction << std::endl;
	DuckDB db(nullptr);
	Connection owner(db);
	Execute(owner, "LOAD openivm; SET openivm_disable_daemon=true; CREATE TABLE source(i INTEGER); "
	               "INSERT INTO source VALUES (1),(1),(2); CREATE MATERIALIZED VIEW mv AS SELECT * FROM source; "
	               "PRAGMA enable_profiling='no_output';");
	const auto metrics = ClientConfig::GetConfig(*owner.context).profiler_settings;
	if (explicit_transaction) {
		Execute(owner, "BEGIN;");
	}
	// Exercise the same owning program abstraction with a deterministic *late*
	// binder failure, after successful schema/table creation and captured DML.
	// The public logical-statement API avoids introducing a test-only SQL function.
	auto binder = Binder::CreateBinder(*owner.context);
	auto plan =
	    BindLifecycleProgram(*owner.context, *binder,
	                         "CREATE SCHEMA \"Late;Schema\"; "
	                         "CREATE TABLE \"Late;Schema\".scratch AS SELECT i, count(*) FROM source GROUP BY i; "
	                         "CREATE TABLE \"Late;Schema\".recursive AS WITH RECURSIVE r(i) AS "
	                         "(VALUES (1) UNION ALL SELECT i+1 FROM r WHERE i<3) SELECT * FROM r; "
	                         "INSERT INTO source VALUES (7); SELECT missing_lifecycle_column FROM source;");
	plan->ResolveOperatorTypes();
	auto statement = make_uniq<LogicalPlanStatement>(std::move(plan));
	statement->query = "OpenIVM late lifecycle bind failure regression";
	auto result = owner.Query(std::move(statement));
	if (!result->HasError() || result->GetError().find("missing_lifecycle_column") == string::npos) {
		throw InvalidInputException("Lifecycle fixture did not reach its intended late binder failure");
	}
	if (ClientConfig::GetConfig(*owner.context).profiler_settings != metrics) {
		throw InvalidInputException("Failed lifecycle execution did not restore profiling metrics");
	}
	if (explicit_transaction) {
		// A binder error normally permits COMMIT. Partial lifecycle writes must
		// instead invalidate the transaction, so COMMIT rolls all of them back.
		Execute(owner, "COMMIT;");
	}
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM (VALUES (1),(1),(2))) "
	               "OR EXISTS (SELECT * FROM (VALUES (1),(1),(2)) EXCEPT ALL SELECT * FROM source) "
	               "OR EXISTS (SELECT 1 FROM duckdb_schemas() WHERE schema_name='Late;Schema') "
	               "THEN error('Failed lifecycle committed partial writes') ELSE true END;");
	Execute(owner, "INSERT INTO source VALUES (1),(2),(3),(3); DELETE FROM source WHERE i=1; "
	               "UPDATE source SET i=4 WHERE i=2;");
	Execute(owner, "PRAGMA refresh('mv');");
	Execute(owner, "SELECT CASE WHEN EXISTS (SELECT * FROM mv EXCEPT ALL SELECT * FROM source) "
	               "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM mv) "
	               "THEN error('Failed lifecycle damaged delta capture') ELSE true END;");
	Execute(owner, "DROP VIEW mv;");
}

int main(int argc, char **argv) {
	if (argc != 2) {
		std::cerr << "Usage: source_delta_publication_test <fresh-database-path>" << std::endl;
		return 2;
	}
	try {
		TestQuotedNamespaceNormalization();
		TestQualifiedTableReferences();
		TestIdentifierComparisons();
		TestTableColumnIdentity();
		TestInternalSchemaReopen(argv[1]);
		TestSourceDeltaLookup();
		TestSharedMetadataUpgrade(false, true);
		TestSharedMetadataUpgrade(true, true);
		TestSharedMetadataUpgrade(true, false);
		TestMetadataReplayMutationOwner(true);
		TestMetadataReplayMutationOwner(false);
		for (bool executor_drop : {false, true}) {
			TestDeferredScratchDrop(false, true, executor_drop);
			TestDeferredScratchDrop(true, true, executor_drop);
			TestDeferredScratchDrop(true, false, executor_drop);
		}
		TestSequentialCreateBatch(false, true);
		TestSequentialCreateBatch(true, true);
		TestSequentialCreateBatch(true, false);
		TestMetadataSchemaMigration(false);
		TestMetadataSchemaMigration(true);
		for (bool existing_metadata : {false, true}) {
			for (bool batched : {false, true}) {
				TestCreateAlterTransaction(existing_metadata, batched, true);
				TestCreateAlterTransaction(existing_metadata, batched, false);
				TestMetadataSchemaSettingTransaction(existing_metadata, batched, true);
				TestMetadataSchemaSettingTransaction(existing_metadata, batched, false);
			}
		}
		for (bool dedicated_schema : {false, true}) {
			for (bool peer_view : {false, true}) {
				TestFailedCreateSourceDelta(dedicated_schema, peer_view, false);
				TestFailedCreateSourceDelta(dedicated_schema, peer_view, true);
			}
		}
		for (bool existing_metadata : {false, true}) {
			for (bool batched : {false, true}) {
				TestAlterTableNamespace(existing_metadata, batched, true, false);
				TestAlterTableNamespace(existing_metadata, batched, false, false);
			}
		}
		TestAlterTableNamespace(true, false, true, true);
		TestAlterTableNamespace(true, false, false, true);
		TestLateLifecycleBindFailure(false);
		TestLateLifecycleBindFailure(true);
		TestMetadataReplayLocations();
		TestMetadataQueryText(false, true);
		TestMetadataQueryText(true, true);
		TestMetadataQueryText(true, false);
		for (bool dedicated_schema : {false, true}) {
			TestCrossCatalogSharedDelta(dedicated_schema, false);
			TestCrossCatalogSharedDelta(dedicated_schema, true);
		}
		for (bool cross_catalog : {false, true}) {
			TestPublicationOwnerCleanup(cross_catalog, false, false, true);
			TestPublicationOwnerCleanup(cross_catalog, true, true, false);
			TestPublicationOwnerCleanup(cross_catalog, true, true, true);
		}
		TestPublicationOwnerCleanup(false, false, true, false);
		TestPublicationOwnerCleanup(false, false, true, true);
		for (bool commit : {false, true}) {
			TestCrossCatalogPublicationConsumers(commit, "Internal Space", false);
			for (const auto &internal_schema : vector<string> {"", "Control.Space", "Internal Space"}) {
				TestCrossCatalogPublicationConsumers(commit, internal_schema, true);
			}
		}
		for (bool batched_drop : {false, true}) {
			TestViewCascade(false, false, true, batched_drop);
			TestViewCascade(true, false, true, batched_drop);
			TestViewCascade(false, true, true, batched_drop);
			TestViewCascade(false, true, false, batched_drop);
			TestViewCascade(true, true, false, batched_drop);
		}
		TestPublicationOwnerReplay(false);
		TestPublicationOwnerReplay(true);
		TestDeltaOwnershipSearchPath(false);
		TestDeltaOwnershipSearchPath(true);
		for (const auto &mutation : vector<string> {"create", "drop", "replace", "recreate"}) {
			for (bool batched_drop : {false, true}) {
				TestSourceCascadeConsumerLocations(mutation, false, batched_drop, true);
				TestSourceCascadeConsumerLocations(mutation, true, batched_drop, true);
				TestSourceCascadeConsumerLocations(mutation, true, batched_drop, false);
			}
		}
		TestStagedCascadeConsumerIdentity();
		for (bool executor_drop : {false, true}) {
			TestSchemaDropPendingAppends(true, executor_drop);
			TestSchemaDropPendingAppends(false, executor_drop);
		}
		TestSchemaCascade(false, true, false);
		TestSchemaCascade(true, true, false);
		TestSchemaCascade(true, false, false);
		TestSchemaCascade(false, true, true);
		for (const auto &schema : vector<string> {"Control.Space", "Internal Space"}) {
			TestOwnedNamespaceCascade(schema, true);
			TestOwnedNamespaceCascade(schema, false);
		}
		for (bool batched_drop : {false, true}) {
			TestUnicodeSourceCascade(false, true, false, batched_drop);
			TestUnicodeSourceCascade(true, true, false, batched_drop);
			TestUnicodeSourceCascade(true, false, false, batched_drop);
			// Writes spanning native catalogs use the autocommit staged route.
			TestUnicodeSourceCascade(false, true, true, batched_drop);
		}
		for (bool disable_optimizer : {false, true}) {
			TestBatchedViewDrop(false, true, disable_optimizer);
			TestBatchedViewDrop(true, true, disable_optimizer);
			TestBatchedViewDrop(true, false, disable_optimizer);
		}
		TestPublication(false);
		TestPublication(true);
		TestConcurrentUntrackedAppends();
		for (bool cross_catalog : {false, true}) {
			TestIndependentUntrackedWriters(cross_catalog, false);
			TestIndependentUntrackedWriters(cross_catalog, true);
		}
		for (bool dedicated_schema : {false, true}) {
			for (const auto &operation : vector<string> {"insert", "delete", "update", "merge"}) {
				for (bool tracked : {false, true}) {
					TestPlanningOnlySourceAdmission(dedicated_schema, tracked, operation, "explain", false);
					TestPlanningOnlySourceAdmission(dedicated_schema, tracked, operation, "explain", true);
				}
				TestExecutedSourceAdmission(dedicated_schema, operation, "analyze", true);
			}
			for (const auto &mode : vector<string> {"prepare", "api_prepare"}) {
				for (bool tracked : {false, true}) {
					TestPlanningOnlySourceAdmission(dedicated_schema, tracked, "insert", mode, false);
					TestPlanningOnlySourceAdmission(dedicated_schema, tracked, "insert", mode, true);
				}
			}
			for (const auto &mode : vector<string> {"analyze", "execute", "api_execute"}) {
				TestExecutedSourceAdmission(dedicated_schema, "insert", mode, false);
				if (mode != "analyze") {
					TestExecutedSourceAdmission(dedicated_schema, "insert", mode, true);
				}
			}
			TestFirstCreateWithOldSourceSnapshot(dedicated_schema, false);
			TestFirstCreateWithOldSourceSnapshot(dedicated_schema, true);
			TestFirstCreateWithOldSourceSnapshot(dedicated_schema, false, true);
			TestFirstCreateWithOldSourceSnapshot(dedicated_schema, true, true);
			TestFirstCreateAfterNonSourceCommits(dedicated_schema, false);
			TestFirstCreateAfterNonSourceCommits(dedicated_schema, true);
			TestFirstCreateAfterTrackedConsumerDrop(dedicated_schema, false);
			TestFirstCreateAfterTrackedConsumerDrop(dedicated_schema, true);
			TestIndependentWriterDuringPublication(dedicated_schema, false);
			TestIndependentWriterDuringPublication(dedicated_schema, true);
			TestSourceAdmissionLockOrder(dedicated_schema, false);
			TestSourceAdmissionLockOrder(dedicated_schema, true);
			for (const auto &first_operation : vector<string> {"insert", "delete", "update"}) {
				TestDMLDuringPublication(dedicated_schema, first_operation, false, true);
				TestDMLDuringPublication(dedicated_schema, first_operation, true, true);
			}
			TestDMLDuringPublication(dedicated_schema, "insert", true, false);
			TestDMLDuringPublication(dedicated_schema, "insert", false, true, false);
			TestDMLBeforePublication(dedicated_schema, true);
			TestDMLBeforePublication(dedicated_schema, false);
			TestDMLWithInvisibleDelta(dedicated_schema);
		}
		for (bool dedicated_schema : {false, true}) {
			for (bool peer_view : {false, true}) {
				TestCreateAfterCommittedDeltaRemoval(dedicated_schema, peer_view, false);
				TestCreateAfterCommittedDeltaRemoval(dedicated_schema, peer_view, true);
			}
		}
		TestDropDuringPublication(false, false);
		TestDropDuringPublication(true, false);
		TestDropDuringPublication(false, true);
		TestDropDuringPublication(false, false, true);
		TestDropDuringPublication(true, false, true);
		TestDropDuringPublication(false, true, true);
		TestMixedLifecycleBatch(false, true);
		TestMixedLifecycleBatch(true, true);
		TestMixedLifecycleBatch(true, false);
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &ex) {
		std::cerr << ex.what() << std::endl;
		return 1;
	}
}
