#include "duckdb.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include <iostream>

using namespace duckdb;

static void Execute(Connection &con, const string &sql) {
	auto result = con.Query(sql);
	if (result->HasError()) {
		throw InvalidInputException(result->GetError());
	}
}

int main(int argc, char **argv) {
	if (argc != 2) {
		return 2;
	}
	try {
		DuckDB db(nullptr);
		Connection con(db);
		auto path = SqlUtils::EscapeValue(argv[1]);
		Execute(con, "LOAD openivm; LOAD ducklake; SET openivm_files_path='" + path +
		                 ".files'; "
		                 "ATTACH 'ducklake:sqlite:" +
		                 path + "' AS lake (DATA_PATH '" + path +
		                 ".data'); "
		                 "CREATE TABLE lake.source(i INTEGER); ATTACH ':memory:' AS other; "
		                 "ATTACH ':memory:' AS empty_catalog;");
		RefreshMetadata metadata(con);
		if (!metadata.GetScheduledViews().empty()) {
			throw InvalidInputException("Expected no scheduled views");
		}
		for (auto catalog : {"memory", "other"}) {
			Execute(con, "USE " + string(catalog));
			Execute(con, "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1), (1), (2);");
			Execute(con, "CREATE MATERIALIZED VIEW scheduled REFRESH EVERY '1 hour' AS SELECT * FROM source;");
			Execute(con, "INSERT INTO source VALUES (2), (3); DELETE FROM source WHERE i=1; "
			             "UPDATE source SET i=4 WHERE i=2;");
			Execute(con, "PRAGMA refresh('scheduled');");
			Execute(con, "SELECT CASE WHEN EXISTS (SELECT * FROM scheduled EXCEPT ALL SELECT * FROM source) "
			             "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM scheduled) "
			             "THEN error('Scheduled view differs from source') ELSE true END;");
		}
		std::cout << "READY" << std::endl;
		string line;
		std::getline(std::cin, line);
		auto views = metadata.GetScheduledViews();
		if (views.size() != 2 || views[0].metadata_catalog != "memory" || views[1].metadata_catalog != "other" ||
		    views[0].view_name != "scheduled" || views[1].view_name != "scheduled") {
			throw InvalidInputException("Scheduler did not discover both native metadata catalogs");
		}
		// A native schema change must synchronize its delta table even while an
		// unrelated DuckLake catalog is locked. A global information_schema scan
		// can fail and silently skip that synchronization.
		Execute(con, "ALTER TABLE source ADD COLUMN extra INTEGER DEFAULT 7;");
		Execute(con, "SELECT extra FROM openivm_delta_source LIMIT 0;");
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &e) {
		std::cerr << e.what() << std::endl;
		return 1;
	}
}
