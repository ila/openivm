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
		                 "ATTACH ':memory:' AS configured; ATTACH ':memory:' AS empty_catalog;");
		RefreshMetadata metadata(con);
		if (!metadata.GetScheduledViews().empty()) {
			throw InvalidInputException("Expected no scheduled views");
		}
		// The last catalog separates control metadata and maintenance objects into quoted schemas.
		for (auto catalog : {"memory", "other", "configured"}) {
			bool configured = string(catalog) == "configured";
			Execute(con, "USE " + string(catalog));
			Execute(con, string("SET openivm_metadata_schema = '") + (configured ? "OpenIVM Control" : "") + "';");
			Execute(con, string("SET openivm_internal_schema = '") + (configured ? "OpenIVM Internal" : "") + "';");
			Execute(con, "CREATE TABLE source(i INTEGER); INSERT INTO source VALUES (1), (1), (2);");
			Execute(con, "CREATE MATERIALIZED VIEW scheduled REFRESH EVERY '1 hour' AS SELECT * FROM source;");
			Execute(con, "INSERT INTO source VALUES (2), (3); DELETE FROM source WHERE i=1; "
			             "UPDATE source SET i=4 WHERE i=2;");
			Execute(con, "PRAGMA refresh('scheduled');");
			Execute(con, "SELECT CASE WHEN EXISTS (SELECT * FROM scheduled EXCEPT ALL SELECT * FROM source) "
			             "OR EXISTS (SELECT * FROM source EXCEPT ALL SELECT * FROM scheduled) "
			             "THEN error('Scheduled view differs from source') ELSE true END;");
		}
		Execute(con, "SET openivm_metadata_schema = '';");
		Execute(con, "SET openivm_internal_schema = '';");
		Execute(con, "SELECT CASE WHEN (SELECT count(*) FROM duckdb_tables() WHERE database_name = 'configured' "
		             "AND schema_name = 'OpenIVM Control' AND table_name = 'openivm_views') = 1 "
		             "AND (SELECT count(*) FROM duckdb_tables() WHERE database_name = 'configured' "
		             "AND schema_name = 'main' AND table_name IN ('openivm_views', 'openivm_delta_tables', "
		             "'openivm_refresh_hooks', 'openivm_refresh_history', 'openivm_refresh_profile', "
		             "'openivm_mv_dependencies')) = 0 "
		             "AND (SELECT count(*) FROM duckdb_tables() WHERE database_name = 'configured' "
		             "AND schema_name = 'OpenIVM Internal' AND table_name = 'openivm_delta_6d61696e_source') = 1 "
		             "AND (SELECT count(*) FROM duckdb_tables() WHERE database_name = 'configured' "
		             "AND schema_name = 'main' AND starts_with(table_name, 'openivm_')) = 0 "
		             "THEN true ELSE error('Configured metadata and internal schemas were not used') END;");
		std::cout << "READY" << std::endl;
		string line;
		std::getline(std::cin, line);
		auto catalogs = RefreshMetadata::MetadataCatalogs(con);
		if (catalogs != vector<string> {"configured", "memory", "other"}) {
			throw InvalidInputException("Source metadata discovery did not find every native catalog");
		}
		auto views = metadata.GetScheduledViews();
		if (views.size() != 3 || views[0].metadata_catalog != "configured" || views[1].metadata_catalog != "memory" ||
		    views[2].metadata_catalog != "other" ||
		    views[0].view_name != "__openivm_mv_636f6e66696775726564_6d61696e_7363686564756c6564_" ||
		    views[1].view_name != "__openivm_mv_6d656d6f7279_6d61696e_7363686564756c6564_" ||
		    views[2].view_name != "__openivm_mv_6f74686572_6d61696e_7363686564756c6564_") {
			throw InvalidInputException("Scheduler did not discover every native metadata catalog");
		}
		// A native schema change must synchronize its delta table even while an
		// unrelated DuckLake catalog is locked. A global information_schema scan
		// can fail and silently skip that synchronization.
		Execute(con, "ALTER TABLE source ADD COLUMN extra INTEGER DEFAULT 7;");
		Execute(con, "SELECT extra FROM configured.\"OpenIVM Internal\".openivm_delta_6d61696e_source LIMIT 0;");
		std::cout << "PASS" << std::endl;
	} catch (const std::exception &e) {
		std::cerr << e.what() << std::endl;
		return 1;
	}
}
