// SQL builders for the daemon benchmark's access to OpenIVM metadata.
//
// `openivm_views.view_name` (and the `view_name` column of the dependent metadata tables) holds the
// internal view key (`__openivm_mv_...`); the SQL-facing name lives in `view_sql_name`, next to
// `view_catalog` / `view_schema`. Metadata written before SQL names were stored separately has NULL
// `view_sql_name` and keeps the SQL name in `view_name`, hence the COALESCE.
#pragma once

#include <string>
#include <vector>

namespace openivm_bench {

inline std::string SqlLiteral(const std::string &s) {
	std::string out = "'";
	for (char c : s) {
		if (c == '\'') {
			out += '\'';
		}
		out += c;
	}
	return out + "'";
}

// Internal keys of all benchmark views (SQL name mv_*), regardless of catalog/schema.
inline std::string StaleBenchmarkKeysSQL() {
	return "SELECT view_name FROM openivm_views WHERE COALESCE(view_sql_name, view_name) LIKE 'mv\\_%' ESCAPE '\\'";
}

// Deletes dependent rows first, then openivm_views, so the key subquery still resolves.
// Rows whose view_name itself is a legacy mv_* name are matched directly, which also removes orphans.
inline std::vector<std::string> StaleBenchmarkMetadataCleanupSQL() {
	const std::string keys = StaleBenchmarkKeysSQL();
	const std::string legacy = "LIKE 'mv\\_%' ESCAPE '\\'";
	std::vector<std::string> out;
	for (auto table : {"openivm_delta_tables", "openivm_refresh_history", "openivm_refresh_hooks"}) {
		out.push_back(std::string("DELETE FROM ") + table + " WHERE view_name IN (" + keys + ") OR view_name " + legacy);
	}
	out.push_back("DELETE FROM openivm_mv_dependencies WHERE parent_view IN (" + keys + ") OR child_view IN (" +
	              keys + ") OR parent_view " + legacy + " OR child_view " + legacy);
	out.push_back("DELETE FROM openivm_views WHERE view_name IN (" + keys + ") OR view_name " + legacy);
	return out;
}

// Internal key of one view given its SQL name and optional "catalog.schema." qualifier (as in
// MVDef::catalog_schema). An empty qualifier means the connection's current catalog and schema.
inline std::string ViewKeySQL(const std::string &name, const std::string &catalog_schema) {
	std::string catalog = "current_database()";
	std::string schema = "current_schema()";
	auto dot = catalog_schema.find('.');
	if (dot != std::string::npos) {
		catalog = SqlLiteral(catalog_schema.substr(0, dot));
		auto rest = catalog_schema.substr(dot + 1);
		if (!rest.empty() && rest.back() == '.') {
			rest.pop_back();
		}
		schema = SqlLiteral(rest);
	}
	return "SELECT view_name FROM openivm_views WHERE lower(COALESCE(view_sql_name, view_name)) = lower(" +
	       SqlLiteral(name) + ") AND lower(COALESCE(view_catalog, " + catalog + ")) = lower(" + catalog +
	       ") AND lower(COALESCE(view_schema, 'main')) = lower(" + schema + ")";
}

inline std::string DeltaMetadataDiagnosticSQL(const std::string &name, const std::string &catalog_schema) {
	return "SELECT table_name, last_update FROM openivm_delta_tables WHERE view_name IN (" +
	       ViewKeySQL(name, catalog_schema) + ")";
}

} // namespace openivm_bench
