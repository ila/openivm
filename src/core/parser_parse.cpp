#include "core/parser.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/parser/parser.hpp"

#include <regex>

namespace duckdb {

ParserExtensionParseResult MaterializedViewParserExtension::ParseFunction(ParserExtensionInfo *info,
                                                                          const string &query) {
	auto query_lower = SqlUtils::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);
	// Strip SQL line comments (-- to end of line) before whitespace normalization.
	// RemoveRedundantWhitespaces collapses '\n' to ' ', which would turn
	// "-- comment\n rest" into "-- comment rest" where the rest is eaten by the comment.
	SqlUtils::StripLineComments(query_lower);
	SqlUtils::RemoveRedundantWhitespaces(query_lower);

	// Handle ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' | SET REFRESH MANUAL
	if (StringUtil::Contains(query_lower, "alter materialized view")) {
		std::regex alter_re("alter\\s+materialized\\s+view\\s+(\"(?:[^\"]+)\"|[a-zA-Z0-9_.]+)\\s+set\\s+refresh\\s+("
		                    "every\\s+'([^']+)'|manual)",
		                    std::regex::icase);
		std::smatch match;
		if (!std::regex_search(query_lower, match, alter_re)) {
			throw ParserException("Invalid ALTER MATERIALIZED VIEW syntax. "
			                      "Expected: ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' "
			                      "or ALTER MATERIALIZED VIEW <name> SET REFRESH MANUAL");
		}
		string alter_view_name = match[1].str();
		if (alter_view_name.size() >= 2 && alter_view_name.front() == '"' && alter_view_name.back() == '"') {
			alter_view_name = alter_view_name.substr(1, alter_view_name.size() - 2);
		}
		string refresh_type = StringUtil::Lower(match[2].str());
		string update_sql;
		if (refresh_type == "manual") {
			update_sql = "UPDATE " + string(openivm::VIEWS_TABLE) + " SET refresh_interval = NULL WHERE view_name = '" +
			             SqlUtils::EscapeSingleQuotes(alter_view_name) + "'";
		} else {
			int64_t interval = SqlUtils::ParseRefreshInterval(match[3].str());
			update_sql = "UPDATE " + string(openivm::VIEWS_TABLE) + " SET refresh_interval = " + to_string(interval) +
			             " WHERE view_name = '" + SqlUtils::EscapeSingleQuotes(alter_view_name) + "'";
		}
		// Pass the UPDATE SQL through MaterializedViewParseData; PlanFunction will execute it
		Parser alter_parser;
		alter_parser.ParseQuery("SELECT 1");
		auto parse_data = make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(
		    std::move(alter_parser.statements[0]), true);
		dynamic_cast<MaterializedViewParseData &>(*parse_data).alter_sql = update_sql;
		return ParserExtensionParseResult(std::move(parse_data));
	}

	if (!StringUtil::Contains(query_lower, "create materialized view") &&
	    !StringUtil::Contains(query_lower, "create or replace materialized view")) {
		return ParserExtensionParseResult();
	}

	OPENIVM_DEBUG_PRINT("[CREATE MV] Intercepted query: %s\n", query_lower.c_str());

	// Detect CREATE OR REPLACE MATERIALIZED VIEW
	bool is_replace = false;
	std::regex or_replace_re("\\bcreate\\s+or\\s+replace\\s+materialized\\s+view\\b", std::regex::icase);
	if (std::regex_search(query_lower, or_replace_re)) {
		is_replace = true;
		// Strip "or replace" so the rest of the pipeline sees "create materialized view"
		query_lower = std::regex_replace(query_lower, std::regex("\\bor\\s+replace\\s+"), "");
		SqlUtils::RemoveRedundantWhitespaces(query_lower);
	}

	// Extract REFRESH EVERY clause before structural rewrite (strips it from the query)
	int64_t refresh_interval = SqlUtils::ExtractRefreshInterval(query_lower);
	OPENIVM_DEBUG_PRINT("[CREATE MV] Refresh interval: %lld seconds\n", (long long)refresh_interval);

	SqlUtils::ReplaceMaterializedView(query_lower);
	// All other rewrites (DISTINCT, AVG, LEFT JOIN key, aggregate aliases) are done
	// at the plan level in PlanFunction via PlanRewrite + LPTS.
	OPENIVM_DEBUG_PRINT("[CREATE MV] After structural rewrite: %s\n", query_lower.c_str());

	Parser p;
	p.ParseQuery(query_lower);

	auto parse_data = make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(std::move(p.statements[0]),
	                                                                                      true, refresh_interval);
	dynamic_cast<MaterializedViewParseData &>(*parse_data).is_replace = is_replace;
	return ParserExtensionParseResult(std::move(parse_data));
}

} // namespace duckdb
