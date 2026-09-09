#include "core/parser.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "core/time_travel_pins.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/qualified_name.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/statement/pragma_statement.hpp"
#include "lpts_parser.hpp"

#include <regex>

namespace duckdb {

static unique_ptr<SQLStatement> BuildInternalPragma(const string &name, const string &query) {
	auto statement = make_uniq<PragmaStatement>();
	statement->info->name = name;
	statement->info->parameters.push_back(make_uniq<ConstantExpression>(Value(query)));
	return std::move(statement);
}

SqlDialect OpenIvmInputDialect(ClientContext &context) {
	Value setting_value;
	if (!context.TryGetCurrentSetting(OPENIVM_INPUT_DIALECT_SETTING, setting_value) || setting_value.IsNull()) {
		return SqlDialect::DUCKDB;
	}
	return ParseSqlDialectSetting(setting_value.ToString(), OPENIVM_INPUT_DIALECT_SETTING);
}

void SetOpenIvmInputDialect(ClientContext &context, SetScope scope, Value &parameter) {
	auto dialect = parameter.IsNull() ? SqlDialect::DUCKDB
	                                  : ParseSqlDialectSetting(parameter.ToString(), OPENIVM_INPUT_DIALECT_SETTING);
	// `parser_override` and `parse_function` are handed the extension info, not a ClientContext, so
	// mirror the resolved dialect there. The info is owned by the DBConfig, so the mirror stays
	// scoped to this database.
	for (auto &extension : ExtensionCallbackManager::Get(context).ParserExtensions()) {
		auto info = dynamic_cast<MaterializedViewParserExtensionInfo *>(extension.parser_info.get());
		if (info) {
			info->SetInputDialect(dialect);
		}
	}
}

static SqlDialect InputDialectFromInfo(ParserExtensionInfo *info) {
	auto materialized_view_info = dynamic_cast<MaterializedViewParserExtensionInfo *>(info);
	return materialized_view_info ? materialized_view_info->InputDialect() : SqlDialect::DUCKDB;
}

ParserOverrideResult MaterializedViewParserExtension::OverrideFunction(ParserExtensionInfo *info, const string &query,
                                                                       ParserOptions &options) {
	try {
		auto extension_result = ParseMaterializedViewStatement(query, InputDialectFromInfo(info));
		if (extension_result.type == ParserExtensionResultType::PARSE_SUCCESSFUL) {
			vector<unique_ptr<SQLStatement>> statements;
			statements.push_back(BuildInternalPragma("openivm_materialized_view_lifecycle", query));
			return ParserOverrideResult(std::move(statements));
		}

		// DuckDB parses these statements natively, so the regular parser-extension
		// fallback never sees them. Route tracked-view drops and cascading source
		// drops through OpenIVM so their cleanup uses the caller transaction.
		ParserOptions native_options = options;
		native_options.extensions = nullptr;
		Parser parser(native_options);
		parser.ParseQuery(query);
		if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::DROP_STATEMENT) {
			return ParserOverrideResult();
		}
		auto &drop = parser.statements[0]->Cast<DropStatement>();
		if (drop.info->type != CatalogType::VIEW_ENTRY &&
		    (drop.info->type != CatalogType::TABLE_ENTRY || !drop.info->cascade)) {
			return ParserOverrideResult();
		}
		vector<unique_ptr<SQLStatement>> statements;
		statements.push_back(BuildInternalPragma("openivm_materialized_view_drop", query));
		return ParserOverrideResult(std::move(statements));
	} catch (std::exception &ex) {
		return ParserOverrideResult(ex);
	}
}

ParserExtensionParseResult MaterializedViewParserExtension::ParseFunction(ParserExtensionInfo *info,
                                                                          const string &query) {
	return ParseMaterializedViewStatement(query, InputDialectFromInfo(info));
}

ParserExtensionParseResult ParseMaterializedViewStatement(const string &query, SqlDialect input_dialect) {
	auto query_lower = SqlUtils::SQLToLowercase(StringUtil::Replace(query, ";", ""));
	StringUtil::Trim(query_lower);
	// Strip SQL line comments (-- to end of line) before whitespace normalization.
	// RemoveRedundantWhitespaces collapses '\n' to ' ', which would turn
	// "-- comment\n rest" into "-- comment rest" where the rest is eaten by the comment.
	SqlUtils::StripLineComments(query_lower);
	SqlUtils::RemoveRedundantWhitespaces(query_lower);

	// Handle ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' | SET REFRESH MANUAL
	if (StringUtil::Contains(query_lower, "alter materialized view")) {
		const string identifier = "(?:\"(?:[^\"]|\"\")*\"|[a-zA-Z_][a-zA-Z0-9_$]*)";
		const string qualified_identifier = identifier + "(?:\\s*\\.\\s*" + identifier + "){0,2}";
		std::regex alter_re("^alter\\s+materialized\\s+view\\s+(" + qualified_identifier +
		                        ")\\s+set\\s+refresh\\s+(every\\s+'([^']+)'|manual)$",
		                    std::regex::icase);
		std::smatch match;
		if (!std::regex_match(query_lower, match, alter_re)) {
			throw ParserException("Invalid ALTER MATERIALIZED VIEW syntax. "
			                      "Expected: ALTER MATERIALIZED VIEW <name> SET REFRESH EVERY '<interval>' "
			                      "or ALTER MATERIALIZED VIEW <name> SET REFRESH MANUAL");
		}
		string alter_view_name = match[1].str();
		auto name_components = QualifiedName::ParseComponents(alter_view_name);
		if (name_components.empty()) {
			throw ParserException("Invalid materialized-view target '%s'", alter_view_name);
		}
		string refresh_type = StringUtil::Lower(match[2].str());
		string alter_value;
		if (refresh_type == "manual") {
			alter_value = "NULL";
		} else {
			int64_t interval = SqlUtils::ParseRefreshInterval(match[3].str());
			alter_value = to_string(interval);
		}
		// Pass the UPDATE SQL through MaterializedViewParseData; PlanFunction will execute it
		Parser alter_parser;
		alter_parser.ParseQuery("SELECT 1");
		auto parse_data =
		    make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(std::move(alter_parser.statements[0]));
		auto &materialized_view_data = dynamic_cast<MaterializedViewParseData &>(*parse_data);
		materialized_view_data.alter_sql = alter_value;
		materialized_view_data.target_name = alter_view_name;
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

	vector<openivm::SnapshotBinding> pin_bindings;
	if (input_dialect != SqlDialect::DUCKDB) {
		// The body is written in another dialect, so DuckDB's parser cannot read it as-is — a
		// Spark/Delta temporal clause (`FROM t VERSION AS OF 366`) dies on `AS`. LPTS rewrites the
		// source spelling into the semantically equivalent DuckDB one, keeping the pin
		// (`AT (VERSION => 366)`) rather than dropping it, which would silently promote the scan to
		// "read latest". Record what each pin is written against first, so the rewrite can be held
		// to it below.
		pin_bindings = openivm::CollectSourceSnapshotBindings(query_lower, input_dialect);
		query_lower = NormalizeInputSqlToDuckDB(query_lower, input_dialect);
		OPENIVM_DEBUG_PRINT("[CREATE MV] After %s input normalization: %s\n", SqlDialectToString(input_dialect).c_str(),
		                    query_lower.c_str());
	}

	Parser p;
	p.ParseQuery(query_lower);
	// Rewriting the temporal clause moves it across the relation's alias, so re-check against the
	// parse tree that every pin still names the relation and alias it was written against.
	openivm::VerifySnapshotBindings(*p.statements[0], pin_bindings);

	auto parse_data = make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(std::move(p.statements[0]),
	                                                                                      refresh_interval);
	auto &materialized_view_data = dynamic_cast<MaterializedViewParseData &>(*parse_data);
	materialized_view_data.is_replace = is_replace;
	materialized_view_data.target_name = SqlUtils::ExtractTableName(query_lower);
	return ParserExtensionParseResult(std::move(parse_data));
}

} // namespace duckdb
