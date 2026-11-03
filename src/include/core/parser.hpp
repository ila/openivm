#ifndef OPENIVM_PARSER_HPP
#define OPENIVM_PARSER_HPP

#include "duckdb.hpp"
#include "duckdb/main/setting_info.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "sql_dialect.hpp"

#include <atomic>
#include <utility>

namespace duckdb {

struct PlannerExtensionInput;
struct BoundStatement;

//! Name of the setting that declares which SQL dialect the caller writes materialized-view bodies in.
constexpr const char *OPENIVM_INPUT_DIALECT_SETTING = "openivm_input_dialect";

//! The token fallback has no ClientContext, so mirror the input dialect for normalization
//! before the materialized-view grammar parses the body.
struct MaterializedViewParserExtensionInfo : ParserExtensionInfo {
	std::atomic<uint8_t> input_dialect {static_cast<uint8_t>(SqlDialect::DUCKDB)};

	SqlDialect InputDialect() const {
		return static_cast<SqlDialect>(input_dialect.load());
	}
	void SetInputDialect(SqlDialect dialect) {
		input_dialect.store(static_cast<uint8_t>(dialect));
	}
};

class MaterializedViewParserExtension : public ParserExtension {
public:
	explicit MaterializedViewParserExtension() {
		parse_function = ParseFunction;
		plan_function = PlanFunction;
		parser_override = OverrideFunction;
		parser_info = make_shared_ptr<MaterializedViewParserExtensionInfo>();
	}

	static ParserExtensionParseResult ParseFunction(ParserExtensionInfo *info, const vector<SimpleToken> &tokens);
	static ParserOverrideResult OverrideFunction(ParserExtensionInfo *info, const string &query,
	                                             ParserOptions &options);
	static ParserExtensionPlanResult PlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                              unique_ptr<ParserExtensionParseData> parse_data);
};

void RegisterMaterializedViewGrammar(DatabaseInstance &db);

//! `openivm_input_dialect` set callback: validates the value and mirrors it onto the parser extension.
void SetOpenIvmInputDialect(ClientContext &context, SetScope scope, Value &parameter);

void BindMaterializedViewLifecycle(PlannerExtensionInput &input, BoundStatement &statement);

struct MaterializedViewParseData : ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;
	int64_t refresh_interval = -1; // seconds, -1 = not specified (manual only)
	bool is_replace = false;       // CREATE OR REPLACE: drop old MV before creating
	string alter_sql;              // non-empty for ALTER MATERIALIZED VIEW (executed directly in plan function)
	string target_name;            // parsed catalog-qualified CREATE/ALTER target
	string view_query;             // the CREATE's AS body, normalized to DuckDB SQL
	string original_query;         // lifecycle SQL for native verification/reparsing

	unique_ptr<ParserExtensionParseData> Copy() const override {
		auto copy = make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(statement->Copy());
		auto &data = dynamic_cast<MaterializedViewParseData &>(*copy);
		data.refresh_interval = refresh_interval;
		data.is_replace = is_replace;
		data.alter_sql = alter_sql;
		data.target_name = target_name;
		data.view_query = view_query;
		data.original_query = original_query;
		return copy;
	}

	string ToString() const override {
		return original_query;
	}

	explicit MaterializedViewParseData(unique_ptr<SQLStatement> statement, int64_t refresh_interval = -1)
	    : statement(std::move(statement)), refresh_interval(refresh_interval) {
	}
};

} // namespace duckdb

#endif // OPENIVM_PARSER_HPP
