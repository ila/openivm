#ifndef OPENIVM_PARSER_HPP
#define OPENIVM_PARSER_HPP

#include "duckdb.hpp"
#include "duckdb/main/setting_info.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "sql_dialect.hpp"

#include <atomic>
#include <utility>

namespace duckdb {

//! Name of the setting that declares which SQL dialect the caller writes materialized-view bodies in.
constexpr const char *OPENIVM_INPUT_DIALECT_SETTING = "openivm_input_dialect";

//! Parser-extension state. `parse_function` and `parser_override` run before any ClientContext
//! exists, so the input dialect is mirrored here from the `openivm_input_dialect` setting: it
//! decides whether a materialized-view body must be normalized out of its source dialect before
//! DuckDB's parser ever sees it.
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

	static ParserExtensionParseResult ParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserOverrideResult OverrideFunction(ParserExtensionInfo *info, const string &query,
	                                             ParserOptions &options);
	static ParserExtensionPlanResult PlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                              unique_ptr<ParserExtensionParseData> parse_data);
};

//! Parse a materialized-view lifecycle statement written in `input_dialect`.
ParserExtensionParseResult ParseMaterializedViewStatement(const string &query, SqlDialect input_dialect);

//! The dialect materialized-view bodies arrive in for this session (`openivm_input_dialect`).
SqlDialect OpenIvmInputDialect(ClientContext &context);

//! `openivm_input_dialect` set callback: validates the value and mirrors it onto the parser extension.
void SetOpenIvmInputDialect(ClientContext &context, SetScope scope, Value &parameter);

string MaterializedViewLifecycleQuery(ClientContext &context, const FunctionParameters &parameters);
string MaterializedViewDropQuery(ClientContext &context, const FunctionParameters &parameters);

struct MaterializedViewParseData : ParserExtensionParseData {
	unique_ptr<SQLStatement> statement;
	int64_t refresh_interval = -1; // seconds, -1 = not specified (manual only)
	bool is_replace = false;       // CREATE OR REPLACE: drop old MV before creating
	string alter_sql;              // non-empty for ALTER MATERIALIZED VIEW (executed directly in plan function)
	string target_name;            // parsed catalog-qualified CREATE/ALTER target

	unique_ptr<ParserExtensionParseData> Copy() const override {
		auto copy = make_uniq_base<ParserExtensionParseData, MaterializedViewParseData>(statement->Copy());
		auto &data = dynamic_cast<MaterializedViewParseData &>(*copy);
		data.refresh_interval = refresh_interval;
		data.is_replace = is_replace;
		data.alter_sql = alter_sql;
		data.target_name = target_name;
		return copy;
	}

	string ToString() const override {
		return statement->ToString();
	}

	explicit MaterializedViewParseData(unique_ptr<SQLStatement> statement, int64_t refresh_interval = -1)
	    : statement(std::move(statement)), refresh_interval(refresh_interval) {
	}
};

} // namespace duckdb

#endif // OPENIVM_PARSER_HPP
