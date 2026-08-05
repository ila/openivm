#ifndef OPENIVM_PARSER_HPP
#define OPENIVM_PARSER_HPP

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"

#include <utility>

namespace duckdb {

class MaterializedViewParserExtension : public ParserExtension {
public:
	explicit MaterializedViewParserExtension() {
		parse_function = ParseFunction;
		plan_function = PlanFunction;
		parser_override = OverrideFunction;
	}

	static ParserExtensionParseResult ParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserOverrideResult OverrideFunction(ParserExtensionInfo *info, const string &query,
	                                             ParserOptions &options);
	static ParserExtensionPlanResult PlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                              unique_ptr<ParserExtensionParseData> parse_data);
};

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
