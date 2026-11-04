#include "core/parser.hpp"

#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/extension_callback_manager.hpp"
#include "duckdb/parser/grammar_extension.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/peg/compiled_grammar.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/extension_statement.hpp"
#include "lpts_parser.hpp"

namespace duckdb {

namespace {

static arena_ptr<TransformResultValue> TransformRefreshEvery(PEGTransformer &transformer, ParseResult &result) {
	auto &list = result.Cast<ListParseResult>();
	auto interval = transformer.Transform<string>(list.GetChild(1));
	return transformer.MakeResult<int64_t>(SqlUtils::ParseRefreshInterval(interval));
}

static arena_ptr<TransformResultValue> TransformRefreshClause(PEGTransformer &transformer, ParseResult &result) {
	return transformer.MakeResult<int64_t>(transformer.Transform<int64_t>(result.Cast<ListParseResult>().GetChild(1)));
}

static arena_ptr<TransformResultValue> TransformRefreshMode(PEGTransformer &transformer, ParseResult &result) {
	auto &choice = result.Cast<ListParseResult>().Child<ChoiceParseResult>(0).GetResult();
	return transformer.MakeResult<int64_t>(transformer.Transform<int64_t>(choice));
}

static arena_ptr<TransformResultValue> TransformRefreshManual(PEGTransformer &transformer, ParseResult &) {
	return transformer.MakeResult<int64_t>(-1);
}

static arena_ptr<TransformResultValue> TransformMaterializedView(PEGTransformer &transformer, ParseResult &result) {
	auto &list = result.Cast<ListParseResult>();
	bool create = result.Name() == "OpenIVMCreate";
	auto target = transformer.Transform<QualifiedName>(list.GetChild(create ? 4 : 3));
	int64_t refresh_interval = -1;
	unique_ptr<SQLStatement> statement;
	bool is_replace = false;
	string query;
	if (create) {
		transformer.TransformOptional(list, 1, is_replace);
		transformer.TransformOptional(list, 5, refresh_interval);
		auto select = transformer.Transform<unique_ptr<SelectStatement>>(list.GetChild(7));
		// A top-level side-effecting CTE executes when the initial data table is
		// created, but DuckDB cannot bind it inside OpenIVM's refresh query. DuckDB
		// rejects nested side-effecting CTEs itself. Reject this one before execution.
		for (const auto &cte : select->node->cte_map.map) {
			auto type = cte.second->query_node->type;
			if (type == QueryNodeType::INSERT_QUERY_NODE || type == QueryNodeType::UPDATE_QUERY_NODE ||
			    type == QueryNodeType::DELETE_QUERY_NODE || type == QueryNodeType::MERGE_QUERY_NODE ||
			    type == QueryNodeType::COPY_QUERY_NODE) {
				throw ParserException("Materialized-view definitions cannot contain data-modifying or COPY CTEs");
			}
		}
		query = select->ToString();
		auto info = make_uniq<CreateTableInfo>(target);
		info->on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
		info->query = std::move(select);
		auto create_statement = make_uniq<CreateStatement>();
		create_statement->info = std::move(info);
		statement = std::move(create_statement);
	} else {
		refresh_interval = transformer.Transform<int64_t>(list.GetChild(6));
		auto select = make_uniq<SelectStatement>();
		auto node = make_uniq<SelectNode>();
		node->select_list.push_back(ConstantExpression::Integer(1));
		select->node = std::move(node);
		statement = std::move(select);
	}
	auto data = make_uniq<MaterializedViewParseData>(std::move(statement), refresh_interval);
	data->is_replace = is_replace;
	data->target_name = target.ToString();
	data->view_query = std::move(query);
	if (create) {
		data->original_query =
		    string(is_replace ? "CREATE OR REPLACE" : "CREATE") + " MATERIALIZED VIEW " + data->target_name;
		if (refresh_interval >= 0) {
			data->original_query += " REFRESH EVERY '" + to_string(refresh_interval / 60) + " minutes'";
		}
		data->original_query += " AS " + data->view_query;
	} else {
		data->alter_sql = refresh_interval < 0 ? "NULL" : to_string(refresh_interval);
		data->original_query =
		    "ALTER MATERIALIZED VIEW " + data->target_name + " SET REFRESH " +
		    (refresh_interval < 0 ? "MANUAL" : "EVERY '" + to_string(refresh_interval / 60) + " minutes'");
	}
	OPENIVM_DEBUG_PRINT("[MV PARSER] Parsed %s for %s\n", create ? "CREATE" : "ALTER", data->target_name.c_str());
	unique_ptr<SQLStatement> extension =
	    make_uniq<ExtensionStatement>(MaterializedViewParserExtension(), std::move(data));
	return transformer.MakeResult<unique_ptr<SQLStatement>>(std::move(extension));
}

static grammar_transform_process_function_t Finalize(const transform_finalize_function_t &transform) {
	return [transform](PEGTransformer &transformer, ParseResult &result) {
		return transformer.MakeProcess<FinalizeTransformProcess>(transformer, result, transform);
	};
}

class MaterializedViewGrammar : public GrammarExtension {
public:
	MaterializedViewGrammar() : GrammarExtension("openivm", "Materialized-view creation and refresh scheduling") {
	}

	vector<GrammarChange> GetChanges() const override {
		return {GrammarChange::AddRule("OpenIVMCreate <- 'CREATE' OrReplace? 'MATERIALIZED' 'VIEW' QualifiedName "
		                               "OpenIVMCreateRefresh? 'AS' SelectStatementInternal",
		                               Finalize(TransformMaterializedView)),
		        GrammarChange::AddRule("OpenIVMCreateRefresh <- 'REFRESH' OpenIVMRefreshEvery",
		                               Finalize(TransformRefreshClause)),
		        GrammarChange::AddRule("OpenIVMRefreshEvery <- 'EVERY' StringLiteral", Finalize(TransformRefreshEvery)),
		        GrammarChange::AddRule("OpenIVMRefreshManual <- 'MANUAL'", Finalize(TransformRefreshManual)),
		        GrammarChange::AddRule("OpenIVMRefreshMode <- OpenIVMRefreshEvery / OpenIVMRefreshManual",
		                               Finalize(TransformRefreshMode)),
		        GrammarChange::AddRule("OpenIVMAlter <- 'ALTER' 'MATERIALIZED' 'VIEW' QualifiedName 'SET' 'REFRESH' "
		                               "OpenIVMRefreshMode",
		                               Finalize(TransformMaterializedView)),
		        GrammarChange::PrependChoice("Statement", "OpenIVMCreate"),
		        GrammarChange::PrependChoice("Statement", "OpenIVMAlter")};
	}
};

static shared_ptr<CompiledGrammar> StandaloneMaterializedViewGrammar() {
	static auto grammar = [] {
		MaterializedViewGrammar extension;
		return CompiledGrammar::Create({extension});
	}();
	return grammar;
}

} // namespace

void RegisterMaterializedViewGrammar(DatabaseInstance &db) {
	GrammarExtension::Register(db, make_shared_ptr<MaterializedViewGrammar>());
}

void SetOpenIvmInputDialect(ClientContext &context, SetScope scope, Value &parameter) {
	auto dialect = parameter.IsNull() ? SqlDialect::DUCKDB
	                                  : ParseSqlDialectSetting(parameter.ToString(), OPENIVM_INPUT_DIALECT_SETTING);
	for (auto &extension : ExtensionCallbackManager::Get(context).ParserExtensions()) {
		auto info = dynamic_cast<MaterializedViewParserExtensionInfo *>(extension.parser_info.get());
		if (info) {
			info->SetInputDialect(dialect);
		}
	}
}

ParserOverrideResult MaterializedViewParserExtension::OverrideFunction(ParserExtensionInfo *, const string &,
                                                                       ParserOptions &options) {
	if (options.context) {
		auto active = ClientConfig::GetConfig(*options.context).active_grammar_extensions;
		if (std::find(active.begin(), active.end(), "openivm") == active.end()) {
			active.push_back("openivm");
		}
		options.compiled_grammar = CompiledGrammar::Create(*options.context, active);
	} else {
		options.compiled_grammar = StandaloneMaterializedViewGrammar();
	}
	// Let DuckDB parse and execute statements in order, using its tokenizer and grammar.
	return ParserOverrideResult();
}

ParserExtensionParseResult MaterializedViewParserExtension::ParseFunction(ParserExtensionInfo *info,
                                                                          const vector<SimpleToken> &tokens) {
	// The native peeler uses this fallback when LOAD and CREATE share an input, or when a
	// non-DuckDB input dialect needs normalization before the MV grammar can accept its body.
	if (tokens.size() < 3 ||
	    (!StringUtil::CIEquals(tokens[0].text, "CREATE") && !StringUtil::CIEquals(tokens[0].text, "ALTER"))) {
		return ParserExtensionParseResult();
	}
	string query;
	idx_t consumed = 0;
	for (auto &token : tokens) {
		if (token.type == TokenType::END_OF_INPUT) {
			break;
		}
		query += token.text + " ";
		consumed++;
		if (token.type == TokenType::TERMINATOR) {
			break;
		}
	}
	auto dialect = info->Cast<MaterializedViewParserExtensionInfo>().InputDialect();
	auto options = ParserOptions::Builtin();
	options.compiled_grammar = StandaloneMaterializedViewGrammar();
	Parser parser(options);
	parser.ParseQuery(NormalizeInputSqlToDuckDB(query, dialect));
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::EXTENSION_STATEMENT) {
		return ParserExtensionParseResult();
	}
	auto &statement = parser.statements[0]->Cast<ExtensionStatement>();
	ParserExtensionParseResult parsed(std::move(statement.parse_data));
	parsed.consumed_tokens = NumericCast<int64_t>(consumed);
	return parsed;
}

} // namespace duckdb
