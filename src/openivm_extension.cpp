#define DUCKDB_EXTENSION_MAIN

#include "openivm_extension.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "openivm_upsert.hpp"
#include "openivm_parser.hpp"
#include "openivm_rewrite_rule.hpp"
#include "openivm_insert_rule.hpp"
#include "openivm_debug.hpp"

#include <map>

namespace duckdb {

struct DoIVMData : public GlobalTableFunctionState {
	DoIVMData() : offset(0) {
	}
	idx_t offset;
	string view_name;
};

unique_ptr<GlobalTableFunctionState> DoIVMInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<DoIVMData>();
	return std::move(result);
}

static unique_ptr<TableRef> DoIVM(ClientContext &context, TableFunctionBindInput &input) {
	return nullptr;
}

static duckdb::unique_ptr<FunctionData> DoIVMBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	string view_catalog_name = StringValue::Get(input.inputs[0]);
	string view_schema_name = StringValue::Get(input.inputs[1]);
	string view_name = StringValue::Get(input.inputs[2]);
	OPENIVM_DEBUG_PRINT("View to be incrementally maintained: %s \n", view_name.c_str());

	input.named_parameters["view_name"] = view_name;
	input.named_parameters["view_catalog_name"] = view_catalog_name;
	input.named_parameters["view_schema_name"] = view_schema_name;

	Connection con(*context.db);
	auto v = con.Query("select sql_string from _duckdb_ivm_views where view_name = '" + view_name + "';");
	if (v->HasError()) {
		throw InternalException("Error while querying view definition");
	}
	string view_query = v->GetValue(0, 0).ToString();

	Parser parser;
	parser.ParseQuery(view_query);
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());

	auto result = make_uniq<DoIVMFunctionData>();
	for (size_t i = 0; i < planner.names.size(); i++) {
		return_types.emplace_back(planner.types[i]);
		names.emplace_back(planner.names[i]);
	}

	return_types.emplace_back(LogicalTypeId::BOOLEAN);
	names.emplace_back("_duckdb_ivm_multiplicity");

	return std::move(result);
}

static void DoIVMFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = dynamic_cast<DoIVMData &>(*data_p.global_state);
	if (data.offset >= 1) {
		return;
	}
	return;
}

static void LoadInternal(ExtensionLoader &loader) {
	auto &instance = loader.GetDatabaseInstance();
	auto &db_config = duckdb::DBConfig::GetConfig(instance);

	db_config.AddExtensionOption("ivm_files_path", "path for compiled files", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_system", "database for cross-system openivm", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_catalog_name", "catalog name", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_schema_name", "schema name", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_done", "whether the query has been parsed", LogicalType::BOOLEAN);

	Connection con(instance);
	auto ivm_parser = duckdb::IVMParserExtension();

	auto ivm_rewrite_rule = duckdb::IVMRewriteRule();
	auto ivm_insert_rule = duckdb::IVMInsertRule();

	ParserExtension::Register(db_config, std::move(ivm_parser));
	OptimizerExtension::Register(db_config, std::move(ivm_rewrite_rule));
	OptimizerExtension::Register(db_config, std::move(ivm_insert_rule));

	TableFunction ivm_func("DoIVM", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR}, DoIVMFunction,
	                       DoIVMBind, DoIVMInit);

	con.BeginTransaction();
	auto &catalog = Catalog::GetSystemCatalog(*con.context);
	ivm_func.bind_replace = reinterpret_cast<table_function_bind_replace_t>(DoIVM);
	ivm_func.name = "DoIVM";
	ivm_func.named_parameters["view_catalog_name"];
	ivm_func.named_parameters["view_schema_name"];
	ivm_func.named_parameters["view_name"];
	CreateTableFunctionInfo ivm_func_info(ivm_func);
	catalog.CreateTableFunction(*con.context, &ivm_func_info);
	con.Commit();

	auto ivm_options = PragmaFunction::PragmaCall("ivm_options", UpsertDeltaQueries,
	                                              {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_options);
	auto ivm = PragmaFunction::PragmaCall("ivm", UpsertDeltaQueries, {LogicalType::VARCHAR});
	loader.RegisterFunction(ivm);
	auto ivm_cross_system = PragmaFunction::PragmaCall(
	    "ivm_cross_system", UpsertDeltaQueries,
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_cross_system);
}

void OpenivmExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string OpenivmExtension::Name() {
	return "openivm";
}

std::string OpenivmExtension::Version() const {
#ifdef EXT_VERSION_OPENIVM
	return EXT_VERSION_OPENIVM;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(openivm, loader) {
	duckdb::LoadInternal(loader);
}
}
