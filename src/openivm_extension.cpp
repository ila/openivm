#define DUCKDB_EXTENSION_MAIN

#include "core/openivm_extension.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_metadata.hpp"
#include "upsert/openivm_cost_model.hpp"
#include "upsert/openivm_upsert.hpp"

#include "duckdb/catalog/catalog_entry/index_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/enums/catalog_type.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/parser/query_error_context.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/parser/tableref/basetableref.hpp"
#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/planner.hpp"
#include "upsert/openivm_upsert.hpp"
#include "core/openivm_parser.hpp"
#include "rules/openivm_rewrite_rule.hpp"
#include "rules/openivm_insert_rule.hpp"
#include "core/openivm_debug.hpp"

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
	string view_query = IVMMetadata(con).GetViewQuery(view_name);
	if (view_query.empty()) {
		throw InternalException("Error while querying view definition");
	}
	OPENIVM_DEBUG_PRINT("[DoIVM Bind] View: %s, Query: %s\n", view_name.c_str(), view_query.c_str());

	Parser parser;
	parser.ParseQuery(view_query);
	auto statement = parser.statements[0].get();
	Planner planner(context);
	planner.CreatePlan(statement->Copy());
	OPENIVM_DEBUG_PRINT("[DoIVM Bind] Plan:\n%s\n", planner.plan->ToString().c_str());

	auto result = make_uniq<TableFunctionData>();
	for (size_t i = 0; i < planner.names.size(); i++) {
		return_types.emplace_back(planner.types[i]);
		names.emplace_back(planner.names[i]);
		OPENIVM_DEBUG_PRINT("[DoIVM Bind] Column %zu: %s (%s)\n", i, planner.names[i].c_str(),
		                    planner.types[i].ToString().c_str());
	}

	return_types.emplace_back(LogicalTypeId::BOOLEAN);
	names.emplace_back(ivm::MULTIPLICITY_COL);

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

	db_config.AddExtensionOption("ivm_files_path", "path for compiled SQL reference files", LogicalType::VARCHAR);
	db_config.AddExtensionOption("ivm_refresh_mode", "refresh strategy: auto, incremental, or full",
	                             LogicalType::VARCHAR, Value("auto"));
	db_config.AddExtensionOption("ivm_adaptive", "enable adaptive cost model (when false, always use IVM)",
	                             LogicalType::BOOLEAN, Value::BOOLEAN(false));
	db_config.AddExtensionOption("ivm_cascade_refresh", "cascade mode: off, upstream, downstream, or both",
	                             LogicalType::VARCHAR, Value("downstream"));

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
	auto ivm_cost = PragmaFunction::PragmaCall("ivm_cost", IVMCostQuery, {LogicalType::VARCHAR});
	loader.RegisterFunction(ivm_cost);
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
