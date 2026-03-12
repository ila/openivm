#ifndef OPENIVM_PARSER_HPP
#define OPENIVM_PARSER_HPP

#include "duckdb.hpp"
#include "duckdb/parser/parser_extension.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "upsert/openivm_upsert.hpp"

#include <utility>

namespace duckdb {

struct DoIVMFunctionData : TableFunctionData {
	DoIVMFunctionData() {
	}
};

struct DoIVMBenchmarkFunctionData : TableFunctionData {
	DoIVMBenchmarkFunctionData() {
	}
};

struct IVMInfo : ParserExtensionInfo {
	unique_ptr<Connection> db_conn;
	bool performed = false;
	explicit IVMInfo(unique_ptr<Connection> db_conn) : db_conn(std::move(db_conn)) {
	}
};

class IVMParserExtension : public ParserExtension {
public:
	explicit IVMParserExtension() {
		parse_function = IVMParseFunction;
		plan_function = IVMPlanFunction;
	}

	static ParserExtensionParseResult IVMParseFunction(ParserExtensionInfo *info, const string &query);
	static ParserExtensionPlanResult IVMPlanFunction(ParserExtensionInfo *info, ClientContext &context,
	                                                 unique_ptr<ParserExtensionParseData> parse_data);
};

BoundStatement IVMBind(ClientContext &context, Binder &binder, OperatorExtensionInfo *info, SQLStatement &statement);

struct IVMOperatorExtension : public OperatorExtension {
	IVMOperatorExtension() {
		Bind = IVMBind;
	}

	std::string GetName() override {
		return "IVM";
	}

	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &) override {
		throw NotImplementedException("IVMOperatorExtension::Deserialize not implemented");
	}
};

struct IVMParseData : ParserExtensionParseData {
	IVMParseData() {
	}

	unique_ptr<SQLStatement> statement;
	bool plan = false;

	unique_ptr<ParserExtensionParseData> Copy() const override {
		return make_uniq_base<ParserExtensionParseData, IVMParseData>(statement->Copy(), false);
	}

	string ToString() const override {
		return statement->ToString();
	}

	explicit IVMParseData(unique_ptr<SQLStatement> statement, bool plan) : statement(std::move(statement)), plan(plan) {
	}
};

class IVMState : public ClientContextState {
public:
	explicit IVMState(unique_ptr<ParserExtensionParseData> parse_data) : parse_data(std::move(parse_data)) {
	}

	void QueryEnd() override {
		parse_data.reset();
	}

	unique_ptr<ParserExtensionParseData> parse_data;
};

class IVMFunction : public TableFunction {
public:
	IVMFunction() {
		name = "IVM function";
		arguments.push_back(LogicalType::BOOLEAN);
		bind = IVMBind;
		init_global = IVMInit;
		function = IVMFunc;
	}

	struct IVMBindData : public TableFunctionData {
		explicit IVMBindData(bool result) : result(result) {
		}
		bool result;
	};

	struct IVMGlobalData : public GlobalTableFunctionState {
		IVMGlobalData() : offset(0) {
		}
		idx_t offset;
	};

	static unique_ptr<FunctionData> IVMBind(ClientContext &context, TableFunctionBindInput &input,
	                                        vector<LogicalType> &return_types, vector<string> &names) {
		names.emplace_back("MATERIALIZED VIEW CREATION");
		return_types.emplace_back(LogicalType::BOOLEAN);
		bool result = false;
		if (IntegerValue::Get(input.inputs[0]) == 1) {
			result = true;
		}
		return make_uniq<IVMBindData>(result);
	}

	static unique_ptr<GlobalTableFunctionState> IVMInit(ClientContext &context, TableFunctionInitInput &input) {
		return make_uniq<IVMGlobalData>();
	}

	static void IVMFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
		auto &bind_data = data_p.bind_data->Cast<IVMBindData>();
		auto &data = dynamic_cast<IVMGlobalData &>(*data_p.global_state);
		if (data.offset >= 1) {
			return;
		}
		auto result = Value::BOOLEAN(bind_data.result);
		data.offset++;
		output.SetValue(0, 0, result);
		output.SetCardinality(1);
	}
};

} // namespace duckdb

#endif // OPENIVM_PARSER_HPP
