#ifndef OPENIVM_PARSER_DDL_HPP
#define OPENIVM_PARSER_DDL_HPP

#include "duckdb.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parser/parser_extension.hpp"

namespace duckdb {

struct DropInfo;

static constexpr const char *OPENIVM_DDL_CLEANUP_PREFIX = "openivm_cleanup:";
static constexpr const char *OPENIVM_DDL_PROFILE_PREFIX = "openivm_profile:";
static constexpr const char *OPENIVM_DDL_PROFILE_RECORD_PREFIX = "openivm_profile_record:";
static constexpr const char *OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX = "openivm_create_delta_from_data:";
static constexpr const char *OPENIVM_TRANSACTIONAL_DDL_FUNCTION = "openivm_transactional_ddl";
static constexpr const char *OPENIVM_STAGED_DDL_FUNCTION = "openivm_staged_ddl";

enum class DDLExecutionMode : uint8_t { CALLER_TRANSACTION, STAGED_CROSS_CATALOG };

// Native lifecycle statements are rendered into a caller-transaction SQL
// program. Helper connections cannot observe that program's uncommitted
// metadata, so retain the metadata operations on the caller context and replay
// them into temporary shadow tables when a later lifecycle/refresh statement
// in the same transaction needs to compile.
class TransactionalMVMetadataState : public ClientContextState {
public:
	static TransactionalMVMetadataState &Get(ClientContext &context);
	static optional_ptr<TransactionalMVMetadataState> TryGet(ClientContext &context);

	void Register(ClientContext &context, const vector<Value> &parameters);
	void RegisterSQL(const string &sql);
	void Apply(Connection &connection) const;

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

private:
	void Clear();

	vector<string> statements;
};

void ConfigureDDLExecutorResult(ParserExtensionPlanResult &result,
                                DDLExecutionMode mode = DDLExecutionMode::STAGED_CROSS_CATALOG);
string RenderTransactionalDDL(ClientContext &context, const vector<Value> &parameters);
void ExecuteStagedDDL(ClientContext &context, const vector<Value> &parameters);
string BuildCreateDeltaFromDataOperation(const string &delta_table, const string &data_table, bool replace);
string BuildDropViewStatement(const DropInfo &drop_info);
unique_ptr<FunctionData> BindDropView(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<GlobalTableFunctionState> InitDropView(ClientContext &context, TableFunctionInitInput &input);
void ExecuteDropView(ClientContext &context, TableFunctionInput &input, DataChunk &output);

} // namespace duckdb

#endif // OPENIVM_PARSER_DDL_HPP
