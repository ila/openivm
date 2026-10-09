#ifndef OPENIVM_PARSER_DDL_HPP
#define OPENIVM_PARSER_DDL_HPP

#include "duckdb.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parser/parser_extension.hpp"

#include <unordered_set>

namespace duckdb {

struct DropInfo;
struct CompiledProgram;

static constexpr const char *OPENIVM_DDL_CLEANUP_PREFIX = "openivm_cleanup:";
static constexpr const char *OPENIVM_DDL_PROFILE_PREFIX = "openivm_profile:";
static constexpr const char *OPENIVM_DDL_PROFILE_RECORD_PREFIX = "openivm_profile_record:";
static constexpr const char *OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX = "openivm_create_delta_from_data:";
static constexpr const char *OPENIVM_DDL_SNAPSHOT_PUBLICATION_PREFIX = "openivm_snapshot_publication:";
// One statement of a staged program to archive, as compiled. Executor operations
// above are archived as the SQL they executed.
static constexpr const char *OPENIVM_DDL_ARCHIVE_STATEMENT_PREFIX = "openivm_archive_statement:";
// Archives the preceding archive statements; the payload describes the program.
static constexpr const char *OPENIVM_DDL_ARCHIVE_WRITE_PREFIX = "openivm_archive_write:";
static constexpr const char *OPENIVM_TRANSACTIONAL_DDL_FUNCTION = "openivm_transactional_ddl";
static constexpr const char *OPENIVM_STAGED_DDL_FUNCTION = "openivm_staged_ddl";

enum class DDLExecutionMode : uint8_t { CALLER_TRANSACTION, STAGED_CROSS_CATALOG };

// Where the DDL executor writes a staged program's compiled-SQL archive.
enum class DDLArchiveMode : uint8_t {
	// In the executor's open metadata transaction, which commits with the program.
	CURRENT_TRANSACTION,
	// In a dedicated metadata transaction; a failure runs the program's cleanup.
	OWN_TRANSACTION,
	// In a dedicated metadata transaction after the program's effects committed and no
	// cleanup applies; a failure is reported as committed but not archived.
	AFTER_COMMIT
};

// Native lifecycle statements are rendered into a caller-transaction SQL
// program. Helper connections cannot observe that program's uncommitted
// metadata, so retain the metadata operations on the caller context and replay
// them into temporary shadow tables when a later lifecycle/refresh statement
// in the same transaction needs to compile.
class TransactionalMVMetadataState : public ClientContextState {
public:
	static TransactionalMVMetadataState &Get(ClientContext &context);
	static optional_ptr<TransactionalMVMetadataState> TryGet(ClientContext &context);

	void Register(ClientContext &context, const vector<Value> &parameters, const string &view_name);
	void RegisterSQL(const string &sql, const string &view_name);
	void IncludeView(const string &view_name);
	void Apply(Connection &connection) const;

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

private:
	void Clear();

	vector<string> statements;
	unordered_set<string> view_names;
};

void ConfigureDDLExecutorResult(ParserExtensionPlanResult &result,
                                DDLExecutionMode mode = DDLExecutionMode::STAGED_CROSS_CATALOG);
string RenderTransactionalDDL(ClientContext &context, const vector<Value> &parameters, const string &metadata_catalog);
// The caller-transaction SQL for one compiled lifecycle statement (excluding profile
// and cleanup markers, which RenderTransactionalDDL handles itself).
vector<string> RenderTransactionalStatement(const string &statement);
void ExecuteStagedDDL(ClientContext &context, const vector<Value> &parameters);
string BuildCreateDeltaFromDataOperation(const string &delta_table, const string &data_table, bool replace);
// Executor operations that archive `program` as committed in the metadata catalog/schema.
vector<string> BuildArchiveProgramOperation(const CompiledProgram &program, DDLArchiveMode mode,
                                            const string &metadata_catalog, const string &metadata_schema);
string BuildDropViewStatement(const DropInfo &drop_info);
string BuildDropTableStatement(const DropInfo &drop_info);
unique_ptr<FunctionData> BindDropView(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<FunctionData> BindDropTable(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<GlobalTableFunctionState> InitDropView(ClientContext &context, TableFunctionInitInput &input);
void ExecuteDropView(ClientContext &context, TableFunctionInput &input, DataChunk &output);

} // namespace duckdb

#endif // OPENIVM_PARSER_DDL_HPP
