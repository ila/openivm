#ifndef OPENIVM_PARSER_DDL_HPP
#define OPENIVM_PARSER_DDL_HPP

#include "duckdb.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/parser/parser_extension.hpp"

#include <unordered_set>

namespace duckdb {

struct DropInfo;
class Binder;
class LogicalOperator;
class ScalarFunction;

static constexpr const char *OPENIVM_DDL_CLEANUP_PREFIX = "openivm_cleanup:";
static constexpr const char *OPENIVM_DDL_PROFILE_PREFIX = "openivm_profile:";
static constexpr const char *OPENIVM_DDL_PROFILE_RECORD_PREFIX = "openivm_profile_record:";
static constexpr const char *OPENIVM_DDL_PROFILE_TABLE_PREFIX = "openivm_profile_table:";
static constexpr const char *OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX = "openivm_create_delta_from_data:";
static constexpr const char *OPENIVM_DDL_SNAPSHOT_PUBLICATION_PREFIX = "openivm_snapshot_publication:";
static constexpr const char *OPENIVM_TRANSACTIONAL_DDL_FUNCTION = "openivm_transactional_ddl";
static constexpr const char *OPENIVM_STAGED_DDL_FUNCTION = "openivm_staged_ddl";
static constexpr const char *OPENIVM_RECORD_SOURCE_WATERMARK_FUNCTION = "openivm_record_source_watermark";

// Retained by the extension statement's table-function plan until post-bind.
struct TransactionalDDLInfo : TableFunctionInfo {
	string view_key;
	string metadata_catalog;
	string metadata_schema;
};

enum class DDLExecutionMode : uint8_t { CALLER_TRANSACTION, STAGED_CROSS_CATALOG };

// Native lifecycle statements are rendered into a caller-transaction SQL
// program. Helper connections cannot observe that program's uncommitted
// metadata, so retain the metadata operations on the caller context and replay
// them into temporary shadow tables when a later lifecycle/refresh statement
// in the same transaction needs to compile.
class TransactionalMVMetadataState : public ClientContextState {
public:
	explicit TransactionalMVMetadataState(ClientContext &context);
	static TransactionalMVMetadataState &Get(ClientContext &context);
	static optional_ptr<TransactionalMVMetadataState> TryGet(ClientContext &context);

	void Register(ClientContext &context, const vector<Value> &parameters, const string &view_name,
	              const string &metadata_catalog, const string &metadata_schema);
	void RegisterSQL(const string &sql, const string &view_name);
	void IncludeView(const string &view_name);
	void RegisterViewDrop(const string &view_name);
	bool HasPendingViewDrop(const string &view_name) const;
	void Apply(Connection &connection) const;
	// Include locations created in this transaction, which durable discovery cannot see.
	vector<pair<string, string>> MetadataLocations() const;

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

private:
	void Clear();

	ClientContext &owner;
	vector<string> statements;
	unordered_set<string> view_names;
	unordered_set<string> dropped_views;
};

void ConfigureDDLExecutorResult(ParserExtensionPlanResult &result,
                                DDLExecutionMode mode = DDLExecutionMode::STAGED_CROSS_CATALOG);
string RenderTransactionalDDL(ClientContext &context, const vector<Value> &parameters, const string &metadata_catalog,
                              const string &metadata_schema);
void ExecuteStagedDDL(ClientContext &context, const vector<Value> &parameters);
string BuildCreateDeltaFromDataOperation(const string &delta_table, const string &data_table, bool replace);
string BuildSourceWatermarkUpdate(const string &metadata_table, const string &view_name, const string &source_name,
                                  const string &watermark, const string &refresh_timestamp, bool record);
ScalarFunction SourceWatermarkFunction();
string BuildDropViewStatement(const DropInfo &drop_info);
string BuildDropTableStatement(const DropInfo &drop_info);
string BuildDropSchemaStatement(const DropInfo &drop_info);
unique_ptr<LogicalOperator> BindLifecycleProgram(ClientContext &context, Binder &binder, const string &program);
TableFunction DropEntryFunction(CatalogType type);

} // namespace duckdb

#endif // OPENIVM_PARSER_DDL_HPP
