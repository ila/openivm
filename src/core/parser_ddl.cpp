#include "duckdb/transaction/meta_transaction.hpp"
#include "core/parser_ddl.hpp"
#include "core/mv_profiler.hpp"

#include "core/compiled_sql_archive.hpp"
#include "core/metadata_location.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/published_view.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/scoped_optimizer_settings.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_config.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_drop.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/parser/tableref/bound_ref_wrapper.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/transaction/local_storage.hpp"

#include <chrono>
#include <cctype>
#include <cstring>
#include <unordered_set>

namespace duckdb {

namespace {

static const vector<string> &TransactionalMetadataTables() {
	static const vector<string> tables = {openivm::VIEWS_TABLE,   "openivm_refresh_hooks", openivm::DELTA_TABLES_TABLE,
	                                      openivm::HISTORY_TABLE, openivm::PROFILE_TABLE,  openivm::MV_DEPS_TABLE};
	return tables;
}

static const vector<string> &MetadataStatementPrefixes() {
	static const vector<string> prefixes = {
	    "create table if not exists ",
	    "alter table ",
	    "insert or replace into ",
	    "insert or ignore into ",
	    "insert into ",
	    "update ",
	    "delete from ",
	};
	return prefixes;
}

// Inspect only the generated mutation's target, never names in its query or literals.
struct MetadataMutationTarget {
	idx_t start = 0;
	idx_t end = 0;
	vector<string> parts;

	bool Matches(const string &table) const {
		return !parts.empty() && StringUtil::CIEquals(parts.back(), table);
	}
	bool Matches(const vector<string> &tables) const {
		return std::any_of(tables.begin(), tables.end(), [&](const string &table) { return Matches(table); });
	}
};

static MetadataMutationTarget ReadMetadataMutationTarget(const string &statement) {
	MetadataMutationTarget target;
	idx_t offset = 0;
	while (offset < statement.size() && std::isspace(static_cast<unsigned char>(statement[offset]))) {
		offset++;
	}
	for (auto &prefix : MetadataStatementPrefixes()) {
		if (offset + prefix.size() <= statement.size() &&
		    StringUtil::CIEquals(statement.data() + offset, prefix.size(), prefix.data(), prefix.size())) {
			target.start = offset + prefix.size();
			target.parts = SqlUtils::ReadQualifiedIdentifier(statement, target.start, target.end);
			break;
		}
	}
	return target;
}

static bool IsReplayableMetadataStatement(const string &statement) {
	return ReadMetadataMutationTarget(statement).Matches(TransactionalMetadataTables());
}

static string MakeTemporaryMetadataDDL(const string &statement) {
	auto lower = StringUtil::Lower(statement);
	const string prefix = "create table if not exists ";
	if (!StringUtil::StartsWith(lower, prefix)) {
		return statement;
	}
	return "create temp table if not exists " + statement.substr(prefix.size());
}

// Search-path changes are never rolled back. A program run in the caller's session
// must not depend on them, or a failure would leave the caller in another schema.
static bool IsSearchPathStatement(const string &statement) {
	auto trimmed = statement;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	return StringUtil::StartsWith(lower, "use ") || StringUtil::StartsWith(lower, "set search_path");
}

// Only generated metadata mutations need qualification. Materialization and
// publication statements contain user SQL, whose literals and CTE names must
// never be interpreted as references to our control tables. Generated metadata
// reads (the duplicate-name guards) are qualified at construction instead.
static string QualifyMetadataMutation(const string &statement, const string &catalog, const string &schema) {
	auto target = ReadMetadataMutationTarget(statement);
	if (!target.Matches(MetadataTableNames())) {
		return statement;
	}
	auto qualified = statement;
	if (target.parts.size() == 1) {
		qualified.replace(target.start, target.end - target.start,
		                  SqlUtils::FullName(catalog, schema, target.parts[0]));
	}
	// Only unqualified reads belong to this location; other registries retain their identity.
	return SqlUtils::RewriteTableReferences(qualified, [&](const vector<string> &parts) {
		if (parts.size() == 1) {
			for (auto &table : MetadataTableNames()) {
				if (StringUtil::CIEquals(parts[0], table)) {
					return SqlUtils::FullName(catalog, schema, table);
				}
			}
		}
		return string();
	});
}

// Only this location's metadata may be redirected to helper-local shadows.
static string LocalizeMetadataStatement(const string &statement, const string &catalog, const string &schema) {
	auto target = ReadMetadataMutationTarget(statement);
	auto local = statement;
	if (target.Matches(TransactionalMetadataTables())) {
		if (target.parts.size() != 3 || !StringUtil::CIEquals(target.parts[0], catalog) ||
		    !StringUtil::CIEquals(target.parts[1], schema)) {
			return "";
		}
		local.replace(target.start, target.end - target.start, SqlUtils::QuoteIdentifier(target.parts.back()));
	}
	return SqlUtils::RewriteTableReferences(local, [&](const vector<string> &parts) {
		if (parts.size() == 3 && StringUtil::CIEquals(parts[0], catalog) && StringUtil::CIEquals(parts[1], schema)) {
			for (auto &table : TransactionalMetadataTables()) {
				if (StringUtil::CIEquals(parts[2], table)) {
					return SqlUtils::QuoteIdentifier(table);
				}
			}
		}
		return string();
	});
}

static bool IsMetadataSchemaStatement(const string &statement) {
	auto trimmed = statement;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	return StringUtil::StartsWith(lower, "create table") || StringUtil::StartsWith(lower, "alter table");
}

static bool MatchesNowCall(const string &statement, idx_t offset) {
	static const string now_call = "now()";
	if (offset + now_call.size() > statement.size()) {
		return false;
	}
	for (idx_t i = 0; i < now_call.size(); i++) {
		if (StringUtil::CharacterToLower(statement[offset + i]) != now_call[i]) {
			return false;
		}
	}
	return true;
}

static string StabilizeTransactionalMetadata(const string &statement, const string &timestamp_sql) {
	string result;
	result.reserve(statement.size() + timestamp_sql.size());
	bool in_single_quote = false;
	bool in_double_quote = false;
	for (idx_t offset = 0; offset < statement.size();) {
		char current = statement[offset];
		if (current == '\'' && !in_double_quote) {
			result.push_back(current);
			if (in_single_quote && offset + 1 < statement.size() && statement[offset + 1] == '\'') {
				result.push_back(statement[offset + 1]);
				offset += 2;
				continue;
			}
			in_single_quote = !in_single_quote;
			offset++;
			continue;
		}
		if (current == '"' && !in_single_quote) {
			result.push_back(current);
			if (in_double_quote && offset + 1 < statement.size() && statement[offset + 1] == '"') {
				result.push_back(statement[offset + 1]);
				offset += 2;
				continue;
			}
			in_double_quote = !in_double_quote;
			offset++;
			continue;
		}
		if (!in_single_quote && !in_double_quote && MatchesNowCall(statement, offset)) {
			result += timestamp_sql;
			offset += 5;
			continue;
		}
		result.push_back(current);
		offset++;
	}
	return result;
}

struct DDLExecutorBindData : public TableFunctionData {
	explicit DDLExecutorBindData(bool result) : result(result) {
	}

	bool result;
	vector<string> ddl;
};

struct DDLExecutorGlobalData : public GlobalTableFunctionState {
	DDLExecutorGlobalData() : offset(0) {
	}

	idx_t offset;
};

unique_ptr<GlobalTableFunctionState> DDLExecutorInitFunction(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DDLExecutorGlobalData>();
}

// The fields of an operation encoded by EncodeDDLOperation, after its prefix.
vector<string> DecodeDDLFields(const string &statement, const char *prefix) {
	vector<string> fields(1);
	for (idx_t i = strlen(prefix); i < statement.size(); i++) {
		if (statement[i] == '\t') {
			fields.emplace_back();
		} else if (statement[i] == '\\' && i + 1 < statement.size()) {
			i++;
			fields.back().push_back(statement[i] == 't' ? '\t' : statement[i]);
		} else {
			fields.back().push_back(statement[i]);
		}
	}
	return fields;
}

// A CREATE profile marker opens a step measured by the executor; a record carries its own duration.
struct ProfileOperation {
	bool measured = false;
	string view_name;
	string step_name = "create_mv_unclassified";
	int64_t duration_ms = 0;
	string detail;
};

bool TryParseProfileOperation(const string &statement, ProfileOperation &op) {
	const char *prefix = OPENIVM_DDL_PROFILE_PREFIX;
	if (StringUtil::StartsWith(statement, OPENIVM_DDL_PROFILE_RECORD_PREFIX)) {
		op.measured = true;
		prefix = OPENIVM_DDL_PROFILE_RECORD_PREFIX;
	} else if (!StringUtil::StartsWith(statement, prefix)) {
		return false;
	}
	auto fields = DecodeDDLFields(statement, prefix);
	if (fields.size() != (op.measured ? 4 : 3)) {
		op.detail = statement.substr(strlen(prefix));
		return true;
	}
	op.view_name = fields[0];
	op.step_name = fields[1];
	op.detail = fields.back();
	if (op.measured) {
		try {
			op.duration_ms = std::stoll(fields[2]);
		} catch (...) {
			op.duration_ms = 0;
		}
	}
	return true;
}

const char *ArchiveModeName(DDLArchiveMode mode) {
	switch (mode) {
	case DDLArchiveMode::CURRENT_TRANSACTION:
		return "current_transaction";
	case DDLArchiveMode::OWN_TRANSACTION:
		return "own_transaction";
	default:
		return "after_commit";
	}
}

struct DeltaSchemaDDL {
	string sql;
	idx_t column_count = 0;
};

struct CreateDeltaOperation {
	bool replace = false;
	string delta_table;
	string data_table;
};

// False for a malformed payload.
bool ParseCreateDeltaOperation(const string &statement, CreateDeltaOperation &op) {
	auto fields = DecodeDDLFields(statement, OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX);
	if (fields.size() != 3 || (fields[0] != "create" && fields[0] != "replace") || fields[1].empty() ||
	    fields[2].empty()) {
		return false;
	}
	op.replace = fields[0] == "replace";
	op.delta_table = fields[1];
	op.data_table = fields[2];
	return true;
}

static DeltaSchemaDDL BuildDeltaSchemaDDL(const vector<pair<string, string>> &source_columns, const string &delta_table,
                                          const string &data_table, OnCreateConflict on_conflict) {
	if (source_columns.empty()) {
		throw CatalogException("Could not derive IVM delta schema from empty column list for data table '%s'",
		                       data_table);
	}
	vector<string> columns;
	unordered_set<string> seen_column_names;
	for (auto &column : source_columns) {
		auto &column_name = column.first;
		auto &column_type = column.second;
		if (column_name.empty() || column_type.empty()) {
			throw CatalogException("Could not derive IVM delta schema from data table '%s': empty column name or type",
			                       data_table);
		}
		if (!seen_column_names.insert(StringUtil::Lower(column_name)).second) {
			throw CatalogException("Could not derive IVM delta schema from data table '%s': duplicate column '%s'",
			                       data_table, column_name);
		}
		if (StringUtil::CIEquals(column_name, openivm::MULTIPLICITY_COL) ||
		    StringUtil::CIEquals(column_name, openivm::TIMESTAMP_COL)) {
			throw CatalogException("Could not derive IVM delta schema from data table '%s': reserved OpenIVM column "
			                       "'%s' is already present",
			                       data_table, column_name);
		}
		columns.push_back(SqlUtils::QuoteIdentifier(column_name) + " " + column_type);
	}
	columns.push_back(string(openivm::MULTIPLICITY_COL) + " INTEGER DEFAULT 1");
	columns.push_back(string(openivm::TIMESTAMP_COL) + " TIMESTAMP DEFAULT now()");
	DeltaSchemaDDL result;
	result.sql = string(on_conflict == OnCreateConflict::REPLACE_ON_CONFLICT  ? "create or replace table "
	                    : on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT ? "create table if not exists "
	                                                                          : "create table ") +
	             delta_table + " (" + StringUtil::Join(columns, ", ") + ")";
	result.column_count = source_columns.size();
	return result;
}

DeltaSchemaDDL BuildCreateDeltaFromDataSQL(Connection &conn, const string &delta_table, const string &data_table,
                                           bool replace) {
	auto described = conn.Query("DESCRIBE SELECT * FROM " + data_table);
	if (described->HasError()) {
		throw CatalogException("Could not derive IVM delta schema from data table '" + data_table +
		                       "': " + described->GetError());
	}
	vector<pair<string, string>> columns;
	for (idx_t row = 0; row < described->RowCount(); row++) {
		if (described->Collection().GetValue(0, row).IsNull() || described->Collection().GetValue(1, row).IsNull()) {
			throw CatalogException(
			    "Could not derive IVM delta schema from data table '%s': DESCRIBE returned a NULL column name or type",
			    data_table);
		}
		columns.emplace_back(described->Collection().GetValue(0, row).ToString(),
		                     described->Collection().GetValue(1, row).ToString());
	}
	return BuildDeltaSchemaDDL(columns, delta_table, data_table,
	                           replace ? OnCreateConflict::REPLACE_ON_CONFLICT : OnCreateConflict::IGNORE_ON_CONFLICT);
}

// Staged execution suspends the caller transaction. Its rollback callback must
// not reclaim source allocations before the staged program publishes ownership.
class StagedSourceDeltaCleanup {
public:
	explicit StagedSourceDeltaCleanup(ClientContext &context)
	    : owner(TransactionalMVLockState::Get(context)), allocations(owner.TakeSourceDeltaAllocations()),
	      publication_locks(owner.TakeSourcePublicationLocks()) {
	}
	~StagedSourceDeltaCleanup() {
		owner.CleanupSourceDeltaAllocations(allocations);
	}

private:
	TransactionalMVLockState &owner;
	vector<RefreshMetadata::DeltaSource> allocations;
	// Staged execution suspends the planning transaction. Keep source exclusion
	// until its helper publishes ownership or reclaims the unowned allocations.
	vector<unique_ptr<SourceAdmissionGuard>> publication_locks;
};

void ExecuteDDL(ClientContext &context, const vector<string> &ddl) {
	if (ddl.empty()) {
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	auto conn = make_uniq<Connection>(db);
	SqlUtils::CopySessionSetting(context, *conn->context, "TimeZone");
	auto &helper_lock_state = TransactionalMVLockState::Get(*conn->context);
	helper_lock_state.SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
	MutationLockGuard mutation_guard(context);
	StagedSourceDeltaCleanup source_delta_cleanup(context);
	bool suspended_autocommit_transaction = false;
	auto restore_outer_transaction = [&]() {
		if (suspended_autocommit_transaction && !context.transaction.HasActiveTransaction()) {
			context.transaction.BeginTransaction();
		}
	};
	if (context.transaction.IsAutoCommit() && context.transaction.HasActiveTransaction()) {
		context.transaction.Rollback(nullptr);
		suspended_autocommit_transaction = true;
	}
	MVProfiler profiler(context, "create_mv_total");
	auto lifecycle_leases = LifecycleLeaseState::TryGet(context);
	string current_profile_step = "create_mv_unclassified";
	string current_profile_detail;
	vector<string> pending_ddl;
	auto preserve_result = conn->Query("SET preserve_insertion_order=false");
	if (preserve_result->HasError()) {
		throw CatalogException("Failed to configure OpenIVM DDL connection: " + preserve_result->GetError());
	}
	auto depth_result = conn->Query("SET max_expression_depth=10000");
	if (depth_result->HasError()) {
		throw CatalogException("Failed to configure OpenIVM DDL connection: " + depth_result->GetError());
	}
	vector<string> cleanup_ddl;
	// The program to archive, as compiled, and the SQL each executor operation ran.
	vector<string> archive_statements;
	unordered_map<string, string> executed_operations;
	auto run_cleanup = [&]() {
		if (!conn->context->transaction.IsAutoCommit()) {
			conn->Rollback();
		}
		for (const auto &cleanup : cleanup_ddl) {
			if (lifecycle_leases && !lifecycle_leases->CanCleanup()) {
				Printer::Print(
				    "OpenIVM retained staged objects because lifecycle lease ownership could not be confirmed");
				return;
			}
			OPENIVM_DEBUG_PRINT("[DDLExecutorExecuteFunction] Cleanup DDL: %s\n", cleanup.c_str());
			auto cleanup_result = conn->Query(cleanup);
			if (cleanup_result->HasError()) {
				OPENIVM_DEBUG_PRINT("[DDLExecutorExecuteFunction] Cleanup failed: %s\n",
				                    cleanup_result->GetError().c_str());
			}
		}
	};
	auto fail_ddl = [&](const string &message) {
		run_cleanup();
		profiler.AddTotal();
		profiler.Flush();
		restore_outer_transaction();
		throw CatalogException("Failed to execute IVM DDL: " + message);
	};
	auto require_lifecycle = [&]() {
		if (lifecycle_leases) {
			try {
				lifecycle_leases->Require();
			} catch (std::exception &ex) {
				fail_ddl(ex.what());
			}
		}
	};
	auto flush_pending = [&]() {
		if (pending_ddl.empty()) {
			return;
		}
		string query;
		size_t bytes = 0;
		for (idx_t i = 0; i < pending_ddl.size(); i++) {
			if (i > 0) {
				query += ";\n";
			}
			query += pending_ddl[i];
			bytes += pending_ddl[i].size();
		}
		OPENIVM_DEBUG_PRINT("[DDLExecutorExecuteFunction] Executing DDL batch (%lu statements): %s\n",
		                    (unsigned long)pending_ddl.size(), query.c_str());
		auto ddl_start = std::chrono::steady_clock::now();
		unique_ptr<QueryResult> r;
		if (profiler.Enabled() || (lifecycle_leases && lifecycle_leases->Active())) {
			auto statements = SqlUtils::SplitSQLStatements(query);
			for (idx_t i = 0; i < statements.size(); i++) {
				require_lifecycle();
				auto statement_start = std::chrono::steady_clock::now();
				r = conn->Query(statements[i]);
				profiler.AddStep("create_mv_sql_stmt", statement_start,
				                 "phase=" + current_profile_step + "; statement=" + to_string(i + 1) + "/" +
				                     to_string(statements.size()) +
				                     "; sql=" + SqlUtils::SQLStatementPreview(statements[i]));
				if (r->HasError()) {
					break;
				}
			}
		} else {
			r = conn->Query(query);
		}
		profiler.AddStep(current_profile_step, ddl_start,
		                 current_profile_detail + "; statements=" + to_string(pending_ddl.size()) +
		                     "; bytes=" + to_string(bytes));
		if (r->HasError()) {
			bool is_unique_index = pending_ddl.size() == 1 &&
			                       StringUtil::Contains(StringUtil::Lower(pending_ddl[0]), "create unique index") &&
			                       StringUtil::Contains(r->GetError(), "Data contains duplicates");
			if (is_unique_index) {
				Printer::Print("Warning: could not create unique index for MV — group_columns "
				               "are not unique in MV output. Refresh will still work (no index).");
				pending_ddl.clear();
				return;
			}
			fail_ddl(r->GetError());
		}
		if (lifecycle_leases && lifecycle_leases->Active() && current_profile_step == "create_mv_physical_commit" &&
		    MetadataLocator::TestPoint(context, "ddl_lease_lost_after_data")) {
			lifecycle_leases->SimulateTakeover();
			fail_ddl("lifecycle lease was taken over after the data commit");
		}
		pending_ddl.clear();
	};
	for (auto &q : ddl) {
		require_lifecycle();
		if (StringUtil::StartsWith(q, OPENIVM_DDL_PROFILE_TABLE_PREFIX)) {
			profiler.SetProfileTable(q.substr(strlen(OPENIVM_DDL_PROFILE_TABLE_PREFIX)));
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_SNAPSHOT_PUBLICATION_PREFIX)) {
			flush_pending();
			auto fields = DecodeDDLFields(q, OPENIVM_DDL_SNAPSHOT_PUBLICATION_PREFIX);
			if (fields.size() != 4) {
				fail_ddl("malformed snapshot-publication payload");
			}
			try {
				auto publication_sql = BuildSnapshotPublicationSQL(*conn, fields[0], fields[1], fields[2], fields[3]);
				executed_operations[q] = publication_sql;
				pending_ddl.push_back(std::move(publication_sql));
			} catch (std::exception &ex) {
				fail_ddl(ex.what());
			}
			flush_pending();
			continue;
		}
		ProfileOperation profile_op;
		if (TryParseProfileOperation(q, profile_op)) {
			flush_pending();
			if (!profile_op.view_name.empty()) {
				profiler.SetViewName(profile_op.view_name, *conn);
			}
			if (profile_op.measured) {
				profiler.AddMeasuredStep(profile_op.step_name, profile_op.duration_ms, profile_op.detail);
			} else {
				current_profile_step = profile_op.step_name;
				current_profile_detail = profile_op.detail;
			}
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)) {
			flush_pending();
			CreateDeltaOperation operation;
			if (!ParseCreateDeltaOperation(q, operation)) {
				fail_ddl("malformed delta-schema payload");
			}
			auto ddl_start = std::chrono::steady_clock::now();
			DeltaSchemaDDL derived;
			try {
				derived =
				    BuildCreateDeltaFromDataSQL(*conn, operation.delta_table, operation.data_table, operation.replace);
			} catch (std::exception &ex) {
				profiler.AddStep(current_profile_step, ddl_start,
				                 current_profile_detail + "; delta_schema_derivation_failed=true");
				fail_ddl(ex.what());
			}
			executed_operations[q] = derived.sql;
			auto statement_start = std::chrono::steady_clock::now();
			auto r = conn->Query(derived.sql);
			profiler.AddStep("create_mv_sql_stmt", statement_start,
			                 "phase=" + current_profile_step +
			                     "; statement=1/1; sql=" + SqlUtils::SQLStatementPreview(derived.sql));
			profiler.AddStep(current_profile_step, ddl_start,
			                 current_profile_detail + "; statements=1; bytes=" + to_string(derived.sql.size()) +
			                     "; derived_from_data_schema=true; columns=" + to_string(derived.column_count));
			if (r->HasError()) {
				fail_ddl(r->GetError());
			}
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_CLEANUP_PREFIX)) {
			flush_pending();
			cleanup_ddl.push_back(q.substr(strlen(OPENIVM_DDL_CLEANUP_PREFIX)));
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_ARCHIVE_STATEMENT_PREFIX)) {
			archive_statements.push_back(q.substr(strlen(OPENIVM_DDL_ARCHIVE_STATEMENT_PREFIX)));
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_ARCHIVE_WRITE_PREFIX)) {
			flush_pending();
			auto archive_start = std::chrono::steady_clock::now();
			auto fields = DecodeDDLFields(q, OPENIVM_DDL_ARCHIVE_WRITE_PREFIX);
			bool after_commit = fields[0] == ArchiveModeName(DDLArchiveMode::AFTER_COMMIT);
			bool own_transaction = fields[0] != ArchiveModeName(DDLArchiveMode::CURRENT_TRANSACTION);
			auto write_archive = [&]() -> string {
				if (fields.size() != 9) {
					return "malformed compiled-SQL archive payload";
				}
				CompiledProgram program;
				program.view_name = fields[3];
				program.view_catalog = fields[4];
				program.view_schema = fields[5];
				program.view_sql_name = fields[6];
				program.operation = fields[7];
				program.compilation_id = fields[8];
				// Archive what ran: executor operations become the SQL they executed.
				for (auto &statement : archive_statements) {
					auto executed = executed_operations.find(statement);
					if (executed != executed_operations.end()) {
						program.statements.push_back(executed->second);
					} else if (StringUtil::StartsWith(statement, OPENIVM_DDL_SNAPSHOT_PUBLICATION_PREFIX) ||
					           StringUtil::StartsWith(statement, OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)) {
						return "the compiled program references an executor operation that did not run";
					} else {
						program.statements.push_back(statement);
					}
				}
				if (own_transaction) {
					auto begun = conn->Query("BEGIN TRANSACTION");
					if (begun->HasError()) {
						return begun->GetError();
					}
				}
				auto error = WriteCompiledProgram(context, *conn, program, CompiledProgramOutcome::COMMITTED, fields[1],
				                                  fields[2]);
				if (!error.empty() || !own_transaction) {
					return error;
				}
				auto committed = conn->Query("COMMIT");
				return committed->HasError() ? committed->GetError() : string();
			};
			auto error = write_archive();
			profiler.AddStep(current_profile_step, archive_start,
			                 current_profile_detail + "; mode=" + fields[0] +
			                     "; archive_statements=" + to_string(archive_statements.size()));
			archive_statements.clear();
			if (error.empty()) {
				continue;
			}
			if (!after_commit) {
				// The program has not committed yet, or its cleanup removes it.
				fail_ddl("could not archive compiled SQL: " + error);
			}
			// The program's effects have committed and cannot be cleaned up; never report
			// success without the archive.
			if (!conn->context->transaction.IsAutoCommit()) {
				conn->Rollback();
			}
			profiler.AddTotal();
			profiler.Flush();
			restore_outer_transaction();
			throw CatalogException("CREATE OR REPLACE MATERIALIZED VIEW committed, but OpenIVM could not archive "
			                       "its compiled SQL: " +
			                       error);
		}
		pending_ddl.push_back(q);
	}
	flush_pending();
	profiler.AddTotal();
	profiler.Flush();
	restore_outer_transaction();
}

unique_ptr<FunctionData> DDLExecutorBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<DDLExecutorBindData>(true);
	for (auto &param : input.inputs) {
		auto q = param.GetValue<string>();
		if (!q.empty()) {
			bind_data->ddl.push_back(std::move(q));
		}
	}
	names.emplace_back("MATERIALIZED VIEW CREATION");
	return_types.emplace_back(LogicalType::BOOLEAN);
	return std::move(bind_data);
}

void DDLExecutorExecuteFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<DDLExecutorBindData>();
	auto &gdata = dynamic_cast<DDLExecutorGlobalData &>(*data_p.global_state);
	if (gdata.offset >= 1) {
		return;
	}
	ExecuteDDL(context, bind_data.ddl);
	output.data[0].SetValue(0, Value::BOOLEAN(bind_data.result));
	output.SetCardinalityUnsafe(1);
	gdata.offset++;
}

} // namespace

TransactionalMVMetadataState::TransactionalMVMetadataState(ClientContext &context) : owner(context) {
}

TransactionalMVMetadataState &TransactionalMVMetadataState::Get(ClientContext &context) {
	return *context.registered_state->GetOrCreate<TransactionalMVMetadataState>("openivm_transactional_mv_metadata",
	                                                                            context);
}

optional_ptr<TransactionalMVMetadataState> TransactionalMVMetadataState::TryGet(ClientContext &context) {
	return context.registered_state->Get<TransactionalMVMetadataState>("openivm_transactional_mv_metadata");
}

void TransactionalMVMetadataState::Register(ClientContext &context, const vector<Value> &parameters,
                                            const string &view_name, const string &metadata_catalog,
                                            const string &metadata_schema) {
	view_names.insert(view_name);
	dropped_views.erase(view_name);
	auto transaction_timestamp = context.transaction.ActiveTransaction().GetCurrentTransactionStartTimestamp();
	// Delta-table DEFAULT now() is transaction-stable. Compile from one
	// microsecond before it so rows inserted into a newly created MV delta
	// table during this same transaction satisfy the compiler's strict `>`
	// watermark predicate.
	transaction_timestamp.value -= 1;
	auto timestamp_sql = Value::TIMESTAMP(transaction_timestamp).ToSQLString() + "::TIMESTAMP";
	for (auto &parameter : parameters) {
		auto statement = parameter.GetValue<string>();
		if (IsReplayableMetadataStatement(statement)) {
			// The caller records the evaluated watermark at execution time. A
			// helper cannot read source deltas created in the caller transaction.
			auto target = ReadMetadataMutationTarget(statement);
			if (StringUtil::CIStartsWith(statement, "update ") && target.Matches(openivm::DELTA_TABLES_TABLE) &&
			    StringUtil::StartsWith(statement.substr(target.end),
			                           " SET last_update = " + string(OPENIVM_RECORD_SOURCE_WATERMARK_FUNCTION) +
			                               "(")) {
				continue;
			}
			statements.push_back(QualifyMetadataMutation(StabilizeTransactionalMetadata(statement, timestamp_sql),
			                                             metadata_catalog, metadata_schema));
		}
	}
}

void TransactionalMVMetadataState::RegisterSQL(const string &sql, const string &view_name) {
	view_names.insert(view_name);
	for (auto &statement : SqlUtils::SplitSQLStatements(sql)) {
		StringUtil::Trim(statement);
		if (IsReplayableMetadataStatement(statement)) {
			statements.push_back(std::move(statement));
		}
	}
}

void TransactionalMVMetadataState::IncludeView(const string &view_name) {
	view_names.insert(view_name);
}

void TransactionalMVMetadataState::RegisterViewDrop(const string &view_name) {
	view_names.insert(view_name);
	dropped_views.insert(view_name);
}

bool TransactionalMVMetadataState::HasPendingViewDrop(const string &view_name) const {
	return dropped_views.count(view_name) != 0;
}

vector<pair<string, string>> TransactionalMVMetadataState::MetadataLocations() const {
	vector<pair<string, string>> locations;
	for (auto &statement : statements) {
		auto target = ReadMetadataMutationTarget(statement);
		if (!target.Matches(TransactionalMetadataTables())) {
			continue;
		}
		if (target.parts.size() != 3) {
			throw InternalException("Transaction-local OpenIVM metadata target must be qualified");
		}
		pair<string, string> location {target.parts[0], target.parts[1]};
		if (std::find(locations.begin(), locations.end(), location) == locations.end()) {
			locations.push_back(std::move(location));
		}
	}
	return locations;
}

void TransactionalMVMetadataState::Apply(Connection &connection) const {
	if (statements.empty()) {
		return;
	}
	// Replayed ALTER statements enter the maintenance optimizer. Every replay
	// helper, including ownership-inventory readers, belongs to the caller that
	// already holds the mutation gate, rather than a competing transaction.
	TransactionalMVLockState::Get(*connection.context)
	    .SetMutationOwner(TransactionalMVLockState::Get(owner).GetMutationOwner());
	// Seed from the durable tables in the connection's metadata schema.
	auto metadata_location = RefreshMetadata::CurrentLocation(connection);
	vector<string> local_statements;
	for (auto &statement : statements) {
		auto local = LocalizeMetadataStatement(statement, metadata_location.catalog, metadata_location.schema);
		if (!local.empty()) {
			local_statements.push_back(std::move(local));
		}
	}
	// A source CASCADE can inspect several metadata catalogs on this connection.
	// Temporary names shadow every search path, so discard the prior location.
	connection.context->RunFunctionInTransaction([&]() {
		for (auto &table : TransactionalMetadataTables()) {
			DropInfo drop;
			drop.type = CatalogType::TABLE_ENTRY;
			drop.SetCatalog(Identifier("temp"));
			drop.SetSchema(Identifier(DEFAULT_SCHEMA));
			drop.SetName(Identifier(table));
			drop.if_not_found = OnEntryNotFound::RETURN_NULL;
			Catalog::GetCatalog(*connection.context, Identifier(drop.GetQualifiedName().Catalog().GetIdentifierName()))
			    .DropEntry(*connection.context, drop);
		}
	});
	OPENIVM_DEBUG_PRINT("[TRANSACTIONAL METADATA] Replay catalog='%s', schema='%s', statements=%zu\n",
	                    metadata_location.catalog.c_str(), metadata_location.schema.c_str(), local_statements.size());
	auto durable_table = [&](const string &table) {
		return SqlUtils::FullName(metadata_location.catalog, metadata_location.schema, table);
	};
	// Build constrained TEMP schemas first. CREATE TABLE AS would discard the
	// primary keys required by INSERT OR REPLACE metadata operations.
	for (auto &statement : local_statements) {
		if (!IsMetadataSchemaStatement(statement)) {
			continue;
		}
		auto result = connection.Query(MakeTemporaryMetadataDDL(statement));
		if (result->HasError()) {
			throw CatalogException("Could not reconstruct transaction-local OpenIVM metadata schema: %s",
			                       result->GetError());
		}
	}
	auto seed_table = [&](const string &table, const string &filter) {
		bool has_constrained_schema = false;
		for (auto &statement : local_statements) {
			auto lower = StringUtil::Lower(statement);
			if (StringUtil::StartsWith(lower, "create table") && lower.find(StringUtil::Lower(table)) != string::npos) {
				has_constrained_schema = true;
				break;
			}
		}
		if (has_constrained_schema) {
			// Seed the constrained shadow with committed rows. A missing durable
			// table is expected for the first MV in a database.
			connection.Query("INSERT OR IGNORE INTO " + table + " BY NAME SELECT * FROM " + durable_table(table) +
			                 filter);
		} else {
			// Less central metadata (for example optional dependency state) may
			// be initialized outside the lifecycle program. Still shadow it so
			// replay can never mutate the durable helper-connection catalog.
			connection.Query("CREATE TEMP TABLE IF NOT EXISTS " + table + " AS SELECT * FROM " + durable_table(table) +
			                 filter);
		}
	};
	auto replay_statement = [&](const string &statement) {
		auto result = connection.Query(MakeTemporaryMetadataDDL(statement));
		if (result->HasError()) {
			throw CatalogException("Could not reconstruct transaction-local OpenIVM metadata: %s", result->GetError());
		}
	};

	// Cascade dependencies are recorded both explicitly for view matching and
	// implicitly when a view consumes another view's delta/data table. Seed and
	// replay all three relation tables first so the closure reflects this
	// transaction's CREATE/REPLACE/DROP statements.
	const vector<string> dependency_tables = {
	    openivm::VIEWS_TABLE,
	    openivm::DELTA_TABLES_TABLE,
	    openivm::MV_DEPS_TABLE,
	};
	for (auto &table : dependency_tables) {
		seed_table(table, "");
	}
	for (auto &statement : local_statements) {
		if (IsMetadataSchemaStatement(statement)) {
			continue;
		}
		if (ReadMetadataMutationTarget(statement).Matches(dependency_tables)) {
			replay_statement(statement);
		}
	}
	unordered_set<string> included_views = view_names;
	unordered_map<string, vector<string>> dependency_graph;
	idx_t dependency_edge_count = 0;
	auto add_dependency = [&](const string &parent, const string &child) {
		dependency_graph[parent].push_back(child);
		dependency_graph[child].push_back(parent);
		dependency_edge_count++;
	};
	auto dependencies = connection.Query("SELECT parent_view, child_view FROM " + string(openivm::MV_DEPS_TABLE));
	if (!dependencies->HasError()) {
		for (idx_t row = 0; row < dependencies->RowCount(); row++) {
			add_dependency(dependencies->Collection().GetValue(0, row).ToString(),
			               dependencies->Collection().GetValue(1, row).ToString());
		}
	}
	unordered_set<string> registered_views;
	auto views = connection.Query("SELECT view_name FROM " + string(openivm::VIEWS_TABLE));
	if (!views->HasError()) {
		for (idx_t row = 0; row < views->RowCount(); row++) {
			registered_views.insert(views->Collection().GetValue(0, row).ToString());
		}
	}
	auto delta_dependencies =
	    connection.Query("SELECT view_name, table_name FROM " + string(openivm::DELTA_TABLES_TABLE));
	if (!delta_dependencies->HasError()) {
		static const string delta_prefix(openivm::DELTA_PREFIX);
		static const string data_prefix(openivm::DATA_TABLE_PREFIX);
		for (idx_t row = 0; row < delta_dependencies->RowCount(); row++) {
			auto child = delta_dependencies->Collection().GetValue(0, row).ToString();
			auto table = delta_dependencies->Collection().GetValue(1, row).ToString();
			string parent;
			if (StringUtil::StartsWith(table, delta_prefix)) {
				parent = table.substr(delta_prefix.size());
			} else if (StringUtil::StartsWith(table, data_prefix)) {
				parent = table.substr(data_prefix.size());
			}
			if (!parent.empty() && registered_views.count(parent)) {
				add_dependency(parent, child);
			}
		}
	}
	vector<string> pending_views(included_views.begin(), included_views.end());
	for (idx_t pending_index = 0; pending_index < pending_views.size(); pending_index++) {
		auto neighbors = dependency_graph.find(pending_views[pending_index]);
		if (neighbors == dependency_graph.end()) {
			continue;
		}
		for (auto &neighbor : neighbors->second) {
			if (included_views.insert(neighbor).second) {
				pending_views.push_back(neighbor);
			}
		}
	}
	OPENIVM_DEBUG_PRINT("[TRANSACTIONAL METADATA] Seed views=%zu, dependency edges=%zu, closure views=%zu\n",
	                    registered_views.size(), dependency_edge_count, included_views.size());
	vector<string> sorted_included_views(included_views.begin(), included_views.end());
	std::sort(sorted_included_views.begin(), sorted_included_views.end());
	string view_filter =
	    StringUtil::Join(sorted_included_views, sorted_included_views.size(), ", ",
	                     [](const string &view_name) { return "'" + SqlUtils::EscapeValue(view_name) + "'"; });
	// Keep the complete replayed ownership relations. Independent committed views
	// can share a source delta without belonging to this dependency closure. DROP
	// must see those consumers as well as ownership added or removed by this
	// transaction before deciding whether to delete the physical delta.
	// Only the per-view bookkeeping below is restricted to the closure.
	for (auto &table : TransactionalMetadataTables()) {
		if (std::find(dependency_tables.begin(), dependency_tables.end(), table) != dependency_tables.end()) {
			continue;
		}
		string filter = view_filter.empty() ? string() : " WHERE view_name IN (" + view_filter + ")";
		seed_table(table, filter);
	}
	for (auto &statement : local_statements) {
		if (IsMetadataSchemaStatement(statement)) {
			continue;
		}
		if (ReadMetadataMutationTarget(statement).Matches(dependency_tables)) {
			continue;
		}
		replay_statement(statement);
	}
}

void TransactionalMVMetadataState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	Clear();
}

void TransactionalMVMetadataState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Clear();
}

void TransactionalMVMetadataState::Clear() {
	statements.clear();
	view_names.clear();
	dropped_views.clear();
}

string EncodeDDLOperation(const char *prefix, const vector<string> &fields) {
	string result = prefix;
	for (idx_t i = 0; i < fields.size(); i++) {
		if (i > 0) {
			result += '\t';
		}
		for (auto c : fields[i]) {
			if (c == '\\' || c == '\t') {
				result += '\\';
			}
			result += c == '\t' ? 't' : c;
		}
	}
	return result;
}

string BuildCreateDeltaFromDataOperation(const string &delta_table, const string &data_table, bool replace) {
	return EncodeDDLOperation(OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX,
	                          {replace ? "replace" : "create", delta_table, data_table});
}

vector<string> BuildArchiveProgramOperation(const CompiledProgram &program, DDLArchiveMode mode,
                                            const string &metadata_catalog, const string &metadata_schema) {
	vector<string> result;
	if (program.statements.empty()) {
		return result;
	}
	for (auto &statement : program.statements) {
		result.push_back(string(OPENIVM_DDL_ARCHIVE_STATEMENT_PREFIX) + statement);
	}
	result.push_back(EncodeDDLOperation(OPENIVM_DDL_ARCHIVE_WRITE_PREFIX,
	                                    {ArchiveModeName(mode), metadata_catalog, metadata_schema, program.view_name,
	                                     program.view_catalog, program.view_schema, program.view_sql_name,
	                                     program.operation, program.compilation_id}));
	return result;
}

string BuildSourceWatermarkUpdate(const string &metadata_table, const string &view_name, const string &source_name,
                                  const string &watermark, const string &refresh_timestamp, bool record) {
	auto value = watermark;
	if (record) {
		value = string(OPENIVM_RECORD_SOURCE_WATERMARK_FUNCTION) + "(" + Value(metadata_table).ToSQLString() + ", " +
		        Value(view_name).ToSQLString() + ", " + Value(source_name).ToSQLString() + ", " + watermark + ", " +
		        refresh_timestamp + ")";
	}
	return "UPDATE " + metadata_table + " SET last_update = " + value + ", last_refresh_ts = " + refresh_timestamp +
	       " WHERE view_name = " + Value(view_name).ToSQLString() +
	       " AND table_name = " + Value(source_name).ToSQLString();
}

static void RecordSourceWatermark(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t row = 0; row < args.size(); row++) {
		auto watermark = args.GetValue(3, row);
		if (!context.transaction.IsAutoCommit()) {
			auto view_name = args.GetValue(1, row).GetValue<string>();
			TransactionalMVMetadataState::Get(context).RegisterSQL(
			    BuildSourceWatermarkUpdate(args.GetValue(0, row).GetValue<string>(), view_name,
			                               args.GetValue(2, row).GetValue<string>(),
			                               watermark.ToSQLString() + "::TIMESTAMP",
			                               args.GetValue(4, row).ToSQLString() + "::TIMESTAMP", false),
			    view_name);
			OPENIVM_DEBUG_PRINT("[TRANSACTIONAL METADATA] Recorded source watermark for '%s'\n", view_name.c_str());
		}
		result.SetValue(row, watermark);
	}
}

ScalarFunction SourceWatermarkFunction() {
	return ScalarFunction(OPENIVM_RECORD_SOURCE_WATERMARK_FUNCTION,
	                      {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::TIMESTAMP,
	                       LogicalType::TIMESTAMP},
	                      LogicalType::TIMESTAMP, RecordSourceWatermark, nullptr, nullptr, nullptr,
	                      LogicalType::INVALID, FunctionStability::VOLATILE);
}

namespace {

// Each step has its own discard sink, so even statements returning no rows
// finish before the next step starts. Ordered UNION only orders result rows;
// it does not order the mutation sinks inside its children.
class PhysicalLifecycleSink : public PhysicalOperator {
public:
	explicit PhysicalLifecycleSink(PhysicalPlan &physical_plan)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {}, 1) {
	}

	bool IsSink() const override {
		return true;
	}

	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override {
		return SinkResultType::NEED_MORE_INPUT;
	}
};

// Dynamic steps are not in the query profiler's initial physical tree. Keep
// their operator metrics inside the enclosing lifecycle operator's time, rather
// than flushing unknown operator pointers into DuckDB's fixed profiling tree.
// Retain this scope through cancellation on failure: tasks may still be draining.
class LifecycleMetricsScope {
public:
	explicit LifecycleMetricsScope(ClientContext &context) : config(ClientConfig::GetConfig(context)) {
		settings.swap(config.tracked_metrics);
	}

	~LifecycleMetricsScope() {
		settings.swap(config.tracked_metrics);
	}

private:
	ClientConfig &config;
	vector<string> settings;
};

// A concurrent duplicate CREATE can lose on the physical data table with DuckDB's write-write
// conflict, which names the internal table. Its key encodes the lowercase catalog, schema and
// view name in hex, so recover the user-facing name. Returns empty if this is not that conflict.
static string DuplicateCreateMessage(const string &error) {
	if (error.find("write-write conflict on create") == string::npos) {
		return string();
	}
	static const string KEY_PREFIX = "openivm_data___openivm_mv_";
	auto start = error.find(KEY_PREFIX);
	if (start == string::npos) {
		return string();
	}
	vector<string> parts;
	string current;
	for (auto i = start + KEY_PREFIX.size(); i < error.size() && parts.size() < 3; i++) {
		char c = error[i];
		if (c == '_') {
			parts.push_back(std::move(current));
			current.clear();
		} else if (std::isxdigit(static_cast<unsigned char>(c))) {
			current += c;
		} else {
			return string();
		}
	}
	if (parts.size() != 3) {
		return string();
	}
	vector<string> decoded;
	for (auto &part : parts) {
		if (part.empty() || part.size() % 2 != 0) {
			return string();
		}
		string name;
		for (idx_t i = 0; i < part.size(); i += 2) {
			name += static_cast<char>(std::stoi(part.substr(i, 2), nullptr, 16));
		}
		decoded.push_back(std::move(name));
	}
	return "materialized view \"" + decoded[2] + "\" already exists in \"" + decoded[0] + "\".\"" + decoded[1] + "\"";
}

struct LifecycleStepState {
	unique_ptr<PhysicalPlan> plan;
	PipelineBuildState build_state;
	shared_ptr<MetaPipeline> pipeline;
};

// Recursive CTE iteration pipelines are detached from the ordinary children.
// Ready them after Build, just as Executor::InitializeInternal does.
static void ReadyLifecycleRecursivePipelines(const PhysicalOperator &op) {
	if (op.type == PhysicalOperatorType::RECURSIVE_CTE) {
		op.Cast<PhysicalRecursiveCTE>().recursive_meta_pipeline->Ready();
	}
	for (auto &child : op.GetChildren()) {
		ReadyLifecycleRecursivePipelines(child.get());
	}
}

struct LifecycleSourceState : public GlobalSourceState {
	bool finished = false;
	// Executor events retain references to the step plans until query cleanup.
	vector<unique_ptr<LifecycleStepState>> steps;
	unique_ptr<LifecycleMetricsScope> metrics;
};

class PhysicalLifecycleProgram : public PhysicalOperator {
public:
	PhysicalLifecycleProgram(PhysicalPlan &physical_plan, string program, bool staged)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BOOLEAN}, 1),
	      program(std::move(program)), staged(staged) {
	}

	bool IsSource() const override {
		return true;
	}

	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
		return make_uniq<LifecycleSourceState>();
	}

	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override {
		auto &state = input.global_state.Cast<LifecycleSourceState>();
		if (state.finished) {
			return SourceResultType::FINISHED;
		}
		auto &client = context.client;
		if (staged) {
			MutationLockGuard guard(client);
			ExecuteStagedDDL(client, {Value(program)});
			chunk.data[0].SetValue(0, Value::BOOLEAN(true));
			chunk.SetCardinalityUnsafe(1);
			state.finished = true;
			return SourceResultType::FINISHED;
		}
		auto &executor = Executor::Get(client);
		state.metrics = make_uniq<LifecycleMetricsScope>(client);
		try {
			ParserOptions options = ParserOptions::Builtin();
			options.extensions = nullptr;
			Parser parser(options);
			parser.ParseQuery(program);
			for (auto &statement : parser.statements) {
				// Internal DROP already has a compiled ownership/cleanup program.
				// Bind only its physical executor, without recursively compiling cleanup.
				if (statement->type == StatementType::DROP_STATEMENT) {
					auto &drop = statement->Cast<DropStatement>();
					if (drop.info->type == CatalogType::TABLE_ENTRY || drop.info->type == CatalogType::VIEW_ENTRY) {
						Parser drop_parser(options);
						drop_parser.ParseQuery(drop.info->type == CatalogType::TABLE_ENTRY
						                           ? BuildDropTableStatement(*drop.info)
						                           : BuildDropViewStatement(*drop.info));
						statement = std::move(drop_parser.statements[0]);
					} else if (drop.info->type == CatalogType::SCHEMA_ENTRY) {
						Parser drop_parser(options);
						drop_parser.ParseQuery(BuildDropSchemaStatement(*drop.info));
						statement = std::move(drop_parser.statements[0]);
					}
				}
				// Binding is deliberately here, after all prior physical steps finish.
				// CREATE SCHEMA/TABLE, replacement and DROP dependencies are now visible
				// in the caller's transaction, without a second connection or executor.
				Planner planner(client);
				planner.CreatePlan(std::move(statement));
				for (auto &entry : planner.properties.modified_databases) {
					auto &database = Catalog::GetCatalog(client, Identifier(entry.first)).GetAttached();
					if (database.IsReadOnly()) {
						throw InvalidInputException(
						    "Cannot execute OpenIVM lifecycle writes on read-only database '%s'", entry.first);
					}
					MetaTransaction::Get(client).ModifyDatabase(database, entry.second.modifications);
				}
				auto logical = std::move(planner.plan);
				// Run the maintenance optimizer extensions even when the caller disables
				// ordinary query optimization: source retractions still need delta capture.
				if (logical->RequireOptimizer()) {
					Optimizer optimizer(*planner.binder, client);
					logical = optimizer.Optimize(std::move(logical));
				}
				// Retain the plan before Build can register recursive CTEs or events
				// with the executor, including when pipeline construction throws.
				state.steps.push_back(make_uniq<LifecycleStepState>());
				auto &step = *state.steps.back();
				PhysicalPlanGenerator physical_planner(client);
				step.plan = physical_planner.Plan(std::move(logical));
				auto &sink = step.plan->Make<PhysicalLifecycleSink>();
				step.pipeline = make_shared_ptr<MetaPipeline>(executor, step.build_state, &sink);
				step.pipeline->Build(step.plan->Root());
				step.pipeline->Ready();
				ReadyLifecycleRecursivePipelines(step.plan->Root());
				vector<shared_ptr<MetaPipeline>> pipelines;
				step.pipeline->GetMetaPipelines(pipelines, true, false);
				vector<shared_ptr<Event>> events;
				executor.ReschedulePipelines(pipelines, events);
				// This is DuckDB's recursive-CTE execution pattern: reschedule work on
				// the active executor and drain it before compiling the next statement.
				while (true) {
					executor.WorkOnTasks();
					if (executor.HasError()) {
						executor.ThrowException();
					}
					bool finished = true;
					for (auto &event : events) {
						finished &= event->IsFinished();
					}
					if (finished) {
						break;
					}
					if (client.IsInterrupted()) {
						throw InterruptException();
					}
				}
				OPENIVM_DEBUG_PRINT("[LIFECYCLE] Executed caller-transaction step %zu\n", state.steps.size());
			}
		} catch (std::exception &ex) {
			// A late bind failure is an execution failure of this whole lifecycle,
			// even when DuckDB would normally permit committing after a binder error.
			auto duplicate = DuplicateCreateMessage(ex.what());
			ValidChecker::Invalidate(client.ActiveTransaction(), duplicate.empty() ? string(ex.what()) : duplicate);
			if (!duplicate.empty()) {
				throw CatalogException("%s", duplicate);
			}
			throw;
		}
		state.metrics.reset();
		chunk.data[0].SetValue(0, Value::BOOLEAN(true));
		chunk.SetCardinalityUnsafe(1);
		state.finished = true;
		return SourceResultType::FINISHED;
	}

	vector<const_reference<PhysicalOperator>> GetSources() const override {
		return {*this};
	}

private:
	const string program;
	const bool staged;
};

class LogicalLifecycleProgram : public LogicalExtensionOperator {
public:
	LogicalLifecycleProgram(TableIndex table_index, string program, bool staged = false)
	    : table_index(table_index), program(std::move(program)), staged(staged) {
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<PhysicalLifecycleProgram>(program, staged);
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, 1);
	}

	vector<TableIndex> GetTableIndex() const override {
		return {table_index};
	}

	bool RequireOptimizer() const override {
		// Each statement is bound and optimized immediately before execution.
		return false;
	}

	bool SupportSerialization() const override {
		return false;
	}

	string GetName() const override {
		return "OPENIVM_LIFECYCLE_PROGRAM";
	}

protected:
	void ResolveTypes() override {
		types = {LogicalType::BOOLEAN};
	}

private:
	const TableIndex table_index;
	const string program;
	const bool staged;
};

} // namespace

unique_ptr<LogicalOperator> BindLifecycleProgram(ClientContext &context, Binder &binder, const string &program) {
	// The native driver binds this one lifecycle after preceding user statements
	// execute. Its generated SQL is still unbound: later steps may use objects
	// created by earlier steps. Never pre-bind the entire program here.
	binder.GetStatementProperties().always_require_rebind = true;
	OPENIVM_DEBUG_PRINT("[LIFECYCLE] Prepared deferred caller-transaction program\n");
	return make_uniq<LogicalLifecycleProgram>(binder.GenerateTableIndex(), program);
}

unique_ptr<LogicalOperator> BindLifecycleProgram(ClientContext &context, Binder &binder, TableIndex bind_index,
                                                 const string &program, bool staged) {
	binder.GetStatementProperties().always_require_rebind = true;
	return make_uniq<LogicalLifecycleProgram>(bind_index, program, staged);
}

static string BuildDropEntryStatement(const DropInfo &drop_info, const string &function) {
	return "SELECT * FROM " + function + "('" +
	       SqlUtils::EscapeValue(drop_info.GetQualifiedName().Catalog().GetIdentifierName()) + "', '" +
	       SqlUtils::EscapeValue(drop_info.GetQualifiedName().Schema().GetIdentifierName()) + "', '" +
	       SqlUtils::EscapeValue(drop_info.GetQualifiedName().Name().GetIdentifierName()) + "', " +
	       (drop_info.cascade ? "true" : "false") + ", " +
	       (drop_info.if_not_found == OnEntryNotFound::RETURN_NULL ? "true" : "false") + ")";
}

string BuildDropViewStatement(const DropInfo &drop_info) {
	return BuildDropEntryStatement(drop_info, "openivm_execute_drop_view");
}

string BuildDropTableStatement(const DropInfo &drop_info) {
	return BuildDropEntryStatement(drop_info, "openivm_execute_drop_table");
}

string BuildDropSchemaStatement(const DropInfo &drop_info) {
	return BuildDropEntryStatement(drop_info, "openivm_execute_drop_schema");
}

struct DropViewBindData : public TableFunctionData {
	DropViewBindData() = default;

	explicit DropViewBindData(const DropInfo &info) : info(info) {
	}

	DropInfo info;
};

struct DropViewGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

static unique_ptr<FunctionData> BindDropEntry(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names,
                                              CatalogType type) {
	if (input.inputs.size() != 5) {
		throw InternalException("OpenIVM DROP executor expected five arguments");
	}
	auto result = make_uniq<DropViewBindData>();
	result->info.type = type;
	result->info.SetCatalog(Identifier(StringValue::Get(input.inputs[0])));
	result->info.SetSchema(Identifier(StringValue::Get(input.inputs[1])));
	result->info.SetName(Identifier(StringValue::Get(input.inputs[2])));
	if (type == CatalogType::SCHEMA_ENTRY) {
		result->info.SetQualifiedName(QualifiedName::FromPath(
		    {Identifier(StringValue::Get(input.inputs[0])), Identifier(StringValue::Get(input.inputs[2]))}));
	}
	result->info.cascade = BooleanValue::Get(input.inputs[3]);
	result->info.if_not_found =
	    BooleanValue::Get(input.inputs[4]) ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	if (!input.binder) {
		throw InternalException("OpenIVM DROP executor requires a binder");
	}
	// Bind when this statement is reached, after preceding batch statements have
	// executed. Native DROP binding resolves search paths (including temp.main),
	// rejects internal entries and registers writes only for persistent objects.
	DropStatement drop;
	drop.info = make_uniq<DropInfo>(result->info);
	auto bound = input.binder->Bind(static_cast<SQLStatement &>(drop));
	auto &resolved = bound.plan->Cast<LogicalDrop>().info->Cast<DropInfo>();
	result->info.SetQualifiedName(resolved.GetQualifiedName());
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static unique_ptr<FunctionData> BindDropView(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	return BindDropEntry(context, input, return_types, names, CatalogType::VIEW_ENTRY);
}

static unique_ptr<FunctionData> BindDropTable(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	return BindDropEntry(context, input, return_types, names, CatalogType::TABLE_ENTRY);
}

static unique_ptr<FunctionData> BindDropSchema(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<Identifier> &names) {
	return BindDropEntry(context, input, return_types, names, CatalogType::SCHEMA_ENTRY);
}

static unique_ptr<GlobalTableFunctionState> InitDropView(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DropViewGlobalState>();
}

static void ExecuteDropView(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<DropViewGlobalState>();
	if (state.finished) {
		return;
	}
	auto &bind_data = input.bind_data->Cast<DropViewBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.info.type == CatalogType::SCHEMA_ENTRY
	                                                 ? bind_data.info.GetQualifiedName().Path().front()
	                                                 : bind_data.info.GetQualifiedName().Catalog());
	DropInfo info(bind_data.info);
	bool source_delta_drop =
	    info.type == CatalogType::TABLE_ENTRY &&
	    SqlUtils::IsDelta(info.GetQualifiedName().Name().GetIdentifierName()) &&
	    Catalog::GetEntry(
	        context,
	        SqlUtils::QualifyLookup(
	            EntryLookupInfo(CatalogType::TABLE_ENTRY,
	                            QualifiedName(Identifier(info.GetQualifiedName().Name().GetIdentifierName()))),
	            Identifier(info.GetQualifiedName().Catalog().GetIdentifierName()),
	            Identifier(info.GetQualifiedName().Schema().GetIdentifierName())),
	        OnEntryNotFound::RETURN_NULL);
	vector<reference<TableCatalogEntry>> schema_tables;
	if (info.type == CatalogType::SCHEMA_ENTRY && info.cascade && catalog.IsDuckCatalog()) {
		auto schema = catalog.GetSchema(context, info.GetQualifiedName().Name(), OnEntryNotFound::RETURN_NULL);
		if (schema) {
			schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type == CatalogType::TABLE_ENTRY) {
					schema_tables.push_back(entry.Cast<TableCatalogEntry>());
				}
			});
		}
	}
	if (catalog.IsDuckCatalog() && (info.type == CatalogType::TABLE_ENTRY || info.type == CatalogType::SCHEMA_ENTRY)) {
		RefreshMetadata::InvalidateSourceGeneration(
		    context, catalog.GetName().GetIdentifierName(),
		    info.type == CatalogType::SCHEMA_ENTRY ? info.GetQualifiedName().Name().GetIdentifierName()
		                                           : info.GetQualifiedName().Schema().GetIdentifierName(),
		    info.GetQualifiedName().Name().GetIdentifierName(), info.type == CatalogType::SCHEMA_ENTRY);
	}
	catalog.DropEntry(context, info);
	if (catalog.IsDuckCatalog() && (info.type == CatalogType::TABLE_ENTRY || info.type == CatalogType::SCHEMA_ENTRY)) {
		RefreshMetadata::RemoveSourceDeltaMapping(
		    context, catalog.GetName().GetIdentifierName(),
		    info.type == CatalogType::SCHEMA_ENTRY ? info.GetQualifiedName().Name().GetIdentifierName()
		                                           : info.GetQualifiedName().Schema().GetIdentifierName(),
		    info.GetQualifiedName().Name().GetIdentifierName(), info.type == CatalogType::SCHEMA_ENTRY);
	}
	// Native schema CASCADE drops through CatalogSet, bypassing the local-storage
	// cleanup in DuckSchemaEntry::DropEntry. Discard pending appends only after a
	// successful DROP, or commit would flush them into the removed table allocation.
	// Use the captured entries, not names that a later CREATE can reuse.
	for (auto &table : schema_tables) {
		LocalStorage::Get(context, catalog).DropTable(table.get().GetStorage());
	}
	if (!schema_tables.empty()) {
		OPENIVM_DEBUG_PRINT("[DROP] Discarded local storage for %zu schema tables in %s.%s\n", schema_tables.size(),
		                    catalog.GetName().GetIdentifierName().c_str(),
		                    info.GetQualifiedName().Name().GetIdentifierName().c_str());
	}
	if (info.type == CatalogType::SCHEMA_ENTRY) {
		// Match PhysicalDrop's search-path handling when dropping the current schema.
		auto &current = ClientData::Get(context).catalog_search_path->GetDefault();
		if (StringUtil::CIEquals(catalog.GetName().GetIdentifierName(), current.GetCatalog().GetIdentifierName()) &&
		    StringUtil::CIEquals(info.GetQualifiedName().Name().GetIdentifierName(),
		                         current.GetSchema().GetIdentifierName())) {
			SchemaSetting::SetLocal(context, DEFAULT_SCHEMA);
		}
	}
	if (source_delta_drop) {
		TransactionalMVLockState::Get(context).RecordSourceDeltaDrop(
		    info.GetQualifiedName().Catalog().GetIdentifierName(), info.GetQualifiedName().Schema().GetIdentifierName(),
		    info.GetQualifiedName().Name().GetIdentifierName());
		OPENIVM_DEBUG_PRINT("[DROP] Recorded transaction-local source delta removal %s\n",
		                    SqlUtils::FullName(info.GetQualifiedName().Catalog(), info.GetQualifiedName().Schema(),
		                                       info.GetQualifiedName().Name())
		                        .c_str());
	}
	output.data[0].SetValue(0, Value::BOOLEAN(true));
	output.SetCardinalityUnsafe(1);
	state.finished = true;
}

vector<string> RenderTransactionalStatement(const string &input, const string &metadata_catalog,
                                            const string &metadata_schema) {
	if (IsSearchPathStatement(input)) {
		return {};
	}
	auto statement = QualifyMetadataMutation(input, metadata_catalog, metadata_schema);
	if (StringUtil::StartsWith(statement, OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)) {
		CreateDeltaOperation operation;
		if (!ParseCreateDeltaOperation(statement, operation)) {
			throw InternalException("Malformed OpenIVM delta-schema operation");
		}
		return {"SELECT true FROM openivm_create_delta_from_data(" + Value(operation.delta_table).ToSQLString() + ", " +
		        Value(operation.data_table).ToSQLString() + ", " + (operation.replace ? "true" : "false") + ")"};
	}
	try {
		auto parser = Parser::GetBuiltinParser();
		parser.ParseQuery(statement);
		if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::DROP_STATEMENT) {
			auto &drop = parser.statements[0]->Cast<DropStatement>();
			if (drop.info->type == CatalogType::VIEW_ENTRY) {
				return {BuildDropViewStatement(*drop.info)};
			}
			if (drop.info->type == CatalogType::TABLE_ENTRY) {
				return {BuildDropTableStatement(*drop.info)};
			}
			if (drop.info->type == CatalogType::SCHEMA_ENTRY) {
				return {BuildDropSchemaStatement(*drop.info)};
			}
		}
	} catch (std::exception &) {
		// The normal DuckDB parser will produce the authoritative error when the
		// transactional program is executed.
	}
	return {statement};
}

TableFunction CreateDeltaFromDataFunction() {
	TableFunction function("openivm_create_delta_from_data",
	                       {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN}, nullptr);
	function.bind_replace = [](ClientContext &context, TableFunctionBindInput &input) -> unique_ptr<TableRef> {
		auto delta_table = input.inputs[0].ToString();
		auto data_table = input.inputs[1].ToString();
		auto parts = SqlUtils::ParseQualifiedIdentifier(data_table);
		if (parts.empty() || parts.size() > 3) {
			throw InternalException("Invalid OpenIVM delta source '%s'", data_table);
		}
		Identifier catalog(parts.size() == 3 ? parts[0] : string());
		Identifier schema(parts.size() >= 2 ? parts[parts.size() - 2] : string());
		Binder::BindSchemaOrCatalog(context, catalog, schema);
		auto &table = Catalog::GetEntry<TableCatalogEntry>(
		    context, QualifiedName(Identifier(catalog), Identifier(schema), Identifier(parts.back())));
		vector<pair<string, string>> columns;
		for (auto &column : table.GetColumns().Logical()) {
			columns.emplace_back(column.Name(), column.Type().ToString());
		}
		// Bind after the preceding lifecycle step creates the data table, in the
		// caller's transaction. Defaults belong in CREATE: ALTER SET DEFAULT now()
		// cannot be replayed before DuckDB initializes its default database.
		auto ddl = BuildDeltaSchemaDDL(columns, delta_table, data_table,
		                               input.inputs[2].GetValue<bool>() ? OnCreateConflict::REPLACE_ON_CONFLICT
		                                                                : OnCreateConflict::ERROR_ON_CONFLICT);
		auto parser = Parser::GetBuiltinParser();
		parser.ParseQuery(ddl.sql);
		auto binder = Binder::CreateBinder(context, input.binder);
		auto bound = binder->Bind(*parser.statements[0]);
		OPENIVM_DEBUG_PRINT("[LIFECYCLE] Bound delta schema with defaults for %s\n", delta_table.c_str());
		return make_uniq<BoundRefWrapper>(std::move(bound), std::move(binder));
	};
	return function;
}

static void SerializeDropEntry(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                               const BoundTableFunction &function) {
	// LogicalGet's fallback deserializer calls bind without a Binder. Retain the
	// native binder's resolved target instead of resolving the search path again.
	serializer.WriteProperty(100, "drop_info", &bind_data->Cast<DropViewBindData>().info);
}

static unique_ptr<FunctionData> DeserializeDropEntry(Deserializer &deserializer, BoundTableFunction &function) {
	auto info = deserializer.ReadProperty<unique_ptr<ParseInfo>>(100, "drop_info");
	return make_uniq<DropViewBindData>(info->Cast<DropInfo>());
}

TableFunction DropEntryFunction(CatalogType type) {
	D_ASSERT(type == CatalogType::TABLE_ENTRY || type == CatalogType::VIEW_ENTRY || type == CatalogType::SCHEMA_ENTRY);
	const bool is_table = type == CatalogType::TABLE_ENTRY;
	const bool is_schema = type == CatalogType::SCHEMA_ENTRY;
	TableFunction function(
	    is_schema ? "openivm_execute_drop_schema"
	              : (is_table ? "openivm_execute_drop_table" : "openivm_execute_drop_view"),
	    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN, LogicalType::BOOLEAN},
	    ExecuteDropView, is_schema ? BindDropSchema : (is_table ? BindDropTable : BindDropView), InitDropView);
	function.serialize = SerializeDropEntry;
	function.deserialize = DeserializeDropEntry;
	return function;
}

string RenderTransactionalDDL(ClientContext &context, const vector<Value> &parameters, const string &metadata_catalog,
                              const string &metadata_schema) {
	string sql;
	idx_t logical_statement_count = 0;
	auto append_statement = [&](const string &statement, bool logical_statement = true) {
		if (statement.empty()) {
			return;
		}
		sql += statement;
		if (statement.back() != ';') {
			sql += ";";
		}
		sql += "\n";
		if (logical_statement) {
			logical_statement_count++;
		}
	};
	bool profile_enabled = SqlUtils::GetBoolSetting(context, "openivm_profile_refresh", false);
	vector<ProfileOperation> profile_rows;
	// The open marker step and the statement count when it opened.
	unique_ptr<pair<ProfileOperation, idx_t>> pending_profile;
	auto finish_profile_marker = [&]() {
		if (!pending_profile) {
			return;
		}
		auto row = std::move(pending_profile->first);
		if (!row.detail.empty()) {
			row.detail += "; ";
		}
		row.detail += "statements=" + to_string(logical_statement_count - pending_profile->second) +
		              "; transactional_program=true; duration_not_measured=true";
		profile_rows.push_back(std::move(row));
		pending_profile.reset();
	};
	for (auto &parameter : parameters) {
		auto statement = parameter.GetValue<string>();
		if (statement.empty() || StringUtil::StartsWith(statement, OPENIVM_DDL_CLEANUP_PREFIX) ||
		    StringUtil::StartsWith(statement, OPENIVM_DDL_PROFILE_TABLE_PREFIX)) {
			continue;
		}
		ProfileOperation profile_op;
		if (TryParseProfileOperation(statement, profile_op)) {
			if (profile_enabled && profile_op.measured) {
				profile_rows.push_back(std::move(profile_op));
			} else if (profile_enabled) {
				finish_profile_marker();
				pending_profile =
				    make_uniq<pair<ProfileOperation, idx_t>>(std::move(profile_op), logical_statement_count);
			}
			continue;
		}
		auto rendered = RenderTransactionalStatement(statement, metadata_catalog, metadata_schema);
		for (idx_t i = 0; i < rendered.size(); i++) {
			// Delta-schema defaults are part of their CREATE TABLE step.
			append_statement(rendered[i], i == 0);
		}
	}
	if (profile_enabled) {
		finish_profile_marker();
		if (!profile_rows.empty()) {
			auto view_name = profile_rows.front().view_name;
			ProfileOperation total;
			total.view_name = view_name;
			total.step_name = "create_mv_total";
			total.detail = "transactional_program=true; duration_not_measured=true";
			profile_rows.push_back(std::move(total));
			auto now = std::chrono::steady_clock::now().time_since_epoch();
			auto refresh_id = view_name + "_create_tx_" +
			                  to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
			// The lifecycle program restores the caller search path before these writes.
			auto profile_table = SqlUtils::FullName(metadata_catalog, metadata_schema, openivm::PROFILE_TABLE);
			OPENIVM_DEBUG_PRINT("[PROFILE] Recording CREATE MV profile in %s\n", profile_table.c_str());
			for (idx_t step_order = 0; step_order < profile_rows.size(); step_order++) {
				auto &row = profile_rows[step_order];
				append_statement("INSERT INTO " + profile_table +
				                     " (refresh_id, view_name, profile_timestamp, step_order, step_name, duration_ms, "
				                     "detail) VALUES ('" +
				                     SqlUtils::EscapeValue(refresh_id) + "', '" + SqlUtils::EscapeValue(row.view_name) +
				                     "', CAST(current_timestamp AS TIMESTAMP), " + to_string(step_order) + ", '" +
				                     SqlUtils::EscapeValue(row.step_name) + "', " + to_string(row.duration_ms) + ", '" +
				                     SqlUtils::EscapeValue(row.detail) + "')",
				                 false);
			}
		}
	}
	append_statement("SELECT true AS \"MATERIALIZED VIEW CREATION\"");
	return sql;
}

void ExecuteStagedDDL(ClientContext &context, const vector<Value> &parameters) {
	vector<string> ddl;
	ddl.reserve(parameters.size());
	for (auto &parameter : parameters) {
		ddl.push_back(parameter.GetValue<string>());
	}
	ExecuteDDL(context, ddl);
}

void ConfigureDDLExecutorResult(ParserExtensionPlanResult &result, DDLExecutionMode mode) {
	result.function = TableFunction("openivm_ddl_executor", {}, DDLExecutorExecuteFunction, DDLExecutorBindFunction,
	                                DDLExecutorInitFunction);
	result.function.name =
	    mode == DDLExecutionMode::CALLER_TRANSACTION ? OPENIVM_TRANSACTIONAL_DDL_FUNCTION : OPENIVM_STAGED_DDL_FUNCTION;
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;
}

} // namespace duckdb
