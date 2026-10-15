#include "core/parser_ddl.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/published_view.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/function/table_function.hpp"
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
#include "duckdb/planner/operator/logical_simple.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
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

// Read only the mutation target, never a metadata name in a query or literal.
static string MetadataTargetReference(const string &statement, const string &prefix, const string &table) {
	if (!StringUtil::CIStartsWith(statement, prefix)) {
		return "";
	}
	auto target = statement.substr(prefix.size());
	auto reference = SqlUtils::FindTableReference("FROM " + target, table);
	if (reference.empty() || !StringUtil::StartsWith(target, reference) ||
	    !StringUtil::CIEquals(SqlUtils::ParseQualifiedIdentifier(reference).back(), table)) {
		return "";
	}
	return reference;
}

static bool HasMetadataTarget(const string &statement, const string &prefix, const string &table) {
	return !MetadataTargetReference(statement, prefix, table).empty();
}

static bool TargetsMetadataTable(const string &statement, const string &table) {
	auto trimmed = statement;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	for (auto &prefix : MetadataStatementPrefixes()) {
		if (HasMetadataTarget(lower, prefix, table)) {
			return true;
		}
	}
	return false;
}

static bool IsReplayableMetadataStatement(const string &statement) {
	auto trimmed = statement;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	for (auto &prefix : MetadataStatementPrefixes()) {
		for (auto &table : TransactionalMetadataTables()) {
			if (HasMetadataTarget(lower, prefix, table)) {
				return true;
			}
		}
	}
	return false;
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
	bool metadata_target = false;
	for (auto &table : MetadataTableNames()) {
		if (TargetsMetadataTable(statement, table)) {
			metadata_target = true;
			break;
		}
	}
	if (!metadata_target) {
		return statement;
	}
	auto qualified = statement;
	for (auto &table : MetadataTableNames()) {
		auto target = SqlUtils::FullName(catalog, schema, table);
		auto trimmed = qualified;
		StringUtil::Trim(trimmed);
		for (const string prefix : {"create table if not exists ", "alter table "}) {
			auto reference = MetadataTargetReference(trimmed, prefix, table);
			if (!reference.empty() && SqlUtils::ParseQualifiedIdentifier(reference).size() == 1) {
				trimmed = trimmed.substr(0, prefix.size()) + target + trimmed.substr(prefix.size() + reference.size());
				qualified = trimmed;
				break;
			}
		}
		// Only unqualified references mean "this metadata location". Qualified ones, such as
		// consumer checks against other catalogs' openivm_delta_tables, keep their identity.
		for (idx_t occurrence = 0;; occurrence++) {
			auto reference = SqlUtils::FindTableReferenceOccurrence(qualified, table, occurrence);
			if (reference.empty()) {
				break;
			}
			if (reference.find('.') != string::npos) {
				continue;
			}
			bool replaced = false;
			qualified = SqlUtils::ReplaceTableReferenceOccurrence(qualified, table, occurrence, target, replaced);
		}
	}
	return qualified;
}

// Retained SQL keeps its physical metadata identity. Only the selected location
// may be redirected to helper-local shadows; other catalogs remain qualified.
static string LocalizeMetadataStatement(const string &statement, const string &catalog, const string &schema) {
	auto local = statement;
	for (auto &table : TransactionalMetadataTables()) {
		for (auto &prefix : MetadataStatementPrefixes()) {
			auto reference = MetadataTargetReference(statement, prefix, table);
			if (reference.empty()) {
				continue;
			}
			auto parts = SqlUtils::ParseQualifiedIdentifier(reference);
			if (parts.size() != 3 || !StringUtil::CIEquals(parts[0], catalog) ||
			    !StringUtil::CIEquals(parts[1], schema)) {
				return "";
			}
			local.replace(prefix.size(), reference.size(), table);
		}
		for (idx_t occurrence = 0;; occurrence++) {
			auto reference = SqlUtils::FindTableReferenceOccurrence(local, table, occurrence);
			if (reference.empty()) {
				break;
			}
			auto parts = SqlUtils::ParseQualifiedIdentifier(reference);
			if (parts.size() == 3 && StringUtil::CIEquals(parts[0], catalog) &&
			    StringUtil::CIEquals(parts[1], schema)) {
				bool replaced = false;
				local = SqlUtils::ReplaceTableReferenceOccurrence(local, table, occurrence, table, replaced);
			}
		}
	}
	return local;
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

struct CreateMVProfileStep {
	int32_t step_order;
	string step_name;
	int64_t duration_ms;
	string detail;
};

class CreateMVProfiler {
public:
	explicit CreateMVProfiler(ClientContext &context_p)
	    : context(context_p), enabled(false), retention_days(31), next_step(0),
	      total_start(std::chrono::steady_clock::now()) {
		Value profile_val;
		enabled = context.TryGetCurrentSetting("openivm_profile_refresh", profile_val) && !profile_val.IsNull() &&
		          profile_val.GetValue<bool>();
		if (!enabled) {
			return;
		}
		Value retention_val;
		if (context.TryGetCurrentSetting("openivm_profile_retention_days", retention_val) && !retention_val.IsNull()) {
			retention_days = std::max<int64_t>(0, retention_val.GetValue<int64_t>());
		}
		auto now = std::chrono::steady_clock::now().time_since_epoch();
		refresh_id = "create_mv_" + to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
	}

	bool Enabled() const {
		return enabled;
	}

	void SetProfileTable(const string &profile_table_p) {
		profile_table = profile_table_p;
	}

	void SetViewName(const string &view_name_p) {
		if (view_name.empty()) {
			view_name = view_name_p;
			refresh_id = view_name + "_" + refresh_id;
		}
	}

	void AddStep(const string &step_name, std::chrono::steady_clock::time_point start,
	             const string &detail = string()) {
		if (!enabled) {
			return;
		}
		auto duration_ms =
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
		steps.push_back({next_step++, step_name, duration_ms, detail});
	}

	void AddMeasuredStep(const string &step_name, int64_t duration_ms, const string &detail = string()) {
		if (!enabled) {
			return;
		}
		steps.push_back({next_step++, step_name, duration_ms, detail});
	}

	void AddTotal() {
		AddStep("create_mv_total", total_start);
	}

	void Flush() {
		if (!enabled || flushed || view_name.empty() || profile_table.empty() || steps.empty()) {
			return;
		}
		flushed = true;
		Connection profile_con(*context.db);
		TransactionalMVLockState::Get(*profile_con.context)
		    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
		// The CREATE planner resolved the metadata owner before staged execution.
		// The caller's active catalog may instead be an unrelated attached database.
		OPENIVM_DEBUG_PRINT("[PROFILE] Recording CREATE MV profile in %s\n", profile_table.c_str());
		// Retention and the new profile share one durable metadata commit.
		profile_con.BeginTransaction();
		profile_con.Query("DELETE FROM " + profile_table +
		                  " WHERE profile_timestamp < current_timestamp::TIMESTAMP - INTERVAL '" +
		                  to_string(retention_days) + " days'");
		// Statement profiles should not add a metadata commit for every DDL operation.
		string values;
		for (auto &step : steps) {
			if (!values.empty()) {
				values += ", ";
			}
			values += "('" + SqlUtils::EscapeValue(refresh_id) + "', '" + SqlUtils::EscapeValue(view_name) + "', " +
			          to_string(step.step_order) + ", '" + SqlUtils::EscapeValue(step.step_name) + "', " +
			          to_string(step.duration_ms) + ", '" + SqlUtils::EscapeValue(step.detail) + "')";
		}
		auto result =
		    profile_con.Query("INSERT OR REPLACE INTO " + profile_table +
		                      " (refresh_id, view_name, step_order, step_name, duration_ms, detail) VALUES " + values);
		if (result->HasError()) {
			OPENIVM_DEBUG_PRINT("[PROFILE] Failed to record CREATE MV profile: %s\n", result->GetError().c_str());
			return;
		}
		auto committed = profile_con.Query("COMMIT");
		if (committed->HasError()) {
			OPENIVM_DEBUG_PRINT("[PROFILE] Failed to commit CREATE MV profile: %s\n", committed->GetError().c_str());
		}
	}

private:
	ClientContext &context;
	bool enabled;
	bool flushed = false;
	int64_t retention_days;
	string view_name;
	string profile_table;
	string refresh_id;
	int32_t next_step;
	std::chrono::steady_clock::time_point total_start;
	vector<CreateMVProfileStep> steps;
};

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

void ParseCreateMVProfileMarker(const string &payload, string &view_name, string &step_name, string &detail) {
	auto first = payload.find('\t');
	auto second = first == string::npos ? string::npos : payload.find('\t', first + 1);
	if (first == string::npos || second == string::npos) {
		step_name = "create_mv_unclassified";
		detail = payload;
		return;
	}
	view_name = payload.substr(0, first);
	step_name = payload.substr(first + 1, second - first - 1);
	detail = payload.substr(second + 1);
}

void ParseCreateMVProfileRecord(const string &payload, string &view_name, string &step_name, int64_t &duration_ms,
                                string &detail) {
	auto first = payload.find('\t');
	auto second = first == string::npos ? string::npos : payload.find('\t', first + 1);
	auto third = second == string::npos ? string::npos : payload.find('\t', second + 1);
	if (first == string::npos || second == string::npos || third == string::npos) {
		step_name = "create_mv_unclassified";
		duration_ms = 0;
		detail = payload;
		return;
	}
	view_name = payload.substr(0, first);
	step_name = payload.substr(first + 1, second - first - 1);
	try {
		duration_ms = std::stoll(payload.substr(second + 1, third - second - 1));
	} catch (...) {
		duration_ms = 0;
	}
	detail = payload.substr(third + 1);
}

struct DeltaSchemaDDL {
	string sql;
	idx_t column_count = 0;
};

void ParseCreateDeltaFromDataPayload(const string &payload, bool &replace, string &delta_table, string &data_table) {
	auto first = payload.find('\t');
	if (first == string::npos) {
		return;
	}
	auto second = payload.find('\t', first + 1);
	if (second == string::npos) {
		return;
	}
	auto mode = payload.substr(0, first);
	if (mode != "create" && mode != "replace") {
		return;
	}
	replace = mode == "replace";
	delta_table = payload.substr(first + 1, second - first - 1);
	data_table = payload.substr(second + 1);
}

DeltaSchemaDDL BuildCreateDeltaFromDataSQL(Connection &conn, const string &delta_table, const string &data_table,
                                           bool replace) {
	auto described = conn.Query("DESCRIBE SELECT * FROM " + data_table);
	if (described->HasError()) {
		throw CatalogException("Could not derive IVM delta schema from data table '" + data_table +
		                       "': " + described->GetError());
	}
	if (described->RowCount() == 0) {
		throw CatalogException("Could not derive IVM delta schema from empty column list for data table '" +
		                       data_table + "'");
	}

	vector<string> columns;
	unordered_set<string> seen_column_names;
	for (idx_t row = 0; row < described->RowCount(); row++) {
		if (described->GetValue(0, row).IsNull() || described->GetValue(1, row).IsNull()) {
			throw CatalogException("Could not derive IVM delta schema from data table '" + data_table +
			                       "': DESCRIBE returned a NULL column name or type");
		}
		auto column_name = described->GetValue(0, row).ToString();
		auto column_type = described->GetValue(1, row).ToString();
		if (column_name.empty() || column_type.empty()) {
			throw CatalogException("Could not derive IVM delta schema from data table '" + data_table +
			                       "': DESCRIBE returned an empty column name or type");
		}
		auto column_name_lc = StringUtil::Lower(column_name);
		if (!seen_column_names.insert(column_name_lc).second) {
			throw CatalogException("Could not derive IVM delta schema from data table '" + data_table +
			                       "': duplicate column '" + column_name + "'");
		}
		if (StringUtil::CIEquals(column_name, openivm::MULTIPLICITY_COL) ||
		    StringUtil::CIEquals(column_name, openivm::TIMESTAMP_COL)) {
			throw CatalogException("Could not derive IVM delta schema from data table '" + data_table +
			                       "': reserved OpenIVM column '" + column_name + "' is already present");
		}
		columns.push_back(SqlUtils::QuoteIdentifier(column_name) + " " + column_type);
	}

	columns.push_back(string(openivm::MULTIPLICITY_COL) + " INTEGER DEFAULT 1");
	columns.push_back(string(openivm::TIMESTAMP_COL) + " TIMESTAMP DEFAULT now()");
	DeltaSchemaDDL result;
	result.sql = string(replace ? "create or replace table " : "create table if not exists ") + delta_table + " (" +
	             StringUtil::Join(columns, ", ") + ")";
	result.column_count = described->RowCount();
	return result;
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
	CreateMVProfiler profiler(context);
	string current_profile_step = "create_mv_unclassified";
	string current_profile_detail;
	vector<string> pending_ddl;
	auto preserve_result = conn->Query("SET preserve_insertion_order=false");
	if (preserve_result->HasError()) {
		throw CatalogException("Failed to configure OpenIVM DDL connection: " + preserve_result->GetError());
	}
	vector<string> cleanup_ddl;
	auto run_cleanup = [&]() {
		if (!conn->context->transaction.IsAutoCommit()) {
			conn->Rollback();
		}
		for (const auto &cleanup : cleanup_ddl) {
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
		unique_ptr<MaterializedQueryResult> r;
		if (profiler.Enabled()) {
			auto statements = SqlUtils::SplitSQLStatements(query);
			for (idx_t i = 0; i < statements.size(); i++) {
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
		pending_ddl.clear();
	};
	for (auto &q : ddl) {
		if (StringUtil::StartsWith(q, OPENIVM_DDL_PROFILE_TABLE_PREFIX)) {
			profiler.SetProfileTable(q.substr(strlen(OPENIVM_DDL_PROFILE_TABLE_PREFIX)));
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_SNAPSHOT_PUBLICATION_PREFIX)) {
			flush_pending();
			auto payload = q.substr(strlen(OPENIVM_DDL_SNAPSHOT_PUBLICATION_PREFIX));
			auto fields = StringUtil::Split(payload, '\t');
			if (fields.size() != 4) {
				fail_ddl("malformed snapshot-publication payload");
			}
			try {
				pending_ddl.push_back(BuildSnapshotPublicationSQL(*conn, fields[0], fields[1], fields[2], fields[3]));
			} catch (std::exception &ex) {
				fail_ddl(ex.what());
			}
			flush_pending();
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_PROFILE_RECORD_PREFIX)) {
			flush_pending();
			string marker_view_name;
			string measured_step_name;
			string measured_detail;
			int64_t measured_duration_ms = 0;
			ParseCreateMVProfileRecord(q.substr(strlen(OPENIVM_DDL_PROFILE_RECORD_PREFIX)), marker_view_name,
			                           measured_step_name, measured_duration_ms, measured_detail);
			if (!marker_view_name.empty()) {
				profiler.SetViewName(marker_view_name);
			}
			profiler.AddMeasuredStep(measured_step_name, measured_duration_ms, measured_detail);
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_PROFILE_PREFIX)) {
			flush_pending();
			string marker_view_name;
			ParseCreateMVProfileMarker(q.substr(strlen(OPENIVM_DDL_PROFILE_PREFIX)), marker_view_name,
			                           current_profile_step, current_profile_detail);
			if (!marker_view_name.empty()) {
				profiler.SetViewName(marker_view_name);
			}
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)) {
			flush_pending();
			bool replace = false;
			string delta_table;
			string data_table;
			ParseCreateDeltaFromDataPayload(q.substr(strlen(OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)), replace,
			                                delta_table, data_table);
			if (delta_table.empty() || data_table.empty()) {
				fail_ddl("malformed delta-schema payload");
			}
			auto ddl_start = std::chrono::steady_clock::now();
			DeltaSchemaDDL derived;
			try {
				derived = BuildCreateDeltaFromDataSQL(*conn, delta_table, data_table, replace);
			} catch (std::exception &ex) {
				profiler.AddStep(current_profile_step, ddl_start,
				                 current_profile_detail + "; delta_schema_derivation_failed=true");
				fail_ddl(ex.what());
			}
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
		pending_ddl.push_back(q);
	}
	flush_pending();
	profiler.AddTotal();
	profiler.Flush();
	restore_outer_transaction();
}

unique_ptr<FunctionData> DDLExecutorBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
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
	output.SetValue(0, 0, Value::BOOLEAN(bind_data.result));
	output.SetCardinality(1);
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
	transaction_timestamp -= 1;
	auto timestamp_sql = Value::TIMESTAMP(transaction_timestamp).ToSQLString() + "::TIMESTAMP";
	for (auto &parameter : parameters) {
		auto statement = parameter.GetValue<string>();
		if (IsReplayableMetadataStatement(statement)) {
			// The caller records the evaluated watermark at execution time. A
			// helper cannot read source deltas created in the caller transaction.
			auto target = MetadataTargetReference(statement, "update ", openivm::DELTA_TABLES_TABLE);
			if (!target.empty() && StringUtil::StartsWith(statement.substr(strlen("update ") + target.size()),
			                                              " SET last_update = " +
			                                                  string(OPENIVM_RECORD_SOURCE_WATERMARK_FUNCTION) + "(")) {
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
		for (auto &table : TransactionalMetadataTables()) {
			for (auto &prefix : MetadataStatementPrefixes()) {
				auto reference = MetadataTargetReference(statement, prefix, table);
				if (reference.empty()) {
					continue;
				}
				auto parts = SqlUtils::ParseQualifiedIdentifier(reference);
				if (parts.size() != 3) {
					throw InternalException("Transaction-local OpenIVM metadata target must be qualified");
				}
				pair<string, string> location {parts[0], parts[1]};
				if (std::find(locations.begin(), locations.end(), location) == locations.end()) {
					locations.push_back(std::move(location));
				}
			}
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
		auto local =
		    LocalizeMetadataStatement(statement, metadata_location.catalog_name, metadata_location.schema_name);
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
			drop.catalog = "temp";
			drop.schema = DEFAULT_SCHEMA;
			drop.name = table;
			drop.if_not_found = OnEntryNotFound::RETURN_NULL;
			Catalog::GetCatalog(*connection.context, drop.catalog).DropEntry(*connection.context, drop);
		}
	});
	OPENIVM_DEBUG_PRINT("[TRANSACTIONAL METADATA] Replay catalog='%s', schema='%s', statements=%zu\n",
	                    metadata_location.catalog_name.c_str(), metadata_location.schema_name.c_str(),
	                    local_statements.size());
	auto durable_table = [&](const string &table) {
		return SqlUtils::FullName(metadata_location.catalog_name, metadata_location.schema_name, table);
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
		for (auto &table : dependency_tables) {
			if (TargetsMetadataTable(statement, table)) {
				replay_statement(statement);
				break;
			}
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
			add_dependency(dependencies->GetValue(0, row).ToString(), dependencies->GetValue(1, row).ToString());
		}
	}
	unordered_set<string> registered_views;
	auto views = connection.Query("SELECT view_name FROM " + string(openivm::VIEWS_TABLE));
	if (!views->HasError()) {
		for (idx_t row = 0; row < views->RowCount(); row++) {
			registered_views.insert(views->GetValue(0, row).ToString());
		}
	}
	auto delta_dependencies =
	    connection.Query("SELECT view_name, table_name FROM " + string(openivm::DELTA_TABLES_TABLE));
	if (!delta_dependencies->HasError()) {
		static const string delta_prefix(openivm::DELTA_PREFIX);
		static const string data_prefix(openivm::DATA_TABLE_PREFIX);
		for (idx_t row = 0; row < delta_dependencies->RowCount(); row++) {
			auto child = delta_dependencies->GetValue(0, row).ToString();
			auto table = delta_dependencies->GetValue(1, row).ToString();
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
	string view_filter;
	vector<string> sorted_included_views(included_views.begin(), included_views.end());
	std::sort(sorted_included_views.begin(), sorted_included_views.end());
	for (auto &view_name : sorted_included_views) {
		if (!view_filter.empty()) {
			view_filter += ", ";
		}
		view_filter += "'" + SqlUtils::EscapeValue(view_name) + "'";
	}
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
		bool already_replayed = false;
		for (auto &table : dependency_tables) {
			if (TargetsMetadataTable(statement, table)) {
				already_replayed = true;
				break;
			}
		}
		if (already_replayed) {
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

string BuildCreateDeltaFromDataOperation(const string &delta_table, const string &data_table, bool replace) {
	return string(OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX) + (replace ? "replace\t" : "create\t") + delta_table +
	       "\t" + data_table;
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
	                      LogicalType::TIMESTAMP, RecordSourceWatermark, nullptr, nullptr, nullptr, nullptr,
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
		settings.swap(config.profiler_settings);
	}

	~LifecycleMetricsScope() {
		settings.swap(config.profiler_settings);
	}

private:
	ClientConfig &config;
	profiler_settings_t settings;
};

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
	PhysicalLifecycleProgram(PhysicalPlan &physical_plan, string program)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, {LogicalType::BOOLEAN}, 1),
	      program(std::move(program)) {
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
		auto &executor = Executor::Get(client);
		state.metrics = make_uniq<LifecycleMetricsScope>(client);
		try {
			ParserOptions options = client.GetParserOptions();
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
					auto &database = Catalog::GetCatalog(client, entry.first).GetAttached();
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
			ValidChecker::Invalidate(client.ActiveTransaction(), ex.what());
			throw;
		}
		state.metrics.reset();
		chunk.SetValue(0, 0, Value::BOOLEAN(true));
		chunk.SetCardinality(1);
		state.finished = true;
		return SourceResultType::FINISHED;
	}

	vector<const_reference<PhysicalOperator>> GetSources() const override {
		return {*this};
	}

private:
	const string program;
};

class LogicalLifecycleProgram : public LogicalExtensionOperator {
public:
	LogicalLifecycleProgram(idx_t table_index, string program) : table_index(table_index), program(std::move(program)) {
	}

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &planner) override {
		return planner.Make<PhysicalLifecycleProgram>(program);
	}

	vector<ColumnBinding> GetColumnBindings() override {
		return GenerateColumnBindings(table_index, 1);
	}

	vector<idx_t> GetTableIndex() const override {
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
	const idx_t table_index;
	const string program;
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

static string BuildDropEntryStatement(const DropInfo &drop_info, const string &function) {
	return "SELECT * FROM " + function + "('" + SqlUtils::EscapeValue(drop_info.catalog) + "', '" +
	       SqlUtils::EscapeValue(drop_info.schema) + "', '" + SqlUtils::EscapeValue(drop_info.name) + "', " +
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
                                              vector<LogicalType> &return_types, vector<string> &names,
                                              CatalogType type) {
	if (input.inputs.size() != 5) {
		throw InternalException("OpenIVM DROP executor expected five arguments");
	}
	auto result = make_uniq<DropViewBindData>();
	result->info.type = type;
	result->info.catalog = StringValue::Get(input.inputs[0]);
	result->info.schema = StringValue::Get(input.inputs[1]);
	result->info.name = StringValue::Get(input.inputs[2]);
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
	auto &resolved = bound.plan->Cast<LogicalSimple>().info->Cast<DropInfo>();
	result->info.catalog = resolved.catalog;
	result->info.schema = resolved.schema;
	result->info.name = resolved.name;
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

static unique_ptr<FunctionData> BindDropView(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	return BindDropEntry(context, input, return_types, names, CatalogType::VIEW_ENTRY);
}

static unique_ptr<FunctionData> BindDropTable(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	return BindDropEntry(context, input, return_types, names, CatalogType::TABLE_ENTRY);
}

static unique_ptr<FunctionData> BindDropSchema(ClientContext &context, TableFunctionBindInput &input,
                                               vector<LogicalType> &return_types, vector<string> &names) {
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
	auto &catalog = Catalog::GetCatalog(context, bind_data.info.catalog);
	DropInfo info(bind_data.info);
	bool source_delta_drop =
	    info.type == CatalogType::TABLE_ENTRY && SqlUtils::IsDelta(info.name) &&
	    Catalog::GetEntry(context, info.catalog, info.schema, EntryLookupInfo(CatalogType::TABLE_ENTRY, info.name),
	                      OnEntryNotFound::RETURN_NULL);
	vector<reference<TableCatalogEntry>> schema_tables;
	if (info.type == CatalogType::SCHEMA_ENTRY && info.cascade && catalog.IsDuckCatalog()) {
		auto schema = catalog.GetSchema(context, info.name, OnEntryNotFound::RETURN_NULL);
		if (schema) {
			schema->Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
				if (entry.type == CatalogType::TABLE_ENTRY) {
					schema_tables.push_back(entry.Cast<TableCatalogEntry>());
				}
			});
		}
	}
	catalog.DropEntry(context, info);
	// Native schema CASCADE drops through CatalogSet, bypassing the local-storage
	// cleanup in DuckSchemaEntry::DropEntry. Discard pending appends only after a
	// successful DROP, or commit would flush them into the removed table allocation.
	// Use the captured entries, not names that a later CREATE can reuse.
	for (auto &table : schema_tables) {
		LocalStorage::Get(context, catalog).DropTable(table.get().GetStorage());
	}
	if (!schema_tables.empty()) {
		OPENIVM_DEBUG_PRINT("[DROP] Discarded local storage for %zu schema tables in %s.%s\n", schema_tables.size(),
		                    catalog.GetName().c_str(), info.name.c_str());
	}
	if (info.type == CatalogType::SCHEMA_ENTRY) {
		// Match PhysicalDrop's search-path handling when dropping the current schema.
		auto &current = ClientData::Get(context).catalog_search_path->GetDefault();
		if (StringUtil::CIEquals(catalog.GetName(), current.catalog) &&
		    StringUtil::CIEquals(info.name, current.schema)) {
			SchemaSetting::SetLocal(context, DEFAULT_SCHEMA);
		}
	}
	if (source_delta_drop) {
		TransactionalMVLockState::Get(context).RecordSourceDeltaDrop(info.catalog, info.schema, info.name);
		OPENIVM_DEBUG_PRINT("[DROP] Recorded transaction-local source delta removal %s\n",
		                    SqlUtils::FullName(info.catalog, info.schema, info.name).c_str());
	}
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	state.finished = true;
}

static void SerializeDropEntry(Serializer &serializer, const optional_ptr<FunctionData> bind_data,
                               const TableFunction &function) {
	// LogicalGet's fallback deserializer calls bind without a Binder. Retain the
	// native binder's resolved target instead of resolving the search path again.
	serializer.WriteProperty(100, "drop_info", &bind_data->Cast<DropViewBindData>().info);
}

static unique_ptr<FunctionData> DeserializeDropEntry(Deserializer &deserializer, TableFunction &function) {
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
	struct ProfileRow {
		string view_name;
		string step_name;
		int64_t duration_ms;
		string detail;
	};
	struct PendingProfileMarker {
		string view_name;
		string step_name;
		string detail;
		idx_t statement_start;
	};

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
	Value profile_value;
	bool profile_enabled = context.TryGetCurrentSetting("openivm_profile_refresh", profile_value) &&
	                       !profile_value.IsNull() && BooleanValue::Get(profile_value);
	vector<ProfileRow> profile_rows;
	unique_ptr<PendingProfileMarker> pending_profile;
	auto finish_profile_marker = [&]() {
		if (!pending_profile) {
			return;
		}
		auto statement_count = logical_statement_count - pending_profile->statement_start;
		auto detail = pending_profile->detail;
		if (!detail.empty()) {
			detail += "; ";
		}
		detail +=
		    "statements=" + to_string(statement_count) + "; transactional_program=true; duration_not_measured=true";
		profile_rows.push_back({pending_profile->view_name, pending_profile->step_name, 0, std::move(detail)});
		pending_profile.reset();
	};
	for (auto &parameter : parameters) {
		auto statement = parameter.GetValue<string>();
		if (statement.empty() || StringUtil::StartsWith(statement, OPENIVM_DDL_CLEANUP_PREFIX) ||
		    StringUtil::StartsWith(statement, OPENIVM_DDL_PROFILE_TABLE_PREFIX)) {
			continue;
		}
		if (StringUtil::StartsWith(statement, OPENIVM_DDL_PROFILE_RECORD_PREFIX)) {
			if (profile_enabled) {
				string view_name;
				string step_name;
				string detail;
				int64_t duration_ms = 0;
				ParseCreateMVProfileRecord(statement.substr(strlen(OPENIVM_DDL_PROFILE_RECORD_PREFIX)), view_name,
				                           step_name, duration_ms, detail);
				profile_rows.push_back({std::move(view_name), std::move(step_name), duration_ms, std::move(detail)});
			}
			continue;
		}
		if (StringUtil::StartsWith(statement, OPENIVM_DDL_PROFILE_PREFIX)) {
			if (profile_enabled) {
				finish_profile_marker();
				string view_name;
				string step_name;
				string detail;
				ParseCreateMVProfileMarker(statement.substr(strlen(OPENIVM_DDL_PROFILE_PREFIX)), view_name, step_name,
				                           detail);
				pending_profile = make_uniq<PendingProfileMarker>(PendingProfileMarker {
				    std::move(view_name), std::move(step_name), std::move(detail), logical_statement_count});
			}
			continue;
		}
		if (StringUtil::StartsWith(statement, OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)) {
			bool replace = false;
			string delta_table;
			string data_table;
			ParseCreateDeltaFromDataPayload(statement.substr(strlen(OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)),
			                                replace, delta_table, data_table);
			if (delta_table.empty() || data_table.empty()) {
				throw InternalException("Malformed OpenIVM delta-schema operation");
			}
			append_statement(string(replace ? "CREATE OR REPLACE TABLE " : "CREATE TABLE ") + delta_table +
			                 " AS SELECT *, 1::INTEGER AS " + string(openivm::MULTIPLICITY_COL) +
			                 ", now()::TIMESTAMP AS " + string(openivm::TIMESTAMP_COL) + " FROM " + data_table +
			                 " LIMIT 0");
			append_statement(
			    "ALTER TABLE " + delta_table + " ALTER " + string(openivm::MULTIPLICITY_COL) + " SET DEFAULT 1", false);
			append_statement("ALTER TABLE " + delta_table + " ALTER " + string(openivm::TIMESTAMP_COL) +
			                     " SET DEFAULT now()",
			                 false);
			continue;
		}
		// The caller's search path already resolves the view query as the user wrote it;
		// metadata is reached through explicit qualification.
		if (IsSearchPathStatement(statement)) {
			continue;
		}
		statement = QualifyMetadataMutation(statement, metadata_catalog, metadata_schema);

		try {
			Parser parser;
			parser.ParseQuery(statement);
			if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::DROP_STATEMENT) {
				auto &drop = parser.statements[0]->Cast<DropStatement>();
				if (drop.info->type == CatalogType::VIEW_ENTRY) {
					append_statement(BuildDropViewStatement(*drop.info));
					continue;
				}
				if (drop.info->type == CatalogType::TABLE_ENTRY) {
					append_statement(BuildDropTableStatement(*drop.info));
					continue;
				}
				if (drop.info->type == CatalogType::SCHEMA_ENTRY) {
					append_statement(BuildDropSchemaStatement(*drop.info));
					continue;
				}
			}
		} catch (std::exception &) {
			// The normal DuckDB parser will produce the authoritative error when the
			// transactional program is executed.
		}
		append_statement(statement);
	}
	if (profile_enabled) {
		finish_profile_marker();
		if (!profile_rows.empty()) {
			auto view_name = profile_rows.front().view_name;
			profile_rows.push_back(
			    {view_name, "create_mv_total", 0, "transactional_program=true; duration_not_measured=true"});
			auto now = std::chrono::steady_clock::now().time_since_epoch();
			auto refresh_id = view_name + "_create_tx_" +
			                  to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
			// The lifecycle program restores the caller search path before these writes.
			auto profile_table = SqlUtils::FullName(metadata_catalog, metadata_schema, openivm::PROFILE_TABLE);
			OPENIVM_DEBUG_PRINT("[PROFILE] Recording CREATE MV profile in %s\n", profile_table.c_str());
			for (idx_t step_order = 0; step_order < profile_rows.size(); step_order++) {
				auto &row = profile_rows[step_order];
				append_statement("INSERT OR REPLACE INTO " + profile_table +
				                     " (refresh_id, view_name, step_order, step_name, duration_ms, detail) VALUES ('" +
				                     SqlUtils::EscapeValue(refresh_id) + "', '" + SqlUtils::EscapeValue(row.view_name) +
				                     "', " + to_string(step_order) + ", '" + SqlUtils::EscapeValue(row.step_name) +
				                     "', " + to_string(row.duration_ms) + ", '" + SqlUtils::EscapeValue(row.detail) +
				                     "')",
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
