#include "core/parser_ddl.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/drop_statement.hpp"
#include "duckdb/planner/binder.hpp"

#include <chrono>
#include <cstring>
#include <unordered_set>

namespace duckdb {

namespace {

static const vector<string> &TransactionalMetadataTables() {
	static const vector<string> tables = {openivm::VIEWS_TABLE,   "openivm_refresh_hooks", openivm::DELTA_TABLES_TABLE,
	                                      openivm::HISTORY_TABLE, openivm::PROFILE_TABLE,  openivm::MV_DEPS_TABLE};
	return tables;
}

static bool ReferencesTransactionalMetadata(const string &statement) {
	auto lower = StringUtil::Lower(statement);
	for (auto &table : TransactionalMetadataTables()) {
		if (lower.find(StringUtil::Lower(table)) != string::npos) {
			return true;
		}
	}
	return false;
}

static bool IsReplayableMetadataStatement(const string &statement) {
	auto trimmed = statement;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	return ReferencesTransactionalMetadata(lower) &&
	       (StringUtil::StartsWith(lower, "create table") || StringUtil::StartsWith(lower, "alter table") ||
	        StringUtil::StartsWith(lower, "insert ") || StringUtil::StartsWith(lower, "update ") ||
	        StringUtil::StartsWith(lower, "delete "));
}

static string MakeTemporaryMetadataDDL(const string &statement) {
	auto lower = StringUtil::Lower(statement);
	const string prefix = "create table if not exists ";
	if (!StringUtil::StartsWith(lower, prefix)) {
		return statement;
	}
	return "create temp table if not exists " + statement.substr(prefix.size());
}

static bool IsMetadataSchemaStatement(const string &statement) {
	auto trimmed = statement;
	StringUtil::Trim(trimmed);
	auto lower = StringUtil::Lower(trimmed);
	return StringUtil::StartsWith(lower, "create table") || StringUtil::StartsWith(lower, "alter table");
}

static string StabilizeTransactionalMetadata(string statement, const string &timestamp_sql) {
	statement = StringUtil::Replace(statement, "now()", timestamp_sql);
	statement = StringUtil::Replace(statement, "NOW()", timestamp_sql);
	return statement;
}

struct CreateMVProfileStep {
	int32_t step_order;
	string step_name;
	int64_t duration_ms;
	string detail;
};

class CreateMVProfiler {
public:
	explicit CreateMVProfiler(ClientContext &context)
	    : enabled(false), retention_days(31), next_step(0), total_start(std::chrono::steady_clock::now()) {
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

	void Flush(DatabaseInstance &db) {
		if (!enabled || flushed || view_name.empty() || steps.empty()) {
			return;
		}
		flushed = true;
		Connection profile_con(db);
		profile_con.Query("DELETE FROM " + string(openivm::PROFILE_TABLE) +
		                  " WHERE profile_timestamp < current_timestamp::TIMESTAMP - INTERVAL '" +
		                  to_string(retention_days) + " days'");
		for (auto &step : steps) {
			auto result = profile_con.Query(
			    "INSERT OR REPLACE INTO " + string(openivm::PROFILE_TABLE) +
			    " (refresh_id, view_name, step_order, step_name, duration_ms, detail) VALUES ('" +
			    SqlUtils::EscapeValue(refresh_id) + "', '" + SqlUtils::EscapeValue(view_name) + "', " +
			    to_string(step.step_order) + ", '" + SqlUtils::EscapeValue(step.step_name) + "', " +
			    to_string(step.duration_ms) + ", '" + SqlUtils::EscapeValue(step.detail) + "')");
			if (result->HasError()) {
				OPENIVM_DEBUG_PRINT("[PROFILE] Failed to record CREATE MV step '%s': %s\n", step.step_name.c_str(),
				                    result->GetError().c_str());
				return;
			}
		}
	}

private:
	bool enabled;
	bool flushed = false;
	int64_t retention_days;
	string view_name;
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

void ExecuteDDL(ClientContext &context, const vector<string> &ddl) {
	if (ddl.empty()) {
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	auto conn = make_uniq<Connection>(db);
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
		profiler.Flush(db);
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
		auto r = conn->Query(query);
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
			auto r = conn->Query(derived.sql);
			profiler.AddStep(current_profile_step, ddl_start,
			                 current_profile_detail + "; statements=1; bytes=" + to_string(derived.sql.size()) +
			                     "; derived_from_data_schema=true; columns=" + to_string(derived.column_count));
			if (r->HasError()) {
				fail_ddl(r->GetError());
			}
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_CLEANUP_PREFIX)) {
			cleanup_ddl.push_back(q.substr(strlen(OPENIVM_DDL_CLEANUP_PREFIX)));
			continue;
		}
		pending_ddl.push_back(q);
	}
	flush_pending();
	profiler.AddTotal();
	profiler.Flush(db);
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

TransactionalMVMetadataState &TransactionalMVMetadataState::Get(ClientContext &context) {
	return *context.registered_state->GetOrCreate<TransactionalMVMetadataState>("openivm_transactional_mv_metadata");
}

optional_ptr<TransactionalMVMetadataState> TransactionalMVMetadataState::TryGet(ClientContext &context) {
	return context.registered_state->Get<TransactionalMVMetadataState>("openivm_transactional_mv_metadata");
}

void TransactionalMVMetadataState::Register(ClientContext &context, const vector<Value> &parameters) {
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
			statements.push_back(StabilizeTransactionalMetadata(std::move(statement), timestamp_sql));
		}
	}
}

void TransactionalMVMetadataState::RegisterSQL(const string &sql) {
	for (auto &statement : StringUtil::Split(sql, ";\n")) {
		if (IsReplayableMetadataStatement(statement)) {
			statements.push_back(std::move(statement));
		}
	}
}

void TransactionalMVMetadataState::Apply(Connection &connection) const {
	if (statements.empty()) {
		return;
	}
	// Build constrained TEMP schemas first. CREATE TABLE AS would discard the
	// primary keys required by INSERT OR REPLACE metadata operations.
	for (auto &statement : statements) {
		if (!IsMetadataSchemaStatement(statement)) {
			continue;
		}
		auto result = connection.Query(MakeTemporaryMetadataDDL(statement));
		if (result->HasError()) {
			throw CatalogException("Could not reconstruct transaction-local OpenIVM metadata schema: %s",
			                       result->GetError());
		}
	}
	for (auto &table : TransactionalMetadataTables()) {
		bool has_constrained_schema = false;
		for (auto &statement : statements) {
			auto lower = StringUtil::Lower(statement);
			if (StringUtil::StartsWith(lower, "create table") && lower.find(StringUtil::Lower(table)) != string::npos) {
				has_constrained_schema = true;
				break;
			}
		}
		if (has_constrained_schema) {
			// Seed the constrained shadow with committed rows. A missing durable
			// table is expected for the first MV in a database.
			connection.Query("INSERT OR IGNORE INTO " + table + " BY NAME SELECT * FROM main." + table);
		} else {
			// Less central metadata (for example optional dependency state) may
			// be initialized outside the lifecycle program. Still shadow it so
			// replay can never mutate the durable helper-connection catalog.
			connection.Query("CREATE TEMP TABLE IF NOT EXISTS " + table + " AS SELECT * FROM main." + table);
		}
	}
	for (auto &statement : statements) {
		if (IsMetadataSchemaStatement(statement)) {
			continue;
		}
		auto result = connection.Query(MakeTemporaryMetadataDDL(statement));
		if (result->HasError()) {
			throw CatalogException("Could not reconstruct transaction-local OpenIVM metadata: %s", result->GetError());
		}
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
}

string BuildCreateDeltaFromDataOperation(const string &delta_table, const string &data_table, bool replace) {
	return string(OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX) + (replace ? "replace\t" : "create\t") + delta_table +
	       "\t" + data_table;
}

string BuildDropViewStatement(const DropInfo &drop_info) {
	return "SELECT * FROM openivm_execute_drop_view('" + SqlUtils::EscapeValue(drop_info.catalog) + "', '" +
	       SqlUtils::EscapeValue(drop_info.schema) + "', '" + SqlUtils::EscapeValue(drop_info.name) + "', " +
	       (drop_info.cascade ? "true" : "false") + ", " +
	       (drop_info.if_not_found == OnEntryNotFound::RETURN_NULL ? "true" : "false") + ")";
}

struct DropViewBindData : public TableFunctionData {
	DropInfo info;
};

struct DropViewGlobalState : public GlobalTableFunctionState {
	bool finished = false;
};

unique_ptr<FunctionData> BindDropView(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 5) {
		throw InternalException("OpenIVM DROP VIEW executor expected five arguments");
	}
	auto result = make_uniq<DropViewBindData>();
	result->info.type = CatalogType::VIEW_ENTRY;
	result->info.catalog = StringValue::Get(input.inputs[0]);
	result->info.schema = StringValue::Get(input.inputs[1]);
	result->info.name = StringValue::Get(input.inputs[2]);
	result->info.cascade = BooleanValue::Get(input.inputs[3]);
	result->info.if_not_found =
	    BooleanValue::Get(input.inputs[4]) ? OnEntryNotFound::RETURN_NULL : OnEntryNotFound::THROW_EXCEPTION;
	if (!input.binder) {
		throw InternalException("OpenIVM DROP VIEW executor requires a binder");
	}
	auto &catalog = Catalog::GetCatalog(context, result->info.catalog);
	input.binder->GetStatementProperties().RegisterDBModify(catalog, context,
	                                                        DatabaseModificationType::DROP_CATALOG_ENTRY);
	return_types.push_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");
	return std::move(result);
}

unique_ptr<GlobalTableFunctionState> InitDropView(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DropViewGlobalState>();
}

void ExecuteDropView(ClientContext &context, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<DropViewGlobalState>();
	if (state.finished) {
		return;
	}
	auto &bind_data = input.bind_data->Cast<DropViewBindData>();
	auto &catalog = Catalog::GetCatalog(context, bind_data.info.catalog);
	DropInfo info(bind_data.info);
	catalog.DropEntry(context, info);
	output.SetValue(0, 0, Value::BOOLEAN(true));
	output.SetCardinality(1);
	state.finished = true;
}

string RenderTransactionalDDL(ClientContext &context, const vector<Value> &parameters) {
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
		if (statement.empty() || StringUtil::StartsWith(statement, OPENIVM_DDL_CLEANUP_PREFIX)) {
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

		try {
			Parser parser;
			parser.ParseQuery(statement);
			if (parser.statements.size() == 1 && parser.statements[0]->type == StatementType::DROP_STATEMENT) {
				auto &drop = parser.statements[0]->Cast<DropStatement>();
				if (drop.info->type == CatalogType::VIEW_ENTRY) {
					append_statement(BuildDropViewStatement(*drop.info));
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
			for (idx_t step_order = 0; step_order < profile_rows.size(); step_order++) {
				auto &row = profile_rows[step_order];
				append_statement("INSERT OR REPLACE INTO " + string(openivm::PROFILE_TABLE) +
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
