#include "core/parser_ddl.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/function/table_function.hpp"

#include <chrono>
#include <cstring>
#include <unordered_set>

namespace duckdb {

namespace {

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

void ParseCreateDeltaFromDataPayload(const string &payload, string &delta_table, string &data_table) {
	auto first = payload.find('\t');
	if (first == string::npos) {
		return;
	}
	delta_table = payload.substr(0, first);
	data_table = payload.substr(first + 1);
}

DeltaSchemaDDL BuildCreateDeltaFromDataSQL(Connection &conn, const string &delta_table, const string &data_table) {
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
	result.sql = "create table if not exists " + delta_table + " (" + StringUtil::Join(columns, ", ") + ")";
	result.column_count = described->RowCount();
	return result;
}

void ExecuteDDL(ClientContext &context, const vector<string> &ddl) {
	if (ddl.empty()) {
		return;
	}
	auto &db = DatabaseInstance::GetDatabase(context);
	// This fresh connection executes all of the MV's initial-load / metadata DDL (including the
	// `CREATE TABLE openivm_data_* AS <view query>` that runs the full view, e.g. an ASOF join).
	// TODO(propagate-settings): it starts from DB defaults and only inherits a couple of options
	// we explicitly re-apply below (preserve_insertion_order, max_expression_depth). Session-scoped
	// settings the user set on the originating `context` are NOT propagated here. We should mirror
	// the relevant ClientContext settings onto this connection (e.g. threads — though `threads` is
	// global-only today — memory_limit, and other execution-affecting options) so the initial-load
	// query runs under the same configuration as the user's session. Related: TODO(asof-deadlock)
	// in test/sql/auto_refresh_asof.test.
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
	auto expression_depth_result = conn->Query("SET max_expression_depth TO 10000");
	if (expression_depth_result->HasError()) {
		throw CatalogException("Failed to configure OpenIVM DDL expression depth: " +
		                       expression_depth_result->GetError());
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
			string delta_table;
			string data_table;
			ParseCreateDeltaFromDataPayload(q.substr(strlen(OPENIVM_DDL_CREATE_DELTA_FROM_DATA_PREFIX)), delta_table,
			                                data_table);
			if (delta_table.empty() || data_table.empty()) {
				fail_ddl("malformed delta-schema payload");
			}
			auto ddl_start = std::chrono::steady_clock::now();
			DeltaSchemaDDL derived;
			try {
				derived = BuildCreateDeltaFromDataSQL(*conn, delta_table, data_table);
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

void ConfigureDDLExecutorResult(ParserExtensionPlanResult &result) {
	result.function = TableFunction("openivm_ddl_executor", {}, DDLExecutorExecuteFunction, DDLExecutorBindFunction,
	                                DDLExecutorInitFunction);
	result.requires_valid_transaction = true;
	result.return_type = StatementReturnType::QUERY_RESULT;
}

} // namespace duckdb
