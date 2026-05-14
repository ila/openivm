#include "core/parser_ddl.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/parser.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/common/printer.hpp"

#include <chrono>
#include <cstring>

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
	for (auto &q : ddl) {
		if (StringUtil::StartsWith(q, OPENIVM_DDL_PROFILE_PREFIX)) {
			string marker_view_name;
			ParseCreateMVProfileMarker(q.substr(strlen(OPENIVM_DDL_PROFILE_PREFIX)), marker_view_name,
			                           current_profile_step, current_profile_detail);
			if (!marker_view_name.empty()) {
				profiler.SetViewName(marker_view_name);
			}
			continue;
		}
		if (StringUtil::StartsWith(q, OPENIVM_DDL_CLEANUP_PREFIX)) {
			cleanup_ddl.push_back(q.substr(strlen(OPENIVM_DDL_CLEANUP_PREFIX)));
			continue;
		}
		OPENIVM_DEBUG_PRINT("[DDLExecutorExecuteFunction] Executing DDL: %s\n", q.c_str());
		auto ddl_start = std::chrono::steady_clock::now();
		auto r = conn->Query(q);
		profiler.AddStep(current_profile_step, ddl_start, current_profile_detail + "; bytes=" + to_string(q.size()));
		if (r->HasError()) {
			bool is_unique_index = StringUtil::Contains(StringUtil::Lower(q), "create unique index") &&
			                       StringUtil::Contains(r->GetError(), "Data contains duplicates");
			if (is_unique_index) {
				Printer::Print("Warning: could not create unique index for MV — group_columns "
				               "are not unique in MV output. Refresh will still work (no index).");
				continue;
			}
			run_cleanup();
			profiler.AddTotal();
			profiler.Flush(db);
			restore_outer_transaction();
			throw CatalogException("Failed to execute IVM DDL: " + r->GetError());
		}
	}
	profiler.AddTotal();
	profiler.Flush(db);
	restore_outer_transaction();
}

} // namespace

unique_ptr<FunctionData> DDLExecutorBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<DDLExecutorFunction::DDLExecutorBindData>(true);
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
	auto &bind_data = data_p.bind_data->Cast<DDLExecutorFunction::DDLExecutorBindData>();
	auto &gdata = dynamic_cast<DDLExecutorFunction::DDLExecutorGlobalData &>(*data_p.global_state);
	if (gdata.offset >= 1) {
		return;
	}
	ExecuteDDL(context, bind_data.ddl);
	output.SetValue(0, 0, Value::BOOLEAN(bind_data.result));
	output.SetCardinality(1);
	gdata.offset++;
}

} // namespace duckdb
