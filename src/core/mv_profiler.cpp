#include "core/mv_profiler.hpp"
#include "core/compiled_sql_archive.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include <algorithm>

namespace duckdb {

MVProfiler::MVProfiler(ClientContext &context, string total_step, string view_name, string view_catalog)
    : context(context), enabled(SqlUtils::GetBoolSetting(context, "openivm_profile_refresh", false)),
      total_step(std::move(total_step)), view_name(std::move(view_name)), view_catalog(std::move(view_catalog)),
      refresh_id(enabled || !this->view_name.empty()
                     ? NewCompilationId(this->view_name.empty() ? "create_mv" : this->view_name)
                     : string()),
      total_start(std::chrono::steady_clock::now()) {
	Value retention;
	if (enabled && context.TryGetCurrentSetting(Identifier("openivm_profile_retention_days"), retention) &&
	    !retention.IsNull()) {
		retention_days = std::max<int64_t>(0, retention.GetValue<int64_t>());
	}
}

void MVProfiler::SetViewName(const string &view_name_p, Connection &con) {
	if (!enabled || !view_name.empty()) {
		return;
	}
	view_name = view_name_p;
	refresh_id = view_name + "_" + refresh_id;
	// The executor has selected CREATE's metadata location before its first marker.
	// Keep that owner: a fresh connection cannot discover a data catalog's marker.
	if (profile_table.empty()) {
		auto session = SqlUtils::SessionDefaultLocation(*con.context);
		profile_table = SqlUtils::FullName(session.catalog, session.schema, openivm::PROFILE_TABLE);
	}
}

void MVProfiler::AddStep(const string &name, std::chrono::steady_clock::time_point start, const string &detail) {
	if (enabled) {
		AddMeasuredStep(
		    name,
		    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count(),
		    detail);
	}
}

void MVProfiler::AddMeasuredStep(const string &name, int64_t duration_ms, const string &detail) {
	if (enabled) {
		steps.push_back({name, duration_ms, detail});
	}
}

void MVProfiler::AddTotal() {
	AddStep(total_step, total_start);
}

void MVProfiler::Flush() {
	if (!enabled || flushed || view_name.empty() || steps.empty()) {
		return;
	}
	flushed = true;
	Connection profile_con(*context.db);
	TransactionalMVLockState::Get(*profile_con.context)
	    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
	if (profile_table.empty()) {
		RefreshMetadata::UseCatalog(context, profile_con, view_catalog);
		profile_table = RefreshMetadata::MetadataTable(profile_con, openivm::PROFILE_TABLE);
	}
	// The CREATE planner resolved the metadata owner before staged execution.
	// The caller's active catalog may instead be an unrelated attached database.
	OPENIVM_DEBUG_PRINT("[PROFILE] Recording MV profile in %s\n", profile_table.c_str());
	// Retention and the new profile share one durable metadata commit.
	profile_con.BeginTransaction();
	profile_con.Query("DELETE FROM " + profile_table +
	                  " WHERE profile_timestamp < current_timestamp::TIMESTAMP - INTERVAL '" +
	                  to_string(retention_days) + " days'");
	// Statement profiles should not add a metadata commit for every DDL operation.
	string values;
	for (idx_t i = 0; i < steps.size(); i++) {
		auto &step = steps[i];
		if (!values.empty()) {
			values += ", ";
		}
		values += "('" + SqlUtils::EscapeValue(refresh_id) + "', '" + SqlUtils::EscapeValue(view_name) +
		          "', CAST(current_timestamp AS TIMESTAMP), " + to_string(i) + ", '" +
		          SqlUtils::EscapeValue(step.step_name) + "', " + to_string(step.duration_ms) + ", '" +
		          SqlUtils::EscapeValue(step.detail) + "')";
	}
	// refresh_id is unique per statement; remote catalogs support neither INSERT OR
	// REPLACE nor DuckDB column defaults.
	auto result = profile_con.Query(
	    "INSERT INTO " + profile_table +
	    " (refresh_id, view_name, profile_timestamp, step_order, step_name, duration_ms, detail) VALUES " + values);
	if (result->HasError()) {
		OPENIVM_DEBUG_PRINT("[PROFILE] Failed to record MV profile: %s\n", result->GetError().c_str());
		return;
	}
	auto committed = profile_con.Query("COMMIT");
	if (committed->HasError()) {
		OPENIVM_DEBUG_PRINT("[PROFILE] Failed to commit MV profile: %s\n", committed->GetError().c_str());
	}
}

} // namespace duckdb
