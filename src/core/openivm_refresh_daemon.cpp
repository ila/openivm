#include "core/openivm_refresh_daemon.hpp"

#include "core/openivm_debug.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_refresh_locks.hpp"
#include "core/openivm_utils.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database_manager.hpp"

#include <chrono>
#include <unordered_set>

namespace duckdb {

void IVMRefreshDaemon::Start(DatabaseInstance &db) {
	bool expected = false;
	if (!started_.compare_exchange_strong(expected, true)) {
		return; // already started
	}
	db_ = &db;
	shutdown_ = false;
	thread_ = std::thread(&IVMRefreshDaemon::Run, this);
}

void IVMRefreshDaemon::Stop() {
	if (!started_.load()) {
		return;
	}
	shutdown_ = true;
	cv_.notify_all();
	if (thread_.joinable()) {
		thread_.join();
	}
	started_ = false;
}

IVMRefreshDaemon::~IVMRefreshDaemon() {
	Stop();
}

int64_t IVMRefreshDaemon::GetEffectiveInterval(const string &view_name) const {
	std::lock_guard<std::mutex> guard(backoff_mutex_);
	auto it = effective_intervals_.find(view_name);
	if (it != effective_intervals_.end()) {
		return it->second;
	}
	return -1; // not backed off, use configured interval
}

bool IVMRefreshDaemon::IsRefreshing(const string &view_name) const {
	std::lock_guard<std::mutex> guard(refreshing_mutex_);
	return currently_refreshing_ == view_name;
}

void IVMRefreshDaemon::Run() {
	OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Started\n");

	while (!shutdown_.load()) {
		{
			std::unique_lock<std::mutex> lock(cv_mutex_);
			cv_.wait_for(lock, std::chrono::seconds(WAKE_INTERVAL_SECONDS), [this] { return shutdown_.load(); });
		}
		if (shutdown_.load()) {
			break;
		}

		try {
			Connection con(*db_);
			OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Woke up\n");

			// Resolve the default catalog dynamically each cycle.
			// Extensions load into the system catalog, but user tables live in the
			// file-based catalog (e.g. "mydb"). SetDefaultDatabase is the C++ API
			// behind DuckDB's USE command.
			// Resolve the default catalog dynamically each cycle.
			// Extensions load into the system catalog, but user tables live in the
			// file-based catalog (e.g. "mydb"). We query current_database() which
			// resolves DatabaseManager::default_database, then call SetDefaultDatabase
			// to switch this connection to that catalog.
			// Resolve the default catalog dynamically each cycle.
			// Extensions load into the system catalog, but user tables live in the
			// file-based catalog (e.g. "mydb"). USE is the SQL equivalent of
			// SetDefaultDatabase and handles its own transaction.
			{
				auto &db_manager = DatabaseManager::Get(*db_);
				if (db_manager.HasDefaultDatabase()) {
					auto cat_r = con.Query("SELECT current_database()");
					if (!cat_r->HasError() && cat_r->RowCount() > 0) {
						auto catalog = cat_r->GetValue(0, 0).ToString();
						OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Before USE: current_database='%s'\n", catalog.c_str());
						if (!catalog.empty()) {
							auto use_r = con.Query("USE " + catalog);
							OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] USE result: %s\n",
							                    use_r->HasError() ? use_r->GetError().c_str() : "OK");
							// Verify
							auto verify = con.Query("SELECT current_database()");
							if (!verify->HasError() && verify->RowCount() > 0) {
								OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] After USE: current_database='%s'\n",
								                    verify->GetValue(0, 0).ToString().c_str());
							}
							// Also try direct table query
							auto test = con.Query("SELECT count(*) FROM _duckdb_ivm_views");
							OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Direct query: %s\n",
							                    test->HasError() ? test->GetError().c_str()
							                                     : test->GetValue(0, 0).ToString().c_str());
						}
					}
				}
			}

			// Read cascade setting from the DB config
			string cascade_mode = "downstream";
			Value cascade_val;
			if (con.context->TryGetCurrentSetting("ivm_cascade_refresh", cascade_val) && !cascade_val.IsNull()) {
				cascade_mode = StringUtil::Lower(cascade_val.ToString());
			}

			// Read adaptive backoff setting
			Value backoff_val;
			if (con.context->TryGetCurrentSetting("ivm_adaptive_backoff", backoff_val) && !backoff_val.IsNull()) {
				adaptive_backoff_ = backoff_val.GetValue<bool>();
			}

			IVMMetadata metadata(con);
			auto scheduled = metadata.GetScheduledViews();
			OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Scheduled views: %lu\n", (unsigned long)scheduled.size());

			// Track views already refreshed this cycle (directly or via cascade) to avoid double work
			std::unordered_set<string> refreshed_this_cycle;

			for (auto &sv : scheduled) {
				if (shutdown_.load()) {
					break;
				}

				// Skip if already refreshed via cascade from an earlier view in this cycle
				if (refreshed_this_cycle.count(sv.view_name)) {
					continue;
				}

				// Determine effective interval (may be backed off)
				int64_t interval = sv.interval_seconds;
				{
					std::lock_guard<std::mutex> guard(backoff_mutex_);
					auto it = effective_intervals_.find(sv.view_name);
					if (it != effective_intervals_.end()) {
						interval = it->second;
					}
				}

				// Check if the view is due for refresh
				if (!sv.last_update.empty()) {
					auto elapsed_result =
					    con.Query("SELECT EXTRACT(EPOCH FROM (now() - '" + sv.last_update + "'::TIMESTAMP))");
					if (!elapsed_result->HasError() && elapsed_result->RowCount() > 0 &&
					    !elapsed_result->GetValue(0, 0).IsNull()) {
						auto elapsed_seconds = elapsed_result->GetValue(0, 0).GetValue<double>();
						if (elapsed_seconds < static_cast<double>(interval)) {
							continue; // not due yet
						}
					}
				}

				// Quick check if the view is already being refreshed (non-blocking).
				if (!IVMRefreshLocks::TryLockView(sv.view_name)) {
					OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Skipping '%s' — refresh already in progress\n",
					                    sv.view_name.c_str());
					continue;
				}
				IVMRefreshLocks::UnlockView(sv.view_name);

				{
					std::lock_guard<std::mutex> guard(refreshing_mutex_);
					currently_refreshing_ = sv.view_name;
				}

				OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Refreshing '%s'\n", sv.view_name.c_str());
				auto before = std::chrono::steady_clock::now();

				try {
					Connection refresh_con(*db_);

					// Check for refresh hooks
					auto hook_r =
					    refresh_con.Query("SELECT hook_sql, mode FROM _duckdb_ivm_refresh_hooks WHERE view_name = '" +
					                      OpenIVMUtils::EscapeValue(sv.view_name) + "'");
					string hook_sql;
					string hook_mode;
					if (!hook_r->HasError() && hook_r->RowCount() > 0) {
						hook_sql = hook_r->GetValue(0, 0).ToString();
						hook_mode = StringUtil::Lower(hook_r->GetValue(1, 0).ToString());
					}

					if (!hook_sql.empty() && hook_mode == "before") {
						auto hr = refresh_con.Query(hook_sql);
						if (hr->HasError()) {
							Printer::Print("Warning: before-hook for '" + sv.view_name + "' failed: " + hr->GetError());
						}
					}

					if (hook_mode != "replace") {
						auto result =
						    refresh_con.Query("PRAGMA ivm('" + OpenIVMUtils::EscapeValue(sv.view_name) + "')");
						if (result->HasError()) {
							Printer::Print("Warning: auto-refresh of '" + sv.view_name +
							               "' failed: " + result->GetError());
						}
					}

					if (!hook_sql.empty() && (hook_mode == "after" || hook_mode == "replace")) {
						auto hr = refresh_con.Query(hook_sql);
						if (hr->HasError()) {
							Printer::Print("Warning: " + hook_mode + "-hook for '" + sv.view_name +
							               "' failed: " + hr->GetError());
						}
					}
				} catch (std::exception &e) {
					Printer::Print("Warning: auto-refresh of '" + sv.view_name + "' failed: " + string(e.what()));
				}

				{
					std::lock_guard<std::mutex> guard(refreshing_mutex_);
					currently_refreshing_.clear();
				}

				// Mark this view and any cascaded views as done for this cycle
				refreshed_this_cycle.insert(sv.view_name);
				if (cascade_mode == "downstream" || cascade_mode == "both") {
					for (auto &dep : metadata.GetDownstreamViews(sv.view_name)) {
						refreshed_this_cycle.insert(dep);
					}
				}
				if (cascade_mode == "upstream" || cascade_mode == "both") {
					for (auto &dep : metadata.GetUpstreamViews(sv.view_name)) {
						refreshed_this_cycle.insert(dep);
					}
				}

				auto after = std::chrono::steady_clock::now();
				auto duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(after - before).count();

				// Adaptive backoff
				if (adaptive_backoff_.load() && duration_seconds > sv.interval_seconds) {
					std::lock_guard<std::mutex> guard(backoff_mutex_);
					int64_t current = sv.interval_seconds;
					auto it = effective_intervals_.find(sv.view_name);
					if (it != effective_intervals_.end()) {
						current = it->second;
					}
					int64_t new_interval = std::min(current * 2, MAX_BACKOFF_SECONDS);
					effective_intervals_[sv.view_name] = new_interval;
					Printer::Print("Warning: refresh of '" + sv.view_name + "' took " + to_string(duration_seconds) +
					               "s (interval: " + to_string(sv.interval_seconds) +
					               "s). Increasing effective interval to " + to_string(new_interval) +
					               "s. Set ivm_adaptive_backoff = false to disable.");
				} else if (adaptive_backoff_.load()) {
					std::lock_guard<std::mutex> guard(backoff_mutex_);
					effective_intervals_.erase(sv.view_name);
				}
			}
		} catch (std::exception &e) {
			OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Error: %s\n", e.what());
		}
	}

	OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Stopped\n");
}

} // namespace duckdb
