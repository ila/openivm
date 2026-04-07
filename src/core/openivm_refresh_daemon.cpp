#include "core/openivm_refresh_daemon.hpp"

#include "core/openivm_debug.hpp"
#include "core/openivm_metadata.hpp"
#include "core/openivm_refresh_locks.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/main/connection.hpp"

#include <chrono>

namespace duckdb {

void IVMRefreshDaemon::Start(shared_ptr<DatabaseInstance> db) {
	bool expected = false;
	if (!started_.compare_exchange_strong(expected, true)) {
		return; // already started
	}
	db_weak_ = db;
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

void IVMRefreshDaemon::SetAdaptiveBackoff(bool enabled) {
	adaptive_backoff_ = enabled;
	if (!enabled) {
		std::lock_guard<std::mutex> guard(backoff_mutex_);
		effective_intervals_.clear();
	}
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

		auto db = db_weak_.lock();
		if (!db) {
			OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] Database destroyed, exiting\n");
			break;
		}

		try {
			Connection con(*db);
			IVMMetadata metadata(con);
			auto scheduled = metadata.GetScheduledViews();

			for (auto &sv : scheduled) {
				if (shutdown_.load()) {
					break;
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
					if (!elapsed_result->HasError() && elapsed_result->RowCount() > 0) {
						auto elapsed_seconds = elapsed_result->GetValue(0, 0).GetValue<double>();
						if (elapsed_seconds < static_cast<double>(interval)) {
							continue; // not due yet
						}
					}
				}

				// Quick check if the view is already being refreshed (non-blocking).
				// If TryLock succeeds, release immediately — the actual locking is done
				// inside UpsertDeltaQueriesLocked called by PRAGMA ivm().
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
					Connection refresh_con(*db);
					auto result = refresh_con.Query("PRAGMA ivm('" + sv.view_name + "')");
					if (result->HasError()) {
						Printer::Print("Warning: auto-refresh of '" + sv.view_name + "' failed: " + result->GetError());
					}
				} catch (std::exception &e) {
					Printer::Print("Warning: auto-refresh of '" + sv.view_name + "' failed: " + string(e.what()));
				}

				{
					std::lock_guard<std::mutex> guard(refreshing_mutex_);
					currently_refreshing_.clear();
				}

				auto after = std::chrono::steady_clock::now();
				auto duration_seconds = std::chrono::duration_cast<std::chrono::seconds>(after - before).count();

				// Adaptive backoff: if refresh took longer than the configured interval,
				// double the effective interval (capped at 24h). Resets when refresh fits.
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
