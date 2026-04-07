#ifndef OPENIVM_REFRESH_DAEMON_HPP
#define OPENIVM_REFRESH_DAEMON_HPP

#include "duckdb.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace duckdb {

// Background thread that periodically refreshes materialized views with a REFRESH EVERY interval.
// Uses a weak_ptr to DatabaseInstance so the DB can shut down without waiting for the daemon.
// The daemon wakes every 30 seconds, checks which views are due, and refreshes them under a
// per-view lock (TryLock — skip if busy, e.g. manual PRAGMA ivm() is running).
class IVMRefreshDaemon {
public:
	// Start the daemon thread. Safe to call multiple times (only starts once).
	void Start(shared_ptr<DatabaseInstance> db);

	// Stop the daemon and join the thread. Called on destruction or explicit shutdown.
	void Stop();

	~IVMRefreshDaemon();

	// Get the effective interval for a view (may be larger than configured due to backoff).
	int64_t GetEffectiveInterval(const string &view_name) const;

	// Check if a view is currently being refreshed by the daemon.
	bool IsRefreshing(const string &view_name) const;

	// Enable/disable adaptive backoff. When disabled, the daemon retries at the original interval.
	void SetAdaptiveBackoff(bool enabled);

private:
	void Run();

	weak_ptr<DatabaseInstance> db_weak_;
	std::thread thread_;
	std::atomic<bool> shutdown_ {false};
	std::atomic<bool> started_ {false};
	std::mutex cv_mutex_;
	std::condition_variable cv_;

	// Adaptive backoff: runtime-only effective intervals (not persisted)
	mutable std::mutex backoff_mutex_;
	std::unordered_map<string, int64_t> effective_intervals_;
	std::atomic<bool> adaptive_backoff_ {true};

	// Track which view the daemon is currently refreshing
	mutable std::mutex refreshing_mutex_;
	string currently_refreshing_;

	static constexpr int64_t WAKE_INTERVAL_SECONDS = 30;
	static constexpr int64_t MAX_BACKOFF_SECONDS = 86400; // 24 hours
};

} // namespace duckdb

#endif // OPENIVM_REFRESH_DAEMON_HPP
