#ifndef REFRESH_DAEMON_HPP
#define REFRESH_DAEMON_HPP

#include "duckdb.hpp"

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <unordered_map>

namespace duckdb {

// Background thread that periodically refreshes materialized views with a REFRESH EVERY interval.
// Holds a raw pointer to DatabaseInstance (valid for the lifetime of the extension).
// The daemon wakes every 30 seconds, checks which views are due, and refreshes them
// after acquiring the shared OpenIVM mutation gate.
class RefreshDaemon {
public:
	// Start the daemon thread. Safe to call multiple times (only starts once).
	void Start(DatabaseInstance &db);

	// Stop the daemon and join the thread. Called on destruction or explicit shutdown.
	void Stop();

	// Request an immediate scheduling cycle (used by explicit daemon restart).
	void Wake();

	~RefreshDaemon();

	// Get the effective interval for a view (may be larger than configured due to backoff).
	int64_t GetEffectiveInterval(const string &view_name) const;

	// Check if a view is currently being refreshed by the daemon.
	bool IsRefreshing(const string &view_name) const;

private:
	void Run();

	DatabaseInstance *db_ = nullptr;
	std::thread thread_;
	std::atomic<bool> shutdown_ {false};
	std::atomic<bool> started_ {false};
	std::atomic<bool> wake_requested_ {false};
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

#endif // REFRESH_DAEMON_HPP
