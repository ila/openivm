#ifndef OPENIVM_REFRESH_LOCKS_HPP
#define OPENIVM_REFRESH_LOCKS_HPP

#include "duckdb.hpp"

#include <mutex>
#include <unordered_map>

namespace duckdb {

// Provides per-view and per-delta-table mutexes for safe concurrent refresh.
//
// Per-view mutex: prevents two concurrent refreshes of the same MV (which would
// cause write-write conflicts on the MV table).
//
// Per-delta-table mutex: serializes the refresh's "read deltas + set last_update"
// critical section with the insert rule's delta row writes. This closes the window
// where concurrent DML deltas could be permanently skipped.
class IVMRefreshLocks {
public:
	// --- View-level locks (prevent concurrent refresh of same MV) ---

	// Blocking lock — used by PRAGMA ivm() (user explicitly wants to refresh, so wait).
	static void LockView(const string &view_name);

	// Non-blocking try-lock — used by the refresh daemon (skip if busy).
	// Returns true if the lock was acquired.
	static bool TryLockView(const string &view_name);

	static void UnlockView(const string &view_name);

	// --- Delta-table-level locks (serialize delta reads/writes) ---

	// Blocking lock — held briefly by both refresh (read + timestamp update)
	// and insert rule (delta row write).
	static void LockDelta(const string &delta_table_name);

	static void UnlockDelta(const string &delta_table_name);

private:
	static std::mutex &GetViewMutex(const string &view_name);
	static std::mutex &GetDeltaMutex(const string &delta_table_name);

	static std::mutex map_mutex_;
	static std::unordered_map<string, unique_ptr<std::mutex>> view_mutexes_;
	static std::unordered_map<string, unique_ptr<std::mutex>> delta_mutexes_;
};

} // namespace duckdb

#endif // OPENIVM_REFRESH_LOCKS_HPP
