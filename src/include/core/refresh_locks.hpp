#ifndef REFRESH_LOCKS_HPP
#define REFRESH_LOCKS_HPP

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
class RefreshLocks {
public:
	// --- View-level locks (prevent concurrent refresh of same MV) ---

	// Blocking lock — used by PRAGMA refresh() (user explicitly wants to refresh, so wait).
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

	// Serializes refresh with transactions that write native delta tables in the same catalog.
	// The writer holds this lock until transaction commit/rollback so a refresh cannot advance
	// its timestamp cursor past an uncommitted delta row.
	static void LockDeltaCatalog(const string &catalog_name);

	static void UnlockDeltaCatalog(const string &catalog_name);

private:
	static std::mutex &GetViewMutex(const string &view_name);
	static std::mutex &GetDeltaMutex(const string &delta_table_name);
	static std::mutex &GetDeltaCatalogMutex(const string &catalog_name);

	static std::mutex map_mutex_;
	static std::unordered_map<string, unique_ptr<std::mutex>> view_mutexes_;
	static std::unordered_map<string, unique_ptr<std::mutex>> delta_mutexes_;
	static std::unordered_map<string, unique_ptr<std::mutex>> delta_catalog_mutexes_;
};

// RAII guard for catalog-level delta transaction locks.
class DeltaCatalogLockGuard {
	string name_;

public:
	explicit DeltaCatalogLockGuard(const string &catalog_name) : name_(catalog_name) {
		RefreshLocks::LockDeltaCatalog(name_);
	}
	~DeltaCatalogLockGuard() {
		RefreshLocks::UnlockDeltaCatalog(name_);
	}
	DeltaCatalogLockGuard(const DeltaCatalogLockGuard &) = delete;
	DeltaCatalogLockGuard &operator=(const DeltaCatalogLockGuard &) = delete;
	DeltaCatalogLockGuard(DeltaCatalogLockGuard &&) = delete;
	DeltaCatalogLockGuard &operator=(DeltaCatalogLockGuard &&) = delete;
};

// RAII guard for delta-table locks. Automatically unlocks on scope exit (including exceptions).
class DeltaLockGuard {
	string name_;

public:
	explicit DeltaLockGuard(const string &delta_table_name) : name_(delta_table_name) {
		RefreshLocks::LockDelta(name_);
	}
	~DeltaLockGuard() {
		RefreshLocks::UnlockDelta(name_);
	}
	DeltaLockGuard(const DeltaLockGuard &) = delete;
	DeltaLockGuard &operator=(const DeltaLockGuard &) = delete;
	DeltaLockGuard(DeltaLockGuard &&) = delete;
	DeltaLockGuard &operator=(DeltaLockGuard &&) = delete;
};

// RAII guard for view locks. Automatically unlocks on scope exit (including exceptions).
class ViewLockGuard {
	string name_;

public:
	explicit ViewLockGuard(const string &view_name) : name_(view_name) {
		RefreshLocks::LockView(name_);
	}
	~ViewLockGuard() {
		RefreshLocks::UnlockView(name_);
	}
	ViewLockGuard(const ViewLockGuard &) = delete;
	ViewLockGuard &operator=(const ViewLockGuard &) = delete;
	ViewLockGuard(ViewLockGuard &&) = delete;
	ViewLockGuard &operator=(ViewLockGuard &&) = delete;
};

// Non-blocking RAII guard for opportunistic view-lock checks.
class TryViewLockGuard {
	string name_;
	bool owns_lock_;

public:
	explicit TryViewLockGuard(const string &view_name)
	    : name_(view_name), owns_lock_(RefreshLocks::TryLockView(name_)) {
	}
	~TryViewLockGuard() {
		if (owns_lock_) {
			RefreshLocks::UnlockView(name_);
		}
	}
	bool OwnsLock() const {
		return owns_lock_;
	}
	TryViewLockGuard(const TryViewLockGuard &) = delete;
	TryViewLockGuard &operator=(const TryViewLockGuard &) = delete;
	TryViewLockGuard(TryViewLockGuard &&) = delete;
	TryViewLockGuard &operator=(TryViewLockGuard &&) = delete;
};

} // namespace duckdb

#endif // REFRESH_LOCKS_HPP
