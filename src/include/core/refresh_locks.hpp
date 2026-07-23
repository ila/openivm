#ifndef REFRESH_LOCKS_HPP
#define REFRESH_LOCKS_HPP

#include "duckdb.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

// Provides per-view and per-delta-table mutexes for safe concurrent refresh.
//
// Per-view mutex: prevents two concurrent refreshes of the same MV (which would
// cause write-write conflicts on the MV table).
//
// Per-delta-table mutex: serializes refreshes that consume the same delta table.
//
// Per-catalog phase gate: allows concurrent DML writers and concurrent refreshes, but
// prevents those two phases from overlapping. This closes the window where refresh
// could advance a watermark past a delta row that commits afterward.
class DeltaCatalogPhaseGate {
public:
	void EnterWrite(ClientContext &owner);
	void ExitWrite(ClientContext &owner);
	void EnterRefresh(ClientContext &owner);
	void ExitRefresh(ClientContext &owner);

private:
	mutex lock;
	std::condition_variable condition;
	unordered_map<ClientContext *, idx_t> active_writers;
	unordered_map<ClientContext *, idx_t> active_refreshes;
	idx_t waiting_refreshes = 0;
};

class RefreshLocks {
public:
	// --- View-level locks (prevent concurrent refresh of same MV) ---

	// Blocking lock — used by PRAGMA refresh() (user explicitly wants to refresh, so wait).
	static void LockView(const string &view_name);

	// Non-blocking try-lock — used by the refresh daemon (skip if busy).
	// Returns true if the lock was acquired.
	static bool TryLockView(const string &view_name);

	static void UnlockView(const string &view_name);

	// --- Delta-table-level locks (serialize overlapping refreshes) ---

	// Blocking lock held by refresh while consuming and checkpointing one delta table.
	static void LockDelta(const string &delta_table_name);

	static void UnlockDelta(const string &delta_table_name);

	static void EnterDeltaWrite(ClientContext &owner, Catalog &catalog);
	static void ExitDeltaWrite(ClientContext &owner, Catalog &catalog);
	static void EnterDeltaRefresh(ClientContext &owner, Catalog &catalog);
	static void ExitDeltaRefresh(ClientContext &owner, Catalog &catalog);

private:
	static std::mutex &GetViewMutex(const string &view_name);
	static std::mutex &GetDeltaMutex(const string &delta_table_name);
	static DeltaCatalogPhaseGate &GetDeltaCatalogGate(Catalog &catalog);

	static std::mutex map_mutex_;
	static std::unordered_map<string, unique_ptr<std::mutex>> view_mutexes_;
	static std::unordered_map<string, unique_ptr<std::mutex>> delta_mutexes_;
	static std::unordered_map<const Catalog *, unique_ptr<DeltaCatalogPhaseGate>> delta_catalog_gates_;
};

// Write guards can be retained by transaction state and released from the commit thread;
// unlike std::mutex ownership, phase-gate membership is not tied to one OS thread.
class DeltaCatalogWriteGuard {
	ClientContext *owner;
	Catalog *catalog;

public:
	DeltaCatalogWriteGuard(ClientContext &owner_p, Catalog &catalog_p) : owner(&owner_p), catalog(&catalog_p) {
		RefreshLocks::EnterDeltaWrite(*owner, *catalog);
	}
	~DeltaCatalogWriteGuard() {
		RefreshLocks::ExitDeltaWrite(*owner, *catalog);
	}
	DeltaCatalogWriteGuard(const DeltaCatalogWriteGuard &) = delete;
	DeltaCatalogWriteGuard &operator=(const DeltaCatalogWriteGuard &) = delete;
	DeltaCatalogWriteGuard(DeltaCatalogWriteGuard &&) = delete;
	DeltaCatalogWriteGuard &operator=(DeltaCatalogWriteGuard &&) = delete;
};

class DeltaCatalogRefreshGuard {
	ClientContext *owner;
	Catalog *catalog;

public:
	DeltaCatalogRefreshGuard(ClientContext &owner_p, Catalog &catalog_p) : owner(&owner_p), catalog(&catalog_p) {
		RefreshLocks::EnterDeltaRefresh(*owner, *catalog);
	}
	~DeltaCatalogRefreshGuard() {
		RefreshLocks::ExitDeltaRefresh(*owner, *catalog);
	}
	DeltaCatalogRefreshGuard(const DeltaCatalogRefreshGuard &) = delete;
	DeltaCatalogRefreshGuard &operator=(const DeltaCatalogRefreshGuard &) = delete;
	DeltaCatalogRefreshGuard(DeltaCatalogRefreshGuard &&) = delete;
	DeltaCatalogRefreshGuard &operator=(DeltaCatalogRefreshGuard &&) = delete;
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

// Native lifecycle and refresh programs are returned as multiple DuckDB
// statements. Keep their logical locks until the surrounding caller transaction
// commits or rolls back, rather than releasing them between statements.
class TransactionalMVLockState : public ClientContextState {
public:
	static TransactionalMVLockState &Get(ClientContext &context);
	static optional_ptr<TransactionalMVLockState> TryGet(ClientContext &context);

	void Acquire(const vector<string> &view_names, const vector<string> &delta_table_names,
	             const vector<Catalog *> &source_catalogs = {});
	bool OwnsRefreshDelta(Catalog &catalog, const string &delta_table_name) const;

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

private:
	void Release();

	unordered_set<string> locked_views;
	unordered_set<string> locked_delta_tables;
	unordered_set<const Catalog *> locked_catalogs;
	vector<unique_ptr<ViewLockGuard>> view_guards;
	vector<unique_ptr<DeltaLockGuard>> delta_guards;
	vector<unique_ptr<DeltaCatalogRefreshGuard>> catalog_guards;
	ClientContext *owner = nullptr;
};

} // namespace duckdb

#endif // REFRESH_LOCKS_HPP
