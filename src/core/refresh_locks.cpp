#include "core/refresh_locks.hpp"

namespace duckdb {

std::mutex RefreshLocks::map_mutex_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::view_mutexes_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::delta_mutexes_;
std::unordered_map<const Catalog *, unique_ptr<DeltaCatalogPhaseGate>> RefreshLocks::delta_catalog_gates_;

static bool HasOtherOwner(const unordered_map<ClientContext *, idx_t> &owners, ClientContext &owner) {
	return !owners.empty() && (owners.size() > 1 || owners.find(&owner) == owners.end());
}

void DeltaCatalogPhaseGate::EnterWrite(ClientContext &owner) {
	std::unique_lock<mutex> guard(lock);
	// Once a refresh is waiting, stop admitting new writers so the finite set of
	// active transactions can drain. The refresh owner's own later DML remains
	// reentrant, which lets transaction-local DML and refresh compose safely.
	condition.wait(guard, [&]() {
		bool owner_is_active = active_writers.find(&owner) != active_writers.end() ||
		                       active_refreshes.find(&owner) != active_refreshes.end();
		return !HasOtherOwner(active_refreshes, owner) && (waiting_refreshes == 0 || owner_is_active);
	});
	active_writers[&owner]++;
}

void DeltaCatalogPhaseGate::ExitWrite(ClientContext &owner) {
	lock_guard<mutex> guard(lock);
	auto entry = active_writers.find(&owner);
	D_ASSERT(entry != active_writers.end() && entry->second > 0);
	if (--entry->second == 0) {
		active_writers.erase(entry);
	}
	condition.notify_all();
}

void DeltaCatalogPhaseGate::EnterRefresh(ClientContext &owner) {
	std::unique_lock<mutex> guard(lock);
	waiting_refreshes++;
	condition.wait(guard, [&]() { return !HasOtherOwner(active_writers, owner); });
	waiting_refreshes--;
	active_refreshes[&owner]++;
}

void DeltaCatalogPhaseGate::ExitRefresh(ClientContext &owner) {
	lock_guard<mutex> guard(lock);
	auto entry = active_refreshes.find(&owner);
	D_ASSERT(entry != active_refreshes.end() && entry->second > 0);
	if (--entry->second == 0) {
		active_refreshes.erase(entry);
	}
	condition.notify_all();
}

std::mutex &RefreshLocks::GetViewMutex(const string &view_name) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto &entry = view_mutexes_[view_name];
	if (!entry) {
		entry = duckdb::unique_ptr<std::mutex>(new std::mutex());
	}
	return *entry;
}

std::mutex &RefreshLocks::GetDeltaMutex(const string &delta_table_name) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto &entry = delta_mutexes_[delta_table_name];
	if (!entry) {
		entry = duckdb::unique_ptr<std::mutex>(new std::mutex());
	}
	return *entry;
}

DeltaCatalogPhaseGate &RefreshLocks::GetDeltaCatalogGate(Catalog &catalog) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto &entry = delta_catalog_gates_[&catalog];
	if (!entry) {
		entry = make_uniq<DeltaCatalogPhaseGate>();
	}
	return *entry;
}

void RefreshLocks::LockView(const string &view_name) {
	GetViewMutex(view_name).lock();
}

bool RefreshLocks::TryLockView(const string &view_name) {
	return GetViewMutex(view_name).try_lock();
}

void RefreshLocks::UnlockView(const string &view_name) {
	GetViewMutex(view_name).unlock();
}

void RefreshLocks::LockDelta(const string &delta_table_name) {
	GetDeltaMutex(delta_table_name).lock();
}

void RefreshLocks::UnlockDelta(const string &delta_table_name) {
	GetDeltaMutex(delta_table_name).unlock();
}

void RefreshLocks::EnterDeltaWrite(ClientContext &owner, Catalog &catalog) {
	GetDeltaCatalogGate(catalog).EnterWrite(owner);
}

void RefreshLocks::ExitDeltaWrite(ClientContext &owner, Catalog &catalog) {
	GetDeltaCatalogGate(catalog).ExitWrite(owner);
}

void RefreshLocks::EnterDeltaRefresh(ClientContext &owner, Catalog &catalog) {
	GetDeltaCatalogGate(catalog).EnterRefresh(owner);
}

void RefreshLocks::ExitDeltaRefresh(ClientContext &owner, Catalog &catalog) {
	GetDeltaCatalogGate(catalog).ExitRefresh(owner);
}

TransactionalMVLockState &TransactionalMVLockState::Get(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<TransactionalMVLockState>("openivm_transactional_mv_locks");
	state->owner = &context;
	return *state;
}

optional_ptr<TransactionalMVLockState> TransactionalMVLockState::TryGet(ClientContext &context) {
	return context.registered_state->Get<TransactionalMVLockState>("openivm_transactional_mv_locks");
}

bool TransactionalMVLockState::OwnsRefreshDelta(Catalog &catalog, const string &delta_table_name) const {
	return locked_catalogs.find(&catalog) != locked_catalogs.end() &&
	       locked_delta_tables.find(delta_table_name) != locked_delta_tables.end();
}

void TransactionalMVLockState::Acquire(const vector<string> &view_names, const vector<string> &delta_table_names,
                                       const vector<Catalog *> &source_catalogs) {
	if (!owner) {
		throw InternalException("OpenIVM transactional lock state has no owning client context");
	}
	auto sorted_catalogs = source_catalogs;
	std::sort(sorted_catalogs.begin(), sorted_catalogs.end(),
	          [](Catalog *left, Catalog *right) { return left->GetName() < right->GetName(); });
	sorted_catalogs.erase(std::unique(sorted_catalogs.begin(), sorted_catalogs.end()), sorted_catalogs.end());
	for (auto catalog : sorted_catalogs) {
		if (locked_catalogs.insert(catalog).second) {
			catalog_guards.push_back(make_uniq<DeltaCatalogRefreshGuard>(*owner, *catalog));
		}
	}

	auto sorted_views = view_names;
	std::sort(sorted_views.begin(), sorted_views.end());
	sorted_views.erase(std::unique(sorted_views.begin(), sorted_views.end()), sorted_views.end());
	for (auto &view_name : sorted_views) {
		if (locked_views.insert(view_name).second) {
			view_guards.push_back(make_uniq<ViewLockGuard>(view_name));
		}
	}

	auto sorted_deltas = delta_table_names;
	std::sort(sorted_deltas.begin(), sorted_deltas.end());
	sorted_deltas.erase(std::unique(sorted_deltas.begin(), sorted_deltas.end()), sorted_deltas.end());
	for (auto &delta_table : sorted_deltas) {
		if (locked_delta_tables.insert(delta_table).second) {
			delta_guards.push_back(make_uniq<DeltaLockGuard>(delta_table));
		}
	}
}

void TransactionalMVLockState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::Release() {
	delta_guards.clear();
	view_guards.clear();
	catalog_guards.clear();
	locked_delta_tables.clear();
	locked_views.clear();
	locked_catalogs.clear();
}

} // namespace duckdb
