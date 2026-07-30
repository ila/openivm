#include "core/refresh_locks.hpp"
#include "core/openivm_debug.hpp"

namespace duckdb {

std::mutex RefreshLocks::map_mutex_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::view_mutexes_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::delta_mutexes_;
std::unordered_map<const Catalog *, unique_ptr<DeltaCatalogPhaseGate>> RefreshLocks::delta_catalog_gates_;

static bool HasOtherOwner(const unordered_map<ClientContext *, idx_t> &owners, ClientContext &owner) {
	return !owners.empty() && (owners.size() > 1 || owners.find(&owner) == owners.end());
}

static bool HasOtherOwner(const unordered_map<string, unordered_map<ClientContext *, idx_t>> &owners,
                          const string &delta_table_name, ClientContext &owner) {
	auto entry = owners.find(delta_table_name);
	return entry != owners.end() && HasOtherOwner(entry->second, owner);
}

static bool HasOwner(const unordered_map<string, unordered_map<ClientContext *, idx_t>> &owners,
                     const string &delta_table_name, ClientContext &owner) {
	auto entry = owners.find(delta_table_name);
	return entry != owners.end() && entry->second.find(&owner) != entry->second.end();
}

static bool HasOwner(const unordered_map<string, unordered_map<ClientContext *, idx_t>> &owners, ClientContext &owner) {
	for (auto &entry : owners) {
		if (entry.second.find(&owner) != entry.second.end()) {
			return true;
		}
	}
	return false;
}

void DeltaCatalogPhaseGate::EnterWrite(ClientContext &owner, const string &delta_table_name) {
	std::unique_lock<mutex> guard(lock);
	OPENIVM_DEBUG_PRINT("[LOCK] enter delta write '%s' owner=%p\n", delta_table_name.c_str(),
	                    static_cast<void *>(&owner));
	// Once a refresh is waiting, stop admitting new writers so the finite set of
	// active transactions can drain. A transaction that already owns any writer
	// slot in this catalog may extend its write set: blocking that extension can
	// make a refresh wait for the transaction while the transaction waits for the
	// refresh.
	while (true) {
		if (HasOwner(active_refreshes, owner) && HasOtherOwner(active_refreshes, delta_table_name, owner)) {
			OPENIVM_DEBUG_PRINT("[LOCK] reject delta write '%s': refresh-owner cycle owner=%p\n",
			                    delta_table_name.c_str(), static_cast<void *>(&owner));
			throw TransactionException(
			    "OpenIVM cannot write source delta table '%s' after this transaction entered refresh while "
			    "another refresh owns that source; retry one transaction",
			    delta_table_name);
		}
		bool owner_is_active = HasOwner(active_writers, owner) || HasOwner(active_refreshes, delta_table_name, owner);
		auto waiting = waiting_refreshes.find(delta_table_name);
		bool refresh_is_waiting = waiting != waiting_refreshes.end() && waiting->second > 0;
		if (!HasOtherOwner(active_refreshes, delta_table_name, owner) && (!refresh_is_waiting || owner_is_active)) {
			break;
		}
		condition.wait(guard);
	}
	active_writers[delta_table_name][&owner]++;
	OPENIVM_DEBUG_PRINT("[LOCK] acquired delta write '%s' owner=%p\n", delta_table_name.c_str(),
	                    static_cast<void *>(&owner));
}

void DeltaCatalogPhaseGate::ExitWrite(ClientContext &owner, const string &delta_table_name) {
	lock_guard<mutex> guard(lock);
	auto writers = active_writers.find(delta_table_name);
	D_ASSERT(writers != active_writers.end());
	auto entry = writers->second.find(&owner);
	D_ASSERT(entry != writers->second.end() && entry->second > 0);
	if (--entry->second == 0) {
		writers->second.erase(entry);
	}
	if (writers->second.empty()) {
		active_writers.erase(writers);
	}
	OPENIVM_DEBUG_PRINT("[LOCK] released delta write '%s' owner=%p\n", delta_table_name.c_str(),
	                    static_cast<void *>(&owner));
	condition.notify_all();
}

void DeltaCatalogPhaseGate::EnterRefresh(ClientContext &owner, const vector<string> &delta_table_names) {
	std::unique_lock<mutex> guard(lock);
	OPENIVM_DEBUG_PRINT("[LOCK] enter delta refresh sources=%zu owner=%p\n", delta_table_names.size(),
	                    static_cast<void *>(&owner));
	if (HasOwner(active_writers, owner)) {
		for (auto &name : delta_table_names) {
			if (HasOtherOwner(active_writers, name, owner)) {
				OPENIVM_DEBUG_PRINT("[LOCK] reject delta refresh '%s': writer upgrade cycle owner=%p\n", name.c_str(),
				                    static_cast<void *>(&owner));
				throw TransactionException(
				    "OpenIVM cannot upgrade this transaction to refresh while another transaction is writing "
				    "source delta table '%s'; retry the refresh after that transaction finishes",
				    name);
			}
		}
	}
	for (auto &name : delta_table_names) {
		waiting_refreshes[name]++;
	}
	condition.wait(guard, [&]() {
		for (auto &name : delta_table_names) {
			if (HasOtherOwner(active_writers, name, owner)) {
				return false;
			}
		}
		return true;
	});
	for (auto &name : delta_table_names) {
		auto waiting = waiting_refreshes.find(name);
		D_ASSERT(waiting != waiting_refreshes.end() && waiting->second > 0);
		if (--waiting->second == 0) {
			waiting_refreshes.erase(waiting);
		}
		active_refreshes[name][&owner]++;
		auto writers = active_writers.find(name);
		if (writers != active_writers.end() && writers->second.empty()) {
			active_writers.erase(writers);
		}
	}
	OPENIVM_DEBUG_PRINT("[LOCK] acquired delta refresh sources=%zu owner=%p\n", delta_table_names.size(),
	                    static_cast<void *>(&owner));
}

void DeltaCatalogPhaseGate::ExitRefresh(ClientContext &owner, const vector<string> &delta_table_names) {
	lock_guard<mutex> guard(lock);
	for (auto &name : delta_table_names) {
		auto refreshes = active_refreshes.find(name);
		D_ASSERT(refreshes != active_refreshes.end());
		auto entry = refreshes->second.find(&owner);
		D_ASSERT(entry != refreshes->second.end() && entry->second > 0);
		if (--entry->second == 0) {
			refreshes->second.erase(entry);
		}
		if (refreshes->second.empty()) {
			active_refreshes.erase(refreshes);
		}
		auto writers = active_writers.find(name);
		if (writers != active_writers.end() && writers->second.empty()) {
			active_writers.erase(writers);
		}
	}
	OPENIVM_DEBUG_PRINT("[LOCK] released delta refresh sources=%zu owner=%p\n", delta_table_names.size(),
	                    static_cast<void *>(&owner));
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

string RefreshLocks::RelationIdentity(const string &catalog_name, const string &schema_name,
                                      const string &relation_name) {
	return StringUtil::Lower(catalog_name) + "." + StringUtil::Lower(schema_name) + "." +
	       StringUtil::Lower(relation_name);
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

void RefreshLocks::EnterDeltaWrite(ClientContext &owner, Catalog &catalog, const string &delta_table_name) {
	GetDeltaCatalogGate(catalog).EnterWrite(owner, delta_table_name);
}

void RefreshLocks::ExitDeltaWrite(ClientContext &owner, Catalog &catalog, const string &delta_table_name) {
	GetDeltaCatalogGate(catalog).ExitWrite(owner, delta_table_name);
}

void RefreshLocks::EnterDeltaRefresh(ClientContext &owner, Catalog &catalog, const vector<string> &delta_table_names) {
	GetDeltaCatalogGate(catalog).EnterRefresh(owner, delta_table_names);
}

void RefreshLocks::ExitDeltaRefresh(ClientContext &owner, Catalog &catalog, const vector<string> &delta_table_names) {
	GetDeltaCatalogGate(catalog).ExitRefresh(owner, delta_table_names);
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
	auto catalog_entry = locked_refresh_deltas.find(&catalog);
	return catalog_entry != locked_refresh_deltas.end() &&
	       catalog_entry->second.find(delta_table_name) != catalog_entry->second.end();
}

void TransactionalMVLockState::Acquire(const vector<string> &view_names, const vector<string> &delta_table_names,
                                       const vector<DeltaGateTarget> &gate_targets) {
	if (!owner) {
		throw InternalException("OpenIVM transactional lock state has no owning client context");
	}
	OPENIVM_DEBUG_PRINT("[LOCK] acquire transaction views=%zu deltas=%zu gates=%zu owner=%p\n", view_names.size(),
	                    delta_table_names.size(), gate_targets.size(), static_cast<void *>(owner));
	auto sorted_views = view_names;
	std::sort(sorted_views.begin(), sorted_views.end());
	sorted_views.erase(std::unique(sorted_views.begin(), sorted_views.end()), sorted_views.end());
	for (auto &view_name : sorted_views) {
		if (locked_views.find(view_name) != locked_views.end()) {
			continue;
		}
		if (catalog_guards.empty()) {
			view_guards.push_back(make_uniq<ViewLockGuard>(view_name));
		} else {
			// This transaction already holds phase gates from an earlier
			// refresh. Never block while extending its view set: lifecycle
			// operations acquire view locks before entering writer phases.
			auto guard = make_uniq<TryViewLockGuard>(view_name);
			if (!guard->OwnsLock()) {
				OPENIVM_DEBUG_PRINT("[LOCK] reject late view '%s' owner=%p\n", view_name.c_str(),
				                    static_cast<void *>(owner));
				throw TransactionException(
				    "OpenIVM cannot lock materialized view '%s' after this transaction has entered refresh; "
				    "retry after the concurrent lifecycle operation finishes",
				    view_name);
			}
			late_view_guards.push_back(std::move(guard));
		}
		locked_views.insert(view_name);
	}

	map<string, pair<Catalog *, vector<string>>> new_targets;
	for (auto &target : gate_targets) {
		if (!target.catalog || target.delta_table_name.empty()) {
			continue;
		}
		auto &locked = locked_refresh_deltas[target.catalog];
		if (locked.insert(target.delta_table_name).second) {
			auto &catalog_targets = new_targets[target.catalog->GetName()];
			catalog_targets.first = target.catalog;
			catalog_targets.second.push_back(target.delta_table_name);
		}
	}
	for (auto &entry : new_targets) {
		auto catalog = entry.second.first;
		auto &names = entry.second.second;
		std::sort(names.begin(), names.end());
		names.erase(std::unique(names.begin(), names.end()), names.end());
		catalog_guards.push_back(make_uniq<DeltaCatalogRefreshGuard>(*owner, *catalog, std::move(names)));
	}

	auto sorted_deltas = delta_table_names;
	std::sort(sorted_deltas.begin(), sorted_deltas.end());
	sorted_deltas.erase(std::unique(sorted_deltas.begin(), sorted_deltas.end()), sorted_deltas.end());
	for (auto &delta_table : sorted_deltas) {
		if (locked_delta_tables.insert(delta_table).second) {
			delta_guards.push_back(make_uniq<DeltaLockGuard>(delta_table));
		}
	}
	OPENIVM_DEBUG_PRINT("[LOCK] acquired transaction views=%zu deltas=%zu owner=%p\n", locked_views.size(),
	                    locked_delta_tables.size(), static_cast<void *>(owner));
}

void TransactionalMVLockState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::Release() {
	OPENIVM_DEBUG_PRINT("[LOCK] release transaction views=%zu deltas=%zu owner=%p\n", locked_views.size(),
	                    locked_delta_tables.size(), static_cast<void *>(owner));
	delta_guards.clear();
	late_view_guards.clear();
	catalog_guards.clear();
	view_guards.clear();
	locked_delta_tables.clear();
	locked_views.clear();
	locked_refresh_deltas.clear();
}

} // namespace duckdb
