#include "core/refresh_locks.hpp"

namespace duckdb {

std::mutex RefreshLocks::map_mutex_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::view_mutexes_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::delta_mutexes_;
std::unordered_map<const Catalog *, unique_ptr<DeltaCatalogPhaseGate>> RefreshLocks::delta_catalog_gates_;

void DeltaCatalogPhaseGate::EnterWrite() {
	std::unique_lock<mutex> guard(lock);
	// Once a refresh is waiting, stop admitting new writers so the finite set of
	// active transactions can drain. Writers otherwise run concurrently.
	condition.wait(guard, [&]() { return active_refreshes == 0 && waiting_refreshes == 0; });
	active_writers++;
}

void DeltaCatalogPhaseGate::ExitWrite() {
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_writers > 0);
	active_writers--;
	if (active_writers == 0) {
		condition.notify_all();
	}
}

void DeltaCatalogPhaseGate::EnterRefresh() {
	std::unique_lock<mutex> guard(lock);
	waiting_refreshes++;
	condition.wait(guard, [&]() { return active_writers == 0; });
	waiting_refreshes--;
	active_refreshes++;
}

void DeltaCatalogPhaseGate::ExitRefresh() {
	lock_guard<mutex> guard(lock);
	D_ASSERT(active_refreshes > 0);
	active_refreshes--;
	if (active_refreshes == 0) {
		condition.notify_all();
	}
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

void RefreshLocks::EnterDeltaWrite(Catalog &catalog) {
	GetDeltaCatalogGate(catalog).EnterWrite();
}

void RefreshLocks::ExitDeltaWrite(Catalog &catalog) {
	GetDeltaCatalogGate(catalog).ExitWrite();
}

void RefreshLocks::EnterDeltaRefresh(Catalog &catalog) {
	GetDeltaCatalogGate(catalog).EnterRefresh();
}

void RefreshLocks::ExitDeltaRefresh(Catalog &catalog) {
	GetDeltaCatalogGate(catalog).ExitRefresh();
}

} // namespace duckdb
