#include "core/refresh_locks.hpp"

namespace duckdb {

std::mutex RefreshLocks::map_mutex_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::view_mutexes_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::delta_mutexes_;
std::unordered_map<string, unique_ptr<std::mutex>> RefreshLocks::delta_catalog_mutexes_;

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

std::mutex &RefreshLocks::GetDeltaCatalogMutex(const string &catalog_name) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto &entry = delta_catalog_mutexes_[catalog_name];
	if (!entry) {
		entry = duckdb::unique_ptr<std::mutex>(new std::mutex());
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

void RefreshLocks::LockDeltaCatalog(const string &catalog_name) {
	GetDeltaCatalogMutex(catalog_name).lock();
}

void RefreshLocks::UnlockDeltaCatalog(const string &catalog_name) {
	GetDeltaCatalogMutex(catalog_name).unlock();
}

} // namespace duckdb
