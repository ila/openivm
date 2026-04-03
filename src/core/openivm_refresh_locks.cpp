#include "core/openivm_refresh_locks.hpp"

namespace duckdb {

std::mutex IVMRefreshLocks::map_mutex_;
std::unordered_map<string, unique_ptr<std::mutex>> IVMRefreshLocks::view_mutexes_;
std::unordered_map<string, unique_ptr<std::mutex>> IVMRefreshLocks::delta_mutexes_;

std::mutex &IVMRefreshLocks::GetViewMutex(const string &view_name) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto &entry = view_mutexes_[view_name];
	if (!entry) {
		entry = duckdb::unique_ptr<std::mutex>(new std::mutex());
	}
	return *entry;
}

std::mutex &IVMRefreshLocks::GetDeltaMutex(const string &delta_table_name) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto &entry = delta_mutexes_[delta_table_name];
	if (!entry) {
		entry = duckdb::unique_ptr<std::mutex>(new std::mutex());
	}
	return *entry;
}

void IVMRefreshLocks::LockView(const string &view_name) {
	GetViewMutex(view_name).lock();
}

bool IVMRefreshLocks::TryLockView(const string &view_name) {
	return GetViewMutex(view_name).try_lock();
}

void IVMRefreshLocks::UnlockView(const string &view_name) {
	GetViewMutex(view_name).unlock();
}

void IVMRefreshLocks::LockDelta(const string &delta_table_name) {
	GetDeltaMutex(delta_table_name).lock();
}

void IVMRefreshLocks::UnlockDelta(const string &delta_table_name) {
	GetDeltaMutex(delta_table_name).unlock();
}

} // namespace duckdb
