#include "core/refresh_locks.hpp"
#include "core/openivm_debug.hpp"

#include <cstdio>
#include <iterator>

namespace duckdb {

std::mutex RefreshLocks::map_mutex_;
std::unordered_map<const DatabaseInstance *, weak_ptr<MutationGate>> RefreshLocks::mutation_gates_;

void MutationGate::Lock(const void *owner) {
	std::unique_lock<mutex> guard(lock);
	if (active_owner == owner) {
		depth++;
		return;
	}
	condition.wait(guard, [&]() { return active_owner == nullptr; });
	active_owner = owner;
	depth = 1;
}

void MutationGate::Unlock(const void *owner) {
	std::lock_guard<mutex> guard(lock);
	if (active_owner != owner || depth == 0) {
		// Releasing a gate this owner does not hold would hand it to an unrelated waiter, so the
		// mismatch is reported rather than acted on.
		//
		// Reported unconditionally, because the bare D_ASSERT that used to stand here compiles out
		// in release builds. A single lost unlock then left the gate held by an owner that would
		// never release it, and every later acquirer blocked on the condition variable below with
		// nothing to indicate why: the symptom surfaced as an unrelated statement hanging at 0% CPU,
		// arbitrarily far from the accounting error that caused it.
		fprintf(stderr,
		        "[openivm] mutation gate release mismatch: owner=%p active_owner=%p depth=%llu. "
		        "The gate stays held; subsequent OpenIVM mutations on this database will block.\n",
		        owner, active_owner, static_cast<unsigned long long>(depth));
		fflush(stderr);
		D_ASSERT(false);
		return;
	}
	depth--;
	if (depth == 0) {
		active_owner = nullptr;
		condition.notify_one();
	}
}

shared_ptr<MutationGate> RefreshLocks::AcquireGate(DatabaseInstance &db) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto entry = mutation_gates_.find(&db);
	if (entry != mutation_gates_.end()) {
		auto existing = entry->second.lock();
		if (existing) {
			return existing;
		}
		// The address is live again but the gate behind it is gone, so this is a different database
		// that happens to have been allocated where an old one stood. Drop the stale mapping.
		mutation_gates_.erase(entry);
	}
	// Discard mappings for databases that have since been closed. Without this the registry keeps an
	// entry for every database the process ever opened.
	for (auto it = mutation_gates_.begin(); it != mutation_gates_.end();) {
		it = it->second.expired() ? mutation_gates_.erase(it) : std::next(it);
	}
	auto gate = make_shared_ptr<MutationGate>();
	mutation_gates_[&db] = gate;
	return gate;
}

TransactionalMVLockState &TransactionalMVLockState::Get(ClientContext &context) {
	auto state = context.registered_state->GetOrCreate<TransactionalMVLockState>("openivm_transactional_mv_locks");
	state->owner = &context;
	if (!state->mutation_owner) {
		state->mutation_owner = &context;
	}
	return *state;
}

void TransactionalMVLockState::AcquireMutationLock() {
	if (!owner) {
		throw InternalException("OpenIVM transactional lock state has no owning client context");
	}
	if (!mutation_guard) {
		mutation_guard = make_uniq<MutationLockGuard>(DatabaseInstance::GetDatabase(*owner), mutation_owner);
		OPENIVM_DEBUG_PRINT("[LOCK] acquired database mutation lock owner=%p\n", static_cast<void *>(owner));
	}
}

void TransactionalMVLockState::SetMutationOwner(const void *owner_token) {
	if (mutation_guard) {
		throw InternalException("OpenIVM cannot change mutation ownership after acquiring the mutation lock");
	}
	mutation_owner = owner_token;
}

void TransactionalMVLockState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::Release() {
	OPENIVM_DEBUG_PRINT("[LOCK] release database mutation lock owner=%p\n", static_cast<void *>(owner));
	mutation_guard.reset();
}

} // namespace duckdb
