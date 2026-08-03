#include "core/refresh_locks.hpp"
#include "core/openivm_debug.hpp"

namespace duckdb {

std::mutex RefreshLocks::map_mutex_;
std::unordered_map<const DatabaseInstance *, unique_ptr<MutationGate>> RefreshLocks::mutation_gates_;

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
		D_ASSERT(false);
		return;
	}
	depth--;
	if (depth == 0) {
		active_owner = nullptr;
		condition.notify_one();
	}
}

MutationGate &RefreshLocks::GetMutationGate(DatabaseInstance &db) {
	std::lock_guard<std::mutex> guard(map_mutex_);
	auto &entry = mutation_gates_[&db];
	if (!entry) {
		entry = make_uniq<MutationGate>();
	}
	return *entry;
}

void RefreshLocks::LockMutation(DatabaseInstance &db, const void *owner) {
	GetMutationGate(db).Lock(owner);
}

void RefreshLocks::UnlockMutation(DatabaseInstance &db, const void *owner) {
	GetMutationGate(db).Unlock(owner);
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
