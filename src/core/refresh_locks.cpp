#include "core/refresh_locks.hpp"
#include "core/openivm_debug.hpp"

#include <cstdio>

namespace duckdb {

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
		// Report accounting errors in release builds without unlocking another owner's gate.
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
	return db.GetObjectCache().GetOrCreate<MutationGate>(MutationGate::ObjectType());
}

TransactionalMVLockState::TransactionalMVLockState(ClientContext &context) : owner(context), mutation_owner(&context) {
}

TransactionalMVLockState &TransactionalMVLockState::Get(ClientContext &context) {
	return *context.registered_state->GetOrCreate<TransactionalMVLockState>("openivm_transactional_mv_locks", context);
}

void TransactionalMVLockState::AcquireMutationLock() {
	// Parallel delta-capture workers share this state. Losing a guard here leaks its gate depth.
	lock_guard<mutex> guard(state_lock);
	if (!mutation_guard) {
		mutation_guard = make_uniq<MutationLockGuard>(DatabaseInstance::GetDatabase(owner), mutation_owner);
		OPENIVM_DEBUG_PRINT("[LOCK] acquired database mutation lock owner=%p\n", static_cast<void *>(&owner));
	}
}

void TransactionalMVLockState::SetMutationOwner(const void *owner_token) {
	lock_guard<mutex> guard(state_lock);
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
	lock_guard<mutex> guard(state_lock);
	OPENIVM_DEBUG_PRINT("[LOCK] release database mutation lock owner=%p\n", static_cast<void *>(&owner));
	mutation_guard.reset();
}

} // namespace duckdb
