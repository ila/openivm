#include "core/refresh_locks.hpp"
#include "core/openivm_debug.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/common/printer.hpp"

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

MutationLockGuard::MutationLockGuard(ClientContext &context)
    : MutationLockGuard(DatabaseInstance::GetDatabase(context),
                        TransactionalMVLockState::Get(context).GetMutationOwner()) {
}

const void *TransactionalMVLockState::GetMutationOwner() {
	lock_guard<mutex> guard(state_lock);
	return mutation_owner;
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

void TransactionalMVLockState::DeferDeltaCleanup(vector<string> statements) {
	lock_guard<mutex> guard(state_lock);
	for (auto &statement : statements) {
		if (std::find(deferred_delta_cleanup.begin(), deferred_delta_cleanup.end(), statement) ==
		    deferred_delta_cleanup.end()) {
			deferred_delta_cleanup.push_back(std::move(statement));
		}
	}
}

void TransactionalMVLockState::TransactionCommit(MetaTransaction &transaction, ClientContext &context) {
	vector<string> cleanup;
	{
		lock_guard<mutex> guard(state_lock);
		cleanup.swap(deferred_delta_cleanup);
	}
	if (!cleanup.empty()) {
		// The caller is already committed. Housekeeping failures must never report
		// a failed commit or roll back a successfully refreshed MV. Retained deltas
		// are excluded by the committed watermark and retried on a later refresh.
		try {
			Connection con(*context.db);
			Get(*con.context).SetMutationOwner(GetMutationOwner());
			for (auto &sql : cleanup) {
				auto result = con.Query(sql);
				if (result->HasError()) {
					Printer::Print("OpenIVM refresh committed; external delta cleanup deferred: " + result->GetError());
				}
			}
		} catch (const std::exception &ex) {
			Printer::Print(string("OpenIVM refresh committed; external delta cleanup deferred: ") + ex.what());
		}
	}
	Release();
}

void TransactionalMVLockState::TransactionRollback(MetaTransaction &transaction, ClientContext &context) {
	Release();
}

void TransactionalMVLockState::Release() {
	lock_guard<mutex> guard(state_lock);
	OPENIVM_DEBUG_PRINT("[LOCK] release database mutation lock owner=%p\n", static_cast<void *>(&owner));
	deferred_delta_cleanup.clear();
	mutation_guard.reset();
}

} // namespace duckdb
