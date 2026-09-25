#ifndef REFRESH_LOCKS_HPP
#define REFRESH_LOCKS_HPP

#include "duckdb.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <condition_variable>
#include <mutex>

namespace duckdb {

// Serializes OpenIVM mutations for one database. The gate is re-entrant for one
// logical owner because helper connections can execute on different worker threads.
class MutationGate : public ObjectCacheEntry {
public:
	static string ObjectType() {
		return "openivm_mutation_gate";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		return optional_idx(); // The database's mutation gate must never be evicted.
	}

	void Lock(const void *owner);
	void Unlock(const void *owner);

private:
	mutex lock;
	std::condition_variable condition;
	const void *active_owner = nullptr;
	idx_t depth = 0;
};

class RefreshLocks {
public:
	// The database owns the gate; guards retain the exact gate they acquired.
	static shared_ptr<MutationGate> AcquireGate(DatabaseInstance &db);
};

class MutationLockGuard {
	shared_ptr<MutationGate> gate;
	const void *owner;

public:
	explicit MutationLockGuard(ClientContext &owner_p);

	MutationLockGuard(DatabaseInstance &db_p, const void *owner_p)
	    : gate(RefreshLocks::AcquireGate(db_p)), owner(owner_p) {
		gate->Lock(owner);
	}
	~MutationLockGuard() {
		gate->Unlock(owner);
	}
	MutationLockGuard(const MutationLockGuard &) = delete;
	MutationLockGuard &operator=(const MutationLockGuard &) = delete;
	MutationLockGuard(MutationLockGuard &&) = delete;
	MutationLockGuard &operator=(MutationLockGuard &&) = delete;
};

// Native lifecycle and refresh programs are returned as multiple DuckDB
// statements. Retain the mutation gate until the caller transaction ends.
class TransactionalMVLockState : public ClientContextState {
public:
	explicit TransactionalMVLockState(ClientContext &context);
	static TransactionalMVLockState &Get(ClientContext &context);

	void AcquireMutationLock();
	void SetMutationOwner(const void *owner_token);
	const void *GetMutationOwner();

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

private:
	void Release();

	mutex state_lock;
	unique_ptr<MutationLockGuard> mutation_guard;
	ClientContext &owner;
	const void *mutation_owner;
};

} // namespace duckdb

#endif // REFRESH_LOCKS_HPP
