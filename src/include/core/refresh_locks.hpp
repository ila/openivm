#ifndef REFRESH_LOCKS_HPP
#define REFRESH_LOCKS_HPP

#include "duckdb.hpp"
#include "duckdb/main/client_context_state.hpp"

#include <condition_variable>
#include <mutex>
#include <unordered_map>

namespace duckdb {

// Serializes OpenIVM mutations for one database. The gate is re-entrant for one
// logical owner because helper connections can execute on different worker threads.
class MutationGate {
public:
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
	// The gate for `db`, created on first use. Callers hold the returned reference for as long as
	// they hold the lock, and that reference is what keeps the gate alive.
	//
	// The registry deliberately holds only weak references. It is keyed by DatabaseInstance address,
	// and addresses are recycled: a process that opens and closes many databases, as the benchmarks
	// do, will eventually allocate a new instance at the address of a destroyed one. Holding the
	// gates by value or by owning pointer meant the new database inherited the dead one's gate,
	// including its active owner and recursion depth, so a fresh database could start out already
	// locked by a context that no longer existed and every acquirer would block forever. Weak
	// references make a gate die with the last guard that holds it, which happens when the database
	// and its client contexts go away, so a recycled address gets a new gate. It also stops the
	// registry growing without bound.
	static shared_ptr<MutationGate> AcquireGate(DatabaseInstance &db);

private:
	static std::mutex map_mutex_;
	static std::unordered_map<const DatabaseInstance *, weak_ptr<MutationGate>> mutation_gates_;
};

class MutationLockGuard {
	// Held rather than looked up again on release. The destructor previously re-resolved the gate
	// from the database address, which is a second chance to find a different object than the one
	// that was locked.
	shared_ptr<MutationGate> gate;
	const void *owner;

public:
	explicit MutationLockGuard(ClientContext &owner_p)
	    : gate(RefreshLocks::AcquireGate(DatabaseInstance::GetDatabase(owner_p))), owner(&owner_p) {
		gate->Lock(owner);
	}
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
	static TransactionalMVLockState &Get(ClientContext &context);

	void AcquireMutationLock();
	void SetMutationOwner(const void *owner_token);

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

private:
	void Release();

	unique_ptr<MutationLockGuard> mutation_guard;
	ClientContext *owner = nullptr;
	const void *mutation_owner = nullptr;
};

} // namespace duckdb

#endif // REFRESH_LOCKS_HPP
