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
	static void LockMutation(DatabaseInstance &db, const void *owner);
	static void UnlockMutation(DatabaseInstance &db, const void *owner);

private:
	static MutationGate &GetMutationGate(DatabaseInstance &db);

	static std::mutex map_mutex_;
	static std::unordered_map<const DatabaseInstance *, unique_ptr<MutationGate>> mutation_gates_;
};

class MutationLockGuard {
	DatabaseInstance *db;
	const void *owner;

public:
	explicit MutationLockGuard(ClientContext &owner_p) : db(&DatabaseInstance::GetDatabase(owner_p)), owner(&owner_p) {
		RefreshLocks::LockMutation(*db, owner);
	}
	MutationLockGuard(DatabaseInstance &db_p, const void *owner_p) : db(&db_p), owner(owner_p) {
		RefreshLocks::LockMutation(*db, owner);
	}
	~MutationLockGuard() {
		RefreshLocks::UnlockMutation(*db, owner);
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
