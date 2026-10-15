#ifndef REFRESH_LOCKS_HPP
#define REFRESH_LOCKS_HPP

#include "duckdb.hpp"
#include "core/refresh_metadata.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/object_cache.hpp"

#include <condition_variable>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace duckdb {

class AttachedDatabase;
class Catalog;

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
	bool TryLock(const void *owner);
	void Unlock(const void *owner);

private:
	friend class MutationGateTestAccess;
	friend class SourceAdmissionGuard;
	// Readers are admitted native writers, tracked or untracked.
	// Publication excludes them only for the qualified source being published.
	struct SourceAdmission {
		transaction_t last_write_commit = 0;
		unordered_map<const void *, idx_t> readers;
		const void *publisher = nullptr;
		idx_t publication_depth = 0;
		idx_t waiting_readers = 0;
		idx_t waiting_publishers = 0;
	};
	void LockSource(const void *owner, const string &source, idx_t catalog_oid, bool publication);
	void UnlockSource(const void *owner, const string &source, idx_t catalog_oid, bool publication,
	                  transaction_t oldest_snapshot);
	void RecordSourceCommit(const string &source, idx_t catalog_oid, transaction_t commit_id);
	bool HasSourceCommitAfter(const string &source, idx_t catalog_oid, transaction_t snapshot);
	mutex lock;
	std::condition_variable condition;
	const void *active_owner = nullptr;
	idx_t depth = 0;
	// Integration tests synchronize on an actual wait without timing the caller.
	idx_t waiting_owners = 0;
	unordered_map<idx_t, unordered_map<string, unique_ptr<SourceAdmission>>> sources;
	unordered_map<const void *, idx_t> admitted_writers;
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

class SourceAdmissionGuard {
public:
	SourceAdmissionGuard(DatabaseInstance &db, const void *owner, Catalog &catalog, string source, bool publication);
	~SourceAdmissionGuard();
	SourceAdmissionGuard(const SourceAdmissionGuard &) = delete;
	SourceAdmissionGuard &operator=(const SourceAdmissionGuard &) = delete;
	const string &Source() const {
		return source;
	}
	void RecordCommit();
	// Called under the owning transaction state's mutex, only by execution.
	void MarkExecuted() {
		executed = true;
	}
	bool HasCommitAfter(transaction_t snapshot);

private:
	shared_ptr<MutationGate> gate;
	const void *owner;
	string source;
	bool publication;
	bool executed = false;
	shared_ptr<AttachedDatabase> source_database;
};

// Native lifecycle and refresh programs are returned as multiple DuckDB
// statements. Retain the mutation gate until the caller transaction ends.
class TransactionalMVLockState : public ClientContextState {
public:
	explicit TransactionalMVLockState(ClientContext &context);
	static TransactionalMVLockState &Get(ClientContext &context);

	void AcquireMutationLock();
	// Untracked writers retain source admission through commit. Tracked writers
	// release temporary admission before entering the mutation gate, then recheck
	// ownership and retain admission again under that gate.
	unique_ptr<SourceAdmissionGuard> AcquireSourceWriteAdmission(const string &catalog, const string &schema,
	                                                             const string &table);
	void RetainSourceWriteAdmission(unique_ptr<SourceAdmissionGuard> admission);
	optional_ptr<TableCatalogEntry> FindSourceDeltaForWrite(const string &catalog, const string &schema,
	                                                        const string &table);
	void MarkSourceWriteExecuted(const string &catalog, const string &schema, const string &table,
	                             const string &planned_delta);
	void AcquireSourcePublicationLock(const string &catalog, const string &schema, const string &table);
	void VerifySourceSnapshot(const string &catalog, const string &schema, const string &table);
	vector<unique_ptr<SourceAdmissionGuard>> TakeSourcePublicationLocks();
	void RestoreSourcePublicationLocks(vector<unique_ptr<SourceAdmissionGuard>> locks);
	void DeferDeltaCleanup(vector<string> statements);
	void DeferUnusedSourceDeltaCleanup(const RefreshMetadata::DeltaSource &source);
	// Helper-committed allocations must be reclaimed if the caller fails to publish ownership.
	void RecordSourceDeltaAllocation(const RefreshMetadata::DeltaSource &source);
	vector<RefreshMetadata::DeltaSource> TakeSourceDeltaAllocations();
	void CleanupSourceDeltaAllocations(const vector<RefreshMetadata::DeltaSource> &sources);
	void RecordSourceDeltaDrop(const string &catalog, const string &schema, const string &table);
	bool HasDroppedSourceDelta(const string &catalog, const string &schema, const string &table);
	optional_ptr<TableCatalogEntry> FindDeferredSourceDelta(const string &catalog, const string &schema,
	                                                        const string &table);
	// Reused replay helpers may reassert the same owner while their guard is held.
	void SetMutationOwner(const void *owner_token);
	const void *GetMutationOwner();

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override;
	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override;

private:
	void CleanupUnusedSourceDeltas(ClientContext &context, const vector<RefreshMetadata::DeltaSource> &sources,
	                               vector<string> cleanup);
	void Release();

	mutex state_lock;
	unique_ptr<MutationLockGuard> mutation_guard;
	unordered_map<string, unique_ptr<SourceAdmissionGuard>> source_write_admissions;
	unordered_map<string, unique_ptr<SourceAdmissionGuard>> source_publication_locks;
	ClientContext &owner;
	const void *mutation_owner;
	vector<string> deferred_delta_cleanup;
	vector<RefreshMetadata::DeltaSource> deferred_source_delta_cleanup;
	vector<RefreshMetadata::DeltaSource> allocated_source_deltas;
	unordered_set<string> dropped_source_deltas;
};

} // namespace duckdb

#endif // REFRESH_LOCKS_HPP
