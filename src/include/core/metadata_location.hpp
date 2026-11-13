#ifndef OPENIVM_METADATA_LOCATION_HPP
#define OPENIVM_METADATA_LOCATION_HPP

#include "duckdb.hpp"
#include "core/openivm_constants.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection.hpp"

#include <mutex>

namespace duckdb {

namespace openivm {
// Database-wide placement of OpenIVM metadata (definitions, dependencies, watermarks,
// refresh history, profiles). Empty catalog + schema 'main' keeps the legacy placement.
constexpr const char *METADATA_CATALOG_SETTING = "openivm_metadata_catalog";
// Refresh lease duration for remote (shared) metadata catalogs.
constexpr const char *METADATA_LEASE_SETTING = "openivm_metadata_lease_seconds";
// Test-only crash and lease-takeover injection for the split metadata/data refresh protocol.
constexpr const char *TEST_FAIL_POINT_SETTING = "openivm_test_fail_point";
// Test-only SQL that the concurrent_commit_* fail points commit from another connection.
constexpr const char *TEST_CONCURRENT_SQL_SETTING = "openivm_test_concurrent_sql";
// Durable pointer, stored in <data catalog>.main, to the metadata location that owns
// the OpenIVM objects of that catalog. Read on every resolution so a reopened
// database finds its metadata without session configuration.
constexpr const char *METADATA_MARKER_TABLE = "openivm_metadata_location";
// An atomic remote schema move leaves a pointer alongside the old schema so
// frontend markers can follow it without a cross-database metadata write.
constexpr const char *METADATA_REDIRECT_TABLE = "openivm_metadata_redirect";
// One row per view whose refresh is owned by a client of a remote metadata catalog.
constexpr const char *REFRESH_LEASE_TABLE = "openivm_refresh_leases";
} // namespace openivm

// Where one set of OpenIVM metadata tables lives. Backing (openivm_data_*) and delta
// tables never follow it: backing tables stay next to the user-facing view and source
// delta tables next to their base tables.
struct MetadataLocation {
	MetadataLocation() = default;
	MetadataLocation(string catalog_p, string schema_p)
	    : catalog(std::move(catalog_p)), schema(std::move(schema_p)), catalog_type("duckdb") {
	}

	string catalog;
	string schema;
	// duckdb_databases().type: "duckdb" (native file/memory) or "postgres" (remote SQL).
	string catalog_type;
	bool read_only = false;
	// Chosen by the openivm_metadata_* settings or a durable marker, not by the legacy
	// rule (the view's native catalog, else the default database; schema main).
	bool explicit_placement = false;
	// Chosen by the settings themselves (as opposed to a marker).
	bool configured = false;

	bool IsNative() const;
	string Prefix() const;
	string Table(const string &table_name) const;
	string DisplayName() const;
	bool Matches(const string &catalog_name, const string &schema_name) const;
	bool Matches(const MetadataLocation &other) const;
};

class MetadataLocator {
public:
	// The metadata location for a view stored in `view_catalog` (empty: the caller's
	// default catalog). Throws when the selected catalog is missing or unsupported,
	// or when a durable marker contradicts the configured placement.
	static MetadataLocation At(Connection &con, const string &catalog, const string &schema);
	static MetadataLocation Resolve(ClientContext &context, Connection &con, const string &view_catalog);
	// Point `con` at `location` so unqualified metadata table names resolve there.
	// Creates the selected schema of an explicit placement when it does not exist yet.
	static void Use(Connection &con, const MetadataLocation &location);
	// Every location that holds OpenIVM delta-table metadata: the configured location,
	// marker targets and legacy native catalogs. Delta cleanup must consult all of them.
	static vector<MetadataLocation> All(Connection &con, const string &table = openivm::DELTA_TABLES_TABLE);

	// Configured placement only refuses to coexist with materialized views registered
	// in another location; existing databases must be migrated explicitly.
	static void ValidateExclusive(Connection &con, const MetadataLocation &location);
	static void RequireWritable(const MetadataLocation &location, const string &operation);
	// DuckDB commits writes to only one attached database per transaction.
	static bool SpansDatabases(const MetadataLocation &location, const string &data_catalog);
	static void RejectExplicitTransaction(ClientContext &context, const MetadataLocation &location,
	                                      const string &data_catalog, const string &operation);
	// DDL that records `location` as the owner of the OpenIVM objects in `data_catalog`.
	static vector<string> MarkerDDL(Connection &con, const MetadataLocation &location, const string &data_catalog);
	// Portable form of "INSERT OR REPLACE": remote catalogs need delete + insert.
	static vector<string> ReplaceRowsSQL(const MetadataLocation &location, const string &insert_or_replace_sql,
	                                     const string &delete_predicate_sql);
	static bool IsNativeCatalog(Connection &con, const string &catalog);
	// The database a fresh connection starts in; external view catalogs keep their
	// internal tables there regardless of the metadata placement.
	static string PhysicalDefaultCatalog(DatabaseInstance &db);
	static int64_t LeaseSeconds(ClientContext &context);

	// Test-only crash injection: throws SimulatedCrashException when
	// openivm_test_fail_point equals `point`.
	static void FailPoint(ClientContext &context, const string &point);
	// Test-only: whether openivm_test_fail_point equals `point`.
	static bool TestPoint(ClientContext &context, const string &point);
};

// Raised by a test fail point. Handlers must not run compensation for it: a crashed
// process cannot clean up either, so recovery has to come from durable state.
class SimulatedCrashException : public Exception {
public:
	explicit SimulatedCrashException(const string &point);
};

// Mutual exclusion of refreshes and lifecycle changes across clients sharing remote metadata.
// Native metadata is single-process (DuckDB file lock) and uses the mutation gate.
// A live holder renews its lease in the background, so only a crashed or partitioned
// client's lease can expire and be taken over. Lease times are PostgreSQL server times.
class RefreshLease {
public:
	RefreshLease(ClientContext &context, const MetadataLocation &location, const string &view_name);
	~RefreshLease();

	bool Active() const {
		return !token.empty();
	}
	// A renewal found the lease taken over or failed.
	bool Lost() const;
	// Lost, or the local deadline passed: the last successful renewal was sent more than
	// two thirds of the lease ago, so another client may take over soon. A hung renewal
	// never reports back; only this deadline detects it.
	bool Expired() const;
	// Throws unless this client may still write materialized view data under the lease.
	// Called before the data phase, before every data statement and before data commit.
	void Require(const string &display_name) const;
	// Runs inside the caller's open metadata transaction, before its other statements.
	// Writes this client's lease row, so a concurrent takeover either waits for that
	// transaction or makes it fail. Returns an error unless exactly one row was owned.
	// Always succeeds for native metadata.
	string Fence(Connection &con) const;
	// Reconfirm ownership before destructive failure cleanup. Failure to contact the
	// metadata server must retain objects, never remove a new owner's objects.
	bool ConfirmOwnership() const;
	// Holds back this client's background renewal; keep it from before Fence() until the
	// fenced transaction commits or rolls back. Empty for native metadata.
	std::unique_lock<std::timed_mutex> PauseRenewal() const;
	// Leave the lease in place, as a crashed process would.
	void Abandon();
	// Test-only: another client took the lease over after it expired. `notice_locally`
	// also makes this client observe the loss, as its deadline would.
	void SimulateTakeover(bool notice_locally);

	struct Heartbeat;

private:
	DatabaseInstance &db;
	MetadataLocation location;
	string view_name;
	string key;
	string token;
	int64_t lease_seconds = 0;
	shared_ptr<Heartbeat> heartbeat;
};

class MutationLockGuard;

// DDL leases outlive planning and its transaction, including staged execution and
// failure cleanup. QueryEnd also releases them after a planning error.
class LifecycleLeaseState : public ClientContextState {
public:
	static LifecycleLeaseState &Get(ClientContext &context);
	static optional_ptr<LifecycleLeaseState> TryGet(ClientContext &context);
	static string SourceKey(const string &catalog, const string &schema, const string &table);
	~LifecycleLeaseState() override;
	bool Active() const {
		return !leases.empty();
	}
	void Acquire(ClientContext &context, const MetadataLocation &location, vector<string> view_names);
	void Require() const;
	bool CanCleanup() const;
	void SimulateTakeover();
	void QueryEnd() override;

private:
	unique_ptr<MutationLockGuard> mutation_guard;
	vector<pair<string, unique_ptr<RefreshLease>>> leases;
};

} // namespace duckdb

#endif // OPENIVM_METADATA_LOCATION_HPP
