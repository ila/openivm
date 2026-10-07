#include "core/metadata_location.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <random>
#include <thread>
#include <unordered_map>

namespace duckdb {

static constexpr const char *NATIVE_CATALOG_TYPE = "duckdb";
static constexpr const char *POSTGRES_CATALOG_TYPE = "postgres";

bool MetadataLocation::IsNative() const {
	return StringUtil::CIEquals(catalog_type, NATIVE_CATALOG_TYPE);
}

string MetadataLocation::Prefix() const {
	return SqlUtils::QualifiedPrefix(catalog, schema);
}

string MetadataLocation::Table(const string &table_name) const {
	return SqlUtils::FullName(catalog, schema, table_name);
}

string MetadataLocation::DisplayName() const {
	return catalog + "." + schema;
}

bool MetadataLocation::Matches(const string &catalog_name, const string &schema_name) const {
	return StringUtil::CIEquals(catalog, catalog_name) && StringUtil::CIEquals(schema, schema_name);
}

bool MetadataLocation::Matches(const MetadataLocation &other) const {
	return Matches(other.catalog, other.schema);
}

SimulatedCrashException::SimulatedCrashException(const string &point)
    : Exception(ExceptionType::IO, "OpenIVM test fail point '" + point + "' simulated a process crash") {
}

namespace {

struct AttachedCatalog {
	bool found = false;
	string name;
	string type;
	bool read_only = false;
};

AttachedCatalog DescribeCatalog(Connection &con, const string &catalog) {
	AttachedCatalog info;
	if (catalog.empty()) {
		return info;
	}
	auto result = con.Query("SELECT database_name, type, readonly FROM duckdb_databases() WHERE NOT internal AND "
	                        "lower(database_name) = lower(" +
	                        Value(catalog).ToSQLString() + ")");
	if (result->HasError()) {
		throw CatalogException("OpenIVM could not resolve metadata catalog: %s", result->GetError());
	}
	if (result->RowCount() == 0) {
		return info;
	}
	info.found = true;
	info.name = result->GetValue(0, 0).ToString();
	info.type = result->GetValue(1, 0).IsNull() ? "" : result->GetValue(1, 0).ToString();
	info.read_only = !result->GetValue(2, 0).IsNull() && result->GetValue(2, 0).GetValue<bool>();
	return info;
}

string SettingText(ClientContext &context, const char *name) {
	Value value;
	if (!context.TryGetCurrentSetting(name, value) || value.IsNull()) {
		return "";
	}
	return value.ToString();
}

void RequireSupportedType(const AttachedCatalog &info) {
	if (StringUtil::CIEquals(info.type, NATIVE_CATALOG_TYPE) ||
	    StringUtil::CIEquals(info.type, POSTGRES_CATALOG_TYPE)) {
		return;
	}
	throw NotImplementedException(
	    "OpenIVM metadata catalog '%s' has type '%s'. Supported metadata catalogs are attached native DuckDB "
	    "databases and PostgreSQL databases attached with the postgres extension (TYPE postgres).",
	    info.name, info.type);
}

MetadataLocation FromCatalog(const AttachedCatalog &info, const string &schema, bool explicit_placement,
                             bool configured) {
	MetadataLocation location;
	location.catalog = info.name;
	location.schema = schema;
	location.catalog_type = info.type;
	location.read_only = info.read_only;
	location.explicit_placement = explicit_placement;
	location.configured = configured;
	return location;
}

bool TryConfigured(ClientContext &context, Connection &con, MetadataLocation &out) {
	auto catalog = SettingText(context, openivm::METADATA_CATALOG_SETTING);
	auto schema = SettingText(context, openivm::METADATA_SCHEMA_SETTING);
	if (schema.empty()) {
		schema = DEFAULT_SCHEMA;
	}
	if (catalog.empty() && StringUtil::CIEquals(schema, DEFAULT_SCHEMA)) {
		return false;
	}
	if (catalog.empty()) {
		// Placement is database-wide, so it must not follow a session's USE.
		catalog = MetadataLocator::PhysicalDefaultCatalog(DatabaseInstance::GetDatabase(context));
	}
	auto info = DescribeCatalog(con, catalog);
	if (!info.found) {
		throw CatalogException("OpenIVM metadata catalog '%s' (openivm_metadata_catalog) is not attached. ATTACH it "
		                       "before creating, refreshing or dropping materialized views.",
		                       catalog);
	}
	RequireSupportedType(info);
	out = FromCatalog(info, schema, true, true);
	return true;
}

// A marker records which metadata location owns the OpenIVM objects of a native catalog.
bool ReadMarker(Connection &con, const string &catalog, MetadataLocation &out) {
	auto info = DescribeCatalog(con, catalog);
	if (!info.found || !StringUtil::CIEquals(info.type, NATIVE_CATALOG_TYPE)) {
		return false;
	}
	if (!con.TableInfo(info.name, DEFAULT_SCHEMA, openivm::METADATA_MARKER_TABLE)) {
		return false;
	}
	auto marker = SqlUtils::FullName(info.name, DEFAULT_SCHEMA, openivm::METADATA_MARKER_TABLE);
	auto rows = con.Query("SELECT metadata_catalog, metadata_schema FROM " + marker);
	if (rows->HasError()) {
		throw CatalogException("OpenIVM could not read metadata location marker %s: %s", marker, rows->GetError());
	}
	if (rows->RowCount() != 1 || rows->GetValue(0, 0).IsNull() || rows->GetValue(1, 0).IsNull()) {
		throw CatalogException("OpenIVM metadata location marker %s must contain exactly one non-NULL row", marker);
	}
	auto target_catalog = rows->GetValue(0, 0).ToString();
	auto target_schema = rows->GetValue(1, 0).ToString();
	auto target = DescribeCatalog(con, target_catalog);
	if (!target.found) {
		throw CatalogException("OpenIVM metadata for materialized views in catalog '%s' is stored in %s.%s, which is "
		                       "not attached. ATTACH that database as '%s' before using these materialized views.",
		                       info.name, target_catalog, target_schema, target_catalog);
	}
	RequireSupportedType(target);
	out = FromCatalog(target, target_schema, true, false);
	OPENIVM_DEBUG_PRINT("[METADATA] Marker in %s selects %s\n", info.name.c_str(), out.DisplayName().c_str());
	return true;
}

} // namespace

MetadataLocation MetadataLocator::Resolve(ClientContext &context, Connection &con, const string &view_catalog) {
	auto catalog = view_catalog;
	if (catalog.empty()) {
		catalog = ClientData::Get(context).catalog_search_path->GetDefault().catalog;
	}
	MetadataLocation location;
	if (TryConfigured(context, con, location)) {
		// External (DuckLake) view catalogs keep their marker in the physical default database.
		auto marker_catalog = catalog;
		if (!catalog.empty() && !IsNativeCatalog(con, catalog)) {
			marker_catalog = PhysicalDefaultCatalog(DatabaseInstance::GetDatabase(context));
		}
		MetadataLocation marked;
		if (!marker_catalog.empty() && ReadMarker(con, marker_catalog, marked) && !marked.Matches(location)) {
			throw CatalogException("OpenIVM metadata for catalog '%s' is recorded in %s, but openivm_metadata_catalog/"
			                       "openivm_metadata_schema select %s. Reset the settings or migrate the metadata; "
			                       "OpenIVM does not split one catalog's materialized views across metadata locations.",
			                       catalog, marked.DisplayName(), location.DisplayName());
		}
		return location;
	}
	// Legacy placement: native view catalogs own their metadata; external catalogs use the
	// connection's default native database.
	auto info = DescribeCatalog(con, catalog);
	if (!info.found || !StringUtil::CIEquals(info.type, NATIVE_CATALOG_TYPE)) {
		// Same resolution as the historical unqualified "USE main" on this connection.
		auto selected = con.Query("USE main");
		if (selected->HasError()) {
			throw CatalogException("OpenIVM could not select metadata catalog: %s", selected->GetError());
		}
		auto current = con.Query("SELECT current_database()");
		if (current->HasError()) {
			throw CatalogException("OpenIVM could not resolve metadata catalog: %s", current->GetError());
		}
		info = DescribeCatalog(con, current->GetValue(0, 0).ToString());
	}
	if (!info.found) {
		return location;
	}
	MetadataLocation marked;
	if (ReadMarker(con, info.name, marked)) {
		return marked;
	}
	return FromCatalog(info, DEFAULT_SCHEMA, false, false);
}

void MetadataLocator::Use(Connection &con, const MetadataLocation &location) {
	string target = SqlUtils::QuoteIdentifier(location.schema.empty() ? string(DEFAULT_SCHEMA) : location.schema);
	if (!location.catalog.empty()) {
		target = SqlUtils::QuoteIdentifier(location.catalog) + "." + target;
	}
	auto result = con.Query("USE " + target);
	if (result->HasError() && location.explicit_placement) {
		// The selected schema is part of the placement; create it on first use.
		if (location.read_only) {
			throw CatalogException("OpenIVM metadata schema %s does not exist and its catalog is attached read-only",
			                       location.DisplayName());
		}
		auto created = con.Query("CREATE SCHEMA IF NOT EXISTS " + target);
		if (created->HasError()) {
			throw CatalogException("OpenIVM could not create metadata schema %s: %s", location.DisplayName(),
			                       created->GetError());
		}
		result = con.Query("USE " + target);
	}
	if (result->HasError()) {
		throw CatalogException("OpenIVM could not select metadata catalog: %s", result->GetError());
	}
	OPENIVM_DEBUG_PRINT("[METADATA] Selected %s\n", target.c_str());
}

vector<MetadataLocation> MetadataLocator::All(Connection &con) {
	vector<MetadataLocation> result;
	auto add = [&](const MetadataLocation &location) {
		for (auto &existing : result) {
			if (existing.Matches(location)) {
				return;
			}
		}
		if (!con.TableInfo(location.catalog, location.schema, openivm::DELTA_TABLES_TABLE)) {
			return;
		}
		result.push_back(location);
	};
	MetadataLocation configured;
	if (TryConfigured(*con.context, con, configured)) {
		add(configured);
	}
	// Source metadata lives in native catalogs. Enumerating external tables also
	// opens their metadata transactions and can block on unrelated DuckLake writes.
	auto rows = con.Query("SELECT database_name, readonly FROM duckdb_databases() WHERE type='duckdb' "
	                      "AND NOT internal ORDER BY database_name");
	if (rows->HasError()) {
		throw CatalogException("OpenIVM could not locate source metadata: %s", rows->GetError());
	}
	for (idx_t row = 0; row < rows->RowCount(); row++) {
		AttachedCatalog info;
		info.found = true;
		info.name = rows->GetValue(0, row).ToString();
		info.type = NATIVE_CATALOG_TYPE;
		info.read_only = !rows->GetValue(1, row).IsNull() && rows->GetValue(1, row).GetValue<bool>();
		MetadataLocation marked;
		if (ReadMarker(con, info.name, marked)) {
			add(marked);
		}
		add(FromCatalog(info, DEFAULT_SCHEMA, false, false));
	}
	return result;
}

void MetadataLocator::ValidateExclusive(Connection &con, const MetadataLocation &location) {
	if (!location.configured) {
		return;
	}
	for (auto &other : All(con)) {
		if (other.Matches(location) || !con.TableInfo(other.catalog, other.schema, openivm::VIEWS_TABLE)) {
			continue;
		}
		auto count = con.Query("SELECT count(*) FROM " + other.Table(openivm::VIEWS_TABLE));
		if (count->HasError()) {
			throw CatalogException("OpenIVM could not inspect metadata in %s: %s", other.DisplayName(),
			                       count->GetError());
		}
		auto registered = count->GetValue(0, 0).GetValue<int64_t>();
		if (registered > 0) {
			throw CatalogException(
			    "openivm_metadata_catalog/openivm_metadata_schema select %s, but %s materialized view(s) are already "
			    "registered in %s. OpenIVM keeps one database's metadata in one location: migrate those views (see "
			    "docs/internals/metadata-placement.md) or reset the settings.",
			    location.DisplayName(), to_string(registered), other.DisplayName());
		}
	}
}

void MetadataLocator::RequireWritable(const MetadataLocation &location, const string &operation) {
	if (location.explicit_placement && location.read_only) {
		throw InvalidInputException("Cannot %s: OpenIVM metadata catalog '%s' is attached read-only. Reading "
		                            "materialized views does not need write access; reattach it read-write to change "
		                            "them.",
		                            operation, location.catalog);
	}
}

bool MetadataLocator::SpansDatabases(const MetadataLocation &location, const string &data_catalog) {
	return !data_catalog.empty() && !StringUtil::CIEquals(location.catalog, data_catalog);
}

void MetadataLocator::RejectExplicitTransaction(ClientContext &context, const MetadataLocation &location,
                                                const string &data_catalog, const string &operation) {
	if (context.transaction.IsAutoCommit() || !location.explicit_placement ||
	    !SpansDatabases(location, data_catalog)) {
		return;
	}
	throw TransactionException(
	    "%s cannot run inside an explicit transaction: OpenIVM metadata is stored in %s, outside database '%s', and "
	    "DuckDB cannot commit writes to both atomically. Run it in autocommit mode, where OpenIVM uses its "
	    "crash-safe refresh protocol, or keep the metadata in the same database as the materialized view.",
	    operation, location.DisplayName(), data_catalog);
}

vector<string> MetadataLocator::MarkerDDL(Connection &con, const MetadataLocation &location,
                                          const string &data_catalog) {
	if (!location.explicit_placement || data_catalog.empty() || !IsNativeCatalog(con, data_catalog) ||
	    location.Matches(data_catalog, DEFAULT_SCHEMA)) {
		return {};
	}
	// One statement, so the marker is never observed half-written.
	auto marker = SqlUtils::FullName(data_catalog, DEFAULT_SCHEMA, openivm::METADATA_MARKER_TABLE);
	return {"CREATE OR REPLACE TABLE " + marker + " AS SELECT " + Value(location.catalog).ToSQLString() +
	        "::VARCHAR AS metadata_catalog, " + Value(location.schema).ToSQLString() + "::VARCHAR AS metadata_schema"};
}

vector<string> MetadataLocator::ReplaceRowsSQL(const MetadataLocation &location, const string &insert_or_replace_sql,
                                               const string &delete_predicate_sql) {
	static const string PREFIX = "insert or replace into ";
	if (location.catalog_type.empty() || location.IsNative() ||
	    !StringUtil::StartsWith(StringUtil::Lower(insert_or_replace_sql), PREFIX)) {
		return {insert_or_replace_sql};
	}
	// Remote SQL catalogs have no INSERT OR REPLACE. CREATE publishes metadata last, so
	// an interruption between the two statements leaves no row for a half-created view;
	// CREATE OR REPLACE then rebuilds it.
	auto rest = insert_or_replace_sql.substr(PREFIX.size());
	auto table = rest.substr(0, rest.find_first_of(" ("));
	return {"DELETE FROM " + table + " WHERE " + delete_predicate_sql, "INSERT INTO " + rest};
}

bool MetadataLocator::IsNativeCatalog(Connection &con, const string &catalog) {
	auto info = DescribeCatalog(con, catalog);
	return info.found && StringUtil::CIEquals(info.type, NATIVE_CATALOG_TYPE);
}

string MetadataLocator::PhysicalDefaultCatalog(DatabaseInstance &db) {
	Connection con(db);
	return DatabaseManager::GetDefaultDatabase(*con.context);
}

int64_t MetadataLocator::LeaseSeconds(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting(openivm::METADATA_LEASE_SETTING, value) && !value.IsNull()) {
		// A zero lease would expire before the first statement.
		return MaxValue<int64_t>(1, value.GetValue<int64_t>());
	}
	return 600;
}

bool MetadataLocator::TestPoint(ClientContext &context, const string &point) {
	return StringUtil::CIEquals(SettingText(context, openivm::TEST_FAIL_POINT_SETTING), point);
}

void MetadataLocator::FailPoint(ClientContext &context, const string &point) {
	if (TestPoint(context, point)) {
		OPENIVM_DEBUG_PRINT("[METADATA] Fail point %s reached\n", point.c_str());
		throw SimulatedCrashException(point);
	}
}

namespace {

int64_t SteadyNowNs() {
	return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
	    .count();
}

// One client's row in the lease table of a PostgreSQL metadata catalog.
struct LeaseRow {
	string catalog;
	string table;
	string view_name;
	string owner;
	int64_t lease_seconds;
};

LeaseRow MakeLeaseRow(const MetadataLocation &location, const string &view_name, const string &owner,
                      int64_t lease_seconds) {
	return {location.catalog, location.Table(openivm::REFRESH_LEASE_TABLE), view_name, owner, lease_seconds};
}

// Lease times come from the PostgreSQL server clock, never from a client's: DuckDB
// evaluates now() locally, so every lease timestamp is read through postgres_query.
// All clients then compare and extend leases on one clock, and client clock skew cannot
// make a live lease look expired. Returns an error, or SQL literals for the server's
// current UTC time and that time plus the lease.
string ReadServerClock(Connection &con, const LeaseRow &row, string &server_now, string &lease_until) {
	auto remote_sql = "SELECT server_now, server_now + INTERVAL '" + to_string(row.lease_seconds) +
	                  " seconds' FROM (SELECT clock_timestamp() AT TIME ZONE 'UTC' AS server_now) server_clock";
	auto clock = con.Query("SELECT * FROM postgres_query(" + Value(row.catalog).ToSQLString() + ", " +
	                       Value(remote_sql).ToSQLString() + ")");
	if (clock->HasError()) {
		return "could not read the PostgreSQL server clock: " + clock->GetError();
	}
	if (clock->RowCount() != 1 || clock->GetValue(0, 0).IsNull() || clock->GetValue(1, 0).IsNull()) {
		return "the PostgreSQL server clock returned no time";
	}
	server_now = clock->GetValue(0, 0).DefaultCastAs(LogicalType::TIMESTAMP).ToSQLString();
	lease_until = clock->GetValue(1, 0).DefaultCastAs(LogicalType::TIMESTAMP).ToSQLString();
	return "";
}

// Extends the lease to the server clock (read after the caller's send time) plus the lease.
// Returns an error, or sets `owned` when exactly one row, this owner's, was extended.
string RenewLeaseRow(Connection &con, const LeaseRow &row, bool &owned) {
	owned = false;
	string server_now, lease_until;
	auto error = ReadServerClock(con, row, server_now, lease_until);
	if (!error.empty()) {
		return error;
	}
	auto renewed = con.Query("UPDATE " + row.table + " SET lease_until = " + lease_until + " WHERE view_name = " +
	                         Value(row.view_name).ToSQLString() + " AND owner = " + Value(row.owner).ToSQLString());
	if (renewed->HasError()) {
		return renewed->GetError();
	}
	owned = renewed->RowCount() == 1 && !renewed->GetValue(0, 0).IsNull() &&
	        renewed->GetValue(0, 0).GetValue<int64_t>() == 1;
	return "";
}

} // namespace

struct RefreshLease::Heartbeat {
	std::mutex lock;
	std::condition_variable wake;
	bool stop = false;
	std::atomic<bool> lost {false};
	// Steady-clock time (ns) after which this client stops writing data: the send time of
	// the last successful renewal plus two thirds of the lease. The server reads its clock
	// for lease_until after that send time, so on the server's clock the lease ends at
	// least a third of the lease after this deadline (assuming the two clocks advance at
	// comparable rates; their offsets do not matter).
	std::atomic<int64_t> deadline_ns {0};
	// Held by a renewal and by a fenced metadata transaction, so this client's own
	// heartbeat never writes the lease row concurrently with its fence.
	std::timed_mutex renew_lock;
	std::thread thread;

	void Stop() {
		{
			std::lock_guard<std::mutex> guard(lock);
			stop = true;
		}
		wake.notify_all();
		if (thread.joinable()) {
			thread.join();
		}
	}
};

namespace {

struct HeldLease {
	string token;
	int64_t lease_seconds = 0;
	idx_t depth = 0;
	bool abandoned = false;
	shared_ptr<RefreshLease::Heartbeat> heartbeat;
};

struct LeaseRegistry {
	std::mutex lock;
	std::unordered_map<string, HeldLease> held;
};

LeaseRegistry &Leases() {
	static LeaseRegistry registry;
	return registry;
}

} // namespace

RefreshLease::RefreshLease(ClientContext &context, const MetadataLocation &location_p, const string &view_name_p)
    : db(DatabaseInstance::GetDatabase(context)), location(location_p), view_name(view_name_p) {
	if (!location.explicit_placement || location.IsNative()) {
		return;
	}
	// Re-entrant within one database instance: hooks and cascades nest refreshes.
	key = to_string(reinterpret_cast<uintptr_t>(&db)) + "\n" + StringUtil::Lower(location.catalog) + "\n" +
	      StringUtil::Lower(location.schema) + "\n" + view_name;
	auto &registry = Leases();
	{
		std::lock_guard<std::mutex> guard(registry.lock);
		auto entry = registry.held.find(key);
		if (entry != registry.held.end()) {
			entry->second.depth++;
			token = entry->second.token;
			lease_seconds = entry->second.lease_seconds;
			heartbeat = entry->second.heartbeat;
			return;
		}
	}
	lease_seconds = MetadataLocator::LeaseSeconds(context);
	const int64_t lease_ns = lease_seconds * 1000000000LL;
	const int64_t usable_ns = lease_ns - lease_ns / 3;
	auto acquire_sent_ns = SteadyNowNs();
	std::random_device entropy;
	std::mt19937_64 generator((static_cast<uint64_t>(entropy()) << 32) ^ entropy() ^
	                          static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
	auto candidate = "openivm-" + to_string(generator()) + "-" + to_string(generator());
	auto row = MakeLeaseRow(location, view_name, candidate, lease_seconds);
	auto view_literal = Value(view_name).ToSQLString();
	Connection con(db);
	con.BeginTransaction();
	string server_now, lease_until;
	auto clock_error = ReadServerClock(con, row, server_now, lease_until);
	if (!clock_error.empty()) {
		con.Query("ROLLBACK");
		throw IOException("Cannot refresh materialized view '%s': OpenIVM %s (metadata catalog %s)", view_name,
		                  clock_error, location.DisplayName());
	}
	// Both the expiry check and the new deadline are server times.
	auto result = con.Query("DELETE FROM " + row.table + " WHERE view_name = " + view_literal + " AND lease_until < " +
	                        server_now);
	if (!result->HasError()) {
		result = con.Query("INSERT INTO " + row.table + " (view_name, owner, lease_until) VALUES (" + view_literal +
		                   ", " + Value(candidate).ToSQLString() + ", " + lease_until + ")");
	}
	if (!result->HasError()) {
		result = con.Query("COMMIT");
	} else {
		con.Query("ROLLBACK");
	}
	if (result->HasError()) {
		string holder = "unknown";
		auto current = con.Query("SELECT lease_until FROM " + row.table + " WHERE view_name = " + view_literal);
		if (!current->HasError() && current->RowCount() > 0 && !current->GetValue(0, 0).IsNull()) {
			holder = current->GetValue(0, 0).ToString();
		}
		throw TransactionException("Materialized view '%s' is being refreshed by another OpenIVM client sharing "
		                           "metadata catalog %s (lease held until %s UTC, PostgreSQL server time). Retry "
		                           "after that refresh finishes or its lease expires "
		                           "(openivm_metadata_lease_seconds). Details: %s",
		                           view_name, location.DisplayName(), holder, result->GetError());
	}
	token = candidate;
	heartbeat = make_shared_ptr<Heartbeat>();
	heartbeat->deadline_ns = acquire_sent_ns + usable_ns;
	// Renew at a third of the lease, so a healthy holder's deadline stays at least a third
	// of the lease ahead.
	auto renew_every = std::chrono::milliseconds(MaxValue<int64_t>(1, lease_seconds * 1000 / 3));
	row.owner = token;
	auto database = db.shared_from_this();
	auto state = heartbeat.get();
	heartbeat->thread = std::thread([state, database, row, renew_every, usable_ns]() {
		while (true) {
			{
				std::unique_lock<std::mutex> guard(state->lock);
				if (state->wake.wait_for(guard, renew_every, [state]() { return state->stop; })) {
					return;
				}
			}
			if (state->lost) {
				return;
			}
			try {
				// Waits while a fenced metadata transaction of this client is open.
				std::lock_guard<std::timed_mutex> renewing(state->renew_lock);
				auto sent_ns = SteadyNowNs();
				Connection renew_con(*database);
				bool owned = false;
				auto error = RenewLeaseRow(renew_con, row, owned);
				if (!error.empty() || !owned) {
					OPENIVM_DEBUG_PRINT("[LEASE] Renewal failed: %s\n", error.empty() ? "not owned" : error.c_str());
					state->lost = true;
				} else {
					state->deadline_ns = sent_ns + usable_ns;
				}
			} catch (...) {
				state->lost = true;
			}
		}
	});
	HeldLease held;
	held.token = token;
	held.lease_seconds = lease_seconds;
	held.depth = 1;
	held.heartbeat = heartbeat;
	std::lock_guard<std::mutex> guard(registry.lock);
	registry.held[key] = std::move(held);
	OPENIVM_DEBUG_PRINT("[LEASE] Acquired refresh lease for %s in %s\n", view_name.c_str(),
	                    location.DisplayName().c_str());
}

RefreshLease::~RefreshLease() {
	if (token.empty()) {
		return;
	}
	bool last = false;
	bool release = false;
	{
		auto &registry = Leases();
		std::lock_guard<std::mutex> guard(registry.lock);
		auto entry = registry.held.find(key);
		if (entry != registry.held.end() && --entry->second.depth == 0) {
			last = true;
			release = !entry->second.abandoned;
			registry.held.erase(entry);
		}
	}
	if (!last) {
		return;
	}
	if (heartbeat) {
		heartbeat->Stop();
	}
	if (!release) {
		return;
	}
	try {
		Connection con(db);
		auto result = con.Query("DELETE FROM " + location.Table(openivm::REFRESH_LEASE_TABLE) +
		                        " WHERE view_name = " + Value(view_name).ToSQLString() +
		                        " AND owner = " + Value(token).ToSQLString());
		if (result->HasError()) {
			OPENIVM_DEBUG_PRINT("[LEASE] Release failed: %s\n", result->GetError().c_str());
		}
	} catch (std::exception &ex) {
		// The lease expires on its own; never mask the refresh outcome.
		OPENIVM_DEBUG_PRINT("[LEASE] Release failed: %s\n", ex.what());
	}
}

bool RefreshLease::Lost() const {
	return heartbeat && heartbeat->lost.load();
}

bool RefreshLease::Expired() const {
	return heartbeat && (heartbeat->lost.load() || SteadyNowNs() >= heartbeat->deadline_ns.load());
}

void RefreshLease::Require(const string &display_name) const {
	if (token.empty() || !Expired()) {
		return;
	}
	throw TransactionException(
	    "IVM refresh of '%s' stopped before writing more materialized view data: its refresh lease in metadata "
	    "catalog %s was taken over or could not be renewed in time. Uncommitted data changes were rolled back; the "
	    "next refresh recomputes the view if earlier data statements committed.",
	    display_name, location.DisplayName());
}

string RefreshLease::Fence(Connection &con) const {
	if (token.empty()) {
		return "";
	}
	// Reading the row would not conflict with a takeover committing before this
	// transaction; writing it does (row lock, or a serialization failure).
	bool owned = false;
	auto error = RenewLeaseRow(con, MakeLeaseRow(location, view_name, token, lease_seconds), owned);
	if (!error.empty()) {
		return "OpenIVM could not confirm the refresh lease for '" + view_name + "': " + error;
	}
	if (!owned) {
		return "OpenIVM lost the refresh lease for '" + view_name +
		       "' to another client; its watermark was not advanced and the next refresh recomputes it";
	}
	return "";
}

std::unique_lock<std::timed_mutex> RefreshLease::PauseRenewal() const {
	if (!heartbeat) {
		return std::unique_lock<std::timed_mutex>();
	}
	std::unique_lock<std::timed_mutex> pause(heartbeat->renew_lock, std::defer_lock);
	// A renewal hung on an unreachable server must not block the refresh indefinitely.
	// Without the pause a concurrent renewal can only make the fenced transaction fail
	// (a serialization failure), never commit stale watermarks.
	if (!pause.try_lock_for(std::chrono::seconds(MaxValue<int64_t>(1, lease_seconds / 3)))) {
		OPENIVM_DEBUG_PRINT("[LEASE] Fencing %s while a renewal is still pending\n", view_name.c_str());
	}
	return pause;
}

void RefreshLease::SimulateTakeover(bool notice_locally) {
	if (token.empty()) {
		return;
	}
	if (notice_locally) {
		heartbeat->lost = true;
	}
	// The new owner's lease has already expired, so the next client can recover at once.
	Connection con(db);
	auto result = con.Query("UPDATE " + location.Table(openivm::REFRESH_LEASE_TABLE) +
	                        " SET owner = 'openivm-simulated-takeover', lease_until = TIMESTAMP '2000-01-01' "
	                        "WHERE view_name = " +
	                        Value(view_name).ToSQLString() + " AND owner = " + Value(token).ToSQLString());
	if (result->HasError()) {
		throw IOException("OpenIVM test takeover failed: %s", result->GetError());
	}
}

void RefreshLease::Abandon() {
	if (token.empty()) {
		return;
	}
	auto &registry = Leases();
	std::lock_guard<std::mutex> guard(registry.lock);
	auto entry = registry.held.find(key);
	if (entry != registry.held.end()) {
		entry->second.abandoned = true;
	}
}

} // namespace duckdb
