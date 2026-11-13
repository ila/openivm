#include "core/metadata_json.hpp"
#include "core/refresh_metadata.hpp"

#include "core/openivm_debug.hpp"
#include "core/parser_create_mv_helpers.hpp"
#include "core/parser_ddl.hpp"
#include "core/refresh_locks.hpp"
#include "core/sql_utils.hpp"
#include "core/published_view.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_search_path.hpp"
#include "rules/column_hider.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/main/appender.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/planner/bound_constraint.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/table_filter_set.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/delete_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include <algorithm>
#include <functional>
#include <sstream>
#include <map>
#include <set>
#include <unordered_set>

namespace duckdb {

namespace {

// The metadata location a helper connection selected. Nested helpers reuse its schema,
// so one operation never splits its metadata reads and writes across schemas.
class MetadataLocationState : public ClientContextState {
public:
	MetadataLocation location;
};

constexpr const char *METADATA_LOCATION_STATE = "openivm_metadata_location";

optional_ptr<MetadataLocationState> TryGetLocationState(ClientContext &context) {
	return context.registered_state->Get<MetadataLocationState>(METADATA_LOCATION_STATE);
}

vector<string> CatalogSchemas(ClientContext &context, Catalog &catalog) {
	vector<string> schemas;
	// ScanSchemas holds the schema CatalogSet lock while invoking its callback.
	// Collect names only; resolving entries inside the callback reacquires that
	// same lock, even when looking up a table in another schema of this catalog.
	catalog.ScanSchemas(context,
	                    [&](SchemaCatalogEntry &schema) { schemas.push_back(schema.name.GetIdentifierName()); });
	std::sort(schemas.begin(), schemas.end());
	return schemas;
}

// Catalog versions describe structural visibility, including transaction-local DDL.
// Cache names only: ownership rows must still be read in the caller's snapshot.
class MetadataSchemaInventory : public ObjectCacheEntry {
public:
	explicit MetadataSchemaInventory(vector<string> schemas) : schemas(std::move(schemas)) {
	}
	static string ObjectType() {
		return "openivm_metadata_schemas";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override {
		idx_t size = sizeof(*this) + schemas.capacity() * sizeof(string);
		for (auto &schema : schemas) {
			size += schema.capacity();
		}
		return size;
	}
	const vector<string> schemas;
};

vector<string> FindMetadataSchemas(ClientContext &context, Catalog &catalog, const string &table) {
	auto version = catalog.IsDuckCatalog() ? catalog.GetCatalogVersion(context) : optional_idx();
	auto &cache = context.db->GetObjectCache();
	// Different live snapshots must not evict each other on every helper lookup.
	// Each immutable version has its own entry in DuckDB's memory-bounded LRU.
	auto key = MetadataSchemaInventory::ObjectType() + ":" + to_string(catalog.GetOid()) + ":" + table + ":" +
	           (version.IsValid() ? to_string(version.GetIndex()) : "");
	if (version.IsValid()) {
		auto cached = cache.Get<MetadataSchemaInventory>(key);
		if (cached) {
			return cached->schemas;
		}
	}
	vector<string> schemas;
	for (auto &schema : CatalogSchemas(context, catalog)) {
		if (catalog.GetEntry<TableCatalogEntry>(context,
		                                        QualifiedName(catalog.GetName(), Identifier(schema), Identifier(table)),
		                                        OnEntryNotFound::RETURN_NULL)) {
			schemas.push_back(schema);
		}
	}
	if (version.IsValid()) {
		cache.Put(key, make_shared_ptr<MetadataSchemaInventory>(schemas));
	}
	OPENIVM_DEBUG_PRINT("[METADATA] Discovered %zu schemas for %s in %s\n", schemas.size(), table.c_str(),
	                    catalog.GetName().c_str());
	return schemas;
}

bool SchemaExists(Connection &con, const string &catalog, const string &schema) {
	bool exists = false;
	con.context->RunFunctionInTransaction([&]() {
		exists = Catalog::GetSchema(*con.context, Identifier(catalog), Identifier(schema),
		                            OnEntryNotFound::RETURN_NULL) != nullptr;
	});
	return exists;
}

void SelectMetadataLocation(Connection &con, const MetadataLocation &location) {
	auto selected = SchemaExists(con, location.catalog, location.schema) ? location.schema : string(DEFAULT_SCHEMA);
	auto target = SqlUtils::QualifiedPrefix(location.catalog, selected);
	target.pop_back();
	auto result = con.Query("USE " + target);
	if (result->HasError()) {
		throw CatalogException("OpenIVM could not select metadata catalog: %s", result->GetError());
	}
	con.context->registered_state->GetOrCreate<MetadataLocationState>(METADATA_LOCATION_STATE)->location = location;
	OPENIVM_DEBUG_PRINT("[METADATA] Selected %s for metadata schema '%s'\n", target.c_str(), location.schema.c_str());
}

string SelectMetadataSchema(const vector<string> &schemas, const string &catalog, const string &preferred) {
	for (auto &schema : schemas) {
		if (StringUtil::CIEquals(schema, preferred)) {
			return schema;
		}
	}
	if (schemas.size() > 1) {
		throw CatalogException("OpenIVM metadata exists in schemas %s of catalog '%s'; set openivm_metadata_schema "
		                       "to the schema that should hold it",
		                       StringUtil::Join(schemas, ", "), catalog);
	}
	if (schemas.size() == 1) {
		// Existing metadata stays authoritative until PrepareMetadataSchema moves it.
		return schemas[0];
	}
	return preferred.empty() ? string(DEFAULT_SCHEMA) : preferred;
}

vector<string> TransactionMetadataSchemas(ClientContext &context, const string &catalog_name) {
	vector<string> schemas;
	if (context.transaction.IsAutoCommit() || !context.transaction.HasActiveTransaction()) {
		return schemas;
	}
	// Replay already pins the location used by this transaction's lifecycle statements.
	// A changed setting must not send later reads or writes to a second registry.
	if (auto state = TransactionalMVMetadataState::TryGet(context)) {
		for (auto &location : state->MetadataLocations()) {
			if (StringUtil::CIEquals(location.first, catalog_name)) {
				schemas.push_back(location.second);
			}
		}
	}
	if (schemas.empty()) {
		// The caller can also see metadata created without a replay record. A fresh
		// helper cannot discover these uncommitted schemas or registry tables.
		auto &catalog = Catalog::GetCatalog(context, Identifier(catalog_name));
		schemas = FindMetadataSchemas(context, catalog, openivm::VIEWS_TABLE);
	}
	return schemas;
}

// Compilation may USE the source schema after selecting its metadata location.
// Ownership reads must retain that location, including its transaction replay.
string MetadataOwnershipTable(Connection &con, const string &table) {
	if (con.TableInfo(Identifier(TEMP_CATALOG), Identifier(DEFAULT_SCHEMA), Identifier(table))) {
		return SqlUtils::FullName(TEMP_CATALOG, DEFAULT_SCHEMA, table);
	}
	return RefreshMetadata::MetadataTable(con, table);
}

// Owners and consumers can be registered outside the selected metadata catalog.
// Inspect the complete inventory, including locations created in this transaction.
bool AnyMetadataOwnership(Connection &con, const string &table,
                          optional_ptr<TransactionalMVMetadataState> transaction_state,
                          const std::function<bool(Connection &, const MetadataLocation &, const string &)> &matches) {
	auto current = RefreshMetadata::CurrentLocation(con);
	auto locations = RefreshMetadata::MetadataLocations(con, table);
	auto include_location = [&](const MetadataLocation &candidate) {
		for (auto &location : locations) {
			if (StringUtil::CIEquals(location.catalog, candidate.catalog) &&
			    StringUtil::CIEquals(location.schema, candidate.schema)) {
				return;
			}
		}
		locations.push_back(candidate);
	};
	if (transaction_state) {
		for (auto &location : transaction_state->MetadataLocations()) {
			include_location({location.first, location.second});
		}
	} else if (con.TableInfo(Identifier(table))) {
		include_location(current);
	}
	// Replay replaces TEMP shadows. Keep it on a separate helper so ownership
	// checks never change the compiler's selected location or transaction snapshot.
	unique_ptr<Connection> replay;
	if (transaction_state) {
		replay = make_uniq<Connection>(*con.context->db);
	}
	for (auto &location : locations) {
		auto target = SqlUtils::FullName(location.catalog, location.schema, table);
		auto reader = &con;
		if (replay) {
			RefreshMetadata::UseLocation(*replay, location);
			transaction_state->Apply(*replay);
			reader = replay.get();
			target = MetadataOwnershipTable(*reader, table);
		} else if (StringUtil::CIEquals(location.catalog, current.catalog) &&
		           StringUtil::CIEquals(location.schema, current.schema)) {
			target = MetadataOwnershipTable(*reader, table);
		}
		if (matches(*reader, location, target)) {
			return true;
		}
	}
	return false;
}

} // namespace

string RefreshMetadata::ConfiguredMetadataSchema(ClientContext &context) {
	Value value;
	if (context.TryGetCurrentSetting(Identifier(openivm::METADATA_SCHEMA_SETTING), value) && !value.IsNull()) {
		return value.ToString();
	}
	return "";
}

vector<string> RefreshMetadata::FindMetadataSchemas(Connection &con, const string &catalog, const string &table) {
	vector<string> schemas;
	con.context->RunFunctionInTransaction([&]() {
		schemas =
		    duckdb::FindMetadataSchemas(*con.context, Catalog::GetCatalog(*con.context, Identifier(catalog)), table);
	});
	return schemas;
}

string RefreshMetadata::ResolveMetadataSchema(Connection &con, const string &catalog, const string &preferred) {
	return SelectMetadataSchema(FindMetadataSchemas(con, catalog, openivm::VIEWS_TABLE), catalog, preferred);
}

MetadataLocation RefreshMetadata::ResolveLocation(ClientContext &context, Connection &con, const string &view_catalog) {
	auto location = MetadataLocator::Resolve(context, con, view_catalog);
	auto transaction_schemas = TransactionMetadataSchemas(context, location.catalog);
	if (!transaction_schemas.empty()) {
		location.schema = SelectMetadataSchema(transaction_schemas, location.catalog, location.schema);
	}
	return location;
}

MetadataLocation RefreshMetadata::UseCatalog(ClientContext &context, Connection &con, const string &view_catalog) {
	TransactionalMVLockState::Get(*con.context)
	    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
	auto location = ResolveLocation(context, con, view_catalog);
	SelectMetadataLocation(con, location);
	return location;
}

void RefreshMetadata::UseSameLocation(ClientContext &context, Connection &con) {
	TransactionalMVLockState::Get(*con.context)
	    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
	auto state = TryGetLocationState(context);
	if (!state || state->location.catalog.empty()) {
		UseCatalog(context, con);
		return;
	}
	SelectMetadataLocation(con, state->location);
}

void RefreshMetadata::UseLocation(Connection &con, const MetadataLocation &location) {
	SelectMetadataLocation(con, location);
}

MetadataLocation RefreshMetadata::CurrentLocation(Connection &con) {
	auto state = TryGetLocationState(*con.context);
	if (state && !state->location.catalog.empty()) {
		return state->location;
	}
	auto result = con.Query("SELECT current_database(), current_schema()");
	if (result->HasError() || result->RowCount() == 0) {
		throw CatalogException("OpenIVM could not resolve its metadata location: %s",
		                       result->HasError() ? result->GetError() : string("no current schema"));
	}
	return MetadataLocator::At(con, result->Collection().GetValue(0, 0).ToString(),
	                           result->Collection().GetValue(1, 0).ToString());
}

string RefreshMetadata::MetadataTable(Connection &con, const string &table) {
	auto location = CurrentLocation(con);
	return SqlUtils::FullName(location.catalog, location.schema, table);
}

void RefreshMetadata::PrepareMetadataSchema(ClientContext &context, const string &view_catalog, bool create_schema) {
	auto configured = ConfiguredMetadataSchema(context);
	// Explicit transactions keep using the existing location; their CREATE program
	// creates a missing schema transactionally.
	if (configured.empty() || !context.transaction.IsAutoCommit()) {
		return;
	}
	Connection con(*context.db);
	// Metadata INSERTs still enter the maintenance optimizer on this helper.
	TransactionalMVLockState::Get(*con.context)
	    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
	MutationLockGuard guard(context);
	auto location = MetadataLocator::Resolve(context, con, view_catalog);
	MetadataLocator::RequireWritable(location, "move metadata schema");
	auto catalog = location.catalog;
	auto current =
	    location.IsNative()
	        ? FindMetadataSchemas(con, catalog, openivm::VIEWS_TABLE)
	        : (con.TableInfo(Identifier(catalog), Identifier(location.schema), Identifier(openivm::VIEWS_TABLE))
	               ? vector<string> {location.schema}
	               : vector<string> {});
	bool configured_holds_views = false;
	for (auto &schema : current) {
		configured_holds_views = configured_holds_views || StringUtil::CIEquals(schema, configured);
	}
	if (current.size() > 1) {
		if (configured_holds_views) {
			// The configured schema is authoritative; never merge another view registry into it.
			return;
		}
		throw CatalogException("OpenIVM metadata exists in schemas %s of catalog '%s'; set openivm_metadata_schema "
		                       "to one of them",
		                       StringUtil::Join(current, ", "), catalog);
	}
	string legacy = current.empty() || configured_holds_views ? "" : current[0];
	if (legacy.empty() && (configured_holds_views || create_schema) &&
	    !StringUtil::CIEquals(configured, DEFAULT_SCHEMA)) {
		// Tables created outside a view registry (at LOAD, or before a first CREATE inside an
		// explicit transaction) still belong with the metadata, including declared RELY FKs.
		for (auto &table : MetadataTableNames()) {
			if (con.TableInfo(Identifier(catalog), Identifier(DEFAULT_SCHEMA), Identifier(table))) {
				legacy = DEFAULT_SCHEMA;
				break;
			}
		}
	}
	if (legacy.empty() && (configured_holds_views || !create_schema || SchemaExists(con, catalog, configured))) {
		return;
	}
	auto run = [&](const string &sql) {
		auto result = con.Query(sql);
		if (result->HasError()) {
			throw CatalogException(result->GetError());
		}
	};
	OPENIVM_DEBUG_PRINT("[METADATA] Moving metadata of %s from '%s' to '%s'\n", catalog.c_str(), legacy.c_str(),
	                    configured.c_str());
	con.BeginTransaction();
	try {
		run("CREATE SCHEMA IF NOT EXISTS " + SqlUtils::QuoteIdentifier(catalog) + "." +
		    SqlUtils::QuoteIdentifier(configured));
		for (auto &table : legacy.empty() ? vector<string>() : MetadataTableNames()) {
			auto source = con.TableInfo(Identifier(catalog), Identifier(legacy), Identifier(table));
			if (!source) {
				continue;
			}
			auto source_name = SqlUtils::FullName(catalog, legacy, table);
			auto target_name = SqlUtils::FullName(catalog, configured, table);
			run(CreateMetadataTableSQL(table, target_name, !location.IsNative()));
			auto target = con.TableInfo(Identifier(catalog), Identifier(configured), Identifier(table));
			if (!target) {
				throw CatalogException("could not create %s", target_name);
			}
			// Copy the shared columns: legacy tables can predate later metadata columns.
			vector<string> columns;
			for (auto &column : source->columns) {
				for (auto &target_column : target->columns) {
					if (StringUtil::CIEquals(column.Name().GetIdentifierName(),
					                         target_column.Name().GetIdentifierName())) {
						columns.push_back(SqlUtils::QuoteIdentifier(column.Name()));
						break;
					}
				}
			}
			auto column_list = StringUtil::Join(columns, ", ");
			run("INSERT INTO " + target_name + " (" + column_list + ") SELECT " + column_list + " FROM " + source_name);
			// This migration owns the old control table. SQL DROP would re-enter
			// source cleanup while both registries exist in this transaction, and
			// discovery would mistake that intermediate state for ambiguity. Use
			// the native catalog operation, as metadata replay does for its shadows.
			// The preceding INSERT registered this catalog's write transaction;
			// native dependency checks and rollback still apply to the drop.
			con.context->RunFunctionInTransaction([&]() {
				DropInfo drop;
				drop.type = CatalogType::TABLE_ENTRY;
				drop.SetCatalog(Identifier(catalog));
				drop.SetSchema(Identifier(legacy));
				drop.SetName(Identifier(table));
				Catalog::GetCatalog(*con.context, Identifier(catalog)).DropEntry(*con.context, drop);
			});
			OPENIVM_DEBUG_PRINT("[METADATA] Moved control table %s to %s\n", source_name.c_str(), target_name.c_str());
		}
		if (!location.IsNative() && !legacy.empty() && !StringUtil::CIEquals(legacy, configured)) {
			// Publish the redirect with the copied metadata, in the same remote transaction.
			// Remove a previous redirect when moving back to an earlier schema.
			for (auto &schema : {configured, legacy}) {
				if (con.TableInfo(Identifier(catalog), Identifier(schema),
				                  Identifier(openivm::METADATA_REDIRECT_TABLE))) {
					con.context->RunFunctionInTransaction([&]() {
						DropInfo drop;
						drop.type = CatalogType::TABLE_ENTRY;
						drop.SetCatalog(Identifier(catalog));
						drop.SetSchema(Identifier(schema));
						drop.SetName(Identifier(openivm::METADATA_REDIRECT_TABLE));
						Catalog::GetCatalog(*con.context, Identifier(catalog)).DropEntry(*con.context, drop);
					});
				}
			}
			auto redirect = SqlUtils::FullName(catalog, legacy, openivm::METADATA_REDIRECT_TABLE);
			run("CREATE TABLE " + redirect + " (metadata_schema VARCHAR NOT NULL)");
			run("INSERT INTO " + redirect + " VALUES (" + Value(configured).ToSQLString() + ")");
		}
		if (con.TableInfo(Identifier(catalog), Identifier(DEFAULT_SCHEMA),
		                  Identifier(openivm::METADATA_MARKER_TABLE))) {
			run("UPDATE " + SqlUtils::FullName(catalog, DEFAULT_SCHEMA, openivm::METADATA_MARKER_TABLE) +
			    " SET metadata_schema = " + Value(configured).ToSQLString() +
			    " WHERE metadata_catalog = " + Value(catalog).ToSQLString());
		}
		con.Commit();
	} catch (std::exception &ex) {
		ErrorData error(ex);
		try {
			con.Rollback();
		} catch (std::exception &) {
		}
		throw CatalogException("OpenIVM could not move its metadata in catalog '%s' from schema '%s' to '%s': %s",
		                       catalog, legacy.empty() ? string(DEFAULT_SCHEMA) : legacy, configured,
		                       error.RawMessage());
	}
}

vector<MetadataLocation> RefreshMetadata::MetadataLocations(Connection &con, const string &table) {
	return MetadataLocator::All(con, table);
}

vector<MetadataLocation> RefreshMetadata::MetadataLocations(ClientContext &context, const string &table) {
	vector<MetadataLocation> locations;
	for (auto &database : DatabaseManager::Get(context).GetDatabases(context)) {
		auto &catalog = database->GetCatalog();
		if (catalog.GetCatalogType() != "duckdb" || catalog.IsSystemCatalog() || catalog.IsTemporaryCatalog()) {
			continue;
		}
		for (auto &schema : duckdb::FindMetadataSchemas(context, catalog, table)) {
			locations.push_back({catalog.GetName().GetIdentifierName(), schema});
		}
	}
	Connection con(*context.db);
	for (auto &location : MetadataLocator::All(con, table)) {
		bool found = false;
		for (auto &existing : locations) {
			if (existing.Matches(location)) {
				existing = location;
				found = true;
				break;
			}
		}
		if (!found) {
			locations.push_back(location);
		}
	}
	return locations;
}

vector<string> RefreshMetadata::MetadataTables(Connection &con, const string &table) {
	vector<string> tables;
	for (auto &location : MetadataLocations(con, table)) {
		tables.push_back(SqlUtils::FullName(location.catalog, location.schema, table));
	}
	return tables;
}

// The compiler's helper connection cannot observe the caller's uncommitted metadata.
// Read through the caller's storage transaction and shadow only the compiler's reads.
void RefreshMetadata::SnapshotTransaction(ClientContext &context) {
	// A schema created by the caller may not exist in this helper's snapshot.
	// Its USE falls back to main, but the recorded metadata owner remains exact.
	auto location = CurrentLocation(con);
	const auto &catalog = location.catalog;
	const auto &schema = location.schema;
	if (!location.IsNative()) {
		// Remote metadata has no DuckDB storage to scan. Explicit transactions that would
		// write it next to MV data are rejected, so committed rows are the exact state.
		return;
	}
	for (auto name :
	     {openivm::VIEWS_TABLE, openivm::DELTA_TABLES_TABLE, openivm::MV_DEPS_TABLE, "openivm_refresh_hooks"}) {
		auto entry = Catalog::GetEntry<TableCatalogEntry>(
		    context, QualifiedName(Identifier(catalog), Identifier(schema), Identifier(name)),
		    OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			continue;
		}
		vector<StorageIndex> column_ids;
		vector<string> columns;
		for (auto &column : entry->GetColumns().Physical()) {
			column_ids.emplace_back(column.StorageOid());
			columns.push_back(SqlUtils::QuoteIdentifier(column.Name()) + " " + column.Type().ToString());
		}
		auto created = con.Query("CREATE OR REPLACE TEMP TABLE " + SqlUtils::QuoteIdentifier(name) + " (" +
		                         StringUtil::Join(columns, ", ") + ")");
		if (created->HasError()) {
			throw CatalogException("Cannot snapshot transaction metadata: %s", created->GetError());
		}
		auto &transaction = DuckTransaction::Get(context, entry->catalog);
		auto &storage = entry->GetStorage();
		TableScanState scan;
		storage.InitializeScan(context, transaction, scan, column_ids);
		DataChunk rows;
		rows.Initialize(Allocator::Get(context), entry->GetTypes());
		Appender appender(con, TEMP_CATALOG, DEFAULT_SCHEMA, name);
		while (true) {
			rows.Reset();
			storage.Scan(transaction, rows, scan);
			if (rows.size() == 0) {
				break;
			}
			appender.AppendDataChunk(rows);
		}
		appender.Close();
	}
	OPENIVM_DEBUG_PRINT("[METADATA] Snapshotted caller transaction in %s\n", catalog.c_str());
}

string RefreshMetadata::GetViewSQLName(const string &view_key) {
	// Metadata written before SQL names were stored separately has no view_sql_name column (see FindViewKey).
	auto result = con.Query("SELECT view_sql_name FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_key) + "'");
	if (result->HasError()) {
		// Only the missing view_sql_name column (a binder error) is legacy metadata; anything else is a real failure.
		if (result->GetErrorObject().Type() == ExceptionType::BINDER) {
			return view_key;
		}
		throw InvalidInputException("Could not read IVM metadata column 'view_sql_name' for materialized view '%s': %s",
		                            view_key, result->GetError());
	}
	if (result->RowCount() == 0 || result->Collection().GetValue(0, 0).IsNull()) {
		return view_key;
	}
	auto name = result->Collection().GetValue(0, 0).ToString();
	return name.empty() ? view_key : name;
}

string RefreshMetadata::FindViewKey(const string &catalog, const string &schema, const string &name) {
	auto query = [&](const string &name_expression) {
		return con.Query("SELECT view_name FROM " + string(openivm::VIEWS_TABLE) + " WHERE " +
		                 SqlUtils::IdentifierEqualsSQL(name_expression, Value(name).ToSQLString()) + " AND " +
		                 SqlUtils::IdentifierEqualsSQL("COALESCE(view_catalog, " + Value(catalog).ToSQLString() + ")",
		                                               Value(catalog).ToSQLString()) +
		                 " AND " +
		                 SqlUtils::IdentifierEqualsSQL("COALESCE(view_schema, 'main')", Value(schema).ToSQLString()));
	};
	auto result = query("COALESCE(view_sql_name, view_name)");
	if (result->HasError()) {
		result = query("view_name"); // Metadata written before SQL names were stored separately.
	}
	if (result->HasError() || !result->RowCount()) {
		return "";
	}
	if (result->RowCount() != 1) {
		throw CatalogException("Ambiguous materialized view '%s'", SqlUtils::FullName(catalog, schema, name));
	}
	return result->Collection().GetValue(0, 0).ToString();
}

string RefreshMetadata::AllocateViewKey(const string &catalog, const string &schema, const string &name) {
	auto existing = FindViewKey(catalog, schema, name);
	if (!existing.empty()) {
		return existing;
	}
	// Allocation must not depend on other CREATEs having committed their metadata.
	// Encode all identifier bytes, including separators, to avoid schema/name collisions.
	string key = "__openivm_mv_";
	const char *hex = "0123456789abcdef";
	for (auto &part : {catalog, schema, name}) {
		for (unsigned char c : StringUtil::Lower(part)) {
			key += hex[c >> 4];
			key += hex[c & 15];
		}
		key += '_';
	}
	return key;
}

string RefreshMetadata::ResolveViewName(const string &view_name, const string &catalog, const string &schema) {
	// Public names resolve by location; internal graph edges already contain storage keys.
	auto exact = con.Query("SELECT view_name FROM " + string(openivm::VIEWS_TABLE) +
	                       " WHERE view_name = " + Value(view_name).ToSQLString());
	if (exact->HasError()) {
		return view_name;
	}
	if (!catalog.empty() && !schema.empty()) {
		auto preferred = FindViewKey(catalog, schema, view_name);
		if (!preferred.empty()) {
			return preferred;
		}
	}
	auto matches =
	    con.Query("SELECT view_name FROM " + string(openivm::VIEWS_TABLE) + " WHERE " +
	              SqlUtils::IdentifierEqualsSQL("COALESCE(view_sql_name, view_name)", Value(view_name).ToSQLString()));
	if (matches->HasError()) {
		matches = con.Query("SELECT view_name FROM " + string(openivm::VIEWS_TABLE) + " WHERE " +
		                    SqlUtils::IdentifierEqualsSQL("view_name", Value(view_name).ToSQLString()));
	}
	if (!matches->HasError() && matches->RowCount() > 1) {
		throw CatalogException("Ambiguous materialized view '%s'; use catalog.schema.view", view_name);
	}
	if (!matches->HasError() && matches->RowCount() == 1) {
		return matches->Collection().GetValue(0, 0).ToString();
	}
	return exact->RowCount() == 1 ? exact->Collection().GetValue(0, 0).ToString() : view_name;
}

bool RefreshMetadata::IsBaseTable(const string &table_name) {
	auto result = con.Query("SELECT 1 FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(table_name) + "'");
	if (result->HasError()) {
		throw InvalidInputException("Could not read IVM metadata table '%s' for '%s': %s", openivm::VIEWS_TABLE,
		                            table_name, result->GetError());
	}
	return result->RowCount() == 0;
}

Value RefreshMetadata::ReadViewValue(const string &view_name, const string &column) {
	auto result = con.Query("SELECT " + SqlUtils::QuoteIdentifier(column) + " FROM " + string(openivm::VIEWS_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError()) {
		throw InvalidInputException("Could not read IVM metadata column '%s' for materialized view '%s': %s", column,
		                            view_name, result->GetError());
	}
	return result->RowCount() == 0 ? Value() : result->Collection().GetValue(0, 0);
}

string RefreshMetadata::ReadViewString(const string &view_name, const string &column) {
	auto value = ReadViewValue(view_name, column);
	return value.IsNull() ? "" : value.ToString();
}

bool RefreshMetadata::ReadViewBool(const string &view_name, const string &column) {
	auto value = ReadViewValue(view_name, column);
	return !value.IsNull() && value.GetValue<bool>();
}

Value RefreshMetadata::ReadDeltaValue(const string &view_name, const string &table_name, const string &column) {
	auto result =
	    con.Query("SELECT " + SqlUtils::QuoteIdentifier(column) + " FROM " + string(openivm::DELTA_TABLES_TABLE) +
	              " WHERE view_name = " + Value(view_name).ToSQLString() +
	              " AND table_name = " + Value(table_name).ToSQLString());
	if (result->HasError()) {
		throw InvalidInputException(
		    "Could not read IVM metadata column '%s' for materialized view '%s' and table '%s': %s", column, view_name,
		    table_name, result->GetError());
	}
	return result->RowCount() == 0 ? Value() : result->Collection().GetValue(0, 0);
}

string RefreshMetadata::GetViewQuery(const string &view_name) {
	return ReadViewString(view_name, "sql_string");
}

RefreshType RefreshMetadata::GetViewType(const string &view_name) {
	auto result = con.Query("SELECT type FROM " + string(openivm::VIEWS_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError() || result->RowCount() == 0) {
		if (result->HasError()) {
			auto locus = con.Query("SELECT current_database(), current_schema()");
			auto locus_text = !locus->HasError() && locus->RowCount() > 0
			                      ? " (connection locus " + locus->Collection().GetValue(0, 0).ToString() + "." +
			                            locus->Collection().GetValue(1, 0).ToString() + ")"
			                      : "";
			throw ParserException("Could not read IVM metadata for materialized view '%s'%s: %s", view_name, locus_text,
			                      result->GetError());
		}
		auto location = CurrentLocation(con);
		throw CatalogException("Materialized view '%s' does not exist in IVM metadata; check view_sql_name, "
		                       "view_catalog and view_schema in %s",
		                       view_name, location.Table(openivm::VIEWS_TABLE));
	}
	auto type_value = result->Collection().GetValue(0, 0);
	if (type_value.IsNull()) {
		throw InvalidInputException("Invalid refresh type NULL in IVM metadata for materialized view '%s'", view_name);
	}
	auto raw_type = type_value.GetValue<int64_t>();
	if (!IsValidRefreshTypeOrdinal(raw_type)) {
		throw InvalidInputException("Invalid refresh type %s in IVM metadata for materialized view '%s'",
		                            std::to_string(raw_type), view_name);
	}
	return static_cast<RefreshType>(raw_type);
}

bool RefreshMetadata::HasMinMax(const string &view_name) {
	return ReadViewBool(view_name, "has_minmax");
}

bool RefreshMetadata::HasLeftJoin(const string &view_name) {
	return ReadViewBool(view_name, "has_left_join");
}

bool RefreshMetadata::HasJoin(const string &view_name) {
	auto value = ReadViewValue(view_name, "has_join");
	if (value.IsNull()) {
		return HasLeftJoin(view_name) || HasFullOuter(view_name);
	}
	return value.GetValue<bool>();
}

bool RefreshMetadata::HasFullOuter(const string &view_name) {
	return ReadViewBool(view_name, "has_full_outer");
}

string RefreshMetadata::GetFullOuterJoinCols(const string &view_name) {
	return ReadViewString(view_name, "full_outer_join_cols");
}

vector<string> RefreshMetadata::GetDeltaTables(const string &view_name) {
	auto result = con.Query("SELECT table_name FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	                        SqlUtils::EscapeValue(view_name) + "'");
	vector<string> tables;
	if (result->HasError()) {
		throw InvalidInputException("Could not read IVM delta table metadata for materialized view '%s': %s", view_name,
		                            result->GetError());
	}
	for (size_t i = 0; i < result->RowCount(); i++) {
		tables.push_back(result->Collection().GetValue(0, i).ToString());
	}
	return tables;
}

string RefreshMetadata::GetLastUpdate(const string &view_name, const string &table_name) {
	auto value = ReadDeltaValue(view_name, table_name, "last_update");
	return value.IsNull() ? "" : value.ToString();
}

vector<MetadataLocation> RefreshMetadata::MetadataLocations(Connection &con) {
	return MetadataLocator::All(con);
}

string RefreshMetadata::SourceTableName(const string &key, const string &catalog, const string &schema) {
	if (!StringUtil::CIStartsWith(key, SqlUtils::QualifiedPrefix(catalog, schema))) {
		return key;
	}
	return SqlUtils::ParseQualifiedIdentifier(key).back();
}

string RefreshMetadata::SourcePredicate(const string &table, const string &catalog, const string &schema) {
	auto name = SourceTableName(table, catalog, schema);
	// Match both legacy bare keys and qualified keys with catalog identifier rules.
	return "(" + SqlUtils::IdentifierEqualsSQL("table_name", Value(name).ToSQLString()) + " OR " +
	       SqlUtils::IdentifierEqualsSQL("table_name", Value(SqlUtils::FullName(catalog, schema, name)).ToSQLString()) +
	       ") AND " +
	       SqlUtils::IdentifierEqualsSQL("COALESCE(source_catalog, " + Value(catalog).ToSQLString() + ")",
	                                     Value(catalog).ToSQLString()) +
	       " AND " +
	       SqlUtils::IdentifierEqualsSQL("COALESCE(source_schema, " + Value(schema).ToSQLString() + ")",
	                                     Value(schema).ToSQLString());
}

string RefreshMetadata::InternalSourceDeltaName(const string &source_schema, const string &table) {
	// Hex-encode the schema like view keys: the separator can then never be ambiguous.
	string encoded;
	const char *hex = "0123456789abcdef";
	for (unsigned char c : StringUtil::Lower(source_schema)) {
		encoded += hex[c >> 4];
		encoded += hex[c & 15];
	}
	return string(openivm::DELTA_PREFIX) + encoded + "_" + table;
}

string RefreshMetadata::SourceDeltaTableName(const string &source_schema, const string &table,
                                             const string &delta_schema) {
	if (delta_schema.empty() || StringUtil::CIEquals(delta_schema, source_schema)) {
		return SqlUtils::DeltaName(table);
	}
	return InternalSourceDeltaName(source_schema, table);
}

// Legacy rows derive the physical name; new rows store it because a colocated
// delta may already occupy the preferred name of a moved source.
static void SetSourceDelta(const string &delta_key, const string &schema, const string &delta_schema,
                           const string &stored_name, string &delta_schema_name, string &delta_table_name) {
	delta_schema_name = delta_schema.empty() ? schema : delta_schema;
	static const string prefix(openivm::DELTA_PREFIX);
	auto base = StringUtil::StartsWith(delta_key, prefix) ? delta_key.substr(prefix.size()) : delta_key;
	delta_table_name =
	    stored_name.empty() ? RefreshMetadata::SourceDeltaTableName(schema, base, delta_schema) : stored_name;
}

enum class SourceDeltaLookup { LOCAL, LOCAL_LEGACY_CANDIDATE, LOCAL_LEGACY, REGISTERED };

static optional_ptr<TableCatalogEntry> FindSourceDeltaImpl(ClientContext &context, const string &catalog,
                                                           const string &schema, const string &table,
                                                           SourceDeltaLookup lookup) {
	// OpenIVM's own companion deltas are created before dependency rows exist.
	// They always stay beside their reserved internal relation.
	if (IncrementalTableNames::IsMaintenanceTable(table)) {
		return Catalog::GetEntry<TableCatalogEntry>(
		    context, QualifiedName(Identifier(catalog), Identifier(schema), Identifier(SqlUtils::DeltaName(table))),
		    OnEntryNotFound::RETURN_NULL);
	}
	const auto delta_key_name = SqlUtils::DeltaName(table);
	const auto qualified_delta_key = SqlUtils::FullName(catalog, schema, delta_key_name);
	optional_ptr<TableCatalogEntry> delta;
	unordered_map<string, unordered_set<string>> delta_owners;
	auto consume = [&](const vector<Value> &values, const string &metadata_catalog) {
		auto value = [&](idx_t field, const string &fallback) {
			return values[field].IsNull() ? fallback : values[field].ToString();
		};
		auto source_catalog = value(1, metadata_catalog);
		auto source_schema = value(2, DEFAULT_SCHEMA);
		if (!StringUtil::CIEquals(catalog, source_catalog)) {
			return;
		}
		auto delta_key = RefreshMetadata::SourceTableName(value(0, ""), source_catalog, source_schema);
		string delta_schema, delta_name;
		SetSourceDelta(delta_key, source_schema, value(3, ""), value(4, ""), delta_schema, delta_name);
		auto physical_key = StringUtil::Lower(SqlUtils::FullName(catalog, delta_schema, delta_name));
		delta_owners[physical_key].insert(
		    StringUtil::Lower(SqlUtils::FullName(source_catalog, source_schema, delta_key)));
		if (!StringUtil::CIEquals(schema, source_schema) ||
		    !StringUtil::CIEquals(delta_key, SqlUtils::DeltaName(table))) {
			return;
		}
		auto candidate = Catalog::GetEntry<TableCatalogEntry>(
		    context, QualifiedName(Identifier(catalog), Identifier(delta_schema), Identifier(delta_name)),
		    OnEntryNotFound::RETURN_NULL);
		if (candidate) {
			if (delta && delta.get() != candidate.get()) {
				throw CatalogException("OpenIVM found multiple internal deltas for source '%s'",
				                       SqlUtils::FullName(catalog, schema, table));
			}
			delta = candidate;
		}
	};
	// Read native ownership through the caller's snapshot, including uncommitted
	// CREATE/DROP. Remote ownership has no DuckDB storage and is read as SQL.
	vector<MetadataLocation> locations;
	if (lookup == SourceDeltaLookup::REGISTERED) {
		locations = RefreshMetadata::MetadataLocations(context, openivm::DELTA_TABLES_TABLE);
	} else if (lookup == SourceDeltaLookup::LOCAL_LEGACY || lookup == SourceDeltaLookup::LOCAL_LEGACY_CANDIDATE) {
		// Older native ownership rows can be read locally without following a
		// controller marker or contacting a detached/remote metadata catalog.
		auto &source_catalog = Catalog::GetCatalog(context, Identifier(catalog));
		if (!source_catalog.IsDuckCatalog()) {
			return nullptr;
		}
		for (auto &source_schema : duckdb::FindMetadataSchemas(context, source_catalog, openivm::DELTA_TABLES_TABLE)) {
			locations.emplace_back(catalog, source_schema);
		}
	} else {
		locations.emplace_back(catalog, DEFAULT_SCHEMA);
	}
	const auto registry =
	    lookup == SourceDeltaLookup::LOCAL ? openivm::SOURCE_DELTA_MAP_TABLE : openivm::DELTA_TABLES_TABLE;
	for (auto &location : locations) {
		auto &metadata_catalog = Catalog::GetCatalog(context, Identifier(location.catalog));
		auto entry = metadata_catalog.GetEntry<TableCatalogEntry>(
		    context, QualifiedName(metadata_catalog.GetName(), Identifier(location.schema), Identifier(registry)),
		    OnEntryNotFound::RETURN_NULL);
		if (!entry) {
			continue;
		}
		vector<StorageIndex> columns;
		vector<LogicalType> types;
		vector<optional_idx> positions;
		vector<string> projection;
		for (auto name : {"table_name", "source_catalog", "source_schema", "delta_schema", "delta_table_name"}) {
			if (entry->ColumnExists(name)) {
				positions.emplace_back(columns.size());
				auto &column = entry->GetColumn(name);
				columns.emplace_back(column.StorageOid());
				types.push_back(column.Type());
				projection.push_back(SqlUtils::QuoteIdentifier(name));
			} else {
				positions.emplace_back();
				projection.push_back("NULL::VARCHAR");
			}
		}
		vector<Value> values(5);
		if (!metadata_catalog.IsDuckCatalog()) {
			Connection reader(*context.db);
			auto rows =
			    reader.Query("SELECT " + StringUtil::Join(projection, ", ") + " FROM " + location.Table(registry));
			if (rows->HasError()) {
				throw CatalogException("OpenIVM could not read source delta ownership: %s", rows->GetError());
			}
			for (idx_t row = 0; row < rows->RowCount(); row++) {
				for (idx_t field = 0; field < values.size(); field++) {
					values[field] = rows->Collection().GetValue(field, row);
				}
				consume(values, location.catalog);
			}
			continue;
		}
		auto &transaction = DuckTransaction::Get(context, entry->catalog);
		auto &storage = entry->GetStorage();
		TableScanState scan;
		TableFilterSet filters;
		if (lookup == SourceDeltaLookup::LOCAL) {
			// The durable routing table has one normalized key per source and a
			// UNIQUE physical target. Validate ownership at publication; writes
			// only need their own row, in the caller's MVCC snapshot.
			filters.PushFilter(
			    ProjectionIndex(positions[0].GetIndex()),
			    ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL,
			                                             Value(StringUtil::Lower(SqlUtils::DeltaName(table)))));
			filters.PushFilter(ProjectionIndex(positions[2].GetIndex()),
			                   ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL,
			                                                            Value(StringUtil::Lower(schema))));
		}
		// DuckDB 2.0 requires a non-null TableFilterSet to contain at least one
		// filter. Legacy/registered ownership scans intentionally read all rows.
		storage.InitializeScan(context, transaction, scan, columns,
		                       lookup == SourceDeltaLookup::LOCAL ? &filters : nullptr);
		DataChunk rows;
		rows.Initialize(Allocator::Get(context), types);
		while (true) {
			rows.Reset();
			storage.Scan(transaction, rows, scan);
			if (!rows.size()) {
				break;
			}
			UnifiedVectorFormat names;
			if (lookup == SourceDeltaLookup::LOCAL_LEGACY_CANDIDATE) {
				rows.data[positions[0].GetIndex()].ToUnifiedFormat(names);
			}
			for (idx_t row = 0; row < rows.size(); row++) {
				if (lookup == SourceDeltaLookup::LOCAL_LEGACY_CANDIDATE) {
					auto index = names.sel->get_index(row);
					if (!names.validity.RowIsValid(index)) {
						continue;
					}
					auto name = UnifiedVectorFormat::GetData<string_t>(names)[index];
					auto matches = [&](const string &key) {
						return StringUtil::CIEquals(name.GetData(), name.GetSize(), key.data(), key.size());
					};
					if (!matches(delta_key_name) && !matches(qualified_delta_key)) {
						continue;
					}
				}
				for (idx_t field = 0; field < values.size(); field++) {
					values[field] =
					    positions[field].IsValid() ? rows.GetValue(positions[field].GetIndex(), row) : Value();
				}
				consume(values, location.catalog);
			}
		}
	}
	if (delta && lookup == SourceDeltaLookup::LOCAL_LEGACY_CANDIDATE) {
		// Only legacy tracked sources need the complete cross-registry ownership check.
		return FindSourceDeltaImpl(context, catalog, schema, table, SourceDeltaLookup::LOCAL_LEGACY);
	}
	if (delta) {
		auto physical_key = StringUtil::Lower(SqlUtils::FullName(catalog, delta->schema.name, delta->name));
		if (delta_owners.at(physical_key).size() != 1) {
			throw CatalogException("OpenIVM delta '%s' is assigned to multiple sources; cannot reuse it for '%s'",
			                       SqlUtils::FullName(catalog, delta->schema.name, delta->name),
			                       SqlUtils::FullName(catalog, schema, table));
		}
	}
	if (!delta) {
		delta = TransactionalMVLockState::Get(context).FindDeferredSourceDelta(catalog, schema, table);
	}
	return delta;
}

optional_ptr<TableCatalogEntry> RefreshMetadata::FindSourceDelta(ClientContext &context, const string &catalog,
                                                                 const string &schema, const string &table) {
	auto delta = FindSourceDeltaImpl(context, catalog, schema, table, SourceDeltaLookup::LOCAL);
	return delta ? delta
	             : FindSourceDeltaImpl(context, catalog, schema, table, SourceDeltaLookup::LOCAL_LEGACY_CANDIDATE);
}

optional_ptr<TableCatalogEntry> RefreshMetadata::FindRegisteredSourceDelta(ClientContext &context,
                                                                           const string &catalog, const string &schema,
                                                                           const string &table) {
	return FindSourceDeltaImpl(context, catalog, schema, table, SourceDeltaLookup::REGISTERED);
}

string RefreshMetadata::SourceGenerationDDL(const string &catalog, const string &schema, const string &table) {
	auto registry = SqlUtils::FullName(catalog, DEFAULT_SCHEMA, openivm::SOURCE_GENERATIONS_TABLE);
	return "CREATE TABLE IF NOT EXISTS " + registry +
	       " (source_schema VARCHAR NOT NULL, table_name VARCHAR NOT NULL, generation VARCHAR NOT NULL, "
	       "PRIMARY KEY(source_schema,table_name)); INSERT INTO " +
	       registry + " VALUES (" + Value(StringUtil::Lower(schema)).ToSQLString() + ", " +
	       Value(StringUtil::Lower(table)).ToSQLString() + ", uuid()::VARCHAR) ON CONFLICT DO NOTHING;";
}

string RefreshMetadata::SourceGeneration(ClientContext &context, const string &catalog, const string &schema,
                                         const string &table) {
	auto registry = Catalog::GetEntry<TableCatalogEntry>(
	    context,
	    QualifiedName(Identifier(catalog), Identifier(DEFAULT_SCHEMA), Identifier(openivm::SOURCE_GENERATIONS_TABLE)),
	    OnEntryNotFound::RETURN_NULL);
	if (!registry) {
		return "";
	}
	TableFilterSet filters;
	filters.PushFilter(ProjectionIndex(0), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL,
	                                                                                Value(StringUtil::Lower(schema))));
	filters.PushFilter(ProjectionIndex(1), ExpressionFilter::CreateComparisonFilter(ExpressionType::COMPARE_EQUAL,
	                                                                                Value(StringUtil::Lower(table))));
	vector<StorageIndex> columns {StorageIndex(0), StorageIndex(1), StorageIndex(2)};
	TableScanState scan;
	auto &transaction = DuckTransaction::Get(context, registry->catalog);
	auto &storage = registry->GetStorage();
	storage.InitializeScan(context, transaction, scan, columns, &filters);
	DataChunk rows;
	rows.Initialize(Allocator::Get(context), registry->GetTypes());
	storage.Scan(transaction, rows, scan);
	return rows.size() ? rows.GetValue(2, 0).ToString() : string();
}

// Registry deletions must participate in the caller's snapshot, rollback and
// local uniqueness bookkeeping, including DROP -> CREATE in one transaction.
static void DeleteRegistryRows(ClientContext &context, TableCatalogEntry &entry,
                               const std::function<bool(DataChunk &, idx_t)> &matches) {
	auto &storage = entry.GetStorage();
	auto &transaction = DuckTransaction::Get(context, entry.catalog);
	vector<StorageIndex> columns;
	auto types = entry.GetTypes();
	for (idx_t i = 0; i < types.size(); i++) {
		columns.emplace_back(i);
	}
	columns.emplace_back(COLUMN_IDENTIFIER_ROW_ID);
	types.push_back(LogicalType::ROW_TYPE);
	TableScanState scan;
	storage.InitializeScan(context, transaction, scan, columns);
	DataChunk rows;
	rows.Initialize(Allocator::Get(context), types);
	vector<unique_ptr<BoundConstraint>> constraints;
	auto state = storage.InitializeDelete(entry, context, constraints);
	while (true) {
		rows.Reset();
		storage.Scan(transaction, rows, scan);
		if (!rows.size()) {
			break;
		}
		Vector row_ids(LogicalType::ROW_TYPE);
		DataChunk deleted;
		deleted.Initialize(Allocator::Get(context), entry.GetTypes());
		idx_t count = 0;
		for (idx_t row = 0; row < rows.size(); row++) {
			if (matches(rows, row)) {
				row_ids.SetValue(count, rows.GetValue(types.size() - 1, row));
				for (idx_t column = 0; column + 1 < types.size(); column++) {
					deleted.data[column].SetValue(count, rows.GetValue(column, row));
				}
				count++;
			}
		}
		if (count) {
			MetaTransaction::Get(context).ModifyDatabase(entry.catalog.GetAttached(),
			                                             DatabaseModificationType::DELETE_DATA);
			LocalAppendState append_state;
			storage.InitializeLocalStorage(append_state, entry.Cast<DuckTableEntry>(), context, constraints);
			deleted.SetCardinalityUnsafe(count);
			LocalStorage::Get(context, entry.catalog).GetStorage(storage)->AppendToDeleteIndexes(row_ids, deleted);
			storage.Delete(*state, context, entry.Cast<DuckTableEntry>(), row_ids, count);
		}
	}
}

void RefreshMetadata::InvalidateSourceGeneration(ClientContext &context, const string &catalog, const string &schema,
                                                 const string &table, bool drop_schema) {
	auto &state = TransactionalMVLockState::Get(context);
	// A declaration can commit after an explicit transaction took its snapshot.
	// Never retire a source while leaving such an invisible assertion's token live.
	Connection committed(*context.db);
	if (committed.TableInfo(Identifier(catalog), Identifier(DEFAULT_SCHEMA),
	                        Identifier(openivm::SOURCE_GENERATIONS_TABLE))) {
		auto query = "SELECT table_name,generation FROM " +
		             SqlUtils::FullName(catalog, DEFAULT_SCHEMA, openivm::SOURCE_GENERATIONS_TABLE) +
		             " WHERE source_schema=" + Value(StringUtil::Lower(schema)).ToSQLString();
		if (!drop_schema) {
			query += " AND table_name=" + Value(StringUtil::Lower(table)).ToSQLString();
		}
		auto rows = committed.Query(query);
		if (rows->HasError()) {
			throw CatalogException(rows->GetError());
		}
		for (idx_t row = 0; row < rows->RowCount(); row++) {
			auto name = rows->Collection().GetValue(0, row).ToString();
			if (!state.HasInvalidatedSourceGeneration(catalog, schema, name) &&
			    SourceGeneration(context, catalog, schema, name) != rows->Collection().GetValue(1, row).ToString()) {
				throw TransactionException("OpenIVM source incarnation changed after this transaction's snapshot; "
				                           "retry before dropping or replacing %s",
				                           SqlUtils::FullName(catalog, schema, name));
			}
		}
	}
	auto registry = Catalog::GetEntry<TableCatalogEntry>(
	    context,
	    QualifiedName(Identifier(catalog), Identifier(DEFAULT_SCHEMA), Identifier(openivm::SOURCE_GENERATIONS_TABLE)),
	    OnEntryNotFound::RETURN_NULL);
	if (registry) {
		DeleteRegistryRows(context, *registry, [&](DataChunk &rows, idx_t row) {
			auto matches = StringUtil::CIEquals(rows.GetValue(0, row).ToString(), schema) &&
			               (drop_schema || StringUtil::CIEquals(rows.GetValue(1, row).ToString(), table));
			if (matches) {
				state.RecordSourceGenerationInvalidation(catalog, schema, rows.GetValue(1, row).ToString());
			}
			return matches;
		});
	}
}

string RefreshMetadata::SourceDeltaMappingDDL(const string &catalog, const string &schema, const string &table,
                                              const string &delta_schema, const string &delta_table) {
	auto mapping = SqlUtils::FullName(catalog, DEFAULT_SCHEMA, openivm::SOURCE_DELTA_MAP_TABLE);
	return "CREATE TABLE IF NOT EXISTS " + mapping +
	       " (source_schema VARCHAR NOT NULL, table_name VARCHAR NOT NULL, delta_schema VARCHAR NOT NULL, "
	       "delta_table_name VARCHAR NOT NULL, PRIMARY KEY (source_schema, table_name), "
	       "UNIQUE (delta_schema, delta_table_name)); INSERT INTO " +
	       mapping + " VALUES (" + Value(StringUtil::Lower(schema)).ToSQLString() + ", " +
	       Value(StringUtil::Lower(SqlUtils::DeltaName(table))).ToSQLString() + ", " +
	       Value(delta_schema).ToSQLString() + ", " + Value(delta_table).ToSQLString() +
	       ") ON CONFLICT (source_schema, table_name) DO UPDATE SET delta_schema = excluded.delta_schema, "
	       "delta_table_name = excluded.delta_table_name";
}

void RefreshMetadata::RemoveSourceDeltaMapping(ClientContext &context, const string &catalog, const string &schema,
                                               const string &name, bool drop_schema) {
	auto mapping = Catalog::GetEntry<TableCatalogEntry>(
	    context,
	    QualifiedName(Identifier(catalog), Identifier(DEFAULT_SCHEMA), Identifier(openivm::SOURCE_DELTA_MAP_TABLE)),
	    OnEntryNotFound::RETURN_NULL);
	if (!mapping) {
		return;
	}
	DeleteRegistryRows(context, *mapping, [&](DataChunk &rows, idx_t row) {
		// Non-cascading source DROP retains the delta and its consumers.
		return (drop_schema && StringUtil::CIEquals(rows.GetValue(0, row).ToString(), schema)) ||
		       (StringUtil::CIEquals(rows.GetValue(2, row).ToString(), schema) &&
		        (drop_schema || StringUtil::CIEquals(rows.GetValue(3, row).ToString(), name)));
	});
}

optional_ptr<TableCatalogEntry> RefreshMetadata::FindSourceDeltaForMutation(ClientContext &context,
                                                                            const string &catalog, const string &schema,
                                                                            const string &table) {
	auto local = FindSourceDelta(context, catalog, schema, table);
	// Admission protects publication, but does not advance an explicit
	// transaction's snapshot. Reconcile a fresh committed read with its own writes
	// before allocating a delta or deciding whether native DML needs capture.
	Connection committed(*context.db);
	optional_ptr<TableCatalogEntry> delta = local;
	committed.context->RunFunctionInTransaction([&]() {
		auto existing = FindSourceDelta(*committed.context, catalog, schema, table);
		if (!existing) {
			// Caller-created native deltas have no committed ownership yet. A
			// committed delta visible only in the caller's old snapshot instead
			// lost its final owner: publishing it would reference a dropped table.
			if (local && local->catalog.IsDuckCatalog() &&
			    local->timestamp != DuckTransaction::Get(context, local->catalog).GetTransactionId()) {
				// Shared allocation precedes publication. A failed CREATE can retire
				// its allocation while this writer waits on publication admission.
				// Only an old snapshot with an actual consumer must be retried.
				if (!FindRegisteredSourceDelta(context, catalog, schema, table)) {
					delta = nullptr;
					return;
				}
				throw TransactionException("OpenIVM source delta ownership changed for '%s'; retry the transaction",
				                           SqlUtils::FullName(catalog, schema, table));
			}
			return;
		}
		// A prior DROP/REPLACE in this transaction may have removed the last
		// consumer and its delta. The helper still sees that committed identity;
		// keep the caller's new ownership (or allocate anew) only when its own
		// successful DROP explains the difference, never for a stale snapshot.
		if (TransactionalMVLockState::Get(context).HasDroppedSourceDelta(
		        catalog, existing->schema.name.GetIdentifierName(), existing->name.GetIdentifierName())) {
			OPENIVM_DEBUG_PRINT("[DELTA OWNERSHIP] Ignoring locally removed committed source delta %s\n",
			                    SqlUtils::FullName(catalog, existing->schema.name, existing->name).c_str());
			return;
		}
		if (local &&
		    (!StringUtil::CIEquals(local->schema.name.GetIdentifierName(), existing->schema.name.GetIdentifierName()) ||
		     !StringUtil::CIEquals(local->name.GetIdentifierName(), existing->name.GetIdentifierName()))) {
			throw TransactionException("OpenIVM source delta ownership changed for '%s'; retry the transaction",
			                           SqlUtils::FullName(catalog, schema, table));
		}
		// Return an entry belonging to the caller's transaction, never the helper's.
		delta = Catalog::GetEntry<TableCatalogEntry>(
		    context, QualifiedName(Identifier(catalog), Identifier(existing->schema.name), Identifier(existing->name)),
		    OnEntryNotFound::RETURN_NULL);
		// A concurrent DROP/recreate may use the same physical name. Reject the
		// old allocation, while retaining catalog changes made by this caller.
		if (!delta || (delta->catalog.IsDuckCatalog() && delta->oid != existing->oid &&
		               delta->timestamp != DuckTransaction::Get(context, delta->catalog).GetTransactionId())) {
			throw TransactionException("OpenIVM source delta for '%s' is not visible; retry the transaction",
			                           SqlUtils::FullName(catalog, schema, table));
		}
		OPENIVM_DEBUG_PRINT("[DELTA OWNERSHIP] Reconciled committed source delta %s\n",
		                    SqlUtils::FullName(catalog, delta->schema.name, delta->name).c_str());
	});
	return delta;
}

string RefreshMetadata::SourceOwnerPredicate(const string &table, const string &catalog, const string &schema,
                                             bool ducklake) {
	// The catalog type keeps a native `openivm_delta_<name>` key and a DuckLake source
	// that happens to use that name from being mistaken for each other.
	if (ducklake) {
		return SourcePredicate(table, catalog, schema) + " AND catalog_type = 'ducklake'";
	}
	return SourcePredicate(SqlUtils::DeltaName(table), catalog, schema) +
	       " AND COALESCE(catalog_type, 'duckdb') <> 'ducklake'";
}

RefreshMetadata::SourceLocation RefreshMetadata::GetSourceLocation(const string &view_name, const string &table_name,
                                                                   const string &fallback_catalog,
                                                                   const string &fallback_schema) {
	SourceLocation loc;
	loc.catalog_name = fallback_catalog;
	loc.schema_name = fallback_schema;
	string delta_schema;
	string delta_table_name;
	auto filter = " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND table_name = '" +
	              SqlUtils::EscapeValue(table_name) + "'";
	auto result = con.Query("SELECT source_catalog, source_schema, delta_schema, delta_table_name FROM " +
	                        string(openivm::DELTA_TABLES_TABLE) + filter);
	if (result->HasError()) {
		// Metadata written before physical delta names were stored.
		result = con.Query("SELECT source_catalog, source_schema, delta_schema, NULL FROM " +
		                   string(openivm::DELTA_TABLES_TABLE) + filter);
	}
	if (result->HasError()) {
		result = con.Query("SELECT source_catalog, source_schema, NULL, NULL FROM " +
		                   string(openivm::DELTA_TABLES_TABLE) + filter);
	}
	if (result->HasError()) {
		throw InvalidInputException("Could not read IVM source location for materialized view '%s' and table '%s': %s",
		                            view_name, table_name, result->GetError());
	}
	if (result->RowCount() > 0) {
		if (!result->Collection().GetValue(0, 0).IsNull()) {
			loc.catalog_name = result->Collection().GetValue(0, 0).ToString();
		}
		if (!result->Collection().GetValue(1, 0).IsNull()) {
			loc.schema_name = result->Collection().GetValue(1, 0).ToString();
		}
		if (!result->Collection().GetValue(2, 0).IsNull()) {
			delta_schema = result->Collection().GetValue(2, 0).ToString();
		}
		if (!result->Collection().GetValue(3, 0).IsNull()) {
			delta_table_name = result->Collection().GetValue(3, 0).ToString();
		}
	}
	loc.table_name = SourceTableName(table_name, loc.catalog_name, loc.schema_name);
	SetSourceDelta(loc.table_name, loc.schema_name, delta_schema, delta_table_name, loc.delta_schema_name,
	               loc.delta_table_name);
	return loc;
}

string RefreshMetadata::GetInternalSchema(const string &view_name) {
	return ReadViewString(view_name, "internal_schema");
}

RefreshMetadata::StoredViewLocation RefreshMetadata::GetInternalLocation(const string &view_name,
                                                                         const string &fallback_catalog,
                                                                         const string &fallback_schema) {
	auto location = GetStoredViewLocation(view_name, fallback_catalog, fallback_schema);
	auto internal_schema = GetInternalSchema(view_name);
	if (!internal_schema.empty()) {
		location.schema_name = internal_schema;
	}
	return location;
}

RefreshMetadata::StoredViewLocation RefreshMetadata::GetStoredViewLocation(const string &view_name,
                                                                           const string &fallback_catalog,
                                                                           const string &fallback_schema) {
	StoredViewLocation loc {fallback_catalog, fallback_schema};
	auto result = con.Query("SELECT view_catalog, view_schema FROM " + string(openivm::VIEWS_TABLE) +
	                        " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
	if (result->HasError()) {
		throw InvalidInputException("Could not read IVM stored location for materialized view '%s': %s", view_name,
		                            result->GetError());
	}
	if (result->RowCount() > 0) {
		if (!result->Collection().GetValue(0, 0).IsNull()) {
			loc.catalog_name = result->Collection().GetValue(0, 0).ToString();
		}
		if (!result->Collection().GetValue(1, 0).IsNull()) {
			loc.schema_name = result->Collection().GetValue(1, 0).ToString();
		}
	}
	if (loc.catalog_name.empty()) {
		auto current = con.Query("SELECT current_database()");
		if (!current->HasError() && current->RowCount() > 0 && !current->Collection().GetValue(0, 0).IsNull()) {
			loc.catalog_name = current->Collection().GetValue(0, 0).ToString();
		}
	}
	if (loc.schema_name.empty()) {
		loc.schema_name = DEFAULT_SCHEMA;
	}
	return loc;
}

bool RefreshMetadata::IsMaterializedViewDelta(const DeltaSource &source,
                                              optional_ptr<TransactionalMVMetadataState> transaction_state) {
	auto table_name = SourceTableName(source.table_name, source.catalog_name, source.schema_name);
	if (!SqlUtils::IsDelta(table_name)) {
		return false;
	}
	auto owner = PublishedSourceViewName(table_name);
	auto matches = [&](Connection &reader, const MetadataLocation &location, const string &views_table) {
		// An MV owns its deltas even when no child consumes them.
		auto query = [&](const string &schema_expression) {
			return reader.Query(
			    "SELECT 1 FROM " + views_table + " WHERE view_name=" + Value(owner).ToSQLString() + " AND " +
			    SqlUtils::IdentifierEqualsSQL("COALESCE(view_catalog, " + Value(location.catalog).ToSQLString() + ")",
			                                  Value(source.catalog_name).ToSQLString()) +
			    " AND " +
			    SqlUtils::IdentifierEqualsSQL("COALESCE(" + schema_expression + ", " +
			                                      Value(source.schema_name).ToSQLString() + ")",
			                                  Value(source.schema_name).ToSQLString()));
		};
		auto result = query("internal_schema, view_schema");
		if (result->HasError()) {
			result = query("view_schema"); // Metadata written before internal schemas existed.
		}
		if (result->HasError()) {
			throw CatalogException("Cannot resolve delta owner for '%s': %s", source.table_name, result->GetError());
		}
		OPENIVM_DEBUG_PRINT("[DELTA OWNERSHIP] Checked MV owner in %s with transaction replay=%d\n",
		                    views_table.c_str(), transaction_state ? 1 : 0);
		return result->RowCount() != 0;
	};
	return AnyMetadataOwnership(con, openivm::VIEWS_TABLE, transaction_state, matches);
}

vector<RefreshMetadata::DeltaSource> RefreshMetadata::GetDeltaSources(const string &view_name,
                                                                      const string &fallback_catalog,
                                                                      const string &fallback_schema) {
	auto filter = " FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "'";
	auto result = con.Query(
	    "SELECT table_name, catalog_type, source_catalog, source_schema, delta_schema, delta_table_name" + filter);
	if (result->HasError()) {
		// Metadata written before physical delta names were stored.
		result =
		    con.Query("SELECT table_name, catalog_type, source_catalog, source_schema, delta_schema, NULL" + filter);
	}
	if (result->HasError()) {
		result = con.Query("SELECT table_name, catalog_type, source_catalog, source_schema, NULL, NULL" + filter);
	}
	vector<DeltaSource> sources;
	if (result->HasError()) {
		throw InvalidInputException("Could not read IVM delta source metadata for materialized view '%s': %s",
		                            view_name, result->GetError());
	}
	for (idx_t row = 0; row < result->RowCount(); row++) {
		DeltaSource source;
		source.table_name = result->Collection().GetValue(0, row).ToString();
		source.catalog_type = result->Collection().GetValue(1, row).IsNull()
		                          ? "duckdb"
		                          : result->Collection().GetValue(1, row).ToString();
		source.catalog_name = result->Collection().GetValue(2, row).IsNull()
		                          ? fallback_catalog
		                          : result->Collection().GetValue(2, row).ToString();
		source.schema_name = result->Collection().GetValue(3, row).IsNull()
		                         ? fallback_schema
		                         : result->Collection().GetValue(3, row).ToString();
		auto delta_schema = result->Collection().GetValue(4, row).IsNull()
		                        ? string()
		                        : result->Collection().GetValue(4, row).ToString();
		auto delta_name = result->Collection().GetValue(5, row).IsNull()
		                      ? string()
		                      : result->Collection().GetValue(5, row).ToString();
		SetSourceDelta(SourceTableName(source.table_name, source.catalog_name, source.schema_name), source.schema_name,
		               delta_schema, delta_name, source.delta_schema_name, source.delta_table_name);
		sources.push_back(std::move(source));
	}
	return sources;
}

bool RefreshMetadata::HasSourceDeltaConsumers(Connection &con, const DeltaSource &source,
                                              optional_ptr<TransactionalMVMetadataState> transaction_state,
                                              const unordered_map<string, string> &excluded_consumers) {
	auto matches = [&](Connection &reader, const MetadataLocation &location, const string &target) {
		auto table = location.Table(openivm::DELTA_TABLES_TABLE);
		auto query = "SELECT count(*) FROM " + target + " WHERE " +
		             SourcePredicate(source.table_name, source.catalog_name, source.schema_name);
		auto consumers = excluded_consumers.find(table);
		if (consumers != excluded_consumers.end() && !consumers->second.empty()) {
			query += " AND view_name NOT IN (" + consumers->second + ")";
		}
		auto remaining = reader.Query(query);
		if (remaining->HasError()) {
			throw CatalogException("OpenIVM could not verify delta-table consumers for '%s': %s", source.table_name,
			                       remaining->GetError());
		}
		OPENIVM_DEBUG_PRINT("[DELTA OWNERSHIP] Checked %s with transaction replay=%d\n", table.c_str(),
		                    transaction_state ? 1 : 0);
		return remaining->Collection().GetValue(0, 0).GetValue<int64_t>() != 0;
	};
	return AnyMetadataOwnership(con, openivm::DELTA_TABLES_TABLE, transaction_state, matches);
}

string RefreshMetadata::ResolveDeltaQualifiedName(const string &view_name, const string &delta_table_name,
                                                  const string &fallback_catalog, const string &fallback_schema) {
	auto loc = GetSourceLocation(view_name, delta_table_name, fallback_catalog, fallback_schema);
	if (loc.catalog_name.empty()) {
		return SqlUtils::QuoteIdentifier(loc.delta_table_name);
	}
	auto schema = loc.delta_schema_name.empty() ? string("main") : loc.delta_schema_name;
	return SqlUtils::FullName(loc.catalog_name, schema, loc.delta_table_name);
}

RefreshMetadata::DeltaChangeStats RefreshMetadata::GetStandardDeltaChangeStats(const string &delta_table_sql,
                                                                               const string &last_update) {
	DeltaChangeStats stats;
	if (last_update.empty()) {
		return stats;
	}
	auto result =
	    con.Query("SELECT COUNT(*), SUM(CASE WHEN " + string(openivm::MULTIPLICITY_COL) +
	              " < 0 THEN 1 ELSE 0 END) FROM " + delta_table_sql + " WHERE " + string(openivm::TIMESTAMP_COL) +
	              " >= '" + SqlUtils::EscapeValue(last_update) + "'::TIMESTAMP");
	// This reads delta data, not metadata. Callers treat !ok conservatively (pending changes and deletes),
	// so an unreadable delta table never makes a refresh skip work.
	if (result->HasError() || result->RowCount() == 0 || result->Collection().GetValue(0, 0).IsNull()) {
		return stats;
	}
	stats.ok = true;
	stats.total = result->Collection().GetValue(0, 0).GetValue<int64_t>();
	if (!result->Collection().GetValue(1, 0).IsNull()) {
		stats.deletes = result->Collection().GetValue(1, 0).GetValue<int64_t>();
	}
	return stats;
}

static string InformationSchemaRelationFilter(const string &catalog_name, const string &schema_name,
                                              const string &table_name) {
	string filter = SqlUtils::IdentifierEqualsSQL("table_name", Value(table_name).ToSQLString());
	if (!catalog_name.empty()) {
		filter += " AND " + SqlUtils::IdentifierEqualsSQL("table_catalog", Value(catalog_name).ToSQLString());
	}
	if (!schema_name.empty()) {
		filter += " AND " + SqlUtils::IdentifierEqualsSQL("table_schema", Value(schema_name).ToSQLString());
	}
	return filter;
}

vector<string> RefreshMetadata::GetTableColumns(const string &catalog_name, const string &schema_name,
                                                const string &table_name) {
	vector<string> columns;
	auto result = con.Query("SELECT column_name FROM information_schema.columns WHERE " +
	                        InformationSchemaRelationFilter(catalog_name, schema_name, table_name) +
	                        " ORDER BY ordinal_position");
	if (result->HasError()) {
		return columns;
	}
	for (idx_t i = 0; i < result->RowCount(); i++) {
		columns.push_back(result->Collection().GetValue(0, i).ToString());
	}
	return columns;
}

bool RefreshMetadata::TableColumnsMatch(const string &catalog_name, const string &schema_name, const string &table_name,
                                        const vector<string> &expected) {
	auto actual = GetTableColumns(catalog_name, schema_name, table_name);
	if (actual.size() != expected.size()) {
		return false;
	}
	for (idx_t i = 0; i < actual.size(); i++) {
		if (!StringUtil::CIEquals(actual[i], expected[i])) {
			return false;
		}
	}
	return true;
}

vector<string> RefreshMetadata::GetUpstreamViews(const string &view_name) {
	// Walk upstream: for view V, find its delta tables. If a delta table is "delta_X"
	// and X is a registered MV, then X is an upstream dependency. Recurse.
	vector<string> result;
	unordered_set<string> visited;

	std::function<void(const string &)> collect = [&](const string &vn) {
		auto delta_tables = GetDeltaTables(vn);
		for (auto &dt : delta_tables) {
			// Extract source MV name from delta table name.
			// Standard: "delta_<source>", DuckLake: "openivm_data_<source>"
			string source;
			static const string delta_prefix(openivm::DELTA_PREFIX);
			static const string data_prefix(openivm::DATA_TABLE_PREFIX);
			if (dt.size() > delta_prefix.size() && dt.substr(0, delta_prefix.size()) == delta_prefix) {
				source = dt.substr(delta_prefix.size());
			} else if (dt.size() > data_prefix.size() && dt.substr(0, data_prefix.size()) == data_prefix) {
				source = dt.substr(data_prefix.size());
			}
			source = PublishedSourceViewName(source.empty() ? dt : source);
			if (!source.empty() && !IsBaseTable(source) && visited.find(source) == visited.end()) {
				visited.insert(source);
				collect(source); // recurse deeper first (ancestors before descendants)
				result.push_back(source);
			}
		}
	};
	collect(view_name);
	return result; // topological order: ancestors first
}

static vector<string> GetDownstreamViewsInternal(Connection &con, const string &view_name, bool throw_on_error) {
	// Find all reachable views that depend on delta_<view_name> or openivm_data_<view_name> as a source.
	// DuckLake chained MVs use openivm_data_* (data table) instead of delta_* (delta table).
	//
	// The returned order must be topological over the reachable downstream DAG. Reverse postorder DFS keeps
	// fanout branches before their fan-in children.
	vector<string> result;
	unordered_set<string> visited;

	std::function<void(const string &)> collect = [&](const string &vn) {
		string delta_name = SqlUtils::DeltaName(vn);
		string data_name = IncrementalTableNames::DataTableName(vn);
		auto dependents = con.Query(
		    "SELECT DISTINCT view_name FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE table_name = '" +
		    SqlUtils::EscapeValue(delta_name) + "' OR table_name = '" + SqlUtils::EscapeValue(data_name) +
		    "' OR table_name = '" + SqlUtils::EscapeValue(PublishedViewName(vn)) + "' OR table_name = '" +
		    SqlUtils::EscapeValue(SqlUtils::DeltaName(PublishedViewName(vn))) + "' ORDER BY view_name");
		if (dependents->HasError() && throw_on_error) {
			throw CatalogException("Cannot resolve downstream consumers for '%s': %s", view_name,
			                       dependents->GetError());
		}
		if (!dependents->HasError()) {
			for (size_t i = 0; i < dependents->RowCount(); i++) {
				string dep = dependents->Collection().GetValue(0, i).ToString();
				if (dep == view_name) {
					continue;
				}
				if (visited.insert(dep).second) {
					collect(dep);
					result.push_back(dep);
				}
			}
		}
	};
	collect(view_name);
	std::reverse(result.begin(), result.end());
	OPENIVM_DEBUG_PRINT("[CASCADE] View '%s' has %zu downstream refresh nodes\n", view_name.c_str(), result.size());
	return result; // topological order: downstream parents before fan-in children
}

vector<string> RefreshMetadata::GetDownstreamViews(const string &view_name) {
	return GetDownstreamViewsInternal(con, view_name, false);
}

vector<string> RefreshMetadata::GetDownstreamViewsStrict(const string &view_name) {
	return GetDownstreamViewsInternal(con, view_name, true);
}

vector<string> RefreshMetadata::GetPipelineRefreshOrder(const vector<string> &targets, const string &cascade_mode) {
	if (cascade_mode != "off" && cascade_mode != "upstream" && cascade_mode != "downstream" && cascade_mode != "both") {
		throw InvalidInputException("refresh_pipeline: unknown cascade mode '%s'", cascade_mode);
	}
	struct Node {
		vector<string> parents;
		vector<string> children;
		idx_t pending = 0;
	};
	std::map<string, Node> graph;
	auto views = con.Query("SELECT view_name FROM " + string(openivm::VIEWS_TABLE));
	if (views->HasError()) {
		throw CatalogException("refresh_pipeline: cannot read materialized views: %s", views->GetError());
	}
	for (idx_t i = 0; i < views->RowCount(); i++) {
		graph.emplace(views->Collection().GetValue(0, i).ToString(), Node());
	}
	std::set<string> selected;
	for (auto &requested : targets) {
		auto target = requested;
		if (graph.find(target) == graph.end()) {
			throw CatalogException("refresh_pipeline: materialized view '%s' does not exist", target);
		}
		selected.insert(target);
	}
	// Read the same source metadata used by single-view cascades, not the optional matcher edges.
	// A child reads the parent's internal tables, which may live in the parent's internal schema.
	auto edge_query = [&](const string &parent_schema) {
		return con.Query("SELECT DISTINCT p.view_name, d.view_name FROM " + string(openivm::VIEWS_TABLE) + " p JOIN " +
		                 string(openivm::DELTA_TABLES_TABLE) + " d ON (d.table_name = '" +
		                 string(openivm::DELTA_PREFIX) + "' || p.view_name OR d.table_name = '" +
		                 string(openivm::DATA_TABLE_PREFIX) + "' || p.view_name OR d.table_name = '" +
		                 string(openivm::VISIBLE_TABLE_PREFIX) + "' || p.view_name OR d.table_name = '" +
		                 string(openivm::DELTA_PREFIX) + string(openivm::VISIBLE_TABLE_PREFIX) +
		                 "' || p.view_name)"
		                 " AND (d.source_catalog IS NULL OR p.view_catalog IS NULL OR " +
		                 SqlUtils::IdentifierEqualsSQL("d.source_catalog", "p.view_catalog") + ")" +
		                 " AND (d.source_schema IS NULL OR p.view_schema IS NULL OR " +
		                 SqlUtils::IdentifierEqualsSQL("d.source_schema", parent_schema) + ")");
	};
	auto edges = edge_query("COALESCE(p.internal_schema, p.view_schema)");
	if (edges->HasError()) {
		edges = edge_query("p.view_schema"); // Metadata written before internal schemas existed.
	}
	if (edges->HasError()) {
		throw CatalogException("refresh_pipeline: cannot read dependencies: %s", edges->GetError());
	}
	for (idx_t i = 0; i < edges->RowCount(); i++) {
		auto parent = edges->Collection().GetValue(0, i).ToString();
		auto child = edges->Collection().GetValue(1, i).ToString();
		if (graph.find(child) == graph.end()) {
			throw CatalogException("refresh_pipeline: dependency refers to missing materialized view '%s'", child);
		}
		graph.at(parent).children.push_back(child);
		graph.at(child).parents.push_back(parent);
	}
	auto expand = [&](bool upstream) {
		vector<string> queue(selected.begin(), selected.end());
		for (idx_t i = 0; i < queue.size(); i++) {
			auto &node = graph.at(queue[i]);
			for (auto &next : upstream ? node.parents : node.children) {
				if (selected.insert(next).second) {
					queue.push_back(next);
				}
			}
		}
	};
	if (cascade_mode == "downstream" || cascade_mode == "both") {
		expand(false);
	}
	// Both completes the dependencies of the downstream selection, including co-parents.
	if (cascade_mode == "upstream" || cascade_mode == "both") {
		expand(true);
	}
	std::set<string> ready;
	for (auto &name : selected) {
		auto &node = graph.at(name);
		for (auto &parent : node.parents) {
			node.pending += selected.count(parent);
		}
		if (node.pending == 0) {
			ready.insert(name);
		}
	}
	vector<string> order;
	while (!ready.empty()) {
		auto name = *ready.begin();
		ready.erase(ready.begin());
		order.push_back(name);
		for (auto &child : graph.at(name).children) {
			if (selected.count(child) && --graph.at(child).pending == 0) {
				ready.insert(child);
			}
		}
	}
	if (order.size() != selected.size()) {
		throw InvalidInputException("refresh_pipeline: selected dependencies contain a cycle");
	}
	OPENIVM_DEBUG_PRINT("[PIPELINE] Selected %zu views for %zu targets (mode=%s)\n", order.size(), targets.size(),
	                    cascade_mode.c_str());
	return order;
}

bool RefreshMetadata::HasDownstreamViews(const string &view_name, bool include_published,
                                         optional_ptr<TransactionalMVMetadataState> transaction_state) {
	auto internal = GetInternalLocation(view_name);
	auto predicate =
	    "(" + SourcePredicate(SqlUtils::DeltaName(view_name), internal.catalog_name, internal.schema_name) + ")";
	if (include_published) {
		for (auto &source : {SqlUtils::DeltaName(PublishedViewName(view_name)), PublishedViewName(view_name)}) {
			predicate += " OR (" + SourcePredicate(source, internal.catalog_name, internal.schema_name) + ")";
		}
	}
	auto matches = [&](Connection &reader, const MetadataLocation &, const string &target) {
		auto result = reader.Query("SELECT 1 FROM " + target + " WHERE " + predicate + " LIMIT 1");
		if (result->HasError()) {
			throw CatalogException("Cannot resolve downstream consumers for '%s': %s", view_name, result->GetError());
		}
		OPENIVM_DEBUG_PRINT("[DELTA OWNERSHIP] Downstream consumers for %s in %s: %d\n", view_name.c_str(),
		                    target.c_str(), result->RowCount() != 0);
		return result->RowCount() != 0;
	};
	return AnyMetadataOwnership(con, openivm::DELTA_TABLES_TABLE, transaction_state, matches);
}

vector<string> RefreshMetadata::ReadViewList(const string &view_name, const string &column) {
	vector<string> values;
	std::istringstream stream(ReadViewString(view_name, column));
	string token;
	while (std::getline(stream, token, ',')) {
		if (!token.empty()) {
			values.push_back(token);
		}
	}
	return values;
}

vector<string> RefreshMetadata::GetGroupColumns(const string &view_name) {
	return ReadViewList(view_name, "group_columns");
}

vector<string> RefreshMetadata::GetWindowOrderColumns(const string &view_name) {
	return ReadViewList(view_name, "window_order_columns");
}

vector<string> RefreshMetadata::GetAggregateTypes(const string &view_name) {
	return ReadViewList(view_name, "aggregate_types");
}

string RefreshMetadata::GetHavingPredicate(const string &view_name) {
	return ReadViewString(view_name, "having_predicate");
}

GroupRecomputeAffectedMode RefreshMetadata::GetGroupRecomputeAffectedMode(const string &view_name) {
	auto value = ReadViewValue(view_name, "group_recompute_affected_mode");
	if (value.IsNull()) {
		return GroupRecomputeAffectedMode::CURRENT_DIFF;
	}
	string mode = value.ToString();
	if (StringUtil::CIEquals(mode, GroupRecomputeAffectedModeName(GroupRecomputeAffectedMode::DIRECT_SOURCE_KEYS))) {
		return GroupRecomputeAffectedMode::DIRECT_SOURCE_KEYS;
	}
	if (StringUtil::CIEquals(mode, GroupRecomputeAffectedModeName(GroupRecomputeAffectedMode::SOURCE_DELTA))) {
		return GroupRecomputeAffectedMode::SOURCE_DELTA;
	}
	if (StringUtil::CIEquals(
	        mode, GroupRecomputeAffectedModeName(GroupRecomputeAffectedMode::SOURCE_DELTA_RELAX_AGGREGATE_FILTER))) {
		return GroupRecomputeAffectedMode::SOURCE_DELTA_RELAX_AGGREGATE_FILTER;
	}
	return GroupRecomputeAffectedMode::CURRENT_DIFF;
}

int64_t RefreshMetadata::GetRefreshInterval(const string &view_name) {
	auto value = ReadViewValue(view_name, "refresh_interval");
	return value.IsNull() ? -1 : value.GetValue<int64_t>();
}

vector<RefreshMetadata::ScheduledView> RefreshMetadata::GetScheduledViews() {
	// Scheduler metadata belongs to native catalogs or an explicit metadata location.
	// Scanning duckdb_tables() also opens external catalogs, taking DuckLake/SQLite
	// read locks while a foreground refresh may be committing its writes.
	string query;
	for (auto &location : MetadataLocations(con)) {
		if (!con.TableInfo(Identifier(location.catalog), Identifier(location.schema),
		                   Identifier(openivm::VIEWS_TABLE))) {
			continue;
		}
		auto catalog_literal = "'" + SqlUtils::EscapeValue(location.catalog) + "'";
		auto schema_literal = "'" + SqlUtils::EscapeValue(location.schema) + "'";
		if (!query.empty()) {
			query += " UNION ALL ";
		}
		query += "SELECT v.view_name, COALESCE(v.view_catalog, " + catalog_literal +
		         "), "
		         "COALESCE(v.view_schema, 'main'), v.refresh_interval, "
		         "(SELECT MIN(d.last_update) FROM " +
		         location.Table(openivm::DELTA_TABLES_TABLE) + " d WHERE d.view_name = v.view_name), " +
		         catalog_literal + ", " + schema_literal + " FROM " + location.Table(openivm::VIEWS_TABLE) +
		         " v WHERE v.refresh_interval IS NOT NULL";
	}
	if (query.empty()) {
		return {};
	}
	auto result = con.Query(query);
	vector<ScheduledView> views;
	if (result->HasError()) {
		OPENIVM_DEBUG_PRINT("[REFRESH DAEMON] GetScheduledViews query error: %s\n", result->GetError().c_str());
	}
	if (!result->HasError()) {
		for (size_t i = 0; i < result->RowCount(); i++) {
			ScheduledView sv;
			sv.metadata_catalog = result->Collection().GetValue(5, i).ToString();
			sv.metadata_schema = result->Collection().GetValue(6, i).ToString();
			sv.view_name = result->Collection().GetValue(0, i).ToString();
			sv.catalog_name =
			    result->Collection().GetValue(1, i).IsNull() ? "" : result->Collection().GetValue(1, i).ToString();
			sv.schema_name = result->Collection().GetValue(2, i).IsNull()
			                     ? DEFAULT_SCHEMA
			                     : result->Collection().GetValue(2, i).ToString();
			sv.interval_seconds = result->Collection().GetValue(3, i).GetValue<int64_t>();
			sv.last_update =
			    result->Collection().GetValue(4, i).IsNull() ? "" : result->Collection().GetValue(4, i).ToString();
			views.push_back(sv);
		}
	}
	std::sort(views.begin(), views.end(), [](const ScheduledView &left, const ScheduledView &right) {
		if (left.metadata_catalog != right.metadata_catalog) {
			return left.metadata_catalog < right.metadata_catalog;
		}
		if (left.metadata_schema != right.metadata_schema) {
			return left.metadata_schema < right.metadata_schema;
		}
		return left.view_name < right.view_name;
	});
	return views;
}

void RefreshMetadata::SetRefreshInProgress(const string &view_name, bool in_progress) {
	con.Query("UPDATE " + string(openivm::VIEWS_TABLE) + " SET refresh_in_progress = " +
	          (in_progress ? "true" : "false") + " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "'");
}

string RefreshMetadata::BuildDeltaConsumersSQL(const string &target, const string &metadata_key,
                                               const string &delta_metadata_table,
                                               const vector<string> &delta_metadata_tables,
                                               const string &source_catalog, const string &source_schema) {
	string qtarget = target.find('.') == string::npos ? SqlUtils::QuoteIdentifier(target) : target;
	auto metadata_table = delta_metadata_table.empty() ? string(openivm::DELTA_TABLES_TABLE) : delta_metadata_table;
	auto consumers = "SELECT last_update FROM " + metadata_table + " WHERE table_name = '" +
	                 SqlUtils::EscapeValue(metadata_key) + "'";
	auto parts = SqlUtils::ParseQualifiedIdentifier(qtarget);
	if (parts.size() == 3) {
		// Consumers are keyed by the source, which an internal-schema delta does not reveal.
		auto predicate = source_catalog.empty() ? SourcePredicate(parts[2], parts[0], parts[1])
		                                        : SourcePredicate(metadata_key, source_catalog, source_schema);
		consumers = "SELECT last_update FROM " + metadata_table + " WHERE " + predicate;
		for (auto &other : delta_metadata_tables) {
			if (!StringUtil::CIEquals(other, metadata_table)) {
				consumers += " UNION ALL SELECT last_update FROM " + other + " WHERE " + predicate;
			}
		}
	}
	return consumers;
}

string RefreshMetadata::BuildDeltaCleanupSQL(const string &target, const string &metadata_key,
                                             const string &delta_metadata_table, vector<string> *deferred_cleanup,
                                             const vector<string> &delta_metadata_tables, const string &source_catalog,
                                             const string &source_schema, const string &data_catalog) {
	string qtarget = target.find('.') == string::npos ? SqlUtils::QuoteIdentifier(target) : target;
	auto consumers = BuildDeltaConsumersSQL(qtarget, metadata_key, delta_metadata_table, delta_metadata_tables,
	                                        source_catalog, source_schema);
	auto sql = "DELETE FROM " + qtarget + " WHERE NOT EXISTS (" + consumers + ") OR " + string(openivm::TIMESTAMP_COL) +
	           " < (SELECT MIN(last_update) FROM (" + consumers + ") consumers);\n";
	auto parts = SqlUtils::ParseQualifiedIdentifier(qtarget);
	if (deferred_cleanup && parts.size() == 3) {
		auto metadata_table = delta_metadata_table.empty() ? string(openivm::DELTA_TABLES_TABLE) : delta_metadata_table;
		auto metadata_parts = SqlUtils::ParseQualifiedIdentifier(metadata_table);
		// Inline cleanup must write the database the refresh's data transaction writes; a
		// source delta stored next to separately placed metadata is in another one.
		bool outside_data = !data_catalog.empty() && !StringUtil::CIEquals(parts[0], data_catalog);
		if ((metadata_parts.size() == 3 && !StringUtil::CIEquals(parts[0], metadata_parts[0])) || outside_data) {
			// Only housekeeping crosses the native write boundary. MV state and its
			// watermark commit together; cleanup reads the committed consumer minimum.
			deferred_cleanup->push_back(std::move(sql));
			return "";
		}
	}
	return sql;
}

// --- DuckLake support ---

string RefreshMetadata::GetCatalogType(const string &view_name, const string &table_name) {
	auto value = ReadDeltaValue(view_name, table_name, "catalog_type");
	return value.IsNull() ? "duckdb" : value.ToString();
}

bool RefreshMetadata::IsDuckLakeTable(const string &view_name, const string &table_name) {
	return GetCatalogType(view_name, table_name) == "ducklake";
}

bool RefreshMetadata::IsDuckLakeCatalog(const string &catalog_name) {
	if (catalog_name.empty()) {
		return false;
	}
	auto result =
	    con.Query("SELECT type FROM duckdb_databases() WHERE " +
	              SqlUtils::IdentifierEqualsSQL("database_name", Value(catalog_name).ToSQLString()) + " LIMIT 1");
	if (result->HasError()) {
		throw CatalogException("OpenIVM could not resolve the type of catalog '%s': %s", catalog_name,
		                       result->GetError());
	}
	return result->RowCount() > 0 && !result->Collection().GetValue(0, 0).IsNull() &&
	       StringUtil::CIEquals(result->Collection().GetValue(0, 0).ToString(), "ducklake");
}

bool RefreshMetadata::IsNativeCatalog(const string &catalog_name) {
	return MetadataLocator::IsNativeCatalog(con, catalog_name);
}

int64_t RefreshMetadata::GetCurrentDuckLakeSnapshot(const string &catalog_name) {
	if (catalog_name.empty()) {
		return -1;
	}
	auto result = con.Query("SELECT id FROM " + SqlUtils::QuoteIdentifier(catalog_name) + ".current_snapshot()");
	if (result->HasError()) {
		throw InvalidInputException("Could not read current snapshot of catalog '%s': %s", catalog_name,
		                            result->GetError());
	}
	if (result->RowCount() == 0 || result->Collection().GetValue(0, 0).IsNull()) {
		return -1;
	}
	return result->Collection().GetValue(0, 0).GetValue<int64_t>();
}

int64_t RefreshMetadata::GetLastSnapshotId(const string &view_name, const string &table_name) {
	auto value = ReadDeltaValue(view_name, table_name, "last_snapshot_id");
	return value.IsNull() ? -1 : value.GetValue<int64_t>();
}

RefreshMetadata::DuckLakeSourceIdentity RefreshMetadata::ResolveDuckLakeSourceIdentity(const string &view_name,
                                                                                       const string &table_name,
                                                                                       const string &catalog_name,
                                                                                       const string &schema_name) {
	DuckLakeSourceIdentity identity;
	auto result =
	    con.Query("SELECT source_table_id FROM " + string(openivm::DELTA_TABLES_TABLE) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND " + SourcePredicate(table_name, catalog_name, schema_name));
	if (result->HasError()) {
		throw InvalidInputException("Could not read source_table_id for '%s' of materialized view '%s': %s", table_name,
		                            view_name, result->GetError());
	}
	if (result->RowCount() > 0 && !result->Collection().GetValue(0, 0).IsNull()) {
		identity.stored_table_id = result->Collection().GetValue(0, 0).GetValue<int64_t>();
	}
	if (catalog_name.empty()) {
		return identity;
	}

	string catalog_prefix = SqlUtils::QuoteIdentifier("__ducklake_metadata_" + catalog_name) + ".";
	string schema_filter = schema_name.empty() ? "main" : schema_name;
	auto current_result = con.Query(
	    "SELECT t.table_id FROM " + catalog_prefix + "ducklake_table t JOIN " + catalog_prefix +
	    "ducklake_schema s ON t.schema_id = s.schema_id WHERE "
	    "t.end_snapshot IS NULL AND "
	    "s.end_snapshot IS NULL AND t.table_name = '" +
	    SqlUtils::EscapeValue(SourceTableName(table_name, catalog_name, schema_name)) + "' AND s.schema_name = '" +
	    SqlUtils::EscapeValue(schema_filter) + "' ORDER BY t.table_id DESC LIMIT 1");
	if (current_result->HasError() || current_result->RowCount() == 0 ||
	    current_result->Collection().GetValue(0, 0).IsNull()) {
		return identity;
	}

	identity.resolved = true;
	identity.current_table_id = current_result->Collection().GetValue(0, 0).GetValue<int64_t>();
	identity.changed = identity.stored_table_id >= 0 && identity.stored_table_id != identity.current_table_id;
	if (identity.stored_table_id == identity.current_table_id) {
		return identity;
	}
	auto update =
	    con.Query("UPDATE " + string(openivm::DELTA_TABLES_TABLE) +
	              " SET source_table_id = " + to_string(identity.current_table_id) + " WHERE view_name = '" +
	              SqlUtils::EscapeValue(view_name) + "' AND " + SourcePredicate(table_name, catalog_name, schema_name));
	if (update->HasError()) {
		OPENIVM_DEBUG_PRINT("[DuckLake] Could not backfill source_table_id for %s.%s: %s\n", view_name.c_str(),
		                    table_name.c_str(), update->GetError().c_str());
	}
	return identity;
}

int64_t RefreshMetadata::GetDuckLakeTableIdAt(const string &catalog_name, const string &schema_name,
                                              const string &table_name, int64_t snapshot_id) {
	if (catalog_name.empty() || table_name.empty() || snapshot_id < 0) {
		return -1;
	}
	string catalog_prefix = SqlUtils::QuoteIdentifier("__ducklake_metadata_" + catalog_name) + ".";
	string schema_filter = schema_name.empty() ? "main" : schema_name;
	string snapshot = to_string(snapshot_id);
	auto visible = [&](const string &alias) {
		return alias + ".begin_snapshot <= " + snapshot + " AND (" + alias + ".end_snapshot IS NULL OR " + alias +
		       ".end_snapshot > " + snapshot + ")";
	};
	auto result =
	    con.Query("SELECT t.table_id FROM " + catalog_prefix + "ducklake_table t JOIN " + catalog_prefix +
	              "ducklake_schema s ON t.schema_id = s.schema_id WHERE " + visible("t") + " AND " + visible("s") +
	              " AND t.table_name = '" + SqlUtils::EscapeValue(table_name) + "' AND s.schema_name = '" +
	              SqlUtils::EscapeValue(schema_filter) + "' ORDER BY t.table_id DESC LIMIT 1");
	if (result->HasError() || result->RowCount() == 0 || result->Collection().GetValue(0, 0).IsNull()) {
		return -1;
	}
	return result->Collection().GetValue(0, 0).GetValue<int64_t>();
}

string RefreshMetadata::BuildDuckLakeRefreshMetadataSQL(const string &view_name, const string &table_name,
                                                        const string &snapshot_expr,
                                                        const string &delta_metadata_table) {
	auto metadata_table = delta_metadata_table.empty() ? string(openivm::DELTA_TABLES_TABLE) : delta_metadata_table;
	return "UPDATE " + metadata_table + " SET last_snapshot_id = " + snapshot_expr +
	       ", last_update = " + string(openivm::UTC_NOW_SQL) + ", last_refresh_ts = " + string(openivm::UTC_NOW_SQL) +
	       " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND table_name = '" +
	       SqlUtils::EscapeValue(table_name) + "';\n";
}

void RefreshMetadata::UpdateDuckLakeRefreshMetadata(const string &view_name, const string &table_name,
                                                    int64_t snapshot_id) {
	auto result = con.Query(BuildDuckLakeRefreshMetadataSQL(view_name, table_name, to_string(snapshot_id)));
	if (result->HasError()) {
		throw Exception(ExceptionType::EXECUTOR, "Cannot update DuckLake refresh metadata: " + result->GetError());
	}
}

// --- Refresh history (learned cost model) ---

void RefreshMetadata::RecordRefreshHistory(const string &view_name, const string &method,
                                           double incremental_compute_est, double incremental_upsert_est,
                                           double recompute_compute_est, double recompute_replace_est,
                                           int64_t actual_duration_ms, idx_t max_history) {
	// Remote metadata catalogs do not apply DuckDB column defaults; supply the key explicitly.
	auto result = con.Query("INSERT INTO " + string(openivm::HISTORY_TABLE) +
	                        " (view_name, refresh_timestamp, method, incremental_compute_est, incremental_upsert_est,"
	                        " recompute_compute_est, recompute_replace_est, actual_duration_ms)"
	                        " VALUES ('" +
	                        SqlUtils::EscapeValue(view_name) + "', CAST(current_timestamp AS TIMESTAMP), '" +
	                        SqlUtils::EscapeValue(method) + "', " + to_string(incremental_compute_est) + ", " +
	                        to_string(incremental_upsert_est) + ", " + to_string(recompute_compute_est) + ", " +
	                        to_string(recompute_replace_est) + ", " + to_string(actual_duration_ms) + ")");
	if (result->HasError()) {
		OPENIVM_DEBUG_PRINT("[HISTORY] Failed to record: %s\n", result->GetError().c_str());
		return;
	}

	// Prune old entries beyond the window
	con.Query("DELETE FROM " + string(openivm::HISTORY_TABLE) + " WHERE view_name = '" +
	          SqlUtils::EscapeValue(view_name) + "' AND refresh_timestamp NOT IN (SELECT refresh_timestamp FROM " +
	          string(openivm::HISTORY_TABLE) + " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) +
	          "' ORDER BY refresh_timestamp DESC LIMIT " + to_string(max_history) + ")");
}

vector<RefreshMetadata::RefreshHistoryEntry> RefreshMetadata::GetRefreshHistory(const string &view_name,
                                                                                const string &method, idx_t limit) {
	// Select the cost components for the given method:
	// - 'full': use recompute_compute_est, recompute_replace_est
	// - 'incremental' / 'group_recompute' / 'window_partition' (anything else):
	//   use incremental_compute_est, incremental_upsert_est. The cost model writes the chosen
	//   strategy's compute/upsert cost into those columns at refresh time, so the
	//   regression learns weights specific to whichever non-full path the view uses.
	bool is_full = (method == "full");
	string col1 = is_full ? "recompute_compute_est" : "incremental_compute_est";
	string col2 = is_full ? "recompute_replace_est" : "incremental_upsert_est";

	auto result =
	    con.Query("SELECT " + col1 + ", " + col2 + ", actual_duration_ms FROM " + string(openivm::HISTORY_TABLE) +
	              " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) + "' AND method = '" +
	              SqlUtils::EscapeValue(method) + "' ORDER BY refresh_timestamp ASC LIMIT " + to_string(limit));

	vector<RefreshHistoryEntry> entries;
	if (result->HasError() || result->RowCount() == 0) {
		return entries;
	}
	for (idx_t i = 0; i < result->RowCount(); i++) {
		RefreshHistoryEntry entry;
		entry.compute_est = result->Collection().GetValue(0, i).GetValue<double>();
		entry.upsert_est = result->Collection().GetValue(1, i).GetValue<double>();
		entry.actual_ms = result->Collection().GetValue(2, i).GetValue<double>();
		entries.push_back(entry);
	}
	return entries;
}

// --- JSON metadata: each struct lists its fields once; MetadataJson::Write/Read serialize them. ---
using MetadataJson::Presence;

template <class IO>
static void Fields(IO &io, typename IO::template Field<DerivedAggregateOutput> &m) {
	io.Str("column", m.output_column);
	io.Str("expression", m.expression_sql);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::GroupRecomputeSourceOccurrence> &m) {
	io.Str("table", m.table_name);
	io.Index("count", m.count);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::DistinctAuxMeta> &m) {
	io.Str("aux_table", m.aux_table);
	io.Arr("cols", m.cols);
	io.Arr("source_exprs", m.source_exprs, Presence::OPTIONAL);
	io.Str("input_sql", m.input_sql);
	io.Str("source", m.source);
	io.Str("filter", m.filter, Presence::OPTIONAL); // empty without a WHERE clause
	io.Str("sum_arg", m.sum_arg);
	io.Str("sum_out", m.sum_out);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::CountDistinctAuxMeta> &m) {
	io.Str("aux_table", m.aux_table);
	io.Str("source", m.source);
	io.Arr("group_cols", m.group_cols);
	io.Arr("group_source_exprs", m.group_source_exprs, Presence::OPTIONAL);
	io.Str("distinct_col", m.distinct_col);
	io.Str("distinct_expr", m.distinct_expr);
	io.Str("output_col", m.output_col);
	io.Str("filter", m.filter, Presence::OPTIONAL);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::SemiAntiAuxMeta> &m) {
	io.Str("aux_table", m.aux_table);
	io.Enum("join_type", m.join_type, SEMI_ANTI_JOIN_TYPE_NAMES);
	io.Str("left_table", m.left_table);
	io.Str("left_alias", m.left_alias);
	io.Str("right_table", m.right_table);
	io.Str("right_alias", m.right_alias);
	io.Str("predicate", m.predicate);
	io.Str("post_filter", m.post_filter, Presence::OPTIONAL);
	io.Str("right_filter", m.right_filter, Presence::OPTIONAL);
	io.Bool("null_aware", m.null_aware, Presence::OPTIONAL);
	io.Str("null_aware_left_col", m.null_aware_left_col, Presence::OPTIONAL);
	io.Str("null_aware_right_expr", m.null_aware_right_expr, Presence::OPTIONAL);
	io.Arr("left_cols", m.left_cols);
	io.Arr("left_exprs", m.left_exprs, Presence::OPTIONAL);
	io.Arr("output_cols", m.output_cols);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::WindowPartitionLineageOp> &m) {
	io.Enum("k", m.kind, WINDOW_LINEAGE_KIND_NAMES);
	io.Str("out", m.output_col);
	io.Str("source", m.source);
	io.Str("source_col", m.source_col);
	io.Str("source_cast", m.source_cast, Presence::OMIT_EMPTY);
	if (m.kind == WindowLineageKind::LOOKUP) {
		io.Str("lookup", m.lookup);
		io.Str("lookup_col", m.lookup_col);
		io.Str("lookup_out", m.lookup_out);
		io.Str("lookup_cast", m.lookup_cast, Presence::OMIT_EMPTY);
		io.Str("lookup_out_cast", m.lookup_out_cast, Presence::OMIT_EMPTY);
	}
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::ProjectionKeyLineage> &m) {
	io.Tag("k", "projection_key");
	io.Str("out", m.output_col);
	io.Str("key_source", m.key_source);
	io.Index("key_occ", m.key_occurrence);
	io.Str("key_col", m.key_col);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::ProjectionKeyLineageArm> &m) {
	io.Str("source", m.source);
	io.Index("occ", m.occurrence);
	io.Str("source_col", m.source_col);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::LeftJoinKeySource> &m) {
	io.Tag("k", "left_join_key_source");
	io.Str("table", m.table);
	io.Index("occ", m.occurrence);
	io.Str("column", m.column);
	io.Str("cast_type", m.cast_type, Presence::OPTIONAL);
	io.Bool("cardinality_transition_check_safe", m.cardinality_transition_check_safe, Presence::OPTIONAL);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::FullOuterScopeInput> &m) {
	io.Str("table", m.table);
	io.Index("occ", m.occurrence);
	io.Index("presence_index", m.presence_index);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::LeftJoinNullableSources> &m) {
	io.Tag("k", "left_join_nullable");
	io.Bool("complete", m.complete, Presence::OPTIONAL);
	io.Arr("tables", m.tables, Presence::OPTIONAL);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::FilteredGroupCountAuxMeta> &m) {
	io.Tag("kind", "filtered_group_count");
	io.Str("aux_table", m.aux_table);
	io.Str("source", m.source);
	io.Str("group_col", m.group_col);
	io.Str("sum_col", m.sum_col);
	io.Str("source_group_expr", m.source_group_expr, Presence::OPTIONAL);
	io.Str("source_sum_expr", m.source_sum_expr, Presence::OPTIONAL);
	io.Str("output_col", m.output_col);
	io.Str("op", m.comparison_op);
	io.Str("threshold", m.threshold_sql);
}

template <class IO>
static void Fields(IO &io, typename IO::template Field<RefreshMetadata::LeftJoinSecondaryMeta> &m) {
	io.Tag("kind", "leftjoin_secondary");
	io.Str("sql", m.sql);
	io.Arr("preserved_cols", m.preserved_cols);
	io.Arr("inner_tables", m.inner_tables);
	io.Arr("inner_keys", m.inner_keys);
	io.Arr("pres_tables", m.pres_tables);
	io.Arr("pres_keys", m.pres_keys);
}

template <class T>
static vector<string> WriteEach(const vector<T> &values) {
	vector<string> objects;
	for (auto &value : values) {
		objects.push_back(MetadataJson::Write(value));
	}
	return objects;
}

// One entry of the lineage_json `items` array, selected by its "k".
static string ReadRefreshLineageEntry(const string &json, const string &kind) {
	for (auto &object : MetadataJson::ExtractJsonObjectsFromArray(json, "items")) {
		string object_kind;
		if (MetadataJson::ExtractJsonString(object, "k", object_kind) && object_kind == kind) {
			return object;
		}
	}
	return "";
}

vector<RefreshMetadata::GroupRecomputeSourceOccurrence>
RefreshMetadata::GetGroupRecomputeSourceOccurrences(const string &view_name) {
	vector<GroupRecomputeSourceOccurrence> occurrences;
	auto json = ReadViewString(view_name, "group_recompute_source_occurrences_json");
	for (auto &object : MetadataJson::ExtractJsonObjectsFromArray(json, "sources")) {
		GroupRecomputeSourceOccurrence occurrence;
		if (MetadataJson::Read(object, occurrence) && !occurrence.table_name.empty()) {
			occurrences.push_back(std::move(occurrence));
		}
	}
	return occurrences;
}

string
RefreshMetadata::GroupRecomputeSourceOccurrencesToJson(const vector<GroupRecomputeSourceOccurrence> &occurrences) {
	MetadataJson::Writer writer;
	writer.Objects("sources", WriteEach(occurrences));
	return writer.Finish();
}

DerivedAggregateOutputInfo RefreshMetadata::GetDerivedAggregateOutputs(const string &view_name) {
	DerivedAggregateOutputInfo info;
	const auto json = ReadViewString(view_name, "derived_aggregate_outputs_json");
	const bool has_complete_prefix = StringUtil::StartsWith(json, "{\"complete\":true,\"outputs\":[");
	if (!has_complete_prefix && !StringUtil::StartsWith(json, "{\"complete\":false,\"outputs\":[")) {
		return info;
	}
	bool parsed_outputs_complete = false;
	auto objects = MetadataJson::ExtractJsonObjectsFromArray(json, "outputs", parsed_outputs_complete);
	info.complete = has_complete_prefix && parsed_outputs_complete && StringUtil::EndsWith(json, "]}");
	for (auto &object : objects) {
		DerivedAggregateOutput output;
		if (MetadataJson::Read(object, output) && !output.output_column.empty() && !output.expression_sql.empty()) {
			info.outputs.push_back(std::move(output));
		} else {
			info.complete = false;
		}
	}
	return info;
}

string RefreshMetadata::DerivedAggregateOutputsToJson(const DerivedAggregateOutputInfo &info) {
	MetadataJson::Writer writer;
	writer.Raw("complete", info.complete ? "true" : "false");
	writer.Objects("outputs", WriteEach(info.outputs));
	return writer.Finish();
}

bool RefreshMetadata::GetDistinctAuxMeta(const string &view_name, DistinctAuxMeta &out) {
	return MetadataJson::Read(ReadViewString(view_name, "distinct_aux_meta_json"), out);
}

string RefreshMetadata::DistinctAuxMetaToJson(const DistinctAuxMeta &meta) {
	return MetadataJson::Write(meta);
}

bool RefreshMetadata::GetCountDistinctAuxMeta(const string &view_name, CountDistinctAuxMeta &out) {
	return MetadataJson::Read(ReadViewString(view_name, "count_distinct_aux_meta_json"), out);
}

string RefreshMetadata::CountDistinctAuxMetaToJson(const CountDistinctAuxMeta &meta) {
	return MetadataJson::Write(meta);
}

bool RefreshMetadata::GetSemiAntiAuxMeta(const string &view_name, SemiAntiAuxMeta &out) {
	return MetadataJson::Read(ReadViewString(view_name, "semi_anti_aux_meta_json"), out);
}

string RefreshMetadata::SemiAntiAuxMetaToJson(const SemiAntiAuxMeta &meta) {
	return MetadataJson::Write(meta);
}

bool RefreshMetadata::GetWindowPartitionLineage(const string &view_name, vector<WindowPartitionLineageOp> &out) {
	out.clear();
	auto json = ReadRefreshLineageEntry(ReadViewString(view_name, "lineage_json"), "window_partition");
	for (auto &object : MetadataJson::ExtractJsonObjectsFromArray(json, "ops")) {
		WindowPartitionLineageOp op;
		if (MetadataJson::Read(object, op)) {
			out.push_back(std::move(op));
		}
	}
	return !out.empty();
}

string RefreshMetadata::WindowPartitionLineageToJson(const vector<WindowPartitionLineageOp> &ops) {
	MetadataJson::Writer writer;
	writer.Tag("k", "window_partition");
	writer.Objects("ops", WriteEach(ops));
	return writer.Finish();
}

bool RefreshMetadata::GetProjectionKeyLineage(const string &view_name, ProjectionKeyLineage &out) {
	auto json = ReadRefreshLineageEntry(ReadViewString(view_name, "lineage_json"), "projection_key");
	if (!MetadataJson::Read(json, out)) {
		return false;
	}
	for (auto &object : MetadataJson::ExtractJsonObjectsFromArray(json, "arms")) {
		ProjectionKeyLineageArm arm;
		if (!MetadataJson::Read(object, arm)) {
			continue;
		}
		// Each step is "table|occurrence|lookup_col|lookup_out".
		vector<string> steps;
		MetadataJson::ExtractJsonStringArray(object, "steps", steps);
		for (auto &step_text : steps) {
			vector<string> parts(1);
			for (char c : step_text) {
				if (c == '|') {
					parts.emplace_back();
				} else {
					parts.back() += c;
				}
			}
			ProjectionKeyLineageStep step;
			if (parts.size() == 4 && MetadataJson::ParseIndex(parts[1], step.occurrence)) {
				step.table = parts[0];
				step.lookup_col = parts[2];
				step.lookup_out = parts[3];
				arm.steps.push_back(std::move(step));
			}
		}
		out.arms.push_back(std::move(arm));
	}
	return !out.arms.empty();
}

string RefreshMetadata::ProjectionKeyLineageToJson(const ProjectionKeyLineage &lineage) {
	vector<string> arms;
	for (auto &arm : lineage.arms) {
		MetadataJson::Writer arm_writer;
		Fields(arm_writer, arm);
		vector<string> steps;
		for (auto &step : arm.steps) {
			steps.push_back(step.table + "|" + to_string(step.occurrence) + "|" + step.lookup_col + "|" +
			                step.lookup_out);
		}
		arm_writer.Arr("steps", steps);
		arms.push_back(arm_writer.Finish());
	}
	MetadataJson::Writer writer;
	Fields(writer, lineage);
	writer.Objects("arms", arms);
	return writer.Finish();
}

bool RefreshMetadata::GetLeftJoinKeySource(const string &view_name, LeftJoinKeySource &out) {
	return MetadataJson::Read(
	    ReadRefreshLineageEntry(ReadViewString(view_name, "lineage_json"), "left_join_key_source"), out);
}

string RefreshMetadata::LeftJoinKeySourceToJson(const LeftJoinKeySource &source) {
	return MetadataJson::Write(source);
}

vector<RefreshMetadata::FullOuterScope> RefreshMetadata::GetFullOuterScopes(const string &view_name) {
	vector<FullOuterScope> scopes;
	auto json = ReadRefreshLineageEntry(ReadViewString(view_name, "lineage_json"), "full_outer_scopes");
	if (json.empty()) {
		return scopes;
	}
	for (auto &object : MetadataJson::ExtractJsonObjectsFromArray(json, "scopes")) {
		FullOuterScope scope;
		bool complete = true;
		for (idx_t side = 0; side < 2; side++) {
			auto &source = side == 0 ? scope.left : scope.right;
			string prefix = side == 0 ? "left_" : "right_";
			complete = complete && MetadataJson::ExtractJsonString(object, prefix + "key", source.key_column) &&
			           MetadataJson::ExtractJsonString(object, prefix + "presence", source.presence_column);
			for (auto &input_json : MetadataJson::ExtractJsonObjectsFromArray(object, prefix + "inputs")) {
				FullOuterScopeInput input;
				complete = MetadataJson::Read(input_json, input) && complete;
				source.inputs.push_back(std::move(input));
			}
			complete = complete && !source.inputs.empty();
		}
		if (complete) {
			scopes.push_back(std::move(scope));
		}
	}
	return scopes;
}

string RefreshMetadata::FullOuterScopesToJson(const vector<FullOuterScope> &scopes) {
	vector<string> objects;
	for (auto &scope : scopes) {
		vector<string> fields;
		for (idx_t side = 0; side < 2; side++) {
			auto &source = side == 0 ? scope.left : scope.right;
			string prefix = side == 0 ? "left_" : "right_";
			fields.push_back(SqlUtils::JsonQuote(prefix + "key") + ":" + SqlUtils::JsonQuote(source.key_column));
			fields.push_back(SqlUtils::JsonQuote(prefix + "presence") + ":" +
			                 SqlUtils::JsonQuote(source.presence_column));
			auto inputs = WriteEach(source.inputs);
			fields.push_back(SqlUtils::JsonQuote(prefix + "inputs") + ":[" + StringUtil::Join(inputs, ",") + "]");
		}
		objects.push_back("{" + StringUtil::Join(fields, ",") + "}");
	}
	return "{\"k\":\"full_outer_scopes\",\"scopes\":[" + StringUtil::Join(objects, ",") + "]}";
}

bool RefreshMetadata::GetLeftJoinNullableSources(const string &view_name, LeftJoinNullableSources &out) {
	return MetadataJson::Read(ReadRefreshLineageEntry(ReadViewString(view_name, "lineage_json"), "left_join_nullable"),
	                          out);
}

string RefreshMetadata::LeftJoinNullableSourcesToJson(const LeftJoinNullableSources &src) {
	return MetadataJson::Write(src);
}

bool RefreshMetadata::GetFilteredGroupCountAuxMeta(const string &view_name, FilteredGroupCountAuxMeta &out) {
	return MetadataJson::Read(ReadViewString(view_name, "aggregate_decomposition_json"), out);
}

string RefreshMetadata::FilteredGroupCountAuxMetaToJson(const FilteredGroupCountAuxMeta &meta) {
	return MetadataJson::Write(meta);
}

// Metadata written before the side lists were JSON arrays fails to parse; refresh then group-recomputes.
bool RefreshMetadata::GetLeftJoinSecondaryMeta(const string &view_name, LeftJoinSecondaryMeta &out) {
	return MetadataJson::Read(ReadViewString(view_name, "leftjoin_secondary_meta_json"), out);
}

string RefreshMetadata::LeftJoinSecondaryMetaToJson(const LeftJoinSecondaryMeta &meta) {
	return MetadataJson::Write(meta);
}

vector<string> RefreshMetadata::ExpectedDistinctAuxColumns(const DistinctAuxMeta &meta) {
	auto expected = meta.cols;
	expected.push_back("_count");
	return expected;
}

vector<string> RefreshMetadata::ExpectedCountDistinctAuxColumns(const CountDistinctAuxMeta &meta) {
	auto expected = meta.group_cols;
	expected.push_back(meta.distinct_col);
	expected.push_back("_count");
	return expected;
}

vector<string> RefreshMetadata::ExpectedFilteredGroupCountAuxColumns(const FilteredGroupCountAuxMeta &meta) {
	return vector<string> {meta.group_col, "openivm_sum"};
}

vector<string> RefreshMetadata::ExpectedSemiAntiAuxColumns(const SemiAntiAuxMeta &meta) {
	auto expected = meta.left_cols;
	expected.push_back("_left_count");
	expected.push_back("_match_count");
	if (meta.null_aware && meta.join_type == SemiAntiJoinType::ANTI) {
		expected.push_back("_right_count");
		expected.push_back("_right_null_count");
	}
	return expected;
}

vector<string> RefreshMetadata::GetAuxiliaryTables(const string &view_name) {
	vector<string> tables;
	DistinctAuxMeta distinct;
	if (GetDistinctAuxMeta(view_name, distinct)) {
		tables.push_back(distinct.aux_table);
	}
	CountDistinctAuxMeta count_distinct;
	if (GetCountDistinctAuxMeta(view_name, count_distinct)) {
		tables.push_back(count_distinct.aux_table);
	}
	FilteredGroupCountAuxMeta filtered;
	if (GetFilteredGroupCountAuxMeta(view_name, filtered)) {
		tables.push_back(filtered.aux_table);
	}
	SemiAntiAuxMeta semi;
	if (GetSemiAntiAuxMeta(view_name, semi)) {
		tables.push_back(semi.aux_table);
	}
	return tables;
}

bool RefreshMetadata::AuxStateNeedsRepair(const string &view_name, const string &catalog_name,
                                          const string &schema_name) {
	DistinctAuxMeta distinct;
	if (GetDistinctAuxMeta(view_name, distinct) &&
	    !TableColumnsMatch(catalog_name, schema_name, distinct.aux_table, ExpectedDistinctAuxColumns(distinct))) {
		return true;
	}
	CountDistinctAuxMeta count_distinct;
	if (GetCountDistinctAuxMeta(view_name, count_distinct) &&
	    !TableColumnsMatch(catalog_name, schema_name, count_distinct.aux_table,
	                       ExpectedCountDistinctAuxColumns(count_distinct))) {
		return true;
	}
	FilteredGroupCountAuxMeta filtered;
	if (GetFilteredGroupCountAuxMeta(view_name, filtered) &&
	    !TableColumnsMatch(catalog_name, schema_name, filtered.aux_table,
	                       ExpectedFilteredGroupCountAuxColumns(filtered))) {
		return true;
	}
	SemiAntiAuxMeta semi;
	if (GetSemiAntiAuxMeta(view_name, semi) &&
	    !TableColumnsMatch(catalog_name, schema_name, semi.aux_table, ExpectedSemiAntiAuxColumns(semi))) {
		return true;
	}
	return false;
}

} // namespace duckdb
