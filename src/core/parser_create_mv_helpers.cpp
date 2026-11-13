#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "core/parser_create_mv_helpers.hpp"

#include "core/compiled_sql_archive.hpp"
#include "core/metadata_location.hpp"
#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_locks.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

string SqlCsvLiteralOrNull(const vector<string> &values) {
	if (values.empty()) {
		return "null";
	}
	return "'" + StringUtil::Join(values, values.size(), ",", SqlUtils::EscapeSingleQuotes) + "'";
}

// Read the caller's snapshot, including metadata inserted earlier in this
// transaction. A helper SQL connection would miss uncommitted stale rows.
static bool HasViewMetadata(ClientContext &context, TableCatalogEntry &table, const string &view_name) {
	auto &transaction = DuckTransaction::Get(context, table.catalog);
	auto &storage = table.GetStorage();
	vector<StorageIndex> columns;
	columns.emplace_back(table.GetColumn("view_name").StorageOid());
	TableScanState scan;
	storage.InitializeScan(context, transaction, scan, columns);
	DataChunk rows;
	rows.Initialize(Allocator::Get(context), {LogicalType::VARCHAR});
	while (true) {
		rows.Reset();
		storage.Scan(transaction, rows, scan);
		if (rows.size() == 0) {
			return false;
		}
		for (idx_t row = 0; row < rows.size(); row++) {
			if (rows.GetValue(0, row).ToString() == view_name) {
				return true;
			}
		}
	}
}

string BuildUpdateViewJsonSQL(const string &column_name, const string &json, const string &view_name) {
	return "UPDATE " + string(openivm::VIEWS_TABLE) + " SET " + column_name + " = '" +
	       SqlUtils::EscapeSingleQuotes(json) + "' WHERE view_name = '" + SqlUtils::EscapeSingleQuotes(view_name) + "'";
}

namespace {

enum class MetadataStore : uint8_t { ANY, LOCAL, REMOTE };
constexpr bool ADDED = true;

// Column definitions of a control-metadata table, in CREATE TABLE order (an entry may hold several
// columns or a table constraint). An ADDED column postdates the table's first release: existing
// tables receive it through `migration` (default: its definition). LOCAL and REMOTE columns exist
// only in native or remote metadata catalogs respectively.
struct MetadataColumn {
	// Implicit string conversion keeps unchanged column definitions readable in the table-definition list.
	// NOLINTNEXTLINE(google-explicit-constructor)
	MetadataColumn(const char *definition, bool added = false, MetadataStore store = MetadataStore::ANY,
	               const char *migration = nullptr)
	    : definition(definition), added(added), store(store), migration(migration ? migration : definition) {
	}
	bool In(bool remote) const {
		return store == MetadataStore::ANY || (store == MetadataStore::REMOTE) == remote;
	}
	const char *definition;
	bool added;
	MetadataStore store;
	const char *migration;
};

} // namespace

static const vector<std::pair<string, vector<MetadataColumn>>> &MetadataTableDefinitions() {
	// Matcher metadata columns (signature_hash..nullified_columns_json) stay NULL unless
	// openivm_enable_view_matching=true. Remote catalogs instead track a per-view refresh_epoch:
	// an epoch bump marks a view whose lease was overrun, so the next refresh recomputes it.
	// Refresh hooks run custom SQL on MV refresh; mode: 'replace' (instead of ivm), 'before', 'after'.
	// Refresh history stores execution stats for learned cost model calibration, per strategy.
	static const vector<std::pair<string, vector<MetadataColumn>>> definitions = {
	    {openivm::MV_DEPS_TABLE,
	     {"parent_view VARCHAR, child_view VARCHAR, edge_kind VARCHAR DEFAULT 'direct'",
	      "PRIMARY KEY (parent_view, child_view)"}},
	    {openivm::VIEWS_TABLE,
	     {"view_name varchar primary key",
	      {"view_catalog varchar default null", ADDED},
	      {"view_schema varchar default null", ADDED},
	      {"view_sql_name varchar default null", ADDED},
	      "sql_string varchar",
	      {"type tinyint", false, MetadataStore::LOCAL},
	      {"type smallint", false, MetadataStore::REMOTE},
	      "has_minmax boolean default false, has_left_join boolean default false",
	      // NULL for existing rows until the backfill in HasJoinBackfillSQL classifies them.
	      {"has_join boolean default false", ADDED, MetadataStore::ANY, "has_join boolean default null"},
	      "last_update timestamp",
	      {"refresh_interval bigint default null", ADDED},
	      {"refresh_in_progress boolean default false", ADDED},
	      "group_columns varchar default null",
	      {"window_order_columns varchar default null", ADDED},
	      "aggregate_types varchar default null",
	      {"derived_aggregate_outputs_json varchar default null", ADDED},
	      "having_predicate varchar default null",
	      {"group_recompute_affected_mode varchar default null", ADDED},
	      {"group_recompute_affected_query varchar default null", ADDED},
	      {"group_recompute_source_occurrences_json varchar default null", ADDED},
	      "has_full_outer boolean default false, full_outer_join_cols varchar default null",
	      {"signature_hash ubigint default null", ADDED, MetadataStore::LOCAL},
	      {"canonical_plan_blob blob default null", ADDED, MetadataStore::LOCAL},
	      {"refresh_epoch bigint default 0", ADDED, MetadataStore::REMOTE},
	      {"output_columns_json varchar default null", ADDED},
	      {"predicate_summary_json varchar default null", ADDED},
	      {"fd_summary_json varchar default null", ADDED},
	      {"source_tables_json varchar default null", ADDED},
	      {"aggregate_decomposition_json varchar default null", ADDED},
	      {"nullified_columns_json varchar default null", ADDED},
	      {"distinct_aux_meta_json varchar default null", ADDED},
	      {"count_distinct_aux_meta_json varchar default null", ADDED},
	      {"semi_anti_aux_meta_json varchar default null", ADDED},
	      {"lineage_json varchar default null", ADDED},
	      {"leftjoin_secondary_meta_json varchar default null", ADDED},
	      {"published_query varchar default null", ADDED},
	      {"pending_after_hook boolean default null", ADDED},
	      {"internal_schema varchar default null", ADDED}}},
	    {openivm::REFRESH_HOOKS_TABLE,
	     {"view_name varchar primary key, hook_sql varchar not null, mode varchar not null default 'after'"}},
	    {openivm::DELTA_TABLES_TABLE,
	     {"view_name varchar, table_name varchar, last_update timestamp, catalog_type varchar default 'duckdb'",
	      "last_snapshot_id bigint default null",
	      {"last_refresh_ts timestamp default null", ADDED},
	      {"pending_row_estimate bigint default null", ADDED},
	      {"pending_estimate_ts timestamp default null", ADDED},
	      {"source_catalog varchar default null", ADDED},
	      {"source_schema varchar default null", ADDED},
	      {"source_table_id bigint default null", ADDED},
	      {"delta_schema varchar default null", ADDED},
	      {"delta_table_name varchar default null", ADDED},
	      "primary key(view_name, table_name)"}},
	    {openivm::HISTORY_TABLE,
	     {"view_name varchar, refresh_timestamp timestamp default current_timestamp, method varchar, "
	      "incremental_compute_est double, incremental_upsert_est double, recompute_compute_est double, "
	      "recompute_replace_est double, actual_duration_ms bigint",
	      {"strategy varchar default 'incremental'", ADDED},
	      "primary key(view_name, refresh_timestamp)"}},
	    {openivm::PROFILE_TABLE,
	     {"refresh_id varchar, view_name varchar, profile_timestamp timestamp default current_timestamp, "
	      "step_order integer, step_name varchar, duration_ms bigint, detail varchar",
	      "primary key(refresh_id, step_order)"}},
	    {openivm::MATCH_LOG_TABLE,
	     {"query_hash UBIGINT, log_timestamp TIMESTAMP, matched_view VARCHAR, chosen_strategy VARCHAR, "
	      "bypass_cost_est DOUBLE, chosen_cost_est DOUBLE, actual_duration_ms BIGINT",
	      "PRIMARY KEY (query_hash, log_timestamp)"}},
	    {openivm::COMPILED_PROGRAMS_TABLE,
	     {"view_name varchar, view_catalog varchar, view_schema varchar, view_sql_name varchar, operation varchar, "
	      "version integer, statement_count integer, compilation_id varchar, compiled_at timestamp, "
	      "last_compilation_id varchar, last_outcome varchar, last_outcome_at timestamp, "
	      "committed_count bigint default 0",
	      "primary key (view_name, operation, version)"}},
	    {openivm::COMPILED_STATEMENTS_TABLE,
	     {"view_name varchar, operation varchar, version integer, stmt_order integer, sql varchar",
	      "primary key (view_name, operation, version, stmt_order)"}},
	    {openivm::REFRESH_LEASE_TABLE,
	     {"view_name varchar primary key, owner varchar not null, lease_until timestamp not null"}},
	    {openivm::CONSTRAINTS_CACHE_TABLE,
	     {"table_name VARCHAR, constraint_kind VARCHAR, columns_json VARCHAR, referenced_table VARCHAR, "
	      "referenced_columns_json VARCHAR, is_trusted BOOLEAN DEFAULT true, child_generation VARCHAR, "
	      "parent_generation VARCHAR",
	      "PRIMARY KEY (table_name, constraint_kind, columns_json)"}},
	};
	return definitions;
}

static const vector<MetadataColumn> &MetadataColumns(const string &table) {
	for (auto &definition : MetadataTableDefinitions()) {
		if (definition.first == table) {
			return definition.second;
		}
	}
	throw InternalException("Unknown OpenIVM metadata table '%s'", table);
}

const vector<string> &MetadataTableNames() {
	static const vector<string> names = []() {
		vector<string> result;
		for (auto &definition : MetadataTableDefinitions()) {
			result.push_back(definition.first);
		}
		return result;
	}();
	return names;
}

string CreateMetadataTableSQL(const string &table, const string &target, bool remote) {
	vector<string> columns;
	for (auto &column : MetadataColumns(table)) {
		if (column.In(remote)) {
			columns.push_back(column.definition);
		}
	}
	return "create table if not exists " + (target.empty() ? table : target) + " (" + StringUtil::Join(columns, ", ") +
	       ")";
}

string CreateMVDependenciesSQL() {
	return CreateMetadataTableSQL(openivm::MV_DEPS_TABLE);
}

void AppendMetadataMigrationSQL(vector<string> &ddl, const string &table, optional_ptr<TableCatalogEntry> existing,
                                bool remote) {
	for (auto &column : MetadataColumns(table)) {
		string definition = column.migration;
		if (column.added && column.In(remote) &&
		    !(existing && existing->ColumnExists(Identifier(definition.substr(0, definition.find(' ')))))) {
			ddl.push_back("alter table " + table + " add column if not exists " + definition);
		}
	}
}

vector<string> HasJoinBackfillSQL() {
	return {"UPDATE " + string(openivm::VIEWS_TABLE) +
	            " SET has_join = true WHERE has_join IS NULL AND (COALESCE(has_left_join, false) OR "
	            "COALESCE(has_full_outer, false) OR view_name IN (SELECT view_name FROM " +
	            string(openivm::DELTA_TABLES_TABLE) +
	            " GROUP BY view_name HAVING COUNT(*) > 1) OR regexp_matches(COALESCE(sql_string, ''), "
	            "'(^|[^A-Za-z0-9_])join([^A-Za-z0-9_]|$)', 'i'))",
	        "UPDATE " + string(openivm::VIEWS_TABLE) + " SET has_join = false WHERE has_join IS NULL"};
}

static void AppendMetadataSchemaDDL(ClientContext &context, const string &catalog, const string &schema,
                                    vector<string> &ddl, bool remote) {
	OPENIVM_DEBUG_PRINT("[CREATE] Checking metadata schema in %s.%s\n", catalog.c_str(), schema.c_str());
	vector<string> tables = {openivm::MV_DEPS_TABLE,      openivm::VIEWS_TABLE,   openivm::REFRESH_HOOKS_TABLE,
	                         openivm::DELTA_TABLES_TABLE, openivm::HISTORY_TABLE, openivm::PROFILE_TABLE};
	if (remote) {
		// Clients sharing remote metadata serialize refreshes per view through a lease.
		tables.push_back(openivm::REFRESH_LEASE_TABLE);
	} else {
		tables.push_back(openivm::MATCH_LOG_TABLE);
		tables.push_back(openivm::CONSTRAINTS_CACHE_TABLE);
	}
	for (auto &table : tables) {
		ddl.push_back(CreateMetadataTableSQL(table, "", remote));
		auto &columns = MetadataColumns(table);
		if (std::none_of(columns.begin(), columns.end(), [](const MetadataColumn &column) { return column.added; })) {
			continue;
		}
		// Inspect this transaction's catalog, not a process-wide initialization flag: older
		// databases and other attached catalogs must still receive missing columns. A missing
		// table is created above with every column.
		auto existing = Catalog::GetEntry<TableCatalogEntry>(
		    context, QualifiedName(Identifier(catalog), Identifier(schema), Identifier(table)),
		    OnEntryNotFound::RETURN_NULL);
		if (existing) {
			AppendMetadataMigrationSQL(ddl, table, existing, remote);
		}
	}
	for (auto &statement : HasJoinBackfillSQL()) {
		ddl.push_back(statement);
	}
	for (auto &statement : CompiledSQLArchiveSchemaDDL()) {
		ddl.push_back(statement);
	}
}

template <class BUILD_DDL>
static bool InitializeSharedDDL(ClientContext &context, Connection &con, const string &probe, BUILD_DDL build_ddl,
                                vector<MVProfileStep> *profile_steps = nullptr) {
	// Explicit transactions retain their atomic, rollbackable initialization.
	if (!context.transaction.IsAutoCommit()) {
		return false;
	}
	auto initialized = [&]() {
		return !con.Query(probe)->HasError();
	};
	if (initialized()) {
		return false;
	}
	// Re-entrant when lifecycle planning already owns the publication gate.
	MutationLockGuard guard(context);
	if (initialized()) {
		return false;
	}
	bool transaction_started = false;
	try {
		con.BeginTransaction();
		transaction_started = true;
		vector<string> ddl;
		build_ddl(ddl);
		OPENIVM_DEBUG_PRINT("[METADATA] Initializing shared tables (%llu statements)\n",
		                    (unsigned long long)ddl.size());
		for (idx_t i = 0; i < ddl.size(); i++) {
			const auto &sql = ddl[i];
			auto start = profile_steps ? std::chrono::steady_clock::now() : std::chrono::steady_clock::time_point();
			auto result = con.Query(sql);
			if (profile_steps) {
				auto duration_ms =
				    std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start)
				        .count();
				profile_steps->push_back({"create_mv_sql_stmt", duration_ms,
				                          "phase=create_mv_system_tables; statement=" + to_string(i + 1) + "/" +
				                              to_string(ddl.size()) + "; sql=" + SqlUtils::SQLStatementPreview(sql)});
			}
			if (result->HasError()) {
				throw CatalogException("OpenIVM shared-table initialization failed: %s", result->GetError());
			}
		}
		con.Commit();
	} catch (std::exception &) {
		if (transaction_started) {
			con.Rollback();
		}
		throw;
	}
	OPENIVM_DEBUG_PRINT("[METADATA] Shared tables initialized\n");
	return true;
}

void InitializeMVMetadata(ClientContext &context, Connection &con, const string &catalog, const string &schema,
                          bool remote, vector<MVProfileStep> *profile_steps) {
	auto table = [&](const char *name) {
		return SqlUtils::FullName(catalog, schema, name);
	};
	string probe =
	    "SELECT v.view_sql_name, v.pending_after_hook, v.internal_schema, d.source_table_id, "
	    "d.delta_schema, d.delta_table_name, h.mode, r.strategy, p.step_order, dep.edge_kind, "
	    "cp.last_outcome, cs.sql" +
	    string(remote ? ", v.refresh_epoch, l.owner" : ", m.query_hash, c.is_trusted") + " FROM " +
	    table(openivm::VIEWS_TABLE) + " v, " + table(openivm::DELTA_TABLES_TABLE) + " d, " +
	    table(openivm::REFRESH_HOOKS_TABLE) + " h, " + table(openivm::HISTORY_TABLE) + " r, " +
	    table(openivm::PROFILE_TABLE) + " p, " + table(openivm::MV_DEPS_TABLE) + " dep, " +
	    table(openivm::COMPILED_PROGRAMS_TABLE) + " cp, " + table(openivm::COMPILED_STATEMENTS_TABLE) + " cs, " +
	    (remote ? table(openivm::REFRESH_LEASE_TABLE) + " l"
	            : table(openivm::MATCH_LOG_TABLE) + " m, " + table(openivm::CONSTRAINTS_CACHE_TABLE) + " c") +
	    " LIMIT 0";
	InitializeSharedDDL(
	    context, con, probe,
	    [&](vector<string> &ddl) { AppendMetadataSchemaDDL(*con.context, catalog, schema, ddl, remote); },
	    profile_steps);
}

bool InitializeSourceDelta(ClientContext &context, Connection &con, const string &delta_table, const string &ddl,
                           const string &mapping_probe) {
	if (!context.transaction.IsAutoCommit()) {
		return false;
	}
	auto probe = "SELECT * FROM " + delta_table + " LIMIT 0";
	bool existed = !con.Query(probe)->HasError();
	if (!mapping_probe.empty()) {
		probe += "; " + mapping_probe;
	}
	bool initialized =
	    InitializeSharedDDL(context, con, probe, [&](vector<string> &statements) { statements.push_back(ddl); });
	return initialized && !existed;
}

void AppendCreateMVSystemTablesDDL(ClientContext &context, const string &catalog, const string &schema,
                                   vector<string> &ddl, const string &view_name, bool is_replace,
                                   const string &view_catalog, const string &view_schema, const string &sql_view_name,
                                   bool remote) {
	AppendMetadataSchemaDDL(context, catalog, schema, ddl, remote);
	auto views_table = SqlUtils::FullName(catalog, schema, openivm::VIEWS_TABLE);
	if (is_replace) {
		// Legacy keys must still belong to the requested relation.
		ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + views_table + " WHERE view_name = '" +
		              SqlUtils::EscapeValue(view_name) + "' AND (NOT " +
		              SqlUtils::IdentifierEqualsSQL("COALESCE(view_catalog, " + Value(catalog).ToSQLString() + ")",
		                                            Value(view_catalog).ToSQLString()) +
		              " OR NOT " +
		              SqlUtils::IdentifierEqualsSQL("COALESCE(view_schema, 'main')", Value(view_schema).ToSQLString()) +
		              ")) THEN error('Cannot replace materialized view: its internal key belongs to another "
		              "catalog or schema. Use the original qualified name.') "
		              "ELSE NULL END AS openivm_name_check");
	}
	if (!is_replace) {
		string escaped_view_name = SqlUtils::EscapeSingleQuotes(view_name);
		string escaped_data_table = SqlUtils::EscapeSingleQuotes(IncrementalTableNames::DataTableName(view_name));
		string escaped_sql_name = SqlUtils::EscapeValue(sql_view_name);
		string location_filter = " AND table_catalog = " + Value(view_catalog).ToSQLString() +
		                         " AND table_schema = " + Value(view_schema).ToSQLString();
		string internal_location_filter =
		    " AND table_catalog = COALESCE(view_catalog, " + Value(view_catalog).ToSQLString() +
		    ") AND table_schema = COALESCE(internal_schema, view_schema, " + Value(view_schema).ToSQLString() + ")";
		string stale_mv_condition = "view_name = '" + escaped_view_name +
		                            "' AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
		                            "table_name = '" +
		                            escaped_sql_name + "'" + location_filter +
		                            ") AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
		                            "table_name = '" +
		                            escaped_data_table + "'" + internal_location_filter + ")";
		// CREATE MV executes as multiple catalog statements. If a process dies or loses
		// a DuckDB file lock after writing metadata but before creating the physical
		// DuckLake/default-catalog objects, a retry should clean that stale row rather
		// than report a misleading duplicate MV.
		auto views = Catalog::GetEntry<TableCatalogEntry>(
		    context, QualifiedName(Identifier(catalog), Identifier(schema), Identifier(openivm::VIEWS_TABLE)),
		    OnEntryNotFound::RETURN_NULL);
		// Remote catalogs have no DuckDB storage to scan; the guarded DELETE is harmless.
		if (views && (remote || HasViewMetadata(context, *views, view_name))) {
			ddl.push_back("DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE " + stale_mv_condition);
		}
		ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + views_table + " WHERE view_name = '" +
		              escaped_view_name + "') THEN error('Duplicate key: materialized view \"" + escaped_sql_name +
		              "\" already exists in the requested catalog and schema.') ELSE NULL END AS openivm_name_check");
	}
}

} // namespace duckdb
