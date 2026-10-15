#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "core/parser_create_mv_helpers.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_locks.hpp"
#include "duckdb/transaction/transaction_context.hpp"
#include "core/sql_utils.hpp"
#include "rules/column_hider.hpp"

#include <chrono>

namespace duckdb {

string SqlCsvLiteralOrNull(const vector<string> &values) {
	if (values.empty()) {
		return "null";
	}
	string result = "'";
	for (size_t i = 0; i < values.size(); i++) {
		if (i > 0) {
			result += ",";
		}
		result += SqlUtils::EscapeSingleQuotes(values[i]);
	}
	result += "'";
	return result;
}

static void AddColumnIfNotExists(vector<string> &ddl, const string &table_name, optional_ptr<TableCatalogEntry> table,
                                 const string &column_definition) {
	if (table && table->ColumnExists(column_definition.substr(0, column_definition.find(' ')))) {
		return;
	}
	ddl.push_back("alter table " + table_name + " add column if not exists " + column_definition);
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

static const vector<std::pair<string, string>> &MetadataTableDefinitions() {
	// Matcher metadata columns (signature_hash..nullified_columns_json) stay
	// NULL unless openivm_enable_view_matching=true; populated by Stage I wiring.
	// Refresh hooks: extensions can register custom SQL to run on MV refresh
	// mode: 'replace' (instead of ivm), 'before' (before ivm), 'after' (after ivm).
	// Refresh history stores execution stats for learned cost model calibration;
	// Stage A.5 adds `strategy` (default 'incremental') for per-strategy regression.
	static const vector<std::pair<string, string>> definitions = {
	    {openivm::MV_DEPS_TABLE, "(parent_view VARCHAR, child_view VARCHAR, edge_kind VARCHAR DEFAULT 'direct',"
	                             " PRIMARY KEY (parent_view, child_view))"},
	    {openivm::VIEWS_TABLE,
	     "(view_name varchar primary key, view_catalog varchar default null,"
	     " view_schema varchar default null, view_sql_name varchar default null, sql_string varchar, type tinyint,"
	     " has_minmax boolean default false, has_left_join boolean default false,"
	     " has_join boolean default false,"
	     " last_update timestamp, refresh_interval bigint default null,"
	     " refresh_in_progress boolean default false,"
	     " group_columns varchar default null,"
	     " window_order_columns varchar default null,"
	     " aggregate_types varchar default null,"
	     " derived_aggregate_outputs_json varchar default null,"
	     " having_predicate varchar default null,"
	     " group_recompute_affected_mode varchar default null,"
	     " group_recompute_source_occurrences_json varchar default null,"
	     " has_full_outer boolean default false,"
	     " full_outer_join_cols varchar default null,"
	     " signature_hash ubigint default null,"
	     " canonical_plan_blob blob default null,"
	     " output_columns_json varchar default null,"
	     " predicate_summary_json varchar default null,"
	     " fd_summary_json varchar default null,"
	     " source_tables_json varchar default null,"
	     " aggregate_decomposition_json varchar default null,"
	     " nullified_columns_json varchar default null,"
	     " distinct_aux_meta_json varchar default null,"
	     " count_distinct_aux_meta_json varchar default null,"
	     " semi_anti_aux_meta_json varchar default null,"
	     " lineage_json varchar default null,"
	     " leftjoin_secondary_meta_json varchar default null,"
	     " published_query varchar default null,"
	     " pending_after_hook boolean default null,"
	     " internal_schema varchar default null)"},
	    {openivm::REFRESH_HOOKS_TABLE, "(view_name varchar primary key, hook_sql varchar not null,"
	                                   " mode varchar not null default 'after')"},
	    {openivm::DELTA_TABLES_TABLE, "(view_name varchar, table_name varchar, last_update timestamp,"
	                                  " catalog_type varchar default 'duckdb', last_snapshot_id bigint default "
	                                  "null,"
	                                  " last_refresh_ts timestamp default null,"
	                                  " pending_row_estimate bigint default null,"
	                                  " pending_estimate_ts timestamp default null,"
	                                  " source_catalog varchar default null,"
	                                  " source_schema varchar default null,"
	                                  " source_table_id bigint default null,"
	                                  " delta_schema varchar default null,"
	                                  " delta_table_name varchar default null,"
	                                  " primary key(view_name, table_name))"},
	    {openivm::HISTORY_TABLE, "(view_name varchar, refresh_timestamp timestamp default "
	                             "current_timestamp,"
	                             " method varchar, incremental_compute_est double, "
	                             "incremental_upsert_est double,"
	                             " recompute_compute_est double, recompute_replace_est double,"
	                             " actual_duration_ms bigint,"
	                             " strategy varchar default 'incremental',"
	                             " primary key(view_name, refresh_timestamp))"},
	    {openivm::PROFILE_TABLE, "(refresh_id varchar, view_name varchar,"
	                             " profile_timestamp timestamp default current_timestamp,"
	                             " step_order integer, step_name varchar, duration_ms bigint, "
	                             "detail varchar,"
	                             " primary key(refresh_id, step_order))"},
	    {openivm::MATCH_LOG_TABLE, "(query_hash UBIGINT,"
	                               " log_timestamp TIMESTAMP,"
	                               " matched_view VARCHAR,"
	                               " chosen_strategy VARCHAR,"
	                               " bypass_cost_est DOUBLE,"
	                               " chosen_cost_est DOUBLE,"
	                               " actual_duration_ms BIGINT,"
	                               " PRIMARY KEY (query_hash, log_timestamp))"},
	    {openivm::CONSTRAINTS_CACHE_TABLE, "(table_name VARCHAR, constraint_kind VARCHAR,"
	                                       " columns_json VARCHAR, referenced_table VARCHAR,"
	                                       " referenced_columns_json VARCHAR,"
	                                       " is_trusted BOOLEAN DEFAULT true,"
	                                       " PRIMARY KEY (table_name, constraint_kind, columns_json))"},
	};
	return definitions;
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

string CreateMetadataTableSQL(const string &table, const string &target) {
	for (auto &definition : MetadataTableDefinitions()) {
		if (definition.first == table) {
			return "create table if not exists " + (target.empty() ? table : target) + " " + definition.second;
		}
	}
	throw InternalException("Unknown OpenIVM metadata table '%s'", table);
}

string CreateMVDependenciesSQL() {
	return CreateMetadataTableSQL(openivm::MV_DEPS_TABLE);
}

static void AppendMetadataSchemaDDL(ClientContext &context, const string &catalog, const string &schema,
                                    vector<string> &ddl) {
	// Inspect this transaction's catalog, not a process-wide initialization flag: older
	// databases and other attached catalogs must still receive missing columns.
	auto views = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, openivm::VIEWS_TABLE,
	                                                  OnEntryNotFound::RETURN_NULL);
	auto deltas = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, openivm::DELTA_TABLES_TABLE,
	                                                   OnEntryNotFound::RETURN_NULL);
	auto history = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, openivm::HISTORY_TABLE,
	                                                    OnEntryNotFound::RETURN_NULL);
	OPENIVM_DEBUG_PRINT("[CREATE] Checking metadata schema in %s.%s\n", catalog.c_str(), schema.c_str());

	ddl.push_back(CreateMVDependenciesSQL());
	ddl.push_back(CreateMetadataTableSQL(openivm::VIEWS_TABLE));
	// Forward-compat ALTER for existing DBs that pre-date `distinct_aux_meta_json`
	// (the CREATE IF NOT EXISTS above is a no-op when the table exists with the older schema).
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "distinct_aux_meta_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "count_distinct_aux_meta_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "semi_anti_aux_meta_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "lineage_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "leftjoin_secondary_meta_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "has_join boolean default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "group_recompute_affected_mode varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views,
	                     "group_recompute_source_occurrences_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "derived_aggregate_outputs_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "published_query varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "view_catalog varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "view_sql_name varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "view_schema varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "window_order_columns varchar default null");

	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "pending_after_hook boolean default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, views, "internal_schema varchar default null");

	ddl.push_back(CreateMetadataTableSQL(openivm::REFRESH_HOOKS_TABLE));
	ddl.push_back(CreateMetadataTableSQL(openivm::DELTA_TABLES_TABLE));
	// Backfill for existing databases without the columns (added post-release).
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "last_refresh_ts timestamp default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "pending_row_estimate bigint default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "pending_estimate_ts timestamp default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "source_catalog varchar default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "source_schema varchar default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "source_table_id bigint default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "delta_schema varchar default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "delta_table_name varchar default null");
	ddl.push_back("UPDATE " + string(openivm::VIEWS_TABLE) +
	              " SET has_join = true WHERE has_join IS NULL AND (COALESCE(has_left_join, false) OR "
	              "COALESCE(has_full_outer, false) OR view_name IN (SELECT view_name FROM " +
	              string(openivm::DELTA_TABLES_TABLE) +
	              " GROUP BY view_name HAVING COUNT(*) > 1) OR regexp_matches(COALESCE(sql_string, ''), "
	              "'(^|[^A-Za-z0-9_])join([^A-Za-z0-9_]|$)', 'i'))");
	ddl.push_back("UPDATE " + string(openivm::VIEWS_TABLE) + " SET has_join = false WHERE has_join IS NULL");

	ddl.push_back(CreateMetadataTableSQL(openivm::HISTORY_TABLE));
	AddColumnIfNotExists(ddl, openivm::HISTORY_TABLE, history, "strategy varchar default 'incremental'");
	ddl.push_back(CreateMetadataTableSQL(openivm::PROFILE_TABLE));
	// View-matching tables share the configured metadata schema.
	ddl.push_back(CreateMetadataTableSQL(openivm::MATCH_LOG_TABLE));
	ddl.push_back(CreateMetadataTableSQL(openivm::CONSTRAINTS_CACHE_TABLE));
}

template <class BUILD_DDL>
static bool InitializeSharedDDL(ClientContext &context, Connection &con, const string &probe, BUILD_DDL build_ddl,
                                vector<CreateMVProfileRecord> *profile_steps = nullptr) {
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
                          vector<CreateMVProfileRecord> *profile_steps) {
	// Probe the selected schema itself: unqualified names could fall back to legacy tables in main.
	auto table = [&](const char *name) {
		return SqlUtils::FullName(catalog, schema, name);
	};
	InitializeSharedDDL(
	    context, con,
	    "SELECT v.view_sql_name, v.pending_after_hook, v.internal_schema, d.source_table_id, "
	    "d.delta_schema, d.delta_table_name, h.mode, "
	    "r.strategy, p.step_order, dep.edge_kind, m.query_hash, c.is_trusted FROM " +
	        table(openivm::VIEWS_TABLE) + " v, " + table(openivm::DELTA_TABLES_TABLE) + " d, " +
	        table(openivm::REFRESH_HOOKS_TABLE) + " h, " + table(openivm::HISTORY_TABLE) + " r, " +
	        table(openivm::PROFILE_TABLE) + " p, " + table(openivm::MV_DEPS_TABLE) + " dep, " +
	        table(openivm::MATCH_LOG_TABLE) + " m, " + table(openivm::CONSTRAINTS_CACHE_TABLE) + " c LIMIT 0",
	    // Inspect the transaction that executes this DDL. Inspecting the caller
	    // here would open its catalog snapshot before the helper commits these
	    // tables, making the subsequent caller lifecycle conflict with them.
	    [&](vector<string> &ddl) { AppendMetadataSchemaDDL(*con.context, catalog, schema, ddl); }, profile_steps);
}

bool InitializeSourceDelta(ClientContext &context, Connection &con, const string &delta_table, const string &ddl) {
	return InitializeSharedDDL(context, con, "SELECT * FROM " + delta_table + " LIMIT 0",
	                           [&](vector<string> &statements) { statements.push_back(ddl); });
}

void AppendCreateMVSystemTablesDDL(ClientContext &context, const string &catalog, const string &schema,
                                   vector<string> &ddl, const string &view_name, bool is_replace,
                                   const string &view_catalog, const string &view_schema, const string &sql_view_name) {
	AppendMetadataSchemaDDL(context, catalog, schema, ddl);
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
		auto views = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, openivm::VIEWS_TABLE,
		                                                  OnEntryNotFound::RETURN_NULL);
		if (views && HasViewMetadata(context, *views, view_name)) {
			ddl.push_back("DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE " + stale_mv_condition);
		}
		ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + views_table + " WHERE view_name = '" +
		              escaped_view_name + "') THEN error('Duplicate key: materialized view \"" + escaped_sql_name +
		              "\" already exists in the requested catalog and schema.') ELSE NULL END AS openivm_name_check");
	}
}

} // namespace duckdb
