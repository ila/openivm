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

string CreateMVDependenciesSQL() {
	return "CREATE TABLE IF NOT EXISTS " + string(openivm::MV_DEPS_TABLE) +
	       " (parent_view VARCHAR, child_view VARCHAR, edge_kind VARCHAR DEFAULT 'direct',"
	       " PRIMARY KEY (parent_view, child_view))";
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
	// Matcher metadata columns (signature_hash..nullified_columns_json) stay
	// NULL unless openivm_enable_view_matching=true; populated by Stage I wiring.
	ddl.push_back(
	    "create table if not exists " + string(openivm::VIEWS_TABLE) +
	    " (view_name varchar primary key, view_catalog varchar default null,"
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
	    " pending_after_hook boolean default null)");
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

	// Refresh hooks: extensions can register custom SQL to run on MV refresh
	// mode: 'replace' (instead of ivm), 'before' (before ivm), 'after' (after ivm)
	ddl.push_back("create table if not exists openivm_refresh_hooks"
	              " (view_name varchar primary key, hook_sql varchar not null,"
	              " mode varchar not null default 'after')");

	ddl.push_back("create table if not exists " + string(openivm::DELTA_TABLES_TABLE) +
	              " (view_name varchar, table_name varchar, last_update timestamp,"
	              " catalog_type varchar default 'duckdb', last_snapshot_id bigint default "
	              "null,"
	              " last_refresh_ts timestamp default null,"
	              " pending_row_estimate bigint default null,"
	              " pending_estimate_ts timestamp default null,"
	              " source_catalog varchar default null,"
	              " source_schema varchar default null,"
	              " source_table_id bigint default null,"
	              " primary key(view_name, table_name))");
	// Backfill for existing databases without the columns (added post-release).
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "last_refresh_ts timestamp default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "pending_row_estimate bigint default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "pending_estimate_ts timestamp default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "source_catalog varchar default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "source_schema varchar default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, deltas, "source_table_id bigint default null");
	ddl.push_back("UPDATE " + string(openivm::VIEWS_TABLE) +
	              " SET has_join = true WHERE has_join IS NULL AND (COALESCE(has_left_join, false) OR "
	              "COALESCE(has_full_outer, false) OR view_name IN (SELECT view_name FROM " +
	              string(openivm::DELTA_TABLES_TABLE) +
	              " GROUP BY view_name HAVING COUNT(*) > 1) OR regexp_matches(COALESCE(sql_string, ''), "
	              "'(^|[^A-Za-z0-9_])join([^A-Za-z0-9_]|$)', 'i'))");
	ddl.push_back("UPDATE " + string(openivm::VIEWS_TABLE) + " SET has_join = false WHERE has_join IS NULL");

	// Refresh history: stores execution stats for learned cost model calibration.
	// Stage A.5 adds `strategy` (default 'incremental') for per-strategy regression.
	ddl.push_back("create table if not exists " + string(openivm::HISTORY_TABLE) +
	              " (view_name varchar, refresh_timestamp timestamp default "
	              "current_timestamp,"
	              " method varchar, incremental_compute_est double, "
	              "incremental_upsert_est double,"
	              " recompute_compute_est double, recompute_replace_est double,"
	              " actual_duration_ms bigint,"
	              " strategy varchar default 'incremental',"
	              " plan_features double[], feature_schema integer default 0,"
	              " exploratory boolean default false,"
	              " primary key(view_name, refresh_timestamp))");
	AddColumnIfNotExists(ddl, openivm::HISTORY_TABLE, history, "strategy varchar default 'incremental'");
	AddColumnIfNotExists(ddl, openivm::HISTORY_TABLE, history, "plan_features double[]");
	AddColumnIfNotExists(ddl, openivm::HISTORY_TABLE, history, "feature_schema integer default 0");
	// Intentionally no DEFAULT here, unlike the column in the CREATE TABLE above and the migration in
	// openivm_extension.cpp. Writing `exploratory boolean default false` in this batch makes the
	// surrounding CREATE MATERIALIZED VIEW fail with "Cannot plan statement of type MULTI", while the
	// same column with the same default is fine in the CREATE TABLE, and the same ALTER is fine
	// without the default. DuckDB rewrites `false` to CAST('f' AS BOOLEAN), which introduces a quoted
	// literal this DDL batch evidently does not survive. Rows predating the column therefore read
	// NULL rather than false, which reads as "not known to be exploratory" and is correct: every row
	// written since carries an explicit value.
	AddColumnIfNotExists(ddl, openivm::HISTORY_TABLE, history, "exploratory boolean");
	ddl.push_back("create table if not exists " + string(openivm::PROFILE_TABLE) +
	              " (refresh_id varchar, view_name varchar,"
	              " profile_timestamp timestamp default current_timestamp,"
	              " step_order integer, step_name varchar, duration_ms bigint, "
	              "detail varchar,"
	              " primary key(refresh_id, step_order))");
}

template <class BUILD_DDL>
static void InitializeSharedDDL(ClientContext &context, Connection &con, const string &probe, BUILD_DDL build_ddl) {
	// Explicit transactions retain their atomic, rollbackable initialization.
	if (!context.transaction.IsAutoCommit()) {
		return;
	}
	auto initialized = [&]() {
		return !con.Query(probe)->HasError();
	};
	if (initialized()) {
		return;
	}
	// Only shared-table initialization is serialized, not MV planning or loading.
	MutationLockGuard guard(context);
	if (initialized()) {
		return;
	}
	vector<string> ddl;
	build_ddl(ddl);
	OPENIVM_DEBUG_PRINT("[METADATA] Initializing shared tables (%llu statements)\n", (unsigned long long)ddl.size());
	bool transaction_started = false;
	try {
		con.BeginTransaction();
		transaction_started = true;
		for (const auto &sql : ddl) {
			auto result = con.Query(sql);
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
}

void InitializeMVMetadata(ClientContext &context, Connection &con, const string &catalog, const string &schema) {
	InitializeSharedDDL(context, con,
	                    "SELECT v.view_sql_name, v.pending_after_hook, d.source_table_id, h.mode, "
	                    "r.strategy, p.step_order, dep.edge_kind FROM openivm_views v, openivm_delta_tables d, "
	                    "openivm_refresh_hooks h, openivm_refresh_history r, openivm_refresh_profile p, "
	                    "openivm_mv_dependencies dep LIMIT 0",
	                    [&](vector<string> &ddl) { AppendMetadataSchemaDDL(context, catalog, schema, ddl); });
}

void InitializeSourceDelta(ClientContext &context, Connection &con, const string &delta_table, const string &ddl) {
	InitializeSharedDDL(context, con, "SELECT * FROM " + delta_table + " LIMIT 0",
	                    [&](vector<string> &statements) { statements.push_back(ddl); });
}

void AppendCreateMVSystemTablesDDL(ClientContext &context, const string &catalog, const string &schema,
                                   vector<string> &ddl, const string &view_name, bool is_replace,
                                   const string &view_catalog, const string &view_schema, const string &sql_view_name) {
	AppendMetadataSchemaDDL(context, catalog, schema, ddl);
	if (is_replace) {
		// Legacy keys must still belong to the requested relation.
		ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + string(openivm::VIEWS_TABLE) +
		              " WHERE view_name = '" + SqlUtils::EscapeValue(view_name) +
		              "' AND (lower(COALESCE(view_catalog, current_database())) <> lower('" +
		              SqlUtils::EscapeValue(view_catalog) + "') OR lower(COALESCE(view_schema, 'main')) <> lower('" +
		              SqlUtils::EscapeValue(view_schema) +
		              "'))) THEN error('Cannot replace materialized view: its internal key belongs to another "
		              "catalog or schema. Use the original qualified name.') "
		              "ELSE NULL END AS openivm_name_check");
	}
	if (!is_replace) {
		string escaped_view_name = SqlUtils::EscapeSingleQuotes(view_name);
		string escaped_data_table = SqlUtils::EscapeSingleQuotes(IncrementalTableNames::DataTableName(view_name));
		string escaped_sql_name = SqlUtils::EscapeValue(sql_view_name);
		string location_filter = " AND table_catalog = " + Value(view_catalog).ToSQLString() +
		                         " AND table_schema = " + Value(view_schema).ToSQLString();
		string stale_mv_condition = "view_name = '" + escaped_view_name +
		                            "' AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
		                            "table_name = '" +
		                            escaped_sql_name + "'" + location_filter +
		                            ") AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE "
		                            "table_name = '" +
		                            escaped_data_table + "'" + location_filter + ")";
		// CREATE MV executes as multiple catalog statements. If a process dies or loses
		// a DuckDB file lock after writing metadata but before creating the physical
		// DuckLake/default-catalog objects, a retry should clean that stale row rather
		// than report a misleading duplicate MV.
		auto views = Catalog::GetEntry<TableCatalogEntry>(context, catalog, schema, openivm::VIEWS_TABLE,
		                                                  OnEntryNotFound::RETURN_NULL);
		if (views && HasViewMetadata(context, *views, view_name)) {
			ddl.push_back("DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE " + stale_mv_condition);
		}
		ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + string(openivm::VIEWS_TABLE) +
		              " WHERE view_name = '" + escaped_view_name +
		              "') THEN error('Duplicate key: materialized view \"" + escaped_sql_name +
		              "\" already exists in the requested catalog and schema.') ELSE NULL END AS openivm_name_check");
	}
}

} // namespace duckdb
