#include "core/parser_create_mv_helpers.hpp"

#include "core/openivm_constants.hpp"
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

static void AddColumnIfNotExists(vector<string> &ddl, const string &table_name, const string &column_definition) {
	ddl.push_back("alter table " + table_name + " add column if not exists " + column_definition);
}

string BuildUpdateViewJsonSQL(const string &column_name, const string &json, const string &view_name) {
	return "UPDATE " + string(openivm::VIEWS_TABLE) + " SET " + column_name + " = '" +
	       SqlUtils::EscapeSingleQuotes(json) + "' WHERE view_name = '" + SqlUtils::EscapeSingleQuotes(view_name) + "'";
}

void AppendCreateMVSystemTablesDDL(vector<string> &ddl, const string &view_name, bool is_replace) {
	// Matcher metadata columns (signature_hash..nullified_columns_json) stay
	// NULL unless openivm_enable_view_matching=true; populated by Stage I wiring.
	ddl.push_back("create table if not exists " + string(openivm::VIEWS_TABLE) +
	              " (view_name varchar primary key, sql_string varchar, type tinyint,"
	              " has_minmax boolean default false, has_left_join boolean default false,"
	              " last_update timestamp, refresh_interval bigint default null,"
	              " refresh_in_progress boolean default false,"
	              " group_columns varchar default null,"
	              " aggregate_types varchar default null,"
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
	              " semi_anti_aux_meta_json varchar default null,"
	              " lineage_json varchar default null)");
	// Forward-compat ALTER for existing DBs that pre-date `distinct_aux_meta_json`
	// (the CREATE IF NOT EXISTS above is a no-op when the table exists with the older schema).
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, "distinct_aux_meta_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, "semi_anti_aux_meta_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, "lineage_json varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, "group_recompute_affected_mode varchar default null");
	AddColumnIfNotExists(ddl, openivm::VIEWS_TABLE, "group_recompute_source_occurrences_json varchar default null");
	if (!is_replace) {
		string escaped_view_name = SqlUtils::EscapeSingleQuotes(view_name);
		string escaped_data_table = SqlUtils::EscapeSingleQuotes(IncrementalTableNames::DataTableName(view_name));
		string stale_mv_condition = "view_name = '" + escaped_view_name +
		                            "' AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" +
		                            escaped_view_name +
		                            "') AND NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '" +
		                            escaped_data_table + "')";
		// CREATE MV executes as multiple catalog statements. If a process dies or loses
		// a DuckDB file lock after writing metadata but before creating the physical
		// DuckLake/default-catalog objects, a retry should clean that stale row rather
		// than report a misleading duplicate MV.
		ddl.push_back("DELETE FROM " + string(openivm::VIEWS_TABLE) + " WHERE " + stale_mv_condition);
		ddl.push_back("SELECT CASE WHEN EXISTS (SELECT 1 FROM " + string(openivm::VIEWS_TABLE) +
		              " WHERE view_name = '" + escaped_view_name +
		              "') THEN error('Duplicate key: materialized view \"" + escaped_view_name +
		              "\" already exists') ELSE NULL END");
	}

	// Refresh hooks: extensions can register custom SQL to run on MV refresh
	// mode: 'replace' (instead of ivm), 'before' (before ivm), 'after' (after ivm)
	ddl.push_back("create table if not exists openivm_refresh_hooks"
	              " (view_name varchar primary key, hook_sql varchar not null,"
	              " mode varchar not null default 'after')");

	ddl.push_back("create table if not exists " + string(openivm::DELTA_TABLES_TABLE) +
	              " (view_name varchar, table_name varchar, last_update timestamp,"
	              " catalog_type varchar default 'duckdb', last_snapshot_id bigint default null,"
	              " last_refresh_ts timestamp default null,"
	              " pending_row_estimate bigint default null,"
	              " pending_estimate_ts timestamp default null,"
	              " source_catalog varchar default null,"
	              " source_schema varchar default null,"
	              " source_table_id bigint default null,"
	              " primary key(view_name, table_name))");
	// Backfill for existing databases without the columns (added post-release).
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, "last_refresh_ts timestamp default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, "pending_row_estimate bigint default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, "pending_estimate_ts timestamp default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, "source_catalog varchar default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, "source_schema varchar default null");
	AddColumnIfNotExists(ddl, openivm::DELTA_TABLES_TABLE, "source_table_id bigint default null");

	// Refresh history: stores execution stats for learned cost model calibration.
	// Stage A.5 adds `strategy` (default 'incremental') for per-strategy regression.
	ddl.push_back("create table if not exists " + string(openivm::HISTORY_TABLE) +
	              " (view_name varchar, refresh_timestamp timestamp default current_timestamp,"
	              " method varchar, incremental_compute_est double, incremental_upsert_est double,"
	              " recompute_compute_est double, recompute_replace_est double,"
	              " actual_duration_ms bigint,"
	              " strategy varchar default 'incremental',"
	              " primary key(view_name, refresh_timestamp))");
	AddColumnIfNotExists(ddl, openivm::HISTORY_TABLE, "strategy varchar default 'incremental'");
	ddl.push_back("create table if not exists " + string(openivm::PROFILE_TABLE) +
	              " (refresh_id varchar, view_name varchar,"
	              " profile_timestamp timestamp default current_timestamp,"
	              " step_order integer, step_name varchar, duration_ms bigint, detail varchar,"
	              " primary key(refresh_id, step_order))");
}

} // namespace duckdb
