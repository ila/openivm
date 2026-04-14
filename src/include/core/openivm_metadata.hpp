#ifndef OPENIVM_METADATA_HPP
#define OPENIVM_METADATA_HPP

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "core/openivm_constants.hpp"

namespace duckdb {

// Centralized access to IVM metadata stored in system tables.
// Wraps all raw SQL queries to _duckdb_ivm_views and _duckdb_ivm_delta_tables
// behind typed methods. Takes a Connection reference — does NOT create its own.
class IVMMetadata {
	Connection &con;

public:
	explicit IVMMetadata(Connection &con) : con(con) {
	}

	// Returns true if the given table name is NOT a tracked materialized view.
	// (i.e., it's a base table that should have its deltas captured)
	bool IsBaseTable(const string &table_name);

	// Get the stored SQL query for a materialized view.
	// Returns empty string if not found.
	string GetViewQuery(const string &view_name);

	// Get the IVM type for a materialized view.
	IVMType GetViewType(const string &view_name);

	// Check if the view uses MIN/MAX aggregates (requires group-recompute).
	bool HasMinMax(const string &view_name);

	// Check if the view involves a LEFT/RIGHT JOIN.
	bool HasLeftJoin(const string &view_name);

	// Get delta table names associated with a view.
	vector<string> GetDeltaTables(const string &view_name);

	// Get the last_update timestamp for a specific delta table entry.
	string GetLastUpdate(const string &view_name, const string &table_name);

	// Update the last_update timestamp to now() for all delta tables of a view.
	void UpdateTimestamp(const string &view_name);

	// Get all upstream MV dependencies in topological order (ancestors first).
	// For table→mv1→mv2→mv3, GetUpstreamViews("mv3") returns ["mv1", "mv2"].
	vector<string> GetUpstreamViews(const string &view_name);

	// Get all downstream MV dependents in topological order (closest first).
	// For table→mv1→mv2→mv3, GetDownstreamViews("mv1") returns ["mv2", "mv3"].
	vector<string> GetDownstreamViews(const string &view_name);

	// Get refresh_interval in seconds for a view. Returns -1 if not set (manual only).
	int64_t GetRefreshInterval(const string &view_name);

	// Get all views with a non-null refresh_interval.
	// Returns tuples of (view_name, interval_seconds, last_update_timestamp_string).
	struct ScheduledView {
		string view_name;
		int64_t interval_seconds;
		string last_update;
	};
	vector<ScheduledView> GetScheduledViews();

	// Set/clear the refresh_in_progress flag for crash safety.
	void SetRefreshInProgress(const string &view_name, bool in_progress);

	// Build SQL to delete old delta rows that all dependent views have already consumed.
	// target: the (possibly schema-qualified) table to delete from.
	// metadata_key: the name used in _duckdb_ivm_delta_tables (unqualified delta name).
	static string BuildDeltaCleanupSQL(const string &target, const string &metadata_key);

	// Get GROUP BY column names for a view. Returns empty vector if not stored.
	vector<string> GetGroupColumns(const string &view_name);

	// Get per-column aggregate function types (min, max, sum, count_star, etc.).
	vector<string> GetAggregateTypes(const string &view_name);

	// --- DuckLake support ---

	// Get the catalog type for a base table entry ('duckdb' or 'ducklake').
	string GetCatalogType(const string &view_name, const string &table_name);

	// Check if a base table entry is backed by DuckLake.
	bool IsDuckLakeTable(const string &view_name, const string &table_name);

	// Get the DuckLake snapshot ID at last refresh. Returns -1 if not set.
	int64_t GetLastSnapshotId(const string &view_name, const string &table_name);

	// Update the DuckLake snapshot ID after refresh.
	void UpdateSnapshotId(const string &view_name, const string &table_name, int64_t snapshot_id);
};

} // namespace duckdb

#endif // OPENIVM_METADATA_HPP
