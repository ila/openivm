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

	// Get delta table names associated with a view.
	vector<string> GetDeltaTables(const string &view_name);

	// Get the last_update timestamp for a specific delta table entry.
	string GetLastUpdate(const string &view_name, const string &table_name);

	// Update the last_update timestamp to now() for all delta tables of a view.
	void UpdateTimestamp(const string &view_name);
};

} // namespace duckdb

#endif // OPENIVM_METADATA_HPP
