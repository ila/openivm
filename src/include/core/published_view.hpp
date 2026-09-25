#pragma once

#include "duckdb.hpp"
#include "sql_dialect.hpp"

namespace duckdb {

// The public relation is a stable scan boundary. Its schema contains only user
// columns, independent of the maintenance plan's helper columns and filters.
string PublishedViewName(const string &view_name);
string PublishedSourceViewName(string source_name);
string BuildPublishViewSQL(const string &view_name, const string &prefix, const string &query,
                           const vector<string> &columns, bool ducklake, const string &metadata_table,
                           const vector<string> &scope_columns = {}, const string &timestamp_sql = "",
                           SqlDialect dialect = SqlDialect::DUCKDB);

} // namespace duckdb
