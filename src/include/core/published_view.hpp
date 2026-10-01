#pragma once

#include "duckdb.hpp"
#include "sql_dialect.hpp"

namespace duckdb {

// The public relation is a stable scan boundary. Its schema contains only user
// columns, independent of the maintenance plan's helper columns and filters.
string PublishedViewName(const string &view_name);
string PublishedSourceViewName(string source_name);
void WarnSnapshotHistoryDeletion(ClientContext &context, LogicalOperator &plan);
bool IsSnapshotPublication(ClientContext &context, const string &catalog, const string &schema,
                           const string &view_name);
string BuildSnapshotPublicationSQL(Connection &con, const string &catalog, const string &published,
                                   const string &data_table, const string &query);
string BuildPublishViewSQL(const string &view_name, const string &prefix, const string &query,
                           const vector<string> &columns, bool ducklake, const string &metadata_table,
                           const vector<string> &scope_columns = {}, const string &timestamp_sql = "",
                           SqlDialect dialect = SqlDialect::DUCKDB, const string &appended_rows = "",
                           const string &scope_rows = "", const vector<string> &metadata_catalogs = {});

} // namespace duckdb
