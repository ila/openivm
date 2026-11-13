#ifndef OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
#define OPENIVM_PARSER_CREATE_MV_HELPERS_HPP

#include "duckdb.hpp"
#include "core/mv_profiler.hpp"

namespace duckdb {

class TableCatalogEntry;

string CreateMVDependenciesSQL();
// Every OpenIVM control-metadata table; openivm_metadata_schema selects their schema.
const vector<string> &MetadataTableNames();
// CREATE TABLE IF NOT EXISTS for a control-metadata table. An empty target uses the unqualified table name.
string CreateMetadataTableSQL(const string &table, const string &target = "", bool remote = false);
// ALTERs adding the columns that a metadata table created by an older release lacks. Without the
// existing table, every later column is added with ADD COLUMN IF NOT EXISTS.
void AppendMetadataMigrationSQL(vector<string> &ddl, const string &table,
                                optional_ptr<TableCatalogEntry> existing = nullptr, bool remote = false);
// Classify has_join for views migrated before the column existed (it references openivm_delta_tables).
vector<string> HasJoinBackfillSQL();
void InitializeMVMetadata(ClientContext &context, Connection &con, const string &catalog, const string &schema,
                          bool remote = false, vector<MVProfileStep> *profile_steps = nullptr);
// True when the helper committed a new, empty source delta.
bool InitializeSourceDelta(ClientContext &context, Connection &con, const string &delta_table, const string &ddl,
                           const string &mapping_probe);
string SqlCsvLiteralOrNull(const vector<string> &values);
void AppendCreateMVSystemTablesDDL(ClientContext &context, const string &catalog, const string &schema,
                                   vector<string> &ddl, const string &view_name, bool is_replace,
                                   const string &view_catalog, const string &view_schema, const string &sql_view_name,
                                   bool remote = false);
string BuildUpdateViewJsonSQL(const string &column_name, const string &json, const string &view_name);

} // namespace duckdb

#endif // OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
