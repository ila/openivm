#ifndef OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
#define OPENIVM_PARSER_CREATE_MV_HELPERS_HPP

#include "duckdb.hpp"

namespace duckdb {

string CreateMVDependenciesSQL();
// Every OpenIVM control-metadata table; openivm_metadata_schema selects their schema.
const vector<string> &MetadataTableNames();
// CREATE TABLE IF NOT EXISTS for a control-metadata table. An empty target uses the unqualified table name.
string CreateMetadataTableSQL(const string &table, const string &target = "");
struct CreateMVProfileRecord {
	string step_name;
	int64_t duration_ms;
	string detail;
};

void InitializeMVMetadata(ClientContext &context, Connection &con, const string &catalog, const string &schema,
                          vector<CreateMVProfileRecord> *profile_steps);
// True when the helper committed a new, empty source delta.
bool InitializeSourceDelta(ClientContext &context, Connection &con, const string &delta_table, const string &ddl);
string SqlCsvLiteralOrNull(const vector<string> &values);
void AppendCreateMVSystemTablesDDL(ClientContext &context, const string &catalog, const string &schema,
                                   vector<string> &ddl, const string &view_name, bool is_replace,
                                   const string &view_catalog, const string &view_schema, const string &sql_view_name);
string BuildUpdateViewJsonSQL(const string &column_name, const string &json, const string &view_name);

} // namespace duckdb

#endif // OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
