#ifndef OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
#define OPENIVM_PARSER_CREATE_MV_HELPERS_HPP

#include "duckdb.hpp"

namespace duckdb {

void InitializeMVMetadata(ClientContext &context, Connection &con);
void InitializeSourceDelta(ClientContext &context, Connection &con, const string &delta_table, const string &ddl);
string SqlCsvLiteralOrNull(const vector<string> &values);
void AppendCreateMVSystemTablesDDL(vector<string> &ddl, const string &view_name, bool is_replace,
                                   const string &view_catalog, const string &view_schema, const string &sql_view_name);
string BuildUpdateViewJsonSQL(const string &column_name, const string &json, const string &view_name);

} // namespace duckdb

#endif // OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
