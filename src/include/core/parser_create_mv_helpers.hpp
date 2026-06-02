#ifndef OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
#define OPENIVM_PARSER_CREATE_MV_HELPERS_HPP

#include "duckdb.hpp"

namespace duckdb {

string SqlCsvLiteralOrNull(const vector<string> &values);
void AppendCreateMVSystemTablesDDL(vector<string> &ddl, const string &view_name, bool is_replace);
string BuildUpdateViewJsonSQL(const string &column_name, const string &json, const string &view_name);

} // namespace duckdb

#endif // OPENIVM_PARSER_CREATE_MV_HELPERS_HPP
