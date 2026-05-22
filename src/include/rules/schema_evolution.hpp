#pragma once

#include "duckdb.hpp"

namespace duckdb {

string FirstMVReferencingColumn(Connection &con, const string &delta_name, const string &table_name,
                                const string &col_name);
void RewriteDependentViewMetadataForRename(Connection &con, const string &delta_name, const string &table_name,
                                           const string &old_name, const string &new_name);

} // namespace duckdb
