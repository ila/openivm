#ifndef OPENIVM_PARSER_DDL_HPP
#define OPENIVM_PARSER_DDL_HPP

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

static constexpr const char *OPENIVM_DDL_CLEANUP_PREFIX = "openivm_cleanup:";
static constexpr const char *OPENIVM_DDL_PROFILE_PREFIX = "openivm_profile:";

unique_ptr<FunctionData> DDLExecutorBindFunction(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names);
void DDLExecutorExecuteFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output);

} // namespace duckdb

#endif // OPENIVM_PARSER_DDL_HPP
