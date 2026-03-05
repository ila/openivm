#ifndef OPENIVM_UTILS_HPP
#define OPENIVM_UTILS_HPP

#include "duckdb.hpp"

#include <string>

namespace duckdb {

// Utility functions for SQL string manipulation.
// Originally from the compiler extension; inlined here to remove the external dependency.

class OpenIVMUtils {
public:
	static void WriteFile(const string &filename, bool append, const string &compiled_query);
	static string ReadFile(const string &file_path);
	static string ExtractTableName(const string &sql);
	static string EscapeSingleQuotes(const string &input);
	static void ReplaceMaterializedView(string &query);
	static string ExtractViewQuery(string &query);
	static string ExtractViewName(const string &query);
	static string SQLToLowercase(const string &sql);
	static void ReplaceCount(string &query);
	static void ReplaceSum(string &query);
	static void RemoveRedundantWhitespaces(string &query);
	static string GenerateDeltaTable(string &query);
};

} // namespace duckdb

#endif // OPENIVM_UTILS_HPP
