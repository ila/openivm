#ifndef OPENIVM_UTILS_HPP
#define OPENIVM_UTILS_HPP

#include "duckdb.hpp"
#include "duckdb/parser/keyword_helper.hpp"

#include <string>

namespace duckdb {

// Utility functions for SQL string manipulation.
// Originally from the compiler extension; inlined here to remove the external dependency.

class OpenIVMUtils {
public:
	static void WriteFile(const string &filename, bool append, const string &compiled_query);
	static string ExtractTableName(const string &sql);
	static string EscapeSingleQuotes(const string &input);
	static void ReplaceMaterializedView(string &query);
	static string ExtractViewQuery(string &query);
	static string ExtractViewName(const string &query);
	static string SQLToLowercase(const string &sql);
	static void RemoveRedundantWhitespaces(string &query);
	static string DeltaName(const string &name);
	static string FullName(const string &catalog, const string &schema, const string &table);
	static string FullDeltaName(const string &catalog, const string &schema, const string &table);
	static bool IsDelta(const string &name);
	static string GenerateDeltaTable(string &query);

	/// Parse a REFRESH EVERY interval string (e.g. "5 minutes", "2 hours") into seconds.
	/// Returns -1 if no interval clause found. Throws on invalid format or < 60 seconds.
	static int64_t ParseRefreshInterval(const string &interval_str);

	/// Extract and strip the REFRESH EVERY clause from a CREATE MATERIALIZED VIEW query.
	/// Returns the parsed interval in seconds, or -1 if not present.
	static int64_t ExtractRefreshInterval(string &query);

	/// Quote an identifier for safe use in generated SQL (handles reserved words and special chars).
	static string QuoteIdentifier(const string &name) {
		return KeywordHelper::WriteOptionallyQuoted(name);
	}

	/// Escape a string value for use inside single-quoted SQL literals.
	/// Use for WHERE view_name = '<escaped>' clauses.
	static string EscapeValue(const string &val) {
		return EscapeSingleQuotes(val);
	}
};

} // namespace duckdb

#endif // OPENIVM_UTILS_HPP
