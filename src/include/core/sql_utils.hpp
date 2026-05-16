#ifndef SQL_UTILS_HPP
#define SQL_UTILS_HPP

#include "duckdb.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "lpts_pipeline.hpp"

#include <string>

namespace duckdb {

// Utility functions for SQL string manipulation.
// Originally from the compiler extension; inlined here to remove the external dependency.

/// Read the `openivm_target_dialect` extension setting and parse it.
/// Returns SqlDialect::DUCKDB if the setting is unset or NULL.
/// Throws InvalidInputException on unrecognised values.
SqlDialect ReadOpenIvmTargetDialect(ClientContext &context);

class SqlUtils {
public:
	static void WriteFile(const string &filename, bool append, const string &compiled_query);
	static string ExtractTableName(const string &sql);
	static string EscapeSingleQuotes(const string &input);
	static void ReplaceMaterializedView(string &query);
	static string ExtractViewQuery(string &query);
	static string ExtractViewName(const string &query);
	static string SQLToLowercase(const string &sql);
	static void RemoveRedundantWhitespaces(string &query);
	/// Strip SQL line comments (-- to end of line) while respecting single-quoted string literals.
	static void StripLineComments(string &query);
	static vector<string> SplitSQLStatements(const string &sql);
	static string SQLStatementPreview(const string &statement);
	static string DeltaName(const string &name);
	static string LastIdentifierPart(string name);
	static string FullName(const string &catalog, const string &schema, const string &table);
	static string FullDeltaName(const string &catalog, const string &schema, const string &table);
	static string QualifiedPrefix(const string &catalog, const string &schema);
	static string QuoteQualifiedPrefix(const string &prefix);
	static string JsonQuote(const string &value);
	static string JsonArray(const vector<string> &values);
	static string DuckLakeTableFunction(const string &function_name, const string &catalog, const string &schema,
	                                    const string &table, int64_t last_snapshot_id, int64_t current_snapshot_id);
	static bool IsDelta(const string &name);
	static string GenerateDeltaTable(string &query);
	static string JoinQuotedColumns(const vector<string> &columns);
	static string JoinQualifiedQuotedColumns(const vector<string> &columns, const string &alias);
	static string BuildAllNullPredicate(const vector<string> &columns);
	static string BuildNullSafeMatch(const vector<string> &columns, const string &lhs_alias, const string &rhs_alias);
	static string BuildNullSafeKeyPredicate(const vector<string> &columns, const string &left_prefix,
	                                        const string &right_prefix);
	static string BuildFullRecomputeSQL(const string &data_table, const string &view_query_sql);
	static string ReplaceAllOccurrences(string haystack, const string &needle, const string &replacement);
	static vector<string> ReplaceEachPlainOccurrence(const string &haystack, const string &needle,
	                                                 const string &replacement);
	static string ReplaceTableReferences(const string &sql, const string &table_name, const string &replacement);
	static string ReplaceTableReferenceOccurrence(const string &sql, const string &table_name, idx_t occurrence,
	                                              const string &replacement, bool &replaced);
	static vector<string> ReplaceEachTableReference(const string &sql, const string &table_name,
	                                                const string &replacement);
	static bool IdentifierMatchesTable(const string &identifier, const string &table_name);
	static string FindTableReference(const string &sql, const string &table_name);

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

#endif // SQL_UTILS_HPP
