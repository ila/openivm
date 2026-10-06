#ifndef OPENIVM_COMPILED_SQL_ARCHIVE_HPP
#define OPENIVM_COMPILED_SQL_ARCHIVE_HPP

#include "duckdb.hpp"

namespace duckdb {

// Archive of the compiled CREATE/REFRESH programs OpenIVM executed, stored in the
// view's metadata catalog/schema. A new version is stored only when the complete
// statement list differs from the latest stored version for the same view and
// operation; all versions are retained until users delete them with SQL. The SQL
// is an inspection artifact: it embeds state-specific cutoffs, snapshot IDs, and
// catalog references, so later operations always compile against current state.
enum class CompiledProgramOutcome : uint8_t {
	// Execution started but failed; its transaction rolled back, so nothing committed.
	ATTEMPTED,
	// The operation's transaction committed. Written inside that transaction where
	// possible, so the row only becomes visible if the operation commits.
	COMMITTED,
	// Execution started, but OpenIVM cannot tell whether its effects committed (for
	// example, a cross-catalog refresh that failed after its own-connection metadata
	// or data statements ran).
	UNKNOWN
};

const char *CompiledProgramOutcomeName(CompiledProgramOutcome outcome);

struct CompiledProgram {
	string view_name; // internal MV key (openivm_views.view_name)
	string view_catalog;
	string view_schema;
	string view_sql_name;
	string operation; // "create" or "refresh"
	string compilation_id;
	vector<string> statements; // complete program in execution order
};

// CREATE TABLE IF NOT EXISTS statements for the archive tables. `catalog`/`schema`
// may be empty to leave the names unqualified.
vector<string> CompiledSQLArchiveSchemaDDL(const string &catalog = "", const string &schema = "");

// One batch of statements that stores `program` as a new version if it changed and
// records `outcome` for the stored version. Runs in the caller's transaction.
vector<string> BuildCompiledSQLArchiveStatements(const CompiledProgram &program, CompiledProgramOutcome outcome,
                                                 const string &catalog, const string &schema);

// Same as executing BuildCompiledSQLArchiveStatements through `con`, in its current
// transaction, but with bound parameters. Returns an empty string or the error message.
string WriteCompiledProgram(Connection &con, const CompiledProgram &program, CompiledProgramOutcome outcome,
                            const string &catalog, const string &schema);

// Records `program` in its own metadata transaction on a helper connection. Returns
// an empty string on success or the error message; it never throws.
string RecordCompiledProgram(ClientContext &context, const string &view_catalog, const CompiledProgram &program,
                             CompiledProgramOutcome outcome);

// Identifier shared by the refresh profile and the archive for one operation.
string NewCompilationId(const string &view_name, const string &operation = "");

} // namespace duckdb

#endif // OPENIVM_COMPILED_SQL_ARCHIVE_HPP
