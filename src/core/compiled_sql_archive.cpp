#include "core/compiled_sql_archive.hpp"
#include "core/parser_create_mv_helpers.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/common/error_data.hpp"

#include <chrono>

namespace duckdb {

bool CompiledSQLArchiveEnabled(ClientContext &context) {
	Value value;
	return context.TryGetCurrentSetting(Identifier("openivm_archive_compiled_sql"), value) && !value.IsNull() &&
	       value.GetValue<bool>();
}

const char *CompiledProgramOutcomeName(CompiledProgramOutcome outcome) {
	switch (outcome) {
	case CompiledProgramOutcome::ATTEMPTED:
		return "attempted";
	case CompiledProgramOutcome::COMMITTED:
		return "committed";
	default:
		return "unknown";
	}
}

static string ArchiveTableName(const string &catalog, const string &schema, const char *table) {
	if (catalog.empty()) {
		return SqlUtils::QuoteIdentifier(table);
	}
	return SqlUtils::FullName(catalog, schema.empty() ? string(DEFAULT_SCHEMA) : schema, table);
}

static vector<string> ArchiveRetentionStatements(ClientContext &context, const CompiledProgram &program,
                                                 const string &catalog, const string &schema) {
	Value retention;
	if (!context.TryGetCurrentSetting(Identifier("openivm_compiled_sql_retention"), retention) || retention.IsNull()) {
		throw InternalException("Compiled SQL archive retention setting is missing");
	}
	auto programs = ArchiveTableName(catalog, schema, openivm::COMPILED_PROGRAMS_TABLE);
	auto statements = ArchiveTableName(catalog, schema, openivm::COMPILED_STATEMENTS_TABLE);
	auto filter = "view_name = " + Value(program.view_name).ToSQLString() +
	              " AND operation = " + Value(program.operation).ToSQLString();
	auto obsolete = filter + " AND version NOT IN (SELECT version FROM " + programs + " WHERE " + filter +
	                " ORDER BY version DESC LIMIT " + to_string(retention.GetValue<int64_t>()) + ")";
	// Delete complete versions together, in the same transaction as the archive write.
	// The newest version stays present, preserving deduplication and version numbering.
	return {"DELETE FROM " + statements + " WHERE " + obsolete, "DELETE FROM " + programs + " WHERE " + obsolete};
}

vector<string> CompiledSQLArchiveSchemaDDL(const string &catalog, const string &schema) {
	return {CreateMetadataTableSQL(openivm::COMPILED_PROGRAMS_TABLE,
	                               ArchiveTableName(catalog, schema, openivm::COMPILED_PROGRAMS_TABLE)),
	        CreateMetadataTableSQL(openivm::COMPILED_STATEMENTS_TABLE,
	                               ArchiveTableName(catalog, schema, openivm::COMPILED_STATEMENTS_TABLE))};
}

// Lifecycle SQL and the parameterized writer use the same version description.
// Only literal rendering differs; the writer keeps long program text parameterized.
static string DescribeCompiledVersionSQL(const CompiledProgram &program, CompiledProgramOutcome outcome,
                                         const string &programs, const string &version, const string &from,
                                         const std::function<string(const Value &)> &render) {
	auto view = render(Value(program.view_name));
	auto operation = render(Value(program.operation));
	auto compilation = render(Value(program.compilation_id));
	return "INSERT INTO " + programs +
	       " (view_name, view_catalog, view_schema, view_sql_name, operation, version, statement_count,"
	       " compilation_id, compiled_at, last_compilation_id, last_outcome, last_outcome_at, committed_count) "
	       "SELECT " +
	       view + ", " + render(Value(program.view_catalog)) + ", " + render(Value(program.view_schema)) + ", " +
	       render(Value(program.view_sql_name)) + ", " + operation + ", " + version + ", " +
	       to_string(program.statements.size()) + ", " + compilation + ", " + openivm::UTC_NOW_SQL + ", " +
	       compilation + ", " + render(Value(CompiledProgramOutcomeName(outcome))) + ", " + openivm::UTC_NOW_SQL +
	       ", " + (outcome == CompiledProgramOutcome::COMMITTED ? "1" : "0") + from + " WHERE " + version +
	       " IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + programs + " p WHERE p.view_name = " + view +
	       " AND p.operation = " + operation + " AND p.version = " + version + ")";
}

static string LatestCompiledVersionSQL(const string &statements, const string &filter) {
	// Both forms return NULL for empty history. DuckDB v1.5.4's MAX statistics
	// optimization asserts when transaction-local CREATE rows are all filtered out
	// by the first refresh lookup, so use an ordered scalar subquery instead.
	return "(SELECT version FROM " + statements + " WHERE " + filter + " ORDER BY version DESC LIMIT 1)";
}

vector<string> BuildCompiledSQLArchiveStatements(ClientContext &context, const CompiledProgram &program,
                                                 CompiledProgramOutcome outcome, const string &catalog,
                                                 const string &schema) {
	vector<string> result;
	if (!CompiledSQLArchiveEnabled(context) || program.statements.empty()) {
		return result;
	}
	result = CompiledSQLArchiveSchemaDDL(catalog, schema);
	auto programs = ArchiveTableName(catalog, schema, openivm::COMPILED_PROGRAMS_TABLE);
	auto statements = ArchiveTableName(catalog, schema, openivm::COMPILED_STATEMENTS_TABLE);
	auto literal = [](const string &value) {
		return "'" + SqlUtils::EscapeSingleQuotes(value) + "'";
	};
	auto view_filter = "view_name = " + literal(program.view_name) + " AND operation = " + literal(program.operation);
	auto latest_version = LatestCompiledVersionSQL(statements, view_filter);
	auto compilation_id = literal(program.compilation_id);
	auto outcome_name = literal(CompiledProgramOutcomeName(outcome));
	auto committed = outcome == CompiledProgramOutcome::COMMITTED ? string("1") : string("0");

	// Store the statements as the next version unless they equal the latest stored
	// version exactly (same count, order, and text). Programs are compared verbatim.
	string values;
	for (idx_t i = 0; i < program.statements.size(); i++) {
		if (i > 0) {
			values += ", ";
		}
		values += "(" + to_string(i) + ", " + literal(program.statements[i]) + ")";
	}
	result.push_back("INSERT INTO " + statements +
	                 " (view_name, operation, version, stmt_order, sql)"
	                 " WITH openivm_new(stmt_order, sql) AS (VALUES " +
	                 values + "), openivm_old AS (SELECT stmt_order, sql FROM " + statements + " WHERE " + view_filter +
	                 " AND version = " + latest_version + ") SELECT " + literal(program.view_name) + ", " +
	                 literal(program.operation) + ", COALESCE(" + latest_version +
	                 ", 0) + 1, stmt_order, sql FROM openivm_new WHERE EXISTS (SELECT 1 FROM openivm_new n"
	                 " FULL OUTER JOIN openivm_old o ON n.stmt_order = o.stmt_order"
	                 " WHERE n.stmt_order IS NULL OR o.stmt_order IS NULL OR n.sql IS DISTINCT FROM o.sql)");
	// Describe a newly stored version.
	result.push_back(DescribeCompiledVersionSQL(program, outcome, programs, "latest.version",
	                                            " FROM (SELECT " + latest_version + " AS version) latest",
	                                            [](const Value &value) { return value.ToSQLString(); }));
	// An unchanged program reuses the latest version; record this operation's outcome there.
	result.push_back("UPDATE " + programs + " SET last_compilation_id = " + compilation_id +
	                 ", last_outcome = " + outcome_name + ", last_outcome_at = " + openivm::UTC_NOW_SQL +
	                 ", committed_count = COALESCE(committed_count, 0) + " + committed + " WHERE " + view_filter +
	                 " AND version = " + latest_version + " AND compilation_id IS DISTINCT FROM " + compilation_id);
	auto cleanup = ArchiveRetentionStatements(context, program, catalog, schema);
	result.insert(result.end(), cleanup.begin(), cleanup.end());
	return result;
}

string WriteCompiledProgram(ClientContext &context, Connection &con, const CompiledProgram &program,
                            CompiledProgramOutcome outcome, const string &catalog, const string &schema) {
	if (!CompiledSQLArchiveEnabled(context) || program.statements.empty()) {
		return string();
	}
	// Same rules as BuildCompiledSQLArchiveStatements, but with bound parameters: DuckDB
	// parses long string literals slowly, and refresh programs are several KB each.
	auto execute = [&](const string &sql, vector<Value> parameters, string &error) {
		unique_ptr<QueryResult> materialized;
		auto prepared = con.Prepare(sql);
		if (prepared->HasError()) {
			error = prepared->GetError();
			return materialized;
		}
		auto result = prepared->Execute(parameters);
		if (result->HasError()) {
			error = result->GetError();
			return materialized;
		}
		materialized = unique_ptr_cast<QueryResult, QueryResult>(std::move(result));
		return materialized;
	};
	string error;
	// Databases created before the archive existed get its tables on first use.
	auto resolved_schema = schema.empty() ? string(DEFAULT_SCHEMA) : schema;
	if (!con.TableInfo(Identifier(catalog), Identifier(resolved_schema),
	                   Identifier(openivm::COMPILED_PROGRAMS_TABLE)) ||
	    !con.TableInfo(Identifier(catalog), Identifier(resolved_schema),
	                   Identifier(openivm::COMPILED_STATEMENTS_TABLE))) {
		for (auto &ddl : CompiledSQLArchiveSchemaDDL(catalog, schema)) {
			auto created = con.Query(ddl);
			if (created->HasError()) {
				return created->GetError();
			}
		}
	}
	auto programs = ArchiveTableName(catalog, schema, openivm::COMPILED_PROGRAMS_TABLE);
	auto statements = ArchiveTableName(catalog, schema, openivm::COMPILED_STATEMENTS_TABLE);
	Value view_name(program.view_name);
	Value operation(program.operation);
	const string view_filter = "view_name = $1 AND operation = $2";
	auto stored = execute("SELECT version, sql FROM " + statements + " WHERE " + view_filter + " AND version = " +
	                          LatestCompiledVersionSQL(statements, view_filter) + " ORDER BY stmt_order",
	                      {view_name, operation}, error);
	if (!stored) {
		return error;
	}
	bool changed = true;
	int32_t version = 1;
	if (stored->RowCount() > 0) {
		version = stored->Collection().GetValue(0, 0).GetValue<int32_t>();
		// Verbatim comparison: same count, order, and text.
		changed = stored->RowCount() != program.statements.size();
		for (idx_t row = 0; !changed && row < stored->RowCount(); row++) {
			auto value = stored->Collection().GetValue(1, row);
			changed = value.IsNull() || StringValue::Get(value) != program.statements[row];
		}
		if (changed) {
			version++;
		}
	}
	Value compilation_id(program.compilation_id);
	Value outcome_name(CompiledProgramOutcomeName(outcome));
	auto committed = Value::BIGINT(outcome == CompiledProgramOutcome::COMMITTED ? 1 : 0);
	auto describe_version = [&]() {
		vector<Value> parameters {Value::INTEGER(version)};
		auto sql = DescribeCompiledVersionSQL(program, outcome, programs, "$1", "", [&](const Value &value) {
			parameters.push_back(value);
			return "$" + to_string(parameters.size());
		});
		return execute(sql, std::move(parameters), error);
	};
	if (changed) {
		vector<Value> sql_values;
		for (auto &statement : program.statements) {
			sql_values.emplace_back(statement);
		}
		if (!execute("INSERT INTO " + statements +
		                 " (view_name, operation, version, stmt_order, sql)"
		                 " SELECT $1, $2, $3, i::INTEGER, $4[i + 1] FROM range(len($4)) t(i)",
		             {view_name, operation, Value::INTEGER(version), Value::LIST(LogicalType::VARCHAR, sql_values)},
		             error) ||
		    !describe_version()) {
			return error;
		}
	} else {
		auto updated =
		    execute("UPDATE " + programs +
		                " SET last_compilation_id = $1, last_outcome = $2, last_outcome_at = " + openivm::UTC_NOW_SQL +
		                ", committed_count = COALESCE(committed_count, 0) + $3"
		                " WHERE view_name = $4 AND operation = $5 AND version = $6",
		            {compilation_id, outcome_name, committed, view_name, operation, Value::INTEGER(version)}, error);
		if (!updated) {
			return error;
		}
		// A version row deleted by hand is described again, as in the SQL form.
		if (updated->Collection().GetValue(0, 0).GetValue<int64_t>() == 0 && !describe_version()) {
			return error;
		}
	}
	for (auto &sql : ArchiveRetentionStatements(context, program, catalog, schema)) {
		auto pruned = con.Query(sql);
		if (pruned->HasError()) {
			return pruned->GetError();
		}
	}
	OPENIVM_DEBUG_PRINT("[ARCHIVE] Applied version retention for %s (%s)\n", program.view_name.c_str(),
	                    program.operation.c_str());
	return string();
}

string RecordCompiledProgram(ClientContext &context, const string &view_catalog, const CompiledProgram &program,
                             CompiledProgramOutcome outcome) {
	if (!CompiledSQLArchiveEnabled(context) || program.statements.empty()) {
		return string();
	}
	try {
		Connection con(*context.db);
		// The caller owns the database-wide mutation gate; let tracked writes re-enter it.
		TransactionalMVLockState::Get(*con.context)
		    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
		// The archive belongs to the view's metadata location (openivm_metadata_catalog/_schema).
		RefreshMetadata::UseCatalog(context, con, view_catalog);
		auto location = RefreshMetadata::CurrentLocation(con);
		string error;
		// One metadata commit for the whole program, never one per statement.
		con.BeginTransaction();
		error = WriteCompiledProgram(context, con, program, outcome, location.catalog, location.schema);
		if (!error.empty()) {
			con.Rollback();
			return error;
		}
		con.Commit();
		OPENIVM_DEBUG_PRINT("[ARCHIVE] Recorded %s %s program for %s (%s)\n", program.operation.c_str(),
		                    CompiledProgramOutcomeName(outcome), program.view_name.c_str(),
		                    program.compilation_id.c_str());
		return string();
	} catch (std::exception &ex) {
		return ErrorData(ex).RawMessage();
	}
}

string NewCompilationId(const string &view_name, const string &operation) {
	auto now = std::chrono::steady_clock::now().time_since_epoch();
	return view_name + "_" + (operation.empty() ? string() : operation + "_") +
	       to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

} // namespace duckdb
