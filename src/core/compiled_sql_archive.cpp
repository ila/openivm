#include "core/compiled_sql_archive.hpp"

#include "core/openivm_constants.hpp"
#include "core/openivm_debug.hpp"
#include "core/refresh_locks.hpp"
#include "core/refresh_metadata.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/common/error_data.hpp"

#include <chrono>

namespace duckdb {

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

vector<string> CompiledSQLArchiveSchemaDDL(const string &catalog, const string &schema) {
	return {"create table if not exists " + ArchiveTableName(catalog, schema, openivm::COMPILED_PROGRAMS_TABLE) +
	            " (view_name varchar, view_catalog varchar, view_schema varchar, view_sql_name varchar,"
	            " operation varchar, version integer, statement_count integer,"
	            " compilation_id varchar, compiled_at timestamp,"
	            " last_compilation_id varchar, last_outcome varchar, last_outcome_at timestamp,"
	            " committed_count bigint default 0,"
	            " primary key (view_name, operation, version))",
	        "create table if not exists " + ArchiveTableName(catalog, schema, openivm::COMPILED_STATEMENTS_TABLE) +
	            " (view_name varchar, operation varchar, version integer, stmt_order integer, sql varchar,"
	            " primary key (view_name, operation, version, stmt_order))"};
}

vector<string> BuildCompiledSQLArchiveStatements(const CompiledProgram &program, CompiledProgramOutcome outcome,
                                                 const string &catalog, const string &schema) {
	vector<string> result;
	if (program.statements.empty()) {
		return result;
	}
	result = CompiledSQLArchiveSchemaDDL(catalog, schema);
	auto programs = ArchiveTableName(catalog, schema, openivm::COMPILED_PROGRAMS_TABLE);
	auto statements = ArchiveTableName(catalog, schema, openivm::COMPILED_STATEMENTS_TABLE);
	auto literal = [](const string &value) {
		return "'" + SqlUtils::EscapeSingleQuotes(value) + "'";
	};
	auto view_filter = "view_name = " + literal(program.view_name) + " AND operation = " + literal(program.operation);
	auto latest_version = "(SELECT max(version) FROM " + statements + " WHERE " + view_filter + ")";
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
	result.push_back("INSERT INTO " + programs +
	                 " (view_name, view_catalog, view_schema, view_sql_name, operation, version, statement_count,"
	                 " compilation_id, compiled_at, last_compilation_id, last_outcome, last_outcome_at,"
	                 " committed_count) SELECT " +
	                 literal(program.view_name) + ", " + literal(program.view_catalog) + ", " +
	                 literal(program.view_schema) + ", " + literal(program.view_sql_name) + ", " +
	                 literal(program.operation) + ", latest.version, " + to_string(program.statements.size()) + ", " +
	                 compilation_id + ", " + openivm::UTC_NOW_SQL + ", " + compilation_id + ", " + outcome_name + ", " +
	                 openivm::UTC_NOW_SQL + ", " + committed + " FROM (SELECT " + latest_version +
	                 " AS version) latest WHERE latest.version IS NOT NULL AND NOT EXISTS (SELECT 1 FROM " + programs +
	                 " p WHERE p.view_name = " + literal(program.view_name) +
	                 " AND p.operation = " + literal(program.operation) + " AND p.version = latest.version)");
	// An unchanged program reuses the latest version; record this operation's outcome there.
	result.push_back("UPDATE " + programs + " SET last_compilation_id = " + compilation_id +
	                 ", last_outcome = " + outcome_name + ", last_outcome_at = " + openivm::UTC_NOW_SQL +
	                 ", committed_count = COALESCE(committed_count, 0) + " + committed + " WHERE " + view_filter +
	                 " AND version = " + latest_version + " AND compilation_id IS DISTINCT FROM " + compilation_id);
	return result;
}

string WriteCompiledProgram(Connection &con, const CompiledProgram &program, CompiledProgramOutcome outcome,
                            const string &catalog, const string &schema) {
	if (program.statements.empty()) {
		return string();
	}
	// Same rules as BuildCompiledSQLArchiveStatements, but with bound parameters: DuckDB
	// parses long string literals slowly, and refresh programs are several KB each.
	auto execute = [&](const string &sql, vector<Value> parameters, string &error) {
		unique_ptr<MaterializedQueryResult> materialized;
		auto prepared = con.Prepare(sql);
		if (prepared->HasError()) {
			error = prepared->GetError();
			return materialized;
		}
		auto result = prepared->Execute(parameters, false);
		if (result->HasError()) {
			error = result->GetError();
			return materialized;
		}
		materialized = unique_ptr_cast<QueryResult, MaterializedQueryResult>(std::move(result));
		return materialized;
	};
	string error;
	// Databases created before the archive existed get its tables on first use.
	auto resolved_schema = schema.empty() ? string(DEFAULT_SCHEMA) : schema;
	if (!con.TableInfo(catalog, resolved_schema, openivm::COMPILED_PROGRAMS_TABLE) ||
	    !con.TableInfo(catalog, resolved_schema, openivm::COMPILED_STATEMENTS_TABLE)) {
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
	auto stored = execute("SELECT version, sql FROM " + statements +
	                          " WHERE view_name = $1 AND operation = $2 AND version = (SELECT max(version) FROM " +
	                          statements + " WHERE view_name = $1 AND operation = $2) ORDER BY stmt_order",
	                      {view_name, operation}, error);
	if (!stored) {
		return error;
	}
	bool changed = true;
	int32_t version = 1;
	if (stored->RowCount() > 0) {
		version = stored->GetValue(0, 0).GetValue<int32_t>();
		// Verbatim comparison: same count, order, and text.
		changed = stored->RowCount() != program.statements.size();
		for (idx_t row = 0; !changed && row < stored->RowCount(); row++) {
			auto value = stored->GetValue(1, row);
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
		// The primary key keeps an existing description, as NOT EXISTS does in the SQL form.
		return execute("INSERT OR IGNORE INTO " + programs +
		                   " (view_name, view_catalog, view_schema, view_sql_name, operation, version, statement_count,"
		                   " compilation_id, compiled_at, last_compilation_id, last_outcome, last_outcome_at,"
		                   " committed_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, " +
		                   string(openivm::UTC_NOW_SQL) + ", $8, $9, " + openivm::UTC_NOW_SQL + ", $10)",
		               {view_name, Value(program.view_catalog), Value(program.view_schema),
		                Value(program.view_sql_name), operation, Value::INTEGER(version),
		                Value::INTEGER(static_cast<int32_t>(program.statements.size())), compilation_id, outcome_name,
		                committed},
		               error);
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
		return string();
	}
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
	if (updated->GetValue(0, 0).GetValue<int64_t>() == 0 && !describe_version()) {
		return error;
	}
	return string();
}

string RecordCompiledProgram(ClientContext &context, const string &view_catalog, const CompiledProgram &program,
                             CompiledProgramOutcome outcome) {
	if (program.statements.empty()) {
		return string();
	}
	try {
		Connection con(*context.db);
		// The caller owns the database-wide mutation gate; let tracked writes re-enter it.
		TransactionalMVLockState::Get(*con.context)
		    .SetMutationOwner(TransactionalMVLockState::Get(context).GetMutationOwner());
		RefreshMetadata::UseCatalog(context, con, view_catalog);
		auto catalog = con.Query("SELECT current_database()");
		if (catalog->HasError()) {
			return catalog->GetError();
		}
		// One metadata commit for the whole program, never one per statement.
		con.BeginTransaction();
		auto error = WriteCompiledProgram(con, program, outcome, catalog->GetValue(0, 0).ToString(), DEFAULT_SCHEMA);
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
