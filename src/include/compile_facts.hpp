#ifndef OPENIVM_COMPILE_FACTS_HPP
#define OPENIVM_COMPILE_FACTS_HPP

#pragma once

#include "duckdb.hpp"
#include "core/sql_utils.hpp"

#include <memory>

namespace duckdb {
namespace openivm {

// Per-call compile context for `openivm_compile_with_facts`. The JSON wire
// schema intentionally exposes only fields consumed by planning today: schema
// version, target SQL dialect, compile-only mode, and the view-delta cascade
// override. Field names are snake_case to match the JSON wire form 1:1 —
// `ParseFactsJson` deserialises the JSON object directly into these fields.
class CompileFacts {
public:
	// Schema evolution: v1 = scalar framing only; v2 = adds nested "universal"
	// and "batch" objects. Both are accepted by the parser.
	static constexpr int CURRENT_SCHEMA_VERSION = 2;
	int schema_version = CURRENT_SCHEMA_VERSION;
	SqlDialect target_dialect = SqlDialect::DUCKDB; // required when parsed from JSON
	bool compile_only = false;                      // default false
	bool force_view_delta_cascade = false;

	// ---- Universal facts (stable for the view's lifetime; from the "universal" object) ----
	// When true, the compiler emits derived universal facts back to the caller as extra
	// "derived_fact" output rows (currently the LEFT-JOIN nullable-source set + referenced sources).
	bool emit_derived_facts = false;
	// Forward scaffolding — parsed but not yet consumed by emission. See compile_facts plan.
	vector<string> source_constraints; // e.g. "table:PK(col)", "t:FK(col)->u(col)"
	vector<string> partitioning;       // e.g. "table:partition(col,...)"
	vector<string> target_capabilities; // e.g. "MERGE", "DELETION_VECTORS", "ROWID"

	// ---- Per-batch facts (vary each refresh; from the "batch" object) ----
	// has_batch_facts gates whether planning may trust active_delta_tables/batch_insert_only.
	// When false (no "batch" object — v1 callers, or v2 omitting it) the compile-only path keeps
	// the conservative all-sources / insert_only=false defaults, preserving today's behavior.
	bool has_batch_facts = false;
	vector<string> active_delta_tables; // prefixed form: openivm_delta_<base>
	bool batch_insert_only = false;

	// Returns a default-constructed CompileFacts wrapping the given dialect.
	// Used by native PRAGMA-refresh callers which have no JSON facts — every
	// field stays at its conservative default.
	static CompileFacts Default(SqlDialect dialect = SqlDialect::DUCKDB);
};

// Parses a `CompileFacts` from JSON. Throws InvalidInputException on
// malformed JSON, missing required `target_dialect`, or wrong types.
// Unknown top-level fields are silently ignored for forward compatibility.
CompileFacts ParseFactsJson(const string &json);

// RAII installer for `CompileFacts` on the active ClientContext. The
// optimizer rules in `join.cpp` and `refresh_insert_rule.cpp` cannot
// receive the facts through their function signatures (the DuckDB
// optimizer API gives them only `ClientContext &`), so we stash the facts
// on `ClientContext::registered_state` while the bind/compile is in flight
// and remove them on scope exit. Callers that never construct this slot
// see an empty `registered_state` entry and fall back to a
// default-constructed `CompileFacts`.
class CompileFactsContextSlot {
public:
	// Slot key used in `ClientContext::registered_state`.
	static constexpr const char *SLOT_KEY = "openivm_compile_facts";

	CompileFactsContextSlot(ClientContext &ctx, shared_ptr<CompileFacts> facts);
	~CompileFactsContextSlot();
	CompileFactsContextSlot(const CompileFactsContextSlot &) = delete;
	CompileFactsContextSlot &operator=(const CompileFactsContextSlot &) = delete;

	// Reads the currently installed facts, returning a default-constructed
	// CompileFacts when no slot is present (preserves native-refresh
	// semantics). Safe to call from any optimizer-rule entry point.
	static CompileFacts Get(ClientContext &ctx);

private:
	ClientContext &ctx;
	bool installed;
};

// Table function entry points for `openivm_compile_with_facts`. The
// function emits one row per top-level SQL statement so consumers don't
// have to split on `;` themselves.
//
// The signatures intentionally use the duckdb:: types directly (no forward
// decls in this namespace — otherwise the C++ name-lookup picks
// `duckdb::openivm::TableFunctionInput` first and shadows the real type).
unique_ptr<duckdb::FunctionData> OpenIvmCompileWithFactsBind(duckdb::ClientContext &context,
                                                             duckdb::TableFunctionBindInput &input,
                                                             duckdb::vector<duckdb::LogicalType> &return_types,
                                                             duckdb::vector<duckdb::string> &names);
unique_ptr<duckdb::GlobalTableFunctionState> OpenIvmCompileWithFactsInit(duckdb::ClientContext &context,
                                                                         duckdb::TableFunctionInitInput &input);
void OpenIvmCompileWithFactsExecute(duckdb::ClientContext &context, duckdb::TableFunctionInput &data_p,
                                    duckdb::DataChunk &output);

} // namespace openivm
} // namespace duckdb

#endif // OPENIVM_COMPILE_FACTS_HPP
