#ifndef OPENIVM_COMPILE_FACTS_HPP
#define OPENIVM_COMPILE_FACTS_HPP

#pragma once

#include "duckdb.hpp"
#include "core/sql_utils.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <memory>

namespace duckdb {
namespace openivm {

struct CompileFactsFkRelation {
	string child_table;
	vector<string> child_columns;
	string parent_table;
	vector<string> parent_columns;
	bool rely = true;
};

// Per-call compile context for `openivm_compile_with_facts`. Field names are
// snake_case to match the JSON wire form 1:1 — `ParseFactsJson` deserialises
// the JSON object directly into these fields, and ignores unknown fields.
class CompileFacts {
public:
	// Bumped when the JSON fact schema changes in a backward-compatible way;
	// unknown/extra fields are ignored by ParseFactsJson, so older writers work.
	static constexpr int CURRENT_SCHEMA_VERSION = 2;
	int schema_version = CURRENT_SCHEMA_VERSION;
	SqlDialect target_dialect = SqlDialect::DUCKDB; // required when parsed from JSON
	bool compile_only = false;                      // default false
	bool force_view_delta_cascade = false;

	// Set true ONLY when an external classifier has PROVEN, from the actual change
	// log, that this batch is append-only (every new commit a blind append /
	// AddFile-only, no RemoveFile dataChange). When true under compile_only it
	// re-enables the insert-only fast paths that compile_only otherwise disables.
	// Defaults false — conservative.
	bool assume_insert_only = false;
	bool running_window_incremental = false;
	bool scd2_range_join_accel = false;
	bool emit_spark_hints = false;

	// Per-source delta shape and RELY FK declarations supplied by the caller.
	// Compile-time facts only; the compile path may not see source catalog
	// constraints (e.g. Spark/Delta tables).
	unordered_map<string, string> delta_shape;
	vector<CompileFactsFkRelation> fk_relations;

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
