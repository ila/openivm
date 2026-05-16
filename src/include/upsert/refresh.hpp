#ifndef REFRESH_HPP
#define REFRESH_HPP

#pragma once

#include "duckdb.hpp"
#include "core/openivm_constants.hpp"

namespace duckdb {

// Generates refresh SQL for each view (including cascaded views) and executes it
// under a per-view lock. This ensures concurrent refresh of the same view is serialized.
void UpsertDeltaQueriesLocked(ClientContext &context, const FunctionParameters &parameters);

// PRAGMA compile_refresh('view_name') — generates the refresh SQL for the view
// without executing it. Returns a substitute SELECT that emits one row with
// (refresh_type INTEGER, refresh_type_name VARCHAR, sql VARCHAR).
//
// The compile uses the `openivm_target_dialect` setting (defaults to 'duckdb')
// so callers (e.g. openivm-spark) can request 'spark' dialect output.
// `openivm_files_path` artifact is still written as a side effect, matching
// the existing `PRAGMA refresh(...)` semantics under `openivm_compile_only=true`.
string CompileRefreshQuery(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // REFRESH_HPP
