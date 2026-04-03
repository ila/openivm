#ifndef OPENIVM_UPSERT_HPP
#define OPENIVM_UPSERT_HPP

#pragma once

#include "duckdb.hpp"
#include "core/openivm_constants.hpp"

namespace duckdb {

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters);

// pragma_function_t wrapper: generates refresh SQL and executes it under a per-view lock.
// This ensures the lock is held through both generation AND execution, preventing
// concurrent refresh of the same view from double-applying deltas.
void UpsertDeltaQueriesLocked(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // OPENIVM_UPSERT_HPP
