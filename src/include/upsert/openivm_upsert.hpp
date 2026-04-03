#ifndef OPENIVM_UPSERT_HPP
#define OPENIVM_UPSERT_HPP

#pragma once

#include "duckdb.hpp"
#include "core/openivm_constants.hpp"

namespace duckdb {

// Generates refresh SQL for each view (including cascaded views) and executes it
// under a per-view lock. This ensures concurrent refresh of the same view is serialized.
void UpsertDeltaQueriesLocked(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // OPENIVM_UPSERT_HPP
