#ifndef REFRESH_HPP
#define REFRESH_HPP

#pragma once

#include "duckdb.hpp"
#include "core/openivm_constants.hpp"

namespace duckdb {

// Generates refresh SQL for each view (including cascaded views) and executes it.
// The caller must own the database-wide OpenIVM mutation gate.
void UpsertDeltaQueriesLocked(ClientContext &context, const FunctionParameters &parameters);
string TransactionalRefreshQuery(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // REFRESH_HPP
