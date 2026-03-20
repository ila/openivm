#ifndef OPENIVM_UPSERT_HPP
#define OPENIVM_UPSERT_HPP

#pragma once

#include "duckdb.hpp"
#include "openivm_constants.hpp"

namespace duckdb {

string UpsertDeltaQueries(ClientContext &context, const FunctionParameters &parameters);

} // namespace duckdb

#endif // OPENIVM_UPSERT_HPP
