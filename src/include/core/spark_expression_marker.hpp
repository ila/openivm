#pragma once

#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

void SparkExpressionMarker(DataChunk &args, ExpressionState &state, Vector &result);

} // namespace duckdb
