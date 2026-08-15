#pragma once

#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

// Registers Spark-compatible scalar functions that DuckDB lacks natively but that
// appear in Spark SQL fed to the openivm compiler (compile/binding coverage).
void RegisterSparkScalarFunctions(ExtensionLoader &loader);

} // namespace duckdb
