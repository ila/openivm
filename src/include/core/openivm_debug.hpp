#pragma once

// Set to 1 to enable verbose OpenIVM debug output, 0 to disable.
// This is separate from DuckDB's DEBUG macro to avoid cluttering test output.
#define OPENIVM_DEBUG 0

#if OPENIVM_DEBUG
#include <cstdio>
#define OPENIVM_DEBUG_PRINT(...) printf(__VA_ARGS__)
#else
#define OPENIVM_DEBUG_PRINT(...) ((void)0)
#endif
