// IVM Column Hider — view creation/drop helpers for hiding internal IVM columns.
//
// The MV data table (_ivm_data_<name>) stores all columns including internal ones
// (_ivm_left_key, _ivm_distinct_count, etc.). A user-facing VIEW (<name>) exposes
// only the user's columns via SELECT * EXCLUDE (...).
//
// This file is intentionally minimal — the naming logic lives in ivm_column_hider.hpp.
// The parser (openivm_parser.cpp) creates the view during CREATE MATERIALIZED VIEW,
// and the insert rule (openivm_insert_rule.cpp) drops it during DROP TABLE.

#include "rules/ivm_column_hider.hpp"

namespace duckdb {
// All logic is in the header (IVMTableNames is header-only).
} // namespace duckdb
