// Shared TPC-C setup and delta-pool helpers for OpenIVM benchmarks.
// Extracted from rewriter_benchmark.cpp so daemon_benchmark can reuse the
// same schema, data generator, and delta pool without duplicating code.

#pragma once

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"

#include <string>
#include <sys/stat.h>
#include <vector>

namespace openivm_bench {

std::string Timestamp();
void Log(const std::string &msg);

bool WriteAllBytes(int fd, const void *buf, size_t n);
bool ReadAllBytes(int fd, void *buf, size_t n);

bool FileExists(const std::string &path);

// TPC-C schema and data. `scale_factor` controls warehouse count (see
// InsertTPCCData for per-table row counts).
void CreateTPCCSchema(duckdb::Connection &con);
void InsertTPCCData(duckdb::Connection &con, int scale_factor);

// Returns 500 deterministic DML statements (fixed RNG seed) mixing UPDATE,
// INSERT, and DELETE across CUSTOMER / STOCK / ORDER_LINE / HISTORY /
// NEW_ORDER / WAREHOUSE.
std::vector<std::string> GenerateDeltaPool(int scale_factor);

// Returns deterministic append-only DML statements for Delta Lake catalogs
// attached as d_<TABLE>. Delta Lake currently has no UPDATE/DELETE/MERGE path
// in DuckDB, so benchmark mutations must be inserts only.
std::vector<std::string> GenerateDeltaAppendOnlyPool(int scale_factor);

// Seed one single-table Delta Lake directory per TPC-C table from the native
// TPC-C database and attach them as d_<TABLE>. Returns false if the delta
// extension or any generated Delta catalog is unavailable.
bool AttachTPCCDeltaCatalogs(duckdb::Connection &con, const std::string &db_path, const std::string &native_catalog);

} // namespace openivm_bench
