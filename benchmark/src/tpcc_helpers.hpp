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

// Per-warehouse cardinality profile for the generated TPC-C data.
//
// COMPACT keeps the historical row counts (100 customers and 100 orders per district, 1k items).
// It is 30x below the TPC-C specification, which makes even a large scale factor a small database
// — useful for benchmarks whose subject is compilation or rewrite overhead rather than data volume.
//
// SPEC follows the TPC-C specification. Use it whenever the measurement depends on the base query
// being genuinely expensive, such as deciding between incremental refresh and full recompute: under
// COMPACT the whole base query costs less than the incremental path's fixed planning floor, so full
// recompute wins everywhere and the crossover is unobservable.
enum class TPCCScaleProfile {
	COMPACT,
	SPEC,
};

// TPC-C schema and data. `scale_factor` controls warehouse count and `profile` controls
// per-warehouse cardinality (see InsertTPCCData for per-table row counts).
void CreateTPCCSchema(duckdb::Connection &con);
void InsertTPCCData(duckdb::Connection &con, int scale_factor, TPCCScaleProfile profile = TPCCScaleProfile::COMPACT);

// Returns 500 deterministic DML statements (fixed RNG seed) mixing UPDATE,
// INSERT, and DELETE across CUSTOMER / STOCK / ORDER_LINE / HISTORY /
// NEW_ORDER / WAREHOUSE.
std::vector<std::string> GenerateDeltaPool(int scale_factor);

} // namespace openivm_bench
