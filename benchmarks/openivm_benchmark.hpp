#ifndef OPENIVM_BENCHMARK_HPP
#define OPENIVM_BENCHMARK_HPP

#include <string>
#include "duckdb.hpp"

namespace duckdb {

int RunIVMBenchmark(const string &out_csv = "",
                    int threads = 8);

} // namespace duckdb

#endif // OPENIVM_BENCHMARK_HPP
