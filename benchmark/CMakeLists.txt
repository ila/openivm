cmake_minimum_required(VERSION 3.15)
project(openivm_benchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find DuckDB
find_package(DuckDB REQUIRED)

# Shared helpers (TPC-C and TPC-DI schema + data generation)
set(HELPERS_SRCS src/tpcc_helpers.cpp src/tpcdi_helpers.cpp)

# Unified rewriter benchmark: runs TPC-C and TPC-DI query suites
add_executable(rewriter_benchmark src/rewriter_benchmark.cpp ${HELPERS_SRCS})
target_link_libraries(rewriter_benchmark PRIVATE DuckDB::DuckDB)
target_include_directories(rewriter_benchmark PRIVATE ${DUCKDB_INCLUDE_DIRS})

# Flag benchmark: measures performance impact of optimization flags on TPC-C
add_executable(flag_benchmark src/flag_benchmark.cpp src/tpcc_helpers.cpp)
target_link_libraries(flag_benchmark PRIVATE DuckDB::DuckDB)
target_include_directories(flag_benchmark PRIVATE ${DUCKDB_INCLUDE_DIRS})

# Cost-model benchmark: compares refresh_cost estimates to measured refresh modes
add_executable(cost_model_benchmark src/cost_model_benchmark.cpp src/tpcc_helpers.cpp)
target_link_libraries(cost_model_benchmark PRIVATE DuckDB::DuckDB)
target_include_directories(cost_model_benchmark PRIVATE ${DUCKDB_INCLUDE_DIRS})

# Daemon benchmark: tests scheduled background refresh
add_executable(daemon_benchmark src/daemon_benchmark.cpp src/tpcc_helpers.cpp)
target_link_libraries(daemon_benchmark PRIVATE DuckDB::DuckDB)
target_include_directories(daemon_benchmark PRIVATE ${DUCKDB_INCLUDE_DIRS})

# TPC-C data generation tool
add_executable(tpcc_setup src/tpcc_setup.cpp)
target_link_libraries(tpcc_setup PRIVATE DuckDB::DuckDB)
target_include_directories(tpcc_setup PRIVATE ${DUCKDB_INCLUDE_DIRS})

# Installation
install(TARGETS rewriter_benchmark flag_benchmark cost_model_benchmark daemon_benchmark tpcc_setup DESTINATION bin)
