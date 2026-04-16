cmake_minimum_required(VERSION 3.15)
project(openivm_benchmark)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find DuckDB
find_package(DuckDB REQUIRED)

# TPC-C data generation tool
add_executable(tpcc_setup src/tpcc_setup.cpp)
target_link_libraries(tpcc_setup PRIVATE DuckDB::DuckDB)
target_include_directories(tpcc_setup PRIVATE ${DUCKDB_INCLUDE_DIRS})

# Installation
install(TARGETS tpcc_setup DESTINATION bin)
