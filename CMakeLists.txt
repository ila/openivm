cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME openivm)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
set(LPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/lpts")
include_directories(src/include ${LPTS_DIR}/src/include)

set(EXTENSION_SOURCES
    src/openivm_extension.cpp
    src/core/openivm_parser.cpp
    src/core/openivm_utils.cpp
    src/core/openivm_metadata.cpp
    src/core/ivm_checker.cpp
    src/core/ivm_plan_rewrite.cpp
    src/upsert/openivm_upsert.cpp
    src/upsert/openivm_compile_upsert.cpp
    src/upsert/openivm_cost_model.cpp
    src/upsert/openivm_index_regen.cpp
    src/rules/openivm_rewrite_rule.cpp
    src/rules/openivm_insert_rule.cpp
    src/rules/ivm_helpers.cpp
    src/rules/ivm_scan_rule.cpp
    src/rules/ivm_filter_rule.cpp
    src/rules/ivm_projection_rule.cpp
    src/rules/ivm_aggregate_rule.cpp
    src/rules/ivm_join_rule.cpp
    src/rules/ivm_union_rule.cpp
    src/rules/ivm_distinct_rule.cpp
    src/rules/ivm_column_hider.cpp
    src/core/openivm_refresh_locks.cpp
    ${LPTS_DIR}/src/logical_plan_to_sql.cpp
    ${LPTS_DIR}/src/lpts_helpers.cpp
    ${LPTS_DIR}/src/lpts_ast.cpp
    ${LPTS_DIR}/src/lpts_pipeline.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

# Benchmark executable - only build for native platforms with jemalloc
if(NOT EMSCRIPTEN AND NOT WIN32 AND TARGET jemalloc_extension)
    add_executable(openivm_benchmark benchmarks/openivm_benchmark.cpp)
    target_link_libraries(openivm_benchmark
        duckdb_static
        duckdb_generated_extension_loader
        ${EXTENSION_NAME}
        core_functions_extension
        parquet_extension
        jemalloc_extension
        duckdb_static)
    target_include_directories(openivm_benchmark PUBLIC benchmarks)
endif()
