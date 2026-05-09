cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME openivm)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
set(LPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/lpts")
include_directories(src/include ${LPTS_DIR}/src/include ${LPTS_DIR}/third_party/ducklake/src/include)

set(EXTENSION_SOURCES
    src/openivm_extension.cpp
    src/core/openivm_parser.cpp
    src/core/openivm_utils.cpp
    src/core/openivm_metadata.cpp
    src/core/ivm_delta_model.cpp
    src/core/ivm_plan_rewrite.cpp
    src/upsert/openivm_upsert.cpp
    src/upsert/openivm_compile_upsert.cpp
    src/upsert/openivm_cost_model.cpp
    src/upsert/openivm_index_regen.cpp
    src/rules/openivm_rewrite_rule.cpp
    src/rules/openivm_insert_rule.cpp
    src/rules/helpers.cpp
    src/rules/scan.cpp
    src/rules/filter.cpp
    src/rules/projection.cpp
    src/rules/aggregate.cpp
    src/rules/join.cpp
    src/rules/delim_join.cpp
    src/rules/ducklake_join.cpp
    src/rules/union.cpp
    src/rules/distinct.cpp
    src/rules/window.cpp
    src/rules/topk.cpp
    src/rules/column_hider.cpp
    src/core/openivm_refresh_locks.cpp
    src/core/openivm_refresh_daemon.cpp
    src/match/plan_canonical.cpp
    src/match/predicate_oracle.cpp
    src/match/equivalence_classes.cpp
    src/match/constraint_cache.cpp
    src/match/mv_catalog_index.cpp
    ${LPTS_DIR}/src/cte_nodes.cpp
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

# Test and benchmark tools - only build for native platforms
if(NOT EMSCRIPTEN AND NOT WIN32)
    set(OPENIVM_LINK_LIBS
        duckdb_static
        duckdb_generated_extension_loader
        ${EXTENSION_NAME}
        core_functions_extension
        parquet_extension
        duckdb_static)

    if(DUCKDB_EXTENSION_JEMALLOC_SHOULD_LINK)
        list(APPEND OPENIVM_LINK_LIBS jemalloc_extension)
    endif()

    # TPC-C data generation tool
    add_executable(tpcc_setup benchmark/src/tpcc_setup.cpp)
    target_link_libraries(tpcc_setup ${OPENIVM_LINK_LIBS})
    target_include_directories(tpcc_setup PUBLIC benchmark)

    # Shared TPC-C helpers used by rewriter_benchmark, daemon_benchmark, and flag_benchmark.
    add_library(tpcc_helpers STATIC benchmark/src/tpcc_helpers.cpp)
    target_link_libraries(tpcc_helpers ${OPENIVM_LINK_LIBS})
    target_include_directories(tpcc_helpers PUBLIC benchmark/src)

    # TPC-DI schema + data helpers (used only by rewriter_benchmark).
    add_library(tpcdi_helpers STATIC benchmark/src/tpcdi_helpers.cpp)
    target_link_libraries(tpcdi_helpers ${OPENIVM_LINK_LIBS})
    target_include_directories(tpcdi_helpers PUBLIC benchmark/src)

    # IVM query benchmark harness (supports --workload tpcc|tpcdi)
    add_executable(rewriter_benchmark benchmark/src/rewriter_benchmark.cpp)
    target_link_libraries(rewriter_benchmark tpcc_helpers tpcdi_helpers ${OPENIVM_LINK_LIBS})
    target_include_directories(rewriter_benchmark PUBLIC benchmark benchmark/src)

    # Refresh-daemon / cascade / pipeline benchmark
    add_executable(daemon_benchmark benchmark/src/daemon_benchmark.cpp)
    target_link_libraries(daemon_benchmark tpcc_helpers ${OPENIVM_LINK_LIBS})
    target_include_directories(daemon_benchmark PUBLIC benchmark benchmark/src)

    # Optimization-flag performance benchmark (Phase 2)
    add_executable(flag_benchmark benchmark/src/flag_benchmark.cpp)
    target_link_libraries(flag_benchmark tpcc_helpers ${OPENIVM_LINK_LIBS})
    target_include_directories(flag_benchmark PUBLIC benchmark benchmark/src)
endif()
