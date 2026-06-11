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
    src/compile_facts.cpp
    src/core/ivm_delta_model.cpp
    src/core/ivm_view_classifier.cpp
    src/core/parser.cpp
    src/core/parser_create_mv_helpers.cpp
    src/core/parser_ddl.cpp
    src/core/parser_parse.cpp
    src/core/parser_plan_helpers.cpp
    src/core/parser_sql_extractors.cpp
    src/core/sql_utils.cpp
    src/core/refresh_metadata.cpp
    src/core/incremental_checker.cpp
    src/core/plan_rewrite.cpp
    src/core/plan_rewrite_aggregates.cpp
    src/delta/delta_compiler.cpp
    src/delta/delta_helpers.cpp
    src/delta/operators/aggregate.cpp
    src/delta/operators/constant.cpp
    src/delta/operators/cte.cpp
    src/delta/operators/delim_join.cpp
    src/delta/operators/dispatch.cpp
    src/delta/operators/distinct.cpp
    src/delta/operators/ducklake_join.cpp
    src/delta/operators/filter.cpp
    src/delta/operators/join.cpp
    src/delta/operators/join_output.cpp
    src/delta/operators/nonlocal.cpp
    src/delta/operators/projection.cpp
    src/delta/operators/scan.cpp
    src/delta/operators/topk.cpp
    src/delta/operators/union.cpp
    src/delta/operators/unnest.cpp
    src/delta/operators/window.cpp
    src/upsert/refresh.cpp
    src/upsert/refresh_delta_fast_paths.cpp
    src/upsert/refresh_group_measure.cpp
    src/upsert/refresh_helpers.cpp
    src/upsert/refresh_sql.cpp
    src/upsert/refresh_window.cpp
    src/upsert/refresh_compiler.cpp
    src/upsert/refresh_compiler_aux.cpp
    src/upsert/refresh_cost_model.cpp
    src/upsert/refresh_index_regen.cpp
    src/rules/incremental_rewrite_rule.cpp
    src/rules/refresh_insert_rule.cpp
    src/rules/schema_evolution.cpp
    src/rules/column_hider.cpp
    src/core/refresh_locks.cpp
    src/core/refresh_daemon.cpp
    src/match/plan_canonical.cpp
    src/match/predicate_oracle.cpp
    src/match/equivalence_classes.cpp
    src/match/constraint_cache.cpp
    src/match/view_catalog_index.cpp
    ${LPTS_DIR}/src/cte_nodes.cpp
    ${LPTS_DIR}/src/lpts_helpers.cpp
    ${LPTS_DIR}/src/sql_dialect.cpp
    ${LPTS_DIR}/src/lpts_parser.cpp
    ${LPTS_DIR}/src/lpts_sql_scanner.cpp
    ${LPTS_DIR}/src/lpts_date_format.cpp
    ${LPTS_DIR}/src/lpts_expression_renderer.cpp
    ${LPTS_DIR}/src/lpts_ast.cpp
    ${LPTS_DIR}/src/lpts_ast_renderer.cpp
    ${LPTS_DIR}/src/lpts_ast_builder.cpp
    ${LPTS_DIR}/src/lpts_ast_flattener.cpp
    ${LPTS_DIR}/src/dialect_function_map.cpp)

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

    # Cost-model accuracy benchmark
    add_executable(cost_model_benchmark benchmark/src/cost_model_benchmark.cpp)
    target_link_libraries(cost_model_benchmark tpcc_helpers ${OPENIVM_LINK_LIBS})
    target_include_directories(cost_model_benchmark PUBLIC benchmark benchmark/src)
endif()
