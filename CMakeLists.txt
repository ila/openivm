cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME openivm)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
set(LPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../lpts")
include_directories(src/include ${LPTS_DIR}/src/include)

set(EXTENSION_SOURCES
    src/openivm_extension.cpp
    src/openivm_parser.cpp
    src/openivm_upsert.cpp
    src/openivm_rewrite_rule.cpp
    src/openivm_index_regen.cpp
    src/openivm_utils.cpp
    src/openivm_compile_upsert.cpp
    src/openivm_insert_rule.cpp
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
