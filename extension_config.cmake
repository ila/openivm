# This file is included by DuckDB's build system. It specifies which extension to load

# Required by LPTS Delta scan serialization and Delta-backed tests/benchmarks.
duckdb_extension_load(parquet)
duckdb_extension_load(delta
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/third_party/lpts/third_party/duckdb-delta
    LOAD_TESTS
)

# Extension from this repo
duckdb_extension_load(openivm
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
