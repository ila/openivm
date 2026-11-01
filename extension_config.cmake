# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(openivm
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# ICU is linked into the test binary so `require icu` time zone tests
# (refresh_timezone*.test) run in CI instead of being silently skipped.
duckdb_extension_load(icu)

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)