# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(openivm
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# DuckLake tests install from this build's repository, including in CI where
# no previously installed extension is available. Only vcpkg-enabled builds
# have DuckLake's roaring dependency; the PostgreSQL and tidy jobs do not.
if(VCPKG_BUILD AND NOT CLANG_TIDY)
    duckdb_extension_load(ducklake
        SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}/third_party/lpts/third_party/ducklake
    )
endif()

# Any extra extensions that should be built
# e.g.: duckdb_extension_load(json)
# DuckDB 2.0 builds extensions without linking them unless explicitly requested.
duckdb_extension_statically_link(openivm)
