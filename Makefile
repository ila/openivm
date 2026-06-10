PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=openivm
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

ifneq (,$(findstring windows,$(DUCKDB_PLATFORM)))
TEST_PATH=/test/unittest.exe
PATH := ./build/release/src:./build/debug/src:./build/reldebug/src:$(PATH)
export PATH
endif
