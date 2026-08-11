PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=openivm
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

.PHONY: test_regular_nterm_compiled

test_release_internal: test_regular_nterm_compiled

test_regular_nterm_compiled:
	python3 test/integration/test_regular_nterm_compiled.py ./build/release/duckdb
