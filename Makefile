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

.PHONY: test_scheduler_catalog

test_release_internal: test_scheduler_catalog

test_scheduler_catalog:
	cmake --build build/release --target scheduler_catalog_test
	python3 test/integration/test_scheduler_catalog.py ./build/release/extension/openivm/scheduler_catalog_test

.PHONY: test_benchmark_metadata

test_release_internal: test_benchmark_metadata

test_benchmark_metadata:
	cmake --build build/release --target benchmark_metadata_test
	dir=$$(mktemp -d) && ./build/release/extension/openivm/benchmark_metadata_test $$dir | grep -qx PASS; status=$$?; rm -rf $$dir; exit $$status
