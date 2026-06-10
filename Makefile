PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=openivm
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

ifneq (,$(filter windows_amd64 windows_arm64,$(DUCKDB_PLATFORM)))
EXT_RELEASE_FLAGS += -DCMAKE_CXX_FLAGS=-U_SECURE_SCL
EXT_DEBUG_FLAGS += -DCMAKE_CXX_FLAGS=-U_SECURE_SCL
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile
