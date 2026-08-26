#!/usr/bin/env bash
set -euo pipefail

project_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
llvm_version=${LLVM_VERSION:-19}
build_dir=${MUTATION_BUILD_DIR:-"${project_dir}/build/mutation"}
report_dir=${MUTATION_REPORT_DIR:-"${build_dir}/reports"}
report_name=${MUTATION_REPORT_NAME:-openivm-join-poc}
test_filter=${MUTATION_TEST_FILTER:-test/sql/inner_join.test}
workers=${MUTATION_WORKERS:-4}
timeout_ms=${MUTATION_TIMEOUT_MS:-120000}
allow_surviving=${MUTATION_ALLOW_SURVIVING:-0}
config_path=${MULL_CONFIG:-"${project_dir}/tools/mutation/mull.join.yml"}
log_path=${MUTATION_LOG_PATH:-"${report_dir}/${report_name}.log"}

if [[ "${config_path}" != /* ]]; then
	config_path="${project_dir}/${config_path}"
fi
if [[ ! -f "${config_path}" ]]; then
	echo "Mull config not found: ${config_path}" >&2
	exit 2
fi
config_path=$(cd "$(dirname "${config_path}")" && pwd)/$(basename "${config_path}")

clang_bin=${CLANG_BIN:-clang-${llvm_version}}
clangxx_bin=${CLANGXX_BIN:-clang++-${llvm_version}}
mull_runner=${MULL_RUNNER:-mull-runner-${llvm_version}}
mull_plugin=${MULL_PLUGIN:-/usr/lib/mull-ir-frontend-${llvm_version}}

for required_command in cmake ninja "${clang_bin}" "${clangxx_bin}" "${mull_runner}"; do
	if ! command -v "${required_command}" >/dev/null 2>&1; then
		echo "missing required command: ${required_command}" >&2
		echo "see ${project_dir}/tools/mutation/README.md for EC2 setup" >&2
		exit 2
	fi
done

if [[ ! -f "${mull_plugin}" ]]; then
	echo "Mull plugin not found at ${mull_plugin}; set MULL_PLUGIN to its path" >&2
	exit 2
fi

mkdir -p "${build_dir}" "${report_dir}"
exec > >(tee -a "${log_path}") 2>&1

finish() {
	status=$?
	trap - EXIT
	printf '[%s] Mutation campaign finished with status %s\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')" "${status}"
	exit "${status}"
}
trap finish EXIT

printf '[%s] Starting OpenIVM mutation campaign\n' "$(date '+%Y-%m-%dT%H:%M:%S%z')"
printf 'host=%s config=%s test_filter=%s workers=%s timeout_ms=%s\n' \
	"$(hostname)" "${config_path}" "${test_filter}" "${workers}" "${timeout_ms}"
export MULL_CONFIG="${config_path}"

cmake -G Ninja \
	-S "${project_dir}/duckdb" \
	-B "${build_dir}" \
	-DCMAKE_BUILD_TYPE=RelWithDebInfo \
	-DCMAKE_C_COMPILER="${clang_bin}" \
	-DCMAKE_CXX_COMPILER="${clangxx_bin}" \
	-DDUCKDB_EXTENSION_CONFIGS="${project_dir}/extension_config.cmake" \
	-DEXTENSION_STATIC_BUILD=1 \
	-DUNITTEST_ROOT_DIRECTORY="${project_dir}" \
	-DBENCHMARK_ROOT_DIRECTORY="${project_dir}" \
	-DENABLE_UNITTEST_CPP_TESTS=FALSE \
	-DENABLE_EXTENSION_AUTOLOADING=FALSE \
	-DENABLE_EXTENSION_AUTOINSTALL=FALSE \
	-DOPENIVM_MULL_PLUGIN="${mull_plugin}" \
	-DOPENIVM_MULL_CONFIG="${config_path}"

cmake --build "${build_dir}" --target unittest --parallel "${workers}"

mull_args=(
	--workers "${workers}"
	--timeout "${timeout_ms}"
	--reporters IDE
	--reporters SQLite
	--report-dir "${report_dir}"
	--report-name "${report_name}"
)
if [[ "${allow_surviving}" == "1" ]]; then
	mull_args+=(--allow-surviving)
fi

"${mull_runner}" "${mull_args[@]}" "${build_dir}/test/unittest" "${test_filter}"

echo "Mutation report: ${report_dir}/${report_name}.sqlite"
echo "Progress log: ${log_path}"
