# OpenIVM mutation-testing POC

This POC uses Mull's LLVM instrumentation to compile the mutations once. It
instruments only the static OpenIVM library linked into `unittest`; DuckDB,
LPTS, and the loadable OpenIVM extension are not mutation targets.

The default campaign mutates `src/delta/operators/join.cpp` and runs the real
`test/sql/inner_join.test` SQLLogicTest case. Mull writes an SQLite result
database and IDE-readable survivor diagnostics under `build/mutation/reports`.

## EC2 prerequisites

An Ubuntu/Debian EC2 instance needs Ninja, LLVM 19, and the matching Mull 19
package. Mull publishes its apt repository at
https://mull.readthedocs.io/en/latest/Installation.html.

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake ninja-build curl git clang-19
curl -1sLf https://dl.cloudsmith.io/public/mull-project/mull-stable/setup.deb.sh | sudo -E bash
sudo apt-get update
sudo apt-get install -y mull-19
```

Initialize the repository's submodules before the first build, then run:

```bash
tools/mutation/run_mull_poc.sh
```

The initial DuckDB build is the expensive part. Subsequent campaigns reuse the
same `build/mutation` tree.

Campaigns fail when mutants survive. Set `MUTATION_ALLOW_SURVIVING=1` only for
an exploratory classification run where a nonzero mutation score is expected.

OpenIVM is compiled at `-O1`: its normal optimized build folds two C++11
`static constexpr` daemon values that become ODR-used at `-O0`. This retains a
useful LLVM mutation pipeline without adding production definitions solely for
the mutation tool.

The Mull configuration is fingerprinted into the static extension compile
command. Switching between the focused and overnight configs therefore makes
Ninja re-instrument OpenIVM, but does not rebuild DuckDB.

## Useful controls

```bash
# Use all available cores and a five-minute per-mutant ceiling.
MUTATION_WORKERS=$(nproc) MUTATION_TIMEOUT_MS=300000 \
  tools/mutation/run_mull_poc.sh

# Run a wider set of real SQL tests without rebuilding.
MUTATION_TEST_FILTER='test/sql/*join*.test' \
  tools/mutation/run_mull_poc.sh

# All OpenIVM delta-compiler files and the complete SQL suite. This is the overnight run.
MULL_CONFIG=tools/mutation/mull.openivm.yml \
MUTATION_REPORT_NAME=openivm-overnight \
MUTATION_WORKERS=$(nproc) \
MUTATION_TEST_FILTER='test/sql/*' \
  tools/mutation/run_mull_poc.sh

# Run the overnight campaign after disconnecting from SSH.
nohup env \
  MULL_CONFIG=tools/mutation/mull.openivm.yml \
  MUTATION_REPORT_NAME=openivm-overnight \
  MUTATION_WORKERS=$(nproc) \
  MUTATION_TEST_FILTER='test/sql/*' \
  tools/mutation/run_mull_poc.sh >/dev/null 2>&1 &

# Follow build steps, test progress, mutant counts, and final status.
tail -f build/mutation/reports/openivm-overnight.log

# Point at versioned tools installed somewhere else.
CLANG_BIN=/opt/llvm/bin/clang \
CLANGXX_BIN=/opt/llvm/bin/clang++ \
MULL_RUNNER=/opt/mull/bin/mull-runner \
MULL_PLUGIN=/opt/mull/lib/mull-ir-frontend \
  tools/mutation/run_mull_poc.sh
```

For a smaller overnight rotation, copy `mull.openivm.yml`, narrow
`includePaths` to one delta-compiler operator or file, and set `MULL_CONFIG` to
the copy. Keep DuckDB and `third_party/` excluded. Targeted SQL filters make
subsystem runs substantially faster than the complete suite.

## Reviewing survivors

Use `mull-ignore` only after proving that a mutation cannot affect correctness.
Put the reason beside the annotation and name the narrowest applicable mutator.
Broad `mull-off` blocks are not used. Optimization thresholds and debug-only
branches are reasonable suppressions; multiplicity arithmetic, masks, bounds,
join classification, and other correctness logic must remain visible until a
test kills the mutant or equivalence is demonstrated.
