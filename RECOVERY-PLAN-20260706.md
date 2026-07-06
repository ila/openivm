# Recovery Plan - 2026-07-06

Current base: `main` at `3b9ddc22` (`Fix semi anti duplicate column rewrite`).
Backup before recovery work: `backup/main-before-cost-model-recovery-20260706`.

## Definitely recover

1. Cost model benchmark and related cost-model fixes - applied
   - Source branch: `main-on-raki-latest` first, fallback `backup/openivm-main-before-raki-rollback-20260706`.
   - Includes `benchmark/src/cost_model_benchmark.cpp`, benchmark CMake wiring, refresh-cost model fixes, benchmark scenarios, and related tests/docs.
   - Apply as individual commits or focused patches. Do not merge the whole branch.
   - Applied through the ms-grounded dual-per-unit prior, fractional delta sweeps, fatter set-based TPC-C benchmark data, cost benchmark docs, and the `NodeKindForOperator` linkage fix.
   - Verified with:
     - `cmake --build build/release --target openivm_extension unittest cost_model_benchmark --parallel 4`
     - `build/release/test/unittest "test/sql/auto_refresh.test"`
     - `./build/release/extension/openivm/cost_model_benchmark --scale 1 --db /tmp/openivm_cost_recovery_sf1_20260706.db --out /tmp/openivm_cost_recovery_sf1_20260706.csv --reps 1 --delta-pcts 0,1 --configs all_on --batch validated --filter Q01`

2. ASOF tests - applied
   - Source branch: `main-on-raki-latest`.
   - Recover split ASOF-heavy SQL suites:
     - `test/sql/auto_refresh_asof.test`
     - `test/sql/cascade_group_recompute_delta_asof.test`
     - `test/sql/compile_refresh_asof.test`
     - `test/sql/nonlocal_operator_recompute_asof.test`
   - Keep current `main` ASOF implementation; recover test coverage carefully.
   - Applied as split SQL tests without `require notwindows` gates.
   - Verified the four ASOF files and the four parent files:
     - `test/sql/auto_refresh_asof.test`
     - `test/sql/cascade_group_recompute_delta_asof.test`
     - `test/sql/compile_refresh_asof.test`
     - `test/sql/nonlocal_operator_recompute_asof.test`
     - `test/sql/auto_refresh.test`
     - `test/sql/cascade_group_recompute_delta.test`
     - `test/sql/compile_refresh.test`
     - `test/sql/nonlocal_operator_recompute.test`

## Review before applying

1. Spark-safe cascade companion work
   - Source branch: `saved/openivm-after-raki-pr-20260706`.
   - Commit: `68831da5 refresh: preserve Spark-safe cascade companions`.
   - Purpose: avoid Spark-unsafe inline temp-table cascade companions and use split-safe companion delta output.

2. Insert-only and dimension-table performance paths
   - Most base behavior is already on `main` through Raki's squashed `909a1063`.
   - Verify whether any WorkloadFacts-facing active-source/insert-only behavior is still missing.
   - Do not replay compile-facts-era mechanics raw.

3. LEFT JOIN SIMPLE_PROJECTION insert-only append fast path
   - Source commit: `54fdb767`.
   - Current `main` has related affected-key pushdown code, but missing behavior/tests should be audited before recovery.

## Do not replay raw

1. Compile-facts delta-shape branch
   - Branch: `codex/raki-delta-shape-sf100-verified`.
   - Treat as behavioral reference only. Translate useful active-source/insert-only semantics into WorkloadFacts.

2. Whole backup branches
   - `backup/openivm-main-before-raki-rollback-20260706` and `main-on-raki-latest` contain stale code that would revert current fixes.
   - Use individual commits or manual patches only.

## Local artifacts

Untracked `.worktrees/`, benchmark CSVs, and old notes are local artifacts and must not be included in recovery commits.
