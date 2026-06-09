# Agent Guide

This file provides guidance to Claude Code and Codex when working with code in this repository.

## Working with the user

When you're stuck — either unable to fix a bug after 2-3 attempts, or tempted to work around the actual problem by redefining the objective — **stop and ask the user for directions**. Explain clearly what the specific problem is (e.g., "CompileAggregateGroups returns wrong SQL when the view has a 3-way join — should I fix the LPTS output or generate SQL manually?"). The user knows this codebase deeply and can often point you to the right solution in one sentence. Do not silently change the goal, declare something impossible, or add bloated workarounds without consulting first. We work as a team.

Always test your changes with real queries (e.g., create a materialized view, insert data, run `PRAGMA refresh()`, check the result) before declaring success, not just unit tests.

Never execute git commands that could lose code. Always ask the user for permission on those.

## Development rules

- **New features must have tests.** Ask the user whether to create a new test file or extend an existing one in `test/sql/`.
- **Never remove a failing test to "fix" a failure.** If a test fails, fix the underlying bug. Tests exist for a reason.
- **Never weaken tests to match current behavior.** If a test exposes a bug, the test stays and the bug gets fixed. Never limit test scope to avoid known failures, skip broken scenarios, mark tests as expected-fail, comment them out, or convert them to TODOs. Write the test for the correct behavior, then make the code pass it. This applies to ALL repos (openivm, lpts, etc.).
- **Do not fix OpenIVM correctness bugs by avoiding incrementalization.** If a query is meant to be incrementalizable, preserve an incremental maintenance path and fix the delta/rewrite/upsert logic. Do not demote it to `FULL_REFRESH`, force full recompute, or relax classification just to make correctness checks pass. If an incremental fix is unclear after a couple of attempts, stop and ask.
- **Every IVM refresh in a test MUST be cross-checked** with `EXCEPT ALL` in both directions to verify full bag equality between the MV and the base query. Never just check COUNT or specific values.
- **Stress tests must batch many conflicting DML ops** (INSERT + DELETE + UPDATE on same rows) before a single refresh. Don't do 1 insert → refresh → 1 delete → refresh — that's not testing delta consolidation.
- **Before implementing anything, search the existing codebase** for similar patterns or solutions. Check if a helper function, utility, or prior approach already addresses the problem. Reuse before reinventing.
- **Use helper functions.** Factor shared logic into helpers rather than duplicating code. Check `src/delta/delta_helpers.cpp`, `src/upsert/refresh_helpers.cpp`, and `src/include/core/sql_utils.hpp` for existing utilities.
- **Never edit the `duckdb/` submodule.** The DuckDB source is read-only. All IVM logic lives in `src/` and `test/`. If you need DuckDB internals, use the public API or ask the user.
- **Keep the paper in mind.** OpenIVM is described in [OpenIVM: a SQL-to-SQL Compiler for Incremental Computations](https://arxiv.org/abs/2404.16486). Refer to it for the theoretical foundations (DBSP framework, Z-sets, delta rules) before making changes to core rewrite or upsert logic.
- **Add `OPENIVM_DEBUG_PRINT` statements** at major code flow points (entry/exit of rewrite rules, upsert compilation, cost model decisions). Use the existing `OPENIVM_DEBUG_PRINT` macro from `src/include/core/openivm_debug.hpp` — it's compiled out when `OPENIVM_DEBUG` is 0.

## What is OpenIVM?

OpenIVM is a DuckDB extension for **Incremental View Maintenance (IVM)**. It lets users create materialized views with `CREATE MATERIALIZED VIEW` and automatically maintain them when underlying tables change, without recomputing the entire view from scratch. Changes are tracked in delta tables and applied incrementally via `PRAGMA refresh('view_name')`.

## Build & Test

```bash
GEN=ninja make                # build (release)
make test                     # run all tests

# single test
build/release/test/unittest "test/sql/inner_join.test"
```

Build outputs go to `build/release/`. DuckDB is a git submodule in `third_party/duckdb/`.

## Architecture

OpenIVM operates in two phases:

### Phase 1: View Creation (Parser + Optimizer Extensions)

When `CREATE MATERIALIZED VIEW <name> AS <query>` is parsed:

1. **Parser extension** (`src/core/parser.cpp`) intercepts the statement
2. Creates delta tables (`openivm_delta_<table>`) for each source table with extra columns: `openivm_multiplicity` (INTEGER signed Z-set weight, +1=insert, −1=delete) and `openivm_timestamp` (TIMESTAMP)
3. Stores view metadata in system tables (`openivm_views`, `openivm_delta_tables`)
4. Runs `PlanRewrite` to normalize FILTER aggregates, DISTINCT, AVG, STDDEV/VARIANCE, CTE refs, and hidden outer-join key/count columns before classification
5. Creates a physical `openivm_data_<view>` table and a user-facing DuckDB view that hides internal `openivm_*` columns; `ORDER BY`/`LIMIT` wrappers are stripped from the stored data query and appended to the user-facing view

### Phase 2: Incremental Refresh (`PRAGMA refresh()`)

When `PRAGMA refresh('view_name')` is called:

1. **Optimizer rewrite rules** transform the view's logical plan to read from delta tables instead of base tables
2. **Upsert compilation** generates SQL to apply the deltas to the materialized view
3. **Cost model** (when `openivm_adaptive_refresh = true`) decides whether IVM or full recompute is cheaper

### Delta IR + Operator Dispatch

Refresh planning builds a typed `DeltaViewModel` (a graph of `DeltaModelNode`, each carrying a kind,
rule kind, maintenance mode, aux-state requirements, and lineage facts) from the optimized view plan —
see `src/core/ivm_delta_model.cpp` and `src/core/ivm_view_classifier.cpp`. The `DeltaCompiler`
(`src/delta/delta_compiler.cpp`) then walks the plan and lowers each node to a delta `DeltaPlanFragment`.

Dispatch is **single-sourced**: `NodeKindForOperator` (in `ivm_delta_model.cpp`) maps an operator type to
a `DeltaModelNodeKind`, and `DispatchDeltaCompile` (`src/delta/operators/dispatch.cpp`) is the one switch
that routes each kind to its `Compile*Delta` function. The model-driven path uses the node's kind; the
copied-subtree path (join inclusion-exclusion terms, whose pointers aren't in the model) derives the kind
from the operator type via the same `NodeKindForOperator`. Every returned fragment is checked for a valid
multiplicity binding (`ValidateDeltaFragment`, a real exception in release builds).

| Operator (`DeltaModelNodeKind`) | Compile function | File |
|---|---|---|
| Table Scan (`SCAN`) | `CompileScanDelta` | `src/delta/operators/scan.cpp` |
| Inner/outer joins, cross product, any join (`JOIN`/`SEMI_ANTI`) | `CompileJoinDelta` | `src/delta/operators/join.cpp` |
| DuckLake / delim joins | `CompileDelimJoinDelta`, `Compile*` | `src/delta/operators/delim_join.cpp`, `ducklake_join.cpp` |
| Projection (`PROJECT`) | `CompileProjectionDelta` | `src/delta/operators/` |
| Aggregate (`AGGREGATE`) | `CompileAggregateDelta` | `src/delta/operators/` |
| Filter (`FILTER`) | `CompileFilterDelta` | `src/delta/operators/` |
| UNION ALL (`UNION`) | `CompileUnionDelta` | `src/delta/operators/` |
| DISTINCT (`DISTINCT`) | `CompileDistinctDelta` | `src/delta/operators/` |
| Window (`WINDOW`) | `CompileWindowDelta` | `src/delta/operators/` |
| ORDER BY / LIMIT / TOP_N (`TOP_K`) | `CompileTopKDelta` | `src/delta/operators/` |
| Constant leaf (`CONSTANT`) | `CompileConstantZeroDelta` (delta side) / `CompileStaticConstantLeaf` (copied base side) | `src/delta/operators/constant.cpp` |

Shared delta-rule helpers live in `src/delta/delta_helpers.cpp` and the join helpers
(`BuildMultiplicityProduct`, `CollectJoinLeaves`, …) in `src/delta/operators/join.cpp` /
`src/include/delta/operators/`. The optimizer entry point is
`src/rules/incremental_rewrite_rule.cpp` (detects the COMPUTEDELTA marker, builds the model, runs the
compiler, adds the INSERT node).

### Join Delta Rule (Inclusion-Exclusion)

For N tables, the join rule generates 2^N - 1 terms using inclusion-exclusion:
- For each non-empty subset S of tables: replace tables in S with their delta scans, keep others as current base table scans
- Combined multiplicity = `(-1)^(k-1) × ∏ wᵢ` over leaves in the mask (Möbius inclusion-exclusion sign × Z-set bilinear product). The Möbius sign is required because OpenIVM's "current base" scan reads `R_now = R_old + ΔR` (deltas already merged into source by the insert rule), so the textbook DBSP all-positive delta-join formula doesn't apply directly.
- All terms are combined with UNION ALL
- Terms are pruned by FK-aware optimization and empty-delta skipping

For DuckLake tables, the join rule uses N-term telescoping instead (exactly N terms, using snapshot-based time travel). See `docs/ducklake.md`.

INNER, LEFT/RIGHT, and FULL OUTER joins are supported. LEFT JOIN aggregates use incremental MERGE by default (`openivm_left_join_merge=true`) and can fall back to group-recompute. FULL OUTER projection views use bidirectional key-based partial recompute; FULL OUTER aggregate views use Zhang & Larson-style MERGE by default (`openivm_full_outer_merge=true`) with group-recompute fallback. Max 16 tables (`openivm::MAX_JOIN_TABLES`).

### Upsert Compilation

Three compilation paths based on view type (`RefreshType`):

| View Type | Function | Strategy |
|---|---|---|
| `AGGREGATE_GROUP` | `CompileAggregateGroups()` | CTE consolidates delta per group, MERGE INTO updates/inserts |
| `AGGREGATE_HAVING` | `CompileAggregateGroups()` or group-recompute | Data table stores all groups; user view applies HAVING predicate. `openivm_having_merge=true` uses MERGE |
| `SIMPLE_AGGREGATE` | `CompileSimpleAggregates()` | Single CTE consolidates all columns, UPDATE adds deltas |
| `SIMPLE_PROJECTION` | `CompileProjectionsFilters()` | Counting-based consolidation (GROUP BY + generate_series/rowid for bag semantics) |
| `WINDOW_PARTITION` | `CompileWindowRecompute()` or DuckLake snapshot diff | Delete/reinsert only affected partitions |
| `GROUP_RECOMPUTE` | `CompileGroupRecompute()` | Delete/reinsert only affected groups; used for inner DISTINCT under aggregate and other non-linear group cases |
| `DISTINCT_INCREMENTAL` | `CompileDistinctIncremental()` | Aux-state path for single-source inner DISTINCT under one SUM when `openivm_distinct_aux_state=true`; falls back to group-recompute if metadata is missing |
| `FULL_REFRESH` | `BuildRecomputeQuery()` | DELETE all rows from `openivm_data_<view>`, INSERT full query, then cleanup deltas |

MIN/MAX and ARG_MIN/ARG_MAX usually force group-recompute unless insert-only fast paths are enabled and safe. AVG is decomposed to SUM+COUNT; STDDEV/VARIANCE are decomposed to SUM, COUNT, and sum-of-squares helper columns.

### Key Architectural Rules

- **Multiplicity column** (`openivm_multiplicity`): INTEGER signed Z-set weight — `+1` = insert, `-1` = delete. Multiplicity > 1 is encoded as repeated rows.
- **Delta tables** are named `openivm_delta_<base_table>` and include a timestamp column for tracking when changes occurred
- **LPTS** (LogicalPlanToString, in `third_party/lpts/`) converts logical plans back to SQL for compilation
- **Cost model** (`src/upsert/refresh_cost_model.cpp`) compares estimated incremental refresh cost vs full recompute cost. Includes filter selectivity estimation and learned regression from execution history (when `openivm_adaptive_refresh` is enabled)
- **View matching is scaffolded but gated off by default** (`openivm_enable_view_matching=false`). Files in `src/match/` define canonicalization, predicate implication, constraint cache, and view candidate index APIs; many implementations are still TODO stubs, so do not assume matcher behavior exists unless the code path is implemented and guarded.
- **AVG on DECIMAL columns drifts 1–2 ULPs vs native `AVG`.** DuckDB's native `AVG(DECIMAL)` uses internal compensated arithmetic that no `SUM(x)/COUNT(x)` decomposition can reproduce bit-exactly. The MV's stored value is semantically correct but not bit-identical to the base query (e.g. MV has `47.989999999999994884` vs base `47.99000000000000199`). The `rewriter_benchmark` verify rounds `DOUBLE`/`FLOAT` columns to 10 decimals before `EXCEPT ALL`; unit tests in `test/sql/` stay strict, so test authors should use `DOUBLE` base columns or cast `AVG(val::DOUBLE)` when exercising AVG on DECIMAL. Full write-up in `docs/limitations.md`.

## Key Source Files

- `src/openivm_extension.cpp` — extension entry point, registers pragmas and optimizer rules
- `src/core/parser.cpp` — parser extension for `CREATE MATERIALIZED VIEW`
- `src/core/incremental_checker.cpp` — single-pass plan analysis and `RefreshType` classification metadata
- `src/core/plan_rewrite.cpp` — CREATE-time logical-plan normalizations before LPTS and classification
- `src/core/refresh_metadata.cpp` — system table queries (view definitions, delta tables, timestamps); a `RefreshMetadata` instance memoizes definition-stable view columns
- `src/core/ivm_delta_model.cpp` — `DeltaViewModel`/`DeltaModelNode` IR construction, `NodeKindForOperator`, lineage population (including the left-join-key lineage)
- `src/core/sql_utils.cpp` — SQL string manipulation, file I/O, table name extraction
- `src/delta/delta_compiler.cpp` — `DeltaCompiler`: builds the refresh-time model and recursively lowers the plan to a delta plan
- `src/delta/operators/dispatch.cpp` — single kind-based `DispatchDeltaCompile` + fragment-contract validation
- `src/delta/operators/join.cpp` — join incrementalization (inclusion-exclusion; `BuildMultiplicityProduct`)
- `src/delta/operators/ducklake_join.cpp` — DuckLake N-term telescoping join rule
- `src/delta/delta_helpers.cpp` — shared delta-rule utilities (CreateDeltaGetNode, DuckLake delta nodes)
- `src/rules/incremental_rewrite_rule.cpp` — optimizer entry point (COMPUTEDELTA marker → model → compile → INSERT)
- `src/rules/refresh_insert_rule.cpp` — adds INSERT operators to inject deltas into views
- `src/upsert/refresh.cpp` — PRAGMA refresh() handler, orchestrates delta computation
- `src/upsert/refresh_compiler.cpp` — generates upsert SQL queries
- `src/upsert/refresh_cost_model.cpp` — incremental refresh vs recompute cost estimation
- `src/upsert/refresh_index_regen.cpp` — table index renumbering for plan copies
- `src/core/refresh_locks.cpp` — per-view and per-delta-table mutexes for concurrent refresh safety
- `src/core/refresh_daemon.cpp` — background thread for automatic REFRESH EVERY
- `src/match/*.cpp` — view-matching scaffolding gated by `openivm_enable_view_matching`

## Configuration Options

| Setting | Type | Default | Description |
|---|---|---|---|
| `openivm_refresh_mode` | VARCHAR | `"incremental"` | `"incremental"`, `"full"`, or `"auto"` |
| `openivm_adaptive_refresh` | BOOLEAN | `false` | Enable adaptive cost model (learned regression + IVM vs recompute decision) |
| `openivm_cascade_refresh` | VARCHAR | `"downstream"` | Cascade mode: `"off"`, `"upstream"`, `"downstream"`, `"both"` |
| `openivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh exceeds interval |
| `openivm_disable_daemon` | BOOLEAN | `false` | Disable the background refresh daemon |
| `openivm_files_path` | VARCHAR | — | Path for compiled query reference files |
| `openivm_cost_decay` | DOUBLE | `0.9` | Decay factor for learned cost model regression (0.0–1.0) |
| `openivm_skip_empty_deltas` | BOOLEAN | `true` | Skip refresh or join terms when deltas are empty |
| `openivm_ducklake_nterm` | BOOLEAN | `true` | N-term telescoping for DuckLake joins (vs 2^N-1) |
| `openivm_fk_pruning` | BOOLEAN | `true` | Prune inclusion-exclusion join terms using FK constraints |
| `openivm_skip_aggregate_delete` | BOOLEAN | `true` | Skip DELETE for insert-only aggregate deltas |
| `openivm_skip_projection_delete` | BOOLEAN | `true` | Skip DELETE/consolidation for insert-only projection deltas |
| `openivm_minmax_incremental` | BOOLEAN | `true` | Use GREATEST/LEAST for MIN/MAX when deltas are insert-only |
| `openivm_having_merge` | BOOLEAN | `true` | Use MERGE for HAVING views instead of group-recompute |
| `openivm_left_join_merge` | BOOLEAN | `true` | Use incremental MERGE for LEFT JOIN aggregates instead of group-recompute |
| `openivm_full_outer_merge` | BOOLEAN | `true` | Use incremental MERGE for FULL OUTER JOIN aggregates (Zhang & Larson) instead of group-recompute |
| `openivm_distinct_aux_state` | BOOLEAN | `false` | Use aux-state DISTINCT maintenance for supported single-source inner DISTINCT under aggregate |
| `openivm_enable_view_matching` | BOOLEAN | `false` | Enable query-time view matching master flag; implementation is still scaffolded |
| `openivm_predicate_oracle` | VARCHAR | `"interval"` | Predicate oracle mode for matching: `"syntactic"`, `"interval"`, or `"sat"` stub |
| `openivm_match_strategies` | VARCHAR | `"all"` | CSV of allowed matcher strategies |
| `openivm_match_estimate_ttl_ms` | BIGINT | `5000` | Freshness window for cached pending-delta estimates |
| `openivm_match_log_decisions` | BOOLEAN | `false` | Log matcher decisions to `openivm_match_log` |
| `openivm_match_log_retention` | BIGINT | `50` | Rows retained per query hash in matcher log |

Views using unsupported constructs (non-deterministic functions such as `RANDOM()`, unsupported aggregates, grouping sets, and some complex CTE/UNION shapes) are automatically classified as `FULL_REFRESH` — no setting needed.

## Debugging

Set `#define OPENIVM_DEBUG 1` in `src/include/core/openivm_debug.hpp` for stderr trace output. Use `EXPLAIN` to see the transformed plan.

## IVM DDL examples

```sql
-- Create a materialized view with automatic refresh
CREATE MATERIALIZED VIEW product_sales REFRESH EVERY '5 minutes' AS
  SELECT product_name, SUM(amount) as total, COUNT(*) as cnt
  FROM orders GROUP BY product_name;

-- Insert new data into the base table (automatically tracked in delta_orders)
INSERT INTO orders VALUES ('Widget', 100);

-- Or refresh manually at any time
PRAGMA refresh('product_sales');

-- Check refresh status (interval, last refresh, next refresh)
PRAGMA refresh_status('product_sales');

-- Check cost estimate (incremental refresh vs full recompute)
PRAGMA refresh_cost('product_sales');

-- Replace an existing MV with a new definition
CREATE OR REPLACE MATERIALIZED VIEW product_sales REFRESH EVERY '10 minutes' AS
  SELECT product_name, SUM(amount) as total, COUNT(*) as cnt, AVG(amount) as avg_amount
  FROM orders GROUP BY product_name;

-- Force full recompute
SET openivm_refresh_mode = 'full';
PRAGMA refresh('product_sales');
```

## Code style (clang-tidy)

The project uses clang-tidy with DuckDB's configuration (`.clang-tidy`). Key naming rules:

- **Classes/Enums**: `CamelCase` (e.g., `IncrementalJoinRule`, `RefreshType`)
- **Functions**: `CamelCase` (e.g., `CompileAggregateGroups`, `CreateDeltaGetNode`)
- **Variables/parameters/members**: `lower_case` (e.g., `view_name`, `mul_binding`)
- **Constants/static/constexpr**: `UPPER_CASE` (e.g., `VIEWS_TABLE`, `DELTA_PREFIX`)
- **Macros**: `UPPER_CASE` (e.g., `OPENIVM_DEBUG_PRINT`)

Other style rules (from `.clang-format`, based on LLVM):

- **Tabs for indentation**, width 4
- **Column limit**: 120
- **Braces**: same line as statement (K&R / Allman-attached)
- **Pointers**: right-aligned (`int *ptr`, not `int* ptr`)
- **No short functions on single line**
- **Templates**: always break after `template<...>`
- **Long arguments**: align after open bracket

Run `make format-fix` to auto-format. Claude Code also runs it automatically via hook after edits.

## Codex compatibility

Codex reads this file through `AGENTS.md`, which should be a symlink to `CLAUDE.md` so project memory has one source of truth.

Project and shared DuckDB skills live in `.claude/skills/`. Codex sees them through symlinks in `/home/ila/.codex/skills/`; refresh those links with `.claude/sync-codex-skills.sh` after adding or renaming a skill.

The `.codex` path in this checkout is a read-only runtime mount, not a project directory, so project-local Codex skills cannot live there.

Claude Code runs `make format-fix` through `.claude/settings.json`. Codex does not automatically consume that hook, so run `make format-fix` explicitly after C++ edits.
