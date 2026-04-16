# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working with the user

When you're stuck — either unable to fix a bug after 2-3 attempts, or tempted to work around the actual problem by redefining the objective — **stop and ask the user for directions**. Explain clearly what the specific problem is (e.g., "CompileAggregateGroups returns wrong SQL when the view has a 3-way join — should I fix the LPTS output or generate SQL manually?"). The user knows this codebase deeply and can often point you to the right solution in one sentence. Do not silently change the goal, declare something impossible, or add bloated workarounds without consulting first. We work as a team.

Always test your changes with real queries (e.g., create a materialized view, insert data, run `PRAGMA ivm()`, check the result) before declaring success, not just unit tests.

Never execute git commands that could lose code. Always ask the user for permission on those.

## Development rules

- **New features must have tests.** Ask the user whether to create a new test file or extend an existing one in `test/sql/`.
- **Never remove a failing test to "fix" a failure.** If a test fails, fix the underlying bug. Tests exist for a reason.
- **Never weaken tests to match current behavior.** If a test exposes a bug, the test stays and the bug gets fixed. Never limit test scope to avoid known failures, skip broken scenarios, mark tests as expected-fail, comment them out, or convert them to TODOs. Write the test for the correct behavior, then make the code pass it. This applies to ALL repos (openivm, lpts, etc.).
- **Every IVM refresh in a test MUST be cross-checked** with `EXCEPT ALL` in both directions to verify full bag equality between the MV and the base query. Never just check COUNT or specific values.
- **Stress tests must batch many conflicting DML ops** (INSERT + DELETE + UPDATE on same rows) before a single refresh. Don't do 1 insert → refresh → 1 delete → refresh — that's not testing delta consolidation.
- **Before implementing anything, search the existing codebase** for similar patterns or solutions. Check if a helper function, utility, or prior approach already addresses the problem. Reuse before reinventing.
- **Use helper functions.** Factor shared logic into helpers rather than duplicating code. Check `src/include/rules/ivm_rule.hpp`, `src/rules/ivm_helpers.cpp`, and `src/include/core/openivm_utils.hpp` for existing utilities.
- **Never edit the `duckdb/` submodule.** The DuckDB source is read-only. All IVM logic lives in `src/` and `test/`. If you need DuckDB internals, use the public API or ask the user.
- **Keep the paper in mind.** OpenIVM is described in [OpenIVM: a SQL-to-SQL Compiler for Incremental Computations](https://arxiv.org/abs/2404.16486). Refer to it for the theoretical foundations (DBSP framework, Z-sets, delta rules) before making changes to core rewrite or upsert logic.
- **Add `OPENIVM_DEBUG_PRINT` statements** at major code flow points (entry/exit of rewrite rules, upsert compilation, cost model decisions). Use the existing `OPENIVM_DEBUG_PRINT` macro from `src/include/core/openivm_debug.hpp` — it's compiled out when `OPENIVM_DEBUG` is 0.

## What is OpenIVM?

OpenIVM is a DuckDB extension for **Incremental View Maintenance (IVM)**. It lets users create materialized views with `CREATE MATERIALIZED VIEW` and automatically maintain them when underlying tables change, without recomputing the entire view from scratch. Changes are tracked in delta tables and applied incrementally via `PRAGMA ivm('view_name')`.

## Build & Test

```bash
GEN=ninja make                # build (release)
make test                     # run all tests

# single test
build/release/test/unittest "test/sql/ivm_join.test"
```

Build outputs go to `build/release/`. DuckDB is a git submodule in `third_party/duckdb/`.

## Architecture

OpenIVM operates in two phases:

### Phase 1: View Creation (Parser + Optimizer Extensions)

When `CREATE MATERIALIZED VIEW <name> AS <query>` is parsed:

1. **Parser extension** (`src/core/openivm_parser.cpp`) intercepts the statement
2. Creates delta tables (`delta_<table>`) for each source table with extra columns: `_duckdb_ivm_multiplicity` (BOOLEAN) and `_duckdb_ivm_timestamp` (TIMESTAMP)
3. Stores view metadata in system tables (`_duckdb_ivm_views`, `_duckdb_ivm_delta_tables`)
4. Rewrites aggregate functions (e.g., adds COUNT alongside SUM for correct incremental maintenance)
5. Creates a regular DuckDB view with the rewritten query

### Phase 2: Incremental Refresh (`PRAGMA ivm()`)

When `PRAGMA ivm('view_name')` is called:

1. **Optimizer rewrite rules** transform the view's logical plan to read from delta tables instead of base tables
2. **Upsert compilation** generates SQL to apply the deltas to the materialized view
3. **Cost model** (when `ivm_adaptive_refresh = true`) decides whether IVM or full recompute is cheaper

### Operator-Specific Rewrite Rules

`src/rules/openivm_rewrite_rule.cpp` dispatches based on operator type:

| Operator | Rule Class | File |
|---|---|---|
| Table Scan | `IvmScanRule` | `src/rules/ivm_scan_rule.cpp` |
| Inner Join | `IvmJoinRule` | `src/rules/ivm_join_rule.cpp` |
| Projection | `IvmProjectionRule` | `src/rules/ivm_projection_rule.cpp` |
| Aggregate | `IvmAggregateRule` | `src/rules/ivm_aggregate_rule.cpp` |
| Filter | `IvmFilterRule` | `src/rules/ivm_filter_rule.cpp` |

All rules inherit from `IvmRule` (`src/include/rules/ivm_rule.hpp`).

### Join Delta Rule (Inclusion-Exclusion)

For N tables, the join rule generates 2^N - 1 terms using inclusion-exclusion:
- For each non-empty subset S of tables: replace tables in S with their delta scans, keep others as current base table scans
- Combined multiplicity = XOR of all delta multiplicities (XOR maps ±1 sign to boolean)
- All terms are combined with UNION ALL
- Terms are pruned by FK-aware optimization and empty-delta skipping

For DuckLake tables, the join rule uses N-term telescoping instead (exactly N terms, using snapshot-based time travel). See `docs/ducklake.md`.

INNER, LEFT/RIGHT, and FULL OUTER joins are supported. LEFT JOIN aggregates use group-recompute. FULL OUTER projection views use bidirectional key-based partial recompute (Zhang & Larson); FULL OUTER aggregates use full recompute (MERGE via `ivm_full_outer_merge` planned). Max 16 tables (`ivm::MAX_JOIN_TABLES`).

### Upsert Compilation

Three compilation paths based on view type (`IVMType`):

| View Type | Function | Strategy |
|---|---|---|
| `AGGREGATE_GROUP` | `CompileAggregateGroups()` | CTE consolidates delta per group, MERGE INTO updates/inserts |
| `SIMPLE_AGGREGATE` | `CompileSimpleAggregates()` | Single CTE consolidates all columns, UPDATE adds deltas |
| `SIMPLE_PROJECTION` | `CompileProjectionsFilters()` | Counting-based consolidation (GROUP BY + generate_series/rowid for bag semantics) |

MIN/MAX/AVG use group-recompute: delete affected groups, re-insert from original query.

### Key Architectural Rules

- **Multiplicity column** (`_duckdb_ivm_multiplicity`): BOOLEAN — `true` = insert, `false` = delete
- **Delta tables** are named `delta_<base_table>` and include a timestamp column for tracking when changes occurred
- **LPTS** (LogicalPlanToString, in `third_party/lpts/`) converts logical plans back to SQL for compilation
- **Cost model** (`src/upsert/openivm_cost_model.cpp`) compares estimated IVM cost vs full recompute cost. Includes filter selectivity estimation and learned regression from execution history (when `ivm_adaptive_refresh` is enabled)

## Key Source Files

- `src/openivm_extension.cpp` — extension entry point, registers pragmas and optimizer rules
- `src/core/openivm_parser.cpp` — parser extension for `CREATE MATERIALIZED VIEW`
- `src/core/openivm_metadata.cpp` — system table queries (view definitions, delta tables, timestamps)
- `src/core/openivm_utils.cpp` — SQL string manipulation, file I/O, table name extraction
- `src/rules/openivm_rewrite_rule.cpp` — rule dispatcher (routes to operator-specific rules)
- `src/rules/openivm_insert_rule.cpp` — adds INSERT operators to inject deltas into views
- `src/rules/ivm_join_rule.cpp` — join incrementalization (inclusion-exclusion)
- `src/rules/ducklake_join.cpp` — DuckLake N-term telescoping join rule
- `src/rules/ivm_helpers.cpp` — shared rule utilities (CreateDeltaGetNode)
- `src/upsert/openivm_upsert.cpp` — PRAGMA ivm() handler, orchestrates delta computation
- `src/upsert/openivm_compile_upsert.cpp` — generates upsert SQL queries
- `src/upsert/openivm_cost_model.cpp` — IVM vs recompute cost estimation
- `src/upsert/openivm_index_regen.cpp` — table index renumbering for plan copies
- `src/core/openivm_refresh_locks.cpp` — per-view and per-delta-table mutexes for concurrent refresh safety
- `src/core/openivm_refresh_daemon.cpp` — background thread for automatic REFRESH EVERY

## Configuration Options

| Setting | Type | Default | Description |
|---|---|---|---|
| `ivm_refresh_mode` | VARCHAR | `"incremental"` | `"incremental"`, `"full"`, or `"auto"` |
| `ivm_adaptive_refresh` | BOOLEAN | `false` | Enable adaptive cost model (learned regression + IVM vs recompute decision) |
| `ivm_cascade_refresh` | VARCHAR | `"downstream"` | Cascade mode: `"off"`, `"upstream"`, `"downstream"`, `"both"` |
| `ivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh exceeds interval |
| `ivm_disable_daemon` | BOOLEAN | `false` | Disable the background refresh daemon |
| `ivm_files_path` | VARCHAR | — | Path for compiled query reference files |
| `ivm_cost_decay` | DOUBLE | `0.9` | Decay factor for learned cost model regression (0.0–1.0) |
| `ivm_skip_empty_deltas` | BOOLEAN | `true` | Skip refresh or join terms when deltas are empty |
| `ivm_ducklake_nterm` | BOOLEAN | `true` | N-term telescoping for DuckLake joins (vs 2^N-1) |
| `ivm_fk_pruning` | BOOLEAN | `true` | Prune inclusion-exclusion join terms using FK constraints |
| `ivm_skip_aggregate_delete` | BOOLEAN | `true` | Skip DELETE for insert-only aggregate deltas |
| `ivm_skip_projection_delete` | BOOLEAN | `true` | Skip DELETE/consolidation for insert-only projection deltas |
| `ivm_minmax_incremental` | BOOLEAN | `true` | Use GREATEST/LEAST for MIN/MAX when deltas are insert-only |
| `ivm_having_merge` | BOOLEAN | `true` | Use MERGE for HAVING views instead of group-recompute |
| `ivm_full_outer_merge` | BOOLEAN | `false` | Use MERGE for FULL OUTER JOIN aggregates (Zhang & Larson) instead of full recompute |

Views using unsupported constructs (RANDOM(), STDDEV, etc.) are automatically classified as `FULL_REFRESH` — no setting needed.

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
PRAGMA ivm('product_sales');

-- Check refresh status (interval, last refresh, next refresh)
PRAGMA ivm_status('product_sales');

-- Check cost estimate (IVM vs full recompute)
PRAGMA ivm_cost('product_sales');

-- Replace an existing MV with a new definition
CREATE OR REPLACE MATERIALIZED VIEW product_sales REFRESH EVERY '10 minutes' AS
  SELECT product_name, SUM(amount) as total, COUNT(*) as cnt, AVG(amount) as avg_amount
  FROM orders GROUP BY product_name;

-- Force full recompute
SET ivm_refresh_mode = 'full';
PRAGMA ivm('product_sales');
```

## Code style (clang-tidy)

The project uses clang-tidy with DuckDB's configuration (`.clang-tidy`). Key naming rules:

- **Classes/Enums**: `CamelCase` (e.g., `IvmJoinRule`, `IVMType`)
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

Run `make format-fix` to auto-format. Formatting runs automatically via hook after edits.
