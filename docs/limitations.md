# Limitations

Materialized views can be created using any SQL construct. Unsupported operators
automatically fall back to full refresh (the entire view is recomputed from scratch
on each `PRAGMA ivm()` call). This page consolidates all known limitations.

The IVM-compatibility check lives in `src/core/ivm_checker.cpp` (`AnalyzeNode`);
anything it flags as `ivm_compatible = false` routes to `IVMType::FULL_REFRESH`.

## Constructs that trigger full refresh

### Aggregate forms

| Construct | Why |
|---|---|
| `COUNT(DISTINCT x)`, `SUM(DISTINCT x)`, `AVG(DISTINCT x)`, any `DISTINCT`-variant aggregate | A delta row's value may already be present in the MV (no change) or new (+1) — requires auxiliary per-value state we don't maintain. Detected via `BoundAggregateExpression::IsDistinct()`. |
| `<agg>(...) FILTER (WHERE predicate)` | The filter interacts with delta rows in ways the `sum(case mult=false then -col else col end)` formula can't reproduce. Detected via `BoundAggregateExpression::filter != nullptr`. |
| `GROUPING SETS`, `CUBE`, `ROLLUP` | Our delta pipeline groups once; can't emit the cross-grouped subtotal rows. Detected via `LogicalAggregate::grouping_sets.size() > 1`. Also note: LPTS doesn't round-trip the ROLLUP annotation, so for these views the parser substitutes the user's original SQL for both initial populate and recompute. |
| Aggregates not in `SUPPORTED_AGGREGATES` (BOOL_OR, BOOL_AND, STRING_AGG, LISTAGG, GROUP_CONCAT, MEDIAN, percentiles, APPROX_*, QUANTILE_*, ANY_VALUE, ARG_MAX, ARG_MIN, …) | Order-dependent, holistic, or non-decomposable; no known delta formula. Supported set: `count_star, count, sum, min, max, avg, list, stddev, stddev_samp, stddev_pop, variance, var_samp, var_pop`. |

### Operators

| Construct | Why |
|---|---|
| Recursive CTEs | Semi-naive evaluation not yet implemented. |
| Unusual join types (SEMI, ANTI, MARK, etc. — anything other than INNER/LEFT/RIGHT/FULL OUTER) | No delta rewrite rule. |
| `LIMIT` without deterministic `ORDER BY` (or with ties on the ORDER BY key) | Row selection is non-deterministic between MV creation and recompute — the MV and the base query can legitimately return different subsets, so recompute and the `EXCEPT ALL` verify diverge. Not a code bug; add a unique `ORDER BY` to make the view deterministic. |
| Any operator the plan walk doesn't recognize (falls into the `default:` branch of the compatibility check) | Conservatively treated as unsupported until a rewrite rule lands. |

### Expressions

| Construct | Why |
|---|---|
| `RANDOM()`, `UUID()`, `NOW()` (when evaluated per-row), any function with `FunctionStability::VOLATILE` or `NON_DETERMINISTIC` | Result depends on evaluation time, so the MV snapshot cannot equal a fresh recompute. Detected via `HasVolatileExpression` on filter / projection / union / distinct / aggregate nodes. |
| Non-additive scalar expression above an aggregate that references aggregate output (e.g. `CASE WHEN SUM(x) > 1000 THEN 'big' ELSE 'small' END`, string concat of aggregate results) | Can't sum deltas of the scalar, can't pass through either (value depends on merged aggregate state). The compiler triggers group-recompute for any non-summable non-key column — see below. |

## Constructs that trigger partial recompute (not full refresh, but not full incremental either)

| Construct | Strategy |
|---|---|
| Window functions (`ROW_NUMBER`, `RANK`, `NTILE`, `LAG`, `LEAD`, …) on a single table | Partition-recompute: only partitions with delta rows are re-evaluated. See [operators/window-functions.md](operators/window-functions.md). **Caveat**: NTILE / RANK / ROW_NUMBER with ties on the `ORDER BY` key are inherently non-deterministic — multiple recomputes of the same data may legitimately produce different bucket / rank assignments. |
| Window functions over JOINs | Full recompute (partition columns may come from a joined table whose delta doesn't carry them, so we can't identify affected partitions). |
| `LEFT JOIN` / `RIGHT JOIN` aggregates | Group-recompute for affected groups (the NULL-padding produced by the outer join for unmatched rows doesn't compose with delta MERGE). See [operators/left-join.md](operators/left-join.md). |
| `FULL OUTER JOIN` projection views | Bidirectional key-based partial recompute (Zhang & Larson). |
| `FULL OUTER JOIN` aggregate views | Group-recompute for affected groups. `ivm_full_outer_merge = true` (MERGE-based strategy) is planned. |

## Join limitations

- Maximum **16 tables** in a single join (inclusion-exclusion bitmask limit — `ivm::MAX_JOIN_TABLES`).
- `CROSS JOIN` is supported (treated as a join with no condition).
- Partial-recompute strategies for `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN` are documented in the partial-recompute table above.

## DuckLake-specific limitations

- **No FK constraints.** DuckLake does not support `FOREIGN KEY` constraints, so
  [FK-aware pruning](optimizations/fk-aware-pruning.md) is not available.
- **No ART indexes.** DuckLake does not support DuckDB-native index types.
  Group column identification uses metadata instead.
- **Single catalog.** All base tables must be in the same DuckLake catalog.

## Supported aggregates and their maintenance strategy

| Aggregate | Incremental | Strategy |
|---|---|---|
| `SUM` | Yes | Delta addition via MERGE. |
| `COUNT`, `COUNT(*)` | Yes | Delta addition via MERGE. |
| `AVG` | Yes (1–2 ULP drift on DECIMAL) | Decomposed to hidden SUM + COUNT; the visible AVG column is recomputed from the merged totals. DuckDB's native `AVG(DECIMAL)` uses internal compensated arithmetic that no `SUM/COUNT` decomposition reproduces bit-exactly — MV values can differ from the base query in the last 1–2 ULPs (e.g. `47.989999999999994884` vs `47.99000000000000199`). Results are semantically correct; the rewriter benchmark verifies DOUBLE/FLOAT columns with `printf('%.12g', col)` (12 significant digits). Unit tests in `test/sql/` stay strict — use a `DOUBLE`/`INT` base column or cast inside the aggregate (`AVG(val::DOUBLE)`) when writing tests that exercise AVG on DECIMAL. |
| `STDDEV`, `VARIANCE`, `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP` | Yes (same 1–2 ULP drift as AVG) | Decomposed to hidden SUM + SUM(x*x) + COUNT; variance recomputed post-MERGE. The MERGE formula wraps in `CASE WHEN count > threshold` (sample: 1, population: 0) and clamps `GREATEST(var, 0::DOUBLE)` before `sqrt`, matching the CREATE-MV formula and preventing sqrt-of-negative crashes when floating-point reassociation drifts a flat-valued group's variance below zero. |
| `MIN`, `MAX` | Partial | Insert-only deltas for a group: incremental via `GREATEST`/`LEAST`. Any delete touching a group: group-recompute (delete the affected groups from the data table, re-insert from the view query). |
| `LIST`, `LIST(x ORDER BY y)` | Yes (via group-recompute) | Lists aren't element-wise summable (different deltas produce different lengths), so affected groups are recomputed from the view query. The intra-aggregate `ORDER BY` is preserved by LPTS. |
| Any visible MV column with a non-summable type (VARCHAR literal, UPPER(group_col), CASE over aggregate, BOOLEAN predicate on aggregate, LIST) | Yes (via group-recompute) | The delta `sum(case mult=false then -col else col end)` formula can't type-check for these columns; detected in `CompileAggregateGroups` via the delta-view column types and routed to group-recompute. Unified path with `LIST` and `MIN`/`MAX` above. |
| `HAVING` | Partial | Group-recompute for affected groups; `ivm_having_merge=true` (default) uses the MERGE path when the stored data table holds all groups. |

## Other limitations

- **DROP MATERIALIZED VIEW** is not fully implemented. Dropping the view via `DROP VIEW`
  removes the view but leaves behind the data table, delta tables, and metadata entries.
  Use `DROP TABLE` on the data table and delta tables manually, and clean up
  `_duckdb_ivm_delta_tables` and `_duckdb_ivm_views`.

- **Schema evolution** is supported for `ADD COLUMN`, `DROP COLUMN`, and `RENAME COLUMN`
  on base tables. `ALTER COLUMN TYPE` is not handled. Dropping or renaming a column
  referenced by an MV is blocked with an error.

- **Transaction isolation during refresh** uses a separate Connection with snapshot
  isolation. Concurrent DML during refresh does not affect the in-progress refresh, but
  the interaction has not been exhaustively audited.

- **Window functions over DuckLake with non-output partition keys or multi-table inputs**
  fall back to full recompute (`DELETE FROM data; INSERT INTO data SELECT * FROM view_query`).
  Partition-level recompute (delete + re-insert only affected partitions via DuckLake
  snapshot-diff) is used when all partition columns appear in the MV's output and there
  is a single base table. When the PARTITION BY column is dropped by the outer SELECT
  (e.g. `WITH cte AS (… PARTITION BY a, b, c …) SELECT a, c, … FROM cte` where `b` is
  projected out), the outer `WHERE <b> IN (…)` can't resolve the column, so we can't
  identify affected rows in the data table. Similarly for `UNION ALL` / multi-base-table
  windows. A proper incremental fix requires rewriting the view query to push the
  partition filter into the base scan; not yet implemented.
