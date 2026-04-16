# Limitations

Materialized views can be created using any SQL construct. Unsupported operators
automatically fall back to full refresh (the entire view is recomputed from scratch
on each `PRAGMA ivm()` call). This page consolidates all known limitations.

## Operators that trigger full refresh

| Construct | Status | Notes |
|---|---|---|
| `STDDEV`, `VARIANCE` | Incremental | Decomposed to SUM + SUM(x^2) + COUNT |
| `COUNT(DISTINCT x)` | Full refresh | Requires auxiliary per-value tracking |
| Window functions (`ROW_NUMBER`, `RANK`, etc.) | Partition recompute | [Single-table: partition recompute. Over JOIN: full recompute](operators/window-functions.md) |
| `FULL OUTER JOIN` | Not supported | View creation fails |
| `GROUPING SETS`, `CUBE`, `ROLLUP` | Full refresh | Decomposable to UNION ALL of GROUP BYs |
| Recursive CTEs | Full refresh | Semi-naive evaluation not yet implemented |
| `RANDOM()`, `UUID()`, non-deterministic functions | Full refresh | Result depends on evaluation time |
| `STRING_AGG`, `LISTAGG`, `GROUP_CONCAT` | Full refresh | Order-dependent, non-decomposable |
| `MEDIAN`, percentiles | Full refresh | Holistic aggregates, require full group state |

## Join limitations

- Maximum **16 tables** in a single join (inclusion-exclusion bitmask limit)
- `FULL OUTER JOIN` is not supported (view creation fails)
- `CROSS JOIN` is supported (treated as a join with no condition)
- For [LEFT/RIGHT JOIN](operators/left-join.md), aggregates use group-recompute instead of
  incremental MERGE

## DuckLake-specific limitations

- **No FK constraints.** DuckLake does not support `FOREIGN KEY` constraints, so
  [FK-aware pruning](optimizations/fk-aware-pruning.md) is not available.
- **No ART indexes.** DuckLake does not support DuckDB-native index types.
  Group column identification uses metadata instead.
- **Single catalog.** All base tables must be in the same DuckLake catalog.

## Aggregate handling

| Aggregate | Incremental | Strategy |
|---|---|---|
| `SUM` | Yes | Delta addition via MERGE |
| `COUNT`, `COUNT(*)` | Yes | Delta addition via MERGE |
| `AVG` | Yes | Decomposed to hidden SUM + COUNT; recomputed post-MERGE |
| `MIN`, `MAX` | Partial | Insert-only groups: incremental. Groups with deletes: recompute affected groups |
| `LIST` (numeric) | Yes | Element-wise list operations |
| `HAVING` | Partial | Group-recompute for affected groups |

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
