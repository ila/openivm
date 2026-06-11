# Limitations

Materialized views can be created using any SQL construct. Unsupported operators
automatically fall back to full refresh (the entire view is recomputed from scratch
on each `PRAGMA refresh()` call). This page consolidates all known limitations.

At view-creation time OpenIVM analyzes the query plan; anything it cannot maintain
incrementally is routed to full refresh.

## Constructs that trigger full refresh

### Aggregate forms

| Construct | Why |
|---|---|
| Aggregates outside the supported set (`STRING_AGG`, `LISTAGG`, `GROUP_CONCAT`, `MEDIAN`, percentiles, `APPROX_*`, `QUANTILE_*`, `ANY_VALUE`, …) | Order-dependent, holistic, or non-decomposable; no known delta formula. Supported set: `COUNT(*)`, `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `LIST`, `STDDEV` (all variants), `VARIANCE` (all variants), `BOOL_AND`, `BOOL_OR`, `ARG_MIN`, and `ARG_MAX`. |
| Correlation / regression aggregates: `CORR`, `COVAR_POP`, `COVAR_SAMP`, `REGR_*` | These aggregates cannot be serialized back to SQL for the delta plan, so they fall back to full refresh. |

> **Note:** `DISTINCT`-variant aggregates (`COUNT(DISTINCT x)`, `SUM(DISTINCT x)`,
> `AVG(DISTINCT x)`, including the `FILTER (WHERE …)` form) and multi-level groupings
> (`GROUPING SETS`, `ROLLUP`, `CUBE`) are **no longer full refresh** — they are maintained by
> affected-group recompute (see the partial-recompute table below and
> [Count distinct](operators/count-distinct.md) / [Grouping sets](operators/grouping-sets.md)).

### Operators

| Construct | Why |
|---|---|
| Recursive CTEs | Semi-naive evaluation not yet implemented. |
| `MARK` joins and semi/anti shapes outside the supported set | SQL NULL-aware membership and complex correlated shapes need state OpenIVM does not maintain. Supported `SEMI JOIN`, `ANTI JOIN`, `EXISTS`, and `NOT EXISTS` projection shapes use the aux-state path; see [Semi and anti join](operators/semi-anti-join.md). |
| `LIMIT` without deterministic `ORDER BY` (or with ties on the ORDER BY key) | Row selection is non-deterministic between MV creation and recompute — the MV and the base query can legitimately return different subsets, so recompute and the `EXCEPT ALL` verify diverge. Not a code bug; add a unique `ORDER BY` to make the view deterministic. |
| Any operator the plan analysis doesn't recognize | Conservatively treated as unsupported until a maintenance rule lands. |
| Any plan that can't be serialized back to SQL for the delta computation | Caught at refresh-plan compile time; OpenIVM falls back to full recompute. Subsumes ordered-set aggregates and a few other corner cases — covered without per-construct enumeration. |

Note: **`ORDER BY` + `LIMIT k`** (top-k) is supported — see the partial-recompute table below and [operators/top-k.md](operators/top-k.md).

### Expressions

| Construct | Why |
|---|---|
| `RANDOM()`, `UUID()`, `NOW()` (when evaluated per-row), any volatile / non-deterministic function | Result depends on evaluation time, so the MV snapshot cannot equal a fresh recompute. |
| Non-additive scalar expression above an aggregate that references aggregate output (e.g. `CASE WHEN SUM(x) > 1000 THEN 'big' ELSE 'small' END`, string concat of aggregate results) | Can't sum deltas of the scalar, can't pass it through either (value depends on merged aggregate state). Such views use affected-group recompute — see below. |

## Constructs that trigger partial recompute (not full refresh, but not full incremental either)

These are maintained incrementally in the sense that a refresh touches only the part of the view
affected by the change — but they re-derive that affected scope from the current data rather than
applying a closed-form additive delta.

| Construct | Strategy |
|---|---|
| `COUNT(DISTINCT)`, `SUM(DISTINCT)`, `AVG(DISTINCT)` (grouped) | Affected-group recompute: only the groups whose rows changed are re-derived. A distinct aggregate can't be folded additively (a repeated value must not change the result), so the affected groups are recomputed exactly. See [Count distinct](operators/count-distinct.md). |
| `GROUPING SETS`, `ROLLUP`, `CUBE` | Affected-group recompute: each changed source row touches one row at each grouping level it participates in; only those grouping-set rows are re-derived. See [Grouping sets](operators/grouping-sets.md). |
| `ASOF JOIN`, `POSITIONAL JOIN` | Affected-row recompute: matching is order-/position-sensitive, so the keys (or positions) touched by a change are re-evaluated against current data and replaced. See [ASOF join](operators/asof-join.md) and [Positional join](operators/positional-join.md). |
| `GROUP BY … ORDER BY col LIMIT k` (aggregate top-k) | Genuinely incremental: all groups are maintained in the data table; `ORDER BY … LIMIT k` is applied by the user-facing view at read time. Empty-delta skip avoids any work when no rows changed. See [operators/top-k.md](operators/top-k.md). |
| `SELECT cols … ORDER BY col LIMIT k` (projection top-k, no GROUP BY) | Incremental maintenance of the unlimited projection; the user-facing view applies `ORDER BY … LIMIT k` at read time. See [operators/top-k.md](operators/top-k.md). |
| `UNION ALL` over per-branch aggregates | Affected-group recompute keyed by branch group keys when derivable; otherwise current-diff recompute keyed by the visible output row. |
| Inner `DISTINCT` directly inside a subquery feeding an outer aggregate (e.g. `SELECT g, SUM(c) FROM (SELECT DISTINCT g, m, c FROM t) GROUP BY g`) | Two paths. **Default**: affected-group recompute — for each base table with a non-empty delta, the view query is scoped to delta-touched rows and only the affected group keys are deleted and reinserted. Correct, but does more work than strictly necessary. **Aux-state path** (`openivm_distinct_aux_state = true`): DBSP-correct minimal maintenance via a per-tuple count auxiliary table — a distinct row fires a change only when its input count crosses zero, driving ±1 into the parent SUM/COUNT. Single base table and a single SUM aggregate only in v0; other shapes use affected-group recompute. |
| `LATERAL` / correlated subquery shapes | Affected-key recompute: visible correlated output columns are used as recompute keys, then only those keys are deleted and reinserted from the view query. Supports correlated aggregate lateral shapes and scalar correlated subqueries. Correct and incremental, but less precise than a fully algebraic correlated delta. |
| Non-additive scalar over aggregate output | Affected-group recompute, since the scalar value can't be summed from deltas. |
| Window functions (`ROW_NUMBER`, `RANK`, `NTILE`, `LAG`, `LEAD`, …) on a single table | Partition recompute: only partitions with delta rows are re-evaluated. See [operators/window-functions.md](operators/window-functions.md). **Caveat**: NTILE / RANK / ROW_NUMBER with ties on the `ORDER BY` key are inherently non-deterministic — multiple recomputes of the same data may legitimately produce different bucket / rank assignments. |
| Window functions over JOINs | Partition recompute when affected partition values can be derived from source lineage; otherwise full recompute. |
| `LEFT JOIN` / `RIGHT JOIN` aggregates | Incremental MERGE for supported aggregate shapes (`openivm_left_join_merge=true`, default); affected-group recompute fallback for shapes where NULL-padding does not compose with delta MERGE. See [operators/left-join.md](operators/left-join.md). |
| `FULL OUTER JOIN` projection views | Bidirectional key-based partial recompute (Zhang & Larson). |
| `FULL OUTER JOIN` aggregate views | MERGE plus targeted recompute for unmatched changes (`openivm_full_outer_merge=true`, default); affected-group recompute fallback when the setting is disabled or the aggregate shape is unsafe. |

## Join limitations

- Maximum **16 tables** in a single join (inclusion-exclusion bitmask limit).
- `INNER JOIN`, `CROSS JOIN`, and arbitrary-predicate joins use the inclusion-exclusion delta rule. `CROSS JOIN` is treated as a join with no condition.
- Partial-recompute strategies for `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN` are documented in the partial-recompute table above.
- `SEMI JOIN`, `ANTI JOIN`, `EXISTS`, and `NOT EXISTS` are incrementally maintained only for the projection/filter shapes documented in [Semi and anti join](operators/semi-anti-join.md). Aggregates over semi/anti output, join-chain inputs, and `IN`/`NOT IN` membership semantics fall back to full refresh.

## DuckLake-specific limitations

- **No FK constraints.** DuckLake does not support `FOREIGN KEY` constraints, so
  [FK-aware pruning](optimizations/fk-aware-pruning.md) is not available.
- **No native indexes.** DuckLake does not support DuckDB-native index types.
  Group column identification uses metadata instead.
- **Single catalog.** All base tables must be in the same DuckLake catalog.
- **Structural changes force a full refresh.** Schema-level changes to a DuckLake source
  (e.g. `ALTER`/`DROP` of a referenced table or column) can't be expressed as a row-level
  delta, so the next refresh recomputes the view in full. See [DuckLake](ducklake.md).

## Supported aggregates and their maintenance strategy

| Aggregate | Incremental | Strategy |
|---|---|---|
| `SUM` | Yes | Delta addition via MERGE. |
| `COUNT`, `COUNT(*)` | Yes | Delta addition via MERGE. |
| `AVG` | Yes (1–2 ULP drift on DECIMAL) | Decomposed to hidden SUM + COUNT; the visible AVG column is recomputed from the merged totals. DuckDB's native `AVG(DECIMAL)` uses internal compensated arithmetic that no `SUM/COUNT` decomposition reproduces bit-exactly — MV values can differ from the base query in the last 1–2 ULPs (e.g. `47.989999999999994884` vs `47.99000000000000199`). Results are semantically correct; the rewriter benchmark verifies floating-point columns to 12 significant digits. Unit tests stay strict — use a `DOUBLE`/`INT` base column or cast inside the aggregate (`AVG(val::DOUBLE)`) when writing tests that exercise AVG on DECIMAL. `AVG` inside a CTE that is then joined to another relation is also fully incremental. |
| `STDDEV`, `VARIANCE`, `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP` | Yes (same 1–2 ULP drift as AVG) | Decomposed to hidden SUM + SUM(x*x) + COUNT; variance recomputed after MERGE. The formula guards the count threshold (sample vs population) and clamps a near-zero variance to zero before `sqrt`, preventing sqrt-of-negative when floating-point reassociation drifts a flat-valued group's variance slightly below zero. Like `AVG`, fully supported inside nested CTE-then-join patterns. |
| `MIN`, `MAX` | Partial | Insert-only deltas for a group: incremental via `GREATEST`/`LEAST`. Any delete touching a group: affected-group recompute (delete the affected groups, re-insert from the view query). |
| `COUNT(DISTINCT)`, `SUM(DISTINCT)`, `AVG(DISTINCT)` | Partial | Affected-group recompute — a distinct aggregate can't be folded additively, so only the changed groups are re-derived. See [Count distinct](operators/count-distinct.md). |
| `BOOL_AND`, `BOOL_OR` | Yes (via affected-group recompute) | BOOLEAN is a non-summable type, so the affected groups are recomputed. Semantically: `BOOL_AND` = "no false values", `BOOL_OR` = "at least one true value". |
| `AGG(...) FILTER (WHERE p)` | Yes | Rewritten to `AGG(CASE WHEN p THEN arg END)` before plan analysis. All COUNT/SUM/AVG/MIN/MAX/STDDEV variants work; affected-group recompute is used when the rewritten aggregate makes the column non-summable (and for the `COUNT(DISTINCT …) FILTER` form). |
| `LIST`, `LIST(x ORDER BY y)` | Yes | Numeric fixed-shape list-valued outputs can use element-wise list arithmetic. `LIST(...) FILTER` and non-summable list shapes use affected-group recompute so DuckDB's NULL-element semantics are preserved. |
| Any visible MV column with a non-summable type (VARCHAR literal, `UPPER(group_col)`, CASE over aggregate, BOOLEAN predicate on aggregate, LIST) | Yes (via affected-group recompute) | The delta-addition formula can't type-check for these columns, so the affected groups are recomputed. Unified path with `LIST` and `MIN`/`MAX` above. |
| `HAVING` | Partial | Affected-group recompute; with `openivm_having_merge=true` (default) the stored data table holds all groups and the MERGE path is used, while the user-facing view applies the predicate. |

## Other limitations

- **DROP MATERIALIZED VIEW syntax** is not supported because DuckDB's parser intercepts
  it before the extension can rewrite it. Use `DROP VIEW <mv_name>` instead. OpenIVM
  cleans the user-facing view, data table, MV delta table, base delta tables that are no
  longer shared, and metadata rows.

- **Schema evolution** is supported for `ADD COLUMN`, `DROP COLUMN`, and `RENAME COLUMN`
  on base tables. `ALTER COLUMN TYPE` is not handled. Dropping a referenced column is
  blocked with an error. Renaming a referenced column rewrites stored MV SQL and refresh
  metadata.

- **Transaction isolation during refresh** uses a separate connection with snapshot
  isolation. Concurrent DML during refresh does not affect the in-progress refresh, but
  the interaction has not been exhaustively audited.

- **Window functions over DuckLake with non-output partition keys or unsupported lineage
  shapes** fall back to full recompute. Partition-level recompute is used when changed
  partition values can be derived from DuckLake snapshot diffs and lineage metadata. When
  the PARTITION BY column is dropped by the outer SELECT (e.g. `WITH cte AS (… PARTITION BY
  a, b, c …) SELECT a, c, … FROM cte` where `b` is projected out), the outer filter can't
  resolve the column, so OpenIVM can't identify affected rows in the data table. A proper
  incremental fix requires rewriting the view query to push the partition filter into the
  base scan; not yet implemented.
