# Limitations

Materialized views can be created using any SQL construct. Unsupported operators
automatically fall back to full refresh (the entire view is recomputed from scratch
on each `PRAGMA refresh()` call). This page consolidates all known limitations.

The IVM-compatibility check lives in `src/core/incremental_checker.cpp` (`AnalyzeNode`);
anything it flags as `incremental_compatible = false` routes to `RefreshType::FULL_REFRESH`.

## Constructs that trigger full refresh

### Aggregate forms

| Construct | Why |
|---|---|
| `COUNT(DISTINCT x)`, `SUM(DISTINCT x)`, `AVG(DISTINCT x)`, any `DISTINCT`-variant aggregate | A delta row's value may already be present in the MV (no change) or new (+1) — requires auxiliary per-value state we don't maintain. Detected via `BoundAggregateExpression::IsDistinct()`. |
| `<agg>(...) FILTER (WHERE predicate)` | Rewritten to `AGG(CASE WHEN p THEN arg END)` by `RewriteAggregateFilters` before the checker sees the plan. Fully incremental — **no full refresh**. Exception: `COUNT(DISTINCT x) FILTER (WHERE p)` still triggers full refresh (DISTINCT not supported). |
| `GROUPING SETS`, `CUBE`, `ROLLUP` | Our delta pipeline groups once; can't emit the cross-grouped subtotal rows. Detected via `LogicalAggregate::grouping_sets.size() > 1`. Also note: LPTS doesn't round-trip the ROLLUP annotation, so for these views the parser substitutes the user's original SQL for both initial populate and recompute. |
| Aggregates not in `SUPPORTED_AGGREGATES` (STRING_AGG, LISTAGG, GROUP_CONCAT, MEDIAN, percentiles, APPROX_*, QUANTILE_*, ANY_VALUE, …) | Order-dependent, holistic, or non-decomposable; no known delta formula. Supported set: `count_star`, `count`, `sum`, `min`, `max`, `avg`, `list`, `stddev`, `stddev_samp`, `stddev_pop`, `variance`, `var_samp`, `var_pop`, `bool_and`, `bool_or`, `arg_min`, and `arg_max`. |
| Correlation / regression aggregates: `CORR`, `COVAR_POP`, `COVAR_SAMP`, `REGR_*` (`REGR_AVGX`, `REGR_AVGY`, `REGR_COUNT`, `REGR_INTERCEPT`, `REGR_R2`, `REGR_SLOPE`, `REGR_SXX`, `REGR_SXY`, `REGR_SYY`) | LPTS does not round-trip these aggregates back to SQL, so the delta plan can't be serialised. Routed to FULL_REFRESH. |
| `HAVING` referencing `IS NULL` / `IS NOT NULL` / a `BOUND_AGGREGATE` directly | Detected by the HAVING rewriter; treated as group-recompute (same path as other partial-recompute HAVING views). |

### Operators

| Construct | Why |
|---|---|
| Recursive CTEs | Semi-naive evaluation not yet implemented. |
| `MARK` joins and semi/anti shapes outside the aux-state extractor | SQL NULL-aware membership and complex correlated shapes need state OpenIVM does not maintain. Supported `SEMI JOIN`, `ANTI JOIN`, `EXISTS`, and `NOT EXISTS` projection shapes use the aux-state path; see [Semi and anti join](operators/semi-anti-join.md). |
| `LIMIT` without deterministic `ORDER BY` (or with ties on the ORDER BY key) | Row selection is non-deterministic between MV creation and recompute — the MV and the base query can legitimately return different subsets, so recompute and the `EXCEPT ALL` verify diverge. Not a code bug; add a unique `ORDER BY` to make the view deterministic. |
| Any operator the plan walk doesn't recognize (falls into the `default:` branch of the compatibility check) | Conservatively treated as unsupported until a rewrite rule lands. |
| Any plan that LPTS (`LogicalPlanToString`) can't serialise back to SQL | Caught at refresh-plan compile time; OpenIVM falls back to full recompute (`DELETE FROM data; INSERT INTO data SELECT * FROM view_query`). Subsumes ordered-set aggregates and a few other corner cases — covered without per-construct enumeration. |

Note: **`ORDER BY` + `LIMIT k`** (top-k) is now supported — see the partial-recompute table below and [operators/top-k.md](operators/top-k.md).

### Expressions

| Construct | Why |
|---|---|
| `RANDOM()`, `UUID()`, `NOW()` (when evaluated per-row), any function with `FunctionStability::VOLATILE` or `NON_DETERMINISTIC` | Result depends on evaluation time, so the MV snapshot cannot equal a fresh recompute. Detected via `HasVolatileExpression` on filter / projection / union / distinct / aggregate nodes. |
| Non-additive scalar expression above an aggregate that references aggregate output (e.g. `CASE WHEN SUM(x) > 1000 THEN 'big' ELSE 'small' END`, string concat of aggregate results) | Can't sum deltas of the scalar, can't pass through either (value depends on merged aggregate state). The compiler triggers group-recompute for any non-summable non-key column — see below. |

## Constructs that trigger partial recompute (not full refresh, but not full incremental either)

| Construct | Strategy |
|---|---|
| `GROUP BY … ORDER BY col LIMIT k` (aggregate top-k) | Genuinely incremental O(D + G): all groups are maintained in `openivm_data_<view>` via the normal `AGGREGATE_GROUP` path; `ORDER BY … LIMIT k` is applied by the VIEW at read time. Empty-delta skip avoids any work when no rows changed. See [operators/top-k.md](operators/top-k.md). |
| `SELECT cols … ORDER BY col LIMIT k` (projection top-k, no GROUP BY) | Incremental maintenance over the unlimited `SIMPLE_PROJECTION` result. The user-facing view applies `ORDER BY … LIMIT k` at read time. See [operators/top-k.md](operators/top-k.md). |
| `UNION ALL` over per-branch aggregates | Uses `GROUP_RECOMPUTE` when branch keys are derivable. If there is no reliable single group-key set, OpenIVM uses current-diff group recompute keyed by the visible output row. |
| Inner `DISTINCT` directly inside a subquery feeding an outer `AGGREGATE` (e.g. `SELECT g, SUM(c) FROM (SELECT DISTINCT g, m, c FROM t) GROUP BY g`) | Two paths are available. **Default (`GROUP_RECOMPUTE`)**: for each base table with a non-empty delta, the LPTS view query is scoped to delta-touched rows; the affected-keys set drives `DELETE` + `INSERT` on `openivm_data_<view>`. Correct but does more work than strictly necessary. **Aux-state path (`openivm_distinct_aux_state = true`, `RefreshType::DISTINCT_INCREMENTAL`)**: DBSP-correct Z-set maintenance via a per-tuple count auxiliary table `openivm_distinct_count_<view>`. Δdistinct fires only when the input count crosses zero (`sgn(R[t])`), driving ±1 into the parent SUM/COUNT MERGE. Strictly minimal delta; only v0 (single base-table DISTINCT, single SUM aggregate). Multi-source DISTINCT demotes to `GROUP_RECOMPUTE`. |
| `LATERAL` / correlated subquery shapes represented as `DELIM_JOIN` / `DEPENDENT_JOIN` | Affected-key `GROUP_RECOMPUTE`: visible correlated output columns are used as recompute keys, then only those keys are deleted/reinserted from the view query. This supports correlated aggregate lateral shapes and scalar correlated subqueries planned as `SINGLE` `DELIM_JOIN`. It is correct and incremental, but less precise than a fully algebraic correlated delta. |
| Window functions (`ROW_NUMBER`, `RANK`, `NTILE`, `LAG`, `LEAD`, …) on a single table | Partition-recompute: only partitions with delta rows are re-evaluated. See [operators/window-functions.md](operators/window-functions.md). **Caveat**: NTILE / RANK / ROW_NUMBER with ties on the `ORDER BY` key are inherently non-deterministic — multiple recomputes of the same data may legitimately produce different bucket / rank assignments. |
| Window functions over JOINs | Partition recompute when affected partition values can be derived from source lineage; otherwise full recompute. |
| `LEFT JOIN` / `RIGHT JOIN` aggregates | Incremental MERGE for supported aggregate shapes when `openivm_left_join_merge=true`; group-recompute fallback for shapes where NULL-padding does not compose with delta MERGE. See [operators/left-join.md](operators/left-join.md). |
| `FULL OUTER JOIN` projection views | Bidirectional key-based partial recompute (Zhang & Larson). |
| `FULL OUTER JOIN` aggregate views | MERGE plus targeted recompute for unmatched changes when `openivm_full_outer_merge=true`; group-recompute fallback is available when the setting is disabled or the aggregate shape is unsafe. |

## Join limitations

- Maximum **16 tables** in a single join (inclusion-exclusion bitmask limit — `openivm::MAX_JOIN_TABLES`).
- `INNER JOIN`, `CROSS JOIN`, and arbitrary-predicate joins use the inclusion-exclusion delta rule. `CROSS JOIN` is treated as a join with no condition.
- Partial-recompute strategies for `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN` are documented in the partial-recompute table above.
- `SEMI JOIN`, `ANTI JOIN`, `EXISTS`, and `NOT EXISTS` are incrementally maintained only for the projection/filter shapes documented in [Semi and anti join](operators/semi-anti-join.md). Aggregates over semi/anti output, join-chain inputs, and `IN`/`NOT IN` membership semantics fall back to full refresh.

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
| `AVG` | Yes (1–2 ULP drift on DECIMAL) | Decomposed to hidden SUM + COUNT; the visible AVG column is recomputed from the merged totals. DuckDB's native `AVG(DECIMAL)` uses internal compensated arithmetic that no `SUM/COUNT` decomposition reproduces bit-exactly — MV values can differ from the base query in the last 1–2 ULPs (e.g. `47.989999999999994884` vs `47.99000000000000199`). Results are semantically correct; the rewriter benchmark verifies DOUBLE/FLOAT columns with `printf('%.12g', col)` (12 significant digits). Unit tests in `test/sql/` stay strict — use a `DOUBLE`/`INT` base column or cast inside the aggregate (`AVG(val::DOUBLE)`) when writing tests that exercise AVG on DECIMAL. `AVG` inside a CTE that is then joined to another relation is also fully incremental — `RewriteDerivedAggregates` descends into nested PROJECTION→AGGREGATE shapes. |
| `STDDEV`, `VARIANCE`, `STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP` | Yes (same 1–2 ULP drift as AVG) | Decomposed to hidden SUM + SUM(x*x) + COUNT; variance recomputed post-MERGE. The MERGE formula wraps in `CASE WHEN count > threshold` (sample: 1, population: 0) and clamps `GREATEST(var, 0::DOUBLE)` before `sqrt`, matching the CREATE-MV formula and preventing sqrt-of-negative crashes when floating-point reassociation drifts a flat-valued group's variance below zero. Like `AVG`, fully supported inside nested CTE-then-join patterns. |
| `MIN`, `MAX` | Partial | Insert-only deltas for a group: incremental via `GREATEST`/`LEAST`. Any delete touching a group: group-recompute (delete the affected groups from the data table, re-insert from the view query). |
| `BOOL_AND`, `BOOL_OR` | Yes (via group-recompute) | BOOLEAN is a non-summable type; detected in `CompileAggregateGroups` and routed to group-recompute. Z-set correct: `BOOL_AND` = `false_count = 0`, `BOOL_OR` = `true_count > 0`. |
| `AGG(...) FILTER (WHERE p)` | Yes | Rewritten by `RewriteAggregateFilters` to `AGG(CASE WHEN p THEN arg END)` before plan analysis. All COUNT/SUM/AVG/MIN/MAX/STDDEV variants work; group-recompute is used when the rewritten aggregate makes the column non-summable. |
| `LIST`, `LIST(x ORDER BY y)` | Yes | Numeric fixed-shape list-valued outputs can use element-wise list arithmetic. `LIST(...) FILTER` and non-summable list shapes use affected-group recompute so DuckDB's NULL-element semantics are preserved. |
| Any visible MV column with a non-summable type (VARCHAR literal, UPPER(group_col), CASE over aggregate, BOOLEAN predicate on aggregate, LIST) | Yes (via group-recompute) | The delta `SUM(openivm_multiplicity * col)` formula can't type-check for these columns; detected in `CompileAggregateGroups` via the delta-view column types and routed to group-recompute. Unified path with `LIST` and `MIN`/`MAX` above. |
| `HAVING` | Partial | Group-recompute for affected groups; `openivm_having_merge=true` (default) uses the MERGE path when the stored data table holds all groups. |

## Other limitations

- **DROP MATERIALIZED VIEW syntax** is not supported because DuckDB's parser intercepts
  it before the extension can rewrite it. Use `DROP VIEW <mv_name>` instead. OpenIVM
  cleans the user-facing view, data table, MV delta table, base delta tables that are no
  longer shared, and metadata rows.

- **Schema evolution** is supported for `ADD COLUMN`, `DROP COLUMN`, and `RENAME COLUMN`
  on base tables. `ALTER COLUMN TYPE` is not handled. Dropping a referenced column is
  blocked with an error. Renaming a referenced column rewrites stored MV SQL and refresh
  metadata, including supported aux-state and lineage metadata.

- **Transaction isolation during refresh** uses a separate Connection with snapshot
  isolation. Concurrent DML during refresh does not affect the in-progress refresh, but
  the interaction has not been exhaustively audited.

- **Window functions over DuckLake with non-output partition keys or unsupported lineage
  shapes** fall back to full recompute (`DELETE FROM data; INSERT INTO data SELECT * FROM
  view_query`). Partition-level recompute is used when changed partition values can be
  derived from DuckLake snapshot diffs and lineage metadata. When the PARTITION BY column
  is dropped by the outer SELECT (e.g. `WITH cte AS (… PARTITION BY a, b, c …) SELECT a,
  c, … FROM cte` where `b` is projected out), the outer `WHERE <b> IN (…)` can't resolve
  the column, so OpenIVM can't identify affected rows in the data table. A proper
  incremental fix requires rewriting the view query to push the partition filter into the
  base scan; not yet implemented.
