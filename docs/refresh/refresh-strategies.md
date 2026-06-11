# Refresh Strategies

OpenIVM supports three refresh strategies for materialized views: **auto**, **incremental**, and **full**. The strategy determines how `PRAGMA refresh('view_name')` applies pending changes.

## Refresh modes

The `openivm_refresh_mode` setting controls which strategy is used at refresh time.

| Mode            | Behavior |
|-----------------|---|
| `auto`          | The system decides. Uses incremental refresh when the view supports it; falls back to full refresh otherwise. |
| `incremental`  (default)  | Forces the IVM delta pipeline. Fails if the view was classified as `FULL_REFRESH` at creation time. |
| `full`          | Forces a complete DELETE + INSERT recomputation, regardless of whether the view supports IVM. |

```sql
-- Use incremental refresh (default)
SET openivm_refresh_mode = 'incremental';
PRAGMA refresh('monthly_totals');

-- Let the system decide (incremental when supported, full otherwise)
SET openivm_refresh_mode = 'auto';
PRAGMA refresh('monthly_totals');

-- Force full recomputation
SET openivm_refresh_mode = 'full';
PRAGMA refresh('monthly_totals');
```

## Upsert compilation by view type

The incremental refresh compiles different SQL depending on the view's `RefreshType` (see [Parser: IVM compatibility classification](../internals/parser.md#ivm-compatibility-classification)).

| View type | Strategy | Why |
|---|---|---|
| `AGGREGATE_GROUP` | `MERGE INTO` on GROUP BY keys | Each group is a unique row. MERGE updates existing groups and inserts new ones in a single pass. |
| `SIMPLE_AGGREGATE` | `UPDATE` (single row) | No GROUP BY means the MV always has exactly one row (SQL guarantees ungrouped aggregates return one row, even on empty input). A plain UPDATE is sufficient. |
| `SIMPLE_PROJECTION` | `DELETE` (rowid + ROW_NUMBER) then `INSERT` (generate_series) | No keys at all — the MV is a bag of tuples with valid duplicates. MERGE cannot target specific duplicate copies, so row-level addressing via rowid is required. |

For views containing `MIN`, `MAX`, or `HAVING`, a **group-recompute** strategy is used instead: affected groups are deleted and re-inserted from the original query.

MERGE requires a key to match source and target rows. `AGGREGATE_GROUP` views have natural keys (the GROUP BY columns), but the other two types do not:

- **Simple aggregates** have a single row with no key columns. MERGE would work (match on a dummy condition, always UPDATE), but adds complexity over a plain UPDATE for no benefit.
- **Projections/filters** allow duplicate rows. MERGE cannot express "delete exactly 3 copies of tuple (a, b) out of 7" — that requires rowid-based targeting.

## Adaptive cost model (experimental)

> **Note:** This feature is experimental. The cost model heuristics may change in future releases.

When `openivm_adaptive_refresh` is enabled, OpenIVM estimates the cost of the
incremental refresh path versus full recomputation before each refresh. It picks the
cheaper option, which helps when delta sizes vary unpredictably.

```sql
SET openivm_adaptive_refresh = true;
PRAGMA refresh('monthly_totals');
```

The cost model compares the estimated cost of the view's refresh strategy with a full
DELETE + INSERT recompute. When `openivm_adaptive_refresh` is `false` (the default),
OpenIVM uses the strategy selected at view creation.

The static estimate is operator-based:

| Operator class | Cost signal |
|---|---|
| Linear operators (`SCAN`, `FILTER`, `PROJECT`, `UNION ALL`) | Pending delta rows, adjusted for filter selectivity. |
| Joins | Active join terms. Empty source deltas remove terms; DuckLake N-term joins count one term per changed source. |
| Aggregates | Estimated affected groups, capped by MV cardinality. |
| Affected-domain recompute | Estimated touched groups, partitions, or rows rather than the full MV. |
| Full recompute | Base scan plus replacing all MV rows. |

For FULL OUTER JOIN views, the static model applies higher upsert cost multipliers
(3x for aggregates, 1.5x for projections) to account for additional recompute phases.
The learned regression model self-corrects from execution history after a few refreshes.

### Inspecting the cost estimate

`PRAGMA refresh_cost('view_name')` returns the cost model's recommendation without performing a refresh.

```sql
PRAGMA refresh_cost('monthly_totals');
```

Returns a single row:

| decision | incremental_cost | recompute_cost | incremental_predicted_ms | recompute_predicted_ms | calibrated |
|---|---:|---:|---:|---:|---|
| incremental | 1200.0 | 50000.0 | 1200.0 | 50000.0 | false |

- `decision`: the selected strategy. Values include `incremental`, `group_recompute`, `window_partition`, `current_diff_recompute`, `distinct_incremental`, `semi_anti_recompute`, and `full`.
- `incremental_cost`: estimated cost of the selected non-full strategy. For affected-domain strategies, this is not a pure delta-only cost.
- `recompute_cost`: estimated cost of a full DELETE + INSERT.
- `incremental_predicted_ms`: learned prediction for the selected non-full strategy, or the static estimate before calibration.
- `recompute_predicted_ms`: learned prediction for full recompute, or the static estimate before calibration.
- `calibrated`: `true` when refresh history was sufficient to fit the learned model.

Use `PRAGMA refresh_history('view_name')` to inspect the refresh records used by the learned model.
For implementation details and preliminary validation results, see
[Cost Model](../internals/cost_model.md).

## Automatic full-refresh detection

OpenIVM automatically classifies a view as full-refresh-only at creation time if its query contains constructs that cannot be maintained incrementally. No configuration is needed.

### Non-deterministic functions

Views that reference non-deterministic functions such as `RANDOM()` or `NOW()` are classified as `FULL_REFRESH`. The result of these functions can change between evaluations, so incremental deltas would be incorrect.

```sql
-- Automatically uses full refresh (RANDOM is non-deterministic)
CREATE MATERIALIZED VIEW sampled AS
  SELECT *, RANDOM() AS r FROM events;
```

### Unsupported operators

Views that use operators not yet supported for IVM are classified as `FULL_REFRESH` with a warning printed at creation time. Unsupported constructs include window functions over joins, unsupported semi/anti shapes, and aggregate functions outside the supported set.

`INNER JOIN`, `CROSS JOIN`, arbitrary-predicate joins, `LEFT JOIN`, `RIGHT JOIN`, and `FULL OUTER JOIN` are supported for incremental maintenance. Supported `SEMI JOIN`, `ANTI JOIN`, `EXISTS`, and `NOT EXISTS` projection shapes use an aux-state path.

```sql
-- Prints a warning; subsequent PRAGMA refresh() uses full refresh
CREATE MATERIALIZED VIEW with_unsupported AS
  SELECT MEDIAN(amount) FROM orders;
```

Supported aggregate functions include `COUNT`, `COUNT(*)`, `SUM`, `MIN`, `MAX`, `AVG`, `LIST`, `STDDEV`/`VARIANCE`, `BOOL_AND`, `BOOL_OR`, `ARG_MIN`, and `ARG_MAX`. Some use group recompute rather than pure MERGE.
