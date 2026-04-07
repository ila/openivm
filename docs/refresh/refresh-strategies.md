# Refresh Strategies

OpenIVM supports three refresh strategies for materialized views: **auto**, **incremental**, and **full**. The strategy determines how `PRAGMA ivm('view_name')` applies pending changes.

## Refresh modes

The `ivm_refresh_mode` setting controls which strategy is used at refresh time.

| Mode            | Behavior |
|-----------------|---|
| `auto`          | The system decides. Uses incremental refresh when the view supports it; falls back to full refresh otherwise. |
| `incremental`  (default)  | Forces the IVM delta pipeline. Fails if the view was classified as `FULL_REFRESH` at creation time. |
| `full`          | Forces a complete DELETE + INSERT recomputation, regardless of whether the view supports IVM. |

```sql
-- Use the default (auto) strategy
SET ivm_refresh_mode = 'auto';
PRAGMA ivm('monthly_totals');

-- Force incremental refresh
SET ivm_refresh_mode = 'incremental';
PRAGMA ivm('monthly_totals');

-- Force full recomputation
SET ivm_refresh_mode = 'full';
PRAGMA ivm('monthly_totals');
```

## Upsert compilation by view type

The incremental refresh compiles different SQL depending on the view's `IVMType` (see [Parser: IVM compatibility classification](../internals/parser.md#ivm-compatibility-classification)).

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

When `ivm_adaptive_refresh` is enabled, OpenIVM estimates the cost of incremental refresh versus full recomputation before each refresh and picks the cheaper option. This is useful when delta sizes vary unpredictably.

```sql
SET ivm_adaptive_refresh = true;
PRAGMA ivm('monthly_totals');
```

The cost model compares estimated cardinalities of the delta tables against the base tables. When `ivm_adaptive_refresh` is `false` (the default), the system always uses IVM for views that support it.

### Inspecting the cost estimate

`PRAGMA ivm_cost('view_name')` returns the cost model's recommendation without performing a refresh.

```sql
PRAGMA ivm_cost('monthly_totals');
```

Returns a single row:

| decision | ivm_cost | recompute_cost |
|---|---|---|
| incremental | 1200.0 | 50000.0 |

- `decision`: `incremental` or `full`, based on which cost is lower.
- `ivm_cost`: estimated cost of applying deltas.
- `recompute_cost`: estimated cost of a full DELETE + INSERT.

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

Views that use operators not yet supported for IVM are classified as `FULL_REFRESH` with a warning printed at creation time. Unsupported constructs include `FULL OUTER JOIN`, window functions, and aggregate functions outside the supported set.

`INNER JOIN`, `LEFT JOIN`, and `RIGHT JOIN` are all supported for incremental maintenance.

```sql
-- Prints a warning; subsequent PRAGMA ivm() uses full refresh
CREATE MATERIALIZED VIEW with_full_outer AS
  SELECT a.id, b.name
  FROM orders a FULL OUTER JOIN customers b ON a.cid = b.id;
```

Supported aggregate functions: `COUNT`, `COUNT(*)`, `SUM`, `MIN`, `MAX`, `AVG`, `LIST`.
