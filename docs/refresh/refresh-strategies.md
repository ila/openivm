# Refresh Strategies

OpenIVM supports three refresh strategies for materialized views: **auto**, **incremental**, and **full**. The strategy determines how `PRAGMA ivm('view_name')` applies pending changes.

## Refresh modes

The `ivm_refresh_mode` setting controls which strategy is used at refresh time.

| Mode | Behavior |
|---|---|
| `auto` (default) | The system decides. Uses incremental refresh when the view supports it; falls back to full refresh otherwise. |
| `incremental` | Forces the IVM delta pipeline. Fails if the view was classified as `FULL_REFRESH` at creation time. |
| `full` | Forces a complete DELETE + INSERT recomputation, regardless of whether the view supports IVM. |

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

## Adaptive cost model

When `ivm_adaptive` is enabled, OpenIVM estimates the cost of incremental refresh versus full recomputation before each refresh and picks the cheaper option. This is useful when delta sizes vary unpredictably.

```sql
SET ivm_adaptive = true;
PRAGMA ivm('monthly_totals');
```

The cost model compares estimated cardinalities of the delta tables against the base tables. When `ivm_adaptive` is `false` (the default), the system always uses IVM for views that support it.

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

Views that use operators not yet supported for IVM are classified as `FULL_REFRESH` with a warning printed at creation time. Unsupported constructs include non-inner joins (e.g., `LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`) and aggregate functions outside the supported set (`COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `LIST`).

```sql
-- Prints a warning; subsequent PRAGMA ivm() uses full refresh
CREATE MATERIALIZED VIEW with_left AS
  SELECT a.id, b.name
  FROM orders a LEFT JOIN customers b ON a.cid = b.id;
```

Supported aggregate functions: `COUNT`, `COUNT(*)`, `SUM`, `MIN`, `MAX`, `AVG`, `LIST`.
