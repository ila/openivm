# Refresh Pipelines

Materialized views can be chained: one MV can be defined over another, forming a pipeline. OpenIVM tracks these dependencies and can cascade refreshes automatically.

## Chained materialized views

A chain is created when a materialized view references another materialized view as a source table.

```sql
CREATE TABLE sales (region VARCHAR, amount INTEGER);

CREATE MATERIALIZED VIEW region_totals AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
  FROM sales GROUP BY region;

CREATE MATERIALIZED VIEW top_regions AS
  SELECT region, total FROM region_totals WHERE total > 1000;

CREATE MATERIALIZED VIEW top_region_count AS
  SELECT COUNT(*) AS cnt FROM top_regions;
```

This creates the chain: `sales` -> `region_totals` -> `top_regions` -> `top_region_count`.

## Cascade modes

The `ivm_cascade_refresh` setting controls how `PRAGMA ivm()` handles dependent views in a pipeline.

| Mode | Behavior |
|---|---|
| `off` | Refresh only the named view. The user manages refresh order manually. |
| `upstream` | Before refreshing the named view, refresh all ancestor views that feed into it. |
| `downstream` (default) | After refreshing the named view, refresh all descendant views that depend on it. |
| `both` | Refresh ancestors first, then the named view, then descendants. |

### off

Each view is refreshed independently. The user must call `PRAGMA ivm()` in the correct order.

```sql
SET ivm_cascade_refresh = 'off';

INSERT INTO sales VALUES ('east', 500);

-- Must refresh in topological order
PRAGMA ivm('region_totals');
PRAGMA ivm('top_regions');
PRAGMA ivm('top_region_count');
```

### downstream (default)

Refreshing an upstream view automatically refreshes all views that depend on it.

```sql
SET ivm_cascade_refresh = 'downstream';

INSERT INTO sales VALUES ('east', 500);

-- Automatically refreshes top_regions and top_region_count after region_totals
PRAGMA ivm('region_totals');
```

### upstream

Refreshing a downstream view first refreshes all ancestor views that feed into it, ensuring it sees the latest data.

```sql
SET ivm_cascade_refresh = 'upstream';

INSERT INTO sales VALUES ('east', 500);

-- Automatically refreshes region_totals and top_regions first, then top_region_count
PRAGMA ivm('top_region_count');
```

### both

Combines upstream and downstream: ancestors are refreshed first, then the target, then descendants.

```sql
SET ivm_cascade_refresh = 'both';

INSERT INTO sales VALUES ('east', 500);

-- Refreshes everything in the pipeline in correct order
PRAGMA ivm('top_regions');
```

## Out-of-order refresh

Refreshing views out of topological order (e.g., refreshing a downstream view before its upstream dependency) is safe. The downstream view will reflect the state of its source at the time of the last upstream refresh. Results are stale, not incorrect. The next properly-ordered refresh brings everything up to date.

## Fan-out

A single materialized view can feed multiple independent downstream chains. Each chain is tracked separately.

```sql
CREATE MATERIALIZED VIEW base_summary AS
  SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
  FROM sales GROUP BY region;

-- Two independent chains from the same source
CREATE MATERIALIZED VIEW chain_a AS
  SELECT region FROM base_summary WHERE total > 1000;

CREATE MATERIALIZED VIEW chain_b AS
  SELECT region FROM base_summary WHERE cnt > 100;
```

With `ivm_cascade_refresh = 'downstream'`, refreshing `base_summary` automatically refreshes both `chain_a` and `chain_b`.
