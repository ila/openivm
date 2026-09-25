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

The `openivm_cascade_refresh` setting controls how `PRAGMA refresh()` handles dependent views in a pipeline.

| Mode | Behavior |
|---|---|
| `off` | Refresh only the named view. The user manages refresh order manually. |
| `upstream` | Before refreshing the named view, refresh all ancestor views that feed into it. |
| `downstream` (default) | After refreshing the named view, refresh all descendant views that depend on it. |
| `both` | Refresh ancestors first, then the named view, then descendants. |

### off

Each view is refreshed independently. The user must call `PRAGMA refresh()` in the correct order.

```sql
SET openivm_cascade_refresh = 'off';

INSERT INTO sales VALUES ('east', 500);

-- Must refresh in topological order
PRAGMA refresh('region_totals');
PRAGMA refresh('top_regions');
PRAGMA refresh('top_region_count');
```

### downstream (default)

Refreshing an upstream view automatically refreshes all views that depend on it.

```sql
SET openivm_cascade_refresh = 'downstream';

INSERT INTO sales VALUES ('east', 500);

-- Automatically refreshes top_regions and top_region_count after region_totals
PRAGMA refresh('region_totals');
```

### upstream

Refreshing a downstream view first refreshes all ancestor views that feed into it, ensuring it sees the latest data.

```sql
SET openivm_cascade_refresh = 'upstream';

INSERT INTO sales VALUES ('east', 500);

-- Automatically refreshes region_totals and top_regions first, then top_region_count
PRAGMA refresh('top_region_count');
```

### both

Combines upstream and downstream: ancestors are refreshed first, then the target, then descendants.

```sql
SET openivm_cascade_refresh = 'both';

INSERT INTO sales VALUES ('east', 500);

-- Refreshes everything in the pipeline in correct order
PRAGMA refresh('top_regions');
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

With `openivm_cascade_refresh = 'downstream'`, refreshing `base_summary` automatically refreshes both `chain_a` and `chain_b`.

## Refresh a selected pipeline

Use `refresh_pipeline` to refresh several starting MVs and their selected dependencies as one ordered run:

```sql
SET openivm_cascade_refresh = 'upstream';
PRAGMA refresh_pipeline('sales_report', 'inventory_report');
```

OpenIVM discovers the graph from current source metadata on each call. No pipeline registration is needed. Arguments are unqualified MV names in the current metadata catalog, as with `refresh`; use `USE` to select an attached native database. DuckLake views use their native controller's metadata.

| Cascade mode | Selected views |
|---|---|
| `off` | Only the named MVs |
| `upstream` | Named MVs and their ancestors |
| `downstream` | Named MVs and their descendants |
| `both` | Named MVs and their descendants, then all ancestors required by that selection |

Every selected MV is visited once, with selected parents before their children, regardless of argument order or repeated names. Independent ready nodes are ordered by name. Shared dependencies are deduplicated across targets. Refreshes whose inputs have no pending changes may be skipped as usual. `downstream` assumes parents outside the selection are already current; `both` includes those co-parents. Ordinary views are expanded when source dependencies are captured.

The run uses sequential refreshes under the existing OpenIVM mutation gate. The caller must finish ingestion before invoking it and keep ingestion paused until it returns; external DuckLake writers are not blocked by this gate. This is not an atomic publication boundary for readers.

Unknown targets, dependency cycles, and definitions that no longer bind are rejected before an autocommit run refreshes any node. The graph is rediscovered after DDL changes; compatible source schema evolution uses the existing schema-update machinery. A dropped target is an error, not silently omitted. Recreate invalidated MVs before retrying.

Each selected node uses its normal refresh hooks. A refresh or hook failure stops the run; earlier autocommit refreshes can already be committed. Native explicit transactions use the existing transactional refresh path and can be rolled back. DuckLake refreshes retain the existing separate data/metadata transaction behavior.

DuckLake `DROP VIEW` uses staged DDL: lake-side objects are removed before native OpenIVM metadata is cleaned up. This matches the existing cross-catalog create/refresh model; it is not one atomic transaction across both catalogs.
