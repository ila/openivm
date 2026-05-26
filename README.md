# OpenIVM

A DuckDB extension for **incremental view maintenance** (IVM). Create materialized views pipelines with standard SQL, then refresh them incrementally — without recomputing the entire query.

Based on the [OpenIVM paper](https://dl.acm.org/doi/10.1145/3626246.3654743) (SIGMOD 2024).

## Quick start

```sql
LOAD 'openivm';

-- Create a base table and a materialized view
CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 'Widget', 100), ('EU', 'Gadget', 200);

CREATE OR REPLACE MATERIALIZED VIEW regional_totals REFRESH EVERY '5 minutes' AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region;

-- Insert new data
INSERT INTO sales VALUES ('US', 'Bolt', 50), ('JP', 'Gear', 300);

-- Refresh manually at any time
PRAGMA refresh('regional_totals');

SELECT * FROM regional_totals ORDER BY region;
-- EU  | 200 | 1
-- JP  | 300 | 1
-- US  | 150 | 2
```

Views with `REFRESH EVERY` are maintained automatically by a background daemon. See [automatic refresh](docs/refresh/automatic-refresh.md).

Base table schema changes (`ADD`, `DROP`, `RENAME COLUMN`) are propagated by OpenIVM. Delta tables are synced, referenced renames update MV metadata, and referenced drops are blocked with an error.

## Data pipelines and DuckLake

Materialized views can be stacked into pipelines, including over [DuckLake](https://ducklake.select/) tables. DuckLake's snapshot-based time travel replaces delta tables with native change tracking. See [DuckLake IVM integration](docs/ducklake.md). 
```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH ':memory:' AS dl (TYPE ducklake);

CREATE TABLE dl.orders (id INT, product VARCHAR, region VARCHAR, amount INT);
INSERT INTO dl.orders VALUES (1, 'Widget', 'US', 500), (2, 'Gadget', 'EU', 200), (3, 'Widget', 'EU', 800);

-- First MV: aggregate by product
CREATE MATERIALIZED VIEW dl.product_totals REFRESH EVERY '5 minutes' AS
    SELECT product, SUM(amount) AS total, COUNT(*) AS cnt FROM dl.orders GROUP BY product;

-- Second MV: built on top of the first
CREATE MATERIALIZED VIEW dl.top_products REFRESH EVERY '10 minutes' AS
    SELECT product, total FROM dl.product_totals WHERE total > 1000;

-- Cascade modes (controls automatic refresh propagation):
SET openivm_cascade_refresh = 'downstream';  -- default: refreshing product_totals also refreshes top_products
SET openivm_cascade_refresh = 'upstream';    -- refreshing top_products first refreshes product_totals
SET openivm_cascade_refresh = 'both';        -- refresh in both directions
SET openivm_cascade_refresh = 'off';         -- no cascade, each view refreshes independently
```
Note: without cascading refresh, views refreshing independently may see stale upstream data — results are consistent but not fresh until the next ordered refresh. See [pipelines](docs/refresh/pipelines.md).

## Supported operators

MVs can be created using any SQL construct. Unsupported operators automatically fall back to [full refresh](docs/refresh/refresh-strategies.md).

| Operator | Strategy | Documentation |
|----------|----------|---------------|
| `SELECT ... FROM`, `WHERE`, expressions | Incremental | [Projection & filter](docs/operators/projection-filter.md) |
| `GROUP BY` + `SUM`, `COUNT`, `AVG` | Incremental | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `STDDEV`, `VARIANCE` (all variants) | Incremental | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `MIN`, `MAX` | Incremental (insert-only) / group-recompute | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `HAVING` | Incremental | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| Ungrouped aggregates | Incremental | [Ungrouped aggregates](docs/operators/ungrouped-aggregates.md) |
| `INNER JOIN`, `CROSS JOIN`, arbitrary join predicates | Incremental | [Inner join](docs/operators/inner-join.md) |
| `LEFT JOIN`, `RIGHT JOIN` | Incremental | [Left join](docs/operators/left-join.md) |
| `FULL OUTER JOIN` | Incremental (MERGE + recompute) | [Full outer join](docs/operators/full-outer-join.md) |
| `SEMI JOIN`, `ANTI JOIN`, `EXISTS`, `NOT EXISTS` | Aux-state incremental for supported projection shapes | [Semi & anti join](docs/operators/semi-anti-join.md) |
| `UNION ALL` | Incremental | [Union all](docs/operators/union-all.md) |
| `DISTINCT` | Incremental | [Distinct](docs/operators/distinct.md) |
| Window functions (`ROW_NUMBER`, `RANK`, etc.) | Partition-level recompute | [Window functions](docs/operators/window-functions.md) |
| `LIST` aggregates | Incremental | [List aggregates](docs/operators/list-aggregates.md) |
| `WITH` (CTEs), decorrelated subqueries, scalar correlated subqueries | Incremental when the lowered plan uses supported operators; scalar `SINGLE` delim shapes use affected-key recompute | [CTEs & subqueries](docs/operators/cte-subquery.md) |


## Settings

| Setting | Type | Default | Description | Documentation |
|---------|------|---------|-------------|---------------|
| `openivm_cascade_refresh` | VARCHAR | `downstream` | Cascade mode: `off`, `upstream`, `downstream`, `both` | [Pipelines](docs/refresh/pipelines.md) |
| `openivm_refresh_mode` | VARCHAR | `incremental` | Refresh strategy: `incremental`, `full`, or `auto` | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `openivm_adaptive_refresh` | BOOLEAN | `false` | Enable adaptive cost model (learned regression) | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `openivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh takes longer than interval | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `openivm_disable_daemon` | BOOLEAN | `false` | Disable the background refresh daemon | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `openivm_skip_empty_deltas` | BOOLEAN | `true` | Skip refresh work when no pending deltas exist | [Empty delta skip](docs/optimizations/empty-delta-skip.md) |
| `openivm_compact_deltas` | BOOLEAN | `true` | Compact raw delta rows into net Z-set deltas before refresh | [Delta consolidation](docs/optimizations/delta-consolidation.md) |
| `openivm_distinct_aux_state` | BOOLEAN | `false` | Use aux-state maintenance for supported inner-DISTINCT-under-aggregate shapes | [Distinct](docs/operators/distinct.md) |
| `openivm_profile_refresh` | BOOLEAN | `false` | Record per-step refresh timings in `openivm_refresh_profile` | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `openivm_files_path` | VARCHAR | — | Directory for compiled SQL reference files | [Internals](docs/internals/delta-tables.md) |


## Pragmas

| Pragma | Description |
|--------|-------------|
| `PRAGMA refresh('view_name')` | Refresh a materialized view |
| `PRAGMA refresh_cost('view_name')` | Show incremental refresh vs full recompute cost estimate (static + calibrated) |
| `PRAGMA refresh_history('view_name')` | Show refresh execution history (for learned cost model) |
| `PRAGMA refresh_options(catalog, schema, view_name)` | Refresh with explicit catalog/schema |
| `PRAGMA refresh_status('view_name')` | Show refresh interval, last/next refresh, and status |

## Documentation

- **[DuckLake integration](docs/ducklake.md)** — IVM over DuckLake tables with native change tracking
- **[Operators](docs/operators/)** — How each SQL operator is incrementalized
- **[Refresh](docs/refresh/)** — Refresh strategies and view pipelines
- **[Optimizations](docs/optimizations/)** — Delta consolidation, FK pruning, empty-delta skip, indexing
- **[Internals](docs/internals/)** — Delta tables, parser, concurrency
- **[Limitations](docs/limitations.md)** — Unsupported operators, known restrictions
- **[Build](docs/build/)** — Building, testing, benchmarks
