# OpenIVM

A DuckDB extension for **incremental view maintenance** (IVM). Create materialized views with standard SQL, then refresh them incrementally — only processing the changes since the last refresh, not recomputing the entire query.

Based on the [OpenIVM paper](https://dl.acm.org/doi/10.1145/3626246.3654743) (SIGMOD 2024).

## Quick start

```sql
LOAD 'openivm';

-- Create a base table and a materialized view with automatic refresh
CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 'Widget', 100), ('EU', 'Gadget', 200);

CREATE MATERIALIZED VIEW regional_totals REFRESH EVERY '5 minutes' AS
    SELECT region, SUM(amount) AS total FROM sales GROUP BY region;

-- Insert new data
INSERT INTO sales VALUES ('US', 'Bolt', 50), ('JP', 'Gear', 300);

-- Or refresh manually at any time
PRAGMA ivm('regional_totals');

SELECT * FROM regional_totals ORDER BY region;
-- EU  | 200
-- JP  | 300
-- US  | 150
```

### Replacing a view

```sql
-- Replace an existing MV with a new definition (drops and recreates atomically)
CREATE OR REPLACE MATERIALIZED VIEW regional_totals REFRESH EVERY '10 minutes' AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region;
```

Base table schema changes (ADD/DROP/RENAME COLUMN) are handled automatically — delta tables are synced and IVM continues to work. Dropping or renaming a column referenced by an MV is blocked with an error.

## DuckLake integration

OpenIVM supports materialized views over [DuckLake](https://ducklake.select/) tables. DuckLake's snapshot-based time travel replaces delta tables with native change tracking, and enables a more efficient join rule (N terms instead of 2^N - 1). See [DuckLake IVM integration](docs/ducklake.md).

```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH ':memory:' AS dl (TYPE ducklake);

CREATE TABLE dl.orders (id INT, product VARCHAR, amount INT);
INSERT INTO dl.orders VALUES (1, 'Widget', 100), (2, 'Gadget', 200);

CREATE MATERIALIZED VIEW dl.order_totals AS
    SELECT product, SUM(amount) AS total FROM dl.orders GROUP BY product;

INSERT INTO dl.orders VALUES (3, 'Widget', 50);
PRAGMA ivm('order_totals');
```

## Supported operators

MVs can be created using any SQL construct. Unsupported operators automatically fall back to [full refresh](docs/refresh/refresh-strategies.md).

| Operator                                | Documentation |
|-----------------------------------------|---------------|
| `SELECT ... FROM`, `WHERE`, expressions | [Projection & filter](docs/operators/projection-filter.md) |
| `GROUP BY` + aggregate function         | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `SUM`, `COUNT`, `AVG` (no GROUP BY)     | [Ungrouped aggregates](docs/operators/ungrouped-aggregates.md) |
| `INNER JOIN`                            | [Inner join](docs/operators/inner-join.md) |
| `LEFT JOIN`, `RIGHT JOIN`               | [Left join](docs/operators/left-join.md) |
| `UNION ALL`                             | [Union all](docs/operators/union-all.md) |
| `DISTINCT`                              | [Distinct](docs/operators/distinct.md) |
| `LIST` aggregates                       | [List aggregates](docs/operators/list-aggregates.md) |
| `WITH` (CTEs), subqueries              | [CTEs & subqueries](docs/operators/cte-subquery.md) |

## Settings

| Setting | Type | Default | Description | Documentation |
|---------|------|---------|-------------|---------------|
| `ivm_cascade_refresh` | VARCHAR | `downstream` | Cascade mode: `off`, `upstream`, `downstream`, `both` | [Pipelines](docs/refresh/pipelines.md) |
| `ivm_refresh_mode` | VARCHAR | `incremental` | Refresh strategy: `incremental`, `full`, or `auto` | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `ivm_adaptive_refresh` | BOOLEAN | `false` | Experimental: enable cost-based strategy selection | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `ivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh takes longer than interval | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `ivm_files_path` | VARCHAR | — | Directory for compiled SQL reference files | [Internals](docs/internals/delta-tables.md) |

### Optimization flags

All default to `true`. Set to `false` to disable an optimization and fall back to the traditional IVM path.

| Setting | Description | Documentation |
|---------|-------------|---------------|
| `ivm_skip_empty_deltas` | Skip refresh or join terms when deltas are empty | [Empty delta skip](docs/optimizations/empty-delta-skip.md) |
| `ivm_ducklake_nterm` | N-term telescoping for DuckLake joins (vs 2^N-1 inclusion-exclusion) | [DuckLake](docs/ducklake.md) |
| `ivm_fk_pruning` | Prune inclusion-exclusion join terms using FK constraints | [FK pruning](docs/optimizations/fk-aware-pruning.md) |
| `ivm_skip_aggregate_delete` | Skip zero-row DELETE for aggregates when insert-only | [Append-only](docs/optimizations/append-only.md) |
| `ivm_skip_projection_delete` | Skip DELETE+consolidation for projections when insert-only | [Append-only](docs/optimizations/append-only.md) |
| `ivm_minmax_incremental` | Use GREATEST/LEAST for MIN/MAX when insert-only | [Append-only](docs/optimizations/append-only.md) |

## Pragmas

| Pragma | Description |
|--------|-------------|
| `PRAGMA ivm('view_name')` | Refresh a materialized view |
| `PRAGMA ivm_cost('view_name')` | Show IVM vs full recompute cost estimate |
| `PRAGMA ivm_options(catalog, schema, view_name)` | Refresh with explicit catalog/schema |
| `PRAGMA ivm_status('view_name')` | Show refresh interval, last/next refresh, and status |

## Documentation

- **[DuckLake integration](docs/ducklake.md)** — IVM over DuckLake tables with native change tracking
- **[Operators](docs/operators/)** — How each SQL operator is incrementalized
- **[Refresh](docs/refresh/)** — Refresh strategies and MV pipelines
- **[Optimizations](docs/optimizations/)** — Delta consolidation, FK pruning, empty-delta skip, indexing
- **[Internals](docs/internals/)** — Delta tables, parser, concurrency
- **[Limitations](docs/limitations.md)** — Unsupported operators, known restrictions
- **[Build](docs/build/)** — Building, testing, benchmarks
