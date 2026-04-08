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

## Settings

| Setting | Type | Default | Description | Documentation |
|---------|------|---------|-------------|---------------|
| `ivm_cascade_refresh` | VARCHAR | `downstream` | Cascade mode: `off`, `upstream`, `downstream`, `both` | [Pipelines](docs/refresh/pipelines.md) |
| `ivm_refresh_mode` | VARCHAR | `incremental` | Refresh strategy: `incremental`, `full`, or `auto` | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `ivm_adaptive_refresh` | BOOLEAN | `false` | Experimental: enable cost-based strategy selection | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `ivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh takes longer than interval | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `ivm_files_path` | VARCHAR | — | Directory for compiled SQL reference files | [Internals](docs/internals/delta-tables.md) |

## Pragmas

| Pragma | Description |
|--------|-------------|
| `PRAGMA ivm('view_name')` | Refresh a materialized view |
| `PRAGMA ivm_cost('view_name')` | Show IVM vs full recompute cost estimate |
| `PRAGMA ivm_options(catalog, schema, view_name)` | Refresh with explicit catalog/schema |
| `PRAGMA ivm_status('view_name')` | Show refresh interval, last/next refresh, and status |

## Documentation

- **[Operators](docs/operators/)** — How each SQL operator is incrementalized
- **[Refresh](docs/refresh/)** — Refresh strategies and MV pipelines
- **[Optimizations](docs/optimizations/)** — Delta consolidation, MERGE, indexing
- **[Internals](docs/internals/)** — Delta tables, parser, system tables
- **[Build](docs/build/)** — Building, testing, benchmarks
