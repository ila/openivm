# OpenIVM

A DuckDB extension for **incremental view maintenance** (IVM). Create materialized views with standard SQL, then refresh them incrementally — only processing the changes since the last refresh, not recomputing the entire query.

Based on the [OpenIVM paper](https://dl.acm.org/doi/10.1145/3626246.3654743) (SIGMOD 2024).

## Quick start

```sql
LOAD 'openivm';

-- Create a base table and a materialized view
CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 'Widget', 100), ('EU', 'Gadget', 200);

CREATE MATERIALIZED VIEW regional_totals AS
    SELECT region, SUM(amount) AS total FROM sales GROUP BY region;

-- Insert new data
INSERT INTO sales VALUES ('US', 'Bolt', 50), ('JP', 'Gear', 300);

-- Refresh incrementally (only processes the 2 new rows, not the entire table)
PRAGMA ivm('regional_totals');

SELECT * FROM regional_totals ORDER BY region;
-- EU  | 200
-- JP  | 300
-- US  | 150
```

## Supported operators

| Operator | Refresh strategy | Documentation |
|----------|-----------------|---------------|
| `SELECT ... FROM` | Incremental | [Projection](docs/operators/projection.md) |
| `WHERE` | Incremental | [Filter](docs/operators/filter.md) |
| `GROUP BY` + `SUM`, `COUNT` | Incremental (MERGE) | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `GROUP BY` + `MIN`, `MAX`, `AVG` | Group recompute | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `SUM`, `COUNT` (no GROUP BY) | Incremental (UPDATE) | [Ungrouped aggregates](docs/operators/ungrouped-aggregates.md) |
| `INNER JOIN` | Incremental (inclusion-exclusion) | [Inner join](docs/operators/inner-join.md) |
| `UNION ALL` | Incremental | [Union all](docs/operators/union-all.md) |
| `LIST` aggregates | Incremental (element-wise) | [List aggregates](docs/operators/list-aggregates.md) |
| `HAVING` | Group recompute | [Filter](docs/operators/filter.md) |
| Expressions (`a * 2`, `CASE WHEN`) | Incremental | [Projection](docs/operators/projection.md) |
| `LEFT JOIN`, `CROSS JOIN` | Full refresh | [Inner join](docs/operators/inner-join.md) |
| `RANDOM()`, `NOW()` | Full refresh | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `STDDEV`, `STRING_AGG` | Full refresh | [Grouped aggregates](docs/operators/grouped-aggregates.md) |

## Settings

| Setting | Type | Default | Description | Documentation |
|---------|------|---------|-------------|---------------|
| `ivm_cascade_refresh` | VARCHAR | `downstream` | Cascade mode: `off`, `upstream`, `downstream`, `both` | [Pipelines](docs/refresh/pipelines.md) |
| `ivm_refresh_mode` | VARCHAR | `auto` | Force refresh strategy: `auto`, `incremental`, `full` | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `ivm_adaptive_refresh` | BOOLEAN | `false` | Experimental: enable cost-based strategy selection | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `ivm_files_path` | VARCHAR | — | Directory for compiled SQL reference files | [Internals](docs/internals/delta-tables.md) |

## Pragmas

| Pragma | Description |
|--------|-------------|
| `PRAGMA ivm('view_name')` | Refresh a materialized view |
| `PRAGMA ivm_cost('view_name')` | Show IVM vs full recompute cost estimate |
| `PRAGMA ivm_options(catalog, schema, view_name)` | Refresh with explicit catalog/schema |

## Documentation

- **[Operators](docs/operators/)** — How each SQL operator is incrementalized
- **[Refresh](docs/refresh/)** — Refresh strategies and MV pipelines
- **[Optimizations](docs/optimizations/)** — Delta consolidation, MERGE, indexing
- **[Internals](docs/internals/)** — Delta tables, parser, system tables
- **[Build](docs/build/)** — Building, testing, benchmarks
