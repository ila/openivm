# Parser

OpenIVM intercepts `CREATE MATERIALIZED VIEW` statements through a DuckDB parser extension. The parser rewrites the statement into a sequence of DDL operations that set up the materialized view, its delta tables, and its metadata.
The original statement is rewritten to `CREATE TABLE IF NOT EXISTS <name> AS <query>`, which materializes the query result into a regular DuckDB table.

## Aggregate function aliasing

Before parsing, the query is lowercased and aggregate functions are given explicit aliases. This ensures that the upsert compiler can reference aggregate columns by a stable name.

| Expression | Rewritten to |
|---|---|
| `COUNT(*)` | `COUNT(*) AS count_star` |
| `COUNT(x)` | `COUNT(x) AS count_x` |
| `SUM(amount)` | `SUM(amount) AS sum_amount` |
| `MIN(price)` | `MIN(price) AS min_price` |
| `MAX(price)` | `MAX(price) AS max_price` |
| `AVG(score)` | `AVG(score) AS avg_score` |

Expressions that already have an explicit `AS` alias are left unchanged. Non-alphanumeric characters in the argument are replaced with underscores in the alias (e.g., `SUM(a + b)` becomes `SUM(a + b) AS sum_a___b`).

The HAVING clause is split from the SELECT before aggregate aliasing and re-attached afterward. This prevents the rewriter from injecting `AS alias` inside the HAVING expression, which would produce invalid SQL.

## DISTINCT rewriting

The parser rewrites `SELECT DISTINCT` into `GROUP BY` + hidden `COUNT(*)` before planning, classifying it as `AGGREGATE_GROUP`. See [Distinct](../operators/distinct.md) for details.

## AVG decomposition

The parser decomposes `AVG(x)` into hidden `_ivm_sum_*` and `_ivm_count_*` columns so that AVG can be maintained incrementally via MERGE. See [Metadata Columns](metadata-columns.md#_ivm_sum_-and-_ivm_count_) for details.

## LEFT JOIN key injection

For `LEFT JOIN` or `RIGHT JOIN` queries, the parser adds a hidden `_ivm_left_key` column containing the preserved-side join key, used by the upsert for partial recompute. For `RIGHT JOIN`, DuckDB internally rewrites it to `LEFT JOIN` (swapping the table order), so the preserved side is always the left table after rewriting. See [Metadata Columns](metadata-columns.md#_ivm_left_key) for details.

## IVM compatibility classification

After rewriting, the parser plans the query and walks the logical plan to classify the view into one of four types:

| Type | Code | Condition |
|---|---|---|
| `AGGREGATE_GROUP` | 0 | Aggregation with GROUP BY columns (including rewritten DISTINCT). |
| `SIMPLE_AGGREGATE` | 1 | Aggregation without GROUP BY (global aggregate). |
| `SIMPLE_PROJECTION` | 2 | Projection or filter, no aggregation. |
| `FULL_REFRESH` | 3 | Contains unsupported constructs. |

The IVM compatibility checker validates the entire plan tree, flagging unsupported join types (only INNER, LEFT, and RIGHT are supported), unsupported aggregate functions (anything outside `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `LIST`), and non-deterministic functions (e.g., `RANDOM()`, `NOW()`). If any unsupported construct is found, the view is classified as `FULL_REFRESH` and a warning is printed.

## Generated DDL

The parser produces a sequence of DDL statements executed during the bind phase:

1. **System tables**: `CREATE TABLE IF NOT EXISTS _duckdb_ivm_views (...)` and `_duckdb_ivm_delta_tables (...)`.
2. **Metadata inserts**: Registers the view name, query string, type, and source table mappings.
3. **MV table**: `CREATE TABLE <view_name> AS <query>` to materialize the initial result.
4. **Delta tables**: One `delta_<table_name>` per source table, with `_duckdb_ivm_multiplicity` and `_duckdb_ivm_timestamp` columns.
5. **Delta view table**: `delta_<view_name>` for downstream chained MV support, with `DEFAULT now()` on the timestamp column.
6. **Index** (AGGREGATE_GROUP only): A unique index on the GROUP BY columns, used by the MERGE INTO upsert strategy.

## System tables

### `_duckdb_ivm_views`

Stores one row per materialized view.

| Column | Type | Description |
|---|---|---|
| `view_name` | `VARCHAR` (PK) | Name of the materialized view. |
| `sql_string` | `VARCHAR` | The original SELECT query defining the view. |
| `type` | `TINYINT` | View classification (see IVM compatibility classification above). |
| `last_update` | `TIMESTAMP` | When the view was last created or replaced. |

Example content:

| view_name | sql_string | type | last_update |
|---|---|---|---|
| `mv_grouped` | `select region, sum(amount) as sum_amount, count(*) as count_star from sales group by region` | 0 | 2026-03-27 10:00:00 |
| `mv_projection` | `select id, name from customers` | 2 | 2026-03-27 10:01:00 |

### `_duckdb_ivm_delta_tables`

Tracks which delta tables feed each materialized view, along with the timestamp of the last refresh.

| Column | Type | Description |
|---|---|---|
| `view_name` | `VARCHAR` | Name of the materialized view. |
| `table_name` | `VARCHAR` | Name of the delta table (e.g., `delta_sales`). |
| `last_update` | `TIMESTAMP` | Timestamp of the last refresh for this view-table pair. |

The primary key is the composite `(view_name, table_name)`.

Example content:

| view_name | table_name | last_update |
|---|---|---|
| `mv_grouped` | `delta_sales` | 2026-03-27 10:05:00 |
| `mv_join` | `delta_orders` | 2026-03-27 10:05:00 |
| `mv_join` | `delta_customers` | 2026-03-27 10:05:00 |
