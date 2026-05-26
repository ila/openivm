# Parser

OpenIVM intercepts `CREATE MATERIALIZED VIEW`, `CREATE OR REPLACE MATERIALIZED VIEW`, and `ALTER MATERIALIZED VIEW` statements through a DuckDB parser extension. The parser rewrites CREATE statements into a sequence of DDL operations that set up the materialized view, its delta tables, and its metadata.
The original statement is rewritten to `CREATE TABLE IF NOT EXISTS <name> AS <query>`, which materializes the query result into a regular DuckDB table. `CREATE OR REPLACE` drops the old MV (view, data table, delta tables, metadata) before creating the new one.

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

The parser decomposes `AVG(x)` into hidden `openivm_sum_*` and `openivm_count_*` columns so that AVG can be maintained incrementally via MERGE. See [Metadata Columns](metadata-columns.md#openivm_sum_-and-openivm_count_) for details.

## LEFT JOIN key injection

For `LEFT JOIN` or `RIGHT JOIN` queries, the parser adds a hidden `openivm_left_key` column containing the preserved-side join key, used by the upsert for partial recompute. For `RIGHT JOIN`, DuckDB internally rewrites it to `LEFT JOIN` (swapping the table order), so the preserved side is always the left table after rewriting. See [Metadata Columns](metadata-columns.md#openivm_left_key) for details.

## REFRESH EVERY

The parser extracts an optional `REFRESH EVERY '<interval>'` clause before the `AS` keyword. The clause is stripped from the query (so DuckDB's parser doesn't see it) and the interval is parsed into seconds. Minimum is 1 minute.

```sql
CREATE MATERIALIZED VIEW mv REFRESH EVERY '5 minutes' AS
    SELECT region, SUM(amount) FROM sales GROUP BY region;
```

The parsed interval (300 seconds) is stored in the `refresh_interval` column of `openivm_views`. When omitted, `refresh_interval` is `NULL` (manual refresh only). See [Automatic Refresh](../refresh/automatic-refresh.md) for how the daemon uses this.

## IVM compatibility classification

After rewriting, the parser plans the query and walks the logical plan to classify the view into a refresh type:

| Type | Code | Condition |
|---|---|---|
| `AGGREGATE_GROUP` | 0 | Aggregation with GROUP BY columns (including rewritten DISTINCT). |
| `SIMPLE_AGGREGATE` | 1 | Aggregation without GROUP BY (global aggregate). |
| `SIMPLE_PROJECTION` | 2 | Projection or filter, no aggregation. |
| `FULL_REFRESH` | 3 | Contains unsupported constructs. |
| `AGGREGATE_HAVING` | 4 | Aggregation with GROUP BY and HAVING clause. Uses group-recompute since groups may enter/leave the result set. |
| `WINDOW_PARTITION` | 5 | Window functions maintained by partition recompute. |
| `GROUP_RECOMPUTE` | 6 | Affected-key DELETE + INSERT for non-linear group shapes. |
| `TOP_K` | 7 | Legacy enum value. Current top-k support stores the full data table and applies ORDER BY/LIMIT in the user-facing view. |
| `DISTINCT_INCREMENTAL` | 8 | Aux-state path for supported inner-DISTINCT-under-aggregate shapes. |
| `SEMI_ANTI_RECOMPUTE` | 9 | Aux-state path for supported SEMI/ANTI/EXISTS projection shapes. |

The IVM compatibility checker validates the entire plan tree, flagging unsupported join shapes, unsupported aggregate functions, and non-deterministic functions (e.g., `RANDOM()`, `NOW()`). Supported join plans include inner joins, cross products, arbitrary-predicate joins, left/right/full outer joins, and the aux-state semi/anti projection shapes. Supported aggregate functions include `COUNT`, `SUM`, `MIN`, `MAX`, `AVG`, `LIST`, `STDDEV`/`VARIANCE`, `BOOL_AND`, `BOOL_OR`, `ARG_MIN`, and `ARG_MAX`. If any unsupported construct is found, the view is classified as `FULL_REFRESH` and a warning is printed.

## Generated DDL

The parser produces a sequence of DDL statements executed during the bind phase:

1. **System tables**: `CREATE TABLE IF NOT EXISTS openivm_views (...)` and `openivm_delta_tables (...)`.
2. **Metadata inserts**: Registers the view name, query string, type, and source table mappings.
3. **MV table**: `CREATE TABLE <view_name> AS <query>` to materialize the initial result.
4. **Delta tables**: One `openivm_delta_<table_name>` per source table, with `openivm_multiplicity` and `openivm_timestamp` columns.
5. **Delta view table**: `openivm_delta_<view_name>` for downstream chained MV support, with `DEFAULT now()` on the timestamp column.
6. **Index** (AGGREGATE_GROUP only): A unique index on the GROUP BY columns, used by the MERGE INTO upsert strategy.

## System tables

### `openivm_views`

Stores one row per materialized view.

| Column | Type | Description |
|---|---|---|
| `view_name` | `VARCHAR` (PK) | Name of the materialized view. |
| `sql_string` | `VARCHAR` | The original SELECT query defining the view. |
| `type` | `TINYINT` | View classification (see IVM compatibility classification above). |
| `has_minmax` | `BOOLEAN` | Whether the view uses MIN/MAX or another aggregate shape that may need group-recompute. |
| `has_left_join` | `BOOLEAN` | Whether the view involves a LEFT/RIGHT JOIN. |
| `last_update` | `TIMESTAMP` | When the view was last created or replaced. |
| `refresh_interval` | `BIGINT` | Automatic refresh interval in seconds. `NULL` = manual only. See [Automatic Refresh](../refresh/automatic-refresh.md). |
| `refresh_in_progress` | `BOOLEAN` | Crash safety flag — `true` while a refresh is in flight. See [Automatic Refresh: Crash safety](../refresh/automatic-refresh.md#crash-safety). |
| `group_columns` | `VARCHAR` | Comma-separated group keys, window partition keys, or source mappings used by refresh. |
| `aggregate_types` | `VARCHAR` | Aggregate function names used by aggregate refresh compilation. |
| `having_predicate` | `VARCHAR` | Stored HAVING predicate for user-facing view filtering. |
| `has_full_outer` | `BOOLEAN` | Whether the view contains a FULL OUTER JOIN. |
| `full_outer_join_cols` | `VARCHAR` | Join-key metadata for FULL OUTER recompute paths. |
| `source_tables_json` | `VARCHAR` | JSON list of source tables used for dependency tracking. |
| `aggregate_decomposition_json` | `VARCHAR` | JSON metadata for aggregate helper paths, including filtered group-count aux state. |
| `distinct_aux_meta_json` | `VARCHAR` | JSON metadata for DISTINCT aux-state maintenance. |
| `semi_anti_aux_meta_json` | `VARCHAR` | JSON metadata for SEMI/ANTI aux-state maintenance. |
| `lineage_json` | `VARCHAR` | JSON lineage metadata for window and projection-key refresh paths. |
| `signature_hash`, `canonical_plan_blob`, `output_columns_json`, `predicate_summary_json`, `fd_summary_json`, `nullified_columns_json` | Mixed | View-matching metadata. These stay NULL unless view matching is enabled. |

Example content:

| view_name | sql_string | type | group_columns | refresh_interval | refresh_in_progress |
|---|---|---|---|---|---|
| `mv_grouped` | `select region, sum(amount) ...` | 0 | `region` | 300 | false |
| `mv_projection` | `select id, name from customers` | 2 | NULL | NULL | false |

### `openivm_delta_tables`

Tracks which delta tables feed each materialized view, along with the timestamp of the last refresh.

| Column | Type | Description |
|---|---|---|
| `view_name` | `VARCHAR` | Name of the materialized view. |
| `table_name` | `VARCHAR` | Name of the delta table (e.g., `openivm_delta_sales`). |
| `last_update` | `TIMESTAMP` | Timestamp of the last refresh for this view-table pair. |
| `catalog_type` | `VARCHAR` | `duckdb` or `ducklake`. |
| `last_snapshot_id` | `BIGINT` | Last consumed DuckLake snapshot for DuckLake sources. |
| `last_refresh_ts` | `TIMESTAMP` | Refresh wall-clock cursor used for chained MV companion rows. |
| `pending_row_estimate` | `BIGINT` | Cached pending-delta estimate for cost/model matching paths. |
| `pending_estimate_ts` | `TIMESTAMP` | Timestamp for the cached pending estimate. |
| `source_catalog` | `VARCHAR` | Source table catalog for cross-catalog refresh. |
| `source_schema` | `VARCHAR` | Source table schema for cross-schema refresh. |
| `source_table_id` | `BIGINT` | DuckLake source-table identity used across renames and catalog lookups. |

The primary key is the composite `(view_name, table_name)`.

Example content:

| view_name | table_name | last_update | catalog_type |
|---|---|---|---|
| `mv_grouped` | `openivm_delta_sales` | 2026-03-27 10:05:00 | `duckdb` |
| `mv_join` | `openivm_delta_orders` | 2026-03-27 10:05:00 | `duckdb` |
| `mv_ducklake` | `orders` | NULL | `ducklake` |
