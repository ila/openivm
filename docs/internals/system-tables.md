# System Tables

OpenIVM stores view metadata in two system tables, created automatically when the first materialized view is defined.

## _duckdb_ivm_views

Stores one row per materialized view.

| Column | Type | Description |
|---|---|---|
| `view_name` | `VARCHAR` (PK) | Name of the materialized view. |
| `sql_string` | `VARCHAR` | The original SELECT query defining the view. |
| `type` | `TINYINT` | View classification for refresh strategy. |
| `last_update` | `TIMESTAMP` | When the view was last created or replaced. |

### Type values

| Value | Name | Description |
|---|---|---|
| 0 | `AGGREGATE_GROUP` | Aggregation with GROUP BY. Uses MERGE INTO upsert. |
| 1 | `SIMPLE_AGGREGATE` | Global aggregate (no GROUP BY). Uses single-row UPDATE. |
| 2 | `SIMPLE_PROJECTION` | Projection or filter, no aggregation. Uses counting-based insert/delete. |
| 3 | `FULL_REFRESH` | Cannot be maintained incrementally. Uses DELETE + INSERT. |

### Example content

| view_name | sql_string | type | last_update |
|---|---|---|---|
| `mv_grouped` | `select region, sum(amount) as sum_amount, count(*) as count_star from sales group by region` | 0 | 2026-03-27 10:00:00 |
| `mv_projection` | `select id, name from customers` | 2 | 2026-03-27 10:01:00 |

## _duckdb_ivm_delta_tables

Tracks which delta tables feed each materialized view, along with the timestamp of the last refresh.

| Column | Type | Description |
|---|---|---|
| `view_name` | `VARCHAR` | Name of the materialized view. |
| `table_name` | `VARCHAR` | Name of the delta table (e.g., `delta_sales`). |
| `last_update` | `TIMESTAMP` | Timestamp of the last refresh for this view-table pair. |

The primary key is the composite `(view_name, table_name)`.

### Example content

| view_name | table_name | last_update |
|---|---|---|
| `mv_grouped` | `delta_sales` | 2026-03-27 10:05:00 |
| `mv_join` | `delta_orders` | 2026-03-27 10:05:00 |
| `mv_join` | `delta_customers` | 2026-03-27 10:05:00 |

The `last_update` column is used for delta cleanup: rows in a delta table are only removed once all dependent views have a `last_update` newer than the delta row's `_duckdb_ivm_timestamp`.
