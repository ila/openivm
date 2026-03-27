# Delta Tables

Delta tables record row-level changes (inserts, deletes, updates) to base tables and materialized views. OpenIVM uses them to compute incremental refreshes without scanning the full base data.

## Naming

Delta tables follow the convention `delta_<table_name>`. For a base table named `sales`, the delta table is `delta_sales`. For a materialized view named `region_totals`, the delta view table is `delta_region_totals`.

## Schema

A delta table has the same columns as its source table, plus two metadata columns:

| Column | Type | Description |
|---|---|---|
| `_duckdb_ivm_multiplicity` | `BOOLEAN` | `true` = inserted row, `false` = deleted row. |
| `_duckdb_ivm_timestamp` | `TIMESTAMP` | When the change was recorded. Defaults to `now()`. |

### Example

Given a base table:

```sql
CREATE TABLE sales (region VARCHAR, amount INTEGER);
```

The delta table `delta_sales` has the schema:

| Column | Type |
|---|---|
| `region` | `VARCHAR` |
| `amount` | `INTEGER` |
| `_duckdb_ivm_multiplicity` | `BOOLEAN` |
| `_duckdb_ivm_timestamp` | `TIMESTAMP` |

## Change tracking

OpenIVM installs an optimizer insert rule that intercepts DML statements on tracked tables and writes corresponding rows to the delta table automatically.

- **INSERT**: Each inserted row is copied to the delta table with `_duckdb_ivm_multiplicity = true`.
- **DELETE**: Each deleted row is copied to the delta table with `_duckdb_ivm_multiplicity = false`.
- **UPDATE**: Decomposed into a delete of the old row (`false`) followed by an insert of the new row (`true`). Both rows are written to the delta table.

All delta rows receive a `_duckdb_ivm_timestamp` of `now()` at the time of the DML operation. The user does not interact with delta tables directly.

## Timestamp-based cleanup

Delta rows are not removed immediately after a refresh. A base table's deltas can only be cleaned up once **all** materialized views that depend on that table have been refreshed past the delta's timestamp. OpenIVM tracks the `last_update` timestamp per view-table pair in the `_duckdb_ivm_delta_tables` system table, and deletes delta rows whose timestamp is older than the minimum `last_update` across all dependent views.

## Delta view tables

For chained materialized views, OpenIVM also creates a delta table for each MV (e.g., `delta_region_totals`). When a refresh computes the incremental change to an MV, the resulting delta rows are written into the MV's delta table so that downstream views can consume them. The `_duckdb_ivm_timestamp` column on delta view tables uses `DEFAULT now()` so that downstream refreshes can filter by time.
