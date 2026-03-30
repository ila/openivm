# Delta Tables

Delta tables record row-level changes (inserts, deletes, updates) to base tables and materialized views. OpenIVM uses them to compute incremental refreshes without scanning the full base data.

A shared delta table lets each view read only the changes it has not yet processed, without re-scanning the base table or coordinating with other views. Further, an UPDATE replaces a row in-place in the base table. The delta table stores both the old row (multiplicity = false) and the new row (multiplicity = true), preserving the full before-and-after picture.

Delta tables follow the convention `delta_<table_name>`. For a base table named `sales`, the delta table is `delta_sales`. For a materialized view named `region_totals`, the delta view table is `delta_region_totals`.

## Schema

A delta table has the same columns as its source table, plus two metadata columns:

| Column | Type | Description |
|---|---|---|
| `_duckdb_ivm_multiplicity` | `BOOLEAN` | `true` = inserted row, `false` = deleted row. |
| `_duckdb_ivm_timestamp` | `TIMESTAMP` | When the change was recorded. Defaults to `now()`. |

OpenIVM also creates a delta table for each MV. When a refresh computes the incremental change to an MV, the resulting delta rows are written into the MV's delta table so that downstream views can consume them.

## Change tracking

OpenIVM implements an optimizer insert rule that intercepts DML statements on tracked tables and writes corresponding rows to the delta table automatically.

- **INSERT**: Each inserted row is copied to the delta table with `_duckdb_ivm_multiplicity = true`.
- **DELETE**: Each deleted row is copied to the delta table with `_duckdb_ivm_multiplicity = false`.
- **UPDATE**: Decomposed into a delete of the old row (`false`) followed by an insert of the new row (`true`). Both rows are written to the delta table.

All delta rows receive a `_duckdb_ivm_timestamp` of `now()` at the time of the DML operation, in a transparent way which does not require user interaction.

## Timestamp-based cleanup

Delta rows are not removed immediately after a refresh: a base table's deltas can only be cleaned up once **all** materialized views that depend on that table have been refreshed past the delta's timestamp. 

## Refresh flow

When refreshing a MV, OpenIVM follows this sequence:

1. **Scan delta tables.** Read rows from each `delta_<base_table>` where `_duckdb_ivm_timestamp >= last_refresh_timestamp`.
2. **Compute the delta query.** Apply the view's incremental operator tree (filter, project, join, aggregate) to the delta rows. This produces the change to the materialized view — the "delta of the MV."
3. **Write to delta view.** INSERT the computed delta rows into `delta_<view_name>` so that downstream chained views can consume them.
4. **Upsert into the MV.** Apply the delta to the materialized view table using MERGE (grouped aggregates), counting-based INSERT/DELETE (projections), or single-row UPDATE (ungrouped aggregates). See the [operator docs](../operators/) for details on each strategy.
5. **Clean up.** Update the `last_update` timestamp in `_duckdb_ivm_delta_tables` and delete consumed delta rows.

For chained views, [companion rows](../optimizations/companion-rows.md) ensure downstream consumers see correct old-to-new state transitions. See [Pipelines](../refresh/pipelines.md) for cascade mode details.
