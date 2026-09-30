# Delta Tables

Delta tables record row-level changes (inserts, deletes, updates) to base tables and materialized views. OpenIVM uses them to compute incremental refreshes without scanning the full base data.

A shared delta table lets each view read only the changes it has not yet processed, without re-scanning the base table or coordinating with other views. Further, an UPDATE replaces a row in-place in the base table. The delta table stores both the old row (multiplicity `-1`) and the new row (multiplicity `+1`), preserving the full before-and-after picture.

Delta tables follow the convention `openivm_delta_<table_name>`. For a base table named `sales`, the delta table is `openivm_delta_sales`, created in the source table's catalog and schema. A materialized view's delta table is named after its internal key rather than its SQL name: `openivm_delta_<internal_key>`, next to its data table (see [Metadata Columns](metadata-columns.md#the-user-facing-view)). Views also keep `openivm_delta_openivm_visible_<internal_key>`, the signed delta of the user-visible output that chained views consume.

## Schema

A delta table has the same columns as its source table, plus two metadata columns:

| Column | Type | Description |
|---|---|---|
| `openivm_multiplicity` | `INTEGER` | Signed Z-set weight: `+1` = inserted row, `-1` = deleted row. Joins multiply weights and apply a Möbius inclusion-exclusion sign — see [`inner-join.md`](../operators/inner-join.md). |
| `openivm_timestamp` | `TIMESTAMP` | When the change was recorded, in UTC. |

OpenIVM also creates a delta table for each MV. When a refresh computes the incremental change to an MV, the resulting delta rows are written into the MV's delta table so that downstream views can consume them.

## Change tracking

The `RefreshInsertRule` optimizer extension (`src/rules/refresh_insert_rule.cpp`) finds `INSERT`, `DELETE`, `UPDATE`, and `MERGE INTO` statements on tracked tables and inserts a streaming capture operator (`src/rules/transactional_delta_capture.cpp`) into their plan. The operator appends the affected rows to the delta table in the caller's transaction, so delta rows commit or roll back together with the DML.

- **INSERT**: Each inserted row is copied to the delta table with `openivm_multiplicity = 1`.
- **DELETE**: Each deleted row is copied to the delta table with `openivm_multiplicity = -1`.
- **UPDATE**: Decomposed into a retraction of the old row (`-1`) and an insertion of the new row (`+1`), written by the same statement.
- **MERGE INTO**, `INSERT ... ON CONFLICT`, `INSERT OR REPLACE`: only rows that are actually inserted, updated, or deleted are captured; rows skipped by `DO NOTHING` or a failing `DO UPDATE ... WHERE` produce no delta.

Delta columns are matched to base columns by name, and generated columns are evaluated for the delta row. All delta rows receive an `openivm_timestamp` of the current UTC time (`Timestamp::GetCurrentTimestamp()`). Writes through a DuckDB process without OpenIVM loaded are not captured.

## Timestamp-based cleanup

Delta rows are not removed immediately after a refresh: a base table's deltas can only be cleaned up once **all** materialized views that depend on that table have been refreshed past the delta's timestamp. After each refresh, OpenIVM deletes rows whose `openivm_timestamp` is below the minimum `last_update` of all consumers recorded in `openivm_delta_tables`, including consumers whose metadata lives in another attached native catalog (`RefreshMetadata::BuildDeltaCleanupSQL`). Cleanup of source deltas stored in another attached database is deferred until commit; see [Concurrency](concurrency.md#locking).

## Refresh flow

When refreshing a MV, OpenIVM follows this sequence:

1. **Scan delta tables.** Read rows from each `openivm_delta_<base_table>` where `openivm_timestamp >= last_update`. DuckLake sources read changes between the stored and current snapshot instead; see [DuckLake](../ducklake.md).
2. **Compute the delta query.** Apply the view's incremental operator tree (filter, project, join, aggregate) to the delta rows. This produces the change to the materialized view — the "delta of the MV."
3. **Write to delta view.** INSERT the computed delta rows into `openivm_delta_<internal_key>`.
4. **Upsert into the MV.** Apply the delta to the materialized view table using MERGE (grouped aggregates), counting-based INSERT/DELETE (projections), or single-row UPDATE (ungrouped aggregates). See the [operator docs](../operators/) for details on each strategy.
5. **Publish.** Recompute the user-visible output of the affected rows and apply the difference to `openivm_visible_<internal_key>`, writing the signed changes to `openivm_delta_openivm_visible_<internal_key>` for downstream views.
6. **Clean up.** Advance the cursor in `openivm_delta_tables` and delete consumed delta rows. Two timestamps are bumped per `(view, table)`:
    - `last_update` is set to `MAX(openivm_timestamp) + 1µs` over rows visible in this transaction's snapshot (the current UTC time if none remain) — *not* `now()`. Using `now()` was unsafe because a row committed between transaction begin and snapshot read could end up with a timestamp ≤ `now()` while still being processed by the next refresh, double-applying it. Anchoring to `MAX(base_ts)+1µs` guarantees the next refresh's `ts ≥ last_update` filter both excludes everything we just processed and includes anything newer than our snapshot.
    - `last_refresh_ts` is set to the current UTC time at transaction start. This is used to filter `openivm_delta_<view>` companion rows produced by chained refreshes — companion rows carry timestamps near `now()` rather than base-row timestamps, so they need a separate cursor.

For chained views, [companion rows](../optimizations/companion-rows.md) ensure downstream consumers see correct old-to-new state transitions. See [Pipelines](../refresh/pipelines.md) for cascade mode details.
