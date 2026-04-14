# Concurrency

## Refresh serialization

Each materialized view has a per-view mutex. When `PRAGMA ivm('view_name')` runs, it
acquires the view's lock before generating or executing any SQL. This prevents two
concurrent refresh calls from applying overlapping deltas to the same view.

The [automatic refresh daemon](../refresh/automatic-refresh.md) uses `TryLockView()` —
if the view is already being refreshed, the daemon skips it and retries at the next
interval.

## Delta table safety

Each delta table has a per-delta-table mutex. The insert rule acquires the delta lock
when writing DML-triggered rows into a delta table. This prevents concurrent INSERTs
from interleaving delta rows in a way that breaks timestamp ordering.

## Snapshot isolation

Refresh executes SQL through a separate `Connection` from the user's session. DuckDB
provides snapshot isolation per transaction, so:

- The refresh reads a consistent snapshot of base tables and delta tables
- Concurrent DML by other connections does not affect the in-progress refresh
- Delta rows written by concurrent DML after the refresh's snapshot are not seen

For DuckLake tables, the snapshot is determined by the `DuckLakeFunctionInfo::snapshot_id`
bound at plan time. `AT VERSION` pinning reads exactly the state at that snapshot.

## Lock hierarchy

| Lock | Scope | Held during | Used by |
|---|---|---|---|
| View mutex | Per view name | Entire refresh cycle | `PRAGMA ivm()`, refresh daemon |
| Delta mutex | Per delta table name | Delta row insertion | Insert rule (DML triggers) |
| Map mutex | Global (static) | Mutex map lookup | Internal — protects the mutex maps |

All locks are non-recursive mutexes. The view mutex is the outermost lock.
