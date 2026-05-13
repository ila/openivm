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

## Refresh cursor advance — race-safe timestamp bookkeeping

Each `(view, base_table)` pair tracks two timestamps in `_duckdb_ivm_delta_tables`:

| Column | Set to | Used by |
|---|---|---|
| `last_update` | `MAX(_duckdb_ivm_timestamp) + 1µs` over rows visible in *this transaction's snapshot*. Falls back to `now()` if the snapshot saw zero delta rows. | The base-delta scan filter on the *next* refresh: `_duckdb_ivm_timestamp >= last_update`. |
| `last_refresh_ts` | `now()` at refresh-transaction-start wall clock. | Filtering `delta_<view>` companion rows from chained refreshes (companion rows carry refresh-time timestamps, not base-row timestamps, so they need a separate cursor). |

`last_update` is anchored to `MAX(base_ts)+1µs` rather than `now()` to make the cursor race-safe. The naive `now()` approach has a subtle bug:

1. `BEGIN TRANSACTION` evaluates `now()` *before* the first catalog access takes a snapshot.
2. A concurrent DML commits between BEGIN and snapshot-read, with timestamp slightly after `now()` but visible in our snapshot.
3. We process this row this refresh.
4. We set `last_update = now()` (which is *less than* this row's ts).
5. The next refresh's filter `ts >= last_update` includes this row again → double-application → MV drift.

Anchoring `last_update` to the maximum timestamp we *actually* processed eliminates the gap: the next refresh's filter excludes everything we've seen and includes everything we haven't. See `src/upsert/refresh.cpp:1370–1403` for the implementation.

## Lock hierarchy

| Lock | Scope | Held during | Used by |
|---|---|---|---|
| View mutex | Per view name | Entire refresh cycle | `PRAGMA ivm()`, refresh daemon |
| Delta mutex | Per delta table name | Delta row insertion | Insert rule (DML triggers) |
| Map mutex | Global (static) | Mutex map lookup | Internal — protects the mutex maps |

All locks are non-recursive mutexes. The view mutex is the outermost lock.
