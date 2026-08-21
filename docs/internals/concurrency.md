# Concurrency

## Mutation serialization

OpenIVM serializes tracked source-table writes, refreshes, and materialized-view
lifecycle operations through one database-wide mutation gate. An explicit transaction
retains the gate until commit or rollback. Helper connections use the same logical
owner, making the gate re-entrant even when DuckDB executes work on another thread.

This coarse boundary prevents refresh/write and parent/child refresh races without a
multi-lock hierarchy. Unrelated OpenIVM mutations in the same database also serialize;
ordinary reads remain concurrent. The [automatic refresh daemon](../refresh/automatic-refresh.md)
waits behind an active mutation and refreshes once it acquires the gate.

## Snapshot isolation

Autocommit refresh executes through a locked helper connection. Refresh inside an
explicit transaction compiles metadata through a helper but executes the generated
program in the caller transaction, so transaction-local DML and MV lifecycle changes
remain visible and atomic. The mutation gate prevents another tracked writer or
refresh from changing OpenIVM state while the refresh is active. DuckDB snapshot
isolation additionally ensures:

- The refresh reads a consistent snapshot of base tables and delta tables
- The refresh sees transaction-local changes made before it acquired its snapshot
- Non-OpenIVM activity cannot change the refresh's visible snapshot

For DuckLake tables, the snapshot is determined by the `DuckLakeFunctionInfo::snapshot_id`
bound at plan time. `AT VERSION` pinning reads exactly the state at that snapshot.

## Refresh cursor advance — race-safe timestamp bookkeeping

Each `(view, base_table)` pair tracks two timestamps in `openivm_delta_tables`:

| Column | Set to | Used by |
|---|---|---|
| `last_update` | `MAX(openivm_timestamp) + 1µs` over rows visible in *this transaction's snapshot*. Falls back to `now()` if the snapshot saw zero delta rows. | The base-delta scan filter on the *next* refresh: `openivm_timestamp >= last_update`. |
| `last_refresh_ts` | `now()` at refresh-transaction-start wall clock. | Filtering `openivm_delta_<view>` companion rows from chained refreshes (companion rows carry refresh-time timestamps, not base-row timestamps, so they need a separate cursor). |

`last_update` is anchored to `MAX(base_ts)+1µs` rather than `now()` to make the cursor race-safe. The naive `now()` approach has a subtle bug:

1. `BEGIN TRANSACTION` evaluates `now()` *before* the first catalog access takes a snapshot.
2. A concurrent DML commits between BEGIN and snapshot-read, with timestamp slightly after `now()` but visible in our snapshot.
3. We process this row this refresh.
4. We set `last_update = now()` (which is *less than* this row's ts).
5. The next refresh's filter `ts >= last_update` includes this row again → double-application → MV drift.

Anchoring `last_update` to the maximum timestamp we *actually* processed eliminates the gap: the next refresh's filter excludes everything we've seen and includes everything we haven't. See `GenerateRefreshSQL()` in `src/upsert/refresh_sql.cpp` for the implementation.

## Locking

| Lock | Scope | Held during | Used by |
|---|---|---|---|
| Mutation gate | Per DuckDB database instance | Entire explicit transaction or autocommit OpenIVM mutation | Delta capture, refresh, lifecycle DDL |
| Map mutex | Global (static) | Mutation-gate lookup | Internal — protects the gate map |

Transactional lock state retains the mutation guard through commit or rollback.
