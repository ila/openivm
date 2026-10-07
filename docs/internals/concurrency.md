# Concurrency

This page covers the mechanics. For the guarantees users can rely on and operating advice, see
[concurrency and operations](../concurrency.md).

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

Autocommit refresh uses the locked helper connection because DuckDB's query-pragma preprocessing ends the caller's
transaction before the generated program runs; this stays until refresh has a native operator. Inside an explicit
transaction, OpenIVM can't rely on precomputed delta activity, so it compiles conservatively (for example without
empty-delta skipping).

For DuckLake tables, the snapshot is determined by the `DuckLakeFunctionInfo::snapshot_id`
bound at plan time. `AT VERSION` pinning reads exactly the state at that snapshot.

## Refresh cursor advance — race-safe timestamp bookkeeping

Each `(view, base_table)` pair tracks two timestamps in `openivm_delta_tables`:

| Column | Set to | Used by |
|---|---|---|
| `last_update` | `MAX(openivm_timestamp) + 1µs` over rows visible in *this transaction's snapshot*. Falls back to the current UTC time (`make_timestamp(epoch_us(now()))`) if the snapshot saw zero delta rows. | The base-delta scan filter on the *next* refresh: `openivm_timestamp >= last_update`. |
| `last_refresh_ts` | Current UTC time at refresh-transaction start. | Filtering `openivm_delta_<view>` companion rows from chained refreshes (companion rows carry refresh-time timestamps, not base-row timestamps, so they need a separate cursor). |

`last_update` is anchored to `MAX(base_ts)+1µs` rather than `now()` to make the cursor race-safe. The naive `now()` approach has a subtle bug:

1. `BEGIN TRANSACTION` evaluates `now()` *before* the first catalog access takes a snapshot.
2. A concurrent DML commits between BEGIN and snapshot-read, with timestamp slightly after `now()` but visible in our snapshot.
3. We process this row this refresh.
4. We set `last_update = now()` (which is *less than* this row's ts).
5. The next refresh's filter `ts >= last_update` includes this row again → double-application → MV drift.

Anchoring `last_update` to the maximum timestamp we *actually* processed eliminates the gap: the next refresh's filter excludes everything we've seen and includes everything we haven't. See `GenerateRefreshSQL()` in `src/upsert/refresh_sql.cpp` for the implementation.

All watermark and captured delta timestamps are UTC: transactional delta capture stamps
rows with `Timestamp::GetCurrentTimestamp()`, and generated SQL uses
`make_timestamp(epoch_us(now()))` (`openivm::UTC_NOW_SQL`) instead of casting
`now()`, which would apply the session time zone.

## Locking

| Lock | Scope | Held during | Used by |
|---|---|---|---|
| Mutation gate | Per DuckDB database instance | Entire explicit transaction or autocommit OpenIVM mutation | Delta capture, refresh, lifecycle DDL, `ALTER TABLE` on tracked tables, first-time metadata setup |

The gate (`MutationGate` in `src/core/refresh_locks.cpp`) is stored in the database
instance's object cache, so its lifetime is tied to that database and it is never
evicted. Delta capture acquires it when a DML statement writes its first delta row.
`TransactionalMVLockState` retains the guard through commit or rollback.

First-time creation of the shared metadata tables and native source delta tables runs
in its own short transaction under the gate; view planning and initial materialization
are not serialized by it. Inside an explicit transaction this setup instead stays part
of the caller transaction and rolls back with it.

When a refresh consumes native source deltas stored in another attached DuckDB
database, their cleanup is deferred until the caller transaction commits
(`TransactionalMVLockState::DeferDeltaCleanup`). Rollback discards it. If the deferred
cleanup fails, the refresh still reports success and prints that cleanup was deferred;
the committed watermark prevents the retained rows from being applied twice.

## Metadata in another database

With `openivm_metadata_catalog` naming another database than the view, the MV data and
its watermarks cannot commit in one DuckDB transaction. Autocommit refresh then commits
an interruption marker, the data and the watermarks in three transactions, and deletes
consumed source deltas only after the watermarks commit. Any interruption leaves the
marker set, so the next refresh recomputes. Explicit transactions that would need both
databases are rejected. Clients sharing a PostgreSQL metadata schema also take a renewed
per-view lease (`openivm_refresh_leases`), stop writing data once a local deadline a third
of the lease before its expiry passes, and fence the watermark commit with a write to
their lease row. See
[metadata placement](metadata-placement.md) for the full protocol and its failure table.
