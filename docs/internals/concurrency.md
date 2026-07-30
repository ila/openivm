# Concurrency

## Refresh serialization

Each materialized view has a per-view mutex. When `PRAGMA refresh('view_name')` runs, it
acquires the view's lock before generating or executing any SQL. This prevents two
concurrent refresh calls from applying overlapping deltas to the same view.

The [automatic refresh daemon](../refresh/automatic-refresh.md) uses `TryLockView()` —
if the view is already being refreshed, the daemon skips it and retries at the next
interval.

## Delta table safety

Native delta writers and refreshes coordinate through a phase gate per catalog. A
writer reserves its delta relation for the lifetime of the caller transaction.
A refresh atomically reserves its complete source set after existing writers drain,
which prevents a multi-source refresh from deadlocking through lock-order inversion.
Once a refresh is waiting, existing writer transactions may extend their write set
but new writers for the reserved sources wait.

Refreshes that share a delta relation additionally use a per-delta mutex while
consuming and checkpointing it. Relation lock identities include catalog and schema,
so unrelated same-named objects do not serialize.

An explicit transaction retains its view, phase-gate, and delta locks until commit
or rollback. A transaction that already owns a refresh is rejected with a
serialization error if a later write would wait on another refresh and create a
cycle; the caller can retry either transaction.

## Snapshot isolation

Autocommit refresh executes through a locked helper connection. Refresh inside an
explicit transaction compiles metadata through a helper but executes the generated
program in the caller transaction, so transaction-local DML and MV lifecycle changes
remain visible and atomic. DuckDB snapshot isolation ensures:

- The refresh reads a consistent snapshot of base tables and delta tables
- Concurrent DML by other connections does not affect the in-progress refresh
- Delta rows written by concurrent DML after the refresh's snapshot are not seen

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

Anchoring `last_update` to the maximum timestamp we *actually* processed eliminates the gap: the next refresh's filter excludes everything we've seen and includes everything we haven't. See `src/upsert/refresh.cpp:1370–1403` for the implementation.

## Lock hierarchy

| Lock | Scope | Held during | Used by |
|---|---|---|---|
| View mutex | Per catalog/schema/view identity | Entire refresh or lifecycle operation | `PRAGMA refresh()`, lifecycle DDL, refresh daemon |
| Catalog phase gate | Per source catalog and schema-qualified delta identity | Writer or refresh transaction | DML delta capture, refresh |
| Delta mutex | Per catalog/schema/delta identity | Delta consumption and checkpoint | Overlapping refreshes |
| Map mutex | Global (static) | Lock-map lookup | Internal — protects the lock maps |

View locks are acquired before source metadata is read. Refreshes then reserve their
complete phase-gate sets before taking sorted delta mutexes. Transactional lock state
retains those guards through commit or rollback.
