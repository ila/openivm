# Automatic Refresh

OpenIVM can automatically refresh materialized views on a schedule using `REFRESH EVERY`. A background daemon thread checks for due views every 30 seconds and refreshes them using the same `PRAGMA ivm()` pipeline.

## Syntax

```sql
-- Refresh every 5 minutes
CREATE MATERIALIZED VIEW mv REFRESH EVERY '5 minutes' AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;

-- No automatic refresh (default — manual PRAGMA ivm() only)
CREATE MATERIALIZED VIEW mv AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;
```

Supported intervals: `N minutes`, `N hours`, `N days`. Minimum is 1 minute.

Manual `PRAGMA ivm()` still works on views with `REFRESH EVERY` — the two mechanisms coexist.

## How it works

The refresh daemon is a background `std::thread` started at extension load. It:

1. Wakes every 30 seconds
2. Queries `_duckdb_ivm_views` for views with `refresh_interval IS NOT NULL`
3. For each view where `now() - last_update >= interval`: calls `PRAGMA ivm('view_name')`
4. Skips views that are already being refreshed (via `TryLockView`)

The daemon holds a non-owning reference to the database and exits cleanly when the database is destroyed.

## Checking refresh status

```sql
PRAGMA ivm_status('mv');
```

Returns a single row:

| Column | Type | Description |
|---|---|---|
| `view_name` | VARCHAR | Name of the materialized view |
| `refresh_interval` | BIGINT | Configured interval in seconds (NULL if manual) |
| `last_refresh` | TIMESTAMP | When the last refresh completed |
| `next_refresh` | TIMESTAMP | Estimated next refresh (last_refresh + interval) |
| `status` | VARCHAR | `idle` or `refreshing` |
| `effective_interval` | BIGINT | Current interval after backoff (same as configured if no backoff) |

## Adaptive backoff

When a refresh takes longer than its interval (e.g., a 3-minute refresh on a 1-minute interval), the daemon doubles the effective interval to avoid running continuously. The effective interval is capped at 24 hours and resets when a refresh completes within the interval.

```
Warning: refresh of 'mv_sales' took 185s (interval: 60s).
Increasing effective interval to 120s. Set ivm_adaptive_backoff = false to disable.
```

Backoff is runtime-only — the configured `refresh_interval` in metadata is never modified. Restarting the database resets all backoffs. Disable with:

```sql
SET ivm_adaptive_backoff = false;
```

## Crash safety

If the process crashes mid-refresh (after the MERGE updates the MV but before `last_update` is advanced), the same deltas could be double-applied on restart. OpenIVM detects this via a `refresh_in_progress` flag in `_duckdb_ivm_views`:

- Set to `true` before the IVM query starts
- Cleared after `last_update` is set
- If still `true` on the next refresh → automatic full recompute to recover

```
Warning: recovering 'mv_sales' from interrupted refresh via full recompute.
```

This adds two small UPDATE statements per refresh cycle (one before, one after the critical section). The flag is never visible to users during normal operation.

## Concurrency

Automatic refresh uses the same per-view locking as manual `PRAGMA ivm()`:

- **Reads during refresh**: always safe (DuckDB MVCC — readers see a consistent snapshot)
- **Two concurrent refreshes of the same view**: serialized by a per-view mutex. The daemon skips views that are locked (e.g., by a manual PRAGMA), and manual PRAGMAs wait for the daemon to finish.
- **DML during refresh**: safe. A per-delta-table mutex in the insert rule prevents delta writes from racing with the refresh's timestamp logic.

## Configuration

| Setting | Type | Default | Description |
|---|---|---|---|
| `ivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh takes longer than the interval |

The refresh interval itself is per-view, set at creation time via `REFRESH EVERY`.

## Metadata

The `refresh_interval` and `refresh_in_progress` columns are stored in `_duckdb_ivm_views`:

```sql
SELECT view_name, refresh_interval, refresh_in_progress
FROM _duckdb_ivm_views
WHERE refresh_interval IS NOT NULL;
```

| view_name | refresh_interval | refresh_in_progress |
|---|---|---|
| mv_sales | 300 | false |
| mv_orders | 60 | false |
