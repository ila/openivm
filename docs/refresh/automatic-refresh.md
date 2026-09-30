# Automatic Refresh

OpenIVM can automatically refresh materialized views on a schedule using `REFRESH EVERY`. A background daemon thread checks for due views every 30 seconds and refreshes them using the same `PRAGMA refresh()` pipeline.

## Syntax

```sql
-- Refresh every 5 minutes
CREATE MATERIALIZED VIEW mv REFRESH EVERY '5 minutes' AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;

-- No automatic refresh (default — manual PRAGMA refresh() only)
CREATE MATERIALIZED VIEW mv AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;
```

Supported intervals: `N minutes` (also `minute`, `min`), `N hours` (`hour`), `N days` (`day`), with `N` a positive integer. Minimum is 1 minute.

Change or remove the schedule of an existing view with `ALTER MATERIALIZED VIEW`:

```sql
ALTER MATERIALIZED VIEW mv SET REFRESH EVERY '1 hour';
ALTER MATERIALIZED VIEW mv SET REFRESH MANUAL;  -- clears refresh_interval
```

Manual `PRAGMA refresh()` still works on views with `REFRESH EVERY` — the two mechanisms coexist.

## How it works

The refresh daemon is a background `std::thread` started at extension load. It:

1. Wakes every 30 seconds
2. Queries `openivm_views` in every attached catalog that has OpenIVM metadata for views with `refresh_interval IS NOT NULL`
3. For each view where `now() - MIN(last_update)` over its sources is at least the (possibly backed-off) interval, or that has no source watermark: calls `PRAGMA refresh_options('<catalog>', '<schema>', '<view>')`
4. Waits for any active OpenIVM mutation, then refreshes the due view

Views refreshed through a cascade earlier in the same cycle are not refreshed again. A failed refresh prints a warning and is retried on the next cycle.

The daemon reads settings through its own connection, so it sees global values only. Use `SET GLOBAL` for settings that should affect scheduled refreshes, for example `SET GLOBAL openivm_cascade_refresh = 'both'`.

`PRAGMA refresh_start_daemon` stops any running daemon, starts a new one on the caller's database, and wakes it immediately. It returns `started = true` and cannot run inside a transaction.

## Checking refresh status

```sql
PRAGMA refresh_status('mv');
```

Returns a single row:

| Column | Type | Description |
|---|---|---|
| `view_name` | VARCHAR | Name of the materialized view |
| `refresh_interval` | BIGINT | Configured interval in seconds (NULL if manual) |
| `last_refresh` | TIMESTAMP | Earliest source watermark, `MIN(last_update)` in `openivm_delta_tables` |
| `next_refresh` | TIMESTAMP | Estimated next refresh (last_refresh + interval) |
| `status` | VARCHAR | `idle`, or `refreshing` while the daemon is refreshing this view |
| `effective_interval` | BIGINT | Current interval after backoff (same as configured if no backoff) |
| `refresh_strategy` | VARCHAR | Stored refresh type, such as `aggregate_group`, `window_partition`, or `full_refresh` |

## Adaptive backoff

When a refresh takes longer than its interval (e.g., a 3-minute refresh on a 1-minute interval), the daemon doubles the effective interval to avoid running continuously. The effective interval is capped at 24 hours and resets when a refresh completes within the interval.

```
Warning: refresh of 'mv_sales' took 185s (interval: 60s).
Increasing effective interval to 120s. Set openivm_adaptive_backoff = false to disable.
```

The comparison uses the configured interval: a refresh slower than `refresh_interval` doubles the effective interval again, and a refresh that finishes within it clears the backoff.

Backoff is runtime-only — the configured `refresh_interval` in metadata is never modified. Restarting the database or the daemon resets all backoffs. The daemon reads this setting on each wake-up; disable with:

```sql
SET GLOBAL openivm_adaptive_backoff = false;
```

## Crash safety

If the process crashes mid-refresh (after the MERGE updates the MV but before `last_update` is advanced), the same deltas could be double-applied on restart. OpenIVM detects this via a `refresh_in_progress` flag in `openivm_views`:

- Set to `true` before the IVM query starts
- Cleared after `last_update` is set
- If still `true` on the next refresh → automatic full recompute to recover

```
Warning: recovering 'mv_sales' from interrupted refresh via full recompute.
```

This adds two small UPDATE statements per refresh cycle (one before, one after the critical section). The flag is never visible to users during normal operation. Native refreshes write the flag, MV changes, and `last_update` in one transaction, so the recovery path matters mainly for DuckLake MVs, whose data and metadata commit separately.

## Concurrency

Automatic refresh uses the same database-wide mutation gate as manual `PRAGMA refresh()`:

- **Reads during refresh**: native MVs are read consistently (the refresh commits in one transaction); DuckLake refreshes are not yet atomic for readers ([#88](https://github.com/ila/openivm/issues/88)). See [concurrency and operations](../concurrency.md).
- **Concurrent refreshes**: serialized by the mutation gate. The daemon and manual refreshes wait for the active mutation to finish.
- **Tracked DML during refresh**: serialized by the same gate, preventing delta writes from racing with refresh bookkeeping.

## Configuration

| Setting | Type | Default | Description |
|---|---|---|---|
| `openivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh takes longer than the interval (daemon reads the global value) |
| `openivm_disable_daemon` | BOOLEAN | `false` | Disable the background refresh daemon at extension load |
| `openivm_profile_refresh` | BOOLEAN | `false` | Record per-step refresh timings in `openivm_refresh_profile` |
| `openivm_profile_retention_days` | BIGINT | `31` | Delete profile rows older than this many days when profiling writes new rows |

The refresh interval itself is per-view, set at creation time via `REFRESH EVERY` or later via `ALTER MATERIALIZED VIEW ... SET REFRESH`.

## Metadata

The `refresh_interval` and `refresh_in_progress` columns are stored in `openivm_views`:

```sql
SELECT view_sql_name, refresh_interval, refresh_in_progress
FROM openivm_views
WHERE refresh_interval IS NOT NULL;
```

| view_sql_name | refresh_interval | refresh_in_progress |
|---|---|---|
| mv_sales | 300 | false |
| mv_orders | 60 | false |

`view_name` in `openivm_views` is the MV's internal key (`__openivm_mv_...`); `view_sql_name` is the name used in SQL.
