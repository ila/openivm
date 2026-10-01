# Window Functions

> Linearity: **NON_LINEAR** (partition recompute — partition-level state required). ([what does this mean?](../internals/linearity.md))

OpenIVM supports materialized views with window functions via **partition-level recompute**.
When base data changes, only the partitions affected by the delta are deleted and
re-inserted from the base query. Unchanged partitions are preserved.

## Supported functions

All DuckDB window functions are supported:

- **Ranking:** ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST
- **Navigation:** LEAD, LAG, FIRST_VALUE, LAST_VALUE, NTH_VALUE
- **Aggregates over windows:** SUM, COUNT, AVG, MIN, MAX (with OVER clause)
- **Custom frames:** ROWS BETWEEN, RANGE BETWEEN, GROUPS BETWEEN

## How it works

### Creation

```sql
CREATE MATERIALIZED VIEW top_per_dept AS
    SELECT id, dept, salary,
           ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary DESC) AS rn
    FROM employees;
```

At creation time, OpenIVM detects the LOGICAL_WINDOW operator and extracts the
PARTITION BY columns (`dept` in this example). The view is classified as
`WINDOW_PARTITION` and the partition columns are stored in metadata.

### Refresh

```sql
INSERT INTO employees VALUES (100, 'eng', 150000);
PRAGMA refresh('top_per_dept');
```

The refresh identifies which partitions have deltas (by querying the base delta
tables for distinct partition key values), then:

1. **DELETE** rows from the MV where the partition key matches an affected partition
2. **INSERT** fresh results from the base query filtered to those partitions

Only `dept = 'eng'` is recomputed. All other departments are untouched.

### Without PARTITION BY

Window functions without PARTITION BY treat the entire result as one partition.
Any delta triggers a full recompute (equivalent to full refresh), but the view
still benefits from empty-delta skipping — no-op when nothing changed.

## Running aggregates (opt-in)

With `SET openivm_running_window_incremental = true` (default `false`), insert-only refreshes of
cumulative running aggregates can append to a partition instead of recomputing it:

```sql
CREATE MATERIALIZED VIEW running_totals AS
    SELECT account, ts, amount,
           SUM(amount) OVER (PARTITION BY account ORDER BY ts) AS balance
    FROM txns;
```

Eligible windows are `SUM`, `COUNT`, `AVG`, `MIN`, `MAX` with a single PARTITION BY column, a single
ascending ORDER BY column, and a frame from `UNBOUNDED PRECEDING` to `CURRENT ROW` (`ROWS` or
`RANGE`); all windows in the view must share the partition and order. For each partition touched by
the batch, if every new row sorts after the partition's current last row (`>=` for `ROWS`), the new
suffix is computed seeded from the stored running state and appended. Other partitions fall back to
partition recompute.

Seeds aggregate the retained input directly (not `avg * count`, which loses precision on large integers), and ORDER BY
or AVG inputs missing from the public projection are kept as hidden maintenance columns. `ROWS` frames need a
deterministic order among peers, so prefer a unique ORDER BY key.

The setting is off by default because it is not a universal speedup: in measurements on 100k–1M-row inputs it cut
refresh time by roughly 20–40% when a batch touched few of many partitions, but was 5–20% slower when a batch touched
one large partition or nearly all partitions, since the extra publication and delta work doesn't pay off there.

## Composite operations

Window functions work with other operators:

- **Window over JOIN:** `SELECT ..., ROW_NUMBER() OVER (...) FROM t1 JOIN t2 ON ...` —
  uses partition recompute when OpenIVM can derive affected partition values from source lineage. Other join shapes use full recompute.
- **Window with WHERE:** `SELECT ..., RANK() OVER (...) FROM t WHERE active = true` —
  the filter is part of the base query used for partition recompute.
- **Composite PARTITION BY:** Multiple columns are supported:
  `PARTITION BY dept, team` uses both columns to identify affected partitions.

## Chained materialized views

Window MVs can be used as sources for downstream MVs:

```sql
CREATE MATERIALIZED VIEW mv_ranked AS
    SELECT id, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary DESC) AS rnk
    FROM employees;

CREATE MATERIALIZED VIEW mv_top2 AS
    SELECT dept, count(*) AS cnt FROM mv_ranked WHERE rnk <= 2 GROUP BY dept;
```

Refresh the chain in order:
```sql
PRAGMA refresh('mv_ranked');  -- partition-level recompute
PRAGMA refresh('mv_top2');    -- reads updated mv_ranked data table
```

The downstream MV reads from the window MV's updated data table. Window refresh bypasses
the generic `ComputeDelta` plan, but refresh still records old-to-new delta rows for
downstream views. This lets chained MVs consume window recompute changes incrementally.

## Limitations

- **Some window-over-join shapes use full recompute.** Partition recompute needs the
  changed partition values. If those values cannot be derived from the changed source
  table or lineage metadata, OpenIVM falls back to full recompute. Single-table windows
  and supported lineage shapes use partition recompute.
- **Insert-only optimization is opt-in and narrow.** By default window functions
  always require full partition recompute regardless of delta type — a single insertion
  can change the numbering of all rows in the partition. See
  [Running aggregates](#running-aggregates-opt-in) for the one exception.
- **ComputeDelta bypass.** Window views use a dedicated partition-recompute refresh path
  instead of the generic ComputeDelta/LPTS pipeline. Downstream delta rows are generated
  from the recomputed old-to-new transition.
- **LPTS fallback (DuckLake).** For window views over DuckLake sources or targets, the
  view query is stored as the original user SQL, not the LPTS-rewritten form, so
  plan-level rewrites (AVG/STDDEV decomposition) are not applied. DuckLake window views
  with a multi-column PARTITION BY use full recompute.
