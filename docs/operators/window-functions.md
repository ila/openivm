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
- **No insert-only optimization.** Unlike grouped aggregates, window functions always
  require full partition recompute regardless of delta type. A single insertion can
  change the numbering of all rows in the partition.
- **ComputeDelta bypass.** Window views use a dedicated partition-recompute refresh path
  instead of the generic ComputeDelta/LPTS pipeline. Downstream delta rows are generated
  from the recomputed old-to-new transition.
- **LPTS fallback.** The view query is stored as the original user SQL, not the
  LPTS-rewritten form. This means plan-level rewrites (AVG/STDDEV decomposition) are
  not applied within window view queries.
