# Window Functions

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
PRAGMA ivm('top_per_dept');
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
  partitions are identified from all base delta tables involved in the join.
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
PRAGMA ivm('mv_ranked');  -- partition-level recompute
PRAGMA ivm('mv_top2');    -- reads updated mv_ranked data table
```

The downstream MV reads from the window MV's updated data table. Note: the delta
view for window MVs is not populated (due to LPTS limitations), so the downstream
refresh is a full recompute of the downstream query, not incremental.

## Limitations

- **No insert-only optimization.** Unlike grouped aggregates, window functions always
  require full partition recompute regardless of delta type. A single insertion can
  change the numbering of all rows in the partition.
- **Delta view not populated.** The DoIVM/LPTS pipeline is bypassed for window views
  because LPTS does not support the WINDOW operator. Downstream chained MVs do a full
  recompute when refreshed (reading from the window MV's data table directly).
- **LPTS fallback.** The view query is stored as the original user SQL, not the
  LPTS-rewritten form. This means plan-level rewrites (AVG/STDDEV decomposition) are
  not applied within window view queries.
