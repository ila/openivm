# Delta Consolidation

## Problem

Delta tables can accumulate multiple entries for the same row within a single refresh cycle.
For example, a row may be inserted, deleted, and re-inserted before the materialized view
is refreshed. Applying these operations one-by-one produces incorrect intermediate states
and redundant work.

## Solution

Before applying deltas, OpenIVM consolidates them using a CTE that collapses all entries
for the same logical row into a single net change.

### Projection Views

Group by all projected columns and sum the multiplicity column to get the net count per tuple.

```sql
WITH consolidated AS (
    SELECT col1, col2, ..., SUM(_duckdb_ivm_multiplicity) AS _net
    FROM delta_table
    WHERE _duckdb_ivm_timestamp = current_ts
    GROUP BY col1, col2, ...
)
```

- `_net > 0` — net insert (the tuple gained copies)
- `_net < 0` — net delete (the tuple lost copies)
- `_net = 0` — no-op (insert and delete cancelled out)

### Aggregate Views

Group by the aggregation keys and fold the value using the multiplicity flag:

```sql
WITH consolidated AS (
    SELECT key1, key2, ...,
           SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -agg_val ELSE agg_val END) AS _net_val
    FROM delta_table
    WHERE _duckdb_ivm_timestamp = current_ts
    GROUP BY key1, key2, ...
)
```

This produces a single signed delta per group key, ready for MERGE into the view.

### Bag Semantics

Materialized views use bag (multiset) semantics — duplicate rows are meaningful.

**Multi-copy inserts:** When `_net > 1`, the consolidated row must expand into multiple
physical rows. This is done with `generate_series(1, _net)`:

```sql
INSERT INTO mv
SELECT col1, col2, ...
FROM consolidated, generate_series(1, _net)
WHERE _net > 0
```

**Precise deletes:** When `_net < 0`, exactly `|_net|` copies must be removed. OpenIVM
assigns a deterministic ordering via `rowid` + `ROW_NUMBER()` to select which copies
to delete:

```sql
DELETE FROM mv
WHERE rowid IN (
    SELECT rowid FROM (
        SELECT rowid, ROW_NUMBER() OVER (PARTITION BY col1, col2, ...) AS rn
        FROM mv
    )
    WHERE rn <= abs(_net)
)
```
