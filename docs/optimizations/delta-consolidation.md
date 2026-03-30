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

Group by all projected columns and sum the multiplicity to get the net count per tuple.

```sql
-- Collapse multiple entries for the same tuple into a single net change
-- +1 for each insertion, -1 for each deletion
WITH consolidated AS (
    SELECT col1, col2, ...,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_table
    WHERE _duckdb_ivm_timestamp >= '{ts}'
    GROUP BY col1, col2, ...
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
-- _net > 0: net insert (the tuple gained copies)
-- _net < 0: net delete (the tuple lost copies)
-- _net = 0: filtered out by HAVING (insert and delete cancelled out)
```

#### Worked example

Suppose you insert, delete, and re-insert a product before refreshing:

```sql
INSERT INTO products VALUES (1, 'Widget', 10);
DELETE FROM products WHERE id = 1 AND price = 10;
INSERT INTO products VALUES (1, 'Widget', 15);
```

The delta table now has three rows:

| id | name | price | multiplicity | timestamp |
|---|---|---|---|---|
| 1 | Widget | 10 | true | t1 |
| 1 | Widget | 10 | false | t2 |
| 1 | Widget | 15 | true | t3 |

Consolidation groups by (id, name, price):

```sql
-- Group identical tuples and compute net change
-- (1, Widget, 10): +1 - 1 = 0 → cancelled out, removed by HAVING
-- (1, Widget, 15): +1 = 1 → net insert
WITH consolidated AS (
    SELECT id, name, price,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_products
    WHERE _duckdb_ivm_timestamp >= '{ts}'
    GROUP BY id, name, price
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
-- Result: only (1, Widget, 15, _net=1) survives
-- The old price (10) fully cancelled out — no wasted work
```

### Aggregate Views

Group by the aggregation keys and fold the value using the multiplicity flag:

```sql
-- Fold insertions (+) and deletions (-) into a single signed delta per group
-- This produces one row per group key, ready for MERGE into the view
WITH consolidated AS (
    SELECT key1, key2,
           SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -agg_val ELSE agg_val END) AS _net_val
    FROM delta_table
    WHERE _duckdb_ivm_timestamp >= '{ts}'
    GROUP BY key1, key2
)
-- Each row is a single signed delta: positive means the group's aggregate increased,
-- negative means it decreased
```

### Bag Semantics

Materialized views use bag (multiset) semantics — duplicate rows are meaningful.

**Multi-copy inserts:** When `_net > 1`, the consolidated row must expand into multiple
physical rows. This is done with `generate_series(1, _net)`:

```sql
-- Replicate the tuple _net times to preserve bag semantics
-- For example, if two identical rows were inserted, _net = 2
INSERT INTO mv
SELECT col1, col2, ...
FROM consolidated, generate_series(1, _net)
WHERE _net > 0;
```

**Precise deletes:** When `_net < 0`, exactly `|_net|` copies must be removed. OpenIVM
assigns a deterministic ordering via `rowid` + `ROW_NUMBER()` to select which copies
to delete:

```sql
-- Remove exactly |_net| copies of each tuple
-- ROW_NUMBER assigns a stable ordering so we always delete the same copies
DELETE FROM mv
WHERE rowid IN (
    SELECT rowid FROM (
        SELECT rowid, ROW_NUMBER() OVER (PARTITION BY col1, col2, ...) AS rn
        FROM mv
    )
    WHERE rn <= abs(_net)
);
```
