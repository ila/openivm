# Delta Consolidation

## Problem

Delta tables can accumulate multiple entries for the same row within a single refresh cycle.
For example, a row may be inserted, deleted, and re-inserted before the materialized view
is refreshed. Applying these operations one-by-one produces incorrect intermediate states
and redundant work.

## Solution

OpenIVM consolidates deltas at two points:

1. **Source delta scans** (`openivm_compact_deltas`, default `true`): each delta scan in the
   incremental plan, for both standard delta tables and DuckLake insertion/deletion scans, is
   grouped by the source columns the view reads, with `SUM(openivm_multiplicity)` and rows with
   a net weight of 0 removed. The view's pushed-down scan filters and the
   `openivm_timestamp >= last_update` watermark are applied first.
2. **Upsert**: the refresh SQL collapses the view's own delta (`openivm_delta_<view>`) into
   one net change per logical row, as described below.

```sql
SET openivm_compact_deltas = false;  -- keep raw delta rows in the incremental plan
```

### Projection Views

Group by all projected columns and sum the (signed integer) multiplicity to get the net count per tuple.

```sql
-- Collapse multiple entries for the same tuple into a single net change
-- openivm_multiplicity is the signed Z-set weight: +1 for insertion, -1 for deletion
WITH consolidated AS (
    SELECT col1, col2, ...,
        SUM(openivm_multiplicity) AS _net
    FROM openivm_delta_table
    WHERE openivm_timestamp >= '{ts}'
    GROUP BY col1, col2, ...
    HAVING SUM(openivm_multiplicity) != 0
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
| 1 | Widget | 10 | +1 | t1 |
| 1 | Widget | 10 | -1 | t2 |
| 1 | Widget | 15 | +1 | t3 |

Consolidation groups by (id, name, price):

```sql
-- Group identical tuples and compute net change
-- (1, Widget, 10): +1 + (-1) = 0 → cancelled out, removed by HAVING
-- (1, Widget, 15): +1            = 1 → net insert
WITH consolidated AS (
    SELECT id, name, price,
        SUM(openivm_multiplicity) AS _net
    FROM openivm_delta_products
    WHERE openivm_timestamp >= '{ts}'
    GROUP BY id, name, price
    HAVING SUM(openivm_multiplicity) != 0
)
-- Result: only (1, Widget, 15, _net=1) survives
-- The old price (10) fully cancelled out — no wasted work
```

### Aggregate Views

Group by the aggregation keys and fold the value by multiplying the column by the signed weight:

```sql
-- Z-set bag-aware sum: weight w∈ℤ scales the column value before SUM.
-- This produces one row per group key, ready for MERGE into the view.
WITH consolidated AS (
    SELECT key1, key2,
           SUM(openivm_multiplicity * agg_val) AS _net_val
    FROM openivm_delta_table
    WHERE openivm_timestamp >= '{ts}'
    GROUP BY key1, key2
)
-- Each row is a single signed delta: positive means the group's aggregate increased,
-- negative means it decreased
```

### Atomic UPDATE-delta writes

A SQL `UPDATE` is decomposed into a retraction (`-1`) of the old row plus an insertion
(`+1`) of the new row. Both rows must commit together — if a concurrent refresh snapshots
the database between them, it sees a half-applied UPDATE and consolidation produces a
spurious net insert or net delete. To prevent that, the insert rule adds a streaming
capture operator (`OPENIVM_TRANSACTIONAL_DELTA_CAPTURE`) to the `UPDATE` plan. For each
updated row it fetches the old row by `rowid` and appends it to `openivm_delta_<table>` with
multiplicity `-1`, followed by the new row with `+1`, through the caller's transaction. Rows are
deduplicated by `rowid`, so a row is captured once per statement. `DELETE`, `INSERT`, and
`MERGE INTO` use the same capture path; for `MERGE INTO`, only rows that an action actually
inserts, updates, or deletes are captured.

This is what makes UPDATE-driven consolidation safe under concurrent refresh: the delta rows
commit or roll back with the base-table change, and tracked DML is serialized with refresh by
the OpenIVM mutation gate.

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
first joins the net-negative tuples to the MV (`IS NOT DISTINCT FROM` on every column), then
ranks only those candidate rows by `rowid` to select which copies to delete:

```sql
-- Remove exactly |_net| copies of each tuple
WITH openivm_net AS (...),
openivm_delete_candidates AS (
    SELECT v.rowid, v.col1, v.col2, ..., d._net
    FROM mv v JOIN openivm_net d
      ON v.col1 IS NOT DISTINCT FROM d.col1 AND v.col2 IS NOT DISTINCT FROM d.col2 ...
    WHERE d._net < 0
), openivm_ranked_deletes AS (
    SELECT rowid, _net,
           ROW_NUMBER() OVER (PARTITION BY col1, col2, ... ORDER BY rowid) AS _rn
    FROM openivm_delete_candidates
)
DELETE FROM mv
WHERE rowid IN (SELECT rowid FROM openivm_ranked_deletes WHERE _rn <= -_net);
```
