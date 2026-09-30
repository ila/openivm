# Ungrouped aggregates

> Linearity: **LINEAR** for SUM/COUNT; **NON_LINEAR** for MIN/MAX. AVG and STDDEV/VARIANCE are decomposed into linear helper columns. ([what does this mean?](../internals/linearity.md))

## Example

```sql
CREATE TABLE scores (val INT);
INSERT INTO scores VALUES (10), (20), (30);

CREATE MATERIALIZED VIEW total_score AS
    SELECT SUM(val) AS total, COUNT(*) AS cnt FROM scores;

-- total=60, cnt=3

INSERT INTO scores VALUES (40);
PRAGMA refresh('total_score');
-- total=100, cnt=4

DELETE FROM scores WHERE val = 10;
PRAGMA refresh('total_score');
-- total=90, cnt=3
```

## How IVM handles it

**Algebraic rule:**

```
new_MV = old_MV + delta(query)
```

For SUM and COUNT (fully decomposable), the delta is a single signed value added to the existing scalar. The materialized view is always a single row. A CTE consolidates all delta columns in one pass, then an UPDATE applies the net change.

## Compiled SQL

### IVM query (delta computation)

```sql
-- Aggregate the delta rows, preserving multiplicity
-- Insertions contribute positive values, deletions contribute negative
WITH scan_0 (t0_val, t0_openivm_multiplicity) AS (
    SELECT val, openivm_multiplicity
    FROM openivm_delta_scores
    WHERE openivm_timestamp >= '{ts}'::TIMESTAMP
),
aggregate_1 (t1_total, t1_cnt, t1_openivm_multiplicity) AS (
    SELECT SUM(t0_val), COUNT_STAR(), t0_openivm_multiplicity
    FROM scan_0
    GROUP BY t0_openivm_multiplicity
)
INSERT INTO openivm_delta_total_score (total, cnt, openivm_multiplicity)
SELECT t1_total, t1_cnt, t1_openivm_multiplicity FROM aggregate_1;
```

### Upsert (consolidated CTE + UPDATE)

```sql
WITH openivm_delta AS (
    -- Consolidate all delta rows into a single net change per column.
    -- Z-set bag-aware sum: weight w∈ℤ scales the column value before SUM
    -- (insertions carry +1, deletions carry −1).
    SELECT
        SUM(openivm_multiplicity * total) AS d_total,
        SUM(openivm_multiplicity * cnt) AS d_cnt
    FROM openivm_delta_total_score
)
-- Add the net delta to the existing single-row MV
-- COALESCE handles NULL: an empty base table produces SUM() = NULL, not 0
UPDATE total_score SET
    total = COALESCE(total, 0) + COALESCE((SELECT d_total FROM openivm_delta), 0),
    cnt = COALESCE(cnt, 0) + COALESCE((SELECT d_cnt FROM openivm_delta), 0);
```

### MIN/MAX (full recompute)

MIN and MAX are not decomposable — deleting the current minimum requires re-scanning the base table to find the new minimum. OpenIVM detects MIN/MAX (and any other non-summable output column, e.g. `LIST`, `COUNT(DISTINCT)`, a VARCHAR result) and replaces the upsert with a full DELETE + INSERT. Unlike grouped aggregates, there is no insert-only `LEAST`/`GREATEST` fast path:

```sql
-- Cannot incrementally update MIN: the deleted row may have been the minimum
-- Recompute the entire single-row MV from scratch
DELETE FROM total_score;
INSERT INTO total_score SELECT MIN(val) AS min_val, COUNT(*) AS cnt FROM scores;
```

## Supported aggregates

| Function | Strategy | Notes |
|----------|----------|-------|
| `SUM` | Incremental (UPDATE) | Net delta added to existing value. |
| `COUNT`, `COUNT(*)` | Incremental (UPDATE) | Net delta added to existing count. |
| `AVG` | Incremental (decomposed) | Hidden SUM + COUNT columns maintained independently; AVG recomputed as SUM / NULLIF(COUNT, 0). |
| `STDDEV`, `VARIANCE` | Incremental (decomposed) | Hidden SUM, SUM-of-squares, and COUNT columns maintained independently; final value recomputed after UPDATE. |
| `MIN`, `MAX` | Full recompute | Entire MV deleted and re-inserted from original query. |
| `STRING_AGG`, `LISTAGG`, `MEDIAN`, quantiles | Full refresh | View classified as `FULL_REFRESH` at creation time. |

## Filtered group count

A count over a filtered grouped subquery has its own aux-state path:

```sql
CREATE MATERIALIZED VIEW positive_groups AS
    SELECT COUNT(*) AS n FROM (
        SELECT g, SUM(x) AS s FROM t GROUP BY g
    ) WHERE s > 0;
```

The shape must be a single-table `GROUP BY` on one column with one plain `SUM`, filtered by
`s > 0` or `s < 0` (the threshold must be 0). OpenIVM keeps a per-group sum in
`openivm_filtered_group_count_<view>` and updates the count from the groups whose sum crosses
the threshold.

## Limitations

- The MV is always a single row. If you delete all base table rows, the aggregates become NULL (not zero).
- AVG decomposition adds two hidden columns (`openivm_sum_*`, `openivm_count_*`) to the MV table.
- STDDEV/VARIANCE decomposition adds hidden SUM, SUM-of-squares, and COUNT columns.
