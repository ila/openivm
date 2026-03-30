# Ungrouped aggregates

## Example

```sql
CREATE TABLE scores (val INT);
INSERT INTO scores VALUES (10), (20), (30);

CREATE MATERIALIZED VIEW total_score AS
    SELECT SUM(val) AS total, COUNT(*) AS cnt FROM scores;

-- total=60, cnt=3

INSERT INTO scores VALUES (40);
PRAGMA ivm('total_score');
-- total=100, cnt=4

DELETE FROM scores WHERE val = 10;
PRAGMA ivm('total_score');
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
WITH scan_0 (t0_val, t0__duckdb_ivm_multiplicity) AS (
    SELECT val, _duckdb_ivm_multiplicity
    FROM delta_scores
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
),
aggregate_1 (t1_total, t1_cnt, t1__duckdb_ivm_multiplicity) AS (
    SELECT SUM(t0_val), COUNT_STAR(), t0__duckdb_ivm_multiplicity
    FROM scan_0
    GROUP BY t0__duckdb_ivm_multiplicity
)
INSERT INTO delta_total_score (total, cnt, _duckdb_ivm_multiplicity)
SELECT t1_total, t1_cnt, t1__duckdb_ivm_multiplicity FROM aggregate_1;
```

### Upsert (consolidated CTE + UPDATE)

```sql
WITH _ivm_delta AS (
    -- Consolidate all delta rows into a single net change per column
    -- false multiplicities become negative (subtracted from the MV)
    SELECT
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -total ELSE total END) AS d_total,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -cnt ELSE cnt END) AS d_cnt
    FROM delta_total_score
)
-- Add the net delta to the existing single-row MV
-- COALESCE handles NULL: an empty base table produces SUM() = NULL, not 0
UPDATE total_score SET
    total = COALESCE(total, 0) + COALESCE((SELECT d_total FROM _ivm_delta), 0),
    cnt = COALESCE(cnt, 0) + COALESCE((SELECT d_cnt FROM _ivm_delta), 0);
```

### MIN/MAX (full recompute)

MIN and MAX are not decomposable — deleting the current minimum requires re-scanning the base table to find the new minimum. OpenIVM detects MIN/MAX and replaces the upsert with a full DELETE + INSERT:

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
| `MIN`, `MAX` | Full recompute | Entire MV deleted and re-inserted from original query. |
| `STDDEV`, `STRING_AGG` | Full refresh | View classified as `FULL_REFRESH` at creation time. |

## Limitations

- The MV is always a single row. If you delete all base table rows, the aggregates become NULL (not zero).
- AVG decomposition adds two hidden columns (`_ivm_sum_*`, `_ivm_count_*`) to the MV table.
