# DISTINCT

## Example

```sql
CREATE TABLE colors (id INT, color VARCHAR);
INSERT INTO colors VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green');

CREATE MATERIALIZED VIEW distinct_colors AS
    SELECT DISTINCT color FROM colors;

-- Result: blue, green, red

-- Insert a duplicate — DISTINCT result does not change
INSERT INTO colors VALUES (5, 'red'), (6, 'blue');
PRAGMA ivm('distinct_colors');
-- Result: blue, green, red (unchanged)

-- Insert a new value
INSERT INTO colors VALUES (7, 'yellow');
PRAGMA ivm('distinct_colors');
-- Result: blue, green, red, yellow
```

## How IVM handles it

**Algebraic rule:**

```
SELECT DISTINCT cols FROM T
→ SELECT cols, COUNT(*) AS _ivm_distinct_count FROM T GROUP BY cols
```

The parser rewrites `SELECT DISTINCT` into a `GROUP BY` with a hidden `COUNT(*)` column before planning. The hidden `_ivm_distinct_count` column tracks how many source rows map to each distinct output row. You never see it in query results.

The view is classified as `AGGREGATE_GROUP` and maintained incrementally using the same MERGE upsert as any other grouped aggregate:

- **Insert a duplicate:** `_ivm_distinct_count` increments (e.g., 1 → 2). No visible change.
- **Delete a duplicate:** `_ivm_distinct_count` decrements (e.g., 2 → 1). No visible change.
- **Delete the last copy:** `_ivm_distinct_count` reaches 0. The row is removed from the MV.
- **Insert a new value:** `_ivm_distinct_count` starts at 1. The row appears in the MV.

There is no separate `DISTINCT` enum in `IVMType` — it reuses `AGGREGATE_GROUP`.

## Compiled SQL

### Internal rewrite

```sql
-- User writes:
CREATE MATERIALIZED VIEW distinct_colors AS
    SELECT DISTINCT color FROM colors;

-- OpenIVM internally rewrites to:
CREATE TABLE distinct_colors AS
    SELECT color, COUNT(*) AS _ivm_distinct_count
    FROM colors
    GROUP BY color;
```

### Upsert (MERGE)

The upsert is identical to [grouped aggregates](grouped-aggregates.md):

```sql
-- Consolidate deltas per distinct value
WITH ivm_cte AS (
    SELECT color,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false
            THEN -_ivm_distinct_count ELSE _ivm_distinct_count END) AS _ivm_distinct_count
    FROM delta_distinct_colors
    GROUP BY color
)
-- MERGE: increment/decrement the hidden count, insert new values
MERGE INTO distinct_colors v USING ivm_cte d
ON v.color IS NOT DISTINCT FROM d.color
WHEN MATCHED THEN UPDATE SET
    _ivm_distinct_count = v._ivm_distinct_count + d._ivm_distinct_count
WHEN NOT MATCHED THEN INSERT (color, _ivm_distinct_count)
    VALUES (d.color, d._ivm_distinct_count);

-- Remove values whose count has reached zero (no more source rows)
DELETE FROM distinct_colors WHERE _ivm_distinct_count = 0;
```

## Multi-column DISTINCT

Multi-column DISTINCT works the same way — all columns become GROUP BY keys:

```sql
CREATE MATERIALIZED VIEW distinct_pairs AS
    SELECT DISTINCT region, product FROM sales;

-- Internally: SELECT region, product, COUNT(*) AS _ivm_distinct_count
--             FROM sales GROUP BY region, product
```

## Limitations

- `DISTINCT` adds a hidden column to the MV table. This is invisible to `SELECT *` but present in the physical storage.
- `SELECT DISTINCT` combined with aggregates (e.g., `SELECT DISTINCT region, SUM(amount)`) is not rewritten — the `GROUP BY` from the aggregate already handles deduplication.
