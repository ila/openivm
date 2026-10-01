# DISTINCT

> Linearity: **NON_LINEAR** (DBSP `δ` operator — drops duplicates; group-recompute via COUNT(*) sentinel). ([what does this mean?](../internals/linearity.md))

## Example

```sql
CREATE TABLE colors (id INT, color VARCHAR);
INSERT INTO colors VALUES (1, 'red'), (2, 'blue'), (3, 'red'), (4, 'green');

CREATE MATERIALIZED VIEW distinct_colors AS
    SELECT DISTINCT color FROM colors;

-- Result: blue, green, red

-- Insert a duplicate — DISTINCT result does not change
INSERT INTO colors VALUES (5, 'red'), (6, 'blue');
PRAGMA refresh('distinct_colors');
-- Result: blue, green, red (unchanged)

-- Insert a new value
INSERT INTO colors VALUES (7, 'yellow');
PRAGMA refresh('distinct_colors');
-- Result: blue, green, red, yellow
```

## How IVM handles it

**Algebraic rule:**

```
SELECT DISTINCT cols FROM T
→ SELECT cols, COUNT(*) AS openivm_distinct_count FROM T GROUP BY cols
```

OpenIVM's plan rewrite (`RewriteDistinct` in `src/core/plan_rewrite.cpp`) replaces the logical `DISTINCT` operator with a `GROUP BY` aggregate carrying a hidden `COUNT(*)` column. The hidden `openivm_distinct_count` column tracks how many source rows map to each distinct output row. You never see it in query results.

The view is classified as `AGGREGATE_GROUP` and maintained incrementally using the same MERGE upsert as any other grouped aggregate:

- **Insert a duplicate:** `openivm_distinct_count` increments (e.g., 1 → 2). No visible change.
- **Delete a duplicate:** `openivm_distinct_count` decrements (e.g., 2 → 1). No visible change.
- **Delete the last copy:** `openivm_distinct_count` reaches 0. The row is removed from the MV.
- **Insert a new value:** `openivm_distinct_count` starts at 1. The row appears in the MV.

There is no separate `DISTINCT` enum in `RefreshType` — it reuses `AGGREGATE_GROUP`.

## Compiled SQL

### Internal rewrite

```sql
-- User writes:
CREATE MATERIALIZED VIEW distinct_colors AS
    SELECT DISTINCT color FROM colors;

-- OpenIVM internally rewrites to:
CREATE TABLE distinct_colors AS
    SELECT color, COUNT(*) AS openivm_distinct_count
    FROM colors
    GROUP BY color;
```

### Upsert (MERGE)

The upsert is identical to [grouped aggregates](grouped-aggregates.md):

```sql
-- Consolidate deltas per distinct value (Z-set bag-aware sum: weight × count)
WITH refresh_cte AS (
    SELECT color,
        SUM(openivm_multiplicity * openivm_distinct_count) AS openivm_distinct_count
    FROM openivm_delta_distinct_colors
    GROUP BY color
)
-- MERGE: increment/decrement the hidden count, insert new values
MERGE INTO distinct_colors v USING refresh_cte d
ON v.color IS NOT DISTINCT FROM d.color
WHEN MATCHED THEN UPDATE SET
    openivm_distinct_count = v.openivm_distinct_count + d.openivm_distinct_count
WHEN NOT MATCHED THEN INSERT (color, openivm_distinct_count)
    VALUES (d.color, d.openivm_distinct_count);

-- Remove values whose count has reached zero (no more source rows)
DELETE FROM distinct_colors WHERE openivm_distinct_count = 0;
```

## Multi-column DISTINCT

Multi-column DISTINCT works the same way — all columns become GROUP BY keys:

```sql
CREATE MATERIALIZED VIEW distinct_pairs AS
    SELECT DISTINCT region, product FROM sales;

-- Internally: SELECT region, product, COUNT(*) AS openivm_distinct_count
--             FROM sales GROUP BY region, product
```

## Limitations

- `DISTINCT` adds a hidden column to the MV table. This is invisible to `SELECT *` but present in the physical storage.
- A redundant `DISTINCT` is dropped: one whose output is exactly the GROUP BY keys of an aggregate (e.g., `SELECT DISTINCT region FROM sales GROUP BY region`), or one over a scalar aggregate. Any other top-level `DISTINCT` over aggregate results (e.g., `SELECT DISTINCT region, SUM(amount) ... GROUP BY region`) is treated as a second non-linear level and falls back to full refresh.

## DISTINCT inside a larger query

A `DISTINCT` below another operator is not maintained with the MERGE above:

- **Inner DISTINCT under an aggregate** (e.g., `SELECT g, COUNT(*) FROM (SELECT DISTINCT g, x FROM t) GROUP BY g`) uses affected-group recompute (`GROUP_RECOMPUTE`). With `SET openivm_distinct_aux_state = true` (default `false`), single-source views instead keep a per-tuple count aux table and propagate ±1 only when a tuple's count crosses zero (`DISTINCT_INCREMENTAL`); multi-source views still use group recompute.
- **Inner DISTINCT below an outer projection** (no aggregate) uses affected-group recompute over the visible output columns.
- For `UNION` without `ALL`, see [union](union-all.md#union-distinct).
