# Grouped aggregates

> Linearity: **LINEAR** for SUM/COUNT; **NON_LINEAR** for MIN/MAX, LIST, and non-summable output columns. AVG and STDDEV/VARIANCE are decomposed into linear helper columns. ([what does this mean?](../internals/linearity.md))

## Example

```sql
CREATE TABLE sales (region VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 100), ('EU', 200);

CREATE MATERIALIZED VIEW sales_summary AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;

INSERT INTO sales VALUES ('US', 50), ('JP', 300);
PRAGMA refresh('sales_summary');
```

## How IVM handles it

**Algebraic rule:**

```
new_MV[key] = old_MV[key] + openivm_delta_agg[key]
```

For each group key, the delta aggregate is computed from the delta table and merged with the existing value. New groups are inserted; groups whose row count reaches zero are deleted.

Hidden helper columns keep this exact:

- If the view has no plain `COUNT(*)`, a hidden `openivm_count_star` column is added so emptied groups can be detected (a `COUNT(col)` or `SUM` can legitimately be 0 for a live group).
- Each `SUM` gets a hidden `openivm_nonnull_sum_count_<n>` column (`COUNT` of its argument), so a group whose remaining inputs are all NULL gets `SUM = NULL` rather than 0.

The delta table is scanned, grouped by the same keys as the view, and consolidated into net changes per group. A `MERGE INTO` statement atomically updates existing groups and inserts new ones.

## Compiled SQL

### IVM query (delta computation)

```sql
-- Aggregate the delta rows, preserving multiplicity as a grouping key
-- This separates insertions from deletions so the upsert can fold them correctly
WITH scan_0 (...) AS (
    SELECT region, amount, openivm_multiplicity
    FROM openivm_delta_sales
    WHERE openivm_timestamp >= '...'
),
aggregate_1 (...) AS (
    SELECT region, openivm_multiplicity, sum(amount), count_star()
    FROM scan_0
    GROUP BY region, openivm_multiplicity
),
projection_2 (...) AS (
    SELECT region, sum_amount, count_star, openivm_multiplicity
    FROM aggregate_1
)
INSERT INTO openivm_delta_sales_summary (region, total, cnt, openivm_multiplicity)
SELECT * FROM projection_2;
```

### Upsert (MERGE)

```sql
-- Consolidate delta rows: fold insertions (+1) and deletions (-1) into a single net delta per group.
-- With integer-weighted multiplicity, this is just SUM(_w * col) — no CASE round-trip needed.
WITH refresh_cte AS (
    SELECT region,
        SUM(openivm_multiplicity * total) AS total,
        SUM(openivm_multiplicity * cnt) AS cnt
    FROM openivm_delta_sales_summary
    GROUP BY region
)
-- Single-pass MERGE: update existing groups and insert new ones atomically
-- IS NOT DISTINCT FROM handles NULL group keys correctly (NULL = NULL)
MERGE INTO sales_summary v USING refresh_cte d
ON v.region IS NOT DISTINCT FROM d.region
-- Existing group: add the net delta to the current aggregate values
WHEN MATCHED THEN UPDATE SET total = v.total + d.total, cnt = v.cnt + d.cnt
-- New group: insert the delta as the initial value
WHEN NOT MATCHED THEN INSERT (region, total, cnt) VALUES (d.region, d.total, d.cnt);

-- Remove groups where all rows have been deleted (the COUNT columns reach zero)
DELETE FROM sales_summary WHERE COALESCE(cnt, 0) = 0;
```

## Supported aggregates

| Function | Strategy | Notes |
|----------|----------|-------|
| `SUM` | Incremental (MERGE) | Delta added to existing value. |
| `COUNT`, `COUNT(*)` | Incremental (MERGE) | Delta added to existing count. |
| `AVG` | Incremental (decomposed) | Rewritten to hidden SUM + COUNT columns. MERGE updates both; AVG recomputed as SUM / NULLIF(COUNT, 0). |
| `STDDEV`, `VARIANCE` | Incremental (decomposed) | Rewritten to hidden SUM, SUM-of-squares, and COUNT columns. The final value is recomputed after MERGE. |
| `MIN`, `MAX` | Group recompute (insert-only: MERGE) | Affected groups deleted and re-inserted from the original query. Deleting the current min/max requires a full group rescan. Insert-only deltas merge with `LEAST`/`GREATEST` instead (`openivm_minmax_incremental`, default `true`). |
| `BOOL_AND`, `BOOL_OR` | Group recompute | BOOLEAN is a non-summable type; affected groups are recomputed from the view query. Z-set correct: `BOOL_AND = false_count = 0`, `BOOL_OR = true_count > 0`. |
| `ARG_MIN`, `ARG_MAX` | Group recompute | The winning value may change when the current extremum is deleted. |
| `COUNT(DISTINCT x)` | Group recompute | Or per-(group, x) aux state with `openivm_stateful_auxstate=true` (default `false`). |
| `LIST` | Group recompute | Every `LIST` aggregate, including `LIST(...) FILTER`, uses affected-group recompute. See [list-valued aggregates](list-aggregates.md). |
| `HAVING` | Incremental (MERGE) | With `openivm_having_merge=true` (default) the data table stores all groups and the user-facing view applies the HAVING predicate. With the setting off, affected groups are recomputed; MIN/MAX and ARG_MIN/ARG_MAX follow their own rows above. |
| `STRING_AGG`, `LISTAGG`, `MEDIAN`, quantiles | Full refresh | Automatically detected; view uses full recompute. |

## FILTER (WHERE predicate)

`AGG(x) FILTER (WHERE p)` is rewritten to `AGG(CASE WHEN p THEN x END)` by the `RewriteAggregateFilters` normalisation pass before the IVM checker sees the plan. This is semantically exact under Z-set algebra: a delta row with weight `w` contributes `w × (p ? x : NULL)`, which is a linear transformation of the Z-set. All of `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `STDDEV` work correctly through this rewrite.

```sql
CREATE MATERIALIZED VIEW active_stats AS
    SELECT dept,
        COUNT(*) FILTER (WHERE active)      AS active_cnt,
        SUM(salary) FILTER (WHERE salary > 50000) AS high_salaries,
        AVG(salary) FILTER (WHERE active)   AS avg_active_salary
    FROM employees GROUP BY dept;
```

**Note**: `COUNT(DISTINCT x) FILTER (WHERE p)` is rewritten the same way and then maintained like any `COUNT(DISTINCT)` (affected-group recompute). `LIST(...) FILTER` is not rewritten, because `LIST` keeps NULL elements; it uses affected-group recompute with the original SQL.

## Expressions

Expressions inside aggregates work transparently:

```sql
CREATE MATERIALIZED VIEW mv AS
    SELECT category,
        SUM(CASE WHEN value > 0 THEN value ELSE 0 END) AS positive_sum
    FROM t GROUP BY category;
```
