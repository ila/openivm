# Grouped aggregates

## Example

```sql
CREATE TABLE sales (region VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 100), ('EU', 200);

CREATE MATERIALIZED VIEW sales_summary AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;

INSERT INTO sales VALUES ('US', 50), ('JP', 300);
PRAGMA ivm('sales_summary');
```

## How IVM handles it

The delta table is scanned, grouped by the same keys as the view, and consolidated into net changes per group. A `MERGE INTO` statement atomically updates existing groups and inserts new ones. Groups whose aggregate values reach zero are deleted.

## Compiled SQL

### IVM query (delta computation)

```sql
WITH scan_0 (...) AS (
    SELECT region, amount, _duckdb_ivm_multiplicity
    FROM delta_sales
    WHERE _duckdb_ivm_timestamp >= '...'
),
aggregate_1 (...) AS (
    SELECT region, _duckdb_ivm_multiplicity, sum(amount), count_star()
    FROM scan_0
    GROUP BY region, _duckdb_ivm_multiplicity
),
projection_2 (...) AS (
    SELECT region, sum_amount, count_star, _duckdb_ivm_multiplicity
    FROM aggregate_1
)
INSERT INTO delta_sales_summary (region, total, cnt, _duckdb_ivm_multiplicity)
SELECT * FROM projection_2;
```

### Upsert (MERGE)

```sql
WITH ivm_cte AS (
    SELECT region,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -total ELSE total END) AS total,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -cnt ELSE cnt END) AS cnt
    FROM delta_sales_summary
    GROUP BY region
)
MERGE INTO sales_summary v USING ivm_cte d
ON v.region IS NOT DISTINCT FROM d.region
WHEN MATCHED THEN UPDATE SET total = v.total + d.total, cnt = v.cnt + d.cnt
WHEN NOT MATCHED THEN INSERT (region, total, cnt) VALUES (d.region, d.total, d.cnt);

DELETE FROM sales_summary WHERE total = 0 AND cnt = 0;
```

## Supported aggregates

| Function | Strategy | Notes |
|----------|----------|-------|
| `SUM` | Incremental (MERGE) | Delta added to existing value |
| `COUNT`, `COUNT(*)` | Incremental (MERGE) | Delta added to existing count |
| `MIN`, `MAX` | Group recompute | Affected groups deleted and re-inserted from the original query |
| `AVG` | Group recompute | Not decomposable as a simple delta |
| `HAVING` | Group recompute | Groups may enter or leave the result set after changes |
| `STDDEV`, `STRING_AGG` | Full refresh | Automatically detected; view uses full recompute |

## Expressions

Expressions inside aggregates work transparently:

```sql
CREATE MATERIALIZED VIEW mv AS
    SELECT category,
        SUM(CASE WHEN value > 0 THEN value ELSE 0 END) AS positive_sum
    FROM t GROUP BY category;
```
