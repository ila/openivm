# CTEs and Subqueries

## Example

```sql
CREATE TABLE employees (id INT, dept VARCHAR, salary INT);
INSERT INTO employees VALUES (1, 'Eng', 100), (2, 'Eng', 200), (3, 'Sales', 150);

CREATE MATERIALIZED VIEW dept_stats AS
    WITH dept_agg AS (
        SELECT dept, SUM(salary) AS total, COUNT(*) AS cnt
        FROM employees GROUP BY dept
    )
    SELECT dept, total, total / cnt AS avg_sal FROM dept_agg;

INSERT INTO employees VALUES (4, 'Eng', 300);
PRAGMA ivm('dept_stats');
```

## How IVM handles it

CTEs and subqueries are handled transparently. DuckDB's planner inlines CTEs and
decorrelates correlated subqueries during query planning, before OpenIVM's optimizer
rules run. By the time the IVM rewrite rules see the logical plan, CTEs have been
expanded into their equivalent join/aggregate/projection operators.

This means:

- **CTEs** work with any supported operator inside them (aggregates, joins, filters, etc.)
- **Scalar subqueries** are decorrelated to left joins with aggregation
- **EXISTS / IN subqueries** are decorrelated to semi-joins
- **NOT EXISTS / NOT IN** are decorrelated to anti-joins

The IVM delta rules apply to the decorrelated plan as usual.

## Limitations

- **Recursive CTEs** are not supported. Views with recursive CTEs fall back to full refresh.
- **Lateral joins** that cannot be decorrelated are not supported.
