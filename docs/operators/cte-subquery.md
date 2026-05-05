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

CTEs and subqueries are handled transparently when DuckDB lowers them to supported
logical-plan shapes. DuckDB usually inlines CTEs and decorrelates subqueries during
query planning, before OpenIVM's optimizer rules run. By the time the IVM rewrite
rules see the logical plan, CTEs have often been expanded into their equivalent
join/aggregate/projection operators.

This means:

- **CTEs** work with any supported operator inside them (aggregates, joins, filters, etc.)
- **Scalar subqueries** may be decorrelated to left joins with aggregation, or to
  DuckDB's scalar `SINGLE` `DELIM_JOIN` shape
- **EXISTS** subqueries can be maintained through the [semi-join aux-state path](semi-anti-join.md)
- **NOT EXISTS** subqueries can be maintained through the [anti-join aux-state path](semi-anti-join.md)

The IVM delta rules apply to the decorrelated plan as usual. If DuckDB decorrelates a subquery into a plan shape OpenIVM does not support, the view falls back to full refresh.

## Scalar correlated subqueries

Scalar correlated subqueries that DuckDB plans as `SINGLE` `DELIM_JOIN` are maintained
with affected-key `GROUP_RECOMPUTE`. OpenIVM extracts the visible correlated outer
columns as recompute keys, deletes only those keys from the MV data table, then
re-inserts their current results from the stored view query.

```sql
CREATE TABLE warehouse (w_id INT, w_name VARCHAR);

CREATE MATERIALIZED VIEW mv_scalar AS
    WITH numbers AS (SELECT generate_series AS n FROM generate_series(1, 20))
    SELECT n, (SELECT w_name FROM warehouse WHERE w_id = n) AS wh_name
    FROM numbers;
```

In this example, changes to `warehouse` affect only the visible key `n` values whose
scalar lookup changed. The refresh is incremental and preserves the scalar-subquery
semantics, including transitions between a matched value and `NULL`.

## Limitations

- **Recursive CTEs** are not supported. Views with recursive CTEs fall back to full refresh.
- **Lateral joins** are incremental when they lower to supported `DELIM_JOIN` /
  `DEPENDENT_JOIN` shapes with visible correlated keys. Other lateral shapes fall back
  to full refresh.
- **IN** and **NOT IN** are not documented as aux-state semi/anti support because their NULL semantics may require MARK joins. Those views are incremental only when the final decorrelated plan uses supported operators.
