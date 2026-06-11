# GROUPING SETS, ROLLUP, CUBE

OpenIVM maintains grouped-aggregate views that compute several grouping levels at once with
`GROUPING SETS`, `ROLLUP`, or `CUBE` — for example a subtotal per `(region, product)`, a subtotal
per `region`, and a grand total, all in one view.

```sql
CREATE TABLE sg(region VARCHAR, product VARCHAR, amount INT);
INSERT INTO sg VALUES ('US','W',100), ('US','G',200), ('EU','W',50);

CREATE MATERIALIZED VIEW gs AS
  SELECT region, product, SUM(amount) AS total
  FROM sg
  GROUP BY ROLLUP(region, product);
-- detail rows per (region,product), a subtotal per region (product = NULL),
-- and a grand total (region = NULL, product = NULL)

INSERT INTO sg VALUES ('US','W',10), ('EU','G',5);
DELETE FROM sg WHERE region='US' AND product='G';
PRAGMA refresh('gs');
```

## How it's maintained

A multi-level grouping produces several grouping-set rows from each source row (its detail group,
the matching subtotals, and the grand total). On refresh, OpenIVM **recomputes only the grouping-set
rows affected by the change** — the groups touched by the delta are re-derived from the current data
and replaced, while grouping levels and groups that weren't touched are left as-is. Far cheaper than
recomputing the whole view, and exact.

The `NULL`s that mark subtotal and grand-total rows are part of the stored result and behave the
same way under refresh as any other group key.

## Caveats

- `ROLLUP(a, b)` and `CUBE(a, b)` expand to a fixed set of grouping levels; every level is
  maintained, so a single source row updates one row at each level it participates in.
- Use `GROUPING()` / `GROUPING_ID()` if you need to distinguish a genuine `NULL` key value from the
  `NULL` that marks a subtotal row.
