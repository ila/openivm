# COUNT(DISTINCT)

OpenIVM maintains grouped views that count distinct values, e.g. unique visitors per page.

```sql
CREATE TABLE visits(page VARCHAR, user_id INT);
INSERT INTO visits VALUES ('home',1), ('home',2), ('home',1), ('about',3);

CREATE MATERIALIZED VIEW cd AS
  SELECT page, COUNT(DISTINCT user_id) AS uniq
  FROM visits
  GROUP BY page;
-- home: 2 (users 1,2), about: 1 (user 3)

INSERT INTO visits VALUES ('home',4), ('about',3), ('contact',9);
DELETE FROM visits WHERE page='home' AND user_id=2;
PRAGMA refresh('cd');
-- home: 2 (users 1,4), about: 1, contact: 1
```

## How it's maintained

`COUNT(DISTINCT)` is not additive — you can't just add the delta's distinct count to the stored
one, because the same value may already be present (a repeat visit must not raise the count, and
removing one of several copies must not lower it). To stay exact, OpenIVM **recomputes only the
affected groups**: when a page's rows change, that page's distinct count is re-derived from the
current data, while every other group keeps its stored value. The cost of a refresh scales with the
number of groups touched by the delta, not the size of the table.

## Caveats

- A group is recomputed whenever any of its rows change, even if the distinct count ends up the
  same (e.g. inserting a duplicate, or deleting one of several copies of a value).
- Multiple `COUNT(DISTINCT)` columns and a mix of `COUNT(DISTINCT)` with ordinary additive
  aggregates (`SUM`, `COUNT(*)`) in the same view are supported; the affected groups are recomputed
  as a unit.
