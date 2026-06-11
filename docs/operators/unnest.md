# UNNEST

OpenIVM incrementally maintains views that `UNNEST` a `LIST`/array column into rows.

```sql
CREATE TABLE u_src(id INT, tags VARCHAR[]);
INSERT INTO u_src VALUES (1, ['a','b']), (2, ['c']);

CREATE MATERIALIZED VIEW tag_rows AS
  SELECT id, unnest(tags) AS tag FROM u_src;
-- (1,'a'), (1,'b'), (2,'c')

INSERT INTO u_src VALUES (3, ['d','e']);
DELETE FROM u_src WHERE id = 2;
PRAGMA refresh('tag_rows');
-- (1,'a'), (1,'b'), (3,'d'), (3,'e')
```

## How it's maintained

`UNNEST` is **maintained fully incrementally**, like any projection. Each source row expands
into a fixed bag of output rows independently of every other row, so a change to one source
row only adds or removes that row's expanded output:

- Inserting a source row adds its expanded rows.
- Deleting a source row removes exactly the rows it produced.

No unchanged data is rescanned — the cost of a refresh is proportional to the number of changed
source rows times the average list length, not to the size of the table.

## Caveats

- A `NULL` or empty list produces no rows (matching DuckDB `UNNEST` semantics), so a source row
  with an empty list contributes nothing to the view — and deleting it removes nothing.
- `UNNEST` under an aggregate (e.g. `SELECT tag, COUNT(*) FROM (… unnest …) GROUP BY tag`) is
  maintained as the relevant aggregate, with the unnest feeding it as a linear input below.
- Multi-column and `UNNEST(..., recursive := true)` follow DuckDB's expansion semantics; the
  expanded columns pass through to the view unchanged.
