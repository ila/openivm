# POSITIONAL JOIN

OpenIVM maintains views built with `POSITIONAL JOIN`, which pairs rows from two inputs by their
ordinal position (row 1 with row 1, row 2 with row 2, …) rather than by a predicate.

```sql
CREATE TABLE left_p(a INT);
CREATE TABLE right_p(b INT);
INSERT INTO left_p VALUES (1), (2), (3);
INSERT INTO right_p VALUES (10), (20), (30);

CREATE MATERIALIZED VIEW pj AS
  SELECT a, b FROM left_p POSITIONAL JOIN right_p;
-- (1,10), (2,20), (3,30)

INSERT INTO left_p VALUES (4);
INSERT INTO right_p VALUES (40);
PRAGMA refresh('pj');
-- (1,10), (2,20), (3,30), (4,40)
```

## How it's maintained

A positional join pairs rows by position, so the output for a given position depends on the row
counts of both inputs — inserting a row anywhere can shift the pairing. OpenIVM keeps the view
correct by **recomputing the affected rows** at refresh rather than assuming a row-for-row delta.
Appending to the end of both inputs (the common pattern) simply extends the result with the newly
paired rows.

## Caveats

- Positional pairing is defined over the inputs' row order; if the two sides have different lengths,
  the surplus rows on the longer side are NULL-padded, per DuckDB semantics.
- This operator is most predictable for append-style workloads where rows are added in lockstep at
  the end of both inputs. Interleaved inserts/deletes that change relative positions are still
  handled correctly, but cause more of the view to be re-derived.
