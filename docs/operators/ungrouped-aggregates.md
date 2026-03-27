# Ungrouped aggregates

## Example

```sql
CREATE TABLE scores (val INT);
INSERT INTO scores VALUES (10), (20), (30);

CREATE MATERIALIZED VIEW total_score AS
    SELECT SUM(val) AS total, COUNT(*) AS cnt FROM scores;

INSERT INTO scores VALUES (40);
PRAGMA ivm('total_score');
-- total=100, cnt=4
```

## How IVM handles it

A single CTE consolidates all delta columns in one pass. The existing single-row MV is updated by adding the consolidated delta values.

## Compiled SQL

### Upsert (consolidated CTE + UPDATE)

```sql
WITH _ivm_delta AS (
    SELECT
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -total ELSE total END) AS d_total,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -cnt ELSE cnt END) AS d_cnt
    FROM delta_total_score
)
UPDATE total_score SET
    total = COALESCE(total, 0) + COALESCE((SELECT d_total FROM _ivm_delta), 0),
    cnt = COALESCE(cnt, 0) + COALESCE((SELECT d_cnt FROM _ivm_delta), 0);
```

`COALESCE` handles the case where the MV starts with NULL values (empty base table produces `SUM() = NULL`).
