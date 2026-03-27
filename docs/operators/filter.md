# Filter

Select rows matching a WHERE predicate, with optional column projection.

```sql
CREATE MATERIALIZED VIEW cheap_products AS
    SELECT id, name, price FROM products WHERE price < 20;
```

## How IVM handles it

The filter is applied directly to the delta scan -- only delta rows that pass the WHERE predicate are propagated. The upsert phase uses the same counting consolidation as projection (GROUP BY all columns, DELETE via `rowid` + `ROW_NUMBER`, INSERT via `generate_series`).

## Compiled SQL

### IVM query (delta propagation with filter)

```sql
WITH scan_0 (t0_id, t0_name, t0_price, t0__duckdb_ivm_multiplicity) AS (
    SELECT id, name, price, _duckdb_ivm_multiplicity
    FROM delta_products
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
),
filter_1 AS (
    SELECT * FROM scan_0
    WHERE (t0_price) < (20)
),
projection_2 (t1_id, t1_name, t1_price, t1__duckdb_ivm_multiplicity) AS (
    SELECT t0_id, t0_name, t0_price, t0__duckdb_ivm_multiplicity
    FROM filter_1
)
INSERT INTO delta_cheap_products (id, name, price, _duckdb_ivm_multiplicity)
SELECT * FROM projection_2;
```

### Upsert (counting consolidation)

```sql
WITH _ivm_net AS (
    SELECT id, name, price,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_cheap_products
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, name, price
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
DELETE FROM cheap_products WHERE rowid IN (
    SELECT v.rowid FROM (
        SELECT rowid, id, name, price,
            ROW_NUMBER() OVER (PARTITION BY id, name, price ORDER BY rowid) AS _rn
        FROM cheap_products
    ) v JOIN _ivm_net d
        ON v.id IS NOT DISTINCT FROM d.id
       AND v.name IS NOT DISTINCT FROM d.name
       AND v.price IS NOT DISTINCT FROM d.price
    WHERE d._net < 0 AND v._rn <= -d._net
);

WITH _ivm_net AS (
    SELECT id, name, price,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_cheap_products
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, name, price
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
INSERT INTO cheap_products SELECT id, name, price
FROM _ivm_net, generate_series(1, _ivm_net._net::BIGINT)
WHERE _ivm_net._net > 0;
```

When a filter is combined with grouped aggregates, the HAVING clause forces a group-recompute for affected groups (see the grouped aggregates doc for that pattern).

## Limitations

Filters work with any predicate that DuckDB supports in WHERE clauses; no additional restrictions beyond the base operator's limitations.
