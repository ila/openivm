# Union all

## Example

```sql
CREATE TABLE orders_us (id INT, product VARCHAR);
CREATE TABLE orders_eu (id INT, product VARCHAR);
INSERT INTO orders_us VALUES (1, 'Widget');
INSERT INTO orders_eu VALUES (2, 'Gadget');

CREATE MATERIALIZED VIEW all_orders AS
    SELECT id, product FROM orders_us
    UNION ALL
    SELECT id, product FROM orders_eu;

INSERT INTO orders_us VALUES (3, 'Bolt');
PRAGMA ivm('all_orders');
```

## How IVM handles it

**Algebraic rule:**

```
delta(T1 UNION ALL T2) = delta(T1) UNION ALL delta(T2)
```

UNION ALL is a **linear operator** — its incremental form is the same as the original, applied to the deltas. Both children are rewritten independently.

## Compiled SQL

```sql
-- Scan delta rows from the first UNION branch (US orders)
WITH scan_0 (...) AS (
    SELECT id, product, _duckdb_ivm_multiplicity
    FROM delta_orders_us WHERE _duckdb_ivm_timestamp >= '...'
),
-- Scan delta rows from the second UNION branch (EU orders)
scan_2 (...) AS (
    SELECT id, product, _duckdb_ivm_multiplicity
    FROM delta_orders_eu WHERE _duckdb_ivm_timestamp >= '...'
),
-- Combine deltas from both branches — same as the original UNION ALL
union_4 (...) AS (
    SELECT * FROM scan_0 UNION ALL SELECT * FROM scan_2
)
-- Write the combined delta into the delta view table
INSERT INTO delta_all_orders (id, product, _duckdb_ivm_multiplicity)
SELECT * FROM union_4;
```

## Upsert (counting consolidation)

The UNION ALL delta is a projection (no aggregation), so the upsert uses the same counting-based consolidation as [projection-filter](projection-filter.md).

```sql
-- Compute the net change per distinct tuple across both branches
WITH _ivm_net AS (
    SELECT id, product,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_all_orders
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, product
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
-- Delete net-removed copies
DELETE FROM all_orders WHERE rowid IN (
    SELECT v.rowid FROM (
        SELECT rowid, id, product,
            ROW_NUMBER() OVER (PARTITION BY id, product ORDER BY rowid) AS _rn
        FROM all_orders
    ) v JOIN _ivm_net d
        ON v.id IS NOT DISTINCT FROM d.id
       AND v.product IS NOT DISTINCT FROM d.product
    WHERE d._net < 0 AND v._rn <= -d._net
);

-- Insert net-added copies
WITH _ivm_net AS (
    SELECT id, product,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_all_orders
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, product
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
INSERT INTO all_orders SELECT id, product
FROM _ivm_net, generate_series(1, _ivm_net._net::BIGINT)
WHERE _ivm_net._net > 0;
```

## Composability

UNION ALL composes with other operators:

```sql
CREATE MATERIALIZED VIEW joined_union AS
    SELECT p.name, o.qty
    FROM products p INNER JOIN (
        SELECT product_id, qty FROM orders_a
        UNION ALL
        SELECT product_id, qty FROM orders_b
    ) o ON p.id = o.product_id;
```

The join rule treats the UNION ALL subtree as an opaque leaf and delegates to the union rule for rewriting.
