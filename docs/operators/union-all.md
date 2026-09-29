# Union all

> Linearity: **LINEAR** (UNION ALL is Z-set addition). ([what does this mean?](../internals/linearity.md))

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
PRAGMA refresh('all_orders');
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
    SELECT id, product, openivm_multiplicity
    FROM openivm_delta_orders_us WHERE openivm_timestamp >= '...'
),
-- Scan delta rows from the second UNION branch (EU orders)
scan_2 (...) AS (
    SELECT id, product, openivm_multiplicity
    FROM openivm_delta_orders_eu WHERE openivm_timestamp >= '...'
),
-- Combine deltas from both branches — same as the original UNION ALL
union_4 (...) AS (
    SELECT * FROM scan_0 UNION ALL SELECT * FROM scan_2
)
-- Write the combined delta into the delta view table
INSERT INTO openivm_delta_all_orders (id, product, openivm_multiplicity)
SELECT * FROM union_4;
```

## Upsert (counting consolidation)

The UNION ALL delta is a projection (no aggregation), so the upsert uses the same counting-based consolidation as [projection-filter](projection-filter.md).

```sql
-- Compute the net change per distinct tuple across both branches
WITH openivm_net AS (
    SELECT id, product,
        SUM(openivm_multiplicity) AS _net
    FROM openivm_delta_all_orders
    WHERE openivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, product
    HAVING SUM(openivm_multiplicity) != 0
)
-- Delete net-removed copies: join negative net tuples to the MV first,
-- then rank only the matched candidates
, openivm_delete_net AS (
    SELECT * FROM openivm_net WHERE _net < 0
), openivm_delete_candidates AS (
    SELECT v.rowid, v.id AS id, v.product AS product, d._net
    FROM all_orders v JOIN openivm_delete_net d
        ON v.id IS NOT DISTINCT FROM d.id
       AND v.product IS NOT DISTINCT FROM d.product
), openivm_ranked_deletes AS (
    SELECT rowid, _net,
        ROW_NUMBER() OVER (PARTITION BY id, product ORDER BY rowid) AS _rn
    FROM openivm_delete_candidates
)
DELETE FROM all_orders WHERE rowid IN (
    SELECT rowid FROM openivm_ranked_deletes WHERE _rn <= -_net
);

-- Insert net-added copies
WITH openivm_net AS (
    SELECT id, product,
        SUM(openivm_multiplicity) AS _net
    FROM openivm_delta_all_orders
    WHERE openivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, product
    HAVING SUM(openivm_multiplicity) != 0
)
INSERT INTO all_orders SELECT id, product
FROM openivm_net, generate_series(1, openivm_net._net::BIGINT)
WHERE openivm_net._net > 0;
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

## UNION (distinct)

`UNION` without `ALL` is not linear: a row stays in the result while any branch still produces it.
OpenIVM maintains it with affected-key recompute (`GROUP_RECOMPUTE`) over the visible output
columns — output rows touched by the delta are deleted and recomputed from the view query. A
`UNION` over aggregates is recomputed by the aggregate's group keys.
