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

Both children of the UNION ALL are rewritten independently:

```
delta(T1 UNION ALL T2) = delta(T1) UNION ALL delta(T2)
```

## Compiled SQL

```sql
WITH scan_0 (...) AS (
    SELECT id, product, _duckdb_ivm_multiplicity
    FROM delta_orders_us WHERE _duckdb_ivm_timestamp >= '...'
),
scan_2 (...) AS (
    SELECT id, product, _duckdb_ivm_multiplicity
    FROM delta_orders_eu WHERE _duckdb_ivm_timestamp >= '...'
),
union_4 (...) AS (
    SELECT * FROM scan_0 UNION ALL SELECT * FROM scan_2
)
INSERT INTO delta_all_orders (id, product, _duckdb_ivm_multiplicity)
SELECT * FROM union_4;
```

The upsert uses [counting-based consolidation](projection.md).

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
