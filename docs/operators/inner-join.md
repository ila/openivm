# Inner join

## Example

```sql
CREATE TABLE users (id INT, name VARCHAR);
CREATE TABLE orders (user_id INT, amount INT);
INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');
INSERT INTO orders VALUES (1, 100), (2, 200);

CREATE MATERIALIZED VIEW user_orders AS
    SELECT u.name, o.amount
    FROM users u INNER JOIN orders o ON u.id = o.user_id;

INSERT INTO orders VALUES (1, 50);
PRAGMA ivm('user_orders');
```

## How IVM handles it

For N tables in the join, OpenIVM generates 2^N - 1 inclusion-exclusion terms. Each term replaces a subset of tables with their delta scans and keeps the rest as current base table scans. The terms are combined with `UNION ALL`, and the multiplicity is computed as the XOR of all delta multiplicities.

The maximum supported join width is 16 tables.

## Compiled SQL (2-table join, 3 terms)

```sql
-- Term 1: delta_users ⨝ orders
scan_0 = SELECT id, name, mul FROM delta_users WHERE ts >= '...'
scan_1 = SELECT user_id, amount FROM orders
join_2 = scan_0 INNER JOIN scan_1 ON (id = user_id)

-- Term 2: users ⨝ delta_orders
scan_4 = SELECT id, name FROM users
scan_5 = SELECT user_id, amount, mul FROM delta_orders WHERE ts >= '...'
join_6 = scan_4 INNER JOIN scan_5 ON (id = user_id)

-- Term 3: delta_users ⨝ delta_orders (cross-term)
-- Multiplicity = XOR: (mul_users) != (mul_orders)
join_11 = delta_users INNER JOIN delta_orders ON (id = user_id)
projection_12 = SELECT ..., (mul1) != (mul2) AS combined_mul FROM join_11

-- UNION ALL all terms
union_13 = term1 UNION ALL term2 UNION ALL term3

-- Project to output columns
INSERT INTO delta_user_orders (name, amount, _duckdb_ivm_multiplicity)
SELECT name, amount, mul FROM union_13;
```

The join result is a projection, so the upsert uses [counting-based consolidation](projection.md).

## Non-inner joins

`LEFT JOIN`, `RIGHT JOIN`, `FULL OUTER JOIN`, and `CROSS JOIN` are automatically detected and use full refresh. Non-GET subtrees (e.g., `UNION ALL` as a join child) are handled transparently.
