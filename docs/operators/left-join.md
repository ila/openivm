# Left join

## Example

```sql
CREATE TABLE customers (id INT, name VARCHAR);
CREATE TABLE orders (customer_id INT, product VARCHAR, amount INT);
INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie');
INSERT INTO orders VALUES (1, 'Widget', 100), (1, 'Gadget', 200);

CREATE MATERIALIZED VIEW customer_orders AS
    SELECT c.name, o.product, o.amount
    FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;
```

Initial result:

| name | product | amount |
|---|---|---|
| Alice | Widget | 100 |
| Alice | Gadget | 200 |
| Bob | NULL | NULL |
| Charlie | NULL | NULL |

```sql
-- Bob gets an order: his NULL row is replaced with real data
INSERT INTO orders VALUES (2, 'Bolt', 50);
PRAGMA ivm('customer_orders');
```

| name | product | amount |
|---|---|---|
| Alice | Widget | 100 |
| Alice | Gadget | 200 |
| Bob | Bolt | 50 |
| Charlie | NULL | NULL |

## How IVM handles it

**Algebraic rule:**

```
delta(L ⟕ R) uses inclusion-exclusion with LEFT→INNER demotion:

Term with only right-side deltas:  L INNER JOIN delta(R)   [demoted from LEFT]
Term with left-side deltas:        delta(L) LEFT JOIN R     [preserves LEFT semantics]
Cross-delta term:                  delta(L) LEFT JOIN delta(R)
```

The delta query uses inclusion-exclusion (same as [inner join](inner-join.md)), with one key difference: terms where **only** right-side leaves have deltas demote `LEFT JOIN` to `INNER JOIN`. This prevents spurious NULL-extended rows — when you join the full left table against only the changed right rows, you only want rows that actually match.

The upsert uses **partial recompute** instead of counting-based consolidation:

1. Find all affected left keys from the delta view.
2. DELETE from the MV all rows matching those keys.
3. Re-INSERT from the original LEFT JOIN query, filtered to those keys.

This avoids the complexity of tracking NULL↔non-NULL transitions incrementally. The cost is proportional to the number of affected left keys, not the total table size.

The parser injects a hidden `_ivm_left_key` column containing the preserved-side join key (see [Metadata Columns](../internals/metadata-columns.md#_ivm_left_key)).

## Compiled SQL (2-table join, 3 terms)

### IVM query (delta propagation)

```sql
-- Term 1: new/deleted customers matched against current orders
-- Left-side has deltas → keep LEFT JOIN semantics
-- New customers with no orders correctly produce NULL-extended rows
scan_0 = SELECT id, name, mul FROM delta_customers WHERE ts >= '...'
scan_1 = SELECT customer_id, product, amount FROM orders
join_2 = scan_0 LEFT JOIN scan_1 ON (id = customer_id)

-- Term 2: current customers matched against new/deleted orders
-- Only right-side has deltas → demote LEFT→INNER
-- We only want rows where the new order actually matches a customer
-- (not every customer NULL-extended against the empty right side)
scan_4 = SELECT id, name FROM customers
scan_5 = SELECT customer_id, product, amount, mul FROM delta_orders WHERE ts >= '...'
join_6 = scan_4 INNER JOIN scan_5 ON (id = customer_id)

-- Term 3: delta_customers ⨝ delta_orders (cross-delta correction)
-- Left-side has deltas → keep LEFT JOIN semantics
-- XOR of multiplicities encodes the inclusion-exclusion sign
join_11 = delta_customers LEFT JOIN delta_orders ON (id = customer_id)
projection_12 = SELECT ..., (mul1) != (mul2) AS combined_mul FROM join_11

-- Combine all terms into a single delta stream
union_13 = term1 UNION ALL term2 UNION ALL term3
INSERT INTO delta_customer_orders (..., _duckdb_ivm_multiplicity)
SELECT ... FROM union_13;
```

### Upsert (partial recompute)

```sql
-- Step 1: find all left keys affected by this refresh cycle
-- These are the customers whose rows may have changed
-- Step 2: delete those customers' entire set of rows from the MV
DELETE FROM customer_orders WHERE (_ivm_left_key) IN (
    SELECT DISTINCT _ivm_left_key FROM delta_customer_orders
    WHERE _duckdb_ivm_timestamp >= '{ts}'
);

-- Step 3: re-insert from the original query, filtered to affected keys only
-- This correctly handles all transitions:
--   NULL→value (customer got their first order)
--   value→NULL (customer's last order was deleted)
--   value→value (order amount changed)
INSERT INTO customer_orders
SELECT * FROM (
    SELECT c.name, o.product, o.amount, c.id AS _ivm_left_key
    FROM customers c LEFT JOIN orders o ON c.id = o.customer_id
) _ivm_lj
WHERE (_ivm_left_key) IN (
    SELECT DISTINCT _ivm_left_key FROM delta_customer_orders
    WHERE _duckdb_ivm_timestamp >= '{ts}'
);
```

## RIGHT JOIN

DuckDB rewrites `RIGHT JOIN` to `LEFT JOIN` internally (swapping the table order). OpenIVM handles them identically. The parser extracts the preserved-side key from the right table before DuckDB applies the rewrite.

```sql
-- This RIGHT JOIN:
SELECT p.name, s.qty FROM sales s RIGHT JOIN products p ON s.product_id = p.id;

-- Is maintained the same way as:
SELECT p.name, s.qty FROM products p LEFT JOIN sales s ON p.id = s.product_id;
```

## Mixed joins

You can combine INNER and LEFT joins in the same query:

```sql
CREATE MATERIALIZED VIEW emp_bonus AS
    SELECT e.name, d.dept_name, b.bonus
    FROM employees e
    INNER JOIN departments d ON e.dept_id = d.id
    LEFT JOIN bonuses b ON e.id = b.emp_id;
```

The inclusion-exclusion generates 2^3 - 1 = 7 terms. Terms where only `bonuses` (the right side of the LEFT JOIN) has deltas will demote the LEFT JOIN to INNER for that term.

## Limitations

- Partial recompute is proportional to the number of affected left keys, not the number of changed rows. If many left keys are affected, the recompute may scan a large portion of the base tables.
- `AGGREGATE_GROUP` views with LEFT JOIN sources use group recompute (not the standard MERGE), since NULL handling in aggregate deltas is not decomposable.
- Maximum 16 tables in the join (same limit as [inner join](inner-join.md)).
