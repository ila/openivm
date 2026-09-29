# Left join

> Linearity: **BILINEAR** ([what does this mean?](../internals/linearity.md))

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
PRAGMA refresh('customer_orders');
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

Term with only right-side deltas:  L INNER JOIN delta(R)          [demoted from LEFT]
Term with only left-side deltas:   delta(L) LEFT JOIN R            [preserves LEFT semantics]
Cross-delta term:                  delta(L) INNER JOIN delta(R)    [demoted from LEFT]
```

The delta query uses inclusion-exclusion (same as [inner join](inner-join.md)), with one key difference: demotion is decided **per join**. In each term, a `LEFT JOIN` is demoted to `INNER JOIN` iff its right (NULL-supplying) subtree contains a delta leaf of that term. This prevents spurious NULL-extended rows — when you join against only the changed right rows, you only want rows that actually match — while chained LEFT JOINs whose right side has no delta in the term keep their NULL-padded rows (`DemoteLeftJoinsForMask` in `src/delta/operators/join.cpp`).

When a LEFT JOIN is kept (not demoted) but its NULL-supplying leaf also has pending deltas in the same refresh, the term would read that leaf's post-DML state for the dangling-row decision and double-count with the higher-order term. OpenIVM guards this by excluding preserved-side rows whose key's match count drops from >0 to 0 in that leaf's delta (an ANTI join against a shared `openivm_transition_keys_<n>` CTE; a MARK-join filter for non-DuckDB target dialects). The guard only applies when the NULL-supplying side is a single base-table leaf.

For projection views, the upsert uses **partial recompute** instead of counting-based consolidation:

1. Find all affected left keys from the delta view.
2. DELETE from the MV all rows matching those keys.
3. Re-INSERT from the original LEFT JOIN query, filtered to those keys.

This avoids the complexity of tracking NULL↔non-NULL transitions incrementally. The cost is proportional to the number of affected left keys, not the total table size. Key matching is NULL-safe (`IS NOT DISTINCT FROM`). When OpenIVM knows which source table supplies the preserved key, the re-INSERT pushes the affected keys into that table's scan instead of filtering the full query result.

Fast paths:

- **Insert-only deltas on the preserved side only** (no pending changes on any NULL-supplying table) are appended directly, like a plain projection, without partial recompute.
- **DuckLake sources with mixed deltas** can use a hybrid path: keys whose row count after applying the delta matches the recomputed count get exact tuple deltas, and only the remaining (NULL↔match transition) keys are recomputed.

The parser injects a hidden `openivm_left_key` column containing the preserved-side join key (see [Metadata Columns](../internals/metadata-columns.md#openivm_left_key-and-openivm_right_key)).

For grouped aggregates over LEFT/RIGHT JOINs, OpenIVM uses the Larson & Zhou MERGE path by default (`openivm_left_join_merge=true`) when the aggregate shape is safe. Unsafe aggregate shapes use affected-group recompute instead.

The MERGE path adds a hidden `openivm_match_count` column (`COUNT` of the NULL-supplying join key) so right-side aggregate columns can transition between NULL and real values. At CREATE time OpenIVM also generates a *secondary delta* for every LEFT JOIN level: an INSERT into the view's delta table that emits the NULL-padded rows appearing or disappearing when a key's match count crosses zero. Refresh runs it before the MERGE. If a secondary delta cannot be built for some level, the view is created as `GROUP_RECOMPUTE`; if its delta sources cannot be resolved at refresh time, that refresh uses affected-group recompute.

## Compiled SQL (2-table join, 3 terms)

### IVM query (delta propagation)

```sql
-- Term 1: new/deleted customers matched against current orders
-- Left-side has deltas → keep LEFT JOIN semantics
-- New customers with no orders correctly produce NULL-extended rows
scan_0 = SELECT id, name, mul FROM openivm_delta_customers WHERE ts >= '...'
scan_1 = SELECT customer_id, product, amount FROM orders
join_2 = scan_0 LEFT JOIN scan_1 ON (id = customer_id)

-- Term 2: current customers matched against new/deleted orders
-- Only right-side has deltas → demote LEFT→INNER
-- We only want rows where the new order actually matches a customer
-- (not every customer NULL-extended against the empty right side)
scan_4 = SELECT id, name FROM customers
scan_5 = SELECT customer_id, product, amount, mul FROM openivm_delta_orders WHERE ts >= '...'
join_6 = scan_4 INNER JOIN scan_5 ON (id = customer_id)

-- Term 3: openivm_delta_customers ⨝ openivm_delta_orders (cross-delta correction)
-- Right side has deltas → demote LEFT→INNER
-- Combined multiplicity = (-1)^(k-1) * w1 * w2 with k=2 → (-1) * w1 * w2
-- (Möbius inclusion-exclusion sign × Z-set bilinear product — see inner-join.md)
join_11 = openivm_delta_customers INNER JOIN openivm_delta_orders ON (id = customer_id)
projection_12 = SELECT ..., (-1) * mul1 * mul2 AS combined_mul FROM join_11

-- Combine all terms into a single delta stream
union_13 = term1 UNION ALL term2 UNION ALL term3
INSERT INTO openivm_delta_customer_orders (..., openivm_multiplicity)
SELECT ... FROM union_13;
```

### Upsert (partial recompute)

```sql
-- Step 1: find all left keys affected by this refresh cycle
-- These are the customers whose rows may have changed
WITH openivm_affected AS (
    SELECT DISTINCT openivm_left_key FROM openivm_delta_customer_orders
    WHERE openivm_timestamp > '{ts}'::TIMESTAMP
)
-- Step 2: delete those customers' entire set of rows from the MV data table
DELETE FROM openivm_data_customer_orders AS openivm_delete_target
USING openivm_affected _d
WHERE _d.openivm_left_key IS NOT DISTINCT FROM openivm_delete_target.openivm_left_key;

-- Step 3: re-insert from the original query, restricted to affected keys only.
-- The affected keys are pushed into the scan of the preserved-side table.
-- This correctly handles all transitions:
--   NULL→value (customer got their first order)
--   value→NULL (customer's last order was deleted)
--   value→value (order amount changed)
WITH openivm_affected AS (...)
INSERT INTO openivm_data_customer_orders
SELECT * FROM (
    SELECT c.name, o.product, o.amount, c.id AS openivm_left_key
    FROM (SELECT openivm_lj_src.* FROM customers openivm_lj_src
          INNER JOIN openivm_affected openivm_lj_aff
            ON openivm_lj_src.id IS NOT DISTINCT FROM openivm_lj_aff.openivm_left_key) c
    LEFT JOIN orders o ON c.id = o.customer_id
) openivm_lj;
```

## RIGHT JOIN

OpenIVM treats `RIGHT JOIN` as a `LEFT JOIN` with the sides swapped: the preserved side is the right input, `openivm_left_key` holds the right-side join key, and a `RIGHT JOIN` is demoted to `INNER` in a delta term iff its left (NULL-supplying) subtree has a delta leaf.

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

The inclusion-exclusion generates 2^3 - 1 = 7 terms. Every term whose mask includes `bonuses` (the right side of the LEFT JOIN) demotes that LEFT JOIN to INNER; terms that change only `employees` and/or `departments` keep it as a LEFT JOIN.

## Limitations

- Partial recompute is proportional to the number of affected left keys, not the number of changed rows. If many left keys are affected, the recompute may scan a large portion of the base tables.
- `AGGREGATE_GROUP` views with LEFT JOIN sources use MERGE only for supported aggregate shapes. If `openivm_left_join_merge=false`, or if the aggregate shape is unsafe, OpenIVM uses affected-group recompute.
- **LEFT JOIN with a *computed* aggregate argument** (anything other than a plain bound column reference — `COALESCE`, `CASE`, an arithmetic expression, a constant), or a projection wrapper that computes over non-group columns, is also forced onto the group-recompute path even when the delta is insert-only. The Larson-Zhou MERGE template doesn't correctly handle the case where a new right-side row converts an existing NULL-padded row into a match for these expressions, so the classifier (`OuterJoinAggregateNeedsRecompute`, called from `src/core/ivm_view_classifier.cpp`) sets `has_minmax=true` (overloaded as a "use group-recompute" signal) and `CompileAggregateGroups` in `src/upsert/refresh_compiler.cpp` skips the MERGE path when no real MIN/MAX is present.
- LEFT/RIGHT JOIN aggregates with a table function on the preserved side also use affected-group recompute.
- Maximum 16 tables in the join (same limit as [inner join](inner-join.md)).

## Settings

| Setting | Default | Description |
|---|---|---|
| `openivm_left_join_merge` | `true` | Use incremental MERGE for supported LEFT/RIGHT JOIN aggregate views |
| `openivm_regular_nterm_left` | `true` | When compiling for an external engine (`compile_only`), use N-term telescoping instead of inclusion-exclusion for `SIMPLE_PROJECTION` views whose joins are only INNER/LEFT (see [inner join](inner-join.md#regular-table-n-term-compilation)) |
