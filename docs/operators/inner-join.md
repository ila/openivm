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

**Algebraic rule (inclusion-exclusion):**

For a two-table join R ⨝ S:

```
delta(R ⨝ S) = delta(R) ⨝ S
             ∪ R ⨝ delta(S)
             ∪ delta(R) ⨝ delta(S)   [with sign correction via XOR]
```

For N tables, this generalizes to 2^N - 1 terms — one for each non-empty subset of tables replaced by their delta scans. Each term replaces a subset of tables with their delta scans and keeps the rest as current base table scans. The terms are combined with `UNION ALL`, and the multiplicity is computed as the XOR of all delta multiplicities (XOR maps the ±1 inclusion-exclusion sign to boolean).

The maximum supported join width is 16 tables.

## Compiled SQL (2-table join, 3 terms)

```sql
-- Term 1: new/deleted users matched against current orders
-- Captures the effect of user changes on the join result
scan_0 = SELECT id, name, mul FROM delta_users WHERE ts >= '...'
scan_1 = SELECT user_id, amount FROM orders
join_2 = scan_0 INNER JOIN scan_1 ON (id = user_id)

-- Term 2: current users matched against new/deleted orders
-- Captures the effect of order changes on the join result
scan_4 = SELECT id, name FROM users
scan_5 = SELECT user_id, amount, mul FROM delta_orders WHERE ts >= '...'
join_6 = scan_4 INNER JOIN scan_5 ON (id = user_id)

-- Term 3: delta_users ⨝ delta_orders (cross-delta correction)
-- Prevents double-counting rows affected by changes to BOTH tables
-- XOR of multiplicities encodes the inclusion-exclusion sign:
--   insert ⨝ insert = true XOR true = false (subtract, because terms 1+2 already counted it)
join_11 = delta_users INNER JOIN delta_orders ON (id = user_id)
projection_12 = SELECT ..., (mul1) != (mul2) AS combined_mul FROM join_11

-- Combine all terms into a single delta stream
union_13 = term1 UNION ALL term2 UNION ALL term3

-- Write the join delta into the delta view table
INSERT INTO delta_user_orders (name, amount, _duckdb_ivm_multiplicity)
SELECT name, amount, mul FROM union_13;
```

The join result is a projection, so the upsert uses [counting-based consolidation](projection-filter.md).

## DuckLake tables

When all join leaves are DuckLake scans, OpenIVM uses the **N-term telescoping** formula
instead of inclusion-exclusion. This produces exactly N terms instead of 2^N - 1 by
leveraging DuckLake's time travel (`AT VERSION`) to read the old state of non-delta tables.

Additionally, terms for unchanged tables (where `last_snapshot_id == current_snapshot_id`)
are skipped at plan time via [empty-delta term skipping](../optimizations/empty-delta-skip.md).

See [DuckLake IVM integration](../ducklake.md) for details.

## Upsert (counting consolidation)

The join delta is a projection (no aggregation), so the upsert uses counting-based consolidation — identical to [projection-filter](projection-filter.md).

```sql
-- Compute the net change per distinct tuple
-- Inserts count +1, deletes count -1; canceling pairs sum to 0 and are filtered out
WITH _ivm_net AS (
    SELECT name, amount,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_user_orders
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY name, amount
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
-- Delete net-removed copies using rowid + ROW_NUMBER for precise bag-semantic deletes
DELETE FROM user_orders WHERE rowid IN (
    SELECT v.rowid FROM (
        SELECT rowid, name, amount,
            ROW_NUMBER() OVER (PARTITION BY name, amount ORDER BY rowid) AS _rn
        FROM user_orders
    ) v JOIN _ivm_net d
        ON v.name IS NOT DISTINCT FROM d.name
       AND v.amount IS NOT DISTINCT FROM d.amount
    WHERE d._net < 0 AND v._rn <= -d._net
);

-- Insert net-added copies using generate_series for bag semantics
WITH _ivm_net AS (
    SELECT name, amount,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_user_orders
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY name, amount
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
INSERT INTO user_orders SELECT name, amount
FROM _ivm_net, generate_series(1, _ivm_net._net::BIGINT)
WHERE _ivm_net._net > 0;
```
