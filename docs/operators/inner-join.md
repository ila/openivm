# Inner join, cross join, and arbitrary join predicates

> Linearity: **BILINEAR** ([what does this mean?](../internals/linearity.md))

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
PRAGMA refresh('user_orders');
```

## How IVM handles it

**Algebraic rule (Möbius inclusion-exclusion over Z-sets):**

For a two-table join R ⨝ S, with current bases pointing at `R_now = R_old + ΔR` and `S_now = S_old + ΔS` (deltas already merged into the source by the insert rule):

```
Δ(R ⨝ S) = ΔR ⨝ S_now    [+1 sign, |mask|=1]
         + R_now ⨝ ΔS    [+1 sign, |mask|=1]
         − ΔR ⨝ ΔS       [−1 sign, |mask|=2 — corrects double-counting]
```

For N tables, this generalises to 2^N − 1 terms — one for each non-empty subset of tables replaced by their delta scans. Each term replaces a subset of tables with their delta scans and keeps the rest as current base table scans. The terms are combined with `UNION ALL`. The combined multiplicity for a term with mask size *k* is

```
combined_w = (-1)^(k-1) × ∏ wᵢ      over leaves i in the mask
```

— the **Möbius inclusion-exclusion sign** times the **Z-set bilinear product** of leaf multiplicities. The Möbius sign is required precisely because the non-delta legs read the current (post-DML) state; without it, the sum over masks double- (and quadruple-, …) counts the cross-terms.

This is *not* the textbook DBSP all-positive delta-join formula, which would apply if non-delta legs read `R_old` instead of `R_now`. OpenIVM reads `R_now` because the changes have already been committed to the source by the time the delta is computed.

The same rule is used for `INNER JOIN`, `CROSS JOIN`, and arbitrary-predicate joins. A cross product is just the same join with no predicate. Non-equality predicates are kept in the generated join terms.

The maximum supported join width is 16 tables.

## Compiled SQL (2-table join, 3 terms)

```sql
-- Term 1: new/deleted users matched against current orders
-- Captures the effect of user changes on the join result
scan_0 = SELECT id, name, mul FROM openivm_delta_users WHERE ts >= '...'
scan_1 = SELECT user_id, amount FROM orders
join_2 = scan_0 INNER JOIN scan_1 ON (id = user_id)

-- Term 2: current users matched against new/deleted orders
-- Captures the effect of order changes on the join result
scan_4 = SELECT id, name FROM users
scan_5 = SELECT user_id, amount, mul FROM openivm_delta_orders WHERE ts >= '...'
join_6 = scan_4 INNER JOIN scan_5 ON (id = user_id)

-- Term 3: openivm_delta_users ⨝ openivm_delta_orders (cross-delta correction)
-- Prevents double-counting rows affected by changes to BOTH tables.
-- Combined multiplicity = (-1)^(k-1) * w1 * w2 with k=2:
--   insert × insert = (-1) * (+1)*(+1) = -1   (subtract — terms 1+2 already counted it)
--   delete × insert = (-1) * (-1)*(+1) = +1
--   delete × delete = (-1) * (-1)*(-1) = -1
join_11 = openivm_delta_users INNER JOIN openivm_delta_orders ON (id = user_id)
projection_12 = SELECT ..., (-1) * mul1 * mul2 AS combined_mul FROM join_11

-- Combine all terms into a single delta stream
union_13 = term1 UNION ALL term2 UNION ALL term3

-- Write the join delta into the delta view table
INSERT INTO openivm_delta_user_orders (name, amount, openivm_multiplicity)
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
-- Inserts carry +1, deletes carry -1; canceling pairs sum to 0 and are filtered out
WITH openivm_net AS (
    SELECT name, amount,
        SUM(openivm_multiplicity) AS _net
    FROM openivm_delta_user_orders
    WHERE openivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY name, amount
    HAVING SUM(openivm_multiplicity) != 0
)
-- Delete net-removed copies using rowid + ROW_NUMBER for precise bag-semantic deletes
DELETE FROM user_orders WHERE rowid IN (
    SELECT v.rowid FROM (
        SELECT rowid, name, amount,
            ROW_NUMBER() OVER (PARTITION BY name, amount ORDER BY rowid) AS _rn
        FROM user_orders
    ) v JOIN openivm_net d
        ON v.name IS NOT DISTINCT FROM d.name
       AND v.amount IS NOT DISTINCT FROM d.amount
    WHERE d._net < 0 AND v._rn <= -d._net
);

-- Insert net-added copies using generate_series for bag semantics
WITH openivm_net AS (
    SELECT name, amount,
        SUM(openivm_multiplicity) AS _net
    FROM openivm_delta_user_orders
    WHERE openivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY name, amount
    HAVING SUM(openivm_multiplicity) != 0
)
INSERT INTO user_orders SELECT name, amount
FROM openivm_net, generate_series(1, openivm_net._net::BIGINT)
WHERE openivm_net._net > 0;
```
