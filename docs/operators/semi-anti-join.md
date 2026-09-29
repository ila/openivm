# Semi and anti join

> Linearity: **NON-LINEAR THRESHOLD** ([what does this mean?](../internals/linearity.md))

## Example

```sql
CREATE TABLE customers (id INT, region VARCHAR);
CREATE TABLE orders (customer_id INT, amount INT);
INSERT INTO customers VALUES (1, 'west'), (2, 'east'), (3, 'west');
INSERT INTO orders VALUES (1, 100);

CREATE MATERIALIZED VIEW customers_with_orders AS
    SELECT c.id, c.region
    FROM customers c
    WHERE EXISTS (
        SELECT 1 FROM orders o
        WHERE o.customer_id = c.id AND o.amount > 50
    );

INSERT INTO orders VALUES (3, 75);
PRAGMA refresh('customers_with_orders');
```

`NOT EXISTS`, `IN (subquery)`, `NOT IN (subquery)`, and explicit `SEMI JOIN` / `ANTI JOIN` use the same maintenance path.

## How IVM handles it

Semi and anti joins are not maintained as ordinary join output. They are threshold operators:

```text
SEMI(left, right, p) = left tuples where match_count(left, right, p) > 0
ANTI(left, right, p) = left tuples where match_count(left, right, p) = 0
```

OpenIVM keeps an auxiliary table named `openivm_semi_anti_state_<view>`. It stores one row per distinct left tuple, plus:

| Column | Meaning |
|---|---|
| `_left_count` | Bag multiplicity of the left tuple. |
| `_match_count` | Number of right rows matching that left tuple under the original predicate. |

For `NOT IN`, the aux table also stores the right-side row count (`_right_count`) and the number of
right rows whose key is NULL (`_right_null_count`), plus a hidden left column recording whether the left
key is NULL. Visibility then follows SQL's null-aware `NOT IN` semantics: a left tuple is visible if it
has no match and either the right side is empty, or its key is not NULL and the right side has no NULL keys.

Refresh uses the aux state instead of emitting raw right-side join matches:

1. Snapshot the old aux rows and compute old visibility (`_match_count > 0` for semi, `_match_count = 0` for anti).
2. Consolidate left deltas by the projected left tuple.
3. Join existing aux left tuples against right deltas with the original predicate and add the net match-count delta.
4. Apply left deltas to `_left_count`.
5. Insert newly appearing left tuples into aux, initializing `_match_count` by joining against the current right table.
6. Find affected left tuples whose left multiplicity or visibility changed.
7. Delete old visible MV rows for affected tuples and insert current visible rows, using `generate_series(1, _left_count)` to preserve bag semantics.
8. Remove aux rows whose `_left_count <= 0`.

The order matters when the same refresh contains both left and right deltas. Right deltas are applied to existing aux rows before new left rows are initialized from the current right table, so same-batch left inserts and right inserts are not double-counted.

## Supported shapes

The aux-state path supports:

- Explicit `SEMI JOIN` and `ANTI JOIN`.
- `WHERE EXISTS (...)` and `WHERE NOT EXISTS (...)` when DuckDB produces the supported semi/anti shape.
- `WHERE x IN (SELECT y FROM ...)` and `WHERE x NOT IN (SELECT y FROM ...)` (equality membership over a single-column subquery), including computed keys such as `CAST(x AS BIGINT)`.
- One left base table and one right base table. For `IN` / `NOT IN`, the left side may also be a derived query.
- Projection/filter stacks over the left tuple.
- Arbitrary predicates, including non-equality predicates and right-side filters inside the `EXISTS` subquery.
- Duplicate left rows under bag semantics.

## Limitations

Aggregates over semi/anti output do not use the aux state. They are classified `GROUP_RECOMPUTE`: with `GROUP BY` keys only the affected groups are recomputed; without group keys the whole (single-row) result is recomputed and diffed against the stored one.

Other unsupported semi/anti shapes fall back to full refresh:

- Join chains where the left side of the semi/anti join is itself a join or another subplan.
- Semi/anti joins whose right input is a derived subquery in `FROM`.
- `UNION ALL` branches containing independent semi/anti views.

For ordinary inner/cross/arbitrary-predicate joins, see [Inner join, cross join, and arbitrary join predicates](inner-join.md).
