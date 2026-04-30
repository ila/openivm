# Top-K: ORDER BY + LIMIT

OpenIVM supports `ORDER BY … LIMIT k` in materialized views. The refresh strategy depends on whether the query also has a `GROUP BY`.

## Aggregate top-k (`GROUP BY … ORDER BY … LIMIT k`)

```sql
CREATE MATERIALIZED VIEW top_sales AS
  SELECT product, SUM(revenue) AS total
  FROM orders
  GROUP BY product
  ORDER BY total DESC
  LIMIT 5;
```

**Strategy**: fully incremental — classified as `AGGREGATE_GROUP`.

The data table `_ivm_data_top_sales` stores **all groups**, not just the top-5. On every `PRAGMA ivm()`, the normal delta-MERGE path updates exactly the groups that changed. The VIEW definition wraps the data table with `ORDER BY total DESC LIMIT 5`, applying the top-k filter at read time.

**Why this is correct (DBSP decomposition)**: `GROUP BY + aggregation` is a Z-set-linear operator — each delta row updates exactly one group, independently. `ORDER BY LIMIT k` is a monotone selection over the aggregate output. Separating them lets the linear part (group maintenance) run incrementally while the monotone selection is applied at read time without any re-evaluation cost.

**Complexity**: O(D + G) per refresh, where D = delta size, G = number of affected groups. Reading the view is O(G log G) for the sort, which DuckDB can push down with `top_n` when `k` is small.

**Caveats**:
- Ties on the `ORDER BY` key are non-deterministic across refreshes. If multiple groups have the same aggregate value at position k, different refreshes may return different subsets at the boundary. Add a tie-breaking column (e.g. `ORDER BY total DESC, product`) for deterministic results.
- `LIMIT WITH TIES` is not yet supported.

## Projection top-k (`SELECT cols … ORDER BY … LIMIT k`, no GROUP BY)

```sql
CREATE MATERIALIZED VIEW top_scores AS
  SELECT player, score
  FROM leaderboard
  ORDER BY score DESC
  LIMIT 10;
```

**Strategy**: incremental maintenance of the unlimited projection — classified as `SIMPLE_PROJECTION`.

`ORDER BY LIMIT k` over a projection is not Z-set linear:

```text
delta(top_k(R)) != top_k(delta(R))
```

OpenIVM handles this with the same DBSP-style split used for aggregate top-k: the maintained data table stores the full projected Z-set, and the user-facing view applies `ORDER BY … LIMIT k` at read time. Inserts and deletes update the projection state incrementally; reading the MV asks DuckDB to compute the top-k over that maintained state.

**Complexity**: refresh cost is proportional to the source deltas plus normal projection maintenance. Reading the view sorts/selects from the maintained projected state; DuckDB can use `TopN` for small `k`.

**Caveats**:
- Same tie-breaking caveat as aggregate top-k.
- `OFFSET` is supported; `LIMIT WITH TIES` is not.

## Implementation notes

- DuckDB's `top_n` optimizer normally fuses `ORDER BY + LIMIT` into a single `LOGICAL_TOP_N` node, but LPTS disables `top_n`, so the planner emits separate `LOGICAL_LIMIT → LOGICAL_ORDER_BY` nodes. The checker handles both shapes transparently.
- For aggregate top-k, the parser strips `LOGICAL_LIMIT + LOGICAL_ORDER_BY` from `select_plan` before LPTS serialization, so the stored `sql_string` contains only the aggregate query. The `ORDER BY … LIMIT k` suffix is appended to the `CREATE VIEW` DDL.
- For projection top-k, the parser performs the same split: the stored `sql_string` contains the unlimited projection, and the `ORDER BY … LIMIT k` suffix is appended to the `CREATE VIEW` DDL.
