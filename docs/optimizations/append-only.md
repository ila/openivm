# Append-Only Optimizations

## Behavior

When all pending delta rows are inserts (no deletes or updates), OpenIVM skips expensive
cleanup steps during refresh:

- **Grouped aggregates**: Skips the zero-row DELETE (`DELETE FROM mv WHERE COALESCE(col, 0) = 0`),
  which normally scans the entire MV table looking for groups that cancelled to zero.
  With insert-only deltas, no group can reach zero.

- **Projection/filter views**: Skips the DELETE phase entirely (the ROW_NUMBER window +
  JOIN that removes net-deleted tuples) and the GROUP BY consolidation. Instead, inserts
  delta rows directly into the MV.

- **MIN/MAX aggregates**: Uses `GREATEST(old, new)` for MAX and `LEAST(old, new)` for MIN
  instead of the full group-recompute strategy (delete affected groups, re-query from base).
  Adding elements can only extend the range, never invalidate the current extremum.

## Settings

| Setting | Default | Description |
|---|---|---|
| `openivm_skip_aggregate_delete` | `true` | Skip zero-row DELETE for grouped aggregates when insert-only |
| `openivm_skip_projection_delete` | `true` | Skip DELETE and consolidation for projections when insert-only |
| `openivm_minmax_incremental` | `true` | Use GREATEST/LEAST for MIN/MAX when insert-only |

Set to `false` to disable and fall back to the traditional IVM path:
```sql
SET openivm_skip_aggregate_delete = false;
SET openivm_skip_projection_delete = false;
SET openivm_minmax_incremental = false;
```

## When it applies

The optimization activates when two conditions are met:

1. **All base table deltas are insert-only** — no rows with `openivm_multiplicity < 0`.
2. **The delta view produces only insert rows** — the join's Möbius inclusion-exclusion sign times the leaf weights doesn't produce any negative-weight rows.

Condition 2 depends on the join rule used:

| View type | Safe? | Reason |
|---|---|---|
| No join (projection, filter, single-table aggregate) | Always | Multiplicity passes through directly from base delta |
| DuckLake join (any number of tables) | Always | [N-term telescoping](../ducklake.md#n-term-telescoping-join-rule) uses one delta per term with no inclusion-exclusion cross-terms |
| Standard inclusion-exclusion join, one table changed | Yes | Other deltas are empty, so no cross-terms fire |
| Standard inclusion-exclusion self-join (one delta table) | No | Both join leaves reference the same delta, so cross-terms always fire |
| Standard inclusion-exclusion join, 2+ tables changed | No | Cross-terms produce negative-weight rows for `k=2, 4, …` mask sizes |
| Regular-table N-term compilation | Adapter-dependent | The canonical `current - delta` old-state expression contains negative helper weights. The external adapter must replace it with an equivalent old-state scan or otherwise prove that the output delta is insert-only. |

### Why standard joins with multiple changes are unsafe

Standard inclusion-exclusion reads the **current** (post-batch) state for non-delta sides.
When both R and S have inserts, the cross-term `ΔR ⨝ ΔS` is emitted with combined weight
`(-1)^(k-1) × ∏ wᵢ = (-1) × (+1) × (+1) = -1` (Möbius sign times the Z-set bilinear product).
This negative-weight row is essential for correctness — it cancels the double-counting that
the |mask|=1 terms introduce — but it would be lost if the DELETE phase is skipped.

DuckLake's N-term telescoping avoids this by reading the **old** state (via `AT VERSION`)
for non-delta sides, so there is no double-counting and no Möbius sign needed.

[Regular-table N-term compilation](../operators/inner-join.md#regular-table-n-term-compilation)
also removes exponential cross-terms. Its portable SQL reconstructs old state as
`current - delta`, so an external adapter decides whether the resulting refresh qualifies
for the append-only fast path.

## Detection

At refresh time, before compiling the upsert SQL:

**Standard tables**: Query each delta table for delete rows:
```sql
SELECT COUNT(*) FROM openivm_delta_table
WHERE openivm_timestamp >= last_update AND openivm_multiplicity < 0
```
Also checks total row count to determine if the delta is empty (no changes at all).

**DuckLake tables**: Compare `last_snapshot_id` with `current_snapshot()` to detect changes,
then query `ducklake_table_deletions()` to check for deletes.

## Insert-only fast path: multiplicity fan-out

When the optimization fires for projection/filter views, the INSERT phase uses
`generate_series` to fan out integer multiplicities directly from the delta — preserving bag
semantics without needing a `GROUP BY` consolidation:

```sql
INSERT INTO mv
SELECT <data columns>
FROM openivm_delta_view, generate_series(1, openivm_multiplicity::BIGINT)
WHERE openivm_multiplicity > 0;
```

The `generate_series(1, _w)` cross-product replicates each row exactly `_w` times. For the
common `_w = 1` case this is a single replicate (i.e., a no-op), but the form is general:
upstream operators are free to emit a single row with weight > 1 instead of N duplicates,
and the fan-out happens here rather than in a join's UNION ALL.

## What is avoided

| Step | Normal path | Append-only path |
|---|---|---|
| CTE consolidation (aggregates) | `SUM(_w * col)` over delta | Same (the integer-weight form is already cheap; CASE round-trip is gone post-Z-set refactor) |
| Zero-row DELETE (aggregates) | Full MV scan: `DELETE WHERE COALESCE(col, 0) = 0` | **Skipped** |
| MIN/MAX group-recompute | Delete affected groups + re-query from base | **MERGE with GREATEST/LEAST** |
| Net consolidation (projections) | `GROUP BY all_cols HAVING SUM(_w) != 0` | **Skipped** |
| ROW_NUMBER DELETE (projections) | Window function + JOIN to find rows to remove | **Skipped** |
| INSERT (projections) | `generate_series(1, _net)` from the consolidated CTE | Direct `INSERT FROM openivm_delta_view, generate_series(1, _w) WHERE _w > 0` (no consolidation CTE) |
