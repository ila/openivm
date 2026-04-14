# Insert-Only Fast Path

## Behavior

When all pending delta rows are inserts (no deletes or updates), OpenIVM skips expensive
cleanup steps during refresh:

- **Grouped aggregates**: Skips the zero-row DELETE (`DELETE FROM mv WHERE COALESCE(col, 0) = 0`),
  which normally scans the entire MV table looking for groups that cancelled to zero.
  With insert-only deltas, no group can reach zero.

- **Projection/filter views**: Skips the DELETE phase entirely (the ROW_NUMBER window +
  JOIN that removes net-deleted tuples) and the GROUP BY consolidation. Instead, inserts
  delta rows directly into the MV.

## When it applies

The optimization activates when two conditions are met:

1. **All base table deltas are insert-only** — no rows with `_duckdb_ivm_multiplicity = false`.
2. **The delta view produces only insert rows** — no sign correction (XOR) flips
   multiplicities to false.

Condition 2 depends on the join rule used:

| View type | Safe? | Reason |
|---|---|---|
| No join (projection, filter, single-table aggregate) | Always | Multiplicity passes through directly from base delta |
| DuckLake join (any number of tables) | Always | [N-term telescoping](../ducklake.md#n-term-telescoping-join-rule) uses one delta per term with no XOR cross-terms |
| Standard join, one table changed | Yes | Other deltas are empty, so no cross-terms fire |
| Standard join, self-join (one delta table) | No | Both join leaves reference the same delta, so cross-terms always fire |
| Standard join, 2+ tables changed | No | Inclusion-exclusion cross-terms use XOR, producing false multiplicities |

### Why standard joins with multiple changes are unsafe

Standard inclusion-exclusion reads the **current** (post-batch) state for non-delta sides.
When both R and S have inserts, the cross-term `ΔR ⋈ ΔS` corrects for double-counting
via XOR: `true XOR true = false`. This false-multiplicity row is essential for correctness
but would be lost if the DELETE phase is skipped.

DuckLake's N-term telescoping avoids this by reading the **old** state (via `AT VERSION`)
for non-delta sides, so there is no double-counting and no XOR correction needed.

## Detection

At refresh time, before compiling the upsert SQL:

**Standard tables**: Query each delta table for delete rows:
```sql
SELECT COUNT(*) FROM delta_table
WHERE _duckdb_ivm_timestamp >= last_update AND _duckdb_ivm_multiplicity = false
```
Also checks total row count to determine if the delta is empty (no changes at all).

**DuckLake tables**: Compare `last_snapshot_id` with `current_snapshot()` to detect changes,
then query `ducklake_table_deletions()` to check for deletes.

## What is avoided

| Step | Normal path | Insert-only path |
|---|---|---|
| CTE consolidation (aggregates) | `SUM(CASE WHEN mul THEN col ELSE -col END)` | Same (DuckDB handles CASE WHEN efficiently) |
| Zero-row DELETE (aggregates) | Full MV scan: `DELETE WHERE COALESCE(col, 0) = 0` | **Skipped** |
| Net consolidation (projections) | `GROUP BY all_cols HAVING SUM(...) != 0` | **Skipped** |
| ROW_NUMBER DELETE (projections) | Window function + JOIN to find rows to remove | **Skipped** |
| INSERT (projections) | `generate_series(1, _net)` for net-insert tuples | Direct `INSERT FROM delta WHERE mul = true` |
