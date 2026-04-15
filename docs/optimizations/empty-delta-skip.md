# Empty Delta Skip

## Behavior

When **all** delta tables for a materialized view are empty, OpenIVM skips the entire
refresh cycle. No query planning, LPTS computation, or SQL generation is performed.

The check runs **after** upstream cascade refreshes, so upstream views have a chance to
populate delta tables before the check.

## Detection

**Standard DuckDB tables:** Check `COUNT(*) FROM delta_table`. If zero for all delta
tables, all deltas are empty.

**DuckLake tables:** Compare `last_snapshot_id` (stored in metadata) with
`current_snapshot()`. If they are equal, no changes have occurred since the last refresh.
This is a metadata-only operation — no table data is read.

The current snapshot is queried once per refresh (not per table).

## Per-Term Skipping (DuckLake Joins)

For DuckLake join views, empty-delta detection also operates at the individual term level.
In the [N-term telescoping join rule](../ducklake.md#n-term-telescoping-join-rule), each
term corresponds to one base table's delta. If that table's `last_snapshot_id` equals the
current snapshot, the term is skipped — avoiding plan copy, renumbering, delta scan creation,
and SQL generation for that term.

In a 5-table star schema where only the fact table changed, 4 of 5 terms are skipped.

A safety fallback ensures at least one term is always generated to avoid an empty UNION ALL.

## Per-Term Skipping (Standard Joins)

For standard (non-DuckLake) joins using inclusion-exclusion, empty-delta detection also
skips individual terms. Each inclusion-exclusion term is a join where some tables use delta
scans. If **any** table in a term's bitmask has zero pending delta rows, the entire join
term produces zero rows (a join with an empty input is always empty) and is skipped.

Detection queries each delta table's row count (filtered by timestamp since last refresh)
in the same pass as the insert-only detection used by FK pruning. Together with FK-aware
pruning, this can eliminate the majority of terms in multi-table joins where only one
table changed.

For example, in a 3-table join where only table A changed, 6 of the 7 inclusion-exclusion
terms contain either B's or C's empty delta and are skipped — only the 1 term with A's
delta alone is generated.

## What Is Avoided

| Step | Skipped |
|---|---|
| Delta query planning | Yes |
| LPTS timestamp bookkeeping | Yes |
| SQL generation for INSERT/DELETE/MERGE | Yes |
| Downstream cascade trigger | Yes |

## Setting

| Setting | Default | Description |
|---|---|---|
| `ivm_skip_empty_deltas` | `true` | Enable empty-delta skipping (early-exit + per-term DuckLake skip) |

```sql
SET ivm_skip_empty_deltas = false;  -- disable: always run full refresh pipeline
```

## When It Does Not Apply

- At least one delta table contains rows (standard) or snapshot IDs differ (DuckLake)
- View type is `FULL_REFRESH` (unsupported operators always recompute)
- `ivm_skip_empty_deltas` is set to `false`
