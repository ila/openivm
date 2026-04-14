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

## What Is Avoided

| Step | Skipped |
|---|---|
| Delta query planning | Yes |
| LPTS timestamp bookkeeping | Yes |
| SQL generation for INSERT/DELETE/MERGE | Yes |
| Downstream cascade trigger | Yes |

## When It Does Not Apply

- At least one delta table contains rows (standard) or snapshot IDs differ (DuckLake)
- View type is `FULL_REFRESH` (unsupported operators always recompute)
