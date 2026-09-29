# Empty Delta Skip

## Behavior

When **all** delta tables for a materialized view are empty, OpenIVM skips the entire
refresh cycle. No query planning, LPTS computation, or SQL generation is performed.

The check runs **after** upstream cascade refreshes, so upstream views have a chance to
populate delta tables before the check. Skipping a view does not stop the cascade:
downstream views are still visited and each skips itself when its own deltas are empty.

## Detection

**Standard DuckDB tables:** Count the delta rows newer than the view's watermark
(`openivm_timestamp >= last_update`) together with the rows that have
`openivm_multiplicity < 0`. If the count is zero for all delta tables, all deltas are empty.
Delta tables are shared by all consumers, so rows retained for other views are not counted.

**DuckLake tables:** Compare `last_snapshot_id` (stored in metadata) with the catalog's
current snapshot. If they are equal, no changes have occurred since the last refresh.
Because DuckLake snapshot ids are catalog-wide, a differing id is checked against the
`ducklake_snapshots()` change manifest for the source's table id (inserted, deleted, inlined,
altered, or dropped). Without changes, the stored snapshot id is advanced and the table counts
as empty; this reads only catalog metadata. Altered or dropped tables, or a changed table
identity, force a full recompute. If the manifest cannot be read, OpenIVM counts
`ducklake_table_insertions()` and `ducklake_table_deletions()` between the two snapshots.

## Per-Term Skipping (DuckLake Joins)

For DuckLake join views, empty-delta detection also operates at the individual term level.
In the [N-term telescoping join rule](../ducklake.md#n-term-telescoping-join-rule), each
term corresponds to one base table's delta. If the detection above finds no changes for that
table, the term is skipped — avoiding plan copy, renumbering, delta scan creation, and SQL
generation for that term.

In a 5-table star schema where only the fact table changed, 4 of 5 terms are skipped.

A safety fallback ensures at least one term is always generated to avoid an empty UNION ALL.

## Per-Term Skipping (Standard Joins)

Standard non-DuckLake joins can use inclusion-exclusion or regular N-term compilation.
Both strategies skip terms for unchanged inputs.

For inclusion-exclusion, each term is a join where some tables use delta scans. If
**any** table in a term's bitmask has zero pending delta rows, the term produces zero
rows and is skipped.

Detection queries each delta table's row count (filtered by timestamp since last refresh)
in the same pass as the insert-only detection used by FK pruning. Together with FK-aware
pruning, this can eliminate the majority of terms in multi-table joins where only one
table changed.

For example, in a 3-table join where only table A changed, 6 of the 7 inclusion-exclusion
terms contain either B's or C's empty delta and are skipped — only the 1 term with A's
delta alone is generated.

For [regular N-term compilation](../operators/inner-join.md#regular-table-n-term-compilation),
compile facts mark unchanged leaves before SQL generation. OpenIVM omits their delta terms
and avoids reconstructing their old state. A three-table join where only A changed therefore
emits one term instead of three.

The adaptive cost model uses the same signal. Empty source deltas do not contribute
active join terms, so `PRAGMA refresh_cost` estimates the work for the terms that can
actually produce rows.

## What Is Avoided

| Step | Skipped |
|---|---|
| Delta query planning | Yes |
| LPTS timestamp bookkeeping | Yes |
| SQL generation for INSERT/DELETE/MERGE | Yes |
| Downstream cascade trigger | No — dependents are still visited and skip themselves if their deltas are empty |

## Setting

| Setting | Default | Description |
|---|---|---|
| `openivm_skip_empty_deltas` | `true` | Enable empty-delta skipping (early-exit + per-term join skip) |

```sql
SET openivm_skip_empty_deltas = false;  -- disable: always run the refresh pipeline
```

## When It Does Not Apply

- At least one delta table contains rows (standard) or snapshot IDs differ (DuckLake)
- View type is `FULL_REFRESH` (unsupported operators always recompute)
- The view's auxiliary state needs repair
- The refresh runs inside an explicit transaction (`BEGIN ... COMMIT`): the refresh program is
  compiled conservatively over all registered sources
- Compile-only callers such as `openivm_compile_with_facts`
- `openivm_skip_empty_deltas` is set to `false`

A view with a [refresh hook](../refresh_hooks.md) that is skipped also skips its hook, unless a
staged after-hook is still pending.

When the setting is `false`, an empty-delta refresh still executes the selected refresh
path. The adaptive cost model includes this fixed overhead in `PRAGMA refresh_cost`.
