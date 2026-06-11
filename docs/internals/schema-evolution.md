# Schema Evolution

OpenIVM handles `ALTER TABLE` on base tables that have materialized views depending on them. Delta tables are automatically synced. Referenced column renames are propagated into MV metadata, while referenced column drops are blocked.

## Supported operations

| ALTER TABLE operation | Column referenced by MV? | Behavior |
|---|---|---|
| ADD COLUMN | n/a | Delta table synced via `ALTER TABLE ADD COLUMN`. IVM continues to work. |
| DROP COLUMN | No | Delta table synced via `ALTER TABLE DROP COLUMN`. IVM continues to work. |
| DROP COLUMN | Yes | Blocked with error: *"Cannot drop column 'x': it is referenced by materialized view 'mv'."* |
| RENAME COLUMN | No | Delta table synced via `ALTER TABLE RENAME COLUMN`. IVM continues to work. |
| RENAME COLUMN | Yes | Stored MV SQL, aux metadata, lineage metadata, and delta table schema are rewritten. IVM continues to work and user-visible output aliases are preserved. |

## How "referenced" is determined

For drops, OpenIVM checks every dependent MV before changing the delta table. It parses the stored query, rewrites column references through a scoped SQL AST traversal, and treats any successful rewrite as a reference. The same check also inspects aux-state and lineage metadata for DISTINCT, filtered group-count, semi/anti, window, and projection-key paths.

The rewrite is scoped to the query block that owns the target table. Unqualified references inside nested subqueries that bind a different table are not rewritten.

## Referenced renames

When a referenced column is renamed, OpenIVM updates the metadata needed by future refreshes:

- `openivm_views.sql_string` is rewritten to use the new source column name.
- Top-level MV output names are preserved, so `SELECT * FROM mv` keeps the original aliases.
- DISTINCT aux-state metadata, filtered group-count metadata, and semi/anti aux-state metadata are rewritten.
- Window partition lineage, projection-key lineage, and stored group-column source mappings are rewritten.
- The base delta table is renamed after metadata rewrite succeeds.

## How it works

OpenIVM intercepts `ALTER TABLE` statements on tracked base tables. For each alter:

1. Checks if the altered table has a delta table (i.e., it's tracked by IVM)
2. Determines the alter type (ADD, DROP, RENAME)
3. For DROP: checks all dependent MVs and blocks if any stored query or metadata references the column
4. For RENAME: rewrites dependent MV metadata, then syncs the delta table schema
5. For ADD: syncs the delta table schema

## Column ordering

Delta tables store base table columns first, followed by `openivm_multiplicity` and `openivm_timestamp`. After `ALTER TABLE ADD COLUMN`, DuckDB appends the new column at the end of the delta table (after the metadata columns). The insert rule uses explicit column lists in its `INSERT INTO openivm_delta_t` statements, so the column ordering mismatch is handled correctly.

## Auxiliary state repair

Some optimized refresh paths maintain auxiliary state tables, such as `openivm_distinct_count_<view>`, `openivm_filtered_group_count_<view>`, and `openivm_semi_anti_state_<view>`. If an aux table schema is stale or missing after a schema change, refresh can rebuild it from the current source data.

Repair is only allowed when no source deltas are pending. If deltas are pending, OpenIVM raises an error and asks you to recreate the view or restore the aux table. This prevents rebuilding aux state from a source table that no longer matches the MV's unprocessed delta cursor.

## Limitations

- `ALTER COLUMN TYPE` (changing a column's data type) is not handled. The delta table will have the old type and INSERTs may fail with a type mismatch.
- Adding a column with a `DEFAULT` value does not backfill the delta table. Only new INSERTs after the ALTER will include the new column.
- `SELECT *` in an existing MV does not expand to include newly added base-table columns. Use `CREATE OR REPLACE MATERIALIZED VIEW` to pick up new columns.
