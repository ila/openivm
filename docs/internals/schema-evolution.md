# Schema Evolution

OpenIVM handles `ALTER TABLE` on base tables that have materialized views depending on them. Delta tables are automatically synced, and operations that would break an MV are blocked.

## Supported operations

| ALTER TABLE operation | Column referenced by MV? | Behavior |
|---|---|---|
| ADD COLUMN | n/a | Delta table synced via `ALTER TABLE ADD COLUMN`. IVM continues to work. |
| DROP COLUMN | No | Delta table synced via `ALTER TABLE DROP COLUMN`. IVM continues to work. |
| DROP COLUMN | Yes | Blocked with error: *"Cannot drop column 'x': it is referenced by materialized view 'mv'."* |
| RENAME COLUMN | No | Delta table synced via `ALTER TABLE RENAME COLUMN`. IVM continues to work. |
| RENAME COLUMN | Yes | Blocked with error: *"Cannot rename column 'x': it is referenced by materialized view 'mv'."* |

## How "referenced" is determined

The check plans the stored `sql_string` from `_duckdb_ivm_views`, walks the logical plan tree, and inspects `LOGICAL_GET` nodes for the target table. If the column appears in the node's bound column references (`GetColumnIds()`), it's referenced. This is precise — no regex or string matching.

## How it works

The `IVMInsertRule` optimizer extension intercepts `LOGICAL_ALTER` nodes. For each `AlterTableInfo`:

1. Checks if the altered table has a delta table (i.e., it's tracked by IVM)
2. Determines the alter type (ADD, DROP, RENAME)
3. For DROP/RENAME: checks all MVs that depend on this table for column references
4. Syncs the delta table schema or blocks the operation

## Column ordering

Delta tables store base table columns first, followed by `_duckdb_ivm_multiplicity` and `_duckdb_ivm_timestamp`. After `ALTER TABLE ADD COLUMN`, DuckDB appends the new column at the end of the delta table (after the metadata columns). The insert rule uses explicit column lists in its `INSERT INTO delta_t` statements, so the column ordering mismatch is handled correctly.

## Limitations

- `ALTER COLUMN TYPE` (changing a column's data type) is not handled. The delta table will have the old type and INSERTs may fail with a type mismatch.
- Adding a column with a `DEFAULT` value does not backfill the delta table. Only new INSERTs after the ALTER will include the new column.
- The MV's stored `sql_string` is not updated after schema changes. If the MV uses `SELECT *`, the MV definition still references the old schema. Use `CREATE OR REPLACE MATERIALIZED VIEW` to pick up new columns.
