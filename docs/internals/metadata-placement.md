# Metadata placement

OpenIVM stores its control state in metadata tables: definitions (`openivm_views`),
sources and watermarks (`openivm_delta_tables`), dependencies, refresh hooks, refresh
history and refresh profiles. By default these tables live in the `main` schema of each
native view catalog, or of the default database for external (DuckLake) view catalogs.
Two settings select one explicit location instead:

```sql
ATTACH 'control.duckdb' AS control;
SET openivm_metadata_catalog = 'control';  -- attached catalog; '' keeps the default placement
SET openivm_metadata_schema = 'openivm';   -- created on first use; default 'main'
```

Placement is a property of the database, not of a session: two sessions using different
locations would split one database's views. Plain `SET` therefore applies globally,
`SET SESSION` is rejected, and helper connections and the refresh daemon see the value.

Supported metadata catalogs are attached native DuckDB databases and PostgreSQL databases
attached with the `postgres` extension (`TYPE postgres`). Other catalog types are
rejected; a catalog name alone never enables a new backend.

Code: `src/include/core/metadata_location.hpp` defines `MetadataLocation` and
`MetadataLocator`. Every metadata read and write resolves its location there:
`RefreshMetadata::UseCatalog` selects it on helper connections, and every generated
statement that names a metadata table qualifies it with the resolved catalog **and**
schema. Features that persist more metadata, such as the compiled-SQL archive (#83) and a
dedicated internal schema (#82), should take their catalog and schema from
`MetadataLocator::Resolve`, not assume `main`.

## Placement rules

Selecting a metadata location never moves data objects:

| Object | Location |
|---|---|
| Metadata tables | the selected catalog and schema |
| `openivm_data_<key>`, `openivm_visible_<key>`, MV delta tables, aux state | next to the user-facing view (its catalog and schema) |
| `openivm_delta_<source>` | next to the source table, so delta capture stays in the writer's transaction |
| Internal tables of a view in an external catalog that is neither native nor DuckLake | the physical default database, `main` schema (unchanged) |
| `openivm_metadata_location` marker | `main` schema of every native catalog that holds the view or a native source |

Delta capture never reads metadata: a base-table write is captured whenever the source's
delta table exists, even while the metadata catalog is detached.

## Discovery on reopen

DuckDB does not persist `ATTACH` or `SET`. When a view is created with an explicit
location, OpenIVM writes `openivm_metadata_location(metadata_catalog, metadata_schema)` in
one statement into `main` of the view's catalog and of each native source catalog.

Resolution order for a view catalog:

1. `openivm_metadata_catalog` / `openivm_metadata_schema`, if set. A marker in the view's
   catalog that names another location is an error, never a silent split.
2. Otherwise the marker in the legacy candidate catalog (the native view catalog, else the
   default database).
3. Otherwise the legacy location `<candidate>.main`.

After reopening, attach the metadata catalog under the alias recorded in the marker;
no `SET` is needed. If the marker names a catalog that is not attached, operations that
need metadata fail with an error naming the alias. DuckLake view catalogs hold no marker;
a deployment with an in-memory frontend sets the location at startup.

Delta cleanup and DROP consult every location that may consume a delta table: the
configured location, marker targets, and legacy native catalogs. An unreachable marker
target aborts rather than risk deleting changes another view still needs.

## Existing databases

A configured location refuses to coexist with views registered elsewhere: CREATE fails
while another location holds registered views. Views are not migrated automatically. To
migrate, drop and recreate the views with the settings in place, or, with no concurrent
writers, copy every `openivm_*` metadata row into the new location in one transaction,
write the markers, verify, then delete the old rows.

Without the settings, nothing changes: legacy placement, statements and transactions are
as before.

## Why metadata and MV data cannot commit together

DuckDB lets one transaction write to only one attached database. When the metadata
catalog differs from the view's catalog, a refresh cannot commit the MV data and its
watermark atomically. OpenIVM never treats that as a silent non-atomic split. It uses an
explicit, crash-safe protocol in autocommit mode and refuses explicit transactions:

### Explicit transactions

`CREATE MATERIALIZED VIEW`, `PRAGMA refresh`/`refresh_pipeline`, `DROP VIEW` and
`DROP TABLE ... CASCADE` inside `BEGIN ... COMMIT` fail with
`cannot run inside an explicit transaction` when the metadata location is in another
database than the view. The caller transaction is left unchanged and must be rolled back.
When the metadata schema is in the view's own database (for example `control.main` views
with `control.openivm` metadata), everything is one database and explicit transactions
stay atomic, including rollback. `ALTER MATERIALIZED VIEW` writes only metadata and is
allowed.

### Refresh protocol (autocommit)

1. **Intent.** One metadata transaction sets `refresh_in_progress = true`.
2. **Data.** One data transaction applies the MV changes for native view catalogs (DuckLake
   data statements keep their existing commit boundaries).
3. **Watermarks.** One metadata transaction advances every source watermark and clears
   `refresh_in_progress`.
4. **Cleanup.** Consumed source-delta rows are deleted after step 3. This only removes rows
   every consumer's committed watermark has passed, so a failure here retains rows that
   later refreshes ignore.

Crash and retry semantics:

| Failure | State left | Next refresh |
|---|---|---|
| Before step 1 commits | unchanged | normal incremental refresh |
| After 1, before 2 commits | marker set, data unchanged | full recompute |
| Step 2 fails cleanly (rolled back) | marker retracted | normal incremental refresh |
| Crash inside 2 | marker set, data rolled back | full recompute |
| After 2, before 3 commits | marker set, data new, watermarks old | full recompute |
| After 3 | consistent | normal |

A full recompute derives the MV from current base data, so it neither loses changes nor
applies them twice, whatever happened before. Recomputed views emit their diff to
downstream views, which therefore also see each change exactly once. Full recomputes take
the same three steps, and recovery clears the marker only in step 3, together with the
new watermarks. A view marked interrupted never uses the empty-delta shortcut.

Within one process the database-wide mutation gate serializes tracked writes with
refresh, so the watermarks written in step 3 equal those observed by step 2. A native
DuckDB data file has a single writer process.

### Remote SQL metadata and concurrent clients

A PostgreSQL metadata schema can be shared by several processes. OpenIVM never scans
DuckDB storage for remote metadata: it reads committed rows through SQL, and explicit
transactions that would write it next to MV data are refused, so committed rows are the
exact state. Remote metadata tables use portable column types, `INSERT OR REPLACE`
becomes delete plus insert (CREATE publishes these rows last, so an interruption leaves no
row and `CREATE OR REPLACE` rebuilds the view), and inserts supply every key column instead
of relying on DuckDB defaults.

Refreshes of one view are serialized across clients by `openivm_refresh_leases`:

- A client inserts `(view_name, owner, lease_until)`; the primary key makes acquisition
  atomic. Expired leases are deleted first. A losing client gets
  `being refreshed by another OpenIVM client`.
- While the refresh runs, a background thread renews the lease every third of
  `openivm_metadata_lease_seconds` (default 600). Only a crashed or partitioned client's
  lease can expire. If a renewal fails, the client rolls back its native data transaction
  before commit.
- Step 3 is fenced: its metadata transaction fails unless this client still holds the lease
  and `refresh_epoch` is unchanged. A client that loses its lease after committing data
  bumps `refresh_epoch` and sets the marker, so the next refresh recomputes. Hooks and
  cascaded refreshes reuse the lease of the refresh that started them.
- A crashed client leaves its lease. Other clients wait for it to expire, then take over
  and recover via full recompute.

Residual risk: a client partitioned from PostgreSQL but still able to commit DuckLake data
past its lease can briefly expose rows that the next refresh recomputes. Choose a lease
longer than the longest refresh. Concurrent DuckLake source writers in other processes
during a refresh are outside this protocol (see [limitations](../limitations.md)).

## Missing and read-only metadata catalogs

- Configured catalog not attached: operations that need metadata fail with
  `is not attached`. Base-table writes are still captured. Reattach, then refresh.
- Read-only catalog: reading MVs works; CREATE, REFRESH, ALTER and DROP fail with
  `attached read-only` before writing anything.
- Missing schema: created on first use when the catalog is writable.

## Replacement and drop

`CREATE OR REPLACE` keeps exactly one metadata row in the selected location. DROP removes
data objects before metadata rows. A DROP interrupted between the two leaves only metadata,
never an unmaintained view; `DROP VIEW IF EXISTS <name>` removes the leftover metadata.
CREATE writes metadata last; a CREATE interrupted earlier leaves objects that
`CREATE OR REPLACE` cleans up.

## Tests

| Scenario | Test |
|---|---|
| Placement of metadata vs backing/delta tables, marker | `test/sql/metadata_location.test` |
| Batched DML refresh, delta cleanup, profiles, history | `test/sql/metadata_location.test` |
| Explicit-transaction rejection; atomic same-database schema | `test/sql/metadata_location.test` |
| Crash after intent, inside data transaction, after data commit; full recompute | `test/sql/metadata_location.test` (`openivm_test_fail_point`) |
| Concurrent connections, daemon scheduling | `test/sql/metadata_location.test` |
| Read-only and missing catalogs, reopen discovery, conflicting setting, existing databases, replace, drop, orphan cleanup | `test/sql/metadata_location.test` |
| Legacy discovery unchanged | `test/integration/scheduler_catalog_test.cpp` |
| PostgreSQL metadata: placement, reopen, crash + abandoned lease, DROP, concurrent DuckLake clients without a local frontend file | `test/integration/test_remote_metadata.py` (CI: `.github/workflows/RemoteMetadata.yml`) |

`openivm_test_fail_point` (`after_intent`, `before_data_commit`, `after_data_commit`) is a
testing hook that simulates a process crash at a protocol step; it runs no compensation.

## Compatibility claims

Remote metadata is validated only against the PostgreSQL 16 service container in CI. No
managed PostgreSQL provider, managed DuckLake service or provider-side extension execution
has been validated. Running OpenIVM still requires a client process with the extension
loaded; storing metadata remotely removes only the persistent local frontend file.
