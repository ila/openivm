# Metadata placement

OpenIVM stores its control state in metadata tables: definitions (`openivm_views`),
sources and watermarks (`openivm_delta_tables`), dependencies, refresh hooks, refresh
history, refresh profiles and the compiled-SQL archive (`openivm_compiled_programs`,
`openivm_compiled_statements`). By default these tables live in the `main` schema of each
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
An empty catalog with a schema other than `main` selects that schema in the physical
default database (the one a new connection starts in), regardless of any session's `USE`.

Supported metadata catalogs are attached native DuckDB databases and PostgreSQL databases
attached with the `postgres` extension (`TYPE postgres`). Other catalog types are
rejected; a catalog name alone never enables a new backend.

Code: `src/include/core/metadata_location.hpp` defines `MetadataLocation` and
`MetadataLocator`. Every metadata read and write resolves its location there:
`RefreshMetadata::UseCatalog` selects it on helper connections, and every generated
statement that names a metadata table qualifies it with the resolved catalog **and**
schema. The compiled-SQL archive (#83) follows the same rule: native refreshes write it
in their data transaction, transactional refreshes in the caller's transaction, and
cross-catalog or remote refreshes in one metadata transaction after the refresh committed,
always in the resolved catalog and schema. Its writes avoid `INSERT OR IGNORE`/`REPLACE`
so they also run on PostgreSQL. Further metadata, such as a dedicated internal schema
(#82), should take its catalog and schema from `MetadataLocator::Resolve`, not assume `main`.

## Placement rules

Selecting a metadata location never moves data objects:

| Object | Location |
|---|---|
| Metadata tables | the selected catalog and schema |
| `openivm_data_<key>`, `openivm_visible_<key>`, MV delta tables, aux state | next to the user-facing view (its catalog and schema) |
| `openivm_delta_<source>` | next to the source table, so delta capture stays in the writer's transaction |
| Internal tables of a view in an external catalog that is neither native nor DuckLake | the physical default database, `main` schema (unchanged) |
| `openivm_metadata_location` marker | `main` schema of every native catalog that holds the view or a native source; for a view in an external (for example DuckLake) catalog, the physical default database |

Loading the extension still creates the legacy, empty history, profile and dependency
tables in `main` of the default database, before any setting can apply. With a selected
location they stay empty, and the metadata is written only to that location.

A materialized view cannot be created inside a remote (PostgreSQL) metadata catalog:
CREATE fails with `remote OpenIVM metadata catalog`. Such a view would share its database
with the metadata and bypass the refresh protocol below, which is not validated.

Delta capture never reads metadata: a base-table write is captured whenever the source's
delta table exists, even while the metadata catalog is detached. `DROP TABLE` of a native
table without a delta table (one OpenIVM does not track) does not resolve the metadata
location either, so it also works while the metadata catalog is detached.

## Discovery on reopen

DuckDB does not persist `ATTACH` or `SET`. When a view is created with an explicit
location, OpenIVM writes `openivm_metadata_location(metadata_catalog, metadata_schema)` in
one statement into `main` of the view's catalog and of each native source catalog. A
DuckLake (or other external) view catalog cannot hold the marker, so it goes into the
physical default (frontend) database, the legacy candidate for such views.

Resolution order for a view catalog:

1. `openivm_metadata_catalog` / `openivm_metadata_schema`, if set. A marker in the view's
   catalog that names another location is an error, never a silent split.
2. Otherwise the marker in the legacy candidate catalog (the native view catalog, else the
   default database).
3. Otherwise the legacy location `<candidate>.main`.

After reopening, attach the metadata catalog under the alias recorded in the marker;
no `SET` is needed. If the marker names a catalog that is not attached, operations that
need metadata fail with an error naming the alias. With a persistent frontend file,
DuckLake views are rediscovered the same way, and a new DuckLake view created without
the settings registers in the recorded location instead of splitting the metadata. A
deployment with an in-memory frontend loses its marker on exit and sets the location at
startup.

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

`CREATE MATERIALIZED VIEW`, `PRAGMA refresh`/`refresh_pipeline`, `DROP VIEW`,
`DROP TABLE ... CASCADE` and `ALTER TABLE ... RENAME COLUMN` of a source that views depend
on fail inside `BEGIN ... COMMIT` with `cannot run inside an explicit transaction` when the
metadata location is in another database than the view (for RENAME, than the source). The
caller transaction is left unchanged and must be rolled back. When the metadata schema is
in the view's own database (for example `control.main` views with `control.openivm`
metadata), everything is one database and explicit transactions stay atomic, including
rollback. `ALTER MATERIALIZED VIEW` writes only metadata and is allowed. `ALTER TABLE ...
DROP COLUMN` of a tracked source writes no metadata (it fails if a view references the
column, otherwise it alters only the delta table in the caller transaction) and is
allowed.

`RENAME COLUMN` rewrites the stored view SQL through a helper connection, which commits
before the caller's ALTER. In autocommit mode a failing ALTER restores the snapshotted
metadata rows, one transaction per metadata table: `INSERT OR REPLACE` on native
catalogs, delete by key plus insert on PostgreSQL. If that restore itself fails, OpenIVM prints
`could not restore materialized view metadata` instead of failing silently.

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

DuckLake sources can change under a refresh, because other processes commit to the lake
without the gate. A DuckLake watermark is therefore never the snapshot that is current
when step 3 runs: such a snapshot can contain another client's commit that step 2 never
read, and the next refresh would start after it. Instead:

- Before compiling, the refresh pins the current snapshot S of every attached DuckLake
  catalog and the table id each DuckLake source name denotes at S. Compilation, the
  empty-delta shortcut and every data statement run later, so they read S or a later
  snapshot.
- After step 2 (before the commit when the data runs in one transaction), it checks the
  DuckLake change manifest (`ducklake_snapshots().changes`) for inserts, deletes, ALTER or
  DROP of those table ids in snapshots after S. If the manifest is unavailable it counts
  `ducklake_table_insertions`/`deletions` instead; an unverifiable source counts as
  changed. Only source tables are checked, so the refresh's own commits to its backing,
  delta and aux tables are excluded explicitly.
- No source changed: every read saw exactly the state at S, so step 3 stores S. Changes
  committed after the check have snapshot ids above S and are read by the next refresh.
- A source changed: step 3 does not run. A transactional data step rolls back (under the
  split protocol the marker is then retracted); committed DuckLake data keeps the marker set. The
  refresh then retries immediately, which for a marked view is a full recompute pinned to
  a new S. After three such attempts it fails with `changed while refreshing`, leaving the
  last stored watermarks or the marker in place.

The same pinned watermark and check apply to every DuckLake source, whichever catalog
holds the metadata.

### Remote SQL metadata and concurrent clients

A PostgreSQL metadata schema can be shared by several processes. OpenIVM never scans
DuckDB storage for remote metadata: it reads committed rows through SQL, and explicit
transactions that would write it next to MV data are refused, so committed rows are the
exact state. Remote metadata tables use portable column types, `INSERT OR REPLACE`
becomes delete plus insert (CREATE publishes these rows last, so an interruption leaves no
row and `CREATE OR REPLACE` rebuilds the view), and inserts supply every key column instead
of relying on DuckDB defaults.

Refreshes of one view are serialized across clients by `openivm_refresh_leases`:

- **Server clock.** Every lease time is a PostgreSQL server time. DuckDB evaluates
  `now()` on the client, so OpenIVM reads `clock_timestamp() AT TIME ZONE 'UTC'` and that
  time plus L through `postgres_query` and writes those values; no client wall clock is
  involved. Clients on different hosts compare and extend leases on the same clock, so
  their clock offsets cannot make a live lease look expired.
- A client reads the server time, deletes the view's lease if `lease_until` is before that
  time, and inserts `(view_name, owner, server time + L)`, all in one transaction. The
  primary key makes acquisition atomic. A losing client gets `being refreshed by another
  OpenIVM client`.
- While the refresh runs, a background thread renews the lease every third of
  `openivm_metadata_lease_seconds` (L, default 600, minimum 1), again to server time + L.
  Only a crashed or partitioned client's lease can expire.
- **Local deadline.** A renewal that hangs never reports failure, so the client also keeps
  a monotonic-clock deadline: the send time of its last successful acquisition or renewal
  plus two thirds of L. The server reads its clock for `lease_until` after that send time,
  so measured on the server's clock the lease ends at least L/3 after the deadline. This
  needs only that the client's monotonic clock and the server's clock advance at comparable
  rates; their offsets do not matter. Before the data phase, before every data statement
  (DuckLake statements commit one by one) and before a native data commit, the client
  stops with `stopped before writing more materialized view data` once a renewal reported
  loss or the deadline passed. A healthy holder's deadline always stays at least L/3 ahead.
- **Fenced watermarks.** Step 3 (and retracting the marker after a clean data rollback)
  first runs `UPDATE openivm_refresh_leases SET lease_until = <server time + L> WHERE
  view_name = ... AND owner = <token>` inside the metadata transaction and fails unless
  exactly one row is affected. Writing the row, rather than reading it, orders the commit
  against a takeover: PostgreSQL either makes the takeover's `DELETE` wait for this
  transaction's row lock (after which the extended lease no longer qualifies as expired,
  or the takeover gets a serialization failure, so the takeover fails), or this `UPDATE`
  finds the row gone or concurrently changed and fails (zero rows, or a serialization
  failure). Step 3 also fails if `refresh_epoch` changed. The client's own heartbeat waits
  while a fenced transaction is open, so the two never write the lease row at the same
  time and spuriously fail each other.
- A client whose step 3 fails after committing data bumps `refresh_epoch` and sets the
  marker, so the next refresh recomputes even if another client cleared it meanwhile.
  Hooks and cascaded refreshes reuse the lease of the refresh that started them.
- A crashed client leaves its lease. Other clients wait for it to expire, then take over
  and recover via full recompute.

Remaining window: a takeover can begin only after the lease expired on the server's
clock, which is at least L/3 after this client's deadline. Data written before the
deadline is therefore committed before any takeover, and the new owner recomputes over
it. The assumptions are that the client's monotonic clock and the server's clock advance
at comparable rates, and that the server's clock is not stepped forward by more than L/3
during a refresh (for example by a manual clock change; NTP slews small corrections).
Within those assumptions, only a single data statement that starts before the deadline and
is still running L/3 later can commit after a takeover. If that happens, its step 3 fails; when PostgreSQL is reachable,
the client then marks the view for recomputation as above. When it is not, the error says
`could not mark the view for recomputation`, and the view can stay wrong until it is
repaired with a forced full refresh once the metadata catalog is reachable:

```sql
SET openivm_refresh_mode = 'full';
PRAGMA refresh('<view>');
SET openivm_refresh_mode = 'incremental';
```

Choose L so that L/3 exceeds the longest single refresh statement. The lease serializes
refreshes, not source writers: DuckLake sources changed by other processes during a
refresh are handled by the pinned watermark and source check described above.

## Missing and read-only metadata catalogs

- Configured catalog not attached: operations that need metadata fail with
  `is not attached`. Base-table writes are still captured. Reattach, then refresh.
- Read-only catalog: reading MVs works; CREATE, REFRESH, ALTER and DROP fail with
  `attached read-only` before writing anything.
- Missing schema: created on first use when the catalog is writable.

## Replacement and drop

`CREATE OR REPLACE` keeps exactly one metadata row in the selected location. When that
location is in another database than the view, CREATE runs as a staged program in
autocommit mode, and the replacement removes only the old view's catalog objects; the
source delta tables it keeps reading stay in place. A staged replacement that fails after
removing the old objects runs CREATE's cleanup, which drops the view together with its
metadata rather than leaving a half-built view; create it again. DROP removes
data objects before metadata rows. A DROP interrupted between the two leaves only metadata,
never an unmaintained view; `DROP VIEW IF EXISTS <name>` removes the leftover metadata.
CREATE writes metadata last; a CREATE interrupted earlier leaves objects that
`CREATE OR REPLACE` cleans up.

## Tests

| Scenario | Test |
|---|---|
| Placement of metadata vs backing/delta tables, marker | `test/sql/metadata_location.test` |
| Batched DML refresh, delta cleanup, profiles, history | `test/sql/metadata_location.test` |
| Explicit-transaction rejection (including RENAME COLUMN); atomic same-database schema | `test/sql/metadata_location.test` |
| RENAME COLUMN restore after a failed ALTER; autocommit rename | `test/sql/metadata_location.test` |
| Crash after intent, inside data transaction, after data commit; full recompute | `test/sql/metadata_location.test` (`openivm_test_fail_point`) |
| Concurrent connections, daemon scheduling | `test/sql/metadata_location.test` |
| Read-only and missing catalogs, untracked DROP TABLE while detached, reopen discovery, conflicting setting, existing databases, replace, drop, orphan cleanup | `test/sql/metadata_location.test` |
| DuckLake views: frontend marker, reopen without settings, crash after the DuckLake data commit | `test/sql/metadata_location_ducklake.test` |
| Legacy discovery unchanged; scheduler discovery of a configured location | `test/integration/scheduler_catalog_test.cpp` |
| PostgreSQL metadata: placement, reopen, crash + abandoned lease, RENAME rejection and portable restore, CREATE inside the metadata catalog rejected, DROP, concurrent DuckLake clients without a local frontend file | `test/integration/test_remote_metadata.py` (CI: `.github/workflows/RemoteMetadata.yml`) |
| Lease lost before the data phase, after the first DuckLake statement, and taken over before the watermark commit (fence) | `test/integration/test_remote_metadata.py` |
| Lease times written from the PostgreSQL server clock; expiry judged on that clock (live 120 s ahead refuses, 1 s past takes over) | `test/integration/test_remote_metadata.py` |
| Rejected CREATE in an explicit transaction creates no metadata schema; ADD COLUMN of a tracked source while the metadata catalog is detached | `test/sql/metadata_location.test` |
| Settings contradicting the frontend marker of a DuckLake view | `test/sql/metadata_location_ducklake.test` |
| DuckLake source committed by another connection after the data statements, with metadata in `control` | `test/sql/metadata_location_ducklake.test` (`concurrent_commit_after_data`) |
| DuckLake source committed before or after the data statements (aggregate and join views); retry count; unrelated tables cause no retry | `test/sql/ducklake_concurrent_source_commit.test` |
| PostgreSQL metadata: injected concurrent commit, and a writer process racing two refreshing processes | `test/integration/test_remote_metadata.py` |

`openivm_test_fail_point` is a testing hook. `after_intent`, `before_data_commit` and
`after_data_commit` simulate a process crash at a protocol step and run no compensation.
`concurrent_commit_before_data` and `concurrent_commit_after_data` commit the SQL in
`openivm_test_concurrent_sql` from another connection during the first refresh attempt,
after compilation or after the data statements, as a concurrent client would.
`lease_lost_before_data` and `lease_lost_mid_data` hand the lease to another (already
expired) owner and make the client observe the loss, as its deadline would;
`lease_lost_before_watermark` hands it over without local notice, so only the fence can
stop the watermark commit. The concurrent row-lock ordering between a fence and a
takeover relies on PostgreSQL semantics and is not reproduced deterministically. CI runs
every client on one host, so clock offsets between hosts are not reproduced either; the
test checks that lease times are server times instead.

## Compatibility claims

The remote metadata integration test runs only against the PostgreSQL 16 service
container in CI (`.github/workflows/RemoteMetadata.yml`); without `OPENIVM_POSTGRES_DSN`,
`make test` skips it. No managed PostgreSQL provider, managed DuckLake service or
provider-side extension execution has been tested. Running OpenIVM still requires a client process with the extension
loaded; storing metadata remotely removes only the persistent local frontend file.
