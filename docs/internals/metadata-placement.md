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
SET openivm_metadata_schema = 'openivm';   -- created on first use; '' retains the existing schema
```

Placement is a property of the database, not of a session: two sessions using different
locations would split one database's views. Plain `SET` therefore applies globally,
`SET SESSION` is rejected, and helper connections and the refresh daemon see the value.
With an empty catalog setting, the schema setting applies in each native view's catalog,
or in the default frontend database for DuckLake views. An empty schema setting retains
the discovered location and uses `main` for new metadata.

Supported metadata catalogs are attached native DuckDB databases and PostgreSQL databases
attached with the `postgres` extension (`TYPE postgres`). Other catalog types are
rejected; a catalog name alone never enables a new backend.

Code: `src/include/core/metadata_location.hpp` defines `MetadataLocation` and
`MetadataLocator`. Every metadata read and write resolves its location there:
`RefreshMetadata::UseCatalog` selects it on helper connections, and every generated
statement that names a metadata table qualifies it with the resolved catalog **and**
schema. When enabled with `SET openivm_archive_compiled_sql = true`, the compiled-SQL
archive (#83) follows the same rule: native refreshes write it
in their data transaction, transactional refreshes in the caller's transaction, and
cross-catalog or remote refreshes in one metadata transaction after the refresh committed,
always in the resolved catalog and schema. Its writes avoid `INSERT OR IGNORE`/`REPLACE`
so they also run on PostgreSQL. `openivm_compiled_sql_retention` bounds distinct versions
per view and operation (default 10); pruning program and statement rows shares the
archive transaction. See [archive controls and cleanup](../build/building.md#archived-compiled-sql).
Control metadata resolves through `MetadataLocator`; physical maintenance objects follow
their source or view catalog and the `openivm_internal_schema` setting below.

## Placement rules

Selecting a metadata location never moves data objects:

| Object | Location |
|---|---|
| Metadata tables | the selected catalog and schema |
| `openivm_data_<key>`, `openivm_visible_<key>`, MV delta tables, aux state | the view's catalog; its schema or `openivm_internal_schema` |
| `openivm_delta_<source>` | the source's catalog; its schema or `openivm_internal_schema`, so capture stays in the writer's transaction |
| Internal tables of a view in an external catalog that is neither native nor DuckLake | the physical default database, `main` schema (unchanged) |
| `openivm_source_generations` FK incarnation tokens | `main` in each native source catalog; invalidated locally on DROP/REPLACE |
| `openivm_source_deltas` routing table | `main` in each native source catalog; stays local when control metadata moves |
| `openivm_metadata_location` marker | For a separate metadata catalog, `main` in native view/source catalogs and the frontend database for external views; no marker is needed inside the metadata catalog itself |

Loading the extension still creates the legacy, empty history, profile and dependency
tables in `main` of the default database, before any setting can apply. With a selected
location they stay empty, and the metadata is written only to that location.

A materialized view cannot be created inside a remote (PostgreSQL) metadata catalog:
CREATE fails with `remote OpenIVM metadata catalog`. Such a view would share its database
with the metadata and bypass the refresh protocol below, which is not validated.

Native delta capture reads the local `main.openivm_source_deltas` routing table, including
uncommitted mappings in the writer's transaction. The normal write path does not contact
the control metadata catalog, so writes and `ADD COLUMN` keep working while that catalog
is detached. If a delta disappears while a writer waits, ownership validation distinguishes
a failed allocation from a retired published delta; the latter can require a transaction retry. CREATE
validates ownership against control metadata; final-consumer cleanup removes local routing
with the source delta. A non-cascading source DROP retains the mapping while its delta
and consumers remain, so a same-named replacement source keeps capturing changes. `DROP TABLE` of a native
table without a delta table (one OpenIVM does not track) does not resolve the metadata
location either, so it also works while the metadata catalog is detached.

## Discovery on reopen

DuckDB does not persist `ATTACH` or `SET`. When metadata lives in another catalog,
OpenIVM writes `openivm_metadata_location(metadata_catalog, metadata_schema)` in one
statement into `main` of the native view and source catalogs. For external views, the
frontend database holds the marker when it differs from the metadata catalog. Native
metadata in the view's own catalog is discovered by schema; it needs no forwarding marker.
In particular, a native view reading another native database does not redirect that
source database's own metadata placement.

Resolution order for a view catalog:

1. `openivm_metadata_catalog` / `openivm_metadata_schema`, if set. A marker in the view's
   catalog that names another **catalog** is an error, never a silent split. A schema
   change within the same catalog uses the atomic migration described below.
2. Otherwise the marker in the legacy candidate catalog (the native view catalog, else the
   default database).
3. Otherwise discover the existing native metadata schema, or use `<candidate>.main` for new metadata.

After reopening, attach the metadata catalog under the alias recorded in the marker;
no `SET` is needed. If the marker names a catalog that is not attached, operations that
need metadata fail with an error naming the alias. With a persistent frontend file,
DuckLake views are rediscovered the same way, and a new DuckLake view created without
the settings registers in the recorded location instead of splitting the metadata. A
deployment with an in-memory frontend loses its marker on exit and sets the location at
startup.

Native registry schema discovery reuses an evictable inventory keyed by catalog identity
and the transaction-visible catalog version. Inventories for multiple live snapshots
coexist within the bounded cache. CREATE, DROP, rollback, and detach/reattach select the
matching inventory. Only schema names are cached; source ownership
rows are still read from the caller’s snapshot, and remote inventories are not cached.

Delta cleanup and DROP consult every location that may consume a delta table: the
configured location, marker targets, and legacy native catalogs. An unreachable marker
target aborts rather than risk deleting changes another view still needs.

## Existing databases

Changing `openivm_metadata_schema` moves existing control metadata within the same
catalog on the next CREATE or refresh. The move is atomic: definitions, dependencies,
watermarks, history, profiles and compiled SQL archives move together, or the old
placement remains intact. Internal backing and delta tables keep their locations.
A PostgreSQL move leaves an `openivm_metadata_redirect` locator in the old schema,
committed with the move, so frontend markers still resolve after reopening.

The move runs in autocommit mode. An explicit transaction keeps its existing metadata
location until commit or rollback. New metadata uses `main` when the schema setting is
empty; existing metadata is discovered in its current schema.

Changing the metadata **catalog** still requires an explicit migration. Select the
catalog before creating views; OpenIVM rejects a conflicting catalog ownership marker.

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
2. **Data.** One data transaction in the view's own database (native catalog or DuckLake)
   applies the MV changes, including the view's own delta rows.
3. **Watermarks.** One metadata transaction advances every source watermark and clears
   `refresh_in_progress`.
4. **Cleanup.** Consumed source-delta rows are deleted after step 3. This only removes rows
   every consumer's committed watermark has passed, so a failure here retains rows that
   later refreshes ignore. Step 2 only reads source delta tables, so it writes one
   database even when a source is stored in the metadata catalog (for example a host view
   over `control.main.<source>` with `control.openivm` metadata) or a DuckLake view reads
   a native source.

Crash and retry semantics:

| Failure | State left | Next refresh |
|---|---|---|
| Before step 1 commits | unchanged | normal incremental refresh |
| After 1, before 2 commits | marker set, data unchanged | full recompute |
| Step 2 fails cleanly (rolled back) | marker retracted, unless another client set it (lease lost or `refresh_epoch` changed) | normal incremental refresh, or full recompute if the marker stayed |
| Crash inside 2 | marker set, data rolled back | full recompute |
| After 2, before 3 commits | marker set, data new, watermarks old | full recompute |
| After 3 | consistent | normal |

A full recompute derives the MV from current base data, so it neither loses changes nor
applies them twice, whatever happened before. Downstream views see each change exactly
once:

- A native view's downstream views read its delta table. Recovery emits the diff between
  the committed backing table and the recomputed result. Step 2 commits the delta rows
  together with the backing table, so an interrupted step 2 leaves neither and a committed
  one leaves both; the diff therefore holds only changes not yet emitted.
- A DuckLake view's downstream views read the snapshot changes of its backing table, so
  they see exactly the committed backing-table states, and the recompute's own changes are
  the remaining difference. Step 2 is one lake transaction, so no partial backing-table
  state is ever committed. The view's own delta table only feeds its upsert. Its rows are
  read only when they are newer than the last refresh, and the next incremental refresh
  deletes them, so a delta row left by an interrupted refresh outside this protocol is
  never applied.

A non-native view with downstream views that read its delta table also runs its data
statements in one transaction outside this protocol, and likewise deletes consumed
native source-delta rows only after its watermarks commit. Full recomputes take
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
- No source changed: every read saw exactly the state at S. Step 3 stores the snapshot T
  through which the check ran (S or later); no source changed in between, so T and S denote
  the same source state, and T skips the refresh's own data commits. Changes committed after
  the check have snapshot ids above T and are read by the next refresh. When the data runs
  in one transaction, a source commit that lands after the check but before step 3 is
  therefore applied by the next refresh, not by this one.
- A source changed: step 3 does not run. A transactional data step rolls back (under the
  split protocol the marker is then retracted). Outside the split protocol, a DuckLake view
  commits its data statements one by one; its committed data keeps the marker set. The
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

Refreshes and materialized-view lifecycle changes coordinate across clients through
`openivm_refresh_leases`. CREATE holds the target name before publishing metadata;
REPLACE and cascading DROP acquire the affected view leases in sorted order and recheck
dependencies before changing objects. CREATE also leases its sources while registering
dependencies, so a concurrent source drop cannot miss the new consumer. Conflicts fail
promptly; retry the operation after the competing operation finishes. These leases are
held through staged execution and failure cleanup, then released when the statement ends.
Cleanup confirms ownership with PostgreSQL before deleting objects. If ownership is lost
or cannot be confirmed, it retains staged objects for recovery with CREATE OR REPLACE.

The lease protocol is:

- **Server clock.** Every lease time is a PostgreSQL server time. DuckDB evaluates
  `now()` on the client, so OpenIVM reads `clock_timestamp() AT TIME ZONE 'UTC'` and that
  time plus L through `postgres_query` and writes those values; no client wall clock is
  involved. Clients on different hosts compare and extend leases on the same clock, so
  their clock offsets cannot make a live lease look expired.
- A client reads the server time, deletes the view's lease if `lease_until` is before that
  time, and inserts `(view_name, owner, server time + L)`, all in one transaction. The
  primary key makes acquisition atomic. A losing client gets `locked by another
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
  and before the data commit, the client stops with `stopped before writing more
  materialized view data` once a renewal reported loss or the deadline passed; the data
  transaction then rolls back. A healthy holder's deadline always stays at least L/3 ahead.
- **Fenced watermarks.** Step 3 (and retracting the marker after a clean data rollback)
  first runs `UPDATE openivm_refresh_leases SET lease_until = <server time + L> WHERE
  view_name = ... AND owner = <token>` inside the metadata transaction and fails unless
  exactly one row is affected. Writing the row, rather than reading it, orders the commit
  against a takeover: PostgreSQL either makes the takeover's `DELETE` wait for this
  transaction's row lock (after which the extended lease no longer qualifies as expired,
  or the takeover gets a serialization failure, so the takeover fails), or this `UPDATE`
  finds the row gone or concurrently changed and fails (zero rows, or a serialization
  failure). Step 3 also fails if `refresh_epoch` changed, and the retraction then matches no
  row, leaving the marker set. The client's own heartbeat waits
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
Within those assumptions, only a data commit that starts before the deadline and is still
running L/3 later can commit after a takeover. If that happens, its step 3 fails; when PostgreSQL is reachable,
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

The feature suite shares scenarios across backends instead of keeping a separate
reproducer for every integration fix. Operator tests elsewhere in `test/sql/`
continue to cover incremental query semantics.

| Contract | Tests |
|---|---|
| Native/DuckLake metadata and maintenance placement, qualified names, profiles, same-named join sources and DAG refresh | `test/sql/metadata_location.test`, `test/sql/internal_schema.test`, `test/sql/internal_schema_ducklake.test` |
| Explicit cross-database transaction rejection, same-database commit/rollback, missing/read-only controllers, offline writes and reopen markers | `test/sql/metadata_location.test` |
| Schema migration, initialization, rollback, shared ownership, fresh-configuration reopen and CREATE/DML/DROP races | `test/integration/source_delta_publication_test.cpp` |
| Crash before/after data commit, partial data writes, clean failure, chained recovery and native-source cleanup | `test/sql/metadata_location.test`, `test/sql/ducklake_chained_recovery.test` |
| Source changes committed during refresh, snapshot watermarks and retry | `test/sql/ducklake_concurrent_source_commit.test` |
| Ordered SQL archive, controls, retention, failure, commit/rollback and migration | `test/sql/metadata.test`, `test/sql/metadata_schema.test` |
| Scheduler discovery and schema synchronization with an unrelated locked catalog | `test/integration/scheduler_catalog_test.cpp` |
| PostgreSQL placement, portability, separate-process races, server-clock leases, takeover/fencing and crash recovery | `test/integration/test_remote_metadata.py` (`.github/workflows/RemoteMetadata.yml`) |

`openivm_test_fail_point` is a testing hook. `after_intent`, `before_data_commit` and
`after_data_commit` simulate a process crash at a protocol step and run no compensation.
`after_view_delta` crashes right after the statement that writes the view's own delta
rows for downstream views, before the backing table changes. `data_statement_error` makes
the data step fail with an ordinary error after its first statement, so the normal failure
handling (rollback, then retracting the marker) runs.
`concurrent_commit_before_data` and `concurrent_commit_after_data` commit the SQL in
`openivm_test_concurrent_sql` from another connection during the first refresh attempt,
after compilation or after the data statements, as a concurrent client would.
`concurrent_commit_then_data_error` commits it after the interruption marker and then
fails the data step like `data_statement_error`.
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

## Internal maintenance objects

Internal objects never move with the metadata schema, and never cross catalogs.
`openivm_internal_schema` selects a dedicated schema for them inside each data
catalog:

```sql
SET openivm_internal_schema = 'openivm_internal';
CREATE MATERIALIZED VIEW sales.product_totals AS
    SELECT product, SUM(amount) AS total FROM sales.orders GROUP BY product;
-- sales.product_totals stays in schema sales; its tables go to openivm_internal.
```

| Object | Unset (default) | `openivm_internal_schema = 'I'` |
|---|---|---|
| User-facing materialized view | The requested catalog and schema | Unchanged |
| Backing table `openivm_data_<key>` | The MV's catalog and schema | `I` in the MV's catalog |
| Publication table `openivm_visible_<key>` and its delta | The MV's catalog and schema | `I` in the MV's catalog |
| MV delta table `openivm_delta_<key>` | The MV's catalog and schema | `I` in the MV's catalog |
| Auxiliary state (DISTINCT counts, semi/anti match counts) | The MV's catalog and schema | `I` in the MV's catalog |
| Unique index on grouped backing tables | The backing table | The backing table |
| Native source delta | `openivm_delta_<table>` beside the source | `openivm_delta_<hex(schema)>_<table>` in `I` of the source's catalog |
| DuckLake source changes | DuckLake snapshots; no delta table | Unchanged |

For a DuckLake MV, `I` is a schema of the DuckLake catalog, created in the lake's
own transaction. Native objects are created in the native catalog's transaction,
so an explicit transaction's `ROLLBACK` removes them and a newly created `I`.

`<key>` is an internal view key derived from the view's catalog, schema and name,
so same-named views in different schemas or catalogs have separate internal
objects. A source delta in an internal schema encodes its source schema in hex,
so same-named sources of different schemas have different preferred names.
When a preferred name is occupied (including by a legacy colocated delta),
OpenIVM allocates a free name with an `__openivm_<number>` suffix. It checks source
ownership before reusing an existing delta rather than adopting a table by name.
CREATE holds the database mutation gate through ownership publication and commit.
Native writers first take shared admission for their qualified source. If no
owned delta exists, admission stays with the writer until commit or rollback;
independent untracked writers can run and commit together. CREATE takes exclusive
publication admission for its sources before allocating deltas. A writer arriving
between allocation and publication therefore waits, then reconciles ownership
against both its own snapshot and a fresh committed read before choosing capture.
Tracked writers release their temporary source admission before taking the
existing mutation gate, then reserve the source again and recheck ownership under
that gate. Both writer paths retain their source admission through commit, so a
later final-consumer DROP cannot hide intervening source writes from old snapshots.

Autocommit CREATE discovers sources through a read-only bind in the caller
context, including its temporary tables and search path. It retains those source
guards while ending the discovery snapshot and starting the transaction that
compiles and executes creation. No lifecycle writes run in the discovery phase.
This prevents initial loading from an older snapshot after waiting for an
uncaptured writer. Lake-only discovery also ends its read snapshot before shared
native metadata initialization, even though it requires no native source guards.
The staged executor retains the same source guards while
suspending the planning transaction, through publication or allocation cleanup.
An explicit transaction keeps its snapshot: first-source capture requires a retry
if an admitted writer committed changes to that qualified source after
the snapshot and there is no existing delta to cover them. Admission records a
source commit marker only after an executed DML plan successfully commits;
planning-only admission from plain `EXPLAIN` or `PREPARE`, rollback, read-only
metadata probes and commits to other sources do not mark that source. The native
DML input pipeline marks execution, so `EXPLAIN ANALYZE` and executed prepared
statements still record commits. Cached prepared plans reacquire source admission
and verify their capture target before executing in a later transaction. The marker
uses the native manager's latest commit ID as a conservative upper bound, because the
writer's transaction object may already be reclaimed before the commit callback.
It remains protected by admission until recorded. Admission release discards idle
markers when no native snapshot can predate them; a new attachment with the same
catalog name has a distinct catalog identity and does not inherit the old markers.
Transactions that already hold untracked-write admission also receive a retry
error if a later statement would wait on a conflicting lifecycle gate, avoiding
a cycle between publication and pending writes.

In autocommit, shared metadata initialization inspects and updates the helper's
catalog snapshot. The native target's internal schema is also initialized before
source planning opens the caller's snapshot. Newly allocated, empty source deltas
use the creation watermark without a caller-side scan of the helper's newer table;
existing deltas retain the caller-visible watermark calculation. Explicit
transactions keep schema creation and metadata upgrades in the caller transaction.
Before allocating a source delta, it reconciles transaction-local ownership with
a fresh committed read, so an explicit transaction with an older metadata snapshot
reuses the shared delta. If that identity conflicts with its local ownership or is
not visible in its catalog snapshot, the transaction must be retried rather than
allocating a second delta.
Successful source-delta drops are recorded for the caller transaction. A later
replacement or DROP followed by CREATE can therefore allocate a new delta (even
in another internal schema) without mistaking its own cleanup for a stale
snapshot. Commit and rollback both clear that record; concurrent allocation
still requires reconciliation with committed ownership.
DROP's transaction-local metadata replay retains complete ownership, including
committed siblings outside the affected view's dependency closure. Cleanup checks
this replayed state before deleting a shared source delta, so transaction-local
ownership additions and removals are considered alongside committed consumers.
Replay retains the fully qualified metadata targets of CREATE, replacement,
view DROP, and source-table CASCADE. A helper redirects only mutations for its
selected metadata catalog and schema to temporary shadows, rebuilding those
shadows when it inspects another location. Qualified CASCADE deletions therefore
remain visible to a later final-consumer DROP in the same transaction.
Delta ownership probes explicitly name the selected metadata location or its
temporary replay tables. Source planning may change a helper's search path without
redirecting replacement, DROP, or commit-time consumer checks to a user schema.
Ordinary table DROP binds through the native planner after earlier batch statements
execute. Scratch tables are therefore resolved in the temporary catalog, rather
than assigned the control metadata schema before their CREATE has executed.
The physical DROP executor uses native DROP binding for the same identity and
transaction-write checks. Plan serialization retains that resolved DROP identity,
including temporary targets, without repeating binding during deserialization.
CREATE/ALTER lifecycle statements also bind at their
native statement position, after preceding source DDL or BEGIN has executed.
Their generated SQL binds and executes sequentially on the caller's active
executor and transaction, so a step can use an object created by its predecessor
and a later CREATE sees uncommitted DROP. A failed step invalidates the lifecycle's
transaction, preventing partial writes from being committed. Native query profiling
reports the enclosing lifecycle operator's time; dynamically compiled steps do not
add operators to DuckDB's already initialized profiling tree.
Source watermarks are recorded as evaluated timestamp values when the caller
executes CREATE or replacement. Replay uses those values without reading physical
delta tables that may still be invisible outside the caller transaction.
`openivm_delta_tables` records each source's catalog and schema, the physical delta
name (`delta_table_name`), and, for moved deltas, the internal schema (`delta_schema`).
Older rows without a stored physical name retain their original naming rule.
`openivm_views.internal_schema` records where a view's internal tables live.
Metadata identity comparisons follow DuckDB's ASCII-only identifier case folding:
`items` and `ITEMS` identify the same table, while quoted schemas `"Ä"` and `"ä"`
remain distinct. Unicode string case folding and application collations must not
combine their ownership or CASCADE cleanup.

Stored original-SQL definitions qualify each source using the CREATE caller's
catalog lookup and search path. This includes public parent-MV names, which differ
from their physical publication tables, and individual same-named references in
different schemas. Refresh and reopen therefore retain the original source
identities even when the active schema is the native metadata schema. Legacy
unqualified definitions use persisted source locations; if a bare name identifies
multiple source locations, refresh reports the ambiguity and requests
`CREATE OR REPLACE` with qualified sources instead of choosing one silently.

**Existing objects.** The setting applies to objects created while it is set.
OpenIVM finds existing tables where they are, through `openivm_views.internal_schema`
and `openivm_delta_tables.delta_schema` (empty means beside the view or source), so
nothing has to be moved by hand:

- A source delta is shared by every view over that source. A new view reuses the
  existing delta wherever it lives; one source never gets a second delta.
- `CREATE OR REPLACE` keeps the replaced view's internal location, so views built
  on it keep reading the same tables. Drop and recreate a view to move it.
- Source-delta allocation and ownership publication share the mutation gate, so
  concurrent CREATEs cannot allocate separate deltas for the same source.
- DROP acquires that gate before resolving ownership or checking remaining
  consumers and retains it through cleanup and commit, including staged cleanup.
  A concurrent CREATE's newly published ownership therefore protects its delta.
- View, source-table and schema CASCADE follow qualified physical dependencies
  across every metadata catalog and remove descendants before their parents.
  Source-delta cleanup excludes removed consumers by their metadata table and
  view key, preserving unrelated consumers in other registries. Autocommit may
  stage cross-catalog cleanup; explicit transactions retain native catalog-write
  checks and rollback, rather than committing cleanup through a helper.
- Replacement removes deltas of sources no longer used when no view in any
  metadata catalog consumes them. Native cleanup in the caller's catalog rolls
  back with replacement. Cleanup in another native catalog runs after commit,
  re-checks committed ownership, and is discarded on rollback.
- Deltas of OpenIVM's own tables (a parent MV's publication) stay beside them and
  belong to that MV until it is dropped, even after its last child is dropped or
  replaced. Cleanup checks owners and consumers across all metadata locations,
  including transaction-local metadata replay.
- Publication emits signed deltas for consumers in every metadata catalog, using
  the same qualified source identity and consumer watermarks as cleanup. A slower
  consumer keeps its pending deltas when another consumer refreshes. Replacement
  also discovers consumers across catalogs before preserving publication state.
  Generated internal targets always include their catalog and schema, including
  for unqualified MVs created in the caller's control-metadata schema.

An MV in an external catalog other than DuckLake keeps its internal objects in the
`main` schema of the native frontend database, never in the metadata schema.

Inspect actual locations with:

```sql
SELECT database_name, schema_name, table_name
FROM duckdb_tables()
WHERE starts_with(table_name, 'openivm_')
ORDER BY database_name, schema_name, table_name;
```
