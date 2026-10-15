# Metadata and internal-object placement

OpenIVM keeps two kinds of objects. Control metadata describes views, their
sources, watermarks and refresh history. Internal maintenance objects hold the
data that refresh reads and writes. They follow separate placement rules.

## Control metadata

The control-metadata tables are `openivm_views`, `openivm_delta_tables`,
`openivm_refresh_hooks`, `openivm_refresh_history`, `openivm_refresh_profile`,
`openivm_mv_dependencies`, `openivm_match_log` and `openivm_constraints_cache`.

**Catalog.** A view in a native DuckDB catalog keeps its metadata in that catalog.
A view in an external catalog, such as DuckLake, keeps its metadata in the native
frontend database. DuckDB cannot commit one transaction across a native catalog
and DuckLake, so OpenIVM commits native metadata separately from DuckLake data.

**Schema.** Within that catalog, `openivm_metadata_schema` selects the schema:

```sql
SET openivm_metadata_schema = 'openivm';
CREATE MATERIALIZED VIEW product_sales AS
    SELECT product_name, SUM(amount) AS total FROM orders GROUP BY product_name;

SELECT view_sql_name, view_catalog, view_schema FROM openivm.openivm_views;
```

| Setting value | Behavior |
|---|---|
| empty (default) | Each catalog keeps its metadata in the schema that already holds it. New metadata uses `main`. |
| a schema name | New metadata is created in that schema. Existing metadata in another schema is moved there automatically. |

The setting is not stored in the database. After reopening, OpenIVM finds the
schema that holds `openivm_views` in each catalog, so later sessions and the
refresh daemon need no setting. If two schemas of one catalog contain
`openivm_views`, OpenIVM reports an error; set `openivm_metadata_schema` to the
schema to use.

### Moving existing metadata

Do not move OpenIVM tables by hand. When `openivm_metadata_schema` names a schema
that does not yet hold a catalog's metadata, the next `CREATE MATERIALIZED VIEW`
or refresh in that catalog moves all control-metadata tables into it in one
transaction. Metadata tables that `main` holds outside a view registry, such as the
constraint cache created at `LOAD` or before a first view created inside a
transaction, move with them, including declared RELY foreign keys. Until then,
OpenIVM reads the metadata where it is.

The move runs outside explicit transactions. Inside `BEGIN ... COMMIT`, OpenIVM
keeps using the existing metadata schema. If a catalog has no metadata yet, the
first materialized view in the transaction creates the configured schema, and
`ROLLBACK` removes it with the view.
Changing or clearing the setting during that transaction does not change its
metadata location: CREATE, ALTER, replacement and DROP keep using the registry
already visible to the caller or recorded by its earlier lifecycle statements.
Commit and rollback discard that transaction's recorded locations.

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
