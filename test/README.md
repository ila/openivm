# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

The native `source_delta_publication_test` integration executable pauses CREATE
after source-delta allocation and before ownership publication, then checks a
competing connection in both autocommit and an explicit transaction whose metadata
snapshot predates ownership publication, shared placement, transaction-local
ownership reuse, and refresh bag equality. `make test` builds and runs it through
`test_release_internal`; `make test_source_delta_publication` runs it on its own.
Together with `test/sql/internal_schema.test` and
`test/sql/internal_schema_ducklake.test`, it covers namespace lifecycle regressions.
The native fixture also pauses first-view publication and synchronizes on actual
source-admission waits for competing INSERT, DELETE and UPDATE batches. Colocated
and dedicated schemas cover autocommit, an older explicit metadata snapshot,
writer rollback, failed publication and retry of an invisible delta. The reverse
ordering holds uncaptured source DML open before CREATE, so its initial load must
use the snapshot established after that writer commits or rolls back.
Independent untracked writers must commit while another transaction remains open,
and an independent source must remain writable while CREATE retains publication
admission. Nonconflicting append/update/delete batches on one untracked source
also retain native writer concurrency. Equal bare names in different quoted schemas and catalogs exercise
admission scope. Lock-order conflicts and first CREATE from a stale source
snapshot require an explicit transaction retry; rollback must leave no partial
writes. Each surviving MV captures another conflicting batch and is checked with
bidirectional `EXCEPT ALL` after refresh.
The DuckLake fixture also verifies snapshot-backed auxiliary refresh and repair
for COUNT(DISTINCT), inner DISTINCT and filtered-group counts: repeated refreshes
must not replay a consumed snapshot, unrelated lake snapshots permit repair, and
pending source changes prevent rebuilding missing state.
First-MV creation from lake-only sources checks dedicated native metadata and
internal schemas. FULL OUTER projection and grouped refreshes read qualified
lake snapshot changes for same-named sources, including an unchanged source and
repeated no-change refreshes, with both native and lake targets. The short-name
capture fixture explicitly registers its source delta and verifies that an
unregistered reserved namesake receives no writes.
Snapshot publications in quoted internal schemas check creation, chained reads,
reopen and refresh. The lifecycle executable also checks reference matching with
quoted dots and escaped quotes: catalog/schema components must remain distinct
when pinning maintenance scans, and existing pins must be replaced exactly once.
Native schema-drop cases verify pending-insert cleanup across repeated schema/table
name reuse, commit and rollback, through both SQL DROP and its physical executor. A
same-named source in another schema keeps its pending data and refreshes with full
bag equality. The larger schema-cascade matrix retains ownership, auxiliary-state
cleanup and native/DuckLake placement checks. Its COUNT(DISTINCT) fixture uses
distinct group/value column names, requires incremental classification, and
checks the auxiliary column identity and full tuple-count bag at creation,
after rollback and after the surviving view refreshes.
The Python integration wrapper supplies a temporary database path and removes it
after execution. After building the executable, a focused diagnostic uses the
same wrapper as the normal test target:

```bash
python3 test/integration/test_source_delta_publication.py ./build/release/extension/openivm/source_delta_publication_test
```

Direct executable invocations require a fresh database path as their sole argument;
invoking it without arguments prints usage and does not run the lifecycle cases.
The executable closes and reopens that database with a fresh
configuration, verifies that placement settings were not persisted, and checks
metadata discovery, source-delta reuse, replacement, refresh bag equality and
final-consumer cleanup with a non-default search path. SQLLogicTest's `restart`
copies extension settings, so its unset-placement case explicitly clears the
setting before restart and retains its physical-placement assertions.
DuckLake fixtures select a persistent native schema before `restart`: SQLLogicTest
restores the saved search path before the fixture can reattach the lake catalog.
It also checks source-delta lookup with separate control, source and maintenance
schemas, including same-named sources and a missing owner. Catalog enumeration
must release its schema lock before resolving table entries. Batched conflicting
writes must still refresh both views to bidirectional bag equality.
Metadata-upgrade cases cover the first CREATE over a legacy registry in autocommit,
explicit commit, and rollback followed by retry. They check rollback of the new
schema and metadata column, refresh bag equality, and final-consumer cleanup.
Scratch-table and scratch-view cleanup also checks native DROP and the physical DROP executor
after CREATE in one batch, with a persistent namesake, quoted names, a separate
control schema, autocommit, explicit commit and rollback. Serializer verification
is enabled for both DROP executor functions, including absent `IF EXISTS` targets;
deserialization must preserve the native binder's resolved temporary identity.
Each case then refreshes
the surviving MV after conflicting batched writes and checks both EXCEPT ALL directions.

It also pauses a CREATE that reuses a committed view's source delta, waits until a
competing DROP actually blocks on the mutation gate, then publishes the new view
before allowing DROP to proceed. Native autocommit, explicit-transaction, and
cross-catalog staged cleanup must preserve the surviving delta, capture batched
INSERT/DELETE/UPDATE operations, and refresh to full bag equality in both directions.
The reverse ordering pins a read-only transaction's metadata and source snapshots,
then commits the final owner's DROP on another connection. CREATE must require a
transaction retry instead of adopting the removed delta, including when another
committed CREATE has reused the exact physical name. The matrix covers colocated
and dedicated internal schemas, separate source/metadata catalogs, quoted names
and a non-default search path. After rollback and retry, every surviving MV must
capture conflicting batched writes and refresh to bidirectional bag equality;
dropping the final owner must reclaim the delta.

`internal_schema.test` also replaces and drops one of two committed, independent
views sharing a source within an explicit transaction. With separate metadata and
maintenance schemas, both commit and rollback must preserve the sibling's delta
and capture batched conflicting writes. Dropping the final owners in one
transaction must then remove that delta.

The lifecycle executable also checks repeated replacement and DROP/recreate before
source CASCADE, with commit and rollback. Transaction-local metadata replay must
preserve the caller's exact source watermark, including nonempty shared deltas and
new physical deltas that helper connections cannot yet see.

The namespace lifecycle fixtures also cover a qualified source-table CASCADE
followed by final-consumer DROP or replacement in the same transaction. Commit
must remove the physical source delta; rollback and independent siblings must
retain ownership and refresh correctly after conflicting batches. The native
integration executable reuses one replay helper across two metadata catalogs and
back, checking that CREATE/CASCADE mutations and temporary shadows retain their
catalog/schema identities.

The same native executable places an unrelated metadata catalog after the source
catalog in enumeration order. It checks transaction-local CREATE, DROP,
replacement, and DROP/recreate before source CASCADE, with both commit and
rollback, plus every mutation in autocommit with direct and batched CASCADE.
Ownership probes also move a helper to another quoted catalog/schema after
selecting metadata, checking both durable rows and an uncommitted replay sibling
without changing the helper's search path.
Each surviving MV receives conflicting batched INSERT/DELETE/UPDATE
and a bidirectional `EXCEPT ALL` comparison; final-consumer cleanup must remove
all maintenance tables. A cross-catalog source consumer and an exclusion list containing its key
also verify that direct CASCADE exclusions and staged final DROP retain the
consumer's catalog/schema identity.

Schema CASCADE cases cover public/source, control, and internal namespaces with
separate placement, quoted names, and a non-default search path. They check
RESTRICT and absent schemas, replacement before DROP, DROP/recreate in one
transaction, commit/rollback, auxiliary state, cross-catalog descendants, shared
siblings, and final-consumer cleanup. Concurrent schema DROP uses the same
deterministic mutation-gate fixture as view DROP. Every surviving MV receives
conflicting batched writes and bidirectional `EXCEPT ALL` checks. The DuckLake
fixture also drops public/source schemas through staged native-metadata cleanup,
retains a shared native source delta for a lake sibling, and removes it with the
final public namespace.

Parent-first view CASCADE uses the same cross-registry dependency closure, including
a child in another native catalog and a grandchild back in the parent's catalog.
Direct and quoted/commented batch cases cover local replacement replay and
commit/rollback. Native and DuckLake cases retain unrelated same-named consumers
and shared source deltas, then check conflicting batched DML with bidirectional
bag equality and final-consumer cleanup. Explicit cross-native-catalog CASCADE
retains DuckDB's write restriction; rollback restores any preceding local cleanup
and replacement, and every restored view still captures conflicting source DML.

Unicode CASCADE cases keep `"Ä".items` and `"ä".items`, plus same-named MVs,
distinct with separate metadata and maintenance schemas. Replacement followed by
direct or mixed-batch CASCADE covers autocommit, explicit commit/rollback, and
staged cleanup across distinct quoted native catalogs. Surviving views receive
conflicting batched writes and bidirectional bag checks before final-consumer
cleanup. Identifier comparison cases also verify ASCII case equivalence, Unicode
distinction and NULL behavior under binary and Unicode case-insensitive string
collations.

Publication-owner cases remove a native parent's last child in the same or another
catalog, using separate quoted control/internal schemas and a non-default search
path. DROP and replacement commit/rollback must retain both parent delta
companions; conflicting batched DML must still refresh each surviving MV to
bidirectional bag equality. A helper selected in another catalog also checks
uncommitted parent creation and deletion through metadata replay, including a new
metadata location, qualified source keys, commit, rollback and final-owner cleanup.
Cross-catalog publication cases keep every child outside the parent's metadata
catalog, exercise append-only and conflicting batched DML, and let one child lag
across refreshes. Parent replacement commit/rollback and subsequent DML must
preserve both children's bag equality and final cleanup.

Before that matrix starts, quoted mixed-case catalog placement is checked in
both native metadata registries. CREATE normalization must preserve quoted
identifiers (including escaped quotes and embedded apostrophes); selecting the
same catalog with different casing must still find its own metadata schema.

Native batch cleanup also runs the source CASCADE ownership matrix with a trailing
statement. Batched view DROP checks quoted, same-named views, auxiliary-state and
publication cleanup, shared-source survival, commit/rollback, and final-consumer
cleanup. It exercises post-bind routing with the optimizer disabled and result
order preservation disabled. Surviving views receive conflicting batched DML and
are checked with bidirectional `EXCEPT ALL` after each refresh.

Mixed DROP/CREATE batches return extension statement ASTs to DuckDB's native
query driver. Each lifecycle compiles only after preceding native statements
execute; generated SQL then binds and executes one step at a time on the caller
context and active executor. This covers source tables and schemas created in the
same input, including `BEGIN`, replacement, native verification and profiling,
quoted semicolons, dollar-quoted literals, nested and keyword comments, intervening
DML, explicit commit and rollback.
Metadata replay supplies helper-local ownership visibility without running physical
programs ahead of their native statement position.
The metadata-setting transaction matrix changes and clears the setting between
CREATE, ALTER, replacement, final-consumer DROP and recreation, using both separate
and batched statements with new and existing registries. Commit/rollback checks
reject split registries and leaked schemas; surviving views receive conflicting
batched writes and bidirectional bag-equality checks after refresh. A new transaction
after rollback must use its newly configured metadata location.
Metadata migration is exercised through both refresh and CREATE, including a
late copy failure after earlier registry tables have moved. The test checks atomic
rollback, successful retry, movement back to `main`, retained hooks and unchanged
internal tables. Each refresh checks bidirectional bag equality after conflicting
batched writes; final-consumer DROP must still remove its source delta.

The mixed lifecycle cases preserve newline/line-comment-concatenated string literals,
nested block comments, dollar-quoted case and literal lifecycle keywords through
`REFRESH EVERY`, replacement, commit and rollback. Their expected results use
explicit literal values. Header normalization leaves the AS body unchanged;
native and lifecycle parsing must both reject block comments between adjacent
literals rather than silently concatenate them. The native and DuckLake
internal-schema SQL fixtures select existing maintenance schemas with different casing for parent and child MVs,
then verify publication, pipeline refresh, reopen and final cleanup. Windows over
`"Window Source"` exercise raw and SQL-quoted lineage with conflicting batched
writes and bidirectional bag equality.
The native lifecycle executable also checks column lookup with ASCII spelling
differences in catalog, schema and table names, preserving column order and
distinct Unicode identities under binary and case-insensitive string collations.

The fixture also injects a late binder failure after successful lifecycle writes.
Autocommit rolls back the statement; an explicit transaction must be invalidated
so COMMIT cannot retain partial writes. Profiling metrics must be restored on
success and failure, and surviving MVs must refresh to bidirectional bag equality.

The internal-schema SQL fixtures keep LEFT JOIN aggregate sources with matching
bare names distinct by catalog/schema. Nullable-side SUM and COUNT explicitly
require incremental aggregate maintenance (type 0) and qualified secondary-delta
metadata. Separate preserved-side SUM fixtures require the existing source-delta
group-recompute path (type 6); they do not add preserved-value reconstruction.
Both paths cover conflicting INSERT/DELETE/UPDATE batches on both sources,
duplicates, quoted output names and NULLs. Native refreshes exercise commit and
rollback; DuckLake refreshes exercise snapshot changes and reopen with unset
placement settings and a native metadata search path. Every refresh checks both
`EXCEPT ALL` directions before final-consumer cleanup.

`left_join_pipeline_secondary_delta.test` also starts with empty committed source
deltas, then refreshes after conflicting writes on both sides in an explicit
transaction. Compile-only plans must retain the outer-join transition guards for
caller-local deltas; deleting both sides must not leave a negative-count group.
Both COMMIT and ROLLBACK check bidirectional bag equality inside and after the
transaction. The qualified-source variants remain in the namespace fixtures.

The lifecycle executable also covers failed autocommit initial materialization,
corrected retry, shared source consumers and final DROP in the same or another
native metadata catalog. New helper-committed deltas must be reclaimed on failure;
existing consumers keep their deltas. CREATE followed by two column renames,
ADD/DROP of an unreferenced column and rejection of a referenced-column DROP runs
with new and existing configured metadata, separate maintenance schemas, mixed
batches, commit and rollback. Cross-native-catalog ALTER retains helper undo.
Every surviving view is refreshed after conflicting writes and checked in both
`EXCEPT ALL` directions. The DuckLake fixture additionally selects a projection
key from the second same-named source in a three-way join and checks failed staged
CREATE over a native source through retry and final cleanup.

The native and DuckLake internal-schema fixtures also require LEFT JOIN projection
maintenance to retain qualified source identities for same-named tables across
schemas, and raw table names containing dots and escaped quotes. They inspect the
generated SQL for affected-key pushdown (native) or hybrid transition maintenance
(DuckLake), so bag equality cannot hide a lost maintenance path. Both sources
receive conflicting INSERT/DELETE/UPDATE batches, including duplicate rows and
NULL keys, with commit/rollback and bidirectional `EXCEPT ALL` checks before cleanup.

Source-publication admission also distinguishes committed untracked writes from
read-only helper commits and writes to a same-named table in another schema.
Explicit first-MV creation with a pinned source snapshot must accept those
unrelated commits and rolled-back source writes, while requesting a transaction
retry for uncaptured committed source writes, including after waiting for a writer.
A snapshot predating another consumer's ownership must still detect source commits
after that consumer refreshes and drops its final delta; rollback keeps the source
unchanged and permits allocation in another maintenance schema. Legacy-registry upgrade retains a
pinned source snapshot through CREATE and commit/rollback. These cases run for
colocated and dedicated maintenance schemas alongside the deterministic concurrent
CREATE/DML and independent-writer cases; refreshes check both `EXCEPT ALL` directions.

Planning-only source admission is checked for plain `EXPLAIN` over INSERT,
DELETE, UPDATE and MERGE, and for SQL/API preparation without execution. A
second connection pins a source snapshot before that planning; commits to an
unrelated source must still permit first-MV creation. Colocated and dedicated
maintenance schemas cover autocommit and explicit writer transactions, including
a tracked consumer created and dropped outside the pinned snapshot. Executed
`EXPLAIN ANALYZE` and SQL/API prepared DML must retain source commit markers;
rollback must discard them. These checks isolate the planned/executed statement
from other writes to its source, then check surviving MV refreshes after
conflicting batched writes with bidirectional `EXCEPT ALL`.
