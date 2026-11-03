# DuckDB 2.0 upgrade investigation

Status: migration candidate on `upgrade/duckdb-2.0-parser-ctes-20261028`, based on
OpenIVM main `7e425376`. The branch uses the released DuckDB v2.0.0 commit
`d54cbbe3df`, extension-ci-tools `e38c2e18`, and LPTS `a376261e` from
`upgrade/duckdb-2.0-20261028`. No main-branch pins changed.

The goal is to replace OpenIVM execution and transaction plumbing with native
DuckDB facilities, then delete the replaced implementation. Avoid adding a
second general-purpose capture or transaction framework.

## Verified against the released binary

Tested DuckDB v2.0.0 `d54cbbe3df` on macOS ARM64, 2026-10-28, using fresh
scratch databases. These are feasibility probes, not an OpenIVM regression run.

| Probe | Result |
|---|---|
| Statement-level INSERT/DELETE/UPDATE transition tables | Capture signed row deltas correctly across a mixed batch, including key-changing updates and rollback; reconstructed bag equals the source in both `EXCEPT ALL` directions |
| COPY FROM | Fires the INSERT capture trigger |
| Close/reopen without OpenIVM | Persisted triggers continue capturing changes |
| MERGE in a trigger body, targeting an untriggered summary | Works for an insert-maintained SUM/COUNT example; both-direction bag comparison passes |
| MERGE targeting a table with triggers | Rejected |
| ON CONFLICT DO UPDATE with NEW TABLE capture | Rejected |
| CREATE OR REPLACE TABLE | Drops the old triggers and does not capture replacement deltas |
| ADD COLUMN | Existing explicit-column capture continues working; this does not establish automatic delta-schema evolution |
| RENAME COLUMN | Rejected because of trigger dependencies |
| PRAGMA as trigger body | Rejected by the parser |
| Trigger body referencing another catalog | Rejected by the binder |
| Trigger on a DuckLake table | Rejected: `Triggers are not supported for this table type` |
| Trigger in a database created by the existing 1.5.4 build | Rejected until storage version supports 2.0; upgrading the executable alone is insufficient |

The simple aggregate example establishes that generated maintenance SQL can run
inside a native trigger. It does not establish support for OpenIVM's complete
refresh compiler, chained views, remote metadata, or DuckLake.

## Where DuckLake support is missing

DuckDB's generic `TableCatalogEntry::CreateTrigger` implementation throws the
observed error; its default `ScanTriggers` yields no triggers. DuckLake's table
entry does not currently override the trigger catalog interfaces. This is an
extension-table integration gap, not an error in the SQL example.

The promising upstream route is to implement persistent, transactional trigger
catalog entries and lifecycle methods in DuckLake, then validate DuckDB's trigger
expansion and old/new transition-table execution on DuckLake writes. Tests must
include reopen, concurrent DDL/DML, rollback, schema changes, and snapshot behavior.
Supporting catalog entries alone is not proof that execution works.

A native shadow/staging table is not a transparent replacement: writes made
directly to DuckLake bypass it, and forwarding trigger writes across catalogs is
rejected. It would change ingestion and retain coordination work. For an unmodified
DuckLake, existing snapshot-based change detection remains the practical route;
there is no verified SQL-only trigger workaround.

## Migration decisions

1. OpenIVM and LPTS are pinned to the exact v2.0.0 release rather than a moving
   branch. The API port covers parser extensions, strongly typed planner indices,
   expression accessors, result collections, storage writes, lifecycle callbacks,
   extension CI tooling, and the matching DuckLake extension.
2. Resolve native MERGE/upsert trigger support upstream before replacing all native
   capture. Existing OpenIVM tests exercise those writes; removing their support
   would be a regression. Keep table-replacement and schema-evolution semantics
   explicit rather than silently losing capture.
3. Replace native capture with statement-level transition-table triggers, keeping
   source and delta writes in the same catalog/transaction. Delete obsolete
   physical capture, preimage fetching, deduplication, and execution wrappers once
   the existing behavior tests pass. Audit all remaining uses before deleting files.
4. Pursue DuckLake trigger support upstream if we want the same mechanism for lake
   sources. Continue snapshot-based capture while that work is incomplete.
5. Consider eager maintenance as a distinct execution policy. A per-statement
   trigger is not an end-of-ingestion-batch or DAG-run boundary. Preserve explicit
   `refresh_pipeline` semantics for batched, once-per-view DAG maintenance.

The main capture implementation is 1,332 lines plus a 106-line header at the base
commit; the mutation optimizer rule is another 575 lines. These are investigation
candidates, not a promised deletion count: MERGE, CTAS, lifecycle, and schema
responsibilities currently share this code. Cross-catalog metadata protocols,
remote leases, and DuckLake commit recovery cannot be removed solely because
native triggers exist.

## Sources and local evidence

- [DuckDB 2.0 trigger documentation](https://duckdb.org/docs/2.0/sql/statements/create_trigger)
- [Generic table trigger hooks](https://github.com/duckdb/duckdb/blob/v2.0.0/src/catalog/catalog_entry/table_catalog_entry.cpp)
- [Trigger binding and catalog/storage restrictions](https://github.com/duckdb/duckdb/blob/v2.0.0/src/planner/binder/statement/bind_create.cpp)
- [Trigger expansion](https://github.com/duckdb/duckdb/blob/v2.0.0/src/planner/binder/query_node/bind_trigger_expansion.cpp)
- [DuckLake table entry](https://github.com/duckdb/ducklake/blob/main/src/include/storage/ducklake_table_entry.hpp)

Local investigation artifacts: `/private/tmp/openivm-duckdb2-probes-20261028/`
contains the SQL probes, Python drivers, `results.json`, and inspected upstream
sources. These files are scratch evidence, not part of the maintained test suite.

## PEG parser and data-modifying CTE support

The user chose to pursue these two features, leaving trigger replacement aside,
and to extend existing SQL test files.

- `parser_parse.cpp` now defines CREATE/REPLACE and ALTER MV grammar rules. Native
  SELECT and qualified-name rules produce ASTs directly. The override selects a
  cached grammar instead of splitting and reparsing every statement. A token-based
  fallback handles extension loading and dialect normalization.
- CREATE/REPLACE, ALTER EVERY/MANUAL, quoted names, comments, copied parsed
  statements, and mixed native/MV batches are covered by the integrated parser
  tests. The malformed adjacent-string example is rejected.
- The native window partition cascade path can emit materialized old/new CTEs,
  followed by DELETE, INSERT, and signed delta publication. Explicit dependencies
  use the DML CTEs' one-row Count results, including for empty modifications.
- DuckLake rejects **multiple modifying CTEs** in one WITH clause. The dispatcher
  therefore checks the target catalog's `SupportsMultipleDMLCTEs()` capability;
  unsupported catalogs and external compilation retain the existing SQL program.
  This preserves the incremental affected-partition strategy.
- Generated SQL from the actual helper passed mixed DML, duplicate, NULL, empty-old,
  empty-new, empty-both, and explicit rollback probes: five native CTE scenarios
  and the same five DuckLake scenarios through the existing program, each with
  both-direction bag comparisons for the data and signed delta. A forced failure
  during native delta insertion also rolled back preceding MV modifications.
  Direct DuckLake CTE failures remain recorded as feasibility evidence.
- Regression scenarios in `test/sql/parser.test` and `test/sql/window.test` cover
  direct grammar use with the override disabled, chained window refresh, archive
  output, and rollback.

## LPTS and repeated CTE plans

DuckDB 2.0 exposes a common-subplan optimizer that can factor repeated logical
work into materialized CTEs. LPTS now exposes this as an explicit pre-serialization
step. Its normal mode retains DuckDB's cost decision. OpenIVM uses a
serialization-size mode that removes cardinality from the decision, because the
stored SQL must remain bounded independently of current table statistics.

This fixes repeated and nested CTE DAGs without falling back to the original SQL
or changing them to full refresh. Refresh compilation expands the shared CTE
references into independent delta leaves before applying the delta algebra. The
regression tests batch INSERT, DELETE, and UPDATE before one refresh and compare
the MV and base query with `EXCEPT ALL` in both directions.

The rewriter benchmark worker also now returns through normal C++ destruction.
Previously it called `_exit()` while the database was still open, left a WAL on
every normal run, and deleted that WAL before the next run. That could discard
recovery state and made compiler stress results appear nondeterministically
crashy. DuckDB now closes or recovers the WAL normally; the harness never deletes
it.

Additional local evidence: `/private/tmp/openivm-v2-peg-probe.log`,
`/private/tmp/openivm-duckdb2-probes-20261028/guarded-cte-results.json`, and
`/private/tmp/openivm-duckdb2-probes-20261028/cte-failure-atomicity.json`.
