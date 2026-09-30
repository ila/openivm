# DuckLake IVM Integration

OpenIVM supports materialized views over [DuckLake](https://ducklake.select/) tables. DuckLake is a lakehouse extension for DuckDB that stores metadata in DuckDB and data as Parquet files. It provides snapshot-based time travel and tracks which files were added or removed per transaction.

When base tables are in a DuckLake catalog, OpenIVM uses snapshot-based change
tracking instead of maintaining separate source-table delta copies. This enables
a more efficient join delta rule (N terms instead of 2^N - 1). Internal MV delta
tables are still created; they are distinct from source-table change tracking.

## Quick start

```sql
INSTALL ducklake;
LOAD ducklake;

-- Attach a DuckLake catalog (in-memory for testing, or a real path)
ATTACH ':memory:' AS dl (TYPE ducklake);

-- Create base tables in the DuckLake catalog
CREATE TABLE dl.products (pid INT, pname VARCHAR);
CREATE TABLE dl.sales (pid INT, qty INT, revenue INT);
INSERT INTO dl.products VALUES (1, 'Alpha'), (2, 'Beta');
INSERT INTO dl.sales VALUES (1, 10, 100), (2, 20, 400);

-- Create a materialized view over DuckLake tables
CREATE MATERIALIZED VIEW dl.product_summary AS
    SELECT p.pname, SUM(s.revenue) AS total_rev, COUNT(*) AS sale_count
    FROM dl.products p INNER JOIN dl.sales s ON p.pid = s.pid
    GROUP BY p.pname;

-- Insert new data and refresh
INSERT INTO dl.sales VALUES (1, 5, 50);
PRAGMA refresh('product_summary');

SELECT * FROM dl.product_summary ORDER BY pname;
-- Alpha | 150 | 2
-- Beta  | 400 | 1
```

## Schemas, metadata, and internal tables

Materialized views are identified by catalog, schema, and SQL name. Different
schemas can contain views with the same name. Use a qualified name when refreshing
or inspecting one of those views:

```sql
-- Control metadata stays in the native frontend database's main schema.
SELECT view_sql_name, view_catalog, view_schema, view_name AS internal_key
FROM main.openivm_views;
PRAGMA refresh('dl.observation.product_summary');
PRAGMA openivm_files('dl.observation.product_summary');
```

Source tables also retain their catalog and schema identity. One MV can join
`dl.observation.products` with `dl.reference.products`, including repeated uses
of either source in a self-join. DuckLake sources in different attached catalogs
use independent snapshot watermarks. Qualify source names in the view definition
when the names would otherwise be ambiguous.

New views receive an internal key derived from their catalog, schema, and name.
Key allocation does not wait for other views to register their names. Existing
views retain their stored keys. First-time setup of the shared metadata and native
source-delta tables is briefly serialized; the initialization guard does not
cover view planning or initial materialization. Explicit transactions retain
rollbackable setup and DuckDB transaction-conflict semantics. Internal backing-table and compiled-file names
use these keys; use `PRAGMA openivm_files` to discover generated file paths.

For a view created as `dl.observation.product_summary`, its backing,
visible-output, and internal MV delta tables live in `dl.observation`.
Native source-table delta tables live with
their source table. Inspect actual locations rather than relying on `SHOW TABLES`
under the current search path:

```sql
SELECT database_name, schema_name, table_name
FROM duckdb_tables()
WHERE starts_with(table_name, 'openivm_')
ORDER BY database_name, schema_name, table_name;
```

For DuckLake MVs, OpenIVM's control tables (`openivm_views`,
`openivm_delta_tables`, dependency and refresh-history tables) already live in
`main` of the native DuckDB frontend database, not as Parquet tables in DuckLake.
Start the CLI with a persistent frontend file to retain them between sessions:

```bash
./build/release/duckdb /absolute/path/to/openivm_frontend.duckdb
```

Then attach the lake and create views in its catalog as in the quick start. After
reopening the same frontend file, attach the same lake under the same catalog name
before refreshing. Starting the CLI without a filename uses an in-memory frontend;
its OpenIVM metadata will not survive closing the process. There is currently no
setting to relocate OpenIVM control tables into an `openivm` schema. Do not move
those tables manually.

For native sources in another attached DuckDB database, refresh commits the MV
and its watermark together, then cleans up external source deltas. Rollback
cancels that cleanup. Cleanup retains changes needed by other consumers, including
MVs whose metadata lives in another native catalog. If cleanup encounters a
conflict, OpenIVM reports that the refresh committed and cleanup was deferred;
the committed watermark prevents those retained changes from being applied twice.

### Empty delta tables

DuckLake source tables do not get physical `openivm_delta_<source>` tables. Their
rows in the native `openivm_delta_tables` metadata table record source locations
and snapshot watermarks; the similarly named metadata table is not a row-change
buffer.

OpenIVM still creates `openivm_delta_<internal_key>` and, where a visible-output boundary
is used, `openivm_delta_openivm_visible_<internal_key>`. These are internal MV maintenance
objects, even for DuckLake targets. Empty contents do not mean change tracking is
broken: DuckLake changes are obtained from snapshots, and native delta buffers
can also be cleared after consumption. Keep these internal objects intact; they
are currently part of view creation and lifecycle handling. Removing unnecessary
DuckLake MV delta objects would require a code change, not manual deletion.

### Compiled SQL and file paths

SQL files are written only when `openivm_files_path` is set before the relevant
compilation. `PRAGMA openivm_files('product_summary')` shows their paths and whether
they exist. See [Inspect the generated SQL](build/building.md#inspect-the-generated-sql)
for directory setup and [saving compiled SQL in a table](build/building.md#inspect-or-save-compiled-sql-without-files).
Saving compiler output as rows already works; automatically retaining the last
executed program in OpenIVM's own metadata is not currently implemented.

## How it works

### Detection

OpenIVM automatically detects DuckLake tables at view creation time. When a base table
is backed by a DuckLake catalog, its entry in `openivm_delta_tables` is stored with
`catalog_type = 'ducklake'`. No user configuration is needed — DuckLake-specific
optimizations activate automatically.

### Source delta detection (snapshot-based)

Standard DuckDB tables use separate delta tables (`openivm_delta_<table>`) with a multiplicity
column and timestamp. DuckLake tables don't need delta tables — DuckLake's built-in
change tracking provides the same information natively.

OpenIVM reads rows inserted and deleted between two snapshots directly from DuckLake.
Insertions get multiplicity `+1`; deletions get multiplicity `-1`. This produces
the same delta format as standard delta tables but without maintaining a separate copy.

The last-refreshed snapshot ID is stored in `openivm_delta_tables` and updated
after each refresh. The next refresh reads changes between the stored snapshot and the
current one.

### N-term telescoping join rule

For joins over DuckLake tables, OpenIVM uses an N-term telescoping formula instead of
the default 2^N - 1 inclusion-exclusion terms.

**Standard inclusion-exclusion:** For N tables, generates all non-empty subsets —
2^N - 1 terms, each replacing a subset of base table scans with delta scans. Eligible
regular-table projection SQL compiled for external engines has a separate
[N-term path](operators/inner-join.md#regular-table-n-term-compilation).

**N-term telescoping** (DuckLake): For N tables, generates exactly N terms:

```
Term 0: delta(T0)   join T1_old  join T2_old  join ... join Tn_old
Term 1: T0_new      join delta(T1) join T2_old  join ... join Tn_old
Term 2: T0_new      join T1_new  join delta(T2) join ... join Tn_old
...
Term N-1: T0_new    join T1_new  join T2_new  join ... join delta(Tn)
```

Each term replaces exactly one table with its delta scan. Tables before the delta read their **current** (new) state; tables after the delta are pinned to their **old** state via `AT VERSION <snapshot_id>`. DuckLake's time travel enables reading the old state without storing a separate copy.

This is algebraically equivalent to inclusion-exclusion but avoids the exponential blowup:

| Tables | Inclusion-exclusion terms | N-term telescoping |
|---|---|---|
| 2 | 3 | 2 |
| 3 | 7 | 3 |
| 4 | 15 | 4 |
| 5 | 31 | 5 |

N-term telescoping can be disabled with `SET openivm_ducklake_nterm = false`, which falls
back to the standard 2^N - 1 inclusion-exclusion rule (also works with DuckLake tables).

### Empty-delta term skipping

When a table hasn't changed since the last refresh, its delta is empty and its term produces zero rows. OpenIVM detects this at plan time and skips generating that term entirely — avoiding the cost of plan copying, renumbering, delta scan creation, and SQL generation. Equal snapshot IDs (`last_snapshot_id == current_snapshot_id`) prove emptiness. Because snapshot IDs are catalog-wide, a changed ID may come from another table, so OpenIVM also probes `ducklake_table_insertions`/`ducklake_table_deletions` for the table itself. For inner joins, a term is also skipped when none of the delta's join keys matches the other side. These checks follow `openivm_skip_empty_deltas` (default `true`).

In a typical star schema (1 fact table + 4 dimensions), only the fact table changes between refreshes. The term count drops from 5 to 1.

If all tables are unchanged, the refresh is skipped entirely via the [empty delta skip](optimizations/empty-delta-skip.md) optimization.

## Supported operators

DuckLake-backed views support the same operator families as standard DuckDB tables, with DuckLake-specific join maintenance:

- Projection, filter, expressions
- Grouped and ungrouped aggregates, including AVG and STDDEV/VARIANCE decomposition
- Inner joins, cross joins, and arbitrary-predicate joins
- Joins use N-term telescoping when every join leaf is a DuckLake scan; for LEFT joins, each term demotes only the outer joins whose NULL-supplying side carries that term's delta
- Left, right, and full outer joins additionally use the standard partial-recompute/MERGE paths for NULL-padded rows
- UNION ALL
- DISTINCT
- Semi/anti joins for supported aux-state shapes
- Window functions on supported single-table shapes
- CTEs and decorrelated subqueries
- Chained/cascading materialized views

## Limitations

- **Metadata and data are separate transactions.** OpenIVM's metadata lives in the native DuckDB database that
  loaded the extension (no separate server needed), while data and deltas live in the attached DuckLake catalog. A
  refresh commits lake-side writes and native metadata separately, not atomically; creation, refresh and `DROP VIEW`
  use staged steps for this reason. A DuckLake file being readable by another DuckDB build says nothing about
  OpenIVM extension compatibility.
- **MotherDuck is unverified.** There is no integration coverage for managed DuckLake on MotherDuck; local success
  does not establish it.

- **No FK constraints.** DuckLake does not support `FOREIGN KEY` constraints, so the [FK-aware pruning](optimizations/fk-aware-pruning.md) optimization is not available. The [empty-delta term skipping](#empty-delta-term-skipping) optimization covers the most common case (unchanged dimension tables).
- **No ART indexes.** DuckLake tables don't support ART index creation. For `AGGREGATE_GROUP` views, group column identification falls back to metadata instead of the index catalog.
