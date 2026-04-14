# DuckLake IVM Integration

OpenIVM supports materialized views over [DuckLake](https://ducklake.select/) tables. DuckLake is a lakehouse extension for DuckDB that stores metadata in DuckDB and data as Parquet files. It provides snapshot-based time travel and tracks which files were added or removed per transaction.

When base tables are in a DuckLake catalog, OpenIVM leverages these features to replace delta tables with native change tracking. This enables a more efficient join delta rule (N terms instead of 2^N - 1) and eliminates the storage overhead of separate delta tables.

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
PRAGMA ivm('product_summary');

SELECT * FROM dl.product_summary ORDER BY pname;
-- Alpha | 150 | 2
-- Beta  | 400 | 1
```

## How it works

### Detection

OpenIVM automatically detects DuckLake tables at view creation time. When a base table
is backed by a DuckLake catalog, its entry in `_duckdb_ivm_delta_tables` is stored with
`catalog_type = 'ducklake'`. No user configuration is needed — DuckLake-specific
optimizations activate automatically.

### Delta detection (no delta tables)

Standard DuckDB tables use separate delta tables (`delta_<table>`) with a multiplicity
column and timestamp. DuckLake tables don't need delta tables — DuckLake's built-in
change tracking provides the same information natively.

OpenIVM reads rows inserted and deleted between two snapshots directly from DuckLake.
Insertions get multiplicity `true`; deletions get multiplicity `false`. This produces
the same delta format as standard delta tables but without maintaining a separate copy.

The last-refreshed snapshot ID is stored in `_duckdb_ivm_delta_tables` and updated
after each refresh. The next refresh reads changes between the stored snapshot and the
current one.

### N-term telescoping join rule

For joins over DuckLake tables, OpenIVM uses an N-term telescoping formula instead of the standard 2^N - 1 inclusion-exclusion terms.

**Standard inclusion-exclusion** (non-DuckLake): For N tables, generates all non-empty subsets — 2^N - 1 terms, each replacing a subset of base table scans with delta scans.

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

N-term telescoping can be disabled with `SET ivm_ducklake_nterm = false`, which falls
back to the standard 2^N - 1 inclusion-exclusion rule (also works with DuckLake tables).

### Empty-delta term skipping

When a table hasn't changed since the last refresh (`last_snapshot_id == current_snapshot_id`), its delta is empty and its term produces zero rows. OpenIVM detects this at plan time by comparing snapshot IDs and skips generating that term entirely — avoiding the cost of plan copying, renumbering, delta scan creation, and SQL generation.

In a typical star schema (1 fact table + 4 dimensions), only the fact table changes between refreshes. The term count drops from 5 to 1.

If all tables are unchanged, the refresh is skipped entirely via the [empty delta skip](optimizations/empty-delta-skip.md) optimization.

## Supported operators

All operators supported for standard DuckDB tables are also supported for DuckLake tables:

- Projection, filter, expressions
- Grouped and ungrouped aggregates (SUM, COUNT, AVG, MIN/MAX)
- Inner join (with N-term telescoping)
- Left join
- UNION ALL
- DISTINCT
- CTEs and subqueries
- Chained/cascading materialized views

## Limitations

- **No FK constraints.** DuckLake does not support `FOREIGN KEY` constraints, so the [FK-aware pruning](optimizations/fk-aware-pruning.md) optimization is not available. The [empty-delta term skipping](#empty-delta-term-skipping) optimization covers the most common case (unchanged dimension tables).
- **No ART indexes.** DuckLake tables don't support ART index creation. For `AGGREGATE_GROUP` views, group column identification falls back to metadata instead of the index catalog.
- **Single DuckLake catalog.** All base tables in a DuckLake-backed materialized view must be in the same DuckLake catalog.

