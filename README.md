# OpenIVM

A DuckDB extension for **incremental view maintenance** (IVM). Create materialized views pipelines with standard SQL, then refresh them incrementally — without recomputing the entire query.

Based on the [OpenIVM paper](https://dl.acm.org/doi/10.1145/3626246.3654743) (SIGMOD 2024).

## Quick start

OpenIVM is currently under testing. We hope to release it as a community extension
by the end of 2026. [Issues and feature requests](https://github.com/ila/openivm/issues)
are welcome.

OpenIVM currently requires a source build with DuckDB v1.5.4. Follow the
[build and setup guide](docs/build/building.md) first; the built CLI already loads OpenIVM.

```sql
LOAD 'openivm'; -- No-op in the source-built CLI (statically linked); needed only in other DuckDB clients.

-- Create a base table and a materialized view
CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 'Widget', 100), ('EU', 'Gadget', 200);

CREATE OR REPLACE MATERIALIZED VIEW regional_totals REFRESH EVERY '5 minutes' AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt FROM sales GROUP BY region;

-- Insert new data
INSERT INTO sales VALUES ('US', 'Bolt', 50), ('JP', 'Gear', 300);

-- Refresh manually at any time
PRAGMA refresh('regional_totals');

SELECT * FROM regional_totals ORDER BY region;
-- EU  | 200 | 1
-- JP  | 300 | 1
-- US  | 150 | 2
```

Run these statements at the prompt or from a SQL file, not as one `-c` argument or one
multi-statement client call: DuckDB expands `PRAGMA refresh` before the preceding
`CREATE MATERIALIZED VIEW` in the same batch has run, so the view is not found yet.

Views with `REFRESH EVERY` are maintained automatically by a background daemon. See [automatic refresh](docs/refresh/automatic-refresh.md).

Base table schema changes (`ADD`, `DROP`, `RENAME COLUMN`) are propagated by OpenIVM. Delta tables are synced, referenced renames update MV metadata, and referenced drops are blocked with an error. See [schema evolution](docs/internals/schema-evolution.md).

## SQL syntax

```sql
CREATE [OR REPLACE] MATERIALIZED VIEW [catalog.][schema.]name [REFRESH EVERY '<interval>'] AS <query>;
ALTER MATERIALIZED VIEW [catalog.][schema.]name SET REFRESH EVERY '<interval>';
ALTER MATERIALIZED VIEW [catalog.][schema.]name SET REFRESH MANUAL;
DROP VIEW [catalog.][schema.]name;  -- removes the MV, its internal tables and metadata
```

`CREATE MATERIALIZED VIEW IF NOT EXISTS` and `DROP MATERIALIZED VIEW` are not supported.
See [parser](docs/internals/parser.md) and [limitations](docs/limitations.md).

## Data pipelines and DuckLake

Materialized views can be stacked into pipelines, including over [DuckLake](https://ducklake.select/) tables. DuckLake's snapshot-based time travel enables delta computation through native change tracking, avoiding storage duplication. See [DuckLake IVM integration](docs/ducklake.md). 
```sql
INSTALL ducklake;
LOAD ducklake;
ATTACH ':memory:' AS dl (TYPE ducklake);

CREATE TABLE dl.orders (id INT, product VARCHAR, region VARCHAR, amount INT);
INSERT INTO dl.orders VALUES (1, 'Widget', 'US', 500), (2, 'Gadget', 'EU', 200), (3, 'Widget', 'EU', 800);

-- First MV: aggregate by product
CREATE MATERIALIZED VIEW dl.product_totals REFRESH EVERY '5 minutes' AS
    SELECT product, SUM(amount) AS total, COUNT(*) AS cnt FROM dl.orders GROUP BY product;

-- Second MV: built on top of the first
CREATE MATERIALIZED VIEW dl.top_products REFRESH EVERY '10 minutes' AS
    SELECT product, total FROM dl.product_totals WHERE total > 1000;

-- Cascade modes (controls automatic refresh propagation):
SET openivm_cascade_refresh = 'downstream';  -- default: refreshing product_totals also refreshes top_products
SET openivm_cascade_refresh = 'upstream';    -- refreshing top_products first refreshes product_totals
SET openivm_cascade_refresh = 'both';        -- refresh in both directions
SET openivm_cascade_refresh = 'off';         -- no cascade, each view refreshes independently
```
Note: without cascading refresh, views refreshing independently may see stale upstream data — results are consistent but not fresh until the next ordered refresh. See [pipelines](docs/refresh/pipelines.md).

## Supported operators

MVs can be created using any SQL construct. Unsupported operators automatically fall back to [full refresh](docs/refresh/refresh-strategies.md).

| Operator | Strategy | Documentation |
|----------|----------|---------------|
| `SELECT ... FROM`, `WHERE`, expressions | Incremental | [Projection & filter](docs/operators/projection-filter.md) |
| `GROUP BY` + `SUM`, `COUNT`, `AVG` | Incremental | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `STDDEV`, `VARIANCE` (all variants) | Incremental | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `MIN`, `MAX` | Incremental (insert-only) / group-recompute | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| `HAVING` | Incremental | [Grouped aggregates](docs/operators/grouped-aggregates.md) |
| Ungrouped aggregates | Incremental | [Ungrouped aggregates](docs/operators/ungrouped-aggregates.md) |
| `INNER JOIN`, `CROSS JOIN`, arbitrary join predicates | Incremental | [Inner join](docs/operators/inner-join.md) |
| `LEFT JOIN`, `RIGHT JOIN` | Incremental | [Left join](docs/operators/left-join.md) |
| `FULL OUTER JOIN` | Incremental (MERGE + recompute) | [Full outer join](docs/operators/full-outer-join.md) |
| `SEMI JOIN`, `ANTI JOIN`, `EXISTS`, `NOT EXISTS` | Aux-state incremental for supported projection shapes | [Semi & anti join](docs/operators/semi-anti-join.md) |
| `UNION ALL` | Incremental | [Union all](docs/operators/union-all.md) |
| `DISTINCT` | Incremental | [Distinct](docs/operators/distinct.md) |
| `ORDER BY ... LIMIT k` (top-k) | Incremental; the limit is applied when the view is read | [Top-k](docs/operators/top-k.md) |
| Window functions (`ROW_NUMBER`, `RANK`, etc.) | Partition-level recompute | [Window functions](docs/operators/window-functions.md) |
| `LIST` aggregates | Incremental | [List aggregates](docs/operators/list-aggregates.md) |
| `WITH` (CTEs), decorrelated subqueries, scalar correlated subqueries | Incremental when the lowered plan uses supported operators; scalar `SINGLE` delim shapes use affected-key recompute | [CTEs & subqueries](docs/operators/cte-subquery.md) |


## Settings

| Setting | Type | Default | Description | Documentation |
|---------|------|---------|-------------|---------------|
| `openivm_cascade_refresh` | VARCHAR | `downstream` | Cascade mode: `off`, `upstream`, `downstream`, `both` | [Pipelines](docs/refresh/pipelines.md) |
| `openivm_refresh_mode` | VARCHAR | `incremental` | `full` forces full recompute; `incremental` (or `auto`) uses the strategy chosen at creation | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `openivm_adaptive_refresh` | BOOLEAN | `false` | Experimental: pick incremental or full refresh per run with the learned cost model | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `openivm_cost_decay` | DOUBLE | `0.9` | Decay factor (0.0–1.0) for the learned cost model; higher adapts more slowly | [Cost model](docs/internals/cost_model.md) |
| `openivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase refresh interval when refresh takes longer than interval | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `openivm_disable_daemon` | BOOLEAN | `false` | Do not start the background refresh daemon at extension load | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `openivm_profile_refresh` | BOOLEAN | `false` | Record per-step refresh timings in `openivm_refresh_profile` | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `openivm_profile_retention_days` | BIGINT | `31` | Delete profile rows older than this many days when profiling writes new rows | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `openivm_files_path` | VARCHAR | — | Directory for compiled SQL reference files (off when unset) | [Build: inspect SQL](docs/build/building.md#inspect-the-generated-sql) |
| `openivm_explain_initial_load` | BOOLEAN | `false` | Print the `CREATE MATERIALIZED VIEW` initial-load SQL and its `EXPLAIN` plans | [Build: inspect SQL](docs/build/building.md#inspect-the-generated-sql) |
| `openivm_explain_initial_load_only` | BOOLEAN | `false` | With `openivm_explain_initial_load`, print the diagnostic without creating the MV | — |
| `openivm_input_dialect` | VARCHAR | `duckdb` | Dialect of incoming `CREATE MATERIALIZED VIEW` bodies: `duckdb` or `spark` (e.g. `VERSION AS OF`) | [Parser](docs/internals/parser.md#input-dialect) |

<details>
<summary>Optimization and compilation toggles</summary>

| Setting | Type | Default | Description | Documentation |
|---------|------|---------|-------------|---------------|
| `openivm_skip_empty_deltas` | BOOLEAN | `true` | Skip refresh work or join terms when deltas are empty | [Empty delta skip](docs/optimizations/empty-delta-skip.md) |
| `openivm_compact_deltas` | BOOLEAN | `true` | Compact raw delta rows into net Z-set deltas before refresh | [Delta consolidation](docs/optimizations/delta-consolidation.md) |
| `openivm_skip_aggregate_delete` | BOOLEAN | `true` | Skip the zero-row DELETE for grouped aggregates when deltas are insert-only | [Append-only](docs/optimizations/append-only.md) |
| `openivm_skip_projection_delete` | BOOLEAN | `true` | Skip DELETE and consolidation for projections when deltas are insert-only | [Append-only](docs/optimizations/append-only.md) |
| `openivm_minmax_incremental` | BOOLEAN | `true` | Use `GREATEST`/`LEAST` for `MIN`/`MAX` when deltas are insert-only | [Append-only](docs/optimizations/append-only.md) |
| `openivm_fk_pruning` | BOOLEAN | `true` | Prune inclusion-exclusion join terms using FK constraints | [FK-aware pruning](docs/optimizations/fk-aware-pruning.md) |
| `openivm_ducklake_nterm` | BOOLEAN | `true` | Use N-term telescoping for DuckLake joins instead of 2^N−1 inclusion-exclusion | [DuckLake](docs/ducklake.md) |
| `openivm_having_merge` | BOOLEAN | `true` | Use MERGE for `HAVING` views (store all groups, filter in the view) instead of group-recompute | [Limitations](docs/limitations.md) |
| `openivm_left_join_merge` | BOOLEAN | `true` | Use incremental MERGE for supported LEFT/RIGHT JOIN aggregates instead of group-recompute | [Left join](docs/operators/left-join.md) |
| `openivm_full_outer_merge` | BOOLEAN | `true` | Use incremental MERGE for FULL OUTER JOIN aggregates instead of group-recompute | [Full outer join](docs/operators/full-outer-join.md) |
| `openivm_distinct_aux_state` | BOOLEAN | `false` | Use aux-state maintenance for supported inner-DISTINCT-under-aggregate shapes | [Distinct](docs/operators/distinct.md) |
| `openivm_stateful_auxstate` | BOOLEAN | `false` | Use persisted aux state for stateful aggregates such as `COUNT(DISTINCT)` instead of group-recompute | — |
| `openivm_running_window_incremental` | BOOLEAN | `false` | Extend cumulative running-window aggregates for append-only suffix batches | — |
| `openivm_enable_data_dependent_optimizers` | BOOLEAN | `true` | Optimize the executed refresh plan using current delta statistics | — |
| `openivm_regular_nterm` | BOOLEAN | `true` | Use N-term telescoping for eligible regular-table inner joins compiled for external engines | [Inner join](docs/operators/inner-join.md#regular-table-n-term-compilation) |
| `openivm_regular_nterm_left` | BOOLEAN | `true` | Extend compile-only N-term telescoping to LEFT JOIN projection views | — |
| `openivm_emit_spark_hints` | BOOLEAN | `false` | Emit Spark optimizer hints in `target_dialect=spark` compiled refresh SQL | — |

</details>


## Pragmas and functions

| Call | Description | Documentation |
|------|-------------|---------------|
| `PRAGMA refresh('view_name')` | Refresh a materialized view; accepts `schema.view`, `catalog.view`, or `catalog.schema.view` | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `PRAGMA refresh_options(catalog, schema, view_name)` | Refresh with explicit catalog/schema | — |
| `PRAGMA refresh_pipeline('view_name', ...)` | Refresh one or more MVs and their cascade dependencies as one ordered run | [Pipelines](docs/refresh/pipelines.md) |
| `PRAGMA refresh_status('view_name')` | Show refresh interval, last/next refresh, daemon status, and refresh strategy | [Automatic refresh](docs/refresh/automatic-refresh.md) |
| `PRAGMA refresh_cost('view_name')` | Show incremental refresh vs full recompute cost estimate (static + calibrated) | [Cost model](docs/internals/cost_model.md) |
| `PRAGMA refresh_history('view_name')` | Show refresh execution history (for learned cost model) | [Refresh strategies](docs/refresh/refresh-strategies.md) |
| `PRAGMA refresh_start_daemon` | (Re)start the background refresh daemon; not allowed inside a transaction | — |
| `PRAGMA openivm_files('view_name')` | Show paths and status of the compiled SQL reference files | [Build: inspect SQL](docs/build/building.md#inspect-the-generated-sql) |
| `PRAGMA openivm_declare_rely_fk(child_table, child_columns, parent_table, parent_columns)` | Declare a trusted (RELY) foreign key used by FK-aware join pruning | — |
| `openivm_compile_with_facts(view_name, facts_json)` | Table function: compile a refresh program as rows without executing it | [Build: compiled SQL](docs/build/building.md#inspect-or-save-compiled-sql-without-files) |

## Schemas, metadata, and compiled SQL

- **Can schemas contain same-named views or source tables?** Yes. Use
  `PRAGMA refresh('dl.observation.product_summary')` to identify a view explicitly.
  A short name works when it identifies one MV; ambiguous names produce an error.
- **Where are internal tables stored?** For DuckLake views, control metadata stays
  in the native frontend database's `main` schema. MV backing and internal delta
  tables live in the MV's schema; native source deltas live with their source.
- **Why are DuckLake delta tables empty?** DuckLake source changes come from
  snapshots. Internal MV delta tables are still created for lifecycle handling
  and can be empty. Leave them in place.
- **Can metadata stay outside DuckLake?** It already does. Start the built CLI with
  `./build/release/duckdb openivm_frontend.duckdb`, then attach your lake. Reopen
  that frontend and reattach the same lake under the same name in later sessions.
  A configurable metadata schema such as `openivm` is not implemented; use `main`.
- **Where is the generated SQL?** File export is opt-in. Create an output directory
  and set `openivm_files_path` before CREATE or refresh. Then call
  `PRAGMA openivm_files('dl.observation.product_summary')` to inspect exact paths.
  This pragma reports files; it does not generate them.
- **Can compiled SQL be stored in a table?** Yes: select from
  `openivm_compile_with_facts` into your own table. Automatic archival in OpenIVM
  metadata is not implemented. Compiled SQL contains state-specific cutoffs;
  compile again for later refreshes.

See [schema and metadata examples](docs/ducklake.md#schemas-metadata-and-internal-tables),
[exporting and inspecting SQL files](docs/build/building.md#inspect-the-generated-sql),
and [saving compiled SQL as rows](docs/build/building.md#inspect-or-save-compiled-sql-without-files).

## Documentation

- **[DuckLake integration](docs/ducklake.md)** — IVM over DuckLake tables with native change tracking
- **[Operators](docs/operators/)** — How each SQL operator is incrementalized
- **[Refresh](docs/refresh/)** — Refresh strategies, automatic refresh, pipelines, and [refresh hooks](docs/refresh_hooks.md)
- **[Optimizations](docs/optimizations/)** — Delta consolidation, FK pruning, empty-delta skip, append-only, indexing
- **[Internals](docs/internals/)** — Delta tables, parser, concurrency, cost model, schema evolution
- **[Concurrency and operations](docs/concurrency.md)** — What concurrent readers, writers and refreshes can rely on; running the daemon, upgrading, troubleshooting
- **[Limitations](docs/limitations.md)** — Unsupported operators, known restrictions
- **[Build](docs/build/building.md)** — Building, testing, benchmarks
