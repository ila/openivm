# DuckLake DAG workload audit — 2026-09-25

Audit baseline: main `18f83dbb`. Follow-up discussion notes, not a promise of deployment support.

## Requested workflow

Batch all source changes, then refresh a DAG of many small transformations once per node after all parents are ready. Run independent ready nodes in parallel. Obtain dependencies from OpenIVM and ideally replace Python orchestration with SQL. Queries include graph-like inner/left/anti/outer joins followed by aggregations and materialized dimensions. Use DuckLake snapshots for reproducibility and later simulations/migrations; develop on Apple Silicon and evaluate MotherDuck deployment.

## Findings to revisit

- Native DuckDB and DuckLake accept mixed INSERT/UPDATE/DELETE batches before refresh.
- Cascades topologically order the reachable graph. For A -> P -> Z <- Q <- B, refreshing P and Q separately downstream runs Z twice and initially exposes a result using stale Q. Upstream refresh of Z refreshes P, Q, Z once each with correct results. Verified in both engines using bidirectional EXCEPT ALL and refresh profiling. Multiple terminal nodes still need batch-wide orchestration to avoid revisiting shared ancestors.
- There is no general ready-node parallel scheduler: the database-wide mutation gate serializes OpenIVM mutations. Internal DuckDB query parallelism is separate.
- Dependency information is available through openivm_delta_tables. The explicit openivm_mv_dependencies edges are currently populated only with experimental view matching enabled; do not present that table as always populated.
- Inner/left/full joins and aggregations have maintenance paths, but query shape determines whether maintenance is incremental, affected-group recompute, or full refresh. A combined LEFT JOIN + ANTI JOIN example selected FULL_REFRESH.
- A DuckLake LEFT JOIN MV feeding an ANTI JOIN MV exposed a correctness bug: after mixed changes and correctly ordered refreshes, the parent matched its query but the child retained three old rows instead of two expected rows. Native DuckDB passed. The source metadata lookup missed the mapping from the logical MV name to its DuckLake openivm_data_ backing table. Regression/fix tracked in the existing ducklake_semi_anti test and shared refresh source lookup.
- DuckLake snapshot change scans and explicitly pinned time-travel queries are supported. No atomic snapshot/publish boundary for an entire DAG was established. Caller-controlled ingestion and publication barriers need discussion.
- A persistent native DuckDB controller database holds OpenIVM metadata while attached DuckLake catalogs hold data. This is not a requirement for a separate database server. Native metadata and external lake writes do not share one atomic transaction.
- MotherDuck integration remains unverified; no repository integration coverage found. Local DuckLake success does not establish managed deployment compatibility.
- No claim that every transformation will be incremental or faster than full refresh is justified without his actual queries and benchmarks.

## Validation at audit time

Actual CLI queries exercised both storage backends, mixed changes, refresh profiling, and bag equality in both directions. Existing chained and DuckLake time-travel tests passed 453 assertions across two cases. Those tests did not cover the newly reproduced chained anti-join failure.

## Chained SEMI/ANTI correction

Fixed the shared source-metadata lookup to recognize the DuckLake backing-table name for a logical MV source. The existing incremental snapshot-delta and auxiliary-state maintenance paths remain enabled. Extended `test/sql/ducklake_semi_anti.test` with the original anti-join shape, chained sources on both sides, semi joins, duplicate tuples, and two mixed-DML batches. Every refresh is checked against the original base-table query with EXCEPT ALL in both directions.

Validation after the fix: 1,286 assertions passed across eight related join, chain, and auxiliary-state test cases; the compiled N-term SQL integration checks passed; the original CLI example now has zero bag differences in both native DuckDB and DuckLake. The other audit findings above remain discussion items.
