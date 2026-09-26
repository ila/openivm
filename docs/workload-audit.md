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

## Pipeline refresh implementation

`PRAGMA refresh_pipeline('target_a', 'target_b')` now selects a fresh dependency graph per call, deduplicates targets, and executes selected MVs sequentially in topological order. Selection follows `openivm_cascade_refresh`: off selects targets only, upstream includes ancestors, downstream includes descendants, and both includes descendants followed by all their required ancestors. Independent ready nodes are ordered by name. No persistent pipeline registration or automatic ingestion-completion trigger is introduced.

The caller keeps ingestion paused. Unchanged nodes can be skipped. Invalid targets, cycles, and unbindable definitions are rejected before an autocommit run executes any node; failures during refresh or hooks stop the run, but earlier autocommit nodes may have committed. Native explicit transactions retain rollback support. Pipeline tests cover schema changes, dropped sources/views, new graph members, and hook failure/retry in the existing chained test files. DuckLake view drops now use the same staged-DDL approach as creation: lake-side removal followed by native metadata cleanup, without claiming cross-catalog atomicity.

Parallel execution, atomic publication of a complete DAG, continuous ingestion against pinned run snapshots, and MotherDuck deployment remain separate discussion items. See [pipeline refresh documentation](refresh/pipelines.md).

Pipeline validation: `make test` passed 11,460 assertions across 89 test cases, with one ICU-dependent test skipped. Compiled N-term SQL integration checks passed. The loadable extension built successfully, and CLI checks passed after reopening a persistent database and renaming a source column.

## Final workload sweep after publication and pipeline changes

Rechecked the email requirements against the graph builder, both refresh execution paths,
and real persisted databases. A deterministic stress workload ran twelve mixed-DML
batches per backend, closing and reopening the controller between batches. Each run
selected overlapping targets in a seven-MV DAG: left join, independent HAVING aggregate,
anti join, a diamond join, grouped cube, ordered top-k, and a terminal projection.
Every MV was compared to ordinary views over the source tables with `EXCEPT ALL` in
both directions: 168 comparisons across native DuckDB and DuckLake. Inputs included
duplicate tuples, NULL keys, inserts, deletes, and repeated updates before each run.
A smaller permanent version extends both existing chained-view test files.

| Mark's requirement | Current evidence and semantics |
| --- | --- |
| Batch source changes, then run the DAG | Both backends passed the mixed-DML/reopen sweep. Ingestion must finish before calling `refresh_pipeline`. |
| Discover dependencies, refresh each selected MV once after its parents | The graph comes from tracked source metadata; overlapping targets are deduplicated and topologically sorted. Use `both` to include missing co-parents of downstream nodes, or `upstream` with all desired terminal targets. |
| Parallel ready nodes | Not implemented. Pipeline nodes execute sequentially; DuckDB can parallelize individual queries. |
| Graph traversal with optional/missing edges | The sweep covers chained left and anti joins, including duplicates and NULL keys. The existing join suites also cover full outer joins. Arbitrary combinations still require checking their selected maintenance strategy. |
| Aggregations over materialized dimensions | The diamond and cube exercise shared dimensions and aggregation after joins. This does not establish performance for his undisclosed queries or data sizes. |
| Reproducible DuckLake snapshots | Snapshot-based source deltas exist. A whole-DAG atomic snapshot/publish boundary and continuous ingestion against a pinned run snapshot are not implemented. |
| Separate metadata/execution instance | A persistent native controller can attach the data catalogs; it need not be a separate server. DuckLake data and native metadata use staged transactions. |
| Older DuckDB databases | File compatibility is separate from extension ABI compatibility. Previously tested v1.1.2/v1.2.1 files can be attached by the matching v1.5.4 build; the extension itself was rejected by those older clients. See the build guide for the exact tested scope. |
| MotherDuck deployment | Still unverified. Local DuckDB/DuckLake coverage cannot establish that the managed service can load or execute this extension. |

The additional window sweep reproduced a NULL-only suffix bug in optional running-SUM
maintenance: an existing sum of 10 became NULL after two NULL inputs. The seed-combination
fix retains the prior sum, preserves all-NULL partitions, and has regression coverage
including conflicting changes in a subsequent batch. This does not change the window's
incremental maintenance classification.

The explicit-ROWS finding is now fixed by extending suffix maintenance. For
`ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, creation stores a hidden
row position from the same window ordering as the cumulative aggregate. Refresh
seeds each aggregate from its final stored position, materializes incoming peer
positions once, and uses those positions for both the data update and cascade
deltas. This preserves intermediate values for tied keys, including negative
sums, instead of treating peers as a RANGE frame.

For the optional suffix path (`openivm_running_window_incremental=true`),
equal-key ROWS arrivals follow the already stored peers. SQL does not prescribe
an order among otherwise tied rows; add a unique ordering key if the application
requires one. NULL placement is preserved explicitly. Mixed ROWS/RANGE windows
still revisit a partition when new rows join an existing RANGE peer group.
Backdated changes and mixed DML retain affected-partition incremental maintenance.
Reopening the database retains the positions; recreating a view creates new seed
state. Pre-change views without this state use their existing partition path.

Validation for this cleanup: the compiled N-term SQL integration check and full
suite passed (12,281 assertions in 89 cases, one ICU-dependent skip). After the final
SQL-rendering adjustment, nine focused parser, Spark-compilation, semi/anti, and
running-window cases passed 1,010 assertions. The final CLI NULL-suffix check was
bag-equal in both directions. The randomized reopen workload also passed all 168
comparisons on the cleanup binary. These numbers describe the preceding cleanup,
before the ROWS extension.

The ROWS extension subsequently passed the compiled N-term integration check and
full SQL suite: 12,371 assertions in 89 cases, with one ICU-dependent skip.
Coverage includes negative cumulative seeds, identical tied peers, NULL values
and NULL order keys, mixed ROWS/RANGE frames, AVG/COUNT/MIN/MAX, nested windows,
reopen, downstream refresh, and conflicting INSERT/UPDATE/DELETE batches. A real
CLI run executed exported SQL with forced cascade deltas and checked both the
visible result and emitted raw delta bags in both directions.

### ROWS suffix performance

Measured sequentially on the same Apple M1 host with four DuckDB threads,
three fresh in-memory databases per configuration and three append batches per
database. Each source has one partition and pairs of identical peers. Timings
cover `PRAGMA refresh` only; all 54 refreshes passed bidirectional `EXCEPT ALL`
checks. The baseline is commit `b61e8aca` with suffix optimization disabled,
using correct affected-partition maintenance. Its incorrect ROWS suffix path
is not used as a performance baseline.

| Initial rows | Appended rows per batch | Partition baseline (ms) | ROWS suffix (ms) |
|---|---:|---:|---:|
| 100,000 | 20 | 96 | 99 |
| 100,000 | 10,000 | 105 | 117 |
| 1,000,000 | 20 | 636 | 642 |

These median timings show no end-to-end speedup for this workload. The new
suffix path avoids reevaluating prior window frames but still scans stored rows
for seeds and publishes the visible result. The setting remains optional.
Reproduce with a saved baseline binary and the new build:

```sh
python3 benchmark/poc/running_rows_poc.py /path/to/baseline/duckdb build/release/duckdb --output /tmp/rows-benchmark
```

Use a new output directory. It retains each SQL workload, CLI log, and timing
results, and stops on SQL errors or bag mismatches.
