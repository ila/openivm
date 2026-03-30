# OpenIVM Production Readiness TODO

## P0: Cost Model Audit

- [ ] **Review cost model heuristics.** The current model (`src/upsert/openivm_cost_model.cpp`) uses `COUNT(*)` queries for cardinality at refresh time — expensive and blocks the refresh. Consider caching or using DuckDB's statistics instead.
- [ ] **Join cost is oversimplified.** `2^(N-1) * total_base_scan` ignores join selectivity, index availability, and join type (LEFT JOIN partial recompute is much cheaper than inclusion-exclusion).
- [ ] **Recompute cost ignores processing.** `total_base_scan + mv_card` doesn't account for join or aggregate processing overhead during full recomputation.
- [ ] **Fanout estimate is naive.** `mv_card / base_card` assumes uniform distribution — heavily skewed joins will produce wildly wrong estimates.
- [ ] **No filter selectivity.** A view with `WHERE price > 1000` that filters 99% of rows still gets the same cost as an unfiltered view.
- [ ] **LEFT JOIN partial recompute cost missing.** The cost model doesn't distinguish between counting-based upsert and partial recompute.
- [ ] **Validate with real workloads.** Run the cost model on TPC-H derived views and compare its predictions against measured refresh times.

## P1: Correctness & Robustness

- [ ] **Transaction isolation during refresh.** The refresh opens a separate `Connection` — verify what snapshot it reads from and whether concurrent DML during refresh can produce inconsistent results.
- [ ] **Error recovery.** If refresh fails mid-way (e.g., out of memory during MERGE), is the MV left in a consistent state? Consider wrapping the full refresh in a single transaction.
- [ ] **Large delta handling.** When delta rows exceed the base table size, IVM is likely slower than full recompute. The cost model should catch this, but verify the threshold experimentally.
- [ ] **DROP MATERIALIZED VIEW.** Not implemented. Needs to clean up: MV table, all delta tables, delta view table, `_duckdb_ivm_views` entry, `_duckdb_ivm_delta_tables` entries, ART index.
- [ ] **NULL group keys edge cases.** `IS NOT DISTINCT FROM` in MERGE handles NULLs, but audit the full path (delta computation → consolidation → MERGE) for correctness with composite NULL keys.

## P2: Documentation

- [ ] **Performance evaluation methodology.** Write a doc explaining how to measure IVM refresh latency, what variables to control, and what metrics to report.
- [ ] **Known limitations page.** Consolidate all operator/feature limitations into a single `docs/limitations.md`.
- [ ] **Cross-system usage guide.** The paper describes cross-system IVM (DuckDB→PostgreSQL). Document how to set this up.

## P3: Performance Evaluation

- [ ] **IVM vs full recompute latency curves.** Measure refresh time at delta sizes 1%, 5%, 10%, 25%, 50% of base table for each operator type.
- [ ] **Operator breakdown.** Profile where time is spent: delta scan, LPTS compilation, SQL generation, upsert execution. Identify bottlenecks.
- [ ] **Chained MV overhead.** Measure the cost of companion row generation for 2-level and 3-level chains.
- [ ] **Comparison with pg_ivm.** Run equivalent queries on PostgreSQL with pg_ivm to establish a baseline.
- [ ] **Scalability.** Test with base tables at 1M, 10M, 100M rows. Identify at what scale IVM breaks even with full recompute.

## P4: Benchmarking Suite

The current suite covers projection, filter, grouped aggregate, 2-way join, 3-way join at 1K–1M rows with 1%–50% delta ratios.

- [ ] **Add missing operators.** LEFT JOIN, ungrouped aggregates, UNION ALL, chained MVs, AVG, MIN/MAX, DISTINCT.
- [ ] **Mixed DML workload.** INSERT + DELETE + UPDATE interleaved in the same refresh cycle to stress delta consolidation.
- [ ] **TPC-H derived queries.** Q1 (grouped aggregates + filter), Q3 (3-way join + aggregate), Q5 (multi-join + aggregate), Q6 (filter + aggregate) as materialized views with incremental refresh after base table inserts.
- [ ] **Latency distribution.** Report p50/p95/p99 refresh times, not just median, to catch tail latency from large deltas or skewed groups.
- [ ] **CI integration.** Run a lightweight benchmark subset on every PR to catch performance regressions.

## P5: Features

- [ ] **Incremental LEFT JOIN aggregates.** Currently uses group recompute. Explore whether NULL-aware MERGE arithmetic can handle the `SUM(NULL) != SUM(0)` problem.
- [ ] **Window functions.** Currently `FULL_REFRESH`. Investigate incremental maintenance for `PARTITION BY + simple aggregate` windows (recompute only affected partitions).
- [ ] **Higher-order IVM.** For join-heavy queries (3+ tables), DBToaster-style auxiliary maps eliminate joins at update time. Evaluate whether the space-time tradeoff is worthwhile for OpenIVM's SQL-to-SQL model.
- [ ] **Automatic refresh.** Trigger-based immediate refresh on DML, similar to pg_ivm. Currently requires explicit `PRAGMA ivm()`.
- [ ] **DISTINCT aggregates.** `COUNT(DISTINCT x)` is not incrementally maintained. Explore approximate (HyperLogLog) or exact (auxiliary set per group) approaches.
