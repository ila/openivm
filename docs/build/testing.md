# Testing OpenIVM

## Run All Tests

```bash
make test
```

This runs every `test/sql/*.test` file with `build/release/test/unittest`, then the
compiled N-term SQL integration check
(`python3 test/integration/test_regular_nterm_compiled.py ./build/release/duckdb`).
Use `make test_debug` after a debug build. The DuckLake tests run `INSTALL ducklake`,
so they need network access unless the extension is already installed.

## Run a Single Test

```bash
build/release/test/unittest "test/sql/inner_join.test"
```

## Test Files

All tests live in `test/sql/*.test` using DuckDB's SQLLogicTest format.

| Test file | Coverage |
|---|---|
| `aggregate.test` | SUM, COUNT, AVG, MIN/MAX, STDDEV/VARIANCE aggregations |
| `projection.test` | Column projection, expression projection, bag-delete consolidation |
| `filter.test` | WHERE clause filtering |
| `inner_join.test` | Inner joins, cross joins, arbitrary predicates, multi-table joins |
| `left_join.test` | LEFT JOIN, RIGHT JOIN, mixed INNER+LEFT, NULL keys |
| `full_outer_join.test` | FULL OUTER JOIN projection and aggregate maintenance |
| `semi_anti_join.test` | SEMI JOIN, ANTI JOIN, EXISTS, NOT EXISTS aux-state maintenance |
| `lateral.test` | LATERAL / DELIM_JOIN refresh, scalar correlated subqueries |
| `union.test` | UNION ALL views |
| `chained.test` | Chained (multi-level) materialized views |
| `window.test` | Partition-level window recompute |
| `incremental_checker.test` | Query constraint validation |
| `pipeline.test` | End-to-end refresh pipeline |
| `metadata.test` | Catalog and metadata tables |
| `parser.test` | SQL parsing and rewriting |
| `insert_rule.test` | Insert rules and delta generation |
| `auto_refresh.test` | Automatic refresh triggers |
| `list.test` | LIST aggregate support |
| `distinct.test` | `SELECT DISTINCT` and DISTINCT aux state |
| `cte.test` | CTE-based view definitions |
| `schema_evolution.test` | `ALTER TABLE` delta sync, referenced-column blocking and rename rewrites |
| `concurrency.test` | MV reads, DML, and refresh under concurrency |
| `transactional_lifecycle.test` | MV lifecycle and refresh inside caller transactions, rollback |
| `refresh_hooks.test` | Refresh hooks |
| `delta_model.test` | CREATE-time delta model metadata |
| `compile_refresh.test` | `openivm_compile_with_facts` compile-only refresh |
| `time_travel.test`, `time_travel_ducklake.test` | Time-travel pins |
| `ducklake_*.test` | The same operator families over DuckLake sources |

## Verification Pattern

Every refresh must be cross-checked with `EXCEPT ALL` in both directions to confirm
the materialized view matches a full recomputation:

```sql
-- No rows should be returned by either query:
SELECT * FROM mv EXCEPT ALL SELECT <mv_query> FROM base_tables;
SELECT <mv_query> FROM base_tables EXCEPT ALL SELECT * FROM mv;
```

This catches both missing rows and extra rows, including duplicates under bag semantics.

Two rules follow from this:

- **Never hide a correctness bug.** Don't weaken an assertion, and don't demote a view to `FULL_REFRESH` or tune the
  cost model to route around a wrong incremental result. The cost model only chooses between strategies that are
  already correct; it must never compensate for classifier uncertainty.
- **Test more than delta row counts.** A test that only checks how many rows landed in a delta table does not prove
  the view is right; every behavioral MV test should end with the bidirectional check above.

## Rewriter Benchmark Checks

For TPCC coverage, run the rewriter benchmark against the TPCC query directory:

```bash
build/release/extension/openivm/rewriter_benchmark \
    --workload tpcc \
    --queries benchmark/queries/tpcc \
    --db /tmp/openivm_tpcc.db \
    --out /tmp/openivm_tpcc.csv \
    --scale 1 \
    --timeout 120
```

The current TPCC metadata is expected to match OpenIVM's actual classification. The
latest recorded result (2026-05-05, when the directory held 2386 queries; it now holds
2505, so rerun before relying on these numbers) was:

| Metric | Result |
|---|---:|
| Total queries | 2386 |
| MV creation OK | 2386 |
| Refresh OK | 2386 |
| Correct | 2386 |
| Incremental | 2273 |
| Full refresh | 113 |
| Crashed | 0 |
| Metadata mismatches | 0 |
