# Testing OpenIVM

## Run All Tests

```bash
make test
```

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

## Verification Pattern

Every refresh must be cross-checked with `EXCEPT ALL` in both directions to confirm
the materialized view matches a full recomputation:

```sql
-- No rows should be returned by either query:
SELECT * FROM mv EXCEPT ALL SELECT <mv_query> FROM base_tables;
SELECT <mv_query> FROM base_tables EXCEPT ALL SELECT * FROM mv;
```

This catches both missing rows and extra rows, including duplicates under bag semantics.

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
latest checked result was:

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

## Cost Model Benchmark Checks

Use the cost-model benchmark to compare automatic refresh decisions against forced
incremental and forced full refresh. Run it on representative data sizes and delta
ratios before changing the adaptive cost model.

```bash
build/release/extension/openivm/cost_model_benchmark \
    --scale 10 \
    --delta-pcts 0,1,5,10,20,50 \
    --reps 3 \
    --out /tmp/openivm_cost_model.csv
```

The benchmark writes a CSV with the model's decision, predicted cost, actual refresh
time, and the fastest forced strategy for each case.
