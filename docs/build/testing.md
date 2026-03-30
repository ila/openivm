# Testing OpenIVM

## Run All Tests

```bash
make test
```

## Run a Single Test

```bash
build/release/test/unittest "test/sql/ivm_join.test"
```

## Test Files

All tests live in `test/sql/*.test` using DuckDB's SQLLogicTest format.

| Test file | Coverage |
|---|---|
| `ivm_aggregate.test` | SUM, COUNT, AVG, MIN/MAX aggregations |
| `ivm_projection.test` | Column projection, expression projection |
| `ivm_filter.test` | WHERE clause filtering |
| `ivm_join.test` | Inner joins, multi-table joins |
| `ivm_union.test` | UNION ALL views |
| `ivm_chained.test` | Chained (multi-level) materialized views |
| `ivm_checker.test` | Query constraint validation |
| `ivm_pipeline.test` | End-to-end refresh pipeline |
| `ivm_metadata.test` | Catalog and metadata tables |
| `ivm_parser.test` | SQL parsing and rewriting |
| `ivm_insert_rule.test` | Insert rules and delta generation |
| `ivm_auto_refresh.test` | Automatic refresh triggers |
| `ivm_list.test` | LIST aggregate support |

## Verification Pattern

Every refresh must be cross-checked with `EXCEPT ALL` in both directions to confirm
the materialized view matches a full recomputation:

```sql
-- No rows should be returned by either query:
SELECT * FROM mv EXCEPT ALL SELECT <mv_query> FROM base_tables;
SELECT <mv_query> FROM base_tables EXCEPT ALL SELECT * FROM mv;
```

This catches both missing rows and extra rows, including duplicates under bag semantics.
