# Performance Methodology

This page describes how to measure OpenIVM so that results are comparable and
verifiable. Per-experiment controls for one published result are in
[Published Boundary Performance](../refresh/published-boundary-performance.md).

## What to control

| Factor | Practice |
|---|---|
| Hardware | Record CPU, memory, storage and OS; run on one idle host. |
| Threads | Fix DuckDB `threads` and report it. |
| Cold vs. warm | State which one is measured. Discard or report warm-up runs separately. |
| Databases | Start each run from a freshly created database, not one reused from an earlier run. |
| Run order | Run configurations sequentially, never concurrently on the same host. |
| Settings | Report every non-default `openivm_*` setting (for example `openivm_refresh_mode`, `openivm_adaptive_refresh`). |

## What to report

- Median and tail latency (for example p95) over repeated runs, not a single run.
- Delta size (rows inserted, deleted and updated since the last refresh) and base table size.
- The refresh breakdown from `openivm_refresh_profile`, to show where time goes.
- The comparison point: incremental refresh against full recompute of the same view.

## Correctness checks

Every measured refresh must be checked against the base query with bag equality, in
both directions:

```sql
(SELECT * FROM my_view EXCEPT ALL (<view query>))
UNION ALL
((<view query>) EXCEPT ALL SELECT * FROM my_view);
```

The result must be empty. Row counts or spot checks are not sufficient.
