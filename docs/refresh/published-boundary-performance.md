# Visible-row publication performance

Measured on 2026-09-25 on an Apple M1 with 16 GiB RAM (arm64 macOS),
with four DuckDB threads, 100,000 initial source rows,
three fresh databases per scenario, and four refresh batches per database.
The baseline is the local binary immediately before visible-row publication,
including the transaction and after-hook fixes from this review.

Each batch performs INSERT, UPDATE, and DELETE before one pipeline refresh.
The changed-key count controls the DML range, not the exact number of changed
rows. Timing covers `PRAGMA refresh_pipeline` only; setup, DML and verification
are excluded. Every refreshed MV is checked with `EXCEPT ALL` in both directions.
Runs were sequential, without concurrent builds or tests. Values below are
medians of all 12 refresh timings; file sizes are medians after CHECKPOINT and
include source data, MV state, publication tables, retained deltas and metadata.

| Pipeline | Changed keys | Before (ms) | After (ms) | Latency ratio | Database MiB (before → after) |
|---|---:|---:|---:|---:|---:|
| chain | 20 | 83.0 | 172.0 | 2.07× | 5.01 → 6.76 |
| chain | 10,000 | 152.0 | 385.0 | 2.53× | 7.51 → 10.76 |
| fanout | 20 | 101.0 | 430.5 | 4.26× | 5.26 → 11.26 |
| fanout | 10,000 | 174.5 | 503.5 | 2.89× | 7.76 → 15.26 |
| having | 20 | 87.0 | 93.5 | 1.07× | 4.51 → 5.51 |
| having | 10,000 | 132.0 | 226.0 | 1.71× | 7.26 → 9.26 |
| topk | 20 | Incorrect results | 39.0 | — | — → 4.76 |
| topk | 10,000 | Incorrect results | 107.5 | — | — → 5.51 |

Publication has measurable latency and storage overhead. Affected-key publication
avoids comparing all groups where the raw maintenance delta provides a safe scope;
global LIMIT and paths without such a delta compare the visible relation.
The benchmark does not establish a speedup over full recomputation or cover
DuckLake performance. DuckLake is covered separately by correctness tests.

The pre-publication top-k chain failed bag equality (17 mismatched rows in the
first tested batch), so its timings are excluded from the comparison. The new
implementation passes all top-k bag checks.

Reproduce using the same host and run the binaries one after the other:

```sh
python3 benchmark/poc/published_boundary_poc.py /path/to/before/duckdb --shapes chain fanout having --output /tmp/publication-before
python3 benchmark/poc/published_boundary_poc.py build/release/duckdb --output /tmp/publication-after
```

Use fresh output directories. The script saves each SQL workload, CLI output,
DuckDB database, and `results.json`. It stops on SQL errors or bag mismatches.

## Follow-up: consolidate publication once and avoid unconsumed deltas

The review fixes replace the two bag differences with a single grouped signed
union of old and new rows. Changed rows stay consolidated during publication;
weights are expanded only when emitting a consumer's delta. Publication does not
retain deltas when no consumer exists, and cleanup removes history left after the
last consumer drops.

The same benchmark was rerun on the same machine with the same parameters and
without concurrent builds/tests. The baseline below is the preceding publication
run above, not a newly interleaved baseline. All 24 database runs passed every
bidirectional bag check.

| Pipeline | Changed keys | Previous publication (ms) | After review fixes (ms) | After database MiB |
|---|---:|---:|---:|---:|
| chain | 20 | 172.0 | 148.0 | 6.51 |
| chain | 10,000 | 385.0 | 326.5 | 10.26 |
| fanout | 20 | 430.5 | 361.0 | 10.26 |
| fanout | 10,000 | 503.5 | 424.0 | 11.51 |
| having | 20 | 93.5 | 90.5 | 5.26 |
| having | 10,000 | 226.0 | 228.5 | 8.76 |
| topk | 20 | 39.0 | 38.0 | 4.76 |
| topk | 10,000 | 107.5 | 103.0 | 5.26 |

Chain/fanout median refresh latency decreased by 14–16%. HAVING and top-k changed
much less; the large HAVING batch was 1.1% slower. The earlier isolated SQL
experiment's 40–48% improvement does not translate into that much end-to-end
improvement, because publication is only part of refresh execution.

## Stable hidden ordinal

The final implementation adds a stable BIGINT ordinal to publication. Ordered
views rank their visible rows; unordered views publish zero. This preserves
ordering across parent replacement, including sorts on non-projected aggregates.
Ordered publication compares the whole visible result because a changed sort
value can shift the ranks of otherwise unchanged rows.

A fresh before/after comparison used the same parameters and sequential runs
on the same machine, with no concurrent builds or tests. The baseline includes
the grouped publication difference and cleanup fixes above, but predates the
ordinal. All 48 database runs passed every bidirectional bag check.

| Pipeline | Changed keys | Before ordinal (ms) | With ordinal (ms) | Database MiB (before → after) |
|---|---:|---:|---:|---:|
| chain | 20 | 149.0 | 286.5 | 6.51 → 9.01 |
| chain | 10,000 | 326.5 | 341.0 | 10.26 → 12.26 |
| fanout | 20 | 360.0 | 331.0 | 10.26 → 10.26 |
| fanout | 10,000 | 423.0 | 397.0 | 11.51 → 13.01 |
| having | 20 | 88.5 | 94.0 | 5.26 → 5.26 |
| having | 10,000 | 202.0 | 206.5 | 8.76 → 8.76 |
| topk | 20 | 38.0 | 56.0 | 4.76 → 4.76 |
| topk | 10,000 | 101.0 | 119.0 | 5.26 → 5.26 |

The small-batch chain regression reproduced with the execution order reversed
(three additional databases per binary). Statement profiling also showed higher
publication comparison and deletion costs in that case. The ordinal therefore
has a measurable cost even for some unordered workloads; the final implementation
does not retain all of the earlier optimization's latency gains. Top-k latency
increased by 18–47% in this workload. These measurements establish correctness
and the observed tradeoff, not a general performance improvement.
