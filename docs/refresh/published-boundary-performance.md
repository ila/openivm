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

## Follow-up: rank only the selected top-k rows

Bounded ordered publication now applies LIMIT/OFFSET before computing the hidden
ordinal. The window therefore ranks only the selected rows (1,000 in this
benchmark), rather than every raw group (100,000 here). Unbounded ORDER BY and
OFFSET-only views retain the direct form to avoid adding a redundant sort.
The generated publication query is stored at MV creation or replacement.

Publication binding extraction also no longer invokes the full plan classifier:
it collects only projection and aggregate bindings along the unary output path.
This removes up to two full plan-facts walks when creating an ordered MV. These
creation-time savings are outside the refresh timing below.

A fresh sequential comparison against the ordinal implementation (`9cf2451c`) used the same
benchmark parameters, without concurrent builds or tests. All 48 database runs
passed every bidirectional bag check.

| Pipeline | Changed keys | Before (ms) | After (ms) | Database MiB (before → after) |
|---|---:|---:|---:|---:|
| chain | 20 | 287.0 | 287.0 | 9.01 → 9.01 |
| chain | 10,000 | 340.0 | 338.0 | 12.01 → 12.26 |
| fanout | 20 | 332.0 | 329.5 | 10.26 → 10.26 |
| fanout | 10,000 | 396.5 | 397.0 | 12.76 → 13.01 |
| having | 20 | 90.0 | 94.0 | 5.26 → 5.26 |
| having | 10,000 | 206.0 | 206.0 | 8.76 → 8.76 |
| topk | 20 | 56.0 | 39.0 | 4.76 → 4.76 |
| topk | 10,000 | 118.5 | 102.0 | 5.26 → 5.26 |

The top-k improvement comes from reducing window input while preserving the
stable publication boundary. It does not remove the small-batch unordered-chain
overhead identified above. Database sizes and timings include the entire pipeline;
they do not establish an improvement for every workload or storage backend.

## Final cleanup pass — 2026-09-26

Compared `93d89448` with the subsequent cleanup using the same 100,000 source rows,
four threads, 20/10,000 changed keys, and four conflicting-DML batches per fresh
database. This pass used two fresh databases per shape/size, with binaries run
sequentially after builds and tests finished. Every result was checked as a bag.

| Shape | Changed keys | Before median (ms) | After median (ms) |
| --- | ---: | ---: | ---: |
| Chain | 20 | 602 | 615.5 |
| Chain | 10,000 | 706 | 716.5 |
| Top-k | 20 | 99 | 97 |
| Top-k | 10,000 | 261 | 225.5 |

A separate running-window compilation trial (three fresh sessions, 30 compilations
per session) measured 20 ms before and 19.5 ms after. These results do not establish
a general speedup. In particular, the large top-k difference needs repeat measurement
before attributing it to this cleanup. Absolute timings in this session are also
higher than the earlier publication experiments; compare paired runs, not sessions.

The code improvements are structural: output bindings are obtained once before
classifier column loops, aggregate-binding resolution shares the group resolver,
the cost model reuses its normal-equation submatrix instead of scanning history
again, and one parsed SQL tree replaces the running-window string/regex parser.
DuckLake snapshot-source resolution and optional metadata reads also share helpers.
The residual persisted small-chain overhead remains a profiling target.
