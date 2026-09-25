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
