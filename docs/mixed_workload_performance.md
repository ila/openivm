# Mixed-mutation performance investigation

## Goal

Make the OpenIVM variants faster than full recomputation for mixed INSERT,
DELETE, and UPDATE workloads without regressing the existing append-only path.
The investigation starts with native DuckDB/DuckLake before applying any
generally useful result to OpenIVM Spark.

## Reproducible benchmark state

- Host: `ec2-18-225-179-87.us-east-2.compute.amazonaws.com`
- ivm-bench worktree: `/home/ubuntu/ivm-bench-sf50-lab`
- OpenIVM base commit: `8d2507309817e457f31f7427e52785f249874c78`
- OAT run: `ccbb1ea0-68d1-4a00-8aae-257cafc86164`
- Workload: TPC-DI SF50; batch 2 deletes 1%, batch 3 updates 1%; serial
- DuckDB baseline: `51.5928 / 91.0564 / 89.9112s`
- OpenIVM before this work: `168.4519 / 481.1531 / 840.2030s`
- Immutable source-committed checkpoints:
  - `mount/shape-lab/sf50/pre-batch2/duckdb-openivm`
  - `mount/shape-lab/sf50/pre-batch3/duckdb-openivm`
- SF30 validation passed all 49 models in all three batches before optimization.

The SF50 checkpoints are captured after source mutations commit and before any
OpenIVM model refresh. Replays clone them and never modify the originals.

## Root cause

`fact_market_history` is the dominant regression:

| Run | Batch 1 | Batch 2 | Batch 3 |
| --- | ---: | ---: | ---: |
| Full DuckDB | 12.208s | 12.981s | 12.862s |
| OpenIVM before optimization | 27.306s | 380.802s | 729.307s |

The old mixed LEFT JOIN path collected every affected `openivm_left_key`,
deleted every materialized row for those keys, and recomputed the full query for
those keys. At SF50, a 1% source deletion touches most company keys, so the
"affected" set is close to the whole view. The batch-2 generated SQL spent most
of its time rewriting the large `openivm_data_fact_market_history` DuckLake
table, not compiling SQL.

The same behavior was already visible at SF30: batch-2
`fact_market_history` took 203.447s, of which its affected-key DELETE took
184.994s; batch 3 added 451.814s, with 426.583s in that DELETE. The replacement
query itself was only 10.981s and 19.400s respectively.

DuckLake indexes are not an available fix: attempting to create one returns
`DuckLake does not support indexes`.

## Candidate that reached 25.9 seconds

Branch/worktree: `codex/mixed-mutation-performance` at
`/private/tmp/openivm-mixed-perf` locally and
`/home/ubuntu/openivm-mixed-perf` on EC2. It is intentionally uncommitted while
the shape is still being measured.

The candidate adds a generic mixed DuckLake LEFT JOIN projection hybrid:

1. Materialize the distinct affected preserved-side keys from the MV delta.
2. For a single equality outer join, compare per-key
   `old_count + primary_delta_count` with the current query's output count.
3. Keys whose counts agree cannot be missing a NULL-padding transition
   correction. Consolidate their signed delta by the complete output tuple.
4. Delete exactly `-multiplicity` matching materialized rows using `rowid` plus
   `ROW_NUMBER`, preserving bag semantics even when identical output tuples are
   duplicated.
5. Insert positive net multiplicities with `generate_series`.
6. Use the existing affected-key delete/recompute only for keys whose counts do
   not agree.

The eligibility proof is stored with the owning `left_join_key_source` lineage
entry. Legacy metadata defaults to the conservative path. Composite predicates
and multiple outer joins fall back because opposite match transitions can
cancel at a coarser key. Cross-system/Spark compilation is unchanged.

On an exact clone of the SF50 pre-batch-2 checkpoint, after replaying only the
real dependency chain through `daily_market`, `dim_security`, `dim_company`,
and `financials`, this changed `fact_market_history` from 380.802s to
**25.865s**. The generated SQL contained the hybrid markers and the runtime
profile was:

| Step | Time |
| --- | ---: |
| Generate MV delta | 2.353s |
| Build affected keys | 0.036s |
| Classify safe/transition keys | 1.705s |
| Consolidate safe tuple net | 0.323s |
| Rowid-ranked exact deletes | 11.479s |
| Positive inserts | 0.556s |
| Transition-key delete/reinsert | 0.866s |
| Drop temporary tables | 0.003s |
| Clear MV delta with `DELETE` | 7.211s |
| SQL generation and metadata overhead | about 1.0s |
| Total refresh | 25.583s profiled / 25.865s wall clock |

This is a 14.7x improvement over the old incremental path, but still slower
than the 12.981s full baseline, so it is not the final result.

## Subsequent exact SF50 experiments

The next candidates isolated the remaining costs:

- If all active deltas are on preserved sides of the LEFT JOIN, NULL-padding
  transitions are impossible. The exact tuple-net path skips the per-key
  cardinality query entirely.
- v2 used a compact `openivm_net_id` for ranked bag deletion and changed leaf
  DuckLake delta cleanup to `TRUNCATE`. It regressed to **39.099s**: the ranked
  delete rose to 24.393s and truncating the non-empty 275MB delta still took
  7.113s. The earlier 0.060s truncate measurement was only on an empty table and
  was not representative. Both changes were rejected.
- v3 retained the nullable-side-quiet shortcut, classified only partial bag
  deletions, directly deleted exhaustive tuples, and used row ranking only for
  genuinely partial duplicate bags. It passed a new duplicate-row case but was
  still **24.520s**. Its exact costs were 1.724s to classify partial tuples,
  8.960s to delete exhaustive tuples, 0.457s for partial ranked deletion, and
  8.388s to clear the persistent DuckLake delta.
- A storage microbenchmark showed why cleanup is expensive: dropping a 275MB
  DuckLake table took 0.046s, while truncating it took 7.389s. Creating the table
  from Parquet took 1.471s. A local persistent DuckDB table was worse to build
  (14.442s), although deleting it took 0.520s.
- v4 therefore materialized the leaf MV delta as a session-local temporary
  table. Views with downstream consumers retain the persistent cascade delta;
  Spark/cross-dialect compilation and append-only refreshes remain unchanged.
  The exact SF50 replay fell to **16.326s** wall clock / **15.582s** profiled.
  Delta generation was 1.640s, partial classification 1.711s, exhaustive target
  deletion 8.959s, partial deletion 0.465s, insertion 0.629s, and temporary
  cleanup 0.738s. This removes almost all persistent-delta write amplification
  but remains about 2.6s slower than the 12.981s full baseline.

The test extends `test/sql/ducklake_left_join.test`. It batches INSERT, DELETE,
and UPDATE before a single refresh; checks transition and nullable-side-quiet
paths; covers duplicate bags, composite predicates, and multiple LEFT JOINs;
and verifies `EXCEPT ALL` in both directions after every refresh. The latest
remote candidate passes 135 assertions.

An SF50 full `EXCEPT ALL` validation attempt was stopped after 503.654s because
it consumed about 115GB RAM and spilled 48GB, matching the previously observed
reason for running SF50 native validation off. The spill files were removed.
Correctness is covered by the real DuckLake SQL test above and by the earlier
SF30 validation in which all 49 models passed in every batch.

The remaining acceptance criteria are:

1. Reduce the remaining target-delete cost so exact SF50 batch 2 is faster than
   the 12.981s full model; v4 is currently 16.326s.
2. Exact SF50 batch-3 replay remains faster than the 12.862s full model.
3. Both directions of full bag equality are empty on the real model.
4. The full focused test, format, tidy, and broader relevant tests pass.
5. Append-only generated SQL and benchmark performance do not regress.

The first EC2 host powered off as scheduled. Work continued on
`ec2-3-19-209-217` with the preserved checkpoint and exact source state.

## Replacement-host results

The `fact_market_history` target contains 1,024,463,873 physical bag rows but
only 13,626,075 distinct tuples (75.18 copies per tuple on average, maximum
124). This explains why a full rebuild is unusually competitive and why target
deletion dominates IVM.

The exact consolidated output delta contains 592,872 tuples: 362,079 negative
tuples remove 27,164,875 rows and 230,793 positive tuples add 17,292,181 rows.
There are zero partial-delete tuples in this batch, and each negative tuple has
a unique `(sk_security_id, sk_date_id)` pair.

Delete-shape timings from identical hard-linked checkpoint clones:

- Existing 13-column direct tuple delete: 8.478s (8.959s in the full v4 profile).
- Two-column `(sk_security_id, sk_date_id)` delete: 7.583s, with zero old full
  tuple matches remaining.
- Generic rowid-map path: materialize exact matches 1.722s, classify partials
  0.015s, exhaustive rowid delete 7.800s, empty partial branch 0.249s; 9.786s
  total versus 11.135s for the current classify/delete subsystem.
- `DELETE ... USING` the rowid map was slower (8.290s delete; 10.278s subsystem)
  and is rejected.

The fastest complete experimental mutation program inlines the one-use LPTS
delta as a temporary view, consolidates it directly, deletes by the two-column
key, and inserts positive multiplicities. Timings were 1.498s delta+net,
7.145s delete, 0.561s insert, and **9.273s total**. This is 28.6% faster than
the 12.981s full DuckDB query on the same batch. The target was not touched on
the first failed inline attempt (missing explicit output aliases); the
corrected run completed and left zero negative full-tuple matches.

The inline-delta optimization is generic and semantics-preserving when the
delta relation has one consumer. The two-column key is not yet suitable for
production without persisted lineage/uniqueness evidence or a runtime
cardinality guard; DuckLake exposes no constraints for these tables, so it
must not be hard-coded for this model.

Production-shape follow-up:

- v5 implemented single-consumer delta inlining and passed the focused real
  DuckLake test. Exact SF50 fell from 16.326s to 15.082s; delta+net became
  1.499s and the 0.738s transient-delta cleanup disappeared.
- v6 replaced two target scans with a bag-correct rowid map. It materializes
  exact tuple matches once, classifies partial multiplicities from that small
  map, deletes exhaustive matches by rowid, and retains ranked deletion only
  for partial bags. The focused test passed 135 assertions. Exact SF50 was
  13.156s wall / 12.914s profiled, effectively tied with the 12.981s full
  baseline.
- Removing the repeated `openivm_net` value from the rowid map (v7) regressed
  DuckLake deletion and was reverted.
- Source qualification and snapshot-metadata assembly each performed one
  catalog lookup per source despite exact locations already being stored in
  `openivm_delta_tables`. Batched metadata reads reduced qualification from
  313ms to 1ms and assembly from 311ms to 9ms. The latest v9 profile is
  12.996s. Target DELETE varies by roughly 0.7s between identical clones, so
  candidate and full are still statistically tied; no performance win is
  claimed for the generic production path yet.

### Runtime exact-delete retry and SF100 scaling

The next generic candidate optimistically applies the consolidated negative
delta with one full-tuple `DELETE ... USING`. It reads DuckLake's changed-row
count and compares it with the exact negative multiplicity expected from the
net delta. If the counts differ, the surrounding DuckLake transaction is
rolled back and the already-compiled ranked-rowid bag-safe program is retried.
This avoids a preflight target scan while retaining exact bag semantics. The
focused DuckLake test now has 143 assertions and deterministically exercises
both the successful fast path and the rollback/retry path, with `EXCEPT ALL`
in both directions after each mixed refresh.

On the SF50 batch-2 checkpoint this path took **11.192s** wall clock, 13.8%
faster than the 12.981s full query. Its main steps were 1.453s to construct the
net delta, 8.534s to delete target tuples, and 0.500s to insert positive tuples.

The win does not scale to SF100. A clean SF100 checkpoint was missing the newer
`left_join_nullable` lineage entry, so the first attempted replay conservatively
used the legacy affected-key recompute and was discarded. After backfilling the
same nullable-side metadata that current MV creation emits, the generated SQL
was verified to use inline delta, exhaustive full-tuple deletion, and no rowid
map. A deterministic 1% deletion removed 545,184 of 54,542,950 `daily_market`
rows and 20,321,838 of 2,034,337,854 target bag rows. The isolated
`fact_market_history` refresh took **68.993s**:

| Step | SF50 | SF100 |
| --- | ---: | ---: |
| Construct net delta | 1.453s | 0.717s |
| Exact target delete | 8.534s | 67.555s |
| Positive insert | 0.500s | 0.001s |
| Total wall clock | 11.192s | 68.993s |

Saved SF100 full-query measurements range from 38.305s to 52.432s. A second
identical replay with the target files in the OS cache took **40.023s** total,
including 38.938s in the target DELETE. The first run had read 8.56GB from disk;
the repeat reported zero filesystem input. The SF100 result is therefore
cache-sensitive and roughly tied with full recomputation rather than a stable
win: 4.5% slower than the fastest saved full run, but 23.7% faster than the
slowest. The cold target DELETE scaled roughly 7.9x when the target doubled
from about 1.02B to 2.03B bag rows; the warm DELETE scaled 4.6x.

Replacing the DuckLake table with an anti-join CTAS was also tested as a way to
avoid delete-vector construction. It was stopped after 90.842s at 105.8GB peak
RSS without completing. Rewriting the surviving 2B-row bag is worse than the
exact delete and is rejected. The exact-delete path remains the best measured
incremental shape, but at SF100 its advantage depends on cache state.

## Other observations

- A 2-million-row synthetic production query was correct but did not establish
  a universal win: full recompute was about 0.19--0.23s and the first hybrid was
  about 0.34--0.35s. Runtime dispatch must therefore remain shape/data aware.
- `daily_market` is the second consistent mixed-workload regression and should
  be investigated after `fact_market_history` meets the acceptance criteria.
- `fact_trade` was among the slowest at SF100 but was already faster than full
  at SF30, so no speculative special case has been added for it.
