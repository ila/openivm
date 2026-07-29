# LEFT JOIN work — handoff instructions

Working notes for continuing the LEFT JOIN correctness and performance work on branch `refactor`.
Everything described here is committed and pushed. Read §9 (methodology) before running any benchmark
and §11 (traps) before believing any measurement — several conclusions in this project were wrong the
first time for reasons documented there.

Baseline at time of writing: `a11f2141`, full suite **18372 assertions / 164 test cases**, green.

---

## 1. Goal

The work started as "make inner and left joins faster in the rewriter benchmark" and split into two
tracks that turned out to be very different in difficulty:

1. **Correctness.** LEFT JOIN aggregate maintenance had multiple real bugs — silent wrong results and
   hard refresh failures. All the ones found are fixed (§5). This track went well.
2. **Performance.** Make incremental refresh beat full recompute for LEFT JOIN views. This track has
   produced a clear characterisation of *where* the cost is (§6) but **no validated optimisation
   lever** — five candidates were tried and all refuted on clean data (§7).

Priority context from the user: **DuckLake matters most** ("most customers use ducklake"); some loss on
standard (non-DuckLake) IVM is acceptable. Do **not** work on the cost model (`openivm_adaptive_refresh`)
— explicitly out of scope by user instruction.

---

## 2. Theory: what the code implements

| piece | what it is | where |
|---|---|---|
| primary delta | join delta rule — 2^N−1 inclusion-exclusion for regular tables, N-term telescoping for DuckLake | `src/delta/operators/join.cpp`, `src/delta/operators/ducklake_join.cpp` |
| **secondary delta** | **Larson & Zhou** outer-join correction: when a key's match count crosses zero, re-add/retract the NULL-padded row so preserved-side aggregates stay correct | `BuildLeftJoinSecondaryDeltaSQL` in `src/core/parser_plan_helpers.cpp` |
| FULL OUTER merge | Zhang & Larson | gated by `openivm_full_outer_merge` |

The secondary delta is generated **once at CREATE time**, stored in `openivm_views.leftjoin_secondary_meta_json`,
and prepended to the MERGE at refresh. It is emitted for **every** LEFT JOIN level, including a single
left join (see §5.7 for why that matters).

---

## 3. File map

**Secondary-delta generation (CREATE time)**
- `src/core/parser_plan_helpers.cpp`
  - `BuildLeftJoinSecondaryDeltaSQL` — loops all LEFT JOIN levels, concatenates fragments, returns
    index-aligned CSV identity lists
  - `BuildLeftJoinSecondaryForLevel` — the SQL for one level
  - `ComputeNullTablesForLevel` — transitive NULL closure (which tables read NULL in this level's
    null-padded row); decides each aggregate's contribution
  - `OuterJoinAggregateNeedsRecompute` — **the gate that forces computed aggregates onto
    GROUP_RECOMPUTE**; the untouched perf headroom (§10.3)
- `src/core/parser.cpp` — calls the generator inside a `BeginTransaction`/`Rollback` (LPTS needs a txn),
  stores metadata, passes `internal_catalog_prefix`
- `src/core/refresh_metadata.{hpp,cpp}` — `LeftJoinSecondaryMeta` {sql, preserved_cols_csv, inner_table,
  inner_key, pres_table, pres_key}; the last four are **CSV lists, one entry per level**
- `src/include/core/openivm_constants.hpp` — `LJSEC_INNER_DELTA_PREFIX`, `LJSEC_PRES_DELTA_PREFIX`,
  `LJSEC_PLACEHOLDER_SUFFIX`

**Refresh time**
- `src/upsert/refresh_sql.cpp`
  - `GenerateRefreshSQL` — assembles the whole program; AGGREGATE_GROUP dispatch is where the secondary
    is resolved and prepended
  - placeholder resolution per level: regular table → `openivm_delta_<t>` filtered by timestamp;
    DuckLake → `ducklake_table_insertions/deletions(cat,sch,tbl,last+1,cur)`. Both yield `(__k, __m)`
  - `has_minmax` composition (line ~809) — this is how `openivm_left_join_merge=false` routes to
    group-recompute
- `src/upsert/refresh_compiler.cpp`
  - `CompileAggregateGroups` — the MERGE, the match_count gating, the empty-group cleanup,
    `out_used_group_recompute`
- `src/upsert/refresh_helpers.cpp` — `BuildAffectedKeyRefreshSQL` (affected-group DELETE+INSERT, plus
  the upsert form), `BuildRecomputeQuery`
- `src/core/sql_utils.cpp` — `BuildFullRecomputeSQL`, `DuckLakeTableFunction`
- `src/delta/delta_helpers.cpp` — `CreateDeltaGetNode` (note: reuses the old GET's `table_index`)

**Tests added by this work**
```
test/sql/left_join_pipeline_secondary_delta.test        3-table pipeline, batched mixed DML
test/sql/left_join_deep_chain_secondary.test            4- and 5-table chains
test/sql/left_join_group_by_join_key.test               GROUP BY column == join key (star schema)
test/sql/left_join_emptied_group.test                   group lifecycle: emptied vs NULL-padded
test/sql/ducklake_left_join_pipeline_secondary.test     the same pipeline over DuckLake sources
test/sql/group_recompute_persistent_unique_index.test   needs `load` + `restart` (file-backed DB)
```

---

## 4. Commits (all on `refactor`, pushed)

```
a11f2141  Drop LEFT JOIN aggregate groups once their last preserved-side row is deleted
44825c29  Fix secondary delta when a GROUP BY column is also the join key
f8d152dc  Emit the LEFT JOIN secondary delta at every level, fixing 4+ table chains
47e37899  Fix LEFT JOIN pipeline secondary delta over DuckLake sources
69df68f3  Fix the same duplicate-key failure on the full-recompute path
85479138  Fix spurious duplicate-key failure in group-recompute on persistent databases
de46b291  Scope the LEFT JOIN secondary delta to the MERGE path only
76de27fb  Add LEFT JOIN pipeline secondary-delta regression test; harden the generator
287b17d9  Fix LEFT JOIN pipeline secondary-delta bugs under combined insert+delete batches
```
Base before this work: `7697928c`.

---

## 5. Bugs fixed

### 5.1 Secondary delta fired for brand-new preserved rows (`287b17d9`)
The correction assumed a dangling row was already materialised. If the preserved row was itself new in
the same batch (order inserted *and* gaining its first line), no dangling row ever existed, so the
correction subtracted a row that was never there. Fixed by gating on `__old_pres_count > 0`
(current preserved count minus that table's own net delta).

### 5.2 Guard excluded both transition directions (`287b17d9`)
`GuardKeptOuterJoinsForMask` anti-joins the mask-driven side against keys whose match count crosses
zero, to avoid double-counting with the higher-order inclusion-exclusion term. It excluded **both**
directions. Only the **downward** (delete) direction is a phantom-row risk; the upward (insert)
direction is the term's sole correct contribution and must survive. Filter is now `old>0 AND new=0`.

### 5.3 MERGE gated preserved-side COUNTs by inner-side match_count (`287b17d9`)
Fixed via `preserved_side_cols` + `is_preserved_side()`, treated like `count_star`.

### 5.4 Secondary emitted on the group-recompute path (`de46b291`)
`CompileAggregateGroups` now reports which path it took (`out_used_group_recompute`) and the secondary
is only prepended when the delta-arithmetic MERGE was actually emitted. **This is scoping, not a bug
fix** — no wrong-result case was attributable to it, and the added test passes with or without it. Said
so in the commit message deliberately.

### 5.5 Duplicate-key failure on persistent DBs (`85479138`, `69df68f3`)
`PRAGMA refresh` failed outright with `Duplicate key ... violates unique constraint`, leaving the MV
stale. **Root cause is upstream in DuckDB**: an on-disk unique index keeps deleted keys for constraint
checking within a transaction. Minimal repro, no openivm involved:
```sql
-- session 1, ON DISK
CREATE TABLE data(k INT, v INT);
INSERT INTO data SELECT i, i*10 FROM range(25) t(i);
CREATE UNIQUE INDEX didx ON data(k);
-- session 2, reopen the file
BEGIN;
DELETE FROM data;                                     -- count(*) now returns 0
INSERT INTO data SELECT i, i*99 FROM range(25) t(i);  -- Duplicate key "k: 0"
COMMIT;
```
openivm's group-recompute deletes and re-inserts every surviving group in one transaction, so it trips
this on exactly the views `parser.cpp` gives a unique index (AGGREGATE_GROUP / AGGREGATE_HAVING).
Fixed in **two** places — the group-recompute branch **and** `BuildFullRecomputeSQL` (reached via the
interrupted-refresh recovery route). Both now: materialise once into a temp table, DELETE only vanished
keys, `INSERT OR REPLACE` the survivors.
- **Worth reporting upstream to DuckDB** — the 6-line repro above is self-contained.
- **The in-memory test suite cannot catch this.** The regression test uses `load` + `restart`.

### 5.6 DuckLake pipelines unmaintainable (`47e37899`)
A 3-table LEFT JOIN chain over DuckLake either produced **silently wrong** results (without the
secondary: an order losing its last line dropped out of `COUNT(o.oid)`) or **hard-failed** (with it).
Two causes: the INSERT target wasn't catalog-qualified (`dl.main.openivm_delta_<view>`), and more
fundamentally the secondary read `openivm_delta_<source>` tables that **DuckLake does not have** — it
tracks changes by snapshot. Now the row sources are placeholders resolved at refresh per backend.
> **Gotcha:** `openivm_delta_tables.table_name` keys **DuckLake sources by their BARE table name**
> (`lineitem`) but **regular sources by `openivm_delta_<table>`**. Probing with the wrong key makes
> `IsDuckLakeTable()` silently return false. This cost a full debug cycle.

### 5.7 Secondary only at the outermost level (`f8d152dc`)
A chain of N tables has N−1 LEFT JOIN levels and a vanishing match at **any** of them can strand a
preserved-side count. Only the outermost was corrected, so 3-table chains were right and 4+ were wrong
(`cust ⟕ ord ⟕ line ⟕ supp`: deleting an order's last line dropped it from `COUNT(o.oid)`). Wrong on
both engines and in all 18 SF50 benchmark scenarios. Contributions need a **transitive NULL closure**,
not a comparison against one inner table — when a line disappears its supplier disappears too, and
anything joined past that. If any level is unsupported the whole correction is skipped: a partial
correction double-counts the levels it does cover.

### 5.8 Binder error when GROUP BY column == join key (`44825c29`)
```
Binder Error: Values list "X" does not have a column named "__k"
```
Refresh failed outright for any LJ pipeline aggregate grouping by a column the deepest join also joins
on — a common star-schema shape. One output column can hold one alias, so the group alias `__g<i>`
overwrote `__k` while the SQL still referenced `X."__k"`. Now the alias the key actually received is
tracked. Reproduces at the minimum pipeline: 3 tables, 2 LEFT JOINs.

### 5.9 Emptied groups never deleted (`a11f2141`)
Deleting a group's last preserved-side row left a zeroed row (`B | 0 | 0`) where recompute drops it.
Reproduced with a **single** left join. Plain and INNER aggregates were fine; only `openivm_match_count`
views. Needed **three** parts, each verified load-bearing:
1. **Emit the secondary for a single LEFT JOIN too.** The old comment claimed the primary delta emits
   the null-padded reappearance there — **it does not**. Verified by extracting the delta INSERT and
   reading the delta table: for a customer losing both orders it held exactly one row,
   `(300, cnt=2, count_star=2, match_count=2, mult=-1)` — a retraction only. The MERGE's gating then
   *synthesises* the visible NULL/0, leaving `count_star` at 0 for a group that still has an output row.
2. **Match hidden `openivm_count_star` by NAME in the MERGE** — it has no `col_agg_type` entry, so
   `agg_type == "count_star"` never matched it and it fell through to the gated branch that zeroes it.
3. **Enable the cleanup for LJ views, anchored on a count_star-type column** (skip when no anchor).

> An earlier attempt did parts 2+3 without part 1 and **deleted valid NULL-padded groups** — reverted.
> Parts 2 and 3 are only safe once part 1 makes `count_star` a true output-row count.

---

## 6. Performance: what we actually know

All numbers below are from a **verified-idle machine** (load ≈ 0, no containers) unless flagged.
Earlier numbers in the session history are void — see §11.1.

### 6.1 DuckLake SF50, LEFT JOIN **aggregates** (ratio = recompute ÷ incremental; >1 = incremental wins)

| shape | 0.001% | 0.1% | 1% | 5% | recompute |
|---|---|---|---|---|---|
| 2-table ⟕, 25 groups | 0.055s **25.9×** | 0.057s **25.8×** | 0.037s **39.4×** | 0.055s **27.7×** | ~1.4–1.5s |
| 3-table pipeline, 25 groups | 4.10s 0.3× | 3.96s 0.4× | 4.43s 0.3× | 7.90s 0.2× | ~1.46s |
| 3-table pipeline, 7.5M groups | 4.15s 0.3× | 4.81s 0.3× | 8.15s 0.2× | 17.8s 0.1× | ~2.0s |

Multi-table deltas at 1% (3-table shape): orders only 4.99s, orders+lineitem 6.32s, all three 5.87s.
insert-only vs delete-only at 0.1% (3-table): 3.54s vs 4.01s — only ~10% apart, the floor dominates.

### 6.2 Standard IVM SF50, identical shapes

| shape | 0.001% | ins-only 0.1% | del-only 0.1% | 1% | 5% |
|---|---|---|---|---|---|
| 2-table | **164×** | **168×** | **203×** | **127×** | **180×** |
| 3-table | **4.7×** | **10.5×** | **2.8×** | **4.6×** | 0.5× |
| fine (7.5M groups) | **3.9×** | **9.4×** | **1.7×** | 1.0× | 0.2× |

**Standard IVM beats DuckLake by ~6× on the identical 3-table shape** (0.708s vs 4.43s at 1%).
Insert-only is 3.9× faster than delete-only on standard (0.112s vs 0.433s) — the insert-only fast paths
work; on DuckLake the floor swamps them.

### 6.3 The DuckLake aggregate floor — the central finding

Refresh cost for the 3-table DuckLake pipeline is **essentially fixed**, not delta-proportional:

| delta | refresh |
|---|---|
| 0 rows | 0.040s (empty-delta skip) |
| **1 row** | **3.893s** |
| 30k rows (0.001%) | 4.10s |
| 3M rows (1%) | 4.26s |

One row costs 3.9s; three million adds ~0.4s. The user's read — "the 4s are just compilation time" —
is consistent with everything measured. Note `generate_refresh_sql` is only **41 ms**, so it is *not*
openivm's SQL-string generation; it is inside statement execution (binding a ~170-line multi-CTE
program with ~10 `ducklake_table_*` calls, each resolving snapshots and parquet file lists).

**The floor grows near-exponentially with join count** (DuckLake aggregate, 1-row delta):

| tables | 3 | 4 | 5 | 6 | 8 | 11 |
|---|---|---|---|---|---|---|
| refresh | 3.2s | 8.2s | 13.9s | 24.7s | **100.9s** | **hard failure** |

At 11 tables: `Binder Error: Max expression depth limit of 10000 exceeded`. See §10.1.

### 6.4 LEFT JOIN **projections** (no GROUP BY) — Raki's real shape

Floors are *tiny* here, so §6.3 is an **aggregate-path pathology, not a general join-count problem**:

| joins | SF1 floor (1 row) | SF50 floor |
|---|---|---|
| 2 | 0.031s | — |
| 5 | 0.043s | — |
| 10 | 0.069s | 0.12–0.16s |

SF1, 10 joins: recompute 0.448s vs incremental 0.739s → **loses 1.65×** (dimensions are only 10k rows,
so DuckDB re-joins 6M×10 trivially).

SF50, 10 joins (**taken at load 2.42 — indicative only, re-measure**):

| delta | recompute | incremental | ratio |
|---|---|---|---|
| 0.01% | 124.0s | 54.0s | **2.3×** |
| 0.1% | 125.9s | 125.1s | 1.0× |
| 1% | 215.0s | 177.7s | 1.2× |

Direction matches the prediction that incremental wins at scale for projections because recompute
scales with fact size while incremental does not.

### 6.5 Where the time goes (EXPLAIN ANALYZE, real generated statement)

DuckLake 3-table primary delta = 1.83s compute + ~0.4s write. Operator CPU totals: `HASH_JOIN` 7.46s,
`HASH_GROUP_BY` 1.31s, everything else <0.05s. Dominant single operator:
```
HASH_JOIN  Join Type: RIGHT   o_custkey = c_custkey
77,500,111 rows    6.75s
```
i.e. the full `customer ⟕ orders` intermediate built to serve a 2.8M-row delta. **Attacking this does
not help** — see §7.5.

---

## 7. What we tried that did NOT work

Five refuted candidates. Do not re-attempt without new evidence.

### 7.1 Physical intermediate tables (higher-order IVM)
Persistent intermediate tables to replace 2^N−1 inclusion-exclusion. Lost to **both** the current
approach and full recompute at SF25/50/100, for shallow (N=3) and deep (N=5) chains. Abandoned.

### 7.2 Delta-key-restricted base scans, standard tables SF25
`orders` restricted to delta keys: 0.314s → 0.337s. **No win** — in-memory hash joins are already fast.

### 7.3 Secondary-delta CTE rewrite
Replacing the correlated `LATERAL` subqueries with pre-aggregated CTEs restricted to delta keys.
**~40% slower**, order-controlled (first position 0.824 vs 1.186; second 0.689 vs 0.953). DuckDB already
decorrelates them better than the hand-written version. Reverted. The correlated form in the code is
**deliberate** — there is a comment saying so.

### 7.4 DuckLake delta-scan CSE
`ducklake_table_deletions` costs ~1.0s per scan (insertions ~0.04s) and the program calls each 5×, so
hoisting into a temp table looked like several seconds of free win. Implemented
`HoistRepeatedDuckLakeDeltaScans`. **No effect at all** (4.20s vs 4.28s) because (a) the `!cross_system`
gate excluded exactly the DuckLake case — `cross_system = (catalog_name != current_database)` is true for
a DuckLake MV — and that gate is legitimate (DuckDB forbids writing two attached DBs in one transaction,
so temps are not safely available there), and (b) the profile had already shown delta scans are *not*
hot. Reverted.

### 7.5 Delta-key restriction on DuckLake — the "10.4× win" that wasn't
Measured 34.5s → 3.3s with identical results, and I built a whole case around it. **It was a contention
artifact.** Re-measured idle: baseline **1.80s**, restricted **2.51s** — **40% slower**. Under contention
the baseline (memory/CPU heavy) degraded ~19× while the restricted variant barely suffered, manufacturing
a 10× effect that does not exist. Reverted before implementation.

---

## 8. Domain context (from Raki, 2026-07-28)

Asked which shape dominates production: chains of LEFT JOINs, or LJ + GROUP BY.

> "When it's a huge chain of LJs, usually it wouldn't have a GROUP BY, I'd usually rewrite the query so
> the LJs are materialized in one table, and then GROUP BY on top of that! [...] the fundamental reason
> there's so many LJs is because in STAR schema you want to avoid orphaned FKs."

Reference: <https://www.snowflake.com/en/blog/engineering/cortex-analyst-joins-star-snowflake-schemas/>

Query corpus: <https://gist.github.com/mdrakiburrahman/136927260e93639c68bc67ff02e015aa> — 94 dbt models,
Fabric Lakehouse + Gluten. LJ-heaviest:

| model | LEFT JOINs | tables | GROUP BY |
|---|---|---|---|
| `int_instance_usage_transaction` | 10 | 13 | no |
| `int_instance_status_transaction` | 10 | 11 | no |
| `int_pit_discovered_28d/7d/daily` | 9 | 10 | no |
| `fact_*_monthly_snapshot` | — | — | **yes** |

**Implication:** the deep LJ chains classify as **`SIMPLE_PROJECTION` (type 2)** — a different refresh
path from everything in §5, which is all `AGGREGATE_GROUP`. Verified: a 10-LJ star with no GROUP BY is
type 2 and refreshes correctly. The aggregates are the narrower `fact_*` snapshots. So the aggregate
floor (§6.3) matters less for their workload than the projection path (§6.4) — weigh that when
prioritising.

---

## 9. Methodology rules (learned the hard way)

1. **Never trust an isolated component timing.** Misled us 5 times. The same join measured 0.34s standalone
   and 6.75s in context, because `SELECT count(*)` lets DuckDB skip materialising payload columns.
   Only these count: a **full-refresh A/B**, or the **real dumped statement** wrapped in
   `CREATE OR REPLACE TABLE ... AS` so all columns materialise.
2. **Verify the machine is idle before and after**, and log it: `cat /proc/loadavg`, `docker ps`,
   `ps -eo pcpu,args --sort=-pcpu | head`. A `bench-spark-openivm` stack ate ~8 of 32 cores and inverted
   results (§11.1). A duckdb build in a container did the same on another day.
3. **Warm the cache** before baselining. A first-run baseline read 31s vs 1.5s warm.
4. **Pair the recompute baseline with each scenario**, measured on the same state — bases grow across a
   sweep.
5. **Check correctness before quoting any timing.** `EXCEPT ALL` both directions. A refresh to a wrong
   answer has meaningless cost (the 4-table shape was wrong in all 18 scenarios and its timings were
   garbage).
6. **Dump the generated SQL and read it** — `SET openivm_files_path='/dir'` writes
   `openivm_upsert_queries_<view>.sql`. Better still, extract the delta INSERT, run it standalone, and
   `SELECT` the delta table. That is what disproved the "primary delta already emits the reappearance"
   comment; reasoning from code comments sent us the wrong way twice.
7. **Timing extraction is fragile.** With `.timer on`, a tag `SELECT 'X' t;` followed by the real
   statement sometimes yields **two** `Run Time` lines and sometimes **one**. Verify per log which, then
   extract. Getting this wrong produced a table of `0.000`s and, worse, a table of plausible-but-wrong
   numbers.

---

## 10. Open items, roughly in priority order

### 10.1 `Max expression depth limit of 10000 exceeded` (hard failure)
Chain-shaped LJ **aggregate** at ~11 tables. Did **not** reproduce in the star shape at any depth 2→12,
so it is chain-specific and uninvestigated. Given the gist has 13-table models this could matter, though
those are projections rather than aggregates. Start by reproducing the chain shape locally at 9/10/11
tables and diffing the generated SQL size/nesting against the star shape.

### 10.2 The DuckLake aggregate floor
~3.9s fixed regardless of delta size, growing near-exponentially with join count (§6.3). No validated
lever. The most concrete unexplored lead: **profile the DuckLake refresh against the standard refresh of
the identical view and diff the operator breakdowns** — standard does the same logical work in 0.708s
where DuckLake takes 4.43s, and that 6× gap on identical shapes has never been explained. Suspects are
DuckDB planning/binding of the large generated program and DuckLake snapshot/parquet metadata resolution,
neither confirmed.

### 10.3 `OuterJoinAggregateNeedsRecompute` — never attempted
`src/core/parser_plan_helpers.cpp` forces outer-join aggregates onto `GROUP_RECOMPUTE` when any
aggregate has a non-pass-through argument (e.g. `SUM(price*(1-discount))` — every TPC-H-shaped view) or a
projection references a non-group binding. Only `COUNT(col)`/`SUM(bare_col)` reach the fast MERGE. This is
the original "SOTA left join" headroom and remains untouched. Note the measured caveat: group-recompute is
1.5–2× *slower* than plain recompute at coarse granularity, so the framing is "stop losing", not
"make a fast thing faster".

### 10.4 Re-measure the SF50 projection numbers
§6.4's SF50 row was taken at load 2.42. Re-run idle. This is the shape Raki's workload actually uses, so
it deserves clean numbers more than the aggregate path does.

### 10.5 Unexplained `count_star = 2`
After a batch where a group gains its first match, `count_star` read 2 where 1 was expected, while
user-visible columns were correct. Low priority but suggests remaining count_star accounting fragility.

### 10.6 Rewriter benchmark never completed
2505 TPCC queries. Two runs reached 2250/2505 completely clean (crash=0, correct==refresh, metadata
mismatches 0) but neither finished — one killed by an EC2 shutdown, one by me to free the machine. Worth
one clean completion as a regression gate.

### 10.7 Report the DuckDB unique-index bug upstream
§5.5 has a self-contained 6-line repro. openivm works around it in two places; upstream should still know.

---

## 11. Traps that cost real time

### 11.1 Benchmark contamination
A `bench-spark-openivm` Docker stack (Spark JVM at ~794% CPU, plus dbt, Livy, SQL Server) ran for part of
one session and **inverted conclusions**: it made standard IVM look like it lost 1.4× on a shape where it
actually wins 4.6×, and manufactured the fake 10.4× in §7.5. Always check, always log.

### 11.2 DuckLake MV metadata is per-session
openivm's system tables (`openivm_views`, …) live in the **default catalog**. With an in-memory default
catalog, DuckLake MV metadata does **not** survive between sessions — you get
`Materialized view X does not exist in IVM metadata`. Do CREATE + DML + refresh + measure in **one
session**, or use a file-backed default DB.

### 11.3 DuckLake MVs must be created inside the DuckLake catalog
`CREATE MATERIALIZED VIEW dl.<name> AS SELECT ... FROM dl.<tables>`. Omitting the `dl.` prefix puts MV
state in the memory catalog while sources are in DuckLake, and refresh then fails looking for delta
tables. Do **not** `USE dl` — that sends openivm's own system tables into the DuckLake catalog.

### 11.4 The in-memory suite cannot catch on-disk bugs
Every `test/sql/*.test` runs in-memory. The unique-index bug (§5.5) only fires with a file-backed DB and
a **freshly loaded** index — the regression test needs `load` **and** `restart` immediately before the
refresh, otherwise it passes vacuously (verified: it did, until the restarts were added).

### 11.5 Dirty benchmark databases fake correctness bugs
After `f8d152dc` the 4-table shape still showed a ~4,587-row residual at SF50. It was **not a bug** — an
artifact of a benchmark DB carrying nine rounds of synthetic mutations. On freshly generated SF50 data the
same shape verifies exactly, including a follow-up multi-table batch. Retest correctness on clean data
before believing a scale-only discrepancy.

### 11.6 `renumber_and_rebind_subtree` return value
`RenumberWrapper.column_bindings` is **not** the post-renumber top-level output — it is a concatenation of
every level's *pre*-renumber bindings gathered during the walk. Use `renumbered.op->GetColumnBindings()`.
Reading the wrong field produced a tautological `t61_oid = t61_oid` join condition and sent us hunting a
non-existent LPTS bug.

---

## 12. Environment

- **Build:** `GEN=ninja make`; single test `build/release/test/unittest "test/sql/<name>.test"`; full suite
  `build/release/test/unittest`. Run `make format-fix` before committing (Claude Code does it via hook;
  Codex must do it explicitly).
- **EC2:** key `~/ila-openivm.pem`, user `ubuntu`, checkout `~/openivm-main-on-raki-latest` on branch
  `refactor`. **The public IP changes on every stop/start** — ask for the current one. `$HOME` survives
  restarts; **`/tmp` does not**, so harness scripts written there are lost (keep them in `$HOME`).
- **Datasets built during this work** (in `$HOME`, may or may not still exist): `bench50.db` (native SF50 +
  `supplier_x`), `dlx/` (DuckLake SF50, ~17GB, with `d1..d10` star dimensions), `proj50.db`.
- **Disk is the binding constraint.** 484GB volume, and a 10-join projection MV over 300M rows is
  enormous — one run took the DB from 16.6GB to 69.5GB and got within 9GB of full. There is a prior
  full-disk crash in the project history. Run a guard alongside long jobs:
  ```bash
  while pgrep -x duckdb >/dev/null; do
    avail=$(df --output=avail -BG / | tail -1 | tr -dc 0-9)
    [ "$avail" -lt 7 ] && { pkill -x duckdb; break; }
    sleep 10
  done
  ```
- **Long SSH commands:** anything over ~2 minutes gets killed by the client. Launch with
  `nohup ... &`, then poll, or wait with an `until ! pgrep -x duckdb; do sleep N; done` loop.
- **`pkill -f <pattern>` will kill your own SSH session** if the pattern appears in the command line.
  Use `pkill -x duckdb`.

---

## 13. Relevant settings

| setting | default | relevance |
|---|---|---|
| `openivm_left_join_merge` | true | false → routes to group-recompute via `has_minmax` |
| `openivm_full_outer_merge` | true | Zhang & Larson FULL OUTER |
| `openivm_skip_empty_deltas` | true | 0-row delta refresh costs 0.04s instead of 3.9s |
| `openivm_ducklake_nterm` | true | N-term telescoping instead of 2^N−1 |
| `openivm_adaptive_refresh` | false | **out of scope by user instruction** |
| `openivm_profile_refresh` | false | per-statement timings in `openivm_refresh_profile` |
| `openivm_files_path` | — | dumps the generated refresh program; indispensable for debugging |
