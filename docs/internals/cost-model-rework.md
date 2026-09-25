# Cost model rework — status and handover

Working notes for whoever picks this up next. Covers what was built, what was measured, what is
still unproven, and the traps that cost time. Companion to [cost_model.md](cost_model.md), which
describes the model as it stood before this work; where the two disagree, this file is newer.

## Takeover investigation (2026-09-25)

The last failed ladder is [GCI run 36061442667](https://github.com/mdrakiburrahman/ivm-bench/actions/runs/36061442667).
Both SF10 and SF100 contain exactly 130 completed rows. Their last row is Q08, mixed workload,
2% delta, repetition 1. SF10 exited with SIGABRT; SF100 timed out. This shared boundary means the
SF100 timeout must not be attributed to setup cost alone. The saved artifacts do not include a
native crash diagnostic or stack trace, so the causes remain unconfirmed.

PR #23 was already merged upstream but absent from this branch and the old runner build. Its two
commits (`3ada9f57`, `99c8a450`) are now merged here in `14f92dd1`. Besides database-owned gate
lifetime, they synchronize access to the transaction's mutation guard: parallel delta-capture
workers previously raced to create and replace it, potentially leaking a held gate. This is a known
defect addressed by the merge, not proof of the cause of either runner failure.

Before rebuilding, a focused SF10 Q08 run completed 9 cycles without errors. The merged concurrency
regression also passed against the old binary, so this machine did not reproduce the race. After
rebuilding, concurrency, adaptive refresh, persistent full-refresh cascade, and cost-model tests
passed (1,200 assertions across four test files).

The benchmark also skipped result validation for forced incremental and full refreshes. All three
modes now use the existing bidirectional `EXCEPT ALL` check when validation is enabled, and the CSV
`correct` field requires all three to pass. Validation stays outside each refresh timing interval.

With those changes, SF10 Q08 completed 30 cycles in 126.83 seconds, with zero errors across all
90 refreshes and calibration reported for 21 cycles. This was a correctness/integration run;
other tests overlapped its early cycles, so its timings are not an isolated performance result.
The CSV and log are `/private/tmp/cost-takeover-q08-after.csv` and
`/private/tmp/cost-takeover-q08-after.log`. Reproduce with:

```bash
build/release/extension/openivm/cost_model_benchmark \
  --scale 10 --filter Q08 --reps 1 --cycles 30 --delta-pcts 1,2,5 \
  --configs all_on --out /private/tmp/cost-takeover-q08-after.csv
```

Before another ladder, run a bounded Linux reproduction with the merged lock fix and retain the
benchmark's stdout/stderr alongside the CSV. A local pass does not establish that the runner crash
or timeout is fixed. The per-operator model remains unvalidated; none of these integration checks
establish prediction quality.

## Why

`PRAGMA refresh_cost` decided between incremental maintenance and full recompute using two scalar
cost terms per strategy, converted to milliseconds by seven hand-tuned constants. Two problems:

- The two sides were not measured by the same instrument. The incremental side was priced from delta
  cardinalities with a fanout proxy, the recompute side from base cardinalities, with separately
  tuned constants. Their predictions were not comparable, they only happened to be on the same scale.
- The constants were fitted on one machine. Nothing adapts them to another.

The goal was a model where both strategies are described in the same terms and the conversion to
milliseconds is learned from what actually happened.

## Where the work lives

| PR | Branch | Contents |
|---|---|---|
| [#21](https://github.com/ila/openivm/pull/21) (merged) | `ila/tpcc-spec-profile` | TPC-C spec scale profile for the benchmark |
| [#22](https://github.com/ila/openivm/pull/22) | `ila/cost-model-plan-reuse` | The cost model itself. Also carries #24 via a merge |
| [#23](https://github.com/ila/openivm/pull/23) | `ila/fix-mutation-gate-lifetime` | Mutation gate deadlock fix |
| [#24](https://github.com/ila/openivm/pull/24) | `ila/benchmark-snapshot-setup` | Benchmark restructure plus the cascade refresh fix |
| [ivm-bench #70](https://github.com/mdrakiburrahman/ivm-bench/pull/70) | `ila/cost-model-sweep-workflow` | `cost_model_bench` feature flag for GCI |

Two branches exist only to run things and are not for merge: `ila/cost-model-sweep-build` (openivm,
merges #22 and #24 so the runner has one ref) and `ila/cost-model-sweep-run` (ivm-bench, carries the
`OPENIVM_COMMIT` pin).

## Architecture

### Plan cost features

Both candidates are described as estimated rows per operator class, read from DuckDB's own estimator
on the plans that will actually run. `PlanFeature` in `src/include/upsert/refresh_cost_model.hpp`:

```
SCAN_ROWS  FILTER_ROWS  PROJECT_ROWS
JOIN_BUILD_ROWS  JOIN_PROBE_ROWS  JOIN_OUTPUT_ROWS
AGGREGATE_INPUT_ROWS  AGGREGATE_OUTPUT_ROWS
ORDER_ROWS  WINDOW_ROWS  SET_OP_ROWS  WRITE_ROWS
```

Joins contribute three features because their costs differ in kind: the build side is materialized,
the probe side streamed, the output is whatever fan-out produces. `WRITE_ROWS` is supplied by the
caller, not read from a plan, because the delta plan computes rows and does not contain the MERGE or
delete-and-insert that writes them.

`PLAN_FEATURE_SCHEMA` versions the layout. History rows carrying a different value are skipped by the
fitter rather than reinterpreted.

### Costing the plan that runs

`GenerateRefreshSQL` used to decide at line ~584 and build the incremental delta plan ~800 lines
later, so the model costed a plan that was never executed. `BuildIncrementalDeltaPlan` is now hoisted
above the decision and the result moved into the delta-production branch, so nothing is planned
twice. Safe because nothing the rewrite rules observe changes between those points: compile facts are
final beforehand, and no setting, context slot or metadata row is written in between.

### The fit

Weighted ridge least squares, non-negative feature weights, free intercept. Non-negativity matters:
an operator cannot make a refresh finish sooner, and unconstrained fitting on correlated features
produces negative coefficients that predict nonsense on plan shapes absent from training.

Fitted across views, not per view. The weights describe how long an operator class takes on this
hardware, which is a property of the machine, so a view refreshing for the first time inherits them.
Pooling is also what makes a thirteen-parameter fit possible: per-view retention is 100 rows.

The sample requirement scales with the parameters the data exercises, three per fitted parameter with
a floor of eight. Most plans leave most features at zero, and a column that never varies contributes
nothing but dimensionality.

### Fallback chain

1. Per-operator weights (pooled, needs the most evidence)
2. Two-term per-view regression (3 parameters, needs 9 samples)
3. Hand-tuned prior

### Exploration

A model that learns only from what it runs cannot observe the road not taken: a strategy it stops
choosing stops producing samples, so the estimate that sidelined it is never corrected. Every tenth
refresh runs the losing strategy while that strategy has fewer than 24 samples. Deterministic, not
random, so runs are reproducible. Self-limiting. Skipped for views that cannot be maintained
incrementally, since full recompute is always legal and the reverse is not. Controlled by
`openivm_adaptive_explore`, recorded in `openivm_refresh_history.exploratory` so its cost is
auditable.

### Storage

`openivm_refresh_history` gains `plan_features DOUBLE[]`, `feature_schema INTEGER`, `exploratory
BOOLEAN`. A DuckDB list rather than JSON: no parser needed to read it back and no extension needed to
write it. Retention rose from 20 rows per view to 100.

## The benchmark

`benchmark/src/cost_model_benchmark.cpp`. A case is one database refreshed `--cycles` times
(default 10). Each cycle applies a delta, branches all three refresh modes from the same state so the
comparison stays fair, and carries the automatic result forward so history accumulates. Delta
percentage rotates across cycles.

This replaced a design where each case was a fresh database refreshed exactly once. That made history
empty by construction, so the sweep could only ever exercise the uncalibrated path whatever model sat
behind it.

Running it:

```bash
# local
build/release/extension/openivm/cost_model_benchmark \
  --scale 10 --filter Q08 --reps 1 --cycles 30 --configs all_on --batch validated --out out.csv

# on the ivm-bench runners, after PR 70 lands
gh workflow run GCI --repo mdrakiburrahman/ivm-bench --ref ila/cost-model-sweep-run \
  -f experiments_file=src/containers/benchmark-server/experiments/cost-model-bench.json
```

`cost-model-bench.json` is the smoke experiment, `cost-model-bench-ladder.json` the full ladder. GCI
runs every experiment in the file it is given and cannot select one, hence two files.

## What was measured

### Prediction accuracy, one database over 30 cycles

Median q-error, ratio of predicted to measured, 1.0 being exact. Measured after the training-target
fix described below.

| Query | Hand prior | Two-term | Per-operator |
|---|---:|---:|---:|
| Q01, aggregate over one table | 1.67 | 1.05 | 1.08 |
| Q06, three-way join | 2.84 | 1.73 | 1.64 (worst 8.75) |
| Q08, join with group by | 1.34 | 1.09 | 1.90 |

### Decision quality across scale

From the GCI ladder, 2,520 cases. **Measured before the training-target fix and before the cycles
restructure**, so it describes the prior, not the learned model.

| Scale | Cases | Errors | Incremental fastest | Model chose it | Agreement | Mean regret | p95 |
|---|---:|---:|---:|---:|---:|---:|---:|
| 1 | 504 | 0 | 35 | 15 | 94% | 1.012 | 1.03 |
| 10 | 504 | 0 | 90 | 54 | 89% | 1.072 | 1.16 |
| 25 | 504 | 0 | 118 | 129 | 76% | 1.187 | 1.68 |
| 50 | 504 | 0 | 120 | 144 | 75% | 1.315 | 2.11 |
| 100 | 504 | 0 | 147 | 225 | 68% | 1.415 | 1.96 |

Note the bias inverts: below scale 25 the model under-picks incremental, above it over-picks.

## What is NOT validated

**The per-operator model.** It is no better than the two-term fit on a simple aggregate, marginally
better in median on a three-way join with a far worse tail, and clearly worse on a join with a group
by. It is currently a regression risk, not an improvement.

Two plausible causes, pointing at different fixes:

- *Too few samples.* A join plan exercises around ten features, so three-per-parameter asks for about
  33 samples; 30-cycle runs reach the threshold right at the edge, where least squares overfits
  hardest. The blown-out tail on Q06 looks like this.
- *Non-stationarity.* Each cycle inserts, so the table grows through the run. Weights fitted on early
  cycles describe a smaller database than the later ones they predict.

The suggested fix is to stop assuming which model is better: fit both, score them on held-out cycles,
use whichever predicts better for that view, fall back to the prior if neither beats it. That
subsumes the threshold question and would have caught this automatically. **Not built.**

## Bugs found, and what they teach

Several were invisible because something reported success. That is the recurring theme.

**Refresh mutation gate deadlock** (PR 23). A sweep sat at 0% CPU for 5.5 hours. The gate registry
was a static map keyed by `DatabaseInstance*` that owned its entries and never erased them; addresses
are recycled, so a new database inherited a dead one's gate including its owner and depth. Also
`MutationGate::Unlock` used a bare `D_ASSERT` on mismatch, which compiles out in release, so a lost
unlock left the gate held forever with no indication why.

**Cascade full refresh duplicate key** (PR 24). Full refresh of an AGGREGATE_GROUP view that has a
downstream view failed on a reopened persistent database. `BuildFullRecomputeSQL` already had a safe
upsert form and `BuildRecomputeQuery` passed the keys to select it; the cascade path reached the data
table through `CompileFullRecompute`, which had no parameter for them. Needs all three of a persistent
reopened database, `refresh_mode=full`, and a downstream view. Covered by
`test/sql/full_refresh_cascade_unique_index.test`.

**Training on the wrong quantity** (PR 22, the most consequential). The history table recorded only
the time to execute the generated program. Planning, the rewrite rules, the LPTS round trip and
upsert codegen were excluded, though on small views they dominate: 5ms of execution inside 15ms
end-to-end. Two consequences: predictions looked like a two- to threefold under-estimate when they
were answering a different question, and the comparison was biased, because generation is expensive
for the incremental path and cheap for recompute. Before the fix, calibration made predictions
*worse*, q-error rising from 1.5 to 3.0.

**Two-term regression fitting 3 parameters on 3 samples** (PR 22). Engaged at the fourth refresh and
predicted 4ms against 13ms measured, worse than the prior it displaced. Now 9.

**Stale cached cardinalities** (PR 22). `EstimateCardinality` memoizes. When a rewrite rule replaces
the subtree under a projection the original plan already costed, the projection keeps the pre-rewrite
number. Reading it estimated a one-row delta over a 100-row table at 101 rows. Descending past
projections is exact, since a projection cannot change cardinality.

**TPC-C generator 30x below spec** (PR 21). 100 customers and 100 orders per district against the
specification's 3000. Scale 100 was a 1M-row database, so full recompute won everything regardless of
model quality and the crossover was unobservable at any reachable scale factor.

**Benchmark catalog names held together by a decimal point** (PR 24). DuckDB derives a catalog name
from the filename up to the first dot, and a view's metadata records the catalog it was created in.
The old filenames embedded the delta percentage, so `1.000000` truncated every variant to the same
catalog. Removing the percentage broke every refresh at once.

**Synthetic keys overrunning 32 bits** (PR 24). A fixed per-cycle stride passed `INT32_MAX` at cycle
22; inserts then failed while the mixed workload's deletes kept succeeding, draining the table to
empty and still reporting success. Keys now come from a watermark advanced by actual usage.

**A non-deterministic benchmark query** (PR 24). T03 used `ARG_MAX(C_ID, C_BALANCE)`, and TPC-C gives
every customer the same balance, so the maximum was tied across 29,700 rows per group. The view and a
fresh evaluation returned different, equally correct answers and `EXCEPT ALL` called it a mismatch.
Inspecting a failing database confirmed the view was right. Now ordered by `(C_BALANCE, C_D_ID,
C_ID)`, which is unique within a warehouse.

**ivm-bench orchestrator** (PR 70). Non-zero exits were logged and the experiment still reported
completed, so a correctness failure passed as success. Output was named by scale factor alone, so two
experiments sharing one overwrote each other. Build selection read the environment before
per-experiment flags were applied.

**Two smaller traps.** `openivm_adaptive_explore` did nothing until it was added to the
planning-settings propagation list, since settings do not reach the inner planning connection unless
named there. And `alter table ... add column if not exists <c> boolean default false` inside the
CREATE MATERIALIZED VIEW DDL batch fails with "Cannot plan statement of type MULTI", while the same
column with the same default is fine in the CREATE TABLE and the same ALTER is fine without the
default; DuckDB rewrites `false` to `CAST('f' AS BOOLEAN)` and the batch does not survive the quoted
literal.

## Open items

1. **Validate or replace the per-operator model.** See above. Model selection on held-out cycles is
   the suggested route.
2. **Scale 10 crashes.** The last GCI ladder aborted scale 10 with exit -6 (SIGABRT) after 130 of 504
   cases, in 2.5 minutes. Uninvestigated. Scales 1, 25 and 50 completed clean in the same run, so it
   is not a general breakage.
3. **Scale 100 runtime.** Twelve hours and still unfinished under the old one-refresh-per-database
   shape, where the timed refresh was 0.4% of wall clock at scale 50. The cycles restructure should
   change this materially but has not been run at scale on the runners.
4. **Re-run the ladder on a build that contains all of this.** Every GCI run so far predates either
   the learned weights or the training-target fix. Re-merge #22 into `ila/cost-model-sweep-build`,
   repin `ila/cost-model-sweep-run`, dispatch.
5. **`PRAGMA refresh_cost` cannot say which model produced a number.** Diagnosing the per-operator
   model meant inferring it from sample counts and timing. A column naming the source would have made
   that immediate.

## Working notes

- CI pins clang-format 11.0.1. Newer versions wrap differently and `make format-fix` refuses to run
  with them. `pip install 'clang_format==11.0.1' cmake-format 'black==24.*' cxxheaderparser pcpp`.
- `make format-check` only covers `src` and `test`, not `benchmark`.
- Most of the suite is in-memory. Bugs involving on-disk indexes need `load` plus `restart`; see
  `group_recompute_persistent_unique_index.test` and `full_refresh_cascade_unique_index.test`.
- Assertions that pass without the change they are meant to test are the main hazard here. Three
  written during this work were vacuous and only caught by deliberately reverting the fix and
  re-running. Worth doing routinely.
