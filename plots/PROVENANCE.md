# Measurement provenance (2026-09-16 draft)

## Augmented TPC-DI SF100

| Source | Benchmark commit | Points used | Outcome |
|---|---|---|---|
| [GCI #589](https://github.com/mdrakiburrahman/ivm-bench/actions/runs/34600019978) | `4e57ab7f5c3aef67538e77525f81c5aa2e560e6c` | 5%, 15%, 25% | Two repetitions completed for all eight engines; runner shutdown interrupted 35%; artifact upload skipped. Timings recovered from the GitHub job log. |
| [GCI #590](https://github.com/mdrakiburrahman/ivm-bench/actions/runs/34851747973) | `d04b03286a17cc59cb38e15eb3d37acfd78d0757` | 35%, 45% | 35% Enzyme repetition 2 failed at dbt compilation (300-second timeout); other engines continued. 45% completed for all engines, two repetitions. |
| [GCI #592](https://github.com/mdrakiburrahman/ivm-bench/actions/runs/35117409741) | `55991bd` | Enzyme 35% only | Rerun dispatched; not part of the saved measured dataset yet. |

The #590 artifact is named `benchmark-results-sf10`, but its resolved experiment
inputs, logs and source-row-count metadata all specify **SF100**. The artifact
name reflects the workflow's default input, not the experiment's scale factor.
Do not use artifact names alone to infer SF.

Configuration: initial batch 100%; insert-only; profiling/query logging/storage
collection enabled; validation disabled; local engines serial and cloud engines
parallel; Spark dbt threads 4. Databricks uses `AUTO` (not forced incremental).
The workload window is 365 days: 5/15/25/35/45% map to 18/55/91/128/164 days.
These are **time-window percentages**, not measured inserted-row percentages.
Batch 3 is the next day, not a second equally large append. The figure uses only
Batch 2, and each experiment starts from a fresh initial materialization.

The native OpenIVM pin is `3b3938f4f8293875b56157f563c6f8cb196a0b41` and the Spark
adapter pin is `a48bc073c11597b95467c45d67f7a21ad72eab26` in these benchmark
revisions. These results predate the affected-source-key optimization experiments.
The generator Dockerfile fetches the upstream Databricks TPC-DI generator without
an immutable revision pin; this limits exact regeneration from source alone.

`data/tpcdi_augmented_sf100_repetitions.csv` contains the reported per-repetition
batch durations (seconds, rounded by the logs to 0.1 s), not reconstructed SQL
operator timings. We preserve all 79 completed batch measurements, including the
single Enzyme 35% repetition, but only groups with two repetitions get a mean.
The summary has 39 measured means and one missing mean. Summaries of log-rounded
values can differ slightly from full-precision artifact means.

Retries remain in the reported end-to-end measurements. Enzyme had a transient
pipeline initialization failure and retry in one repetition at both 5% and 45%;
the whiskers expose the large spread. Two samples do not establish statistical
significance. No matched SF100 Databricks FULL-policy baseline is available in
these two source runs; older SF10 full-refresh results must not be relabelled SF100.

## Draft-only placeholders

At the user's request, the absolute-runtime figure interpolates the missing
Enzyme 35% mean between the measured 25% mean (513.15 s) and 45% mean (795.80 s):
**654.475 s**. This is not a benchmark result or a performance prediction. A hollow
marker, dashed connectors and TODO text identify it; the caption repeats the
warning. It is stored separately from measured data and excluded from ratios and
quantitative claims. The large retry-related variation makes this interpolation
particularly unsuitable for conclusions.

The mixed-DML figure is an empty labelled placeholder. The insertion-only runs
cannot establish mixed-workload performance. Workload ratios, changed-row budgets,
and repeated measurements remain TODOs.

## Historical data

Other `data/*run_<id>.csv` files were carried over with values unchanged from the existing
paper folder. Their filename identifies the GitHub Actions run under
`mdrakiburrahman/ivm-bench`; they are historical snapshots, not part of the new
augmented sweep. Existing provenance/validation limitations must be checked before
promoting their preliminary figures to final claims. Compiler coverage counts
and the corpus characterization scripts were also inherited; no new coverage
evaluation was performed for this manuscript packaging task.

Only compact benchmark metrics and generic R scripts are published here. The
Overleaf project contains figures, not raw logs or private Fabric workload inputs.
