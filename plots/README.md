# IVMBench paper plots

R plotting scripts and compact CSV inputs for the working IVMBench paper.
The manuscript is maintained separately in Overleaf; this directory does not
contain the paper, private production queries/data, credentials, or raw logs.
See [PROVENANCE.md](PROVENANCE.md) before interpreting preliminary measurements.

## Reproduce the current augmented-workload figures

From the repository root, with R and `ggplot2`, `dplyr`, and `systemfonts` installed:

```sh
Rscript plots/plot_augmented_sweep.R
```

Outputs go to ignored `plots/output/`. Copy the three PDF figures into the
Overleaf project's top-level `figures/` directory. An explicit destination works:

```sh
Rscript plots/plot_augmented_sweep.R plots/data/tpcdi_augmented_sf100_repetitions.csv /path/to/overleaf/figures
```

The script produces absolute runtimes grouped by host engine, ratios of native
full-refresh means to OpenIVM means, and an empty mixed-DML placeholder.
Only pairs of two measured repetitions contribute means or ratios. Whiskers
are the observed min/max, not confidence intervals. The Enzyme 35% hollow point
is a **layout-only linear interpolation**, labelled TODO in the figure and
written separately to `tpcdi_augmented_sf100_placeholders.csv`. It is never
inserted into the measurement CSV or used in speedup calculations.

To recover the archived measurements again, obtain the GCI #589 job log and the
GCI #590 artifact's `mount/oat-state/latest/benchmark-server.log`, then run:

```sh
Rscript plots/extract_augmented_sweep.R /path/to/589-job.log /path/to/590-benchmark-server.log /tmp/repetitions.csv
```

The extractor selects 5/15/25% from #589 and 35/45% from #590 so the interrupted
35% attempt in #589 cannot contaminate the recovery pair. The supplied snapshot
has 79 reported batch measurements: 39 complete pairs plus one Enzyme 35% run.

## Other paper scripts

| Script | Input / purpose |
|---|---|
| `plot_tpcdi_batch2_insert_sweep.R` | Historical standard TPC-DI row-percentage sweep CSV; not the augmented day-window workload |
| `plot_tpcdi_engine_batches.R` | Batch initialization/refresh timings from a CSV |
| `plot_storage_overhead.R` | Storage accounting CSV |
| `plot_tpcdi_io.R` | Reported I/O CSV |
| `plot_compiler_coverage.R` | Saved compiler coverage counts |
| `plot_tpcc_characteristics.R` | Public query corpus; pass the exact historical corpus explicitly to reproduce paper counts |
| `plot_tpcdi_cpu.R`, `plot_tpcdi_network_io.R` | Explicit downloaded artifact path: `mount/stats/<sf>`; no hard-coded local paths |
| `plot_fabric_characteristics.R` | Generic plotting/analysis code; requires a separately supplied authorized dbt project, not distributed here |

These scripts retain the paper's existing serif styling and colors. Older
scripts may also require `jsonlite`, `scales`, `tibble`, or `tidyr`; they report
missing packages rather than installing anything. Scripts with CSV inputs accept
an optional input path and output directory (the insert sweep accepts an output
filename). Generated output is ignored by Git. Historical CSV filenames preserve
the original GitHub run IDs; do not silently combine them with different pins or
workload configurations.

## Updating the missing point

GCI #592 reruns only SF100 Enzyme at 128 augmented days, two repetitions, AUTO,
profiling/storage on, validation off. Replace both Enzyme 35% records together
after checking the rerun's resolved inputs and statuses. Remove the interpolation
layer and TODO from the plot and paper caption; keep the measurement provenance.
The mixed-DML placeholder has no inferred or measured runtime values.
