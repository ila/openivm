# Cost Model

OpenIVM uses the cost model when `openivm_adaptive_refresh` is enabled. The model
compares the selected incremental refresh strategy with a full recompute, then chooses
the lower predicted cost.

The implementation lives in `src/upsert/refresh_cost_model.cpp`.

## Decision Target

The model does not invent a new refresh strategy. It prices the strategy assigned to the
view at creation time and compares it with a full DELETE + INSERT recompute.

| View classification | Priced non-full strategy |
|---|---|
| Regular projection, filter, join, aggregate | `incremental` |
| `GROUP_RECOMPUTE` | `group_recompute` |
| `WINDOW_PARTITION` | `window_partition` |
| `CURRENT_DIFF_RECOMPUTE` | `current_diff_recompute` |
| `DISTINCT_INCREMENTAL` | `distinct_incremental` |
| `SEMI_ANTI_RECOMPUTE` | `semi_anti_recompute` |
| `FULL_REFRESH` or `TOP_K` | `full` |

`PRAGMA refresh_cost('view_name')` returns the selected decision and the static or
calibrated predictions used to make it:

```sql
PRAGMA refresh_cost('monthly_totals');
```

| Column | Meaning |
|---|---|
| `decision` | Strategy selected for the next refresh. |
| `incremental_cost` | Static cost of the selected non-full strategy. |
| `recompute_cost` | Static cost of full recompute. |
| `incremental_predicted_ms` | Learned prediction for the non-full strategy, or static cost before calibration. |
| `recompute_predicted_ms` | Learned prediction for full recompute, or static cost before calibration. |
| `calibrated` | Whether enough refresh history existed to fit a learned model. |

## Static Model

The static model collects plan and table statistics in one walk of the optimized view
plan:

- base cardinality for each source table;
- actual source-table row count, used to estimate pushed-down filter selectivity;
- pending delta cardinality for each source table;
- join, aggregate, full-outer-join, and DuckLake flags;
- operator classes: linear, product, stateful, and full-recompute-only nodes.

For standard DuckDB tables, pending delta cardinality comes from
`openivm_delta_<table>` rows newer than the view's last refresh timestamp. When refresh
planning already computed delta activity, the cost model reuses that result. For
DuckLake tables, pending cardinality is estimated from insertion and deletion table
functions between the last consumed snapshot and the current snapshot.

### Join Terms

For joins, compute cost is driven by the number of delta join terms.

If `openivm_skip_empty_deltas` is enabled, only sources with non-empty deltas are priced.
If the setting is disabled, all join leaves are priced, because the refresh compiler also
builds terms that contain inactive sources.

| Join mode | Term estimate |
|---|---|
| Standard inclusion-exclusion | `2^priced_sources - 1` |
| DuckLake N-term | `priced_sources` |
| FK-pruned single-source insert into referenced table | `0` terms for the pruned case |

The estimated delta result size uses a fanout proxy:

```text
sum(delta_rows(table) * mv_rows / actual_rows(table)) * filter_selectivity
```

`actual_rows(table)` is the unfiltered table cardinality. This avoids overestimating
fanout when DuckDB's scan cardinality already includes a pushed-down filter.

### Linear Operators

For unary plans, compute cost is the total number of pending delta rows. The estimated
output delta applies pushed-down and residual filter selectivity.

```text
incremental_compute = total_delta_rows
estimated_delta_result = filtered_delta_rows * residual_filter_selectivity
```

### Upsert Cost

The model separates compute work from write-side work.

| Output shape | Upsert estimate |
|---|---|
| Grouped aggregate | `min(delta_result, mv_rows) * 2` |
| Projection or filter | `delta_result * (1 + log2(mv_rows))` |
| FULL OUTER aggregate | aggregate estimate multiplied by `3` |
| FULL OUTER projection | projection estimate multiplied by `1.5` |

The multipliers account for extra recompute phases in full-outer maintenance, such as
unmatched-row key extraction and NULL-group repair.

### Full Recompute

Full recompute is modeled as running the full query once and replacing every row in the
materialized data table.

```text
recompute_compute = total_base_scan + mv_rows
recompute_replace = mv_rows * 2
recompute_cost = recompute_compute + recompute_replace
```

### Strategy Overrides

Some refresh types do not use pure delta MERGE. Their non-full costs are adjusted to
match the refresh path that actually runs:

- `GROUP_RECOMPUTE` prices affected-key recompute variants and DELETE + INSERT of
  affected groups.
- `WINDOW_PARTITION` prices delta scanning plus the affected partition fraction of the
  base scan.
- `CURRENT_DIFF_RECOMPUTE` prices the same compute and replace work as full recompute.
- `DISTINCT_INCREMENTAL` adds aux-state maintenance over affected distinct tuples.
- `SEMI_ANTI_RECOMPUTE` prices aux-state/domain recompute for supported semi/anti
  projection shapes.

## Learned Calibration

When adaptive refresh is enabled, OpenIVM also tries to calibrate the static estimates
against refresh history.

The learned model is fit separately for the selected non-full strategy and for full
recompute. It uses weighted ridge regression with non-negative compute and upsert
weights:

```text
actual_ms ~= w_compute * compute_est + w_upsert * upsert_est + intercept
```

Recent samples receive more weight. `openivm_cost_decay` controls the decay factor and
defaults to `0.9`. At least three history samples are required before a strategy is
marked calibrated.

During cold start, non-full strategies receive a fixed compilation/planning overhead
when adaptive refresh is enabled and either:

- at least one delta source is active; or
- `openivm_skip_empty_deltas` is disabled.

This biases small-data cases toward full recompute until enough history exists. True
empty-delta no-ops with empty-delta skipping enabled do not pay this overhead.

## Preliminary Benchmark Results

The current benchmark compares automatic refresh with forced incremental and forced full
refresh. It records the model decision, predicted costs, actual refresh times, fastest
forced strategy, correctness, pending delta rows, and errors.

Latest focused local run:

```bash
build/release/extension/openivm/cost_model_benchmark \
    --scale 1 \
    --delta-pcts 0,1,5,10,20,50 \
    --reps 1 \
    --filter Q05,S02,S03,P1 \
    --out /tmp/cost_model_delete_fix_focus.csv
```

Scope:

| Dimension | Value |
|---|---|
| Queries | `Q05`, `S02`, `S03`, `P1` |
| Scale factor | `1` |
| Delta percentages | `0`, `1`, `5`, `10`, `20`, `50` |
| Workloads | insert-only, mixed DML, empty-delta |
| Flag configs | all optimizations on, all optimizations off, empty-delta skip off |
| Repetitions | `1` |

Summary:

| Metric | Result |
|---|---:|
| Cases | 132 |
| Errors | 0 |
| Correct results | 132 |
| Mixed nonzero-delta cases with pending delta rows | 60 / 60 |
| Full vs non-full decision mismatches | 0 |

Decision breakdown:

| Delta percentage | Automatic full | Automatic non-full | Fastest forced full | Fastest forced non-full |
|---:|---:|---:|---:|---:|
| 0 | 8 | 4 | 8 | 4 |
| 1 | 24 | 0 | 24 | 0 |
| 5 | 24 | 0 | 24 | 0 |
| 10 | 24 | 0 | 24 | 0 |
| 20 | 24 | 0 | 24 | 0 |
| 50 | 24 | 0 | 24 | 0 |

The 0% cases are empty-delta refreshes. With empty-delta skipping enabled, the fastest
strategy is the no-op/non-full path for supported views. With skipping disabled, the
refresh pipeline still runs and full recompute is often cheaper at this small scale.

These results are preliminary. They validate the current small-scale boundary after the
delete-workload fix, but they do not establish the break-even point for larger data. The
next useful runs are scale factors where incremental refresh should win for low delta
percentages, with enough repetitions to reduce warm-cache noise.
