# Cost Model

OpenIVM uses the cost model when `openivm_adaptive_refresh` is enabled. The model
compares the view's normal maintenance path with a full recompute, then chooses the
lower predicted cost.

## Decision Target

Each materialized view has a maintenance path determined by its query shape. For example,
a grouped aggregate usually uses a keyed MERGE, while a window query may recompute only
affected partitions. In `auto` mode, the cost model asks one question: is that normal
maintenance path cheaper than rebuilding the whole materialized view?

| Query shape | Normal maintenance path |
|---|---|
| Projection, filter, join, grouped aggregate | Incremental delta maintenance |
| Non-linear grouped aggregate shape | Affected-group recompute |
| Window function | Affected-partition recompute |
| Current-diff shape | Current result diff against the old MV data |
| Supported inner DISTINCT under aggregate | DISTINCT aux-state maintenance |
| Supported SEMI/ANTI projection | SEMI/ANTI aux-state recompute |
| Unsupported or full-refresh-only shape | Full recompute |

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
If the setting is disabled, all join leaves are priced, because refresh still evaluates
terms that contain unchanged sources.

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

## Example

Consider a materialized view over a 3-table join:

```sql
CREATE MATERIALIZED VIEW order_summary AS
    SELECT c_region, SUM(ol_amount) AS revenue
    FROM customer
    JOIN orders ON c_id = o_c_id
    JOIN order_line ON o_id = ol_o_id
    GROUP BY c_region;
```

If only `order_line` has pending changes and empty-delta skipping is enabled, the join
delta has one active source. The standard inclusion-exclusion rule prices one join term:

```text
2^1 - 1 = 1
```

If both `orders` and `order_line` changed, the model prices three terms:

```text
2^2 - 1 = 3
```

Those terms estimate the compute side of incremental maintenance. The aggregate MERGE
cost is then based on the estimated number of affected `c_region` groups. The full
recompute alternative scans the base tables, produces the full result, deletes the old
MV rows, and inserts the new rows.

For a small database, the fixed cost of planning and executing the incremental path can
make full recompute cheaper even for a 1% change. For a larger database, the same 1%
change should usually favor incremental maintenance if the affected join fanout is not
too large.

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
