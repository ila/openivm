# Performance: when is IVM a good fit?

Incremental view maintenance trades a cheaper refresh for some extra storage and a more complex
update path. It is a clear win for some workloads and a clear loss for others. This page is a
practical guide to telling them apart.

> **Note:** the guidance below is the model behind OpenIVM's design and cost estimates. Quantitative
> benchmarks (refresh latency vs. delta size, per-operator break-even points) are still being
> collected and will be added here.

## The core trade-off

A full recompute costs about *"recompute the whole view from the base tables"*. An incremental
refresh costs about *"process only the changes, then apply them to the stored result"*. So the
question is simply:

> Is processing the changes (plus applying them) cheaper than recomputing the whole view?

That holds when **the changes are small relative to the base data** and **the view is expensive to
compute**. It fails when a refresh touches most of the view anyway, or when the view is so cheap to
recompute that the bookkeeping isn't worth it.

## IVM tends to win when…

- **Deltas are small vs. the base.** A few thousand changed rows against a multi-million-row table
  is the sweet spot. The smaller the change ratio, the bigger the win.
- **The view is costly to recompute.** Multi-table joins, large aggregations, and anything that
  scans a lot of data to produce a comparatively small result benefit most — the work you *avoid*
  on each refresh is large.
- **The view is refreshed often.** The maintenance state is amortized across many refreshes; high
  refresh frequency favors paying once to store state and refreshing cheaply.
- **The operators are linear.** Projections, filters, `UNION ALL`, and summable grouped aggregates
  (`SUM`, `COUNT`, and the decomposed `AVG`/`STDDEV`/`VARIANCE`) maintain in time proportional to the
  delta, touching nothing else. These are the cheapest to keep up to date. See
  [operator linearity](linearity.md).

## IVM tends to lose when…

- **Deltas are a large fraction of the base.** Once a refresh has to revisit most of the view, you
  are doing recompute-sized work plus delta bookkeeping — full recompute is simpler and faster.
- **The view is trivially cheap to recompute.** A small single-table projection may recompute faster
  than the cost of tracking and applying deltas.
- **The operators are non-linear and the affected scope is wide.** `DISTINCT`-based aggregates,
  multi-level groupings, window functions, and `ASOF`/`POSITIONAL` joins are maintained by
  *recomputing an affected scope* (a set of groups, a partition, or a span of rows) rather than by a
  closed-form per-row delta. If a single change touches a large partition or many groups, that scope
  approaches a full recompute. These still avoid rescanning unrelated data, but the per-change cost
  is higher than for linear operators.

## The delta-size crossover

For a fixed view, refresh cost grows with delta size. Full-recompute cost is roughly constant
(it always does the whole view). Plotting both against delta size, they cross at some point:

- **Below the crossover** — small deltas — incremental refresh is faster.
- **Above the crossover** — large deltas — full recompute is faster.

Where the crossover sits depends on the view: expensive, join-heavy, linear views push it high (IVM
wins even for fairly large deltas); cheap or non-linear views push it low. As a rough mental model,
once a batch of changes approaches a sizable fraction of the base table, lean toward full recompute.

## Letting OpenIVM decide

You don't have to choose per refresh. The [adaptive cost model](cost_model.md) estimates both paths
and picks the cheaper one when enabled:

```sql
SET openivm_adaptive_refresh = true;   -- estimate incremental vs full, pick the cheaper
```

It combines a static estimate (input sizes, operator strategy, delta sizes) with a learned
correction from observed refresh times, and it accounts for the recompute-style strategies that
don't follow the simple linear-delta model. Inspect a view's current estimate with:

```sql
PRAGMA refresh_cost('view_name');
```

When in doubt for a workload with unpredictable delta sizes, enabling adaptive refresh is the safe
default: it keeps IVM's advantage for small deltas and falls back to full recompute when a batch is
large. To force one path regardless, set `openivm_refresh_mode` to `incremental` or `full`.

## Reducing overhead

- **Refresh often enough to keep deltas small** — this keeps you on the winning side of the
  crossover and bounds delta-table storage.
- **Keep views linear where you can** — a summable aggregate maintains far more cheaply than a
  distinct or windowed one over the same data.
- **Mind the state cost** of `HAVING` (stores all groups) and the DISTINCT / SEMI-ANTI auxiliary
  tables — see [State and storage overhead](state-and-overhead.md).
