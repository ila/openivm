# State and storage overhead

Incremental maintenance is not free: to refresh a view without rescanning its base tables, OpenIVM
keeps extra state on disk. This page explains **what is stored, for which kinds of view, and why**,
so you can reason about the space overhead of a materialized view before you create it.

All of this state is internal. The columns and tables below are hidden behind the user-facing view;
`SELECT * FROM <view>` never shows them.

## Always present

Every materialized view carries two pieces of storage:

| What | Named | Holds | Rough size |
|---|---|---|---|
| **MV data table** | `openivm_data_<view>` | the materialized result, plus any hidden columns (below) | ~ the size of the view result |
| **Delta tables** | `openivm_delta_<table>`, one per source table | the changes (inserts/deletes) not yet folded into the view | grows with un-refreshed changes; consumed on refresh |

A delta table is the base table's columns plus a signed multiplicity and a timestamp (see
[Metadata columns](metadata-columns.md)). Multiple views can share one delta table; each consumes
only the rows newer than its own last refresh. Delta rows are cleared once every view that needs
them has consumed them, so delta storage is bounded by your refresh cadence — refresh more often to
keep it small.

## Hidden columns on the data table

Depending on what the view computes, the data table grows extra columns so the result can be
maintained additively instead of recomputed. These cost a few bytes per stored row.

| View computes | Extra stored per row | Why |
|---|---|---|
| `COUNT(*)` / grouped aggregate | a group count | lets a group be deleted when it empties out |
| `AVG(x)` | hidden sum + count of `x` | the visible average is recomputed from the two running totals |
| `STDDEV(x)` / `VARIANCE(x)` | hidden sum + sum-of-squares + count of `x` | variance is recomputed from the three running totals |
| `SELECT DISTINCT` | a per-row count | the row leaves the view only when its count reaches zero |
| `HAVING` | (nothing extra per row, but **all groups are stored**) | the data table keeps every group; the user-facing view applies the `HAVING` predicate at read time, so groups can cross the threshold both ways without a recompute |
| `LEFT`/`RIGHT JOIN` | the preserved-side join key, a match count | lets the affected keys be located and NULL-padding transitions handled |
| `FULL OUTER JOIN` | join keys and match counts for both sides | either side's changed key can delete and reinsert the affected rows |

## Auxiliary state tables (some view types only)

A few view types keep a **separate** auxiliary table beyond the data table, because the correct
delta depends on accumulated counts that aren't visible in the result itself.

| View type | Auxiliary state | Shape | Overhead |
|---|---|---|---|
| Inner `DISTINCT` under an aggregate, with `openivm_distinct_aux_state = true` | a per-distinct-tuple count table | one row per distinct input tuple `(distinct cols, count)` | proportional to the number of distinct input tuples — can exceed the view itself if the inner DISTINCT is much larger than the grouped result |
| `SEMI`/`ANTI`/`EXISTS` projection shapes | a per-left-tuple match-count table | one row per left tuple `(left cols, match count)` | proportional to the left input |
| Filtered grouped aggregates (`… FILTER (WHERE …)`) | a per-group filtered count | one row per group | proportional to the number of groups |

When the corresponding setting is off (or the shape isn't supported), these views fall back to
**affected-group recompute**, which keeps **no persistent auxiliary state** — it re-derives the
affected groups from the base tables at refresh time instead. That trades disk for compute: no extra
storage, but a heavier refresh.

### Strategies that keep no persistent aux state

Recompute-based strategies store nothing beyond the data table — they re-read the base data for the
affected scope on each refresh:

- **Affected-group recompute** — distinct aggregates, multi-level groupings (`GROUPING SETS`/`ROLLUP`/`CUBE`), non-summable aggregate outputs.
- **Affected-partition recompute** — window functions.
- **Current-diff / affected-row recompute** — `ASOF JOIN`, `POSITIONAL JOIN`, some union-of-aggregate shapes.
- **Full refresh** — unsupported constructs (recomputes the whole view).

## Per-view metadata facts

OpenIVM records a small set of per-view facts in its system catalog (`openivm_views`) so that each
refresh knows how to maintain the view without re-analyzing the query. Several are stored as JSON:

| Fact | Records |
|---|---|
| `source_tables_json` | the base tables the view depends on, for dependency tracking and cascades |
| `aggregate_decomposition_json` | how each aggregate was decomposed (e.g. AVG → sum + count) and any filtered group-count auxiliary state |
| `distinct_aux_meta_json` | the distinct columns, grouping, and aggregate wiring for the DISTINCT auxiliary-state path |
| `semi_anti_aux_meta_json` | the left/right sources, predicate, and key columns for SEMI/ANTI auxiliary-state maintenance |
| `lineage_json` | the column lineage used to locate affected partitions/keys for window and projection-key recompute |

These are compact (one short JSON value per view) and exist only to make refresh deterministic; they
don't grow with the data. A further set of view-matching fact columns exists but stays empty unless
[view matching](../configuration.md) is enabled.

## Rules of thumb

- A plain projection/filter or summable grouped aggregate adds only a handful of bytes per stored
  row — overhead is negligible.
- `AVG`/`STDDEV`/`VARIANCE` add a couple of hidden numeric columns per aggregated column.
- `HAVING` views store **all** groups, including the ones currently filtered out — size by the full
  group count, not the visible result.
- The DISTINCT and SEMI/ANTI auxiliary tables are the only state that can rival or exceed the view
  in size; everything else scales with the result or with un-refreshed deltas.
- Recompute strategies cost **no** persistent state but a heavier refresh — the trade-off is disk
  vs. compute.
