# Configuration

All OpenIVM settings are DuckDB extension options — set them with `SET` (session-global) and read
them with `current_setting('<name>')`:

```sql
SET openivm_refresh_mode = 'full';
SELECT current_setting('openivm_adaptive_refresh');
```

This page is the authoritative list of every setting OpenIVM registers. Two things to know up front:

- **Precedence:** `openivm_refresh_mode = 'full'` is a hard override — it forces full recompute
  regardless of the cost model. `openivm_adaptive_refresh` (the cost-based incremental-vs-full
  decision) only applies when the mode is `incremental` or `auto`.
- **User policy vs internal tuning:** the *refresh policy*, *daemon*, and *profiling* settings are
  the ones you'd normally touch. The *optimization toggles* default to on
  and exist mainly to disable a specific incremental optimization for debugging or A/B measurement —
  most users never change them.

## Refresh policy

| Setting | Type | Default | Description |
|---|---|---|---|
| `openivm_refresh_mode` | VARCHAR | `incremental` | Refresh strategy: `incremental`, `full`, or `auto`. `full` hard-overrides the cost model. |
| `openivm_adaptive_refresh` | BOOLEAN | `false` | Experimental: enable the adaptive cost model (learned regression + incremental-vs-recompute decision). When off, always use IVM. |
| `openivm_cost_decay` | DOUBLE | `0.9` | Decay factor for the learned cost-model regression (0.0–1.0; higher = slower adaptation to recent runs). |
| `openivm_cascade_refresh` | VARCHAR | `downstream` | Cascade mode for dependent MVs: `off`, `upstream`, `downstream`, or `both`. |

See also: [`refresh/refresh-strategies.md`](refresh/refresh-strategies.md),
[`internals/cost_model.md`](internals/cost_model.md).

## Daemon / scheduling

| Setting | Type | Default | Description |
|---|---|---|---|
| `openivm_adaptive_backoff` | BOOLEAN | `true` | Auto-increase a view's refresh interval when a refresh takes longer than the interval. |
| `openivm_disable_daemon` | BOOLEAN | `false` | Disable the background refresh daemon (for shadow / compile-only databases). |

See also: [`refresh/automatic-refresh.md`](refresh/automatic-refresh.md).

## Optimization toggles (default on; advanced)

These enable specific incremental-maintenance optimizations. They are on by default; set one to
`false` to fall back to the safe general path (useful for debugging or measuring the optimization's
effect). Correctness does not depend on them.

| Setting | Type | Default | Description |
|---|---|---|---|
| `openivm_skip_empty_deltas` | BOOLEAN | `true` | Skip the whole refresh, or individual inclusion-exclusion join terms, when the relevant deltas are empty. |
| `openivm_compact_deltas` | BOOLEAN | `true` | Compact raw delta rows into net logical Z-set deltas (sum multiplicities, drop zeros) before refresh. |
| `openivm_fk_pruning` | BOOLEAN | `true` | Prune inclusion-exclusion join terms that algebraically cancel under FK constraints. |
| `openivm_skip_aggregate_delete` | BOOLEAN | `true` | Skip the zero-row DELETE phase for grouped aggregates when deltas are insert-only. |
| `openivm_skip_projection_delete` | BOOLEAN | `true` | Skip DELETE and consolidation for projections when deltas are insert-only. |
| `openivm_minmax_incremental` | BOOLEAN | `true` | Use `GREATEST`/`LEAST` for MIN/MAX when deltas are insert-only (vs group-recompute). |
| `openivm_ducklake_nterm` | BOOLEAN | `true` | Use N-term telescoping for DuckLake joins instead of 2^N−1 inclusion-exclusion. |
| `openivm_having_merge` | BOOLEAN | `true` | Use MERGE for HAVING views (store all groups; the user-facing view applies the predicate) instead of group-recompute. |
| `openivm_left_join_merge` | BOOLEAN | `true` | Use incremental MERGE for LEFT JOIN aggregates (Larson & Zhou) instead of group-recompute. |
| `openivm_full_outer_merge` | BOOLEAN | `true` | Use incremental MERGE for FULL OUTER JOIN aggregates (Zhang & Larson) instead of group-recompute. |
| `openivm_distinct_aux_state` | BOOLEAN | `false` | Use auxiliary count state for inner DISTINCT under an aggregate (emit ±1 only on count transitions across zero) instead of affected-group recompute. Single-source views only in v0. |
| `openivm_enable_data_dependent_optimizers` | BOOLEAN | `false` | Experimental / unsafe: keep data-dependent optimizers (`statistics_propagation`) enabled while planning the delta template. Off by default because the template is reused against future data — folding current contents (count→constant, predicate→empty, COALESCE→column) freezes/corrupts the delta. |

See also: [`optimizations/empty-delta-skip.md`](optimizations/empty-delta-skip.md),
[`optimizations/append-only.md`](optimizations/append-only.md),
[`optimizations/fk-aware-pruning.md`](optimizations/fk-aware-pruning.md),
[`optimizations/delta-consolidation.md`](optimizations/delta-consolidation.md),
[`optimizations/delta-skipping-rules.md`](optimizations/delta-skipping-rules.md), [`ducklake.md`](ducklake.md),
[`operators/left-join.md`](operators/left-join.md),
[`operators/full-outer-join.md`](operators/full-outer-join.md),
[`operators/distinct.md`](operators/distinct.md).

## Profiling / debug / IO

| Setting | Type | Default | Description |
|---|---|---|---|
| `openivm_files_path` | VARCHAR | (none) | Directory for compiled-SQL reference files (`openivm_compiled_queries_*`, `openivm_upsert_queries_*`); when set, OpenIVM writes the generated SQL there for inspection. |
| `openivm_profile_refresh` | BOOLEAN | `false` | Record per-step materialized-view refresh timings into `openivm_refresh_profile`. |
| `openivm_profile_retention_days` | BIGINT | `31` | When profiling is written, delete profile rows older than this many days. |
| `openivm_explain_initial_load` | BOOLEAN | `false` | Print the `CREATE MATERIALIZED VIEW` initial-load SQL and its EXPLAIN plans. |
| `openivm_explain_initial_load_only` | BOOLEAN | `false` | Print only the initial-load explain output, then stop (do not run the initial load). |

See also: [`refresh/automatic-refresh.md`](refresh/automatic-refresh.md).
