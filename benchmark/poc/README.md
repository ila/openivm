# View Matching POCs

Proof-of-concept experiments supporting the smart-view-matching research direction
(see `~/.claude/projects/-home-ila-Code-openivm/memory/project_view_matching_roadmap.md`).

## POC 1 — single-table aggregate (goldilocks_poc.py)

**Question**: does IVM refresh + scan beat running the query directly on base
for a single-table aggregate?

**Finding**: **NO**, not at any delta fraction, up to 2M rows.

| scale | IVM (ms) | bypass (ms) | speedup |
|---|---|---|---|
| 200K rows  | ~30 | ~15 | bypass 2x faster |
| 2M rows    | ~32 | ~20 | bypass 1.7x faster |

Why: DuckDB's vectorized GROUP BY over a single table is so fast that it
undercuts OpenIVM's SQL-compilation + MERGE overhead (~25-30ms fixed cost).
Single-table aggregate MVs don't benefit from IVM at these scales.

**Implication for paper**: single-table aggregate is NOT where IVM shines. The
matcher's IVM-vs-bypass decision should learn to pick bypass here.

Run: `python3 goldilocks_poc.py --base-rows 2000000 --reps 3`.

## POC 2 — 2-way join aggregate (join_poc.py)

**Question**: does IVM beat bypass when the query has a join, so bypass has
to re-run the join?

**Finding**: **YES**, with a clean Goldilocks zone that widens with scale.

### 2M lineitem scale (500K orders × 4)

| delta_frac | IVM (ms) | bypass (ms) | winner | speedup |
|---|---|---|---|---|
| 0.01%  | 31  | 44  | IVM    | 1.41x |
| 1%     | 37  | 45  | IVM    | 1.22x |
| 5%     | 42  | 46  | IVM    | 1.09x |
| 10%    | 49  | 44  | bypass | 0.90x |
| 20%    | 55  | 55  | tie    | 0.99x |
| 50%    | 81  | 51  | bypass | 0.64x |
| 100%   | 92  | 55  | bypass | 0.60x |

Crossover: ~10-15% delta.

### 6M lineitem scale (1.5M orders × 4) — TPC-H SF1-ish

| delta_frac | IVM (ms) | bypass (ms) | winner | speedup |
|---|---|---|---|---|
| 0.1%   | 35  | 103 | IVM    | 2.94x |
| 1%     | 40  | 90  | IVM    | 2.28x |
| 5%     | 50  | 89  | IVM    | 1.78x |
| 10%    | 58  | 98  | IVM    | 1.68x |
| 20%    | 70  | 103 | IVM    | 1.47x |
| 50%    | 103 | 106 | IVM    | 1.04x |

Crossover: beyond 50% — IVM wins across the entire realistic range at this scale.

**Implication for paper**:
- Goldilocks zone is real and reproducible.
- Zone **widens with base size** — at 6M rows, IVM wins up to 50% delta; at 2M,
  it narrows to 15%.
- Speedup at small deltas is 1.4-3x — meaningful but not order-of-magnitude.
- The crossover moves with scale AND query complexity, which is exactly what
  motivates **per-query cost-based** decisions rather than static heuristics.

Run: `python3 join_poc.py --n-orders 1500000 --avg-li 4 --reps 3`.

## What's missing — POC 2+ directions

- **Wider joins (3-4 tables)**. Should widen the zone further. N-way join cost
  scales super-linearly for bypass while IVM's inclusion-exclusion has 2^N terms
  but stays bounded.
- **MV-on-MV cascade**. Measure the four strategies: bypass / stale+residual /
  cascade-refresh-chain / full-recompute-chain. This is the real POC 2.
- **Stale + inline delta compensation** (Tier 2 novelty anchor). Requires
  extending OpenIVM or hand-writing the residual SQL. Compare against bypass
  for queries where the MV is stale by N rows.
- **Real workload fit**. Run on TPC-C (existing `rewriter_benchmark`) or the
  Phase-1 AI-generated query suite. Count queries in the crossover region.

## Files

- `goldilocks_poc.py` — POC 1, single-table aggregate.
- `join_poc.py` — 2-way join aggregate.
- `poc1_200k.csv`, `poc1_2m.csv` — POC 1 raw data.
- `poc2_2m_dense.csv`, `poc2_6m.csv` — POC 2 raw data.
- `smoke.csv`, `join_smoke.csv` — smoke-test artifacts (ignore).

## Caveats

- Single-node DuckDB, tempdir DB per run, cold cache every run. Consistent
  baseline but not production-realistic.
- OpenIVM's SQL-compilation overhead (~25-30ms fixed) is a dominant constant
  at these scales. Scaling base rows by 10x doesn't 10x the IVM cost.
- The `bypass` strategy here re-runs the exact MV definition query. In
  practice, bypass might be a subtly different query (matcher found a partial
  match). That subtlety is for the full matcher evaluation, not this POC.
- OpenIVM's `DROP MATERIALIZED VIEW` is not implemented yet, so we can't
  measure the "full MV rebuild" strategy as a separate option. Workaround:
  for single-MV, full-rebuild ≡ bypass (same query against base). For MV-on-MV,
  this will matter and needs extending OpenIVM.

## Encountered OpenIVM limitations

1. **`CREATE MATERIALIZED VIEW` parser strips newlines between `AS` and
   `SELECT`** — had to collapse to single-line SQL. Check
   `src/core/openivm_parser.cpp`.
2. **`DROP MATERIALIZED VIEW` returns "Cannot drop this type yet"** — blocks
   the rebuild-strategy measurement.

Both are minor paper-time blockers but worth filing as issues.
