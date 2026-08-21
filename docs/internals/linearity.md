# Operator linearity

Each node in the delta model carries a **rule kind** that determines how its delta rule is
derived from the operator's algebra. The classification is captured in
`DeltaRuleKind`/`DeltaModelNode` (see `src/include/core/ivm_view_classifier.hpp`) and
compiled by the recursive delta operator planner under `src/delta/operators/`:

```cpp
enum class DeltaRuleKind { LINEAR, PRODUCT, STATEFUL, NON_LINEAR, FULL_ONLY };
```

This taxonomy is the same one DBSP §6 uses (Budiu et al., VLDB 2023) and determines the
shape of `ΔQ` for an operator `Q`.

## The three classes

### LINEAR

`Δ(Q(R)) = Q(ΔR)`. The rule applies the operator to the delta unchanged; no auxiliary
state is needed and cost is proportional to `|delta|`.

| Operator | Delta rule |
|---|---|
| Table scan | `CompileScanDelta` |
| Projection | `CompileProjectionDelta` |
| Filter | `CompileFilterDelta` |
| UNION ALL (bag union) | `CompileUnionDelta` |
| `SUM`, `COUNT` (over linear inputs) | propagated through `CompileAggregateDelta` |

The aggregate delta compiler is structurally LINEAR for summable aggregates: it passes the
multiplicity column through as a group-by key. AVG and STDDEV/VARIANCE are decomposed into linear helper
columns before upsert compilation. Non-linear aggregate forms such as MIN/MAX deletes,
LIST filters, and non-summable output columns are detected at compile time and routed
to **group recompute** in `CompileAggregateGroups`, so the per-rule classification stays
clean.

### BILINEAR

Linear in each input separately. The delta rule expands to multiple terms, each weighted
by the **Z-set bilinear product** of leaf multiplicities. The default current-state
formulation uses a **Möbius inclusion-exclusion sign** and produces `2^N − 1` terms.
N-term telescoping produces at most N terms by mixing current and reconstructed old states.
OpenIVM uses telescoping for DuckLake joins and eligible regular-table refresh SQL compiled
for external engines.

| Operator | Delta rule |
|---|---|
| INNER JOIN, CROSS JOIN, arbitrary-predicate joins | `CompileJoinDelta` |
| LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN | `CompileJoinDelta` plus outer-join upsert paths |
| DuckLake telescoping join | `BuildDuckLakeJoinTerms` |
| Regular-table compile-only telescoping join | `BuildRegularJoinTerms` |

See [`operators/inner-join.md`](../operators/inner-join.md) for the algebraic derivation
of the combined-multiplicity formula.

### NON_LINEAR

Neither linear nor bilinear. The delta requires the *accumulated* state of one or more
inputs — there is no closed-form per-row rule. OpenIVM falls back to:

- **Auxiliary state** for threshold operators such as SEMI/ANTI
- **Group recompute** for affected groups (DISTINCT, MIN/MAX with deletes, LIST filters)
- **Partition recompute** for affected partitions (window functions)
- **Full refresh** when neither fits

| Operator | Delta rule | Fallback |
|---|---|---|
| `DISTINCT` (δ in DBSP) | `CompileDistinctDelta` | Group recompute via COUNT(*) sentinel |
| `SEMI JOIN`, `ANTI JOIN` | `CompileDelimJoinDelta` + aux-state upsert | Match-count threshold state |
| Window functions | `CompileWindowDelta` | Partition recompute |

DISTINCT is non-linear *even on positive Z-sets* — it drops duplicates, which can't be
expressed as a sum over deltas. SEMI and ANTI joins are threshold operators over
right-side match counts. Window functions (ROW_NUMBER, RANK, NTILE, LAG, LEAD,
running aggregates) depend on partition order; a single insert/delete can re-rank
every row in the partition.

## Why this matters

The rule kind is a **document-time invariant**: it tells you what cost to expect
and what state OpenIVM has to maintain to keep the MV correct. It also gates the
`append-only` optimisation (see [`optimizations/append-only.md`](../optimizations/append-only.md)).
LINEAR operators preserve insert-only semantics directly. For BILINEAR joins, safety
depends on the selected join rule and whether its emitted delta contains negative weights.

Adding a new operator should start with: pick the linearity class, then derive the
delta rule that the class permits.
