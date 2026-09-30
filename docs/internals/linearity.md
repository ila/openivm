# Operator linearity

Each node in the delta model carries a **rule kind** that describes how its delta rule is
derived from the operator's algebra. The classification is captured in
`DeltaRuleKind`/`DeltaModelNode` (see `src/include/core/ivm_view_classifier.hpp`),
assigned by `RuleKindForNode` in `src/core/ivm_delta_model.cpp`, and the nodes are
compiled by the recursive delta operator planner under `src/delta/operators/`
(`CompileDeltaOperatorWithModel` in `dispatch.cpp` selects the rule by node kind):

```cpp
enum class DeltaRuleKind { LINEAR, PRODUCT, STATEFUL, NON_LINEAR, FULL_ONLY };
```

The taxonomy follows DBSP §6 (Budiu et al., VLDB 2023): linear, bilinear (`PRODUCT`),
and non-linear operators, with non-linear operators split into those OpenIVM maintains
with state or affected-key recompute (`STATEFUL`) and those it cannot maintain locally
(`NON_LINEAR`). Any node of a view with an unsupported construct is `FULL_ONLY`.

| Rule kind | Node kinds |
|---|---|
| `LINEAR` | Scan, filter, projection, UNION ALL, UNNEST, CTE, constant leaves |
| `PRODUCT` | Inner, cross, and arbitrary-predicate joins |
| `STATEFUL` | LEFT/RIGHT/FULL OUTER joins, ASOF joins, aggregates without MIN/MAX, `COUNT(DISTINCT)` or LIST, DISTINCT, SEMI/ANTI, window, top-k |
| `NON_LINEAR` | Aggregates with MIN/MAX/ARG_MIN/ARG_MAX, DISTINCT aggregates, or LIST; POSITIONAL joins; SAMPLE |
| `FULL_ONLY` | Every node when the view has an unsupported-construct reason |

## Rule kinds

### LINEAR

`Δ(Q(R)) = Q(ΔR)`. The rule applies the operator to the delta unchanged; no auxiliary
state is needed and cost is proportional to `|delta|`.

| Operator | Delta rule |
|---|---|
| Table scan | `CompileScanDelta` |
| Projection | `CompileProjectionDelta` |
| Filter | `CompileFilterDelta` |
| UNION ALL (bag union) | `CompileUnionDelta` |
| UNNEST | `CompileUnnestDelta` |
| CTE / CTE reference | `CompileCteDelta` |
| Constant leaf | `CompileConstantZeroDelta` |

Aggregates are classified `STATEFUL` even when summable: `CompileAggregateDelta` groups
the delta by the multiplicity column, but the net change must then be merged into the
stored group state. AVG and STDDEV/VARIANCE are decomposed into summable helper columns
before upsert compilation. Non-summable forms such as MIN/MAX deletes, LIST filters, and
non-summable output columns are routed to **group recompute** in `CompileAggregateGroups`.

### PRODUCT (bilinear)

Linear in each input separately. The delta rule expands to multiple terms, each weighted
by the **Z-set bilinear product** of leaf multiplicities. The default current-state
formulation uses a **Möbius inclusion-exclusion sign** and produces `2^N − 1` terms.
N-term telescoping produces at most N terms by mixing current and reconstructed old states.
OpenIVM uses telescoping for DuckLake joins and eligible regular-table refresh SQL compiled
for external engines.

| Operator | Delta rule |
|---|---|
| INNER JOIN, CROSS JOIN, arbitrary-predicate joins | `CompileJoinDelta` (inclusion-exclusion by default) |
| DuckLake telescoping join | `BuildDuckLakeJoinTerms` |
| Regular-table compile-only telescoping join | `BuildRegularJoinTerms` |

See [`operators/inner-join.md`](../operators/inner-join.md) for the algebraic derivation
of the combined-multiplicity formula.

### STATEFUL

Not linear, but maintainable from the delta plus stored state or an affected-key
recompute:

- **Stored group state** for aggregates (MERGE of summed deltas)
- **Auxiliary state** for threshold operators such as SEMI/ANTI and DISTINCT
- **Partition recompute** for affected partitions (window functions)
- **Outer-join key recompute or match counts** for LEFT/RIGHT/FULL OUTER joins

| Operator | Delta rule | State |
|---|---|---|
| Aggregate | `CompileAggregateDelta` | MV group state, MERGE |
| LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN | `CompileJoinDelta` plus outer-join upsert paths | Preserved-side keys, match counts |
| `DISTINCT` (δ in DBSP) | `CompileDistinctDelta` | COUNT(*) sentinel or distinct-count aux table |
| `SEMI JOIN`, `ANTI JOIN` | `CompileDelimJoinDelta` / `CompileJoinDelta` + aux-state upsert | Match-count threshold state |
| Window functions | `CompileWindowDelta` | Partition recompute |
| Top-k (`ORDER BY` + `LIMIT`) | `CompileTopKDelta` (strips the limit) | Unlimited backing table |
| ASOF join | Guard only (`CompileAsofJoinDelta`) | Maintained by `WINDOW_PARTITION`, `GROUP_RECOMPUTE`, or full refresh |

DISTINCT is non-linear *even on positive Z-sets* — it drops duplicates, which can't be
expressed as a sum over deltas. SEMI and ANTI joins are threshold operators over
right-side match counts. Window functions (ROW_NUMBER, RANK, NTILE, LAG, LEAD,
running aggregates) depend on partition order; a single insert/delete can re-rank
every row in the partition.

### NON_LINEAR

The delta requires the *accumulated* state of an input and has no local rule.
Aggregates with MIN/MAX (and ARG_MIN/ARG_MAX), DISTINCT aggregates, or LIST use
group recompute or an aux table (`COUNT_DISTINCT_INCREMENTAL`). POSITIONAL joins and
SAMPLE have guard-only rules (`CompilePositionalJoinDelta`, `CompileSampleDelta`) and
are always classified `FULL_REFRESH`.

## Why this matters

The rule kind is a **document-time invariant**: it tells you what cost to expect
and what state OpenIVM has to maintain to keep the MV correct. The delta model derives
`DeltaUpdateSemantics` alongside it (`AddNodeUpdateSemantics`), which gates the
`append-only` optimisation (see [`optimizations/append-only.md`](../optimizations/append-only.md)).
Projections and linear aggregates are append-only safe; joins, DISTINCT, SEMI/ANTI, and
window nodes are marked delete- and update-sensitive.

Adding a new operator should start with: pick the linearity class, then derive the
delta rule that the class permits.
