# Operator linearity

Every operator in a view is maintained according to its **linearity class** — how its output delta
relates to its input delta. The class determines what a refresh has to do and what state, if any,
OpenIVM must keep. This is the same taxonomy DBSP uses (Budiu et al., VLDB 2023) and it determines
the shape of `ΔQ` for an operator `Q`.

## The three classes

### LINEAR

`Δ(Q(R)) = Q(ΔR)`. The operator is applied to the delta unchanged; no auxiliary state is needed and
the cost is proportional to `|delta|`.

| Operator | Delta behavior |
|---|---|
| Table scan | reads the delta directly |
| Projection | applies the projection to the delta |
| Filter | applies the predicate to the delta |
| UNION ALL (bag union) | concatenates branch deltas |
| `SUM`, `COUNT` (over linear inputs) | the multiplicity is carried through as part of the group key |

Aggregate maintenance is structurally linear for summable aggregates: the signed multiplicity is
folded into the running totals. `AVG` and `STDDEV`/`VARIANCE` are decomposed into linear helper
columns (sum, count, sum-of-squares) so they too maintain additively. Non-linear aggregate forms
(MIN/MAX with deletes, distinct aggregates, non-summable output columns) are detected at view
creation and routed to **affected-group recompute** instead, so the per-operator classification
stays clean.

### BILINEAR

Linear in each input separately. The delta rule expands to multiple terms, each weighted by the
**Z-set bilinear product** of leaf multiplicities times a **Möbius inclusion-exclusion sign**. For an
N-table inner join this is `2^N − 1` terms; for a DuckLake telescoping join it is exactly N.

| Operator | Notes |
|---|---|
| INNER JOIN, CROSS JOIN, arbitrary-predicate joins | inclusion-exclusion over delta/current legs |
| LEFT JOIN, RIGHT JOIN, FULL OUTER JOIN | inclusion-exclusion plus outer-join upsert handling |
| DuckLake telescoping join | N-term snapshot-based form |

See [`operators/inner-join.md`](../operators/inner-join.md) for the algebraic derivation of the
combined-multiplicity formula.

### NON_LINEAR

Neither linear nor bilinear. The delta depends on the *accumulated* state of one or more inputs —
there is no closed-form per-row rule. OpenIVM keeps these correct by re-deriving only the affected
scope, or by maintaining a small amount of auxiliary state:

- **Auxiliary state** for threshold operators such as SEMI/ANTI joins (per-tuple match counts)
- **Affected-group recompute** for distinct, MIN/MAX with deletes, multi-level groupings, and
  non-summable aggregate outputs
- **Affected-partition recompute** for window functions
- **Full refresh** when none of the above fits

| Operator | How it stays correct |
|---|---|
| `DISTINCT` (δ in DBSP) | hidden count per distinct row; the row leaves the view when its count reaches zero |
| `COUNT/SUM/AVG(DISTINCT)`, `GROUPING SETS`/`ROLLUP`/`CUBE` | affected-group recompute |
| `SEMI JOIN`, `ANTI JOIN` | per-tuple match-count auxiliary state |
| Window functions | affected-partition recompute |
| `ASOF JOIN`, `POSITIONAL JOIN` | affected-row recompute (order/position sensitive) |

DISTINCT is non-linear *even on positive Z-sets* — it drops duplicates, which can't be expressed as
a sum over deltas. SEMI and ANTI joins are threshold operators over right-side match counts. Window
functions depend on partition order; a single insert or delete can re-rank every row in the
partition.

## Why this matters

The linearity class is a **view-creation invariant**: it tells you the cost to expect and what state
OpenIVM has to maintain to keep the view correct. It also gates the
[append-only optimization](../optimizations/append-only.md) — LINEAR operators preserve insert-only
semantics through the delta pipeline, while BILINEAR joins do not (cross-terms produce negative
weights via the Möbius sign), which is why that optimization only fires for single-delta-table joins.

Adding support for a new operator starts with: pick its linearity class, then derive the maintenance
the class permits.
