---
name: explain-ivm
description: Reference material for Incremental View Maintenance concepts. Auto-loaded when discussing IVM theory, delta computation, or view maintenance algorithms.
---

## Incremental View Maintenance (IVM) Overview

IVM is the problem of efficiently updating materialized views when the underlying
base tables change, without recomputing the view from scratch.

### Core Idea

Given a view V = Q(T₁, T₂, ..., Tₙ), when a base table Tᵢ changes by ΔTᵢ,
compute ΔV such that V_new = V_old + ΔV. The cost of computing ΔV should be
proportional to |ΔTᵢ|, not |Tᵢ|.

### Delta Rules by Operator

**Projection** (π): Δ(π(R)) = π(ΔR)
- Deltas pass through projections unchanged.

**Selection/Filter** (σ): Δ(σ_p(R)) = σ_p(ΔR)
- Apply the same filter predicate to the delta.

**Join** (⋈): Δ(R ⋈ S) for changes ΔR:
- Simple (one table changes): ΔR ⋈ S
- Both tables change (inclusion-exclusion for N tables):
  - Generate 2^N - 1 terms for all non-empty subsets of tables
  - Each term replaces subset tables with their deltas
  - Sign/multiplicity handled via XOR of boolean multiplicities

**Aggregation** (γ):
- **SUM**: V_new[g] = V_old[g] + Σ(delta values for group g)
- **COUNT**: V_new[g] = V_old[g] + |inserts for g| - |deletes for g|
- **MIN/MAX**: Requires auxiliary data or recomputation when the current min/max is deleted
- **AVG**: Maintained as SUM/COUNT

### Multiplicity (Bag Semantics)

IVM operates on bags (multisets), not sets. Each tuple has a multiplicity:
- +1 for insertions, -1 for deletions
- OpenIVM uses BOOLEAN: `true` = insert (+1), `false` = delete (-1)

### Counting Algorithm (for aggregates)

The standard approach for maintaining aggregates with GROUP BY:
1. For each group key in the delta, compute the aggregate delta
2. If the group exists in the view: update in place (add delta to existing value)
3. If the group is new: insert a new row
4. If the group's count reaches 0: delete the row

### IVM vs Full Recompute

IVM is faster when |ΔT| << |T|. But when deltas are large relative to the base
tables, full recomputation may be cheaper. The **adaptive cost model** compares:
- IVM cost ∝ |ΔT| × (join fan-out, aggregation cost)
- Recompute cost ∝ |T₁| × |T₂| × ... (full query cost)

### Related Systems

| System | Approach | Key Feature |
|---|---|---|
| **DBToaster** | Higher-order IVM | Recursive delta compilation, auxiliary views |
| **DBSP** | Algebraic (Z-sets) | Streams, incremental operators, Feldera runtime |
| **pg_ivm** | Trigger-based | PostgreSQL extension, counting algorithm |
| **Snowflake** | Micro-partition tracking | Dynamic tables, target lag, DAG pipelines |
| **Materialize** | Dataflow (Timely/DD) | Shared arrangements, streaming joins |
| **Noria** | Dataflow | Partially-stateful operators, eviction |

### Key Papers

- Gupta & Mumick (1999) — "Maintenance of Materialized Views: Problems, Techniques, and Applications" (survey)
- Koch et al. (2014) — "DBToaster: Higher-order Delta Processing for Dynamic, Frequently Fresh Views" (VLDB)
- Budiu et al. (2023) — "DBSP: Automatic Incremental View Maintenance" (VLDB)
- OpenIVM paper — arxiv.org/abs/2404.16486
