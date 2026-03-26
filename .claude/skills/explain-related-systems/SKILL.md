---
name: explain-related-systems
description: Reference material for IVM-related systems and papers — DBSP, DBToaster, pg_ivm, Snowflake Dynamic Tables, Materialize. Auto-loaded when discussing related work, comparisons, or IVM literature.
---

## Related Systems and Papers

### OpenIVM Paper

**"OpenIVM: a SQL-to-SQL Compiler for Incremental Computations"**
Ilaria Battiston (CWI), Kriti Kathuria (U. Waterloo), Peter Boncz (CWI)
SIGMOD-Companion '24, Santiago, Chile. [arxiv.org/abs/2404.16486](https://arxiv.org/abs/2404.16486)

Key contributions:
- SQL-to-SQL compiler for IVM — all maintenance expressed as standard SQL, no separate engine
- Uses DuckDB as a library for parsing/planning, outputs SQL in any dialect
- Follows DBSP framework (Z-sets, differentiation/integration operators)
- Multiplicity tracked as boolean column (`_duckdb_ivm_multiplicity`)
- 4-step propagation: insert ΔV, upsert into V, delete zeroed rows, cleanup deltas
- Supports projections, filters, GROUP BY, SUM, COUNT; joins via inclusion-exclusion
- Cross-system: compile once, execute on any SQL engine (DuckDB, PostgreSQL, etc.)

---

### DBSP (Database Stream Processor)

**"DBSP: Automatic Incremental View Maintenance for Rich Query Languages"**
Mihai Budiu, Tej Chajed, Frank McSherry, Leonid Ryzhyk, Val Tannen
VLDB 2023 (Best Paper Award). [arXiv:2203.16684](https://arxiv.org/abs/2203.16684)

**Core model:**
- **Z-sets**: Functions from tuples to integers (weights). Positive = present, negative = deleted.
  Form an abelian group under pointwise addition. Unify set/bag/delta semantics.
- **Streams**: Infinite sequences of Z-sets indexed by time. Each element = delta at that timestep.
- **Four fundamental operators**: Lifting (pointwise), Delay (z⁻¹), Differentiation (D), Integration (I).
  D and I are inverses: I(D(s)) = s.

**Incrementalization formula:** For any query Q: `Q^delta = D ∘ lift(Q) ∘ I`
(integrate deltas → apply query → differentiate output). Composes: `(Q1 ∘ Q2)^delta = Q1^delta ∘ Q2^delta`.

**Incremental operators:**
- **Linear** (filter, project, union, map): `Q^delta = Q` — just apply to delta. Free.
- **Bilinear** (join): `Δ(R ⋈ S) = ΔR ⋈ ΔS + z⁻¹(I(R)) ⋈ ΔS + ΔR ⋈ z⁻¹(I(S))`
  (new-new + old-new + new-old). Cost: O(|DB| × |Δ|).
- **Non-linear** (DISTINCT): Requires accumulated state via I. Most expensive.

**Aggregation:**
- SUM, COUNT: Linear in weights → cheap incremental.
- AVG: Decompose to SUM/COUNT.
- MIN, MAX: Non-linear, non-monotone → need full group state.
- GROUP BY: Linear (just redistributes by key).

**Indexed Z-sets:** `Map<K, Z[V]>` — models GROUP BY. Each key maps to a Z-set of values.
Also an abelian group, enabling incremental group-level updates.

**Implementation:** Feldera (feldera.com) — commercial streaming SQL engine built on DBSP.

**Relevance to OpenIVM:** DBSP provides the theoretical foundation. OpenIVM follows DBSP's
differentiation/integration operators and Z-set semantics. Key difference: DBSP is a
streaming dataflow runtime; OpenIVM compiles to SQL statements executed on existing engines.

---

### DBToaster

**"DBToaster: Higher-order Delta Processing for Dynamic, Frequently Fresh Views"**
Yanif Ahmad, Oliver Kennedy, Christoph Koch, Milos Nikolic
VLDB 2012, extended in VLDB Journal 2014.

**Core idea: Higher-order IVM.**
If maintaining V incrementally via delta queries is good, then maintaining the delta
queries incrementally is even better. Recurse until deltas become trivially computable.

- **First-order**: V_new = V_old + dQ(Δ)
- **Second-order**: Materialize dQ as a view, maintain it with d(dQ)
- **Third-order and beyond**: Continue until no joins remain in delta expressions

Each successive delta is structurally simpler (fewer joins). For acyclic queries with
N relations, terminates at N-th order. Hierarchical queries: O(1) per single-tuple update.

**Compilation approach:**
- Input: SQL query → Output: standalone C++ or Scala code (no database needed at runtime)
- Frontend (OCaml): Parses SQL, converts to AGCA ring algebra, derives deltas
- Backend (Scala/LMS): Generates trigger functions — one per (relation, update-type) pair
- Auxiliary data stored in in-memory hash maps ("maps") indexed by free variables

**Delta rules (AGCA ring):**
- Bag union: d(Q1 + Q2) = dQ1 + dQ2
- Join/product: d(Q1 × Q2) = dQ1 × Q2 + Q1 × dQ2 + dQ1 × dQ2
- Aggregation: d(AggSum([], Q)) = AggSum([], dQ)

**Supported:** SUM, COUNT, COUNT DISTINCT, AVG, equi/theta joins, nested subqueries.
**Not supported:** MIN, MAX (no group inverse), OUTER JOIN, DISTINCT, UNION, window functions.

**Performance:** 3-6 orders of magnitude faster than traditional DBMS for view refresh.
TPC-H queries: 10K-100K+ updates/second on single core.

**Relevance to OpenIVM:** DBToaster's higher-order approach materializes auxiliary views
for faster updates — a more aggressive strategy than OpenIVM's current first-order approach.
The adaptive materialization research direction (C4) explores when to adopt DBToaster-style
auxiliary maps vs. OpenIVM's current compile-and-execute SQL approach.

---

### pg_ivm (PostgreSQL IVM)

**GitHub:** [sraoss/pg_ivm](https://github.com/sraoss/pg_ivm)
**Author:** Yugo Nagata (SRA OSS, Japan). Actively maintained, v1.13 (Oct 2025).
Supports PostgreSQL 13-18.

**Approach:** Trigger-based immediate maintenance using PostgreSQL's transition tables.

**Change tracking:**
- AFTER triggers installed on all base tables when IMMV is created via `create_immv()`
- PostgreSQL transition tables provide OLD TABLE (before) and NEW TABLE (after) rows
- Maintenance is immediate (within the same transaction)

**Algorithm:** Based on Larson & Zhou (2007). Steps:
1. Analyze view definition → generate maintenance graph
2. Compute delta queries by replacing base table with transition table in query tree
3. Apply deltas: INSERT/DELETE/UPDATE on the IMMV

**Counting algorithm (hidden columns):**
- `__ivm_count__`: Tuple multiplicity for DISTINCT views (deleted when count → 0)
- `__ivm_count_<agg>__`: Non-NULL input count per aggregate
- `__ivm_sum_<agg>__`: Running sum for AVG maintenance

**Supported:** SPJ, DISTINCT, GROUP BY, count/sum/avg/min/max, simple subqueries,
EXISTS in WHERE, CTEs, outer joins (v1.13, with restrictions).
**Not supported:** HAVING, LIMIT, UNION/INTERSECT/EXCEPT, window functions, UDAs.

**min/max handling:** Not purely incremental — when current min/max is deleted, the
entire group is rescanned to find the new value. This is a known limitation.

**Relevance to OpenIVM:** pg_ivm is the closest PostgreSQL equivalent. Key differences:
- pg_ivm uses triggers (immediate); OpenIVM uses optimizer rules (lazy by default)
- pg_ivm operates on query trees in C; OpenIVM compiles to SQL strings
- pg_ivm is PostgreSQL-only; OpenIVM targets any SQL engine via SQL-to-SQL compilation
- Both use counting/multiplicity for bag semantics

---

### Snowflake Dynamic Tables

**"Streaming Democratized: Ease Across the Latency Spectrum with Delayed View Semantics"**
Snowflake team, SIGMOD 2025. [arXiv:2504.10438](https://arxiv.org/abs/2504.10438)

**Core concept:** Declarative transformation with a **target lag** freshness guarantee.
```sql
CREATE DYNAMIC TABLE my_dt TARGET_LAG = '5 minutes' WAREHOUSE = wh AS SELECT ...;
```

**Refresh modes:**
- AUTO: Snowflake picks incremental vs. full based on cost
- INCREMENTAL: Delta propagation through the query plan
- FULL: Complete recomputation

**Change tracking:** Internal streams on base tables (micro-partition-level granularity).
Row-level deltas computed by comparing micro-partition metadata between transaction points.
Cost proportional to changed data volume, not total table size.

**Incremental operators:**
- Join: `(ΔL ⋈ R) ∪ (L ⋈ ΔR)` — standard delta join
- Aggregate: Recompute only affected groups
- Window functions: Recompute affected partitions
- Supported: INNER/OUTER JOIN, GROUP BY, window functions, CTEs, UNION ALL

**DAG pipelines:** Dynamic tables can reference other dynamic tables.
Snowflake manages topological refresh ordering and lag propagation.
`TARGET_LAG = DOWNSTREAM` inherits from consumers.

**Delayed View Semantics (DVS):** Formal model — DT contents are equivalent to the
view evaluated at some past point in time. Provides clean transactional reasoning.

**Relevance to OpenIVM:**
- Snowflake's adaptive refresh (auto = pick incremental vs. full) maps directly to
  OpenIVM's `ivm_adaptive` cost model
- Target lag concept relevant for the refresh scheduling question
- DAG chaining relevant for OpenIVM's chained materialized views
- The SIGMOD 2025 paper formalizes transaction isolation + IVM interaction

---

### Comparison Table

| | OpenIVM | DBSP/Feldera | DBToaster | pg_ivm | Snowflake DT |
|---|---|---|---|---|---|
| **Approach** | SQL-to-SQL compiler | Streaming dataflow | Compiled triggers | DB triggers | Cloud service |
| **IVM order** | First-order | First-order | Higher-order | First-order | First-order |
| **Runtime** | Any SQL engine | Feldera runtime | Standalone C++/Scala | PostgreSQL | Snowflake |
| **Change tracking** | Delta tables + optimizer | Streams (Z-sets) | Trigger functions | Transition tables | Micro-partition streams |
| **Refresh** | Lazy (PRAGMA) or eager | Continuous streaming | Per-tuple triggers | Immediate (in-txn) | Scheduled (target lag) |
| **Joins** | Inner (inclusion-exclusion) | All (bilinear delta) | All (recursive delta) | Inner + outer (v1.13) | Inner + outer |
| **Aggregates** | SUM, COUNT (+MIN/MAX partial) | All (linear cheap) | SUM, COUNT, AVG | count, sum, avg, min, max | All standard |
| **Cross-system** | Yes (SQL output) | No (Feldera only) | No (compiled code) | No (PG only) | No (Snowflake only) |
| **Adaptive** | Cost model (ivm_adaptive) | No | No | No | Auto mode |
