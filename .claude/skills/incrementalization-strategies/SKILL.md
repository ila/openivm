---
name: incrementalization-strategies
description: Deep research reference on incrementalization strategies for IVM — higher-order deltas, factorized IVM, compilation-based approaches, algebraic optimizations, cost models, constraint exploitation, and emerging techniques. Auto-loaded when discussing how to make IVM faster, delta optimization, auxiliary views, higher-order IVM, F-IVM, CROWN, compilation for IVM, IVM cost models, or research directions.
---

# Incrementalization Strategies: Making IVM Faster

Comprehensive research reference on techniques for making the incremental maintenance
path itself faster — not whether to use IVM, but how to do IVM better. Organized by
research family, with concrete adaptation ideas for OpenIVM's SQL-to-SQL architecture.

---

## 1. Higher-Order IVM (Recursive Delta Processing)

### 1.1 DBToaster: The Foundation

**Ahmad, Kennedy, Koch, Nikolic.** "DBToaster: Higher-order Delta Processing for
Dynamic, Frequently Fresh Views." VLDB Journal 23(2), 2014.

**Core idea:** If maintaining V via delta query dQ is expensive (dQ still contains joins),
maintain dQ itself incrementally via d(dQ). Recurse until deltas become trivially
computable (zero joins remaining).

**The algorithm:**
1. Start with view query Q over relations R1,...,Rn
2. Compute first-order delta dQ/dRi for each relation Ri — replaces Ri with singleton update tuple
3. Each differentiation eliminates one relation from the join graph
4. After k differentiations, the k-th order delta has zero joins — just hash map lookups
5. Materialize all intermediate deltas as auxiliary in-memory hash maps ("viewlets")
6. Single-tuple update triggers a cascade of O(1) map lookups

**Viewlet transform:** For a 3-way join R(A,B) JOIN S(B,C) JOIN T(C,D):
- First-order: dR JOIN S JOIN T (still 2 joins)
- Materialize auxiliary view V_ST(B,D) = SUM over C of S(B,C) * T(C,D)
- Now: dR JOIN V_ST (1 join — just a hash lookup per delta row)
- V_ST itself maintained incrementally when S or T changes

**Termination:** For acyclic queries with N relations, terminates at order N.
Disconnected components in the join hypergraph can be independently aggregated.

**Complexity:** For hierarchical queries: O(1) amortized per single-tuple update.
Standard first-order IVM: O(|DB|) per update for joins. Improvement: factor of |DB|.

**Compilation pipeline:**
- Frontend (OCaml): SQL → AGCA ring algebra → recursive delta derivation → algebraic simplification
- Backend (Scala/LMS): AGCA IR → trigger functions using Lightweight Modular Staging → C++/Scala code
- Generated code: standalone binary, no SQL engine at runtime, in-memory hash maps only

**Adaptation for OpenIVM:** Generate auxiliary materialized views at CREATE MV time.
Each auxiliary view has its own delta maintenance SQL. The PRAGMA ivm() handler
executes MERGE statements in topological order through the viewlet DAG. This stays
within the SQL-to-SQL paradigm while gaining higher-order benefits.

---

### 1.2 Ring Programming Foundation

**Koch.** "Incremental Query Evaluation in a Ring of Databases." PODS 2010.

Databases under bag union (+) and join (*) form a commutative ring (Z, +, *, 0, 1).
Any AGCA expression has a polynomial normal form. Taking delta w.r.t. a single-tuple
update to Ri reduces the polynomial degree by 1. After degree-many differentiations,
you reach a constant.

This is the algebraic proof that higher-order IVM always terminates and produces
polynomial-sized auxiliary maps for acyclic queries.

---

## 2. Factorized IVM (F-IVM)

**Nikolic, Olteanu.** "Incremental View Maintenance with Triple Lock Factorization
Benefits." SIGMOD 2018. Extended: VLDB Journal 2023.

### 2.1 The Triple Lock

Three simultaneous benefits:

1. **Factorized computation:** Query result stored as a tree of views (not flat table).
   For R(A,B) JOIN S(B,C), store: B → {A values from R} × {C values from S}.
   Avoids Cartesian product explosion.

2. **Higher-order IVM:** View tree decomposes into increasingly simpler views.
   Updates propagate bottom-up through the tree with O(1) cost per node.

3. **Ring abstraction:** Payload at each leaf is a ring element. Changing the ring
   changes what's computed: Z for counting, R for SUM, matrices for covariance.

### 2.2 Variable Order

The key compile-time decision: choose a *variable order* (tree decomposition of query
variables) that determines the shape of all data structures and update code.

For q-hierarchical queries (where for any two atoms sharing a variable, one's variables
are a subset of the other's): O(1) update time, O(1) enumeration delay — provably optimal.

### 2.3 Heavy/Light Decomposition (for non-hierarchical queries)

For queries outside the hierarchical class (e.g., triangle queries):
- Partition join-key values into "heavy" (degree >= tau) and "light" (degree < tau)
- Heavy values: few distinct keys, pre-join them (small result)
- Light values: low degree, process on-the-fly during enumeration
- Choose tau = |DB|^epsilon for tunable tradeoff

Triangle queries: O(|DB|^(1/2)) amortized update time (provably optimal under OMv conjecture).

**Key papers on heavy/light:**
- Kara, Ngo, Nikolic, Olteanu, Zhang. "Counting Triangles under Updates." ICDT 2019 (Best Paper).
- Kara et al. "Maintaining Triangle Queries under Updates." ACM TODS 2020.
- Kara et al. "Trade-offs in Static and Dynamic Evaluation of Hierarchical Queries." PODS 2020.

### 2.4 Adaptation for OpenIVM

The factorized representation (tree of hash maps) isn't directly expressible as flat
SQL tables. But the variable-order concept could guide how OpenIVM structures auxiliary
views. For a star join, instead of materializing the full join, maintain per-dimension
aggregate views that are probed during refresh.

F-IVM's batch processing model (process all deltas at once through the view tree)
aligns well with OpenIVM's delta tables that accumulate between refreshes.

**GitHub:** [fdbresearch/FIVM](https://github.com/fdbresearch/FIVM)

---

## 3. Change Propagation Without Joins (CROWN)

**Wang, Hu, Dai, Yi.** "Change Propagation Without Joins." PVLDB 16(5), 2023.

### 3.1 The Problem CROWN Solves

Standard IVM for joins materializes intermediate join results — the main cause of
polynomial space/time blowup. CROWN avoids this entirely.

### 3.2 The Algorithm

Instead of storing the join result, store each relation separately with per-tuple
auxiliary indexes:

For Q = R(A,B) JOIN S(B,C):
- R stored in trie indexed by (A,B)
- S stored in trie indexed by (B,C)
- Hash indexes link tuples across relations by join key B

When tuple inserted into R(a,b):
1. Insert (a,b) into R's trie
2. Update hash index on B=b
3. NO join computation at update time

Results produced via constant-delay enumeration: traverse join tree top-down,
using indexes to find matching tuples in other relations.

### 3.3 Complexity

- Standard IVM: 2^N-1 terms, materializes intermediate joins. Space: O(|DB|^k)
- CROWN: Zero intermediate materialization. Space: O(|DB|). Update: amortized O(1)
  for insert-only acyclic queries.

### 3.4 Adaptation for OpenIVM

Hardest to adapt to SQL-to-SQL (requires trie indexes). Approximate via:
- Semi-join reductions: `SELECT * FROM deltaR WHERE EXISTS (SELECT 1 FROM S WHERE S.B = deltaR.B)`
- LATERAL joins to avoid full intermediate materialization
- Correlated subqueries that push computation to the index layer

**GitHub:** [hkustDB/CROWN](https://github.com/hkustDB/CROWN)

---

## 4. Constraint-Based Optimization

### 4.1 Foreign Key Exploitation

**Svingos, Hernich, Gildhoff, Papakonstantinou, Ioannidis.** "Foreign Keys Open the
Door for Faster Incremental View Maintenance." SIGMOD 2023.

For R JOIN S where R.fk → S.pk: the FK constraint guarantees every R tuple matches
exactly one S tuple. This means changes to S are always captured by the dR term —
the R JOIN dS term is redundant.

**Impact:** For star schema (1 fact + N dimensions with FKs):
- Standard inclusion-exclusion: 2^(N+1) - 1 terms
- With FK optimization: just N+1 terms (one per table)

**For OpenIVM:** Detect FK constraints at CREATE MV time, prune redundant
inclusion-exclusion terms in `ivm_join_rule.cpp`. Pure win — no extra data structures.

### 4.2 ID-Based IVM Acceleration

**Katsis, Ong, Papakonstantinou, Zhao.** "Utilizing IDs to Accelerate Incremental
View Maintenance." SIGMOD 2015.

Propagate row IDs (primary keys) instead of full tuples through delta computation.
Join on narrow ID sets, fetch actual values only at the end.

For wide tables (100+ columns): dramatic reduction in intermediate data volume.

### 4.3 Static vs Dynamic Relations

**Kara, Luo, Nikolic, Olteanu, Zhang.** "Tractable Conjunctive Queries over Static
and Dynamic Relations." ICDT 2025.

When some relations are effectively static (dimension tables with no deltas), the
maintenance complexity can drop dramatically — even intractable queries become
tractable if static relations sufficiently constrain the dynamic ones.

**For OpenIVM:** At refresh time, check which base tables have empty delta tables.
Skip inclusion-exclusion terms involving only delta-empty tables.

### 4.3.1 DuckDB Optimizer and Empty Delta Scans

**Finding (verified 2026-04-14):** DuckDB's optimizer does NOT automatically skip
empty delta branches. Specifically:

- DuckDB has `empty_result_pullup.cpp` that eliminates joins when a child is a
  `LogicalEmptyResult` node. But DuckLake delta scans are `LogicalGet` nodes with
  table function bindings — they do NOT produce `LogicalEmptyResult` at plan time.
- DuckLake's `SCAN_INSERTIONS`/`SCAN_DELETIONS` do NOT short-circuit when
  `start_snapshot == end_snapshot` — they still execute metadata queries.
- All plan-level work in `BuildDuckLakeJoinTerms` (plan Copy, renumber_and_rebind,
  CollectJoinLeaves, CreateDeltaGetNode, ResolveOperatorTypes, projection creation)
  is done eagerly for ALL N terms before execution begins.
- LPTS SQL generation also processes all N UNION ALL'd terms regardless of emptiness.

**Implication:** Empty-delta term skipping MUST be implemented explicitly in OpenIVM's
join rule code by comparing `last_snapshot_id` with the current snapshot at plan time.
DuckDB cannot infer this from the plan structure alone.

**Standard DuckDB tables (verified 2026-04-14):** The situation is different. DuckDB's
`TableScanCardinality()` returns actual row counts for standard tables, and the
statistics propagator CAN convert a scan on a 0-row table to `LogicalEmptyResult`.
With the timestamp WHERE clause, detection depends on min/max column statistics.
At runtime, building a hash table on 0 rows is essentially free.

However, OpenIVM's plan-level overhead (plan Copy, renumber, CreateDeltaGetNode per
term) happens BEFORE DuckDB's statistics propagation runs in the optimizer pipeline.
Detecting empty deltas from OpenIVM's side requires N SQL queries (one per leaf),
whose overhead may exceed what's saved by skipping plan copies for small N.

**Decision:** Empty-delta term skipping for standard joins (1.2) is deferred —
DuckDB handles the common case reasonably well, and the detection cost is non-trivial.
Implemented for DuckLake only (1.1) where DuckDB cannot optimize and snapshot
comparison is a metadata-only O(1) check.

### 4.4 Insert-Only Optimization

**Kara, Nikolic, Olteanu, Zhang.** "Insert-Only versus Insert-Delete in Dynamic
Query Evaluation." SIGMOD 2024.

Insert-only is fundamentally cheaper than insert-delete. For append-only workloads:
- Skip delta consolidation (no cancellation needed)
- For GROUP BY: only UPDATE/INSERT, never DELETE (count can't reach 0)
- For projections: skip the entire DELETE pass

**For OpenIVM:** Check `SELECT COUNT(*) FROM delta WHERE NOT _duckdb_ivm_multiplicity` = 0.
If true, use a cheaper insert-only maintenance path.

---

## 5. Compilation and Code Generation for IVM

### 5.1 Multi-Stage Compilation (LMS)

**Shaikhha, Klonatos, Koch et al.** "How to Architect a Query Compiler." SIGMOD 2016.
"How to Architect a Query Compiler, Revisited." SIGMOD 2018.

Stack of DSLs for IVM compilation:
- Level 4: Relational algebra with aggregation (delta derivation here)
- Level 3: Collection operations — maps, filters, folds (data structure selection)
- Level 2: Imperative loops over hash maps (loop fusion, SIMD)
- Level 1: Low-level C with explicit memory management

Each level has its own optimization passes. DBToaster uses this exact pipeline.
The delta derivation happens at L4; auxiliary view selection at L3.

### 5.2 Staged Interpretation → Compilation (Futamura Projection)

**Tahboub, Essertel, Rompf.** "How to Architect a Query Compiler, Revisited." SIGMOD 2018.
**Rompf, Amin.** "A SQL to C Compiler in 500 Lines of Code." ICFP 2015.

Take a query interpreter, add staging annotations (mark query structure as static,
data as dynamic), and the Futamura projection automatically produces a compiler.

**For OpenIVM:** The delta rule application in `openivm_rewrite_rule.cpp` is essentially
an interpreter that transforms plans. With staging, it could produce specialized
maintenance code for each view definition — still SQL output, but derived by partial
evaluation rather than ad-hoc string construction.

### 5.3 Semi-Ring Dictionary Language (SDQL)

**Shaikhha, Huot, Smith, Olteanu.** "Functional Collection Programming with Semi-Ring
Dictionaries." OOPSLA 2022.

All data represented as semi-ring dictionaries: maps from keys to semiring values.
Relations = dictionaries from tuples to multiplicities (= Z-sets). Unifies relational
algebra, linear algebra, and tensor operations under one framework.

Compilation: SDQL → algebraic optimization → dictionary specialization → loop fusion → C code.
Competitive with Typer (compiled SQL), SciPy (linear algebra), TACO (tensors).

### 5.4 Compiled vs Vectorized for Delta Processing

**Kersten, Leis, Kemper, Neumann, Pavlo, Boncz.** "Everything You Always Wanted to
Know About Compiled and Vectorized Queries." PVLDB 11(13), 2018.

Key finding for IVM: delta tables are small (cache-resident → compilation wins),
base tables being probed are large (memory-bound → vectorization wins). Optimal:
hybrid approach — compile delta-side pipeline, vectorize base-table probes.

DuckDB already vectorizes. The delta consolidation step (small, compute-intensive)
is where compilation would add most value.

### 5.5 Feldera: SQL → Rust Compiled DBSP

**Budiu, Chajed, McSherry, Ryzhyk, Tannen.** "DBSP: Automatic Incremental View
Maintenance for Rich Query Languages." VLDB 2023 (Best Paper).

Feldera compiles SQL → Calcite IR → DBSP circuit → Rust code → native binary.
Uses shared arrangements (persistent indexed Z-sets) for inter-operator state sharing.
Orders of magnitude faster than SQL-based IVM for throughput-critical workloads.

Demonstrates the ceiling: full compilation eliminates all SQL parsing/planning overhead.
The question for OpenIVM: is there a middle ground between SQL strings and native code?

### 5.6 Adaptive Compilation Latency

**Neumann.** "Evolution of a Compiling Query Engine." PVLDB 14(11), 2021.
**Kohn, Leis, Neumann.** "Adaptive Execution of Compiled Queries." ICDE 2018.

Compilation latency is a real problem for frequent refreshes with small deltas.
Solution: start interpreting, compile in background, switch seamlessly.

**For OpenIVM:** For small deltas, SQL execution (current approach) may beat any
compilation overhead. For large deltas, pre-compiled plans would help. The cost model
should factor in delta size when choosing execution strategy.

### 5.7 Pre-Compiled Sub-Operators (InkFuse)

**Wagner, Neumann.** "Incremental Fusion: Unifying Compiled and Vectorized Query
Execution." ICDE 2024.

Decompose operators into a finite set of sub-operators (hash lookup, comparison,
aggregate update). Pre-generate vectorized primitives for each. At runtime, start
with pre-generated primitives (zero latency), fuse in background.

**For OpenIVM:** The sub-operators for delta processing (delta scan, multiplicity check,
aggregate update, merge-into) are a small, known set. Pre-compile them at
CREATE MATERIALIZED VIEW time as DuckDB UDFs or operators.

---

## 6. Cost Models for IVM

### 6.1 Tempura: Cost-Based Optimizer for Incremental Processing

**Wang, Zeng et al.** "Tempura: A General Cost-Based Optimizer Framework for
Incremental Data Processing." PVLDB 14(1), 2020. Extended: VLDB Journal 2023.

**Key innovation: Time-Varying Relations (TVR).**

A TVR models a table's state as a function of time, with snapshots and deltas.
The optimizer reasons about WHICH version of each table to use in delta computation:
- Snapshot (full state at time t)
- Delta (changes between t1 and t2)
- Or a combination

**Per-operator strategy selection:** Instead of a binary IVM-vs-recompute decision for
the whole view, Tempura picks the cheapest strategy PER OPERATOR:
- For a query `SELECT ... FROM R JOIN S JOIN T WHERE ... GROUP BY ...`:
  - Use IVM for R-S join (small delta) but recompute S-T join (large delta)
  - Incremental aggregate for SUM/COUNT, group-recompute for MIN/MAX
- Compile to a "hybrid" plan mixing incremental and recompute fragments

**Built on Apache Calcite.** Defines TVR-rewrite rules that transform regular plans
into incremental plans. Unifies four different incremental methods into a single
cost-based search space.

**For OpenIVM:** Extend the cost model in `openivm_cost_model.cpp` from binary
(IVM vs recompute) to per-operator. Annotate each node in the rewrite rule with
its delta-vs-recompute cost. The RewritePlan method chooses cheaper strategy per node.

### 6.2 Enzyme: Historical Execution Profiles

**Databricks.** "Enzyme: Incremental View Maintenance for Data Engineering."
arXiv:2603.27775, 2025.

Enzyme's cost model uses empirical feedback from past refreshes (not just cardinality
estimates). Steps:
1. **Fingerprinting:** Match current query plan to historical executions via normalized
   physical plan fingerprints
2. **Profile lookup:** Retrieve execution time, rows processed, memory used from past runs
3. **Strategy evaluation:** Score multiple candidate maintenance plans (row-based IVM,
   partition overwrite, full recompute) using profiled costs
4. **Selection:** Pick cheapest strategy per refresh

Also includes **query decomposition** into independently maintainable "maintenance units"
— sub-expressions that can be refreshed in parallel with different strategies.

**For OpenIVM:** Store execution statistics (duration, rows processed) per refresh in
metadata. Use historical data to calibrate the cost model. The `_duckdb_ivm_views`
table could track per-view refresh history.

### 6.3 Agamotto: When to Refresh (Scheduling Cost Model)

**Huang et al.** "Agamotto: Scheduling of Deadline-Oriented Incremental Query Execution
under Uncertain Resource Price." PVLDB 18(6), 2025.

Models refresh scheduling as a Markov Decision Process:
- State: accumulated delta size, resource price, time to deadline
- Actions: refresh now, wait (batch more), partial refresh
- Objective: minimize total cost subject to freshness deadline

Key finding: batching more deltas amortizes per-refresh overhead, but increases
staleness. Agamotto achieves costs within 10x of a "prophet" with perfect foresight.

**For OpenIVM:** The `REFRESH EVERY` daemon uses fixed intervals. Adaptive scheduling
based on delta accumulation rate would be more principled. Monitor delta table sizes,
trigger refresh when estimated IVM cost crosses a threshold.

### 6.4 Query Classification for Strategy Selection

**Olteanu.** "Recent Increments in Incremental View Maintenance." PODS Gems 2024.

Classifies queries into complexity tiers that determine the best maintenance strategy:

| Query Class | Best Update Time | Strategy |
|---|---|---|
| q-hierarchical | O(1) | Auxiliary maps (F-IVM/DBToaster) |
| Free-connex acyclic | O(1) delay | Dynamic Yannakakis semi-joins |
| General acyclic | O(|DB|^epsilon) | Heavy/light decomposition |
| Cyclic (e.g., triangle) | O(|DB|^(1/2)) | IVM-epsilon with heavy/light |
| General cyclic | O(|DB|^(w-1)) | Standard first-order (current OpenIVM) |

**For OpenIVM:** At CREATE MV time, classify the query structure and select the optimal
maintenance strategy. Store classification in metadata. Dispatch to different join delta
strategies in `IvmJoinRule::Rewrite()` based on classification.

---

## 7. Shared Maintenance Across Views

### 7.1 Shared Arrangements (Materialize)

**McSherry, Lattuada, Schwarzkopf, Roscoe.** "Shared Arrangements: Practical
Inter-Query Sharing for Streaming Dataflows." PVLDB 13(10), 2020.

An "arrangement" is an indexed, maintained Z-set shared across multiple operators.
When base data changes, the arrangement is updated once; all queries see the update.

Uses hierarchical batch merge (LSM-tree-like) for efficient incremental updates.
Memory: proportional to |DB|, not |DB| x |queries|.

For delta joins: each of N relations is the "delta source" in turn, remaining N-1
read from shared arrangements. Gives N terms (not 2^N-1) with zero intermediate state.

**For OpenIVM:** When refreshing multiple MVs that share base tables, compute shared
intermediate results (e.g., pre-join delta_orders with products dimension) once,
reuse across views via shared CTEs or temp tables.

### 7.2 Noria: Partially-Stateful Dataflow

**Gjengset et al.** "Noria: Dynamic, Partially-Stateful Data-Flow for High-Performance
Web Applications." OSDI 2018.

Key idea: not all intermediate state needs to be materialized. Noria selectively
materializes only the state that is frequently accessed ("hot" paths), evicting
cold state and recomputing on demand.

**For OpenIVM:** For auxiliary views (higher-order IVM), don't materialize all of them.
Use access frequency to decide which auxiliary views to keep vs. recompute on refresh.

### 7.3 Differential Dataflow and Nested Incrementalization

**McSherry, Murray, Isaacs.** "Differential Dataflow." CIDR 2013.

Generalizes IVM from linear time sequences to partially ordered timestamps,
enabling nested iteration and multi-version concurrency. Collection traces store
all changes across all times.

For chained MVs: the delta of MV1 (already computed) can be directly fed as input
to MV2's maintenance without re-reading MV1. Implemented via CTEs or temp tables
sharing intermediate delta results across multiple MERGE statements.

---

## 8. Emerging and Unconventional Approaches

### 8.1 Provenance Sketches for Delta Pruning

**Li, Glavic.** "In-memory Incremental Maintenance of Provenance Sketches." EDBT 2026.

Compact bitvector sketches that over-approximate which input tuples contribute to
which output tuples. At refresh time, test delta rows against the sketch to skip
irrelevant ones. Bloom filters on join keys provide additional pruning.

**For OpenIVM:** Build Bloom filter on MV join keys. Before full delta computation,
filter delta rows against the Bloom filter. For low-selectivity joins, can eliminate
>90% of delta rows before they enter the join. Implement as pre-filter in
`CreateDeltaGetNode()`.

### 8.2 Partition-Level Window Function IVM (Enzyme)

For views with PARTITION BY: identify which partitions have deltas, delete and
re-insert only those partitions from the base query. Unchanged partitions preserved.

```sql
DELETE FROM mv WHERE partition_key IN (SELECT DISTINCT partition_key FROM delta_table);
INSERT INTO mv SELECT ... FROM base_query WHERE partition_key IN (...);
```

This is the practical path to supporting window functions in IVM.

### 8.3 MVIVM: Temporal-to-Spatial Reduction

**Hu, Wang, Yi.** "Insert-Only versus Insert-Delete in Dynamic Query Evaluation."
SIGMOD 2024.

Encodes tuple lifespans [insert_time, delete_time) as extra multivariate attributes.
Insert-delete maintenance reduces to insert-only maintenance over an extended query.
Matches insert-only complexity bounds for acyclic queries.

### 8.4 Semi-Naive Evaluation for Recursive SQL (DBSP)

For recursive views (recursive CTEs), DBSP's semi-naive circuit generates a SQL loop
that iterates only on new tuples, avoiding redundant recomputation of fixed-point
iterations. Compile to recursive CTE with delta-only processing.

### 8.5 Worst-Case Optimal Joins for Delta Computation

**Wang, Willsey, Suciu.** "Free Join: Unifying Worst-Case Optimal and Traditional
Joins." SIGMOD 2023.

Free Join uses "hybrid hash tries" that adaptively switch between hash-join and
trie-join behavior per node. Consistently outperforms both binary joins and
Generic Join. For cyclic delta queries, this could provide asymptotic improvements
over OpenIVM's current binary-join-based inclusion-exclusion.

### 8.6 GPU-Accelerated Delta Processing

**Sioulas et al.** "Benchmarking Stream Join Algorithms on GPUs." EDBT 2024.

GPU delta joins effective for large batches (>10K tuples). Pattern: copy delta to GPU,
probe hash table in parallel, copy results back. The consolidation step (parallel
reduction / GROUP BY SUM) is particularly well-suited to GPU.

---

## 9. IVM for Specific SQL Operators

### 9.1 OUTER JOIN IVM

**Larson, Zhou.** "Efficient Maintenance of Materialized Outer-Join Views." ICDE 2007.
**Gupta, Mumick.** "Incremental Maintenance of Aggregate and Outerjoin Expressions." Information Systems, 2006.

The key insight is decomposing maintenance into:
- **Primary deltas** (direct changes, same as inner join deltas)
- **Secondary deltas** (NULL-padded tuples that must be inserted or removed)

When a new matching tuple arrives for a LEFT JOIN, a previously NULL-padded tuple must be
deleted and replaced with a properly joined tuple. The algorithm uses Join-Disjunctive
Normal Form (JDNF) to systematically enumerate all affected tuples.

Supports LEFT, RIGHT, and FULL OUTER JOIN. OpenIVM currently uses group-recompute for
LEFT JOIN aggregates — incremental outer join IVM would eliminate this.

### 9.2 Window Functions

**Research gap.** No dedicated paper on incremental ROW_NUMBER, RANK, LEAD/LAG, or
running aggregates exists. The difficulty: a single insertion can cascade changes across
an entire partition.

**Practical approach (Enzyme, Snowflake):** Partition-level recompute. Identify affected
partitions from delta, delete those partitions from MV, re-insert from base query filtered
to those partitions. Same pattern as OpenIVM's MIN/MAX group-recompute but for partitions.

```sql
DELETE FROM mv WHERE partition_key IN (SELECT DISTINCT partition_key FROM delta);
INSERT INTO mv SELECT ... FROM base_query WHERE partition_key IN (...);
```

For partition-level aggregates (SUM/COUNT OVER PARTITION), treat each partition as a
separate grouped aggregate — fully incremental. For positional functions (ROW_NUMBER,
LEAD/LAG), partition-level recompute is the best known approach.

**Sliding window aggregation:**
- Tangwongsan et al. (PVLDB 2015, 2019, 2023): O(1) amortized for arbitrary aggregate functions.
- Arroyo uses segment trees for O(1) incremental sliding window updates.
- Scotty (Traub et al., ICDE 2018): stream slicing for efficient multi-window aggregation.

### 9.3 COUNT(DISTINCT) / SUM(DISTINCT)

DBToaster supports COUNT(DISTINCT) via per-group auxiliary maps tracking per-value multiplicity.
When a value's count drops to 0, it leaves the distinct set; when rising from 0, it enters.

**Implementation:** Auxiliary table `(group_key, distinct_value, count_of_value)`. Maintained
as a normal AGGREGATE_GROUP MV. COUNT(DISTINCT) = COUNT(*) from aux WHERE count > 0.
Requires O(distinct_values) auxiliary storage per group.

**Approximate:** HyperLogLog for COUNT(DISTINCT) — DuckDB has `approx_count_distinct`.
Not incrementally mergeable in standard form, but Google BigQuery uses HLL for MV maintenance.

### 9.4 Set Operations (UNION, INTERSECT, EXCEPT)

DBSP provides clean algebraic treatment:
- **UNION ALL:** Delta passes through (linear). Free.
- **UNION (set):** Requires duplicate tracking (same as DISTINCT — counting column).
- **INTERSECT:** Track counts from both sides. Tuple in result only if count > 0 on both.
- **EXCEPT:** Track counts similarly. Result = left side minus right side counts.

All modeled as Z-set operations — straightforward in the counting/multiplicity framework.

### 9.5 Correlated Subqueries (EXISTS, IN, scalar)

Standard approach: **decorrelation** — convert to joins before applying IVM.
- EXISTS → semi-join
- NOT EXISTS → anti-join  
- IN → semi-join
- Scalar subquery → left join + aggregate

Once decorrelated, normal join + aggregate delta rules apply. Materialize documents
decorrelation strategies. pg_ivm supports EXISTS in WHERE (with restrictions: no OR/NOT).

**Abeysinghe, He, Rompf.** "Efficient Incrementialization of Correlated Nested Aggregate
Queries using RPAI." SIGMOD 2022. Tree-based index for O(log n) range shifts and aggregate
queries for correlated nested aggregate subqueries.

### 9.6 LATERAL Join

**Research gap.** No paper addresses delta rules for LATERAL/APPLY specifically.
LATERAL can often be decorrelated to a join (DuckDB and Materialize both do this).
Once decorrelated, standard join IVM applies. When decorrelation is impossible
(e.g., table-valued functions), no known IVM technique exists.

### 9.7 GROUPING SETS / CUBE / ROLLUP

**Research gap for IVM specifically.** These are syntactic sugar for UNION ALL of
multiple GROUP BY queries at different granularities. Decompose CUBE into 2^n separate
grouped aggregates, maintain each independently. Opportunity: share delta computation
across grouping levels (a delta row's contribution to GROUP BY (A,B) implies its
contribution to GROUP BY (A)).

### 9.8 Top-K (ORDER BY + LIMIT)

**Yi, Yu, Yang, Xia, Chen.** "Efficient Maintenance of Materialized Top-k Views."
ICDE 2003. Extended: Distributed and Parallel Databases 2009.

Maintain a top-k' view where k' > k, creating a buffer zone. Deletions from the view
can be satisfied from the buffer without hitting the base table. Expected amortized
refill cost is O(1). Insertions are cheap (compare against k'-th element).

### 9.9 Recursive CTEs

Semi-naive evaluation from Datalog: `delta_T(k) = pi(delta_T(k-1) join R) - T(k-1)`.
DBSP extends to non-monotonic recursion over Z-sets. Feldera implements this.

**Nishikawa et al.** "Dynamic Pruning for Recursive Joins." SIGMOD 2025. Generates pruning
filters dynamically during recursive execution, 135x speedup for manufacturing traceability.

**Ammar et al.** "Optimizing Differentially-Maintained Recursive Queries on Dynamic Graphs."
PVLDB 15(11), 2022. Selectively drops operator differences and recomputes when memory overhead
of differential computation becomes too high.

### 9.10 STRING_AGG / LISTAGG

**Research gap.** Order-dependent, non-decomposable. Inserting in the middle requires
reconstructing the string. Only known approach: group-recompute (delete group, rebuild
from base). No incremental delta rule exists.

### 9.11 MEDIAN / Percentiles

Holistic (non-decomposable) aggregates. Exact = store all values per group (group-recompute).
Approximate = t-digest or similar sketch structures (incrementally mergeable).

### 9.12 HAVING

Not a separate operator for IVM. Strategy: maintain all groups (including those failing
HAVING) in the data table, with a hidden flag or by maintaining full aggregates. After
MERGE, apply HAVING as post-step: `DELETE FROM mv WHERE NOT (having_predicate)`.
Eliminates group-recompute for HAVING views.

### 9.13 STDDEV / VARIANCE

Decomposable: STDDEV = sqrt((SUM(x^2)*COUNT - SUM(x)^2) / (COUNT*(COUNT-1))).
Maintain hidden SUM(x), SUM(x^2), COUNT columns. Same decomposition pattern as AVG.
Recompute STDDEV from maintained components post-MERGE.

---

## 10. Additional Papers (2015-2026)

### 10.1 Theoretical Foundations

**Berkholz, Keppeler, Schweikardt.** "Answering Conjunctive Queries under Updates." PODS 2017.
Proves q-hierarchical CQs are exactly the class admitting constant update time and
constant-delay enumeration. The theoretical foundation for all subsequent IVM complexity work.

**Idris, Ugarte, Vansummeren.** "The Dynamic Yannakakis Algorithm." SIGMOD 2017.
Dynamic Constant-delay Linear Representations (DCLRs) for free-connex acyclic CQs.
Linear-time updates, constant-delay enumeration, constant-time lookups, linear space.

**Hu, Wang.** "Towards Update-Dependent Analysis of Query Maintenance." PODS 2025 (Distinguished Paper).
IVM under realistic (non-worst-case) update patterns. Many queries requiring sqrt(|DB|) time
under arbitrary updates can be maintained in constant amortized time under insert-only or FIFO.

### 10.2 Join Processing for IVM

**Freitag, Bandle, Schmidt, Kemper, Neumann.** "Adopting Worst-Case Optimal Joins in
Relational Database Systems." PVLDB 13(11), 2020. First practical WCO joins in a full
relational DBMS (Umbra). Relevant because WCO joins avoid large intermediates for cyclic deltas.

**Birler, Kemper, Neumann.** "Robust Join Processing with Diamond Hardened Joins."
PVLDB 17(11), 2024. Lookup & Expand (L&E) decomposition for robust join processing.

**Bekkers, Neven, Vansummeren, Wang.** "Instance-Optimal Acyclic Join Processing Without
Regret." PVLDB 18(8), 2025. Shredded Yannakakis achieves 62.5x speedups on 85.3% of queries.

**Fent, Neumann.** "A Practical Approach to Groupjoin and Nested Aggregates." PVLDB 14(11), 2021.

### 10.3 Adaptive / Autonomous Materialization

**Ahmed, Bello, Witkowski, Kumar.** "Automated Generation of Materialized Views in Oracle."
PVLDB 13(12), 2020. ECSE algorithm for workload-driven MV generation.

**Han, Chai, Liu, Li, Wei, Zhan.** "Dynamic Materialized View Management using Graph Neural
Network." ICDE 2023. GNN predicts view utility under shifting workloads.

**Mirjafari et al.** "Adaptive Materialization with Deep Reinforcement Learning." ICDE 2022.
Deep RL to decide when to materialize vs recompute.

**Xu, Wang, Xue et al.** "UniView: A Unified Autonomous Materialized View Management System."
PVLDB 17(12), 2024. Cross-platform (Spark SQL, PostgreSQL, ClickHouse) MV management.

**Jindal, Karanasos, Rao, Patel.** "Selecting Subexpressions to Materialize at Datacenter Scale."
PVLDB 11(7), 2018. BigSubs algorithm for shared sub-expression selection across thousands of jobs.

### 10.4 Compilation and Execution for IVM

**Winter, Schmidt, Neumann, Kemper.** "Meet Me Halfway: Split Maintenance of Continuous Views."
PVLDB 2020. Split maintenance between insert-time (eager) and query-time (lazy). 10x higher
insert throughput than traditional IVM.

**Kohn, Leis, Neumann.** "Adaptive Execution of Compiled Queries." ICDE 2018. Dynamic switching
between JIT compilation and interpretation at morsel granularity. For IVM: interpret small deltas,
compile large ones.

**Koch, Lupei, Tannen.** "Incremental View Maintenance for Collection Programming." PODS 2016.
Efficient incrementalization of positive nested relational calculus (NRC+) on bags.

**Nikolic, Dashti, Koch.** "How to Win a Hot Dog Eating Contest: Distributed Incremental View
Maintenance with Batch Updates." SIGMOD 2016. Batch vs tuple-at-a-time tradeoffs.

### 10.5 Consistency and Distribution

**Power, Achalla, Cottone, Macasaet, Hellerstein.** "Wrapping Rings in Lattices: An Algebraic
Symbiosis of IVM and Eventual Consistency." PaPoC@EuroSys 2024. Bridges CRDT lattice algebra
with IVM ring algebra for eventually-consistent incremental computation.

### 10.6 Graph and Non-Relational IVM

**Szarnyas.** "Incremental View Maintenance for Property Graph Queries." SIGMOD 2018 (PhD symposium).
Reduces property graph queries to nested relational algebra for incremental evaluation.

**Pang, Zou, Yu, Yang.** "Materialized View Selection & View-Based Query Planning for Regular
Path Queries." SIGMOD 2024. First MV selection for graph database path queries.

**Zhao, Rusu, Dong, Wu, Nugent.** "Incremental View Maintenance over Array Data." SIGMOD 2017.

---

## 11. Summary: Speedup Potential

| Innovation | Current OpenIVM | With Innovation | Speedup |
|---|---|---|---|
| Higher-order IVM | O(\|DB\|) per update for joins | O(1) for hierarchical | \|DB\| |
| Factorized (F-IVM) | 2^N join terms, flat tables | N view tree nodes | Exponential in width |
| CROWN (no joins) | O(\|DB\|^k) space | O(\|DB\|) space | Polynomial |
| Heavy/light | O(\|DB\|) for cyclic | O(\|DB\|^0.5) for triangles | Square root |
| FK pruning | 2^N-1 terms | As few as 1 term | Up to 2^N |
| Static table detection | Process all tables | Skip delta-empty tables | Proportional |
| Per-operator cost model | Binary IVM/recompute | Hybrid plan | Case-dependent |
| Insert-only fast path | Full consolidation | Skip consolidation | 2-3x |
| Shared arrangements | Independent per-view | Shared intermediates | # shared tables |
| Pre-compiled plans | Re-parse SQL per refresh | Cached plans | Parse/plan overhead |

## 12. Recommended Reading Order

1. **Olteanu, "Recent Increments in IVM"** (PODS Gems 2024) — the survey that maps the field
2. **Ahmad et al., DBToaster** (VLDB Journal 2014) — higher-order IVM foundation
3. **Nikolic & Olteanu, F-IVM** (SIGMOD 2018) — factorized + ring abstraction
4. **Wang et al., CROWN** (PVLDB 2023) — no-join change propagation
5. **Svingos et al., FK optimization** (SIGMOD 2023) — immediately actionable
6. **Wang et al., Tempura** (PVLDB 2020) — per-operator cost model
7. **Enzyme** (arXiv 2025) — industrial state of the art
8. **Shaikhha et al., "How to Architect a Query Compiler"** (SIGMOD 2016) — compilation pipeline
9. **Shaikhha et al., SDQL** (OOPSLA 2022) — semiring dictionary compilation
10. **DBSP** (VLDB 2023) — the algebraic framework OpenIVM builds on

## 13. Key References

### Core IVM Theory
- DBToaster: [arXiv:1207.0137](https://arxiv.org/abs/1207.0137)
- Koch, Ring of Databases: [ACM DL](https://dl.acm.org/doi/abs/10.1145/1807085.1807100)
- F-IVM: [arXiv:1703.07484](https://arxiv.org/abs/1703.07484) | [GitHub](https://github.com/fdbresearch/FIVM)
- CROWN: [arXiv:2301.04003](https://arxiv.org/abs/2301.04003) | [GitHub](https://github.com/hkustDB/CROWN)
- DBSP: [arXiv:2203.16684](https://arxiv.org/abs/2203.16684)
- Olteanu survey: [arXiv:2404.17679](https://arxiv.org/abs/2404.17679)
- Berkholz et al. (q-hierarchical): [PODS 2017](https://dl.acm.org/doi/10.1145/3034786.3034789)
- Dynamic Yannakakis: [SIGMOD 2017](https://dl.acm.org/doi/10.1145/3035918.3064027)
- Hu & Wang (update-dependent): [PODS 2025](https://2025.sigmod.org/pods_papers.shtml)

### Constraint Exploitation
- FK optimization: [ACM DL](https://dl.acm.org/doi/10.1145/3588720)
- IVM-epsilon: [arXiv:1804.02780](https://arxiv.org/abs/1804.02780)
- Static/Dynamic: [arXiv:2404.16224](https://arxiv.org/abs/2404.16224)
- Insert-Only vs Delete: [arXiv:2312.09331](https://arxiv.org/abs/2312.09331)
- ID-Based IVM: [SIGMOD 2015](https://dl.acm.org/doi/10.1145/2723372.2723731)

### Cost Models & Adaptive
- Tempura: [ACM DL](https://dl.acm.org/doi/10.14778/3421424.3421427)
- Enzyme: [arXiv:2603.27775](https://arxiv.org/abs/2603.27775)
- Agamotto: [PVLDB 18(6)](https://dl.acm.org/doi/10.14778/3749672.3749679)
- Oracle Auto MV: [PVLDB 13(12)](https://dl.acm.org/doi/10.14778/3415478.3415533)
- UniView: [PVLDB 17(12)](https://dl.acm.org/doi/abs/10.14778/3685800.3685873)

### Compilation & Execution
- SDQL: [arXiv:2103.06376](https://arxiv.org/abs/2103.06376)
- Compiler architecture: [ACM DL](https://dl.acm.org/doi/10.1145/2882903.2882906)
- Compiled vs Vectorized: [PVLDB 11(13)](https://dl.acm.org/doi/10.14778/3275366.3284966)
- InkFuse: [ICDE 2024](https://doi.org/10.1109/ICDE60146.2024.00029)
- Split Maintenance (TUM): [PVLDB 2020](https://www.vldb.org/pvldb/vol13/p2620-winter.pdf)
- Adaptive Execution (TUM): [ICDE 2018](https://db.in.tum.de/~leis/papers/adaptiveexecution.pdf)

### Join Processing
- Free Join: [arXiv:2301.10841](https://arxiv.org/abs/2301.10841)
- Diamond Hardened Joins: [PVLDB 17(11)](https://dl.acm.org/doi/10.14778/3681954.3681995)
- Shredded Yannakakis: [PVLDB 18(8)](https://doi.org/10.14778/3742728.3742737)
- WCO Joins in RDBMS: [PVLDB 13(11)](https://dl.acm.org/doi/10.14778/3407790.3407800)
- Leapfrog Triejoin: [ICDT 2014](https://openproceedings.org/ICDT/2014/paper_13.pdf)

### Operator-Specific IVM
- Outer Join IVM (Larson & Zhou): [ICDE 2007](http://www.cs.columbia.edu/~jrzhou/pub/OJViewMaintenance.pdf)
- Aggregate & Outerjoin (Gupta & Mumick): [Info Systems](https://www3.cs.stonybrook.edu/~hgupta/ps/aggr-is.pdf)
- Top-K Views: [ICDE 2003](http://publish.illinois.edu/yuguo/files/2012/12/icde031.pdf)
- RPAI (correlated subqueries): [ACM DL](https://dl.acm.org/doi/abs/10.1145/3514221.3517889)
- Sliding Window Aggregation: [PVLDB 8(7), 2015](https://dl.acm.org/doi/10.14778/2752939.2752940)
- Scotty (window slicing): [ICDE 2018](https://ieeexplore.ieee.org/document/8509356/)
- Dynamic Pruning (recursive joins): [SIGMOD 2025](https://dl.acm.org/doi/10.1145/3722212.3724434)

### Systems
- Shared Arrangements: [ACM DL](https://dl.acm.org/doi/10.14778/3401960.3401974)
- Provenance Sketches: [EDBT 2026](https://www.cs.uic.edu/~bglavic/dbgroup/2025/06/04/EDBT-incremental.html)
- Streaming View (Alibaba): [PVLDB 18(12)](https://dl.acm.org/doi/10.14778/3750601.3750634)
- Noria: [OSDI 2018](https://www.usenix.org/conference/osdi18/presentation/gjengset)
- Differential Dataflow: [CIDR 2013](https://www.cidrdb.org/cidr2013/Papers/CIDR13_Paper111.pdf)
- Rings in Lattices (CRDT+IVM): [PaPoC 2024](https://doi.org/10.1145/3642976.3653030)
- Snowflake Dynamic Tables: [arXiv:2504.10438](https://arxiv.org/abs/2504.10438)
- ART: [ICDE 2013](https://db.in.tum.de/~leis/papers/ART.pdf)
