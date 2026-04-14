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
  OpenIVM's `ivm_adaptive_refresh` cost model
- Target lag concept relevant for the refresh scheduling question
- DAG chaining relevant for OpenIVM's chained materialized views
- The SIGMOD 2025 paper formalizes transaction isolation + IVM interaction

---

### Comparison Table

| | OpenIVM | DBSP/Feldera | DBToaster | pg_ivm | Snowflake DT | Databricks Enzyme | Flink SQL | Materialize | RisingWave |
|---|---|---|---|---|---|---|---|---|---|
| **Approach** | SQL-to-SQL compiler | Streaming dataflow | Compiled triggers | DB triggers | Cloud service | Engine-internal (Spark) | Retraction streams | Shared arrangements | Streaming DB |
| **IVM order** | First-order | First-order | Higher-order | First-order | First-order | First-order | First-order | First-order | First-order |
| **Runtime** | Any SQL engine | Feldera runtime | Standalone C++/Scala | PostgreSQL | Snowflake | Spark | Flink | Materialize | RisingWave |
| **Change tracking** | Delta tables / DuckLake snapshots | Streams (Z-sets) | Trigger functions | Transition tables | Micro-partition streams | Delta Lake CDF | Changelog (+I/-U/+U/-D) | Shared arrangements | Barrier checkpoints |
| **Refresh** | Lazy (PRAGMA) or scheduled daemon | Continuous | Per-tuple triggers | Immediate (in-txn) | Scheduled (target lag) | Batch or streaming | Continuous | Continuous | Continuous |
| **Joins** | Inner + LEFT (inclusion-exclusion / N-term) | All (bilinear delta) | All (recursive delta) | Inner + outer | Inner + outer | Inner + outer (2-term) | All types | Arbitrary N-way | All incl. anti/semi |
| **Aggregates** | SUM, COUNT, AVG (+MIN/MAX partial) | All (linear cheap) | SUM, COUNT, AVG | count, sum, avg, min, max | All standard | All + partition-level window | All + LISTAGG | All via hierarchical reduction | All + APPROX_COUNT_DISTINCT |
| **Window funcs** | Not yet (FULL_REFRESH) | Yes (delay operators) | No | No | Yes (partition recompute) | Yes (partition overwrite) | Yes (full support) | Temporal filters only | Yes (OVER windows) |
| **Cross-system** | Yes (SQL output) | No | No | No | No | No | No | No | No |
| **Adaptive** | Cost model | No | No | No | Auto mode | Learned cost model (87.5% accuracy) | No | No | No |

---

### Databricks Enzyme

**"Enzyme: Incremental View Maintenance for Data Engineering"**
Databricks, arXiv:2603.27775, March 2026. Powers Delta Live Tables (DLT).

**Architecture:** Engine-internal Spark optimizer extension. Traverses the logical plan
bottom-up producing three outputs per operator: previous state (psi), updated state
(psi'), and change representation (Delta-psi).

**Key innovations:**

- **Learned cost model:** Fingerprints query plans, retrieves execution time/rows/memory
  from historical executions. Scores multiple strategies (row-based IVM, partition overwrite,
  full recompute). 87.5% accuracy in choosing optimal strategy.
- **Query decomposition into maintenance units:** Sub-expressions refreshed independently
  with different strategies — not all-or-nothing IVM vs recompute.
- **Partition-level window function maintenance:** When input and MV share partition columns,
  overwrites only affected partitions.
- **Effectivized changesets:** Compacts CDF by grouping rows by all columns, summing change
  types (+1/-1), keeping only non-zero net changes (analogous to OpenIVM's delta consolidation CTE).
- **2-term join formula:** Delta(L join R) = (Delta-L join R) + (L' join Delta-R). Simpler
  than inclusion-exclusion but requires materializing L' (full current state).
- **Delta Lake CDF:** Row-level changes with `_change_type` (insert/update_preimage/update_postimage/delete),
  `_commit_version`, `_commit_timestamp`. Similar to DuckLake snapshots.

**Relevance to OpenIVM:** Learned cost model directly applicable (store refresh metrics in metadata).
Partition-level window IVM is the practical path for window function support.
Enzyme's 2-term formula vs OpenIVM's N-term inclusion-exclusion is a tradeoff: simpler but
requires full-state materialization for non-delta sides.

---

### Apache Flink SQL

**"Apache Flink: Stream and Batch Processing in a Single Engine"**
Carbone et al., IEEE Data Eng. Bulletin, 2015. Flink SQL uses retraction-based IVM.

**Refresh model:** Continuous streaming. 4-message changelog (+I = insert, -U = update before,
+U = update after, -D = delete). More expressive than OpenIVM's boolean multiplicity.

**Supported operators:** All join types (inner, outer, semi, anti, temporal, interval).
All standard aggregates including LISTAGG, FIRST_VALUE, LAST_VALUE. Full window support
(tumbling, sliding, session, cumulative, OVER windows). Mini-batch aggregation for throughput.

**Relevance to OpenIVM:** Flink's retraction model (-U/+U pair for updates) avoids
the need for MERGE — potentially simpler upsert logic. Mini-batch aggregation (buffer
deltas, apply in batch) is exactly what OpenIVM does with lazy refresh.

---

### RisingWave

**"RisingWave: A Distributed SQL Streaming Database"**
SIGMOD 2024 (industry track). PostgreSQL wire-compatible streaming database.

**Key features:** All join types including anti/semi. APPROX_COUNT_DISTINCT. Barrier-based
checkpointing. Cloud-native (S3-backed state). Decoupled compute/storage.

**Relevance to OpenIVM:** PostgreSQL compatibility shows viability of streaming IVM behind
a standard SQL interface. Outer join maintenance with NULL-matching state tracking is relevant.

---

### Materialize (details beyond comparison table)

**"Shared Arrangements: Practical Inter-Query Sharing for Streaming Dataflows"**
McSherry, Lattuada, Schwarzkopf, Roscoe. PVLDB 13(10), 2020.

**Key innovation: Shared arrangements.** An "arrangement" is an indexed, maintained Z-set
shared across multiple operators. When base data changes, the arrangement is updated once;
all queries see the update. Uses hierarchical batch merge (LSM-tree-like).

For delta joins: each of N relations is the "delta source" in turn, remaining N-1
read from shared arrangements. Gives N terms (not 2^N-1) with zero intermediate state.
This is conceptually similar to DuckLake N-term telescoping but uses in-memory indexes
instead of time travel.

---

### Oracle Materialized View Fast Refresh

Industry's oldest production IVM system (since Oracle 8i, ~1998).

**Change tracking:** Materialized view logs on base tables (row-level change capture,
analogous to delta tables). Stores rowid + changed columns + timestamps.

**Fast refresh:** ON COMMIT or ON DEMAND. Supports SUM, COUNT, AVG, STDDEV, VARIANCE
with fast refresh (COUNT must be included alongside SUM for correct maintenance — same
pattern as OpenIVM's aggregate rewriting). MIN/MAX require full refresh.

**PCT (Partition Change Tracking):** Refresh only affected partitions. Directly relevant
to DuckLake integration where micro-partitions are tracked per-snapshot.

---

### Microsoft SQL Server Indexed Views

Immediate maintenance (on every DML). Extremely restricted: inner joins only, no subqueries,
no outer joins, SUM/COUNT_BIG only, deterministic expressions only. The restrictions guarantee
correctness for immediate maintenance without complex delta reasoning.

**Relevance:** Shows what's feasible for guaranteed-correct immediate IVM. Azure Stream
Analytics (separate product) provides full streaming SQL with temporal joins and all window types.

---

### Google BigQuery Materialized Views

Auto-refreshed on ~30min schedule. Limited to single-table aggregations with inner joins.
COUNT, SUM, AVG, APPROX_COUNT_DISTINCT (HyperLogLog). Star schema restriction.
Any deletion in base table forces full refresh.

**Relevance:** Transparent query rewriting: optimizer routes queries through stale MV + live
delta. This "MV query rewriting" approach could complement OpenIVM for read-path optimization.

---

### Alibaba Streaming View

**Zhang et al.** "Streaming View: An Efficient Data Processing Engine for Modern Real-Time
Data Warehouse." PVLDB 18(12), 2025.

Native IVM inside AnalyticDB warehouse. Replaces Lambda/Kappa external stream processing.
Supports complex ETL pipelines incrementally. Production deployment at Alibaba Cloud.

---

### Noria / ReadySet

**Gjengset et al.** "Noria: Dynamic, Partially-Stateful Data-Flow for High-Performance
Web Applications." OSDI 2018.

**Key innovation: Partially-stateful dataflow.** Operators don't need to materialize all state.
Hot paths stay materialized; cold state evicted and reconstructed on demand via "upqueries."
For MIN/MAX: upquery re-fetches from base table on deletion (similar to group-recompute
but only for accessed keys).

**Relevance:** Maps to selective materialization of intermediate results — only cache
hot groups for auxiliary views. Directly relevant for adaptive materialization (C4).

---

### ksqlDB (Confluent)

**Jafarpour et al.** "ksqlDB: A Stream-Relational Database System." EDBT 2024 (industry).

Kafka-native streaming SQL. Table-table join (both sides changelog-driven) is conceptually
closest to OpenIVM's delta-delta join terms. RocksDB-backed state.
Grace period concept for late-arriving data relevant for event-time semantics.

---

### Apache Calcite

**Begoli et al.** "Apache Calcite: A Foundational Framework for Optimized Query Processing
Over Heterogeneous Data Sources." SIGMOD 2018.

Not a runtime — a query planning framework. Lattice framework for MV selection: which MVs
to create given a workload. Lattice tiles represent pre-aggregated join results. MV
substitution rules rewrite queries to use existing MVs.

**Relevance:** Calcite's lattice framework could guide automatic auxiliary MV selection
for higher-order IVM.

---

### TUM Systems

**Split Maintenance of Continuous Views**
Winter, Schmidt, Neumann, Kemper. PVLDB 2020.
Splits view maintenance between insert time (eager: filtering, projection) and query time
(lazy: aggregation finalization). Up to 10x higher insert throughput than traditional IVM.

**InkFuse: Incremental Fusion**
Wagner, Neumann. ICDE 2024.
Pre-compiled sub-operators assembled at runtime for zero-latency query startup. The
sub-operator library concept is applicable: delta query pipelines could be assembled
from pre-compiled fragments.

**ART (Adaptive Radix Tree)**
Leis, Kemper, Neumann. ICDE 2013. Synchronization: DaMoN 2016 (OLC).
Default index in HyPer/Umbra/DuckDB. Adapts node sizes (Node4/16/48/256).
For OpenIVM: ART is used for unique constraint enforcement on MV GROUP BY keys.
DuckDB's MERGE INTO uses hash joins internally, NOT ART lookups.

---

### Redshift AutoMV

ML-based automatic creation of materialized views from observed query workload patterns.
No user intervention required. Auto-refresh on staleness detection.

---

### Timeplus/Proton, Arroyo, DeltaStream

Smaller streaming SQL engines. **Proton**: open-source, built on ClickHouse, unified
historical+streaming queries. **Arroyo**: Rust-based, segment trees for sliding window
aggregation (O(1) incremental updates). **DeltaStream**: cloud-native streaming SQL over Kafka/Kinesis.

---

### ClickHouse Materialized Views

Insert-time trigger (block-level). Single-table only (no join MVs). AggregatingMergeTree
stores partial aggregates merged at query time. Extremely high throughput for append-only
workloads. Not truly incremental for deletes/updates.
