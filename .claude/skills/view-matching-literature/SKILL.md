---
name: view-matching-literature
description: Deep research reference on view matching (answering queries using views, materialized-view query rewriting, subsumption-based rewriting) spanning classical PODS/SIGMOD theory, the Goldstein-Larson matcher, Calcite's implementation, commercial systems (Oracle, SQL Server, DB2, Snowflake, BigQuery, Redshift, Databricks, Firebolt, ClickHouse), adjacent result/semantic caching, and Microsoft/Google computation-reuse work. Auto-loaded when discussing view matching, query rewriting with MVs, subsumption, cache-aware optimization, or integrating a matcher into OpenIVM.
---

# View Matching: Literature and Systems Reference

Terminology note. "View matching" = "answering queries using views" = "query
rewriting with materialized views" = "subsumption-based rewriting". Halevy's survey
uses the first phrase; commercial systems say "query rewrite" (Oracle) or "view
matching" (SQL Server). Scope varies: data-integration work looks for *any* (possibly
contained) rewriting; query-optimization work requires an *equivalent* rewriting cheaper
than the original.

This skill is for designing a matcher inside OpenIVM. It catalogs the algorithms,
cites the papers, and maps capabilities of real systems — so we can decide what to
port vs. invent.

---

## 1. Classical Academic Foundations

### 1.1 The problem, formally
- **Levy, Mendelzon, Sagiv, Srivastava — "Answering Queries Using Views", PODS 1995.**
  Introduced the problem for conjunctive queries (CQs). For CQs without built-in
  predicates a rewriting exists iff there is a containment mapping from the view body
  into the query. Rewriting is NP-complete in query size. The "bucket algorithm"
  descends from here: for each query subgoal, compute a bucket of views that could
  cover it, then combine and test containment.

- **Chaudhuri, Krishnamurthy, Potamianos, Shim — "Optimizing Queries with Materialized
  Views", ICDE 1995.** First paper to push MV rewriting into a cost-based (System R /
  Cascades) optimizer. Views become alternative access paths integrated into join
  enumeration; "compensation" produces residual predicates when the view is more
  general than the query. Distinguishes *equivalent* rewriting (safe everywhere) from
  *contained* rewriting (data-integration only).

### 1.2 Aggregate rewriting
- **Srivastava, Dar, Jagadish, Levy — "Answering Queries with Aggregation Using
  Views", VLDB 1996.** Extends to COUNT/SUM/MIN/MAX/AVG. SUM and COUNT compose
  (re-aggregate via SUM over view's COUNT); MIN/MAX compose; **AVG does not compose**
  unless SUM+COUNT are stored separately — this is exactly why `openivm_parser.cpp`
  rewrites AVG into SUM/COUNT at MV creation time.
- **Gupta, Harinarayan, Quass — "Aggregate-Query Processing in Data Warehousing
  Environments", VLDB 1995.** Lattice-of-cuboids reasoning. Core rule: view's GROUP BY
  must be a subset of the query's GROUP BY (or derivable via FDs), and aggregates must
  be re-aggregatable.
- **Cohen, Nutt, Sagiv — "Rewriting queries with arbitrary aggregation functions
  using views", ACM TODS 2006 (earlier PODS/ICDT versions).** Characterizes when
  aggregate rewriting is possible via "aggregation preservation".

### 1.3 MiniCon and descendants
- **Pottinger & Halevy — "MiniCon: A Scalable Algorithm for Answering Queries Using
  Views", VLDB Journal 2001.** MiniCon Descriptions (MCDs) encode "how this view covers
  this subgoal under this variable mapping, preserving head-variable distinctions".
  Far fewer combinations to enumerate than the bucket algorithm; de-facto standard in
  data-integration literature.
- **Afrati, Li, Mitra — "Answering Queries Using Views with Arithmetic Comparisons",
  PODS 2002** and many follow-ups extending MCDs to FDs, arithmetic, and ranges.

### 1.4 The IBM DB2 line — most influential practical paper
- **Zaharioudakis, Cochrane, Lapis, Pirahesh, Urata — "Answering Complex SQL Queries
  Using Automatic Summary Tables", SIGMOD 2000.** The canonical industrial paper.
  - *Signature matching* on normalized query blocks (IBM's QGM — Query Graph Model).
  - Handles SPJGA (select-project-join-groupby-aggregate) with outer joins and UNION ALL.
  - Uses FDs + PK/FK constraints to prove *join losslessness*: a join against a
    dimension table can be eliminated if the query's aggregate doesn't depend on it.
  - *Compensation* layer applies residual predicates and re-aggregation after match.
  - Foundation of DB2 MQT matching.

### 1.5 Goldstein & Larson — the SQL Server matcher
- **Goldstein & Larson — "Optimizing Queries Using Materialized Views: A Practical,
  Scalable Solution", SIGMOD 2001.** The reference for industrial view matching. Core
  algorithm:
  1. **Filter tree.** MVs indexed by a tree keyed on (normalized source-table set,
     join-predicate equivalence classes, residual-predicate ranges, output columns).
     Given a query, descend the tree to produce a small candidate set. Scales to
     thousands of MVs.
  2. **Equivalence classes of columns.** Collect columns tied by equality predicates
     (join equalities + residual equalities) into classes. Query and view match when
     their equivalence-class partitions are compatible.
  3. **Range compatibility.** Every non-equality predicate is an interval on an
     equivalence class. Query range must be contained in the view range; the
     difference is a *compensating predicate*.
  4. **Output column check.** Every required column must be computable from the view
     (identity, arithmetic on stored columns, or re-aggregation).
  5. **Grouping check.** Query's GROUP BY must be derivable from view's GROUP BY via
     FDs; aggregates must be re-aggregatable (SUM/COUNT/MIN/MAX; AVG via SUM+COUNT).

  This is what SQL Server indexed-view matching implements. If OpenIVM is going to
  have a matcher, reading this paper cover-to-cover is the first step.

### 1.6 Halevy survey; chase-based techniques
- **Halevy — "Answering Queries Using Views: A Survey", VLDB Journal 2001.** Still the
  best single overview. Contrasts integration ("find any rewriting") vs optimization
  ("find an equivalent cheaper rewriting").
- **Deutsch, Popa, Tannen — "Physical Data Independence, Constraints, and Optimization
  with Universal Plans", VLDB 1999** (and later 2006 work). Chase-and-back-chase: chase
  the query using views-as-constraints to a universal plan, then back-chase by
  replacing subplans with view accesses, guided by cost. Very general, expensive, not
  widely industrialized. Deutsch's UPenn PhD thesis (2002) has the full treatment.

### 1.7 PODS / theoretical side
- **Afrati & Chirkova — "Answering Queries Using Views"** (Morgan & Claypool monograph,
  2017). The definitive theoretical book; covers CQ, CQAC (with arithmetic),
  aggregates, recursion.
- **Nash, Segoufin, Vianu — "Views and queries: Determinacy and rewriting",
  PODS 2007 / TODS 2010.** A view-set *determines* a query iff any two databases
  agreeing on the views agree on the query; determinacy is necessary for rewriting to
  exist, but there are gaps (determinacy decidable yet no first-order rewriting).
- **Deutsch, Nash, Remmel — "The chase revisited", PODS 2008.**
- **Afrati — "Determinacy and query rewriting for conjunctive queries and views",
  TCS 2011.**

### 1.8 Oracle foundations
- **Bello, Dias, Downing, Feenan, Finnerty, Norcott, Sun, Witkowski, Ziauddin —
  "Materialized Views in Oracle", VLDB 1998.** First Oracle MV rewriter.
- **Witkowski, Bellamkonda, Bozkaya, Dorman, Folkert, Gupta, Sheng, Subramanian —
  "Advanced SQL Modeling in RDBMS", ACM TODS 2005 / SIGMOD 2003.** Aggregate modeling
  that later rewriting extensions (grouping sets, MODEL clause) built on.

---

## 2. Open-Source / Research Engines

### 2.1 Apache Calcite — the reference open-source matcher
- **Begoli, Camacho-Rodríguez, Hyde, Mior, Lemire — "Apache Calcite", SIGMOD 2018.**
- **Camacho-Rodríguez et al. — "Apache Hive: From MapReduce to Enterprise-Grade Big
  Data Warehousing", SIGMOD 2019.** Section on MV rewriting. Hive 3 integrates
  Calcite's MV rules.
- **Two mechanisms in Calcite:**
  1. **Substitution-based rewriting** (`SubstitutionVisitor`,
     `MaterializedViewSubstitutionVisitor`). Search for sub-plan equivalences
     between query plan and MV plan; replace matched sub-plans with a scan of the MV.
     Good for exact match.
  2. **Unification / Goldstein-Larson-style** (`MaterializedViewRule` and siblings in
     `org.apache.calcite.rel.rules.materialize.*`). Equivalence classes, range
     compatibility, compensation predicates, aggregate rollup. Implemented primarily
     by Jesús Camacho-Rodríguez.
- Supports PROJECT, FILTER, JOIN (inner + some outer), AGGREGATE, UNION ALL. AVG
  synthesized from SUM+COUNT like OpenIVM does.
- Limitations: conservative with outer joins; no window-function rewriting; one MV
  per query block (no chaining); PK-FK reasoning used only for join-elimination.
- Docs: https://calcite.apache.org/docs/materialized_views.html

### 2.2 StarRocks, Apache Doris, SelectDB
- **StarRocks** has the most capable *open-source* matcher (3.0+, 2023+). SPJG with
  aggregate rollup, equivalence classes, range compatibility, UNION handling. Doc:
  https://docs.starrocks.io/docs/using_starrocks/async_mv/
- **Apache Doris** Nereids optimizer (~2023) — MV rewriting, less mature than
  StarRocks.
- **SelectDB** = commercial fork of Doris, same matcher.

### 2.3 Trino / Presto
- Supports MVs (2021+), but *matching* is largely absent — "table redirection"
  features only. Widely recognized gap.

### 2.4 DuckDB (as of 2026)
- No native view-matching functionality. No built-in MV concept. Common-subexpression
  elimination exists *within* a single query but no cross-query MV rewrite. OpenIVM
  fills the MV gap; a matcher would be a natural follow-on contribution.

### 2.5 Umbra / HyPer (TUM) — Klocke thesis is the reference here
- **Klocke, "View Matching on Query Graphs", Master's Thesis, TUM, March 2022.**
  Supervisor Thomas Neumann; Advisor Michael Freitag. Implemented in **Umbra**.
  Core idea: reduce view matching to a **subgraph-isomorphism / graph-overlay
  problem** between extended query graphs — the *same* query graph the optimizer
  builds for join ordering. Linear-time O(|V_V| + |V_Q| + |E_Q|).
  - Handles SPJ, outer joins (hyperedges encoding reordering constraints from
    Galindo-Legaria / Rosenthal), non-SPOJ operators via "connecting operators"
    that split the query graph into SPOJ components at arbitrary operator
    boundaries.
  - Four Goldstein-Larson-style criteria in graph terms: cardinality (hub of
    FK-PK cardinality-preserving joins), tuple inclusion (predicate implication
    `p(e_Q) ⇒ p(e_V)`, optionally via SAT or pairwise fast path), attribute
    inclusion (union-find equivalence classes), residual-predicate derivation.
  - **Plan-generator integration**: extends the Moerkotte-Neumann DP-table to a
    "MapTable" so the optimizer considers views during enumeration, not as a
    post-pass. Guarantees optimal plan given the view set.
  - **Benchmark**: matcher completes in ~5s on a query with 2^13 = 8192 relations
    where Umbra's full semantic analysis takes ~13 minutes — three orders of
    magnitude faster. But Umbra had no MVs at time of writing, so the paper
    *couldn't* measure MV speedup — it measured matcher overhead only.
  - **Explicit future work in §6** (this list matters — these are the open
    extensions Klocke names): aggregations (SUM/COUNT rollup and AVG=s/c
    rewrite), unions, anti-joins, nullification, join degradation (outer-in-view
    answering inner-in-query), hub-generation for extension joins, and a
    standardized view-matching benchmark.
  - **References Klocke cites**: Levy-Mendelzon-Sagiv, Halevy survey,
    Goldstein-Larson, Larson-Zhou outer-join matching (VLDB Journal 2007),
    Bello et al. Oracle, Zaharioudakis DB2, Chaudhuri ICDE'95, Cohen-Nutt-Sagiv
    aggregate rewriting, Galindo-Legaria "Outerjoins as disjunctions" SIGMOD
    1994, Rosenthal-Galindo-Legaria "Query graphs... freely-reorderable
    outerjoins" SIGMOD 1990, Moerkotte-Neumann "Dynamic programming strikes
    back" SIGMOD 2008. Does *not* cite Calcite, Camacho-Rodríguez (Hive MV
    rewrite), Enzyme (too recent).
  - **Local copy**: `/home/ila/Code/openivm/Master_Thesis__View_Matching (21).pdf`.

**Implications for OpenIVM.** The base matcher algorithm is essentially done; the
open extensions are precisely the ones OpenIVM's infrastructure already supports
(aggregates, IVM, freshness, DuckLake snapshots). An OpenIVM matcher paper should
credit Klocke explicitly and frame as "Klocke's graph-overlay matcher, extended
with aggregate rollup + IVM-aware freshness + snapshot-interval residuals."

### 2.6 Noria — partial materialization (not classical matching)
- **Gjengset et al. — "Noria: dynamic, partially-stateful data-flow for high-performance
  web applications", OSDI 2018.** Materializes only portions touched by reads;
  cache misses trigger upquery recomputation. Queries target views by name, not a
  matcher.

### 2.7 PostgreSQL / pg_ivm
- **pg_ivm** (SRA OSS, 2022+) — IVM for PG MVs but no matcher.
- PostgreSQL core has no MV rewriter. Discussed on pgsql-hackers, never merged.

---

## 3. Commercial Systems — Real Matcher Capabilities

### 3.1 Oracle — arguably still the most mature
- **Enforced / trusted** constraints (PK, FK, NOT NULL); **dimensions** (day→month→year
  hierarchies); **rewrite modes** `ENABLE QUERY REWRITE`, `REWRITE_INTEGRITY
  = {ENFORCED|TRUSTED|STALE_TOLERATED}`.
- Four techniques: full text match, partial text match, general rewrite
  (Goldstein-Larson-like with compensation), dimension-based rollup.
- `REWRITE_OR_ERROR` hint forces rewrite-or-fail.
- Docs: Oracle DW Guide, "Basic" and "Advanced Query Rewrite" chapters. Handles SPJG,
  grouping sets, some UNION ALL. Outer-join rewriting limited.

### 3.2 SQL Server / Azure SQL
- Implements Goldstein-Larson 2001 for **indexed views**. Automatic; `WITH (NOEXPAND)`
  forces, `OPTION (EXPAND VIEWS)` forbids.
- Strict requirements: SCHEMABINDING, deterministic expressions, no outer joins, no
  subqueries, no UNION, no TOP, specific ANSI settings.
- Azure Synapse Dedicated and Fabric Warehouse have their own variants with some
  aggregate extensions and relaxations.

### 3.3 IBM DB2 — MQTs
- Materialized Query Tables with `REFRESH DEFERRED|IMMEDIATE`; rewrite governed by
  `SET CURRENT REFRESH AGE` / `MAINTAINED TABLE TYPES FOR OPTIMIZATION`. Direct
  descendant of Zaharioudakis 2000. Historically one of the strongest matchers;
  handles SPJG with outer joins, UNION ALL, aggregate rollup via FD reasoning.

### 3.4 Teradata
- Aggregate Join Index (AJI), Sparse Join Index, Hash Index. Matcher integrated in
  Teradata Optimizer; aggregate rollup, residual predicates, FD-based join elimination.
  Not much academic literature.

### 3.5 Snowflake
- **Dageville et al. — "The Snowflake Elastic Data Warehouse", SIGMOD 2016** —
  architecture paper, not much on matchers.
- Snowflake MVs are clustered, auto-refreshed, but the matcher is **restricted**: SPJ
  with a single source table + aggregates; no joins in the MV definition (as of 2024
  docs; may have softened). Lightweight compared to Oracle/SQL Server.
- **Search Optimization Service** (point lookups) and **Query Acceleration Service**
  (offload scans) are *not* matching — they're secondary structures / runtime
  techniques.
- **Result cache** (per account, 24h TTL) matches on normalized query text + data
  version; no semantic rewriting.
- Docs: https://docs.snowflake.com/en/user-guide/views-materialized

### 3.6 Google BigQuery
- MVs with "smart tuning" / automatic refresh; "automatic rewrite" supports SPJG with
  aggregate rollup. Limited join support. No public paper on the rewriter algorithm
  specifically.
- **BI Engine** is an in-memory cache, not a matcher.
- Docs: https://cloud.google.com/bigquery/docs/materialized-views-intro

### 3.7 Amazon Redshift
- **Automated materialized views (AutoMV)** — creates, maintains, and rewrites
  workload-driven. SPJG + aggregate rollup + some UNION.
- Separate per-cluster **Result Cache**.
- Related Amazon Science publications from Narasayya/Chaudhuri alums.

### 3.8 Databricks — Delta Live Tables + Enzyme
- MVs in Unity Catalog (GA 2024) refreshed incrementally via Delta Live Tables.
- **Enzyme (Yadav, Abeysinghe, Yang, Helt, Ung, Chen, Hu, Wei, Yang, van Bussel,
  Chatterji, Roy, Lappas, Papakonstantinou, Fayyaz, Aslam, Bunker, Armbrust, Shankar —
  "Enzyme: Incremental View Maintenance for Data Engineering", SIGMOD-Companion
  2026).** arXiv: https://arxiv.org/pdf/2603.27775v1
  - Not primarily a view-matching paper — it's IVM for Databricks. But its
    *fingerprinting* step (see §4.2 of the paper) is a simple exact-match form of
    view matching: the normalizer produces a canonical Catalyst logical plan, the
    fingerprint evaluator hashes it, and a changed fingerprint triggers complete
    recomputation. Normalization includes CTE inlining, filter merging, projection
    collapse, commutative operator ordering, and UDF bytecode signatures (for Python
    UDFs). Fingerprint versioning preserves stability across system upgrades.
  - Architecture: Normalizer → Fingerprint Evaluator → Decomposer (technique enablers:
    AVG→SUM/COUNT, explicit group/partition key propagation, row_id synthesis) →
    Incremental Plan Generator (bottom-up delta rules, composable change plans
    (pre-state, post-state, delta); same algebra as DBSP/OpenIVM) → Cost Model
    (incremental vs full recompute; learned from historical executions) → Execution
    (Spark Catalyst, MERGE INTO or REPLACE WHERE).
  - Special cases: **temporal filters** (`current_timestamp()` / `current_date()` —
    three-term delta `π_(¬old∧new)(T) + ΔT_current - π_(old∧¬new)(T)`); **partition
    overwrite** when tables and MVs share partitioning; **leveraging materialized
    data** — use the MV's previous aggregate rather than recomputing pre-state.
  - Non-determinism: detects `rand()`, `collect_set`, floating-point AVG/SUM, unordered
    window ties; applies semantics-preserving rewrites where possible, falls back to
    full recompute otherwise.
  - Takeaway for OpenIVM: Enzyme is the closest contemporary sibling. Its matcher
    is weaker than Goldstein-Larson (exact-match via fingerprint only) — a real
    OpenIVM matcher plus the existing IVM engine would go beyond Enzyme.
- **Photon (Behm et al., VLDB 2022)** is an execution engine, not a matcher.
- Databricks **disk cache** / **Delta cache** operate on files, not queries.

### 3.9 Firebolt
- "Aggregating indexes", "Join indexes", "Primary indexes" — pre-computed MVs the
  optimizer transparently uses. SPJG + aggregate rollup. No paper.

### 3.10 ClickHouse
- **Projections** (21.x+) — per-table alternative orderings/aggregations, auto-matched
  at query time. Lightweight matching: predicate pushdown compatibility + aggregate
  rollup. Narrower than Goldstein-Larson.
- **Materialized views** are INSERT-trigger-based; no automatic SELECT rewriter uses
  them (as of 2024). Queries must target MVs directly.

### 3.11 MotherDuck
- Dual execution (local + cloud DuckDB). Public blogs (Jordan Tigani — "Big Data is
  Dead" 2023, "The Data Cache Lake" series; Hannes Mühleisen's DuckDB talks) focus on
  column-level and block-level caching, zero-copy, hybrid execution. **No public
  MotherDuck design on MV rewriting or semantic view matching** — emphasis is data
  caching, not query-level matching.
- **Haderer, "Declarative Caching in MotherDuck"**, Master's Thesis, VU/UvA,
  October 2024 (Boncz/Leskes/Holanda). Introduces **Accelerated Approximate Views
  (AAVs)**: a partial client-side query cache via a DuckDB parser extension with
  `CREATE RESULT my_result AS SELECT ...`. Decision tree at query time: (1)
  sufficient cache → read cache, (2) pending cache + guaranteed satisfiable (LIMIT
  below threshold) → wait, (3) insufficient → re-execute original. *Not a
  matcher* — it's partial-result caching with LIMIT-bounded satisfiability, a
  custom catalog for cache metadata, and background cache-build scheduling (the
  Wasm single-thread case is most of the paper). Adjacent to OpenIVM as
  infrastructure precedent (parser extension, custom catalog, background
  population) but orthogonal in problem space. Local copy:
  `/home/ila/.claude/projects/-home-ila-Code-openivm/b103b57e-7e6e-4e16-82e9-4af49a69c2be/tool-results/webfetch-1777023079907-0kaco3.pdf`.

### 3.12 SingleStore / MemSQL
- MVs supported; matcher limited; result cache per node.

### 3.13 Alibaba / Asian cloud
- **PolarDB-X** — Calcite-derived rewriter.
- **MaxCompute / AnalyticDB** — proprietary rewriters; some VLDB/SIGMOD industrial
  papers; limited public docs.
- **Hologres** — column+row hybrid; "dynamic index" claims but not a classical matcher.

### 3.14 Others
- **Vertica projections** — every table stored as one-or-more projections; optimizer
  transparently picks the best one. Lamb et al., "The Vertica Analytic Database:
  C-Store 7 Years Later", VLDB 2012. Probably the system where the "pick the right
  precomputed structure" story is pushed hardest.
- **MonetDB** — no classical matcher; has recycler work (see §4).
- **Citus** — no MV matching.
- **Kinetica** — GPU-accelerated, limited MV support.

---

## 4. Adjacent: Result / Semantic Caching, Recycling, Computation Reuse

### 4.1 Semantic caching
- **Dar, Franklin, Jonsson, Srivastava, Tan — "Semantic Data Caching and Replacement",
  VLDB 1996.** Cache regions defined by predicates; overlapping queries probe cache
  + generate residual queries. Influential for client-side middleware caches.
- **Keller & Basu — "A predicate-based caching scheme for client-server database
  architectures", VLDB 1996.**
- **DBProxy (Amiri et al., IBM Research, 2003)** — web-tier DB cache using containment.

### 4.2 Intermediate recycling
- **Ivanova, Kersten, Nes, Gonçalves — "An Architecture for Recycling Intermediates in
  a Column-Store", SIGMOD 2010.** MonetDB recycler: cache MAL operator results;
  match incoming plans against cached intermediates at fine granularity.
- **Nagel, Boncz, Viglas — "Recycling in Pipelined Query Execution", ICDE 2013.**
- **Perez & Jermaine — "History-Aware Query Optimization With Materialized Intermediate
  Views", ICDE 2014.** Select *which* intermediates to materialize from observed
  workload and rewrite later queries to reuse.

### 4.3 Microsoft SCOPE / Cosmos — the strongest industrial line
- **Jindal, Qiao, Patel, Yin, Di, Bag, Friedman, Lu, Karanasos, Rao — "Computation
  Reuse in Analytics Job Service at Microsoft", SIGMOD 2018.** 40-60% of SCOPE job
  work is redundant across jobs. Detects common sub-plans and materializes them.
- **Jindal, Karanasos, Rao et al. — "Peregrine: Workload Optimization for Cloud Query
  Engines", SIGMOD/SoCC 2019.** Workload→optimizer feedback loop: column learning,
  cardinality correction, MV selection.
- **"CloudViews" (Jindal et al., VLDB 2018)** — sharing MVs across workloads.
- **"Helios" (Potharaju et al., VLDB 2020)** — scalable metadata for reuse at scale.
- This is probably the most impressive body of industrial computation-reuse work in
  the last decade. Directly relevant to OpenIVM's adaptive-materialization direction.

### 4.4 Google
- **Mesa (Gupta et al., VLDB 2014)** — geo-replicated analytical warehouse for ads;
  aggregates maintained via delta tables with MVCC. Direct influence on OpenIVM's
  delta model.
- **F1 Lightning (Yang et al., VLDB 2020)** — HTAP via CDC replication into columnar
  store. View-level rather than query-level matching.
- **Procella (Chattopadhyay et al., VLDB 2019)** — YouTube analytics; precomputed
  aggregates + file-level cache + query-result cache.
- **Napa (Agarwal et al., VLDB 2021)** — real-time MVs at Google scale; explicit
  freshness/cost/latency tradeoff via "queryable timestamps". Highly relevant to
  OpenIVM's REFRESH EVERY model.

---

## 5. Recent / ML-for-Matching (treat as softer citations)

- **Jindal, Interlandi et al.** — continued workload-aware materialization at
  Microsoft GSL.
- **Ding, Das, Marcus, Wu, Narasayya, Chaudhuri — "AI Meets AI: Leveraging Query
  Executions to Improve Index Recommendations", SIGMOD 2019.** Same "learn from
  workload" paradigm, applied to indexes.
- **Marcus, Negi, Mao, Zhang, Alizadeh, Kraska, Papaemmanouil, Tatbul — Neo / Bao** —
  learned query optimization; Bao (SIGMOD 2021) could in principle learn to use MVs.
- LLM-based matching work is emerging (2024-2026) but fuzzy enough that specific
  citations should be verified before being load-bearing.

---

## 6. OpenIVM Integration — Design Notes

### 6.1 Addressing the user's premise

> "Is there no real engine properly supporting view matching, am I wrong?"

Partially wrong, partially right.
- **Mature matchers exist** in Oracle, SQL Server, DB2, Teradata, Calcite (and thus
  Hive/StarRocks/Doris).
- But the **open-source state is significantly behind**: Calcite is the best
  open-source matcher and is already second-tier compared to Oracle.
- **All systems have substantial gaps**: SPJGA only, no window rewriting, rare
  outer-join rewriting, no multi-MV chaining, shallow FD reasoning, staleness-aware
  matching essentially absent except Oracle's coarse `REFRESH AGE`.
- The newest industrial IVM work — **Enzyme (Databricks SIGMOD 2026)** — does not
  include a real matcher; its "fingerprint" step is exact-match only.

Translation: there is real room to contribute. The opportunity is not "invent view
matching" — that's 30+ years old — but to combine an industrial-strength matcher with
IVM + cost-aware freshness in a way no existing system does.

### 6.2 Realistic matcher designs for OpenIVM

**A. Port Goldstein-Larson on top of LPTS.** LPTS already converts logical plans to
SQL and back; extend it to canonicalize plans (equivalence classes, range
representation, GROUP-BY normalization) and run the SIGMOD 2001 filter-tree +
compensation algorithm. This alone would make DuckDB the best open-source matcher
outside Calcite.

**B. Delta-aware matching.** Most matchers treat MVs as static and either use them or
not. OpenIVM knows `|ΔT|` at refresh time. Extend the cost model to score a rewriting
as `cost(use_MV + apply_pending_delta)` vs `cost(rewrite_to_base)`. When the delta is
small, prefer the MV-plus-fresh-delta path even if the MV is "stale". Novel.

**C. DBSP-aware aggregate rewriting.** With `_duckdb_ivm_multiplicity` tracking ±1,
the matcher can answer queries whose GROUP BYs differ from the view's using the
SUM/COUNT pair + multiplicity — beyond the classical compositional rules.

**D. DuckLake time-range matching.** Snapshot MVs can answer `AS OF` queries that
existing matchers don't handle. Oracle and Snowflake support time travel but don't
*match* time-range queries to time-range MVs.

**E. Multi-MV chaining with FD-aware join elimination.** Only DB2 does this well.
A modest extension of (A).

**F. Adaptive materialization via matcher feedback.** Microsoft Peregrine-style: log
every "almost-match" (match that failed due to one missing column or one extra
predicate) and propose MV definitions that would make future queries match. Ties into
OpenIVM's existing cost model + learned regression.

### 6.3 Publishable combination

Matcher (A) + (B) + (C) + (D) is a concrete paper: "IVM-aware, delta-cost-aware
view matching for DuckDB over DuckLake". None of those four pieces is individually
novel, but the combination does not exist in any shipping or research system. Enzyme
is the closest comparable, and its matcher is weaker.

### 6.4 What to reuse, what to invent
- **Reuse**: Goldstein-Larson filter-tree + equivalence-class machinery; Calcite's
  substitution rule as a fast path; Srivastava-Dar-Jagadish-Levy aggregate rollup;
  FD-driven join elimination from Zaharioudakis et al.
- **Invent**: delta-aware cost integration; IVM-with-multiplicity-aware aggregate
  matching; DuckLake snapshot range matching.

---

## 7. Per-System Capability Summary (Quick Reference)

For each system: algorithm family, the SQL constructs it handles in its matcher, whether
it understands MV-on-MV pipelines, and the obvious gap. Take vendor claims with a grain of
salt; where possible this is grounded in docs + papers, but details evolve.

### Oracle
- **Algorithm**: Goldstein-Larson-style with four layered strategies (text match → full
  exact → partial text → general rewrite). Uses `CREATE DIMENSION` hierarchies + RELY
  constraints for rollup and join-elimination. **Contained rewriting supported.**
- **Handles**: projection, filter compensation, inner join, LEFT OUTER (join-back to PK),
  UNION ALL MVs (since 9i), GROUP BY rollup via dimensions, SUM/COUNT/MIN/MAX/AVG,
  HAVING as residual, FD/PK-FK join elim, multi-MV combination.
- **Doesn't handle**: window functions, recursive CTE, EXCEPT, FULL OUTER rewriting,
  DISTINCT-COUNT sketches, cross-MV pipeline chains (matcher is flat).
- **Staleness**: `QUERY_REWRITE_INTEGRITY = ENFORCED | TRUSTED | STALE_TOLERATED` — the
  most explicit knob of any system.
- **Pipelines**: nested MVs allowed but matcher doesn't navigate DAGs first-class.

### Microsoft SQL Server / Azure SQL
- **Algorithm**: Substitution-based with strict schema-binding. Implements the
  Goldstein-Larson filter-tree in its optimizer's View Matching module.
  **Equivalent-only.**
- **Handles**: projection, filter subsumption, inner join, SUM/COUNT_BIG.
- **Doesn't handle**: outer joins, UNION, MIN/MAX, AVG, DISTINCT, HAVING, subqueries,
  CTEs, window functions, set-difference — all **disallowed in indexed views**.
- **Staleness**: none; indexed views are synchronously maintained.
- **Pipelines**: not allowed (indexed views on indexed views disallowed).

### IBM DB2 (MQTs)
- **Algorithm**: Reference Goldstein-Larson descendant, from Zaharioudakis et al.
  SIGMOD 2000. Uses informational (RELY) constraints + staging tables for incremental
  propagation.
- **Handles**: projection, filter, inner join, LEFT OUTER (when nullability preserved),
  UNION ALL MVs first-class, GROUP BY rollup, SUM/COUNT/MIN/MAX/AVG, HAVING,
  FD/PK-FK join elim, multi-MV combination. **Contained rewriting supported.**
- **Doesn't handle**: window functions, recursive CTE, set-difference, full outer.
- **Staleness**: `CURRENT REFRESH AGE` + `MAINTAINED BY` clauses.
- **Pipelines**: staging tables suggest a DAG but MQT-on-MQT rewriting is not
  advertised as first-class.

### Teradata (Aggregate Join Index)
- **Algorithm**: Substitution with aggregate-matching extensions. Classes: single-table,
  multi-table, aggregate, sparse. Uses declared RI constraints for join-elim.
- **Handles**: projection, inner join, GROUP BY rollup (AJI's raison d'être),
  SUM/COUNT/MIN/MAX/AVG, FD-based join elim.
- **Doesn't handle**: UNION, window, CTE, outer joins (mostly), set-difference, async
  staleness knobs.
- **Pipelines**: none.

### Snowflake
- **Algorithm**: Substitution on single-table MVs only — **no joins allowed in MV**
  definition. Matcher is effectively filter+projection+aggregate subsumption. Result
  cache is orthogonal textual-hash (24h, per-account).
- **Handles**: projection, filter subsumption, SUM/COUNT/MIN/MAX on single table.
- **Doesn't handle**: joins, UNION, outer joins, AVG (excluded), window, CTE, rollup
  rewrite, DISTINCT sketches, multi-MV.
- **Staleness**: automatic background refresh, consistent reads.
- **Pipelines**: none for MVs. **Dynamic Tables** are a separate DAG-style feature
  (streaming/incremental) but have no matcher — queries address them by name.

### Google BigQuery
- **Algorithm**: Substitution with "smart tuning" automatic rewrite. Single- or
  single-join MV matching with aggregate rollup on partition-aligned groupings.
  BI Engine is orthogonal in-memory cache.
- **Handles**: projection, filter, partition-aligned rollup, SUM/COUNT/MIN/MAX/AVG,
  **`APPROX_COUNT_DISTINCT` / HLL sketch rewrite** (the interesting bit).
- **Doesn't handle**: outer joins, UNION, window, recursive CTE, set-difference,
  multi-MV, FD-based join elim.
- **Staleness**: `max_staleness` clause — explicit freshness knob like Oracle's.
- **Pipelines**: not first-class.

### Amazon Redshift (AutoMV)
- **Algorithm**: Substitution + workload-driven MV *creation* recommender (AutoMV,
  notable autonomic angle). Matching itself is classical subsumption.
- **Handles**: projection, filter, inner join, GROUP BY rollup,
  SUM/COUNT/MIN/MAX/AVG, partial outer-join rewriting, partial CTE.
- **Doesn't handle**: UNION, window, recursive CTE, set-difference, multi-MV.
- **Staleness**: configurable auto-refresh.
- **Pipelines**: none.

### Databricks (DLT + Enzyme, SIGMOD 2026)
- **Algorithm**: Enzyme uses normalized-plan **fingerprint hashing** (not classical
  matching — invalidation trigger, not general subsumption). DLT provides the
  pipeline-DAG abstraction around MVs. Contained rewriting via DBSP-style delta rules.
- **Handles**: projection, filter, inner + partial outer joins, UNION ALL,
  SUM/COUNT/MIN/MAX/AVG, HAVING, **window with watermarks**, subqueries/CTEs,
  multi-MV via pipeline DAG (this is the unique strength).
- **Doesn't handle**: recursive CTE, set-difference beyond shallow, full-outer IVM
  beyond research.
- **Staleness**: SLA-based per-table refresh schedules.
- **Pipelines**: **first-class** — DLT is the only system in this list where MV DAGs
  are the primary abstraction. Matcher naturally threads through chains.
- *Citations*: Armbrust et al., "Delta Live Tables", CIDR 2023; Yadav et al., "Enzyme:
  Incremental View Maintenance for Data Engineering", SIGMOD-Companion 2026
  (arXiv:2603.27775).

### Firebolt
- **Algorithm**: Substitution via specialized index types (aggregating, join, primary).
- **Handles**: projection, filter, inner join (via join index), rollup,
  SUM/COUNT/MIN/MAX/AVG.
- **Doesn't handle**: outer joins, UNION, window, CTE, multi-MV, FD reasoning.
- **Pipelines**: none.

### ClickHouse
- **Algorithm**: **Projections matched per data part** at query planning time — unusual
  *part-granularity matching*. MVs themselves are INSERT-trigger-based and have
  **no auto-match** — users query them by name.
- **Handles (projections only)**: projection, partial filter, aggregate projections
  with `-State` combinators for partial-aggregate re-aggregation (including HLL via
  `uniqHLL12State`).
- **Doesn't handle**: joins in projections, outer joins, UNION, HAVING, window, CTE,
  recursive, set-difference, multi-MV.
- **Pipelines**: MV chains via triggers, no matcher awareness.

### SingleStore
- **Algorithm**: Substitution on incrementally maintained MVs; textual-hash result
  cache.
- **Handles**: projection, partial filter, partial inner join, SUM/COUNT/MIN/MAX.
- **Doesn't handle**: outer joins, UNION, HAVING, window, CTE, recursive,
  set-difference, multi-MV.
- **Pipelines**: none.

### Vertica (Projections + LAPs)
- **Algorithm**: Projections are the physical layout (every query matches a
  projection — physical design, not Goldstein-Larson). **Live Aggregate Projections**
  and **Top-K Projections** are MV-like; matched via subsumption.
- **Handles**: projection, filter, pre-join projections, rollup for LAPs,
  SUM/COUNT/MIN/MAX.
- **Doesn't handle**: AVG/DISTINCT incrementally, outer joins, UNION, window, CTE,
  recursive, set-difference, multi-MV.
- **Pipelines**: none.

### MotherDuck
- **Algorithm**: **No matcher** — result/data caching only; exact-text result cache.
- **Pipelines**: none. Green-field for OpenIVM as the MV layer atop MotherDuck.

### Apache Calcite
- **Algorithm**: Most *algorithmically complete open-source* matcher. Two paths:
  (a) unification-based substitution (`SubstitutionVisitor`); (b) Goldstein-Larson-
  style (`MaterializedViewRule` family). **Calcite Lattice** extends aggregate
  matching to rollup-cube hierarchies.
- **Handles**: projection, filter compensation, inner join, partial LEFT/RIGHT outer,
  UNION ALL partial, lattice-aware rollup, SUM/COUNT/MIN/MAX/AVG, HAVING,
  FD/PK-FK join elim, partial multi-MV. Contained rewriting.
- **Doesn't handle**: window, recursive CTE, set-difference, HLL, FULL OUTER.
- **Staleness**: N/A (library; embedder's problem).
- **Pipelines**: library-level; embedder decides.

### Apache Hive (on Calcite)
- **Algorithm**: Inherits Calcite + adds **snapshot-aware incremental rebuild** on
  ACID tables (Hive 3+). Matcher understands MV freshness relative to source txn IDs
  — the closest analog to what DuckLake enables. Transactional MVs with incremental
  maintenance.
- **Handles**: Calcite's set + staleness against source snapshots.
- **Pipelines**: MV-on-MV supported; matcher extends through chains shallowly.
- *Citations*: Camacho-Rodríguez et al., SIGMOD 2019.

### StarRocks / Apache Doris
- **Algorithm**: Calcite-inspired Goldstein-Larson reimplementation in C++. Partitioned
  incremental refresh with automatic rewrite. Doris diverges on pipeline semantics.
- **Handles**: projection, filter, inner + LEFT OUTER, UNION ALL MVs first-class,
  rollup, SUM/COUNT/MIN/MAX/AVG, **DISTINCT via bitmap, HLL via `hll_union`
  (rewritable)**, HAVING, partial FD/PK-FK join elim, multi-MV.
- **Doesn't handle**: window, recursive CTE, set-difference, FULL OUTER.
- **Staleness**: `max_refresh_lag` per MV.
- **Pipelines**: MV-on-MV allowed; matcher navigates shallowly.

---

### Compact Matrix

| System       | Alg family        | Joins | Outer | UNION | Rollup | Window | RecCTE | Multi-MV | Pipelines | Staleness knob |
|--------------|-------------------|-------|-------|-------|--------|--------|--------|----------|-----------|----------------|
| Oracle       | Goldstein-Larson  | Y     | L     | Y     | Y      | N      | N      | Y        | flat      | 3-mode         |
| SQL Server   | Substitution      | Y     | N     | N     | L      | N      | N      | N        | N         | —              |
| DB2 MQT      | Goldstein-Larson  | Y     | L     | Y     | Y      | N      | N      | Y        | shallow   | CURRENT REFRESH AGE |
| Teradata AJI | Substitution+     | Y     | L     | N     | Y      | N      | N      | L        | N         | —              |
| Snowflake MV | Substitution      | N     | N     | N     | L      | N      | N      | N        | N         | auto           |
| BigQuery     | Substitution      | L     | N     | N     | Y      | N      | N      | N        | N         | max_staleness  |
| Redshift AMV | Substitution+ML   | Y     | L     | N     | Y      | N      | N      | N        | N         | auto           |
| Databricks   | Fingerprint+DBSP  | Y     | L     | Y     | L      | L(WM)  | N      | **Y**    | **DAG**   | SLA per table  |
| Firebolt     | Index-substit.    | Y     | N     | N     | Y      | N      | N      | N        | N         | sync           |
| ClickHouse   | Per-part          | N     | N     | N     | Y      | N      | N      | N        | N         | sync           |
| SingleStore  | Substitution      | L     | N     | N     | L      | N      | N      | N        | N         | config         |
| Vertica      | Physical+LAP      | L     | N     | N     | Y      | N      | N      | N        | N         | sync           |
| MotherDuck   | none              | —     | —     | —     | —      | —      | —      | —        | —         | —              |
| Calcite      | Substit.+G-L      | Y     | L     | L     | Y      | N      | N      | L        | N/A       | N/A            |
| Hive         | Calcite+snapshot  | Y     | L     | L     | Y      | N      | N      | L        | shallow   | **snapshot**   |
| StarRocks    | G-L in C++        | Y     | L     | Y     | Y      | N      | N      | Y        | shallow   | max_refresh_lag|

`Y`=yes, `L`=limited/partial, `N`=no. `WM`=window with watermark.

---

## 8. Pipeline Support Observation

The user's intuition is right. **Of all the systems above, only Databricks DLT has
true first-class MV-pipeline support** (MV DAGs with dependency-aware refresh and
compensation chained across levels). Snowflake Dynamic Tables define pipelines but
don't match them. Hive, Oracle, DB2, StarRocks allow MV-on-MV but their matchers
navigate chains shallowly — usually "either the top MV matches, or none." No system
does **cross-MV compensation** (query matches MV_3 for 80% + compensates from MV_1).

---

## 9. Novel Angles for OpenIVM (ranked by strategic leverage)

Eight concrete opportunities. The top three are simultaneously (a) absent or shallow
across all 16 systems above, (b) within DuckDB's reach, and (c) aligned with existing
OpenIVM infrastructure.

### 9.1 DuckLake snapshot-range / time-range matching — **top pick**
Only Hive (ACID) and partially Databricks DLT understand "MV valid up to snapshot S;
query reads snapshot S'." OpenIVM can define a **snapshot-interval subsumption**
relation: given an MV covering `[s_low, s_high]` and a query at `s_q`, produce a
*compensation plan over the snapshot delta*. Partially-stale MVs answer fresh queries
by applying only the incremental delta. Generalizes Hive's coarse REBUILD trigger.
No commercial matcher does this as a first-class primitive.

### 9.2 Pipeline-DAG matcher with cross-MV compensation — **top pick**
Only Databricks DLT handles pipelines first-class, and even DLT doesn't do cross-MV
compensation. Build a matcher that **walks an MV DAG and finds the cheapest chain of
partial rewrites** — match at level 3 for 80%, compensate from level 1 for the
remaining 20%. Combines with OpenIVM's existing `ivm_cascade_refresh`.

### 9.3 IVM-aware adaptive matching — **top pick**
Every classical matcher (including Calcite) treats matching as yes/no. Enzyme
(Databricks SIGMOD 2026) is the first to consider IVM cost at rewrite decision time,
but only for refresh-vs-recompute, not for matching itself. OpenIVM already has
`ivm_adaptive_refresh` + learned regression. Extend: at optimization time, consider
`cost(use_MV + apply_pending_delta)` vs `cost(rewrite_to_base) + cost(refresh_later)`.
Choose per query. Frame as "delta-cost-aware matching."

### 9.4 Window-function IVM + matching
**Universally missing** — only Databricks does watermarked approximation. DBSP has
theoretical groundwork (Budiu et al., VLDB 2023). Incremental LAG/LEAD/row_number
over append-only segments + matching is non-trivial but publishable.

### 9.5 Sketch-aware matcher (HLL, t-digest, KLL, Theta)
BigQuery, ClickHouse, StarRocks each support some sketch re-aggregation but shallow
matching. Register a sketch type with merge semantics; let the matcher reason about
it as a first-class aggregate family. OpenIVM's LPTS can already serialize sketch
operators.

### 9.6 EXCEPT / set-difference and recursive-CTE IVM matching
Nobody does this in production. DBSP covers it theoretically. Even partial support
is a differentiator — DuckDB already executes recursive CTEs, so IVM+matching for
reachability / BOM explosion / transitive closure has clear applications.

### 9.7 Per-partition / per-snapshot matching (borrowed from ClickHouse)
ClickHouse matches projections per data part. OpenIVM could match **per DuckLake
snapshot range** or per partition, picking different MVs for different slices of
a query's time range. Combines with 9.1 into a multi-source compensation planner.

### 9.8 FULL OUTER JOIN matcher (Zhang & Larson extended)
Support is shallow everywhere. OpenIVM already plans `ivm_full_outer_merge` per
AGENTS.md/CLAUDE.md. Extending the matcher to reason about FULL OUTER MV subsumption
(a FULL OUTER MV answering INNER/LEFT queries by filtering null-side keys) is
concrete, bounded, defensible.

---

## 10. Detailed Per-System Matcher History and Limits

This section goes beyond the capability-matrix table. For each system: when the MV
machinery was introduced, what SQL features were in the world at the time (which
explains most of today's blind spots), what the MV itself can actually contain, and
what the matcher *specifically* fails on — the latter is usually where the footguns
live.

### 10.1 Oracle (MVs since 8i, 1998)

Oracle shipped materialized views in **Oracle 8i (1998)** under the name "snapshots"
— a term inherited from Oracle 7's (1992) replication primitive. The query-rewrite
optimizer arrived with 8i; **9i (2001)** added *partition change tracking* and
*general query rewrite*; **10g** added `DBMS_ADVANCED_REWRITE.DECLARE_REWRITE_EQUIVALENCE`
for DBA-declared equivalences the optimizer can't prove; **12c (2013)** added
synchronous refresh with staging logs and `ON STATEMENT` refresh; **19c** stabilized
*real-time MVs* (MVs that compute a delta on the fly to answer fresh queries).
**23ai (2023)** adds JSON-relational duality views but these are a separate mechanism
outside the classical matcher. Design lineage: Chaudhuri/Krishnamurthy/Potamianos/Shim
ICDE'95 and Levy/Mumick/Sagiv.

**MV capabilities.** Over SPJG, set ops (UNION ALL/UNION/INTERSECT/MINUS), subqueries
in FROM, window functions (with fast-refresh restrictions), limited LATERAL. Can be
partitioned/indexed/compressed independently. Refresh modes: COMPLETE, FAST
(incremental — requires MV logs with `ROWID, SEQUENCE, INCLUDING NEW VALUES`, sometimes
`COMMIT SCN`), FORCE, NEVER. Refresh timing: ON DEMAND, ON COMMIT, ON STATEMENT (12.2+),
DBMS_SCHEDULER. Fast-refreshable MVs have many documented restrictions: aggregate MVs
must include `COUNT(*)` and `COUNT(col)` for every `SUM(col)`; DISTINCT isn't
fast-refreshable unless internally rewritten; joins must be inner except in
restricted 11g+ cases. `DBMS_MVIEW.EXPLAIN_MVIEW` reports per-MV capability (PCT,
fast refresh, rewrite) *and why not* when unavailable — a genuinely useful debugging
feature no other vendor matches.

**Matcher.** Four modes: `TEXT_MATCH`, `PARTIAL_TEXT_MATCH`, `GENERAL` (cost-based
Goldstein-Larson), `STALE_TOLERATED`. Predicate subsumption works reliably for ranges
and IN-lists (the `region IN ('us-east','us-west')` vs MV with `IN ('us-east','us-west','asia')`
case is matched with a residual filter). Join-back via declared FK+NOT-NULL or RELY
dimensions. Aggregate rollup via `CREATE DIMENSION` chains.

**Blind spots.** Nondeterministic under complex nested correlated subqueries.
`QUERY_REWRITE_INTEGRITY = ENFORCED` refuses RELY-backed MVs — real warehouses must
set `TRUSTED`, widening the rewrite window based on DBA assertion. MVs with window
functions are almost never matched against non-identical queries. **Type-coercion
differences** (VARCHAR2/CHAR, NUMBER precision) defeat matching more often than users
expect — a recurrent AskTom/MOS complaint. Collation-sensitive matching tightened
in 12.2 but remains a source of rewrite failures on linguistic collations.

### 10.2 Microsoft SQL Server (indexed views since 2000)

Introduced in **SQL Server 2000 Enterprise** and documented in
**Goldstein & Larson, "Optimizing Queries Using Materialized Views: A Practical,
Scalable Solution", SIGMOD 2001** — still the canonical filter-tree view-matching
paper. **2005** added partitioned-view matching, **2008** filtered indexed views,
**2014–2019** columnstore/in-memory OLTP tweaks, **2022** no material view-matching
changes (adds Synapse-Link externals). Azure Synapse Dedicated SQL Pools gained
incrementally-maintained MVs around 2019 via a different engine.

**MV capabilities.** A view with a unique clustered index, maintained synchronously
on every DML. Restrictions: must be `SCHEMABINDING`, two-part references, no `*`,
no outer joins, no `UNION`, `DISTINCT`, `TOP`, most subqueries, CTEs, table-valued
functions, `ROWSET` functions, or most nondeterministic functions. Aggregates:
`SUM`, `COUNT_BIG`, indirectly `AVG`; `MIN`/`MAX` only in non-indexed views.
Famously, **`COUNT(*)` is disallowed — you must use `COUNT_BIG(*)`** because
maintenance needs a BIGINT counter. Floating-point columns cannot appear in grouping
keys. Session options (ANSI_NULLS, QUOTED_IDENTIFIER, NUMERIC_ROUNDABORT) must match
between definition and query.

**Matcher.** Tries matching *for every query* whether or not the query mentions the
view (automatic matching). Goldstein-Larson filter tree, cost-based pick.
Compensates for extra columns in the MV (projection stripping), extra rows (residual
filter), rollup grouping, equi-join subsumption with lossless join-back.

**Blind spots.** No outer-join matching (outer joins disallowed in indexed views).
No non-equi-join matching. Semantically equivalent predicates written differently
often fail to match (Paul White's sqlperformance.com has extensive documentation of
these cases — `a+b` vs `b+a` in residuals, CAST defeating matching, etc.). Collation
mismatch on join columns doesn't trigger a COLLATE compensation. `DATETIME2`
precision differences defeat matching. Non-Enterprise editions don't automatically
use indexed-view indexes unless `WITH (NOEXPAND)` is named — a licensing-driven
footgun rather than a matcher limitation.

### 10.3 IBM DB2 (ASTs 1998 → MQTs 2002)

Automatic Summary Tables (ASTs) arrived in **DB2 UDB V5.2 (1998)**, renamed
**Materialized Query Tables (MQTs) in V8 (2002)**. Design and matcher:
**Zaharioudakis, Cochrane, Lapis, Pirahesh, Urata, "Answering Complex SQL Queries
Using Automatic Summary Tables", SIGMOD 2000** — still one of the most complete
treatments of SPJG view matching anywhere. DB2 for z/OS picked up MQTs in V8 (2004).
Staging tables for incremental `REFRESH DEFERRED` since V8.

**MV capabilities.** Distinguishes **system-maintained** (DB2 refreshes — subtypes
`REFRESH IMMEDIATE` synchronous with heavy restrictions, and `REFRESH DEFERRED` full
recompute on `REFRESH TABLE` or incremental via staging) from **user-maintained**
(application refreshes; DB2 only rewrites). Supports SPJG, outer joins (in
user-maintained or deferred), UNION ALL, CTEs, most scalar expressions. MQTs can be
partitioned/indexed/clustered/compressed. `CURRENT REFRESH AGE` and
`CURRENT MAINTAINED TABLE TYPES FOR OPTIMIZATION` control which MQTs the optimizer
considers — lets DBAs trust stale MQTs for analytics while excluding them from
transactional queries.

**Matcher.** Widely considered the most capable commercial SPJG matcher. Rewrites
through subqueries including correlated ones, not just flat SPJG. Outer-join-to-
inner conversion under provable NOT NULL + FK. Aggregate rollup across multiple
dimensions simultaneously. Residual IN-list subsumption. **Multiple MQTs in one
query** — different joins satisfied by different MQTs. Column-EC reasoning proves
redundant joins.

**Blind spots.** Window functions in definitions block matching (matcher predates
OLAP functions). LATERAL / TABLE function references not matched. GROUPING SETS /
CUBE / ROLLUP in the query partially matchable against plain MQTs via decomposition
but unreliable the other way. UDFs mark an MQT non-rewritable unless declared
DETERMINISTIC + NO EXTERNAL ACTION. DECIMAL precision/scale differences on grouping
keys defeat matching — DB2 requires exact type match there. Killer feature:
`EXPLAIN_MQT` reports *why* a candidate MQT was rejected. Oracle and SQL Server
have nothing equivalent.

### 10.4 Teradata (Join Index 1997 → AJI 2001)

Join Index in **V2R3 (1997)**, Aggregate Join Indexes in **V2R4 (2001)**, Sparse
Join Indexes in V2R5. No single reference paper; SQL Reference: Database Design
documents attempted rewrites. Synchronously maintained — no async MV in Teradata.
No outer joins in AJIs requiring cross-dimension maintenance (version-dependent);
no window, CTE, recursive SQL, limited subqueries. Global indexes and hash/row
partitioning compatible with AJIs; column partitioning compatible from TD 14.

**Matcher.** Aggregate rollup, range/IN-list subsumption, join-back via RI (may be
`NOT ENFORCED` — optimizer trust flag). Big practical weakness: **matching is sensitive
to the exact form of the query** — semantic equivalence not normalized into is not
matched. Rewriting the query slightly can cause AJI use to appear or disappear.
Documented in Orange Books. CHAR-vs-VARCHAR on join keys is especially problematic
(CHAR is space-padded, comparisons coerce, but matcher conservatively refuses).
`NOT CASESPECIFIC` collations defeat matching. *Speculative*: 17.x seems to have
improved nested-view rewrite but exact release notes unverified.

### 10.5 Snowflake (MVs 2019 → Dynamic Tables 2023)

Previewed MVs at 2018 summit, GA **2019**. **Dynamic Tables** announced 2022, GA
**2023** — a second, declarative-pipeline approach on top of the query engine. No
public matcher paper; SIGMOD 2016 Snowflake paper predates MV support.

**Classic MVs**: single base table only, filters + projections + aggregates, no joins,
no window functions, no UDFs, no non-deterministic functions, no HAVING, no ORDER BY,
no LIMIT. Async background maintenance. Queries use them even before fully caught up,
with automatic fallback to base for uncovered portions — this **"use MV for covered
rows, base for the delta" split** is distinctive; most systems rewrite wholesale or
not at all. **Dynamic Tables** lift all restrictions: joins, windows, multi-source,
chains. Refreshed on user-specified `TARGET_LAG`, incrementally when possible,
falling back to full recompute. But **Dynamic Tables are not automatically matched**
— queries must name them. Major caveat: Dynamic Tables are not transparent MVs in
the Oracle/SQL Server sense, they're managed derived datasets applications explicitly
query.

**Blind spots.** No join matching in classic MVs. Aggregate rollup between compatible
MVs not attempted. Type coercion on predicates defeats matching (e.g., `ts::DATE =
'2024-01-01'` vs `ts BETWEEN '...' AND '...'` isn't normalized). Pruning matching
well-engineered but undocumented.

### 10.6 Google BigQuery (MVs 2020)

GA **2020**, initially single-table aggregate-only. 2021–2023 expanded to inner
joins, some outer joins, non-aggregate projections. Automatic refresh via BigQuery's
append-mostly storage — naturally IVM-friendly.

**Allowed in definitions**: SELECT, WHERE, GROUP BY, HAVING, joins (support grew
over time), SUM/COUNT/AVG/MIN/MAX + `APPROX_COUNT_DISTINCT` (HLL). **Disallowed**:
window, LIMIT, ORDER BY, UNION, most subqueries, UDFs. Partitioned/clustered.

**Matcher.** Predicate subsumption, aggregate rollup, partition/cluster pruning on
top of MV.

**Known footnotes.** A **max-staleness cliff** governs consideration; default is
bounded (practical upper bound around 7–30 days depending on config; has changed
more than once — verify against current docs). DML BigQuery can't incrementally
reflect (complex UPDATEs) invalidates the MV, forcing full recompute or silent base
fallback. Matching across JOIN views initially unsupported, still more restrictive
than single-table.

### 10.7 Amazon Redshift (MVs 2019 → AutoMV 2022)

Preview 2019, GA 2020, **AutoMV in 2022** — workload-analysis-driven MV creation/
refresh/drop. Distinctive in the commercial space. Engineering in-house; no public
paper; re:Invent talks describe delta-log incremental maintenance.

**MVs**: SPJG with inner and some outer joins, set operations, a range of aggregates.
Refresh AUTO/MANUAL/scheduled. Specific docs list of "incremental-eligible"
constructs; outside that list, full recompute.

**Matcher.** Auto on by default. Predicate subsumption, aggregate rollup, join-back.

**Blind spots.** Outer joins in the MV severely restrict matching even when logically
compatible. Window functions in the MV disable matching. UDFs (Python/Lambda)
disable matching. VARCHAR/CHAR strict; implicit-cast queries often fail. Historical
incorrect-plan bugs around nested-subquery-plus-matching patched 2021–2023 —
practitioner guidance is to EXPLAIN-verify intended MV use.

### 10.8 Databricks (DLT 2022 → Enzyme ~2024/2026)

DLT launched 2022. Databricks MVs are DLT-managed by default. **Project Enzyme** is
the IVM engine; public discussion from 2022+; Yadav et al. *"Enzyme: Incremental
View Maintenance for Data Engineering"*, SIGMOD-Companion 2026 (arXiv 2603.27775).
Exact GA timing not confirmed.

**MV capabilities.** SQL or Python within a pipeline. Joins, aggregates, window
functions (with incremental caveats), UDFs, Delta time travel as a source.
Incremental maintenance requires append-only or CDF sources; mutable sources force
full recompute or `APPLY CHANGES INTO`.

**Matcher.** Databricks SQL engine (Photon + DBSQL) does *not* broadly auto-match ad-hoc
queries against MVs in the Oracle/SQL Server sense. Users query MVs by their
DLT-registered name. Some optimizer substitution exists but narrower than commercial
RDBMS matchers. Enzyme focuses on *maintaining* MVs, not rewriting.

**Blind spots.** Auto-matching is the weak point vs Oracle/SQL Server — a design
choice (users know which table they're querying) but a real transparent-MV gap.
Incremental refresh has narrow construct support relative to full. Delta vacuum +
time-travel retention affect IVM delta availability.

### 10.9 Firebolt, ClickHouse, SingleStore, Vertica (briefer)

**Firebolt** launched 2020 with aggregating indexes as first-class; later added join
indexes. Syncly maintained on ingest. Automatic-rewrite matcher; narrow construct
set (no window in indexes); type-strict matching; specific aggregate allowlist.

**ClickHouse** has had MVs-as-INSERT-triggers since ~2016 — an unusual design where
the MV is a table + trigger that runs on each source INSERT batch. UPDATEs/DELETEs
on the source are **not reflected** — classic newcomer footgun (every ClickHouse
Stack Overflow MV thread mentions this). **Projections** (v21.6, 2021) are properly
matched: per-part pre-computed aggregations automatically chosen. Projections span
a single anchor table only. No cross-MV rewrite, no join-back.

**SingleStore IMVs** added ~2022–2023 (version unverified). SPJG with some
restrictions; log-based delta. Auto-rewrite for a subset — scope uncertain, verify
vendor docs.

**Vertica projections** descend from the C-Store paper (Stonebraker et al., VLDB
2005). Projections are *storage*, not MVs — every table is physically one or more
sorted/compressed projections. **Live Aggregate Projections** and **Top-K
Projections** (mid-2010s) are MV-like: pre-aggregated, syncly maintained. Optimizer
chooses among projections for every query — mature within a narrow (single-anchor)
scope. Cross-table matching doesn't exist — it's storage layout, not rewrite engine.

### 10.10 Apache Calcite, Hive, StarRocks/Doris (briefer)

**Calcite** MV matching: initial `MaterializedViewRule` by Julian Hyde, 2015,
projection/filter only. Substantial rework by **Jesús Camacho-Rodríguez** (CALCITE-1731,
2018+): full Goldstein-Larson + aggregate rollup + lattice. His Paris-Saclay thesis
(2019) *Efficient Techniques for Evaluation of Queries in Big Data Systems* documents
it. Conservative — prefers false negatives to unsound rewrites. Outer-join handling
limited; window functions in MVs not matched. Coverage depends on which subrules
the embedder registers.

**Hive 3.0 (2018)** added Calcite-based MVs. Incremental rebuild requires
transactional (ACID) sources; non-ACID forces full rebuild. Camacho-Rodríguez et al.
SIGMOD 2019 paper documents the design. Partition pruning can intervene and
occasionally defeat an otherwise-valid MV match.

**StarRocks** (2020 fork of Doris) and **Apache Doris** (originally Palo, Baidu):
async MVs around v1.2 (2022–2023). C++ reimplementations of Calcite-style rewrite
plus MPP-specific optimizations. StarRocks markets a strong auto-rewrite story; Doris
catching up. Both support nested-MV matching (MV over MV) but navigate chains
shallowly. Both suffer from "type coercion defeats matching" and "semantically
equivalent but syntactically different predicates don't match" — characteristic
of younger matchers.

### 10.11 Cross-cutting observations

1. **Matcher age correlates with SQL-feature blindness.** Oracle predates windows and
   handles them poorly; DB2's designers prioritized nested subqueries. SQL Server's
   2001 matcher forbids outer joins in indexed views entirely rather than try to
   match them.
2. **Type coercion is universally the silent killer** — biggest source of "my MV
   isn't being used" reports, under-documented everywhere.
3. **Sync-maintenance systems restrict MV definitions heavily; async systems restrict
   matching heavily** (SQL Server indexed views vs BigQuery/Snowflake classic MVs).
4. **`EXPLAIN`-style rewrite introspection varies wildly.** DB2 `EXPLAIN_MQT`,
   Oracle `DBMS_MVIEW.EXPLAIN_MVIEW`, Calcite `RelOptMaterialization` tracing —
   all genuinely useful. Snowflake/BigQuery/Redshift are opaque.
5. **Older mature matchers handle aggregate rollup and FK-based join-back; newer
   cloud matchers often don't, or do so only for narrow patterns.** Counter to
   "cloud is always better" narratives.

---

## 11. Open Table Formats and MV / Matching

Open table formats (OTFs) sit between object storage and query engines, exposing
manifests, snapshot IDs, and change streams. None were originally designed with MVs
as a first-class concept, but their substrate — immutable snapshots, monotonic
commit logs, row-level change feeds — is exactly what IVM needs. The pattern is
*engines bolting MV semantics onto format primitives*.

### 11.1 Apache Iceberg
(Ryan Blue et al., Netflix, 2017; now ASF.) Snapshot IDs are the key IVM artifact:
every MV records the source-table snapshot it was built from; freshness is a cheap
snapshot-ID compare. **Iceberg View Spec v1 (2023)** + **MV spec draft (late 2024)**
standardize the *"storage-table + view-definition"* pattern: MV = view with a
storage table that remembers which source snapshots produced it. Engine-side MVs:
Dremio Reflections; **Trino** `CREATE MATERIALIZED VIEW` on Iceberg (since ~2022);
Starburst Galaxy "smart indexing"; Snowflake Iceberg-backed + Dynamic Tables (2024);
Spark SQL has no native MVs. Tabular (Ryan Blue's company, acquired by Databricks
2024) was pushing Iceberg MV semantics — now folded into Databricks Unity.

No peer-reviewed paper specifically on an Iceberg MV matcher. Closest: Camacho-
Rodríguez et al. SIGMOD 2019 on Hive ACID MVs — the snapshot-ID pattern Iceberg
inherited.

### 11.2 Delta Lake
(**Armbrust et al., "Delta Lake: High-Performance ACID Table Storage over Cloud
Object Stores", VLDB 2020.**) Transaction log `_delta_log` plays the snapshot role.
**DLT** is a pipeline abstraction, not a matcher. **Databricks MVs** GA 2024 use
Enzyme. Delta-native (OSS): change data feed, transaction log, deletion vectors.
Databricks-specific: MVs, Enzyme, DLT, auto-compaction. Microsoft Fabric also ships
Delta MVs via Synapse/Polaris rewriter.

### 11.3 Apache Hudi
(Uber, Vinoth Chandar et al., 2017 — predates Iceberg/Delta.) **Incremental queries**
are the substrate: `WHERE _hoodie_commit_time > 't'` returns row-level changes since
a commit — closer to Debezium than snapshot-diff. No native MV, but Chandar's
Onehouse.ai blog posts (2022–2024) describe "Hudi as an IVM engine" patterns.
Time-travel by commit timestamp (not snapshot ID) — semantically equivalent but
implementation surface differs from Iceberg/Delta.

### 11.4 Apache Paimon (ex Flink Table Store)
(ASF, graduated 2024; Alibaba/Flink origins.) Most IVM-oriented OTF. LSM
primary-key tables with changelog streams feeding Flink. **Partial-update and
aggregation merge engines are stateful MVs stored inside the format.** No ad-hoc
matcher — users declare partial-update tables as pipeline outputs. Novelty: MV and
base live in the same format with uniform changelog semantics.

### 11.5 LakeFS
(Treeverse, 2020+.) Git-style branching/merging over object storage; format-agnostic.
No MV matching. Relevant for MV *versioning* — an MV built from `main@commit123` is
cleanly reproducible. Open research lane; no published work exploits this.

### 11.6 DuckLake
(DuckDB Labs, 2024+ — Hannes Mühleisen, Mark Raasveldt et al.) Catalog + metadata
in a SQL database (DuckDB/Postgres/any SQL); data as Parquet on object storage.
Exposes: SQL-queryable snapshot history, cheap time-travel (snapshot IDs are plain
integers), transactional catalog (ACID metadata), per-snapshot change feeds via
catalog queries. OpenIVM's N-term telescoping already uses this; a matcher would
piggyback on the same mechanism.

### 11.7 Apache XTable / OneTable
(Originally OneTable, Onehouse; ASF incubation 2024.) Interop layer translating
between Iceberg/Delta/Hudi metadata. No MV concept. Relevant: a matcher at the
XTable abstraction level would be format-portable. No published work — research
opportunity.

### 11.8 Freshness-aware / temporal matching — what exists
Hive's snapshot-ID freshness check (Camacho-Rodríguez) is the main published
mechanism: rewrite is valid iff MV snapshot covers all committed changes visible to
the query's read timestamp. Interval-style matching (MV valid over `[t1,t2]`, query
asks `[t3,t4]`):
- **Zhou, Larson, Goldstein, "Dynamic Materialized Views", ICDE 2007** — control
  plane for MV validity intervals.
- **Agrawal, Chaudhuri, Narasayya, "Automated Selection of Materialized Views and
  Indexes in SQL Databases", VLDB 2000** — workload-driven selection (not temporal).
- "Partial temporal coverage" matching (MV covers part of query's interval, residual
  from base for the rest) is **open in production systems**; academic proposals
  exist but haven't shipped.

### 11.9 Metadata-as-MV pushdown
Iceberg/Delta manifests contain column min/max stats. A query with `WHERE x > 100`
can often be answered as "count = 0" or "rows = N" directly from manifest stats
without scanning data — **matching a query against a manifest as if it were a
coarse aggregate MV**.
- Iceberg puffin files + column stats (spec 2023) formalize this.
- "Sundial" (Durner et al., CIDR 2024) — treats manifest metadata as first-class
  index. *Citation partly reconstructed; verify.*
- "Amoeba" / "Qd-tree" (Yang et al., SIGMOD 2020) — learned data layout relevant
  for matching predicates to physical partitioning.

Underexplored lane: every Iceberg manifest as a materialized summary; matching
against summary + residual data scan.

---

## 12. IVM Frameworks with MV-Graph Concepts

### 12.1 Materialize — differential dataflow
(McSherry/Narayan/Benesch, ~2019; **McSherry et al., "Differential Dataflow", CIDR
2013**; Naiad, SOSP 2013.) MVs are first-class dataflows maintained incrementally.
**Shared arrangements**: multiple MVs sharing an operator-prefix share arranged
state — reuse at *dataflow-construction time*, not query-rewrite time. No ad-hoc
query → MV rewrite; the user names the MV. Jamie Brandon's blog (scattered-thoughts.net)
explains this design choice explicitly.

### 12.2 Feldera / DBSP
(**Budiu, Chajed, McSherry, Ryzhyk, Tannen, "DBSP: Automatic Incremental View
Maintenance for Rich Query Languages", VLDB 2023.**) Typed Z-set streams, operator
circuit. Circuit is static; queries compile into nodes. No subcircuit-reuse matcher
in the published work, but the compositional structure is natural substrate for
future matching — subcircuit reuse is the analogue of subplan matching. Underpins
OpenIVM's Z-set/multiplicity reasoning (see arXiv 2404.16486).

### 12.3 RisingWave
(Yingjun Wu et al., OSS since 2021; **"RisingWave: Real-Time Event Streaming with
Battle-tested SQL Semantics", SIGMOD 2023.**) MVs named; ad-hoc queries reference
them explicitly. No auto-matcher. Innovation is in storage (Hummock) and cost-based
state-materialization decisions.

### 12.4 Proton / Timeplus, ksqlDB, Flink SQL / Paimon
Proton — ClickHouse-style streaming SQL, no matcher. ksqlDB — `CREATE TABLE AS
SELECT` → RocksDB state store; named MVs; no matcher. Flink SQL — dynamic tables
(Hueske, 2017 Flink blog); Paimon storage layer; no matcher.

### 12.5 Noria — partial materialization, a different paradigm
(**Gjengset et al., "Noria: Dynamic, Partially-Stateful Data-flow for High-Performance
Web Applications", OSDI 2018**; Gjengset PhD, MIT 2021.) Different from classical
matching:
- MV stores only rows queried recently.
- Cache miss → **upquery** up the dataflow to populate that slice on demand.
- No "can MV answer query?" check. MV is a cache; miss triggers recomputation.

Classical matching is *eager*; Noria is *lazy*. Hybrid matchers that choose between
upquery-style lazy and rewrite-based eager matching depending on query shape are an
open research opportunity.

### 12.6 dbt
SQL-model DAG executor with no matcher and no IVM. Workload pattern — thousands of
SQL models in a DAG — is exactly what pipeline-DAG-aware matching would help.
`manifest.json` provides the DAG for free. `incremental` materialization is a
hand-rolled user micro-batch IVM via `is_incremental()` predicates.

### 12.7 LinkedIn Coral
(github.com/linkedin/coral.) SQL IR + translation across engines (Hive/Spark/Presto).
No MV rewrite itself, but the natural place for a cross-engine matcher to live —
portable IR. No published paper.

### 12.8 Apache Gluten
(github.com/apache/incubator-gluten — Intel/ASF.) Offloads Spark SQL to
Velox/ClickHouse. Inherits Spark MVs if any; no matcher added.

---

## 13. Matching Algorithm Mechanics — Implementation Reference

This is a design reference for implementing a matcher. Each subsection below maps
directly to a component you'd build.

### 13.1 Match classification taxonomy
Match is a *relation* between Q and V, not a yes/no. Canonical breakdown:

- **Exact match** — Q and V semantically identical. Implementation: canonicalize
  plan, hash, look up.
- **Equivalent rewriting (Goldstein-Larson, SIGMOD 2001)** — `∃ C s.t. C(V) ≡ Q`.
  Compensable: residual selection, residual projection, residual re-aggregation
  when Q groups coarser than V, residual lossless FK-PK join. Not compensable:
  Q uses a column V doesn't expose; Q groups *finer* than V; aggregate doesn't
  decompose (MEDIAN, STDDEV without sufficient statistics).
- **Subsumption / contained rewriting (Halevy VLDBJ'01)** — V contains more than Q
  needs. Under bag semantics, equivalent ⊆ contained. Contained rewriting is for
  data-integration LAV (Information Manifold, MiniCon); **for OLAP/IVM you only
  want equivalent rewriting.**
- **Partial match** — V covers a subset of Q's rows, remainder from base UNION ALL'd.
  Rare in production. Worth it when V is large, stale-but-useful for the hot
  partition, cold partition is small. Usually implemented as partition-aligned MVs
  — easier and sound.
- **Multi-view match** — combine 2+ MVs. Oracle and DB2 document it; algorithmic
  core is *view-joining*: treat available MVs as virtual tables, run a cost-based
  join enumerator. Exponential blow-up; aggressive pruning essential. Calcite
  handles single-view; multi-view emerges from rule reapplication.
- **Pipeline / DAG match** — walk the MV DAG instead of treating MVs independently.
  Q matching V1's shape can also hit V2's shape if compensation ties back to V1.
- **Semantic / approximate** — V answers Q with bounded error (sampling, sketches).
  Orthogonal; handled by AQP engines (BlinkDB, VerdictDB).

### 13.2 Normalization pipeline
Power is bounded by the normalizer. Canonical forms: **QGM** (IBM Starburst/DB2;
Pirahesh/Hellerstein/Hasan SIGMOD 1992), **Calcite RelNode** (with digest),
**Spark Catalyst LogicalPlan** (with `canonicalized` tree).

Standard normalizations to apply before hashing/matching:
- Predicate CNF (DB2) or DNF (some matchers); CNF plays better with equivalence
  classes.
- Constant folding — careful: can eliminate predicate shapes the matcher needs.
- Equijoin → equivalence class: `a=b AND b=c AND a>5` → EC{a,b,c} with `>5` on the
  representative.
- Join commutativity + associativity reordering (smallest-leaf-id-first). **Outer
  joins do not freely commute** — Galindo-Legaria & Rosenthal, "Outerjoin
  Simplification and Reordering", TODS 1997 — reorder only under null-rejection
  conditions.
- GROUP BY key ordering.
- Projection list sorted for canonical hash (but original order preserved for
  output).
- Predicate pushdown.
- Common subexpression extraction.
- `NVL(x,y)` → `COALESCE(x,y)`; `CASE WHEN x IS NULL THEN y ELSE x END` → `COALESCE`.
- CASE normalization: fold constant branches; **do not sort when-clauses** — CASE
  is ordered.
- NULL-safe equality: `x IS NOT DISTINCT FROM y` ≠ `x = y`.
- Type coercion: make implicit coercions explicit CAST nodes. Pitfall: reordering
  must preserve coercion semantics — `x::INT = 1.5` ≠ `x = 1.5::INT`.

Pitfalls: over-normalization destroys information (`x>5 AND x>3` folded to `x>5`
loses the ability to match a view on `x>3`). Outer-join reordering is a minefield.
Float constant-folding unsafe under strict FP. NULL in equivalence classes: `a=b`
doesn't put NULLs in the same class — track with a separate null-rejecting flag.

### 13.3 Goldstein-Larson in depth

**Filter tree** (§3.1 of G-L paper). Multi-level index over the MV catalog. Level
order, most selective first:
1. Source-table set (bitmap of base tables).
2. Join shape (canonical string of join graph).
3. Grouping columns (sorted key set).
4. Output columns (sorted key set).
5. Equivalence classes.
6. Range predicates per EC.

Built offline when MVs register, rebuilt incrementally on add/drop. Lookup O(depth)
with small constants; at the leaf, run full compensation check against candidates.

**Equivalence class construction.** For query Q:
- Initialize each column to its own EC.
- For each equality `a=b`, union ECs of `a` and `b`.
- For each `a=const`, attach the constant to `a`'s EC.

Do the same for V. Q's column is "EC-compatible with V" iff V's EC contains Q's
EC members, and constants are consistent.

**Range predicates.** Per EC, the conjunctive range `[lo, hi]` with open/closed
endpoints. For equivalence rewrite: **V's range must contain Q's range** — V is
permissive, Q restrictive. Residual = Q_range minus V_range, applied as filter on V.
Interval arithmetic; careful with types (DATE vs TIMESTAMP, DECIMAL precision,
string collation).

**Column computability.** Q's output column `c` is computable from V iff there's
an expression tree over V's outputs + constants + deterministic functions yielding
`c`.

Aggregate decomposition rules:
- `SUM(x)` from `SUM(x)` — re-aggregate `SUM(SUM(x))` when Q coarser.
- `COUNT(*)` from `SUM(COUNT(*))`.
- `COUNT(x)` from `SUM(COUNT(x))` (V's COUNT excludes NULLs).
- `AVG(x)` from `SUM(SUM(x))/SUM(COUNT(x))` — requires V expose both. OpenIVM
  already rewrites AVG→SUM/COUNT at parse time.
- `MIN`/`MAX` from `MIN(MIN)`/`MAX(MAX)`.
- **STDDEV, VAR, CORR, COVAR, REGR_*** — not decomposable from just SUM/COUNT.
  Need sufficient-statistics view (SUM, SUM_SQ, COUNT, SUM_XY). OpenIVM commit
  `18643f8` already flags these as LPTS-unroundtrippable — treat as non-compensable.
- `DISTINCT` aggregates — not decomposable under coarsening.
- `MEDIAN`, `PERCENTILE` — not decomposable (without sketches).

**Grouping derivability.** Q's GROUP BY derivable from V's iff every Q key is in
V's key set (subset) OR every Q key is functionally determined by V's keys via
declared FDs/PK-FK. Derivable + coarser → re-aggregate. Finer → not compensable.

**Output of match** = rewrite plan:
```
{ view: <id>,
  residual_filters: [...],
  residual_projection: [...],
  reaggregation: {group_by, aggs} | None,
  residual_join: [...] | [] }
```
Then cost-estimate against base plan.

### 13.4 Calcite's two algorithms
`org.apache.calcite.plan.SubstitutionVisitor` and
`org.apache.calcite.rel.rules.materialize.MaterializedViewRule`.

**SubstitutionVisitor** = plan-tree unification. Walk in lockstep, match node types.
Fast; brittle under reordering. Calcite mitigates by applying join-commute rules
first.

**MaterializedViewRule** (and subclasses `MaterializedViewProjectFilterRule`,
`MaterializedViewOnlyFilterRule`, `MaterializedViewProjectAggregateRule`,
`MaterializedViewOnlyAggregateRule`, `MaterializedViewOnlyJoinRule`) implements G-L:
(1) compute Q's ECs + ranges + column-origin map, (2) compute V's, (3) check
source-table containment, (4) EC compatibility, (5) compensation predicates,
(6) column computability, (7) aggregate grouping derivability + decomposability,
(8) build rewrite RelNode. *Class names approximate to current Calcite; verify.*

**Calcite Lattice** (`org.apache.calcite.materialize.Lattice`) — pre-declared
star/snowflake schema: fact + dimensions + measures. MVs registered against a
lattice are indexed by grouping set; matcher picks the MV whose grouping set is a
*refinement* of the query's, enabling re-aggregation. Specialized DAG matcher for
OLAP cubes.

Limitations: SubstitutionVisitor is shape-fragile. MaterializedViewRule expensive
on large plans; EC computation + compensation can blow up. Many-MVs performance
issues; prefiltering helps. Neither handles windows well. Single-view; multi-view
emerges from rule re-application, not joint enumeration.

### 13.5 Functional dependencies and join elimination

**DB2 RELY** — `FOREIGN KEY ... NOT ENFORCED RELY`. Optimizer trusts without
enforcing; responsibility on user. Widely used in DW — enforced FKs too expensive.

**Oracle dimensions** — `CREATE DIMENSION ... LEVEL ... HIERARCHY ... ATTRIBUTE`.
Hierarchical FDs + attribute-of relationships. Richer than RELY because explicitly
OLAP-shaped.

**Lossless-join detection.** Join `R ⋈ S ON R.fk=S.pk` lossless for R iff:
- `S.pk` is PK (unique not-null),
- `R.fk` NOT NULL (or inner join filters nulls),
- FK enforced or RELY'd.

Under these, join neither adds nor removes R-side rows — bag semantics preserved.
**Dropping tables during matching**: if Q joins T1⋈T2 PK-FK-losslessly and only
reads T1 columns, and V is defined without T2, V can still match. Pairs with MV
matching: normalize Q and V by dropping PK-FK-lossless tables before comparison.
OpenIVM's `ivm_fk_pruning` uses the same idea on the delta side; the matcher would
use it on the original-query side.

### 13.6 Catalog indexing approaches

| Approach | Build cost | Lookup cost | Recall |
|---|---|---|---|
| Linear scan | 0 | O(N·C) | 100% |
| Signature exact | O(N) | O(1) | low (exact only) |
| Multi-level digest | O(N·L) | O(L) | medium |
| Filter tree | O(N·D) | O(D + leaf) | high |
| Filter tree + per-leaf EC index | O(N·D) | O(D + log leaf) | high |

`C` = compensation check cost, `N` = MV count, `D` = tree depth, `L` = digest
levels. For OpenIVM at current scale, linear is fine; target filter-tree keyed on
(source-table bitmap, group-by keys, aggregate signature) when MV count grows.
DB2 uses QGM signatures. Calcite's `MaterializationService` keeps a list — known
scalability gap.

### 13.7 Pipeline / DAG matching

**Representation.** Nodes = base tables ∪ MVs. Edges = reads-from.
Per MV: source nodes, definition plan, freshness timestamp, row count, cost
estimate. Per edge: reads-from + the subplan expressing the dependency.

In OpenIVM: built by parsing each MV's definition, resolving each table reference
against `_duckdb_ivm_views`. If it resolves to another MV → MV-MV edge; otherwise
base-table edge. Store in a new `_duckdb_ivm_mv_edges`. On CREATE/DROP/REPLACE,
maintain edges + invalidate matcher caches. `ivm_cascade_refresh` already walks
dependencies at refresh time; the matcher graph would share structure.

**Walking strategies.**
- **Bottom-up** (from base): topological order, try each MV, keep all winners,
  cost-compare. Best when MVs are intermediate building blocks / shared arrangements.
- **Top-down** (from MVs closest to Q): match Q against the MV most resembling it;
  if no match, try its children (MVs it depends on). Best when MVs are
  pre-aggregated.
- **Branch-and-bound**: lower-bound the cost of any rewrite rooted at a given node;
  prune when bound exceeds current best.

**When to stop.** Current MV incompatible (EC, missing columns, incompatible
grouping), children's combined cost exceeds budget, or good-enough match found and
descent unlikely to help. *No paper with a formal stopping criterion for MV-DAG
walking that I know of.*

**Literature closest to a DAG matcher.**
- **Jindal et al., "Computation Reuse in Analytics Job Service at Microsoft", SIGMOD
  2018** — SCOPE subexpression matching across jobs. Not strictly MV-DAG but the
  primitives (canonicalization, signature indexing, overlap detection) carry over.
- **Jindal et al., "CloudViews", PVLDB 11(12) 2018** — MV recommendation + reuse;
  scores candidate MVs by workload utility; offline not online.
- **Jindal et al., "Peregrine", SIGMOD 2019** — workload optimization with shared
  MVs including online matching. Closest to what you want.
- **Halevy survey 2001** — multi-view addressed mostly in LAV (MiniCon/bucket).
- **Koch, "Incremental Query Evaluation in a Ring of Databases", PODS 2010** —
  compositional view reasoning; the theory that lets you do DAG-aware IVM.

**A true joint online DAG matcher is, AFAIK, an open area.** All the practical
systems (DB2, Oracle, Calcite) do single-MV matching and let rule reapplication
compose.

### 13.8 IVM-aware match selection (the core novelty)

Once rewrites R1..Rk are produced, you choose. Variables classical matchers don't
track:
- **View staleness**: `delta_size(V) / row_count(V)`.
- **Rewriting cost**: compensated plan on V's stale data + residual ops.
- **Refresh cost**: `PRAGMA ivm(V)` — OpenIVM's `openivm_cost_model.cpp` already
  estimates this.
- **Base-table scan cost**: Q with no MV.
- **Fresh-required vs stale-tolerable**: if Q requires freshness, must refresh first.

Per candidate:
```
cost(Ri) = [refresh_cost(Vi) if fresh_required else 0]
         + scan_cost(Vi)
         + compensation_cost(Ri)
```
Compare against `cost(base_plan)`. Min wins.

Extending OpenIVM's cost model: (1) plug candidate rewrites into DuckDB optimizer,
get estimate per rewrite; (2) freshness tracking per MV (timestamp, delta row count
— OpenIVM already has delta tables, add a lightweight estimator); (3) freshness
threshold policy.

**Enzyme's cost model** decides per MV per refresh between incremental and full.
Whether it also handles per-query MV *selection* is unclear from public material.
The two are independent; **doing both jointly is the design point for OpenIVM.**

### 13.9 Formal complexity and decidability

Conjunctive-query answering using views: decidable; bucket algorithm (Levy,
Rajaraman, Ordille PODS 1995); MiniCon (Pottinger & Halevy VLDBJ 2001).

Richer languages: Nash/Segoufin/Vianu TODS 2010 — determinacy can coincide with or
diverge from rewritability; gap depends on language. CQs with arithmetic
comparisons: undecidable in general.

NP-hardness: CQ containment NP-complete (Chandra & Merlin STOC 1977). Rewriting
using views NP-hard for CQs (Levy et al. 1995). Bucket/MiniCon exponential worst-case,
efficient in practice.

**Chase termination** (for chase-and-back-chase approaches): not guaranteed for
arbitrary dependencies. Sufficient conditions: weak acyclicity (Fagin/Kolaitis/
Miller/Popa TCS 2005), stratification (Deutsch/Nash/Remmel PODS 2008). Practical
takeaway: chase is overkill for equivalence rewriting. **Stay with Goldstein-Larson
unless you specifically need the chase** — it works in a decidable, polynomial-time
fragment.

---

## 14. Theorized-but-Unimplemented Directions (potential novelty lanes)

Academic proposals that have not productionized — each is a candidate novelty
direction.

- **Chase-based matching** (Deutsch/Popa/Tannen VLDB 1999; Deutsch/Tannen ICDT 2003).
  Uses the chase to reason about containment under constraints, subsuming MV matching
  with integrity constraints. Never productionized — too heavy for optimizer time
  budgets. A bounded-chase restricted to FK-PK dependencies would be tractable; no
  system has shipped it.
- **Determinacy-based view selection** (Nash/Segoufin/Vianu PODS 2007/TODS 2010).
  Asks: given V and Q, is Q determined by V? Stronger than matching — a complete
  rewriting criterion. Undecidable in general; tractable only in restricted
  fragments. **Never shipped.**
- **Partial-view rewriting with residual base scan**. Calcite has a limited version
  (predicate-overlap); general residuals unhandled. Room for systems work.
- **Multi-MV cooperative rewriting** (Zaharioudakis et al. SIGMOD 2000). IBM DB2
  AST / Oracle MVs do pieces; modern OSS matchers (Calcite) handle only single-MV
  robustly. **OSS gap.**
- **Cross-level (pipeline-DAG) rewriting**. CloudViews/Peregrine shipped at
  Microsoft Cosmos; no OSS equivalent. Known gap.
- **Workload-driven MV selection with online rewriting loop** — Peregrine/CloudViews
  style. Exists at Microsoft scale; nothing comparable in OSS.
- **Approximate view matching with error bounds**. BlinkDB (EuroSys 2013), VerdictDB
  (Park et al., SIGMOD 2018). Integration with classical exact matching is weak.
- **Learned matchers (GNN over plan embeddings)**. Neo (VLDB 2019), Bao (SIGMOD 2021)
  adjacent. No production system for MV selection learning specifically. Workshop
  papers exist; no canonical citation.
- **Temporal / interval matching with residual**. Zhou/Larson/Goldstein ICDE 2007
  is the main published work. Hive does all-or-nothing snapshot-ID match;
  interval-with-residual is absent in OSS. Direct research opportunity given
  DuckLake's snapshot substrate.

---

## 15. Pipeline DAG Matching — Concrete Use Cases

Where matching against an intermediate DAG node (not just the leaf MV) is
genuinely valuable.

### 15.1 Medallion architecture (bronze/silver/gold)
Databricks canonical pattern.
- **Bronze**: raw ingest (append-only Delta).
- **Silver**: cleaned, conformed, joined with dimensions.
- **Gold**: business-level aggregates.

Analyst query: `SELECT SUM(revenue) FROM gold_sales WHERE region='EU' AND date>'2026-01-01'`.
- **Flat matcher**: matches gold. Done.
- **Variant**: `... WHERE region='EU' AND product_category='industrial' AND date>'2026-01-01'`.
  `product_category` was rolled away in gold but exists in silver.
- **Pipeline matcher**: recognizes silver covers it and silver is cheaper than bronze.
  Rewrites against silver.

Gold is too rolled up, bronze too raw — a DAG-aware matcher finds the right level
automatically. **This is the main use case.**

### 15.2 dbt model DAGs
200–2000 models; marts at top, staging at bottom. Ad-hoc query from a BI tool
naturally targets a mart. Variant with one extra column might be answerable from a
layer below. dbt's `manifest.json` exposes the DAG statically — matcher could
traverse it, find the highest-level model covering the query.

No existing tool does this automatically. Semantic-layer products approximate via
cube abstractions: **Cube.js pre-aggregations** (github.com/cube-js/cube) — user
declares pre-aggregations at multiple granularities, Cube picks smallest covering
the query. Closest production example of pipeline-aware matching in the modern
stack.

### 15.3 OLAP cube rollup levels
Classical: daily → weekly → monthly → yearly. "Monthly query" → monthly MV. "2 weeks"
→ weekly or roll up daily. References: Harinarayan/Rajaraman/Ullman SIGMOD 1996
(cube selection); **Apache Kylin** (pre-computed cuboids, cuboid matcher); Mondrian
(Pentaho). Doris/StarRocks/ClickHouse projections all do this narrowly. Oldest,
most-proven case for pipeline-aware matching.

### 15.4 ML feature stores
Feast, Tecton, Hopsworks. Offline + online features share a derivation DAG. Training
queries over a time window could pick the right layer (raw event /
aggregated-5min / aggregated-daily). **Tecton feature views** implicitly do a form
of matching — declared transformations, engine decides serve-from-prematerialized vs
compute-on-the-fly.

### 15.5 Regulatory reporting (banking/insurance)
One base transaction table, dozens of reports (Basel III, Solvency II, AML). Many
share predicates ("USD-denominated", "above threshold X"). Pipeline-DAG matcher finds
most-specific shared intermediate and reuses it across reports. Industry pattern;
no canonical published reference.

### 15.6 Observability (traces/metrics/logs)
Prometheus recording rules, Tempo traces, Loki logs. Metrics aggregated at multiple
granularities (1s/1m/5m/1h). Alerts target the coarsest level satisfying SLO; ad-hoc
investigation might need finer. **Prometheus + Thanos downsampling** is two levels;
queries manually directed. Matcher would automate. **M3 (Uber, VLDB 2019)** —
multi-resolution TS store, primitive matcher.

### 15.7 Freshness SLAs that vary per layer
Silver 5-min stale, gold 1-hour stale. Query with "need within 10 min" — reject
gold, use silver. **OpenIVM/DuckLake snapshot-ID substrate shines here**: every
layer's MV records source snapshot ID, freshness is a cheap compare.

### 15.8 The killer combined example
Base `orders` (10 TB). Silver `orders_enriched = orders ⋈ customers ⋈ products`
(12 TB). Gold `daily_revenue = SUM(price*qty) GROUP BY date, region` (100 MB).

Query C: `SELECT date, region, SUM(price*qty) FROM ... WHERE date > NOW() - INTERVAL 10 MIN GROUP BY date, region`.

- Gold stale 1 hour → flat matcher either rejects (strict freshness) or returns
  stale data.
- Pipeline matcher: silver is 5 min stale (OK) — match against silver + aggregate
  on the fly. **Or**: use gold for old part + delta-scan for the last-10-min residual.

This last pattern — **gold + residual = fresh answer** — combines pipeline-DAG
awareness with temporal-interval matching with residual base scan. **No OSS system
does this today.** Natural endgame for OpenIVM over DuckLake given the snapshot
substrate already exposed.

---

## 16. Key URLs

- Calcite materialized views: https://calcite.apache.org/docs/materialized_views.html
- Calcite source: `org.apache.calcite.rel.rules.materialize.*`
- Oracle DW Guide: https://docs.oracle.com/en/database/oracle/oracle-database/23/dwhsg/
- SQL Server indexed views: https://learn.microsoft.com/en-us/sql/relational-databases/views/create-indexed-views
- StarRocks async MV: https://docs.starrocks.io/docs/using_starrocks/async_mv/
- BigQuery MVs: https://cloud.google.com/bigquery/docs/materialized-views-intro
- Snowflake MVs: https://docs.snowflake.com/en/user-guide/views-materialized
- Enzyme (Databricks SIGMOD 2026): https://arxiv.org/pdf/2603.27775v1

---

## 17. Fuzziness flags

- Specific MotherDuck / Databricks matcher internals beyond what's in Enzyme:
  not deeply documented publicly.
- LLM-based view matching (2024-2026): emerging, citations uncertain.
- Exact current scope of Snowflake / BigQuery matchers may have widened since 2024
  docs — verify before quoting.
