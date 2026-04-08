---
name: dbt-coral-reference
description: Reference for dbt (data build tool) and LinkedIn Coral — SQL transformation frameworks relevant to IVM. Auto-loaded when discussing dbt models, incremental models, Coral SQL translation, cross-engine SQL compilation, or data transformation pipelines.
---

# dbt and LinkedIn Coral — Reference for IVM Context

## 1. dbt (data build tool)

### 1.1 Overview

dbt is a SQL-first transformation framework for analytics engineering. Users define
transformations as SELECT statements ("models"), and dbt handles materialization,
dependency ordering, testing, and documentation. It runs inside the data warehouse
— no separate compute engine.

**Core philosophy:** SQL is the interface. Transformations are version-controlled
SELECT statements. The DAG is inferred from `{{ ref('model_name') }}` references.

### 1.2 Architecture

```
models/               -- SQL files (one SELECT per file)
  staging/
    stg_orders.sql    -- {{ config(materialized='view') }}
    stg_customers.sql
  marts/
    fct_revenue.sql   -- {{ config(materialized='incremental') }}
    dim_products.sql  -- {{ config(materialized='table') }}
dbt_project.yml       -- project config
```

**Execution:** `dbt run` compiles Jinja+SQL templates → executes DDL/DML on the
warehouse. The DAG is topologically sorted; models run in dependency order.

**Adapters:** dbt supports multiple backends via adapters — Snowflake, BigQuery,
Redshift, DuckDB, PostgreSQL, Databricks, etc. Each adapter translates dbt's
abstract operations into engine-specific SQL.

### 1.3 Materialization Strategies

| Strategy | DDL Generated | When to Use |
|---|---|---|
| `view` | `CREATE VIEW AS SELECT ...` | Lightweight, always fresh |
| `table` | `CREATE TABLE AS SELECT ...` | Full rebuild every run |
| `incremental` | `INSERT/MERGE INTO ... SELECT ... WHERE ...` | Append/upsert new rows |
| `ephemeral` | CTE (inlined, no DDL) | Reusable subquery, no table |

### 1.4 Incremental Models

dbt's incremental materialization is the closest analogue to IVM in the dbt world,
but operates very differently:

```sql
-- models/fct_revenue.sql
{{ config(
    materialized='incremental',
    unique_key='order_id',
    incremental_strategy='merge'
) }}

SELECT
    order_id,
    customer_id,
    SUM(amount) as total_amount,
    COUNT(*) as line_count
FROM {{ ref('stg_orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
GROUP BY 1, 2
```

**How it works:**
1. First run: `CREATE TABLE AS SELECT ...` (full computation)
2. Subsequent runs: Only process rows matching the `is_incremental()` filter
3. New rows merged into existing table via the chosen strategy

**Incremental strategies:**
- `append`: Simple INSERT (no dedup)
- `merge`: SQL MERGE on `unique_key` (upsert)
- `delete+insert`: Delete matching rows, then insert
- `insert_overwrite`: Replace entire partitions (BigQuery, Spark)

**Critical limitation vs IVM:** dbt incremental models require the user to manually
specify the filter condition (typically a timestamp or event-based predicate). The
system does not automatically track changes or compute deltas. If the filter is wrong,
the materialization silently produces incorrect results. There is no formal delta
computation or correctness guarantee.

### 1.5 Relevance to OpenIVM

| Aspect | dbt Incremental | OpenIVM |
|---|---|---|
| **Change detection** | User-specified filter (`WHERE updated_at > ...`) | Automatic via delta tables |
| **Delta computation** | User writes the delta query | Compiler derives delta from view definition |
| **Correctness** | No formal guarantee (depends on filter) | Algebraically correct (DBSP/Z-set framework) |
| **Aggregates** | User must handle (often: full recompute of group) | Automatic incremental maintenance |
| **Joins** | User must handle (often: recompute all) | Inclusion-exclusion delta rule |
| **Cross-engine** | Yes (via adapters) | Yes (SQL-to-SQL compilation) |
| **DAG management** | Built-in (ref graph) | Cascade refresh (`ivm_cascade_refresh`) |
| **Ecosystem** | Massive (testing, docs, packages, CI/CD) | Focused on correctness and performance |

**Key insight:** dbt's incremental models are a manual, heuristic approximation of
what IVM does automatically. dbt shifts the burden of correctness to the user; OpenIVM
derives correct maintenance from the query definition. However, dbt's ecosystem
(testing, documentation, lineage, CI/CD) is far more mature.

**Potential integration:** OpenIVM could serve as a backend for dbt's incremental
strategy — replacing the user-specified filter with compiler-derived delta queries.
This would give dbt users automatic, correct incremental maintenance while keeping
dbt's orchestration and ecosystem benefits.

### 1.6 dbt Metrics and Semantic Layer

dbt's semantic layer (MetricFlow) defines metrics as reusable, governed definitions:

```yaml
metrics:
  - name: revenue
    type: simple
    type_params:
      measure: total_revenue
    filter: |
      {{ Dimension('order__is_completed') }} = true
```

Metrics are materialized on-demand or pre-computed. The semantic layer adds another
potential use case for IVM: incrementally maintaining pre-computed metric caches.

---

## 2. LinkedIn Coral

### 2.1 Overview

Coral is an open-source SQL analysis and translation library developed at LinkedIn.
It provides a common intermediate representation (IR) for SQL queries, enabling
translation between SQL dialects without losing semantic information.

**GitHub:** [linkedin/coral](https://github.com/linkedin/coral)

**Core problem:** LinkedIn's data platform spans Hive, Spark, Trino/Presto, and
other engines. Views defined in one engine must be queryable from others. Direct
string-level SQL translation is fragile; Coral provides a principled IR-based approach.

### 2.2 Architecture

```
SQL String (Hive/Spark/Trino)
        ↓  [Parser]
   Coral IR (RelNode tree)
        ↓  [Rewrite rules]
   Optimized/Translated IR
        ↓  [Generator]
SQL String (target dialect)
```

**IR foundation:** Coral uses Apache Calcite's relational algebra (RelNode) as its
intermediate representation. This provides:
- Logical operators: TableScan, Project, Filter, Join, Aggregate, Sort, Union
- Type system with implicit/explicit cast handling
- UDF registry for cross-engine function translation
- View expansion (inline view definitions before translation)

### 2.3 Key Components

**Coral-Hive:** Parses HiveQL → Coral IR. Handles Hive-specific syntax: LATERAL VIEW,
TRANSFORM, Hive UDFs, complex types (ARRAY, MAP, STRUCT).

**Coral-Spark:** Generates Spark SQL from Coral IR. Maps Hive UDFs to Spark equivalents.

**Coral-Trino:** Generates Trino SQL from Coral IR. Handles Trino's stricter type system
and different function names.

**Coral-Schema:** Extracts and translates Avro schemas, enabling schema evolution tracking
across engines.

**Coral-Incremental:** Experimental module for incremental view maintenance using Coral's
IR. Computes delta queries by transforming the RelNode tree — structurally similar to
OpenIVM's optimizer-based approach.

### 2.4 View Translation Example

```
-- Original (HiveQL):
CREATE VIEW engagement AS
SELECT user_id, LATERAL VIEW explode(actions) t AS action_type
FROM user_events
WHERE ds = '2024-01-01';

-- Coral translates to Trino:
SELECT user_id, action_type
FROM user_events
CROSS JOIN UNNEST(actions) AS t(action_type)
WHERE ds = '2024-01-01';
```

Coral handles semantic differences: `LATERAL VIEW explode()` → `CROSS JOIN UNNEST()`,
implicit type coercions, function name mappings, etc.

### 2.5 Coral-Incremental (IVM Module)

Coral-Incremental is an experimental component that performs IVM at the IR level:

- Operates on Calcite RelNode trees (not SQL strings)
- Applies delta rules to transform a view's logical plan into a maintenance plan
- Supports: filter, project, join (inner), aggregate (SUM, COUNT)
- Output is a RelNode tree that can be rendered to any supported SQL dialect

**Delta rules implemented:**
- **Filter/Project:** Linear — apply to delta directly
- **Join:** `Δ(R ⋈ S) = (ΔR ⋈ S) ∪ (R ⋈ ΔS) ∪ (ΔR ⋈ ΔS)` (standard 3-term)
- **Aggregate:** Group-level recompute for affected groups

### 2.6 Relevance to OpenIVM

| Aspect | Coral | OpenIVM |
|---|---|---|
| **IR** | Calcite RelNode (Java) | DuckDB LogicalOperator (C++) |
| **Translation** | IR → SQL string via dialect generators | LPTS (LogicalPlanToString) |
| **IVM** | Experimental (Coral-Incremental) | Production (full pipeline) |
| **Cross-engine** | Yes (Hive, Spark, Trino) | Yes (any SQL engine via SQL output) |
| **UDF handling** | Registry-based translation | Not yet supported |
| **Join delta** | 3-term formula | Inclusion-exclusion (2^N - 1 terms) |
| **Ecosystem** | LinkedIn data platform | DuckDB extension |

**Shared design principle:** Both Coral and OpenIVM perform SQL-to-SQL compilation via
an intermediate relational algebra representation. Both face the same fundamental
challenge: faithfully translating logical plans back to SQL strings in the target dialect.

**Key differences:**
- Coral uses Calcite's mature Java-based relational algebra; OpenIVM uses DuckDB's C++ plan nodes
- Coral focuses on dialect translation (same query, different engines); OpenIVM focuses on
  query transformation (view query → delta/maintenance query)
- Coral-Incremental is experimental; OpenIVM has a complete IVM pipeline with upsert compilation

**Potential synergy:** Coral's dialect translation could complement OpenIVM's IVM compilation.
OpenIVM derives the delta query; Coral could translate it to the target engine's SQL dialect.
This would extend OpenIVM's cross-engine story beyond what LPTS provides.

---

## 3. Comparison: Incremental Approaches

| | OpenIVM | dbt Incremental | Coral-Incremental |
|---|---|---|---|
| **Approach** | SQL-to-SQL via optimizer | User-written filter + merge | IR-level delta rules |
| **Correctness** | Algebraic (DBSP) | User responsibility | Algebraic (Calcite IR) |
| **Automation** | Fully automatic | Manual filter specification | Automatic (experimental) |
| **Aggregates** | SUM, COUNT, MIN, MAX | Manual | SUM, COUNT |
| **Joins** | Inclusion-exclusion | Manual | 3-term delta |
| **Production ready** | Yes | Yes (widely used) | Experimental |
| **Cross-engine** | Yes (SQL output) | Yes (adapters) | Yes (Calcite generators) |
| **Change tracking** | Delta tables | Timestamp/event filters | Not specified (IR-level) |

## 4. References

- dbt documentation: https://docs.getdbt.com/
- dbt incremental models: https://docs.getdbt.com/docs/build/incremental-models
- MetricFlow: https://docs.getdbt.com/docs/build/about-metricflow
- Coral GitHub: https://github.com/linkedin/coral
- Coral blog post: https://engineering.linkedin.com/blog/2020/coral
- Coral-Incremental: part of the Coral repository, `coral-incremental/` module
- Walaa ElDin Moustafa et al., "Coral: A SQL Translation, Analysis, and Rewrite Engine" (LinkedIn, 2020)
