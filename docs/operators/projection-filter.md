# Projection and filter

## Example (projection)

```sql
CREATE TABLE employees (id INT, name VARCHAR, dept VARCHAR);
INSERT INTO employees VALUES (1, 'Alice', 'Eng'), (2, 'Bob', 'Sales');

CREATE MATERIALIZED VIEW emp_names AS
    SELECT id, name FROM employees;

INSERT INTO employees VALUES (3, 'Charlie', 'Eng');
PRAGMA ivm('emp_names');
```

Expressions in SELECT such as `a * 2`, `b + c`, and `CASE WHEN` work transparently — they are applied to the delta rows the same way they would be applied to the base rows.

```sql
CREATE MATERIALIZED VIEW mv_expr AS
    SELECT a * 2 AS doubled, b + c AS total, c - b AS diff FROM expr_base;
```

## Example (filter)

```sql
CREATE TABLE products (id INT, name VARCHAR, price INT);
INSERT INTO products VALUES (1, 'Widget', 10), (2, 'Gadget', 25);

CREATE MATERIALIZED VIEW cheap_products AS
    SELECT id, name, price FROM products WHERE price < 20;

INSERT INTO products VALUES (3, 'Bolt', 5);
PRAGMA ivm('cheap_products');
```

## How IVM handles it

**Algebraic rules:**

```
delta(pi_A(R))      = pi_A(delta(R))              -- projection passes through deltas
delta(sigma_p(R))   = sigma_p(delta(R))            -- filter passes through deltas
delta(pi_A(sigma_p(R))) = pi_A(sigma_p(delta(R)))  -- combined: filter, then project the delta
```

Filters and projections are **linear operators** — their incremental form is the same as their original form, applied to the delta instead of the base table. Only delta rows that pass the WHERE predicate (if any) are propagated.

The upsert uses **counting-based consolidation**: each distinct tuple gets a net count (_net). Net insertions are replicated via `generate_series`, net deletions are removed via `rowid` + `ROW_NUMBER`. This preserves full bag semantics including duplicate rows.

## Compiled SQL (projection)

### IVM query (delta propagation)

```sql
-- Scan the delta table for rows changed since the last refresh
-- Each row carries the original columns plus a multiplicity flag
WITH scan_0 (t0_id, t0_name, t0__duckdb_ivm_multiplicity) AS (
    SELECT id, name, _duckdb_ivm_multiplicity
    FROM delta_employees
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
),
-- Apply the same projection as the view definition
-- The projection is "free" — just pass through the requested columns
projection_1 (t1_id, t1_name, t1__duckdb_ivm_multiplicity) AS (
    SELECT t0_id, t0_name, t0__duckdb_ivm_multiplicity
    FROM scan_0
)
INSERT INTO delta_emp_names (id, name, _duckdb_ivm_multiplicity)
SELECT * FROM projection_1;
```

## Compiled SQL (filter)

### IVM query (delta propagation with filter)

```sql
-- Scan the delta table for rows changed since the last refresh
WITH scan_0 (t0_id, t0_name, t0_price, t0__duckdb_ivm_multiplicity) AS (
    SELECT id, name, price, _duckdb_ivm_multiplicity
    FROM delta_products
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
),
-- Apply the WHERE predicate to the delta rows
-- Only rows that satisfy the filter are propagated — the rest are irrelevant
filter_1 AS (
    SELECT * FROM scan_0
    WHERE (t0_price) < (20)
),
projection_2 (t1_id, t1_name, t1_price, t1__duckdb_ivm_multiplicity) AS (
    SELECT t0_id, t0_name, t0_price, t0__duckdb_ivm_multiplicity
    FROM filter_1
)
INSERT INTO delta_cheap_products (id, name, price, _duckdb_ivm_multiplicity)
SELECT * FROM projection_2;
```

## Upsert (counting consolidation)

The upsert is identical for projection and filter views. Shown here for the projection example:

```sql
-- Step 1: compute the net change per distinct tuple
-- +1 for each insertion, -1 for each deletion, sum to get net effect
-- HAVING filters out tuples where inserts and deletes cancel out (_net = 0)
WITH _ivm_net AS (
    SELECT id, name,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_emp_names
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, name
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
-- Step 2: delete net-removed copies
-- ROW_NUMBER assigns a deterministic ordering within each group of identical tuples
-- We delete exactly |_net| copies, starting from the lowest rowid
DELETE FROM emp_names WHERE rowid IN (
    SELECT v.rowid FROM (
        SELECT rowid, id, name,
            ROW_NUMBER() OVER (PARTITION BY id, name ORDER BY rowid) AS _rn
        FROM emp_names
    ) v JOIN _ivm_net d
        ON v.id IS NOT DISTINCT FROM d.id
       AND v.name IS NOT DISTINCT FROM d.name
    WHERE d._net < 0 AND v._rn <= -d._net
);

-- Step 3: insert net-added copies
-- generate_series(1, _net) replicates the tuple _net times for bag semantics
WITH _ivm_net AS (
    SELECT id, name,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_emp_names
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, name
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
INSERT INTO emp_names SELECT id, name
FROM _ivm_net, generate_series(1, _ivm_net._net::BIGINT)
WHERE _ivm_net._net > 0;
```

## Limitations

- Projection-only views support full bag semantics including duplicate rows, mixed INSERT/DELETE/UPDATE in a single refresh cycle, and computed expressions.
- Filters work with any predicate that DuckDB supports in WHERE clauses.
- When a filter is combined with grouped aggregates, the HAVING clause forces a group-recompute for affected groups (see [Grouped Aggregates](grouped-aggregates.md)).
