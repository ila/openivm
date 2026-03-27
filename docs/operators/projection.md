# Projection

Select columns from a single table, optionally with computed expressions.

```sql
CREATE MATERIALIZED VIEW emp_names AS
    SELECT id, name FROM employees;
```

Expressions in SELECT such as `a * 2`, `b + c`, and `CASE WHEN` work transparently -- they are applied to the delta rows the same way they would be applied to the base rows.

```sql
CREATE MATERIALIZED VIEW mv_expr AS
    SELECT a * 2 AS doubled, b + c AS total, c - b AS diff FROM expr_base;
```

## How IVM handles it

OpenIVM scans the delta table for new/deleted rows, projects the requested columns, and inserts them into the delta view. The upsert phase consolidates net changes per distinct tuple using counting: net insertions are replicated via `generate_series`, net deletions are removed via `rowid` + `ROW_NUMBER`.

## Compiled SQL

### IVM query (delta propagation)

```sql
WITH scan_0 (t0_id, t0_name, t0__duckdb_ivm_multiplicity) AS (
    SELECT id, name, _duckdb_ivm_multiplicity
    FROM delta_employees
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
),
projection_1 (t1_id, t1_name, t1__duckdb_ivm_multiplicity) AS (
    SELECT t0_id, t0_name, t0__duckdb_ivm_multiplicity
    FROM scan_0
)
INSERT INTO delta_emp_names (id, name, _duckdb_ivm_multiplicity)
SELECT * FROM projection_1;
```

### Upsert (counting consolidation)

```sql
-- Net change per distinct tuple
WITH _ivm_net AS (
    SELECT id, name,
        SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS _net
    FROM delta_emp_names
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
    GROUP BY id, name
    HAVING SUM(CASE WHEN _duckdb_ivm_multiplicity THEN 1 ELSE -1 END) != 0
)
-- Delete net-removed copies using rowid + ROW_NUMBER
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

-- Insert net-added copies using generate_series
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

Projection-only views support full bag semantics including duplicate rows, mixed INSERT/DELETE/UPDATE in a single refresh cycle, and computed expressions.
