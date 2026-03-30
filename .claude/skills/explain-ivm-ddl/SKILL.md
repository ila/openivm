---
name: explain-ivm-ddl
description: Reference for OpenIVM DDL syntax — CREATE MATERIALIZED VIEW, PRAGMA ivm, delta tables, and configuration. Auto-loaded when discussing view setup, delta tables, or IVM refresh.
---

## OpenIVM DDL Overview

OpenIVM extends DuckDB SQL with materialized view support via a parser extension
(`src/core/openivm_parser.cpp`). It intercepts `CREATE MATERIALIZED VIEW` statements,
creates delta tracking infrastructure, and provides pragmas for incremental refresh.

### Creating a Materialized View

```sql
CREATE MATERIALIZED VIEW mv_name AS
  SELECT col1, SUM(col2) as total, COUNT(*) as cnt
  FROM base_table
  GROUP BY col1;
```

What happens internally:
1. Parser intercepts the statement before DuckDB processes it
2. Creates `delta_base_table` with the same schema plus:
   - `_duckdb_ivm_multiplicity` (BOOLEAN): `true` = insert, `false` = delete
   - `_duckdb_ivm_timestamp` (TIMESTAMP): when the change was recorded
3. Registers the view in `_duckdb_ivm_views` (stores view name, query, type)
4. Registers delta table mappings in `_duckdb_ivm_delta_tables`
5. Rewrites aggregates (e.g., adds COUNT alongside SUM for correct maintenance)
6. Creates a regular DuckDB view for the initial query

### Refreshing a Materialized View

```sql
-- Incremental refresh (default)
PRAGMA ivm('mv_name');

-- With explicit catalog/schema
PRAGMA ivm_options('catalog', 'schema', 'mv_name');

-- Check cost estimate (IVM vs full recompute)
PRAGMA ivm_cost('mv_name');

-- Cross-system refresh
PRAGMA ivm_cross_system('mv_name', 'catalog', 'schema', 'system', 'path');
```

### Delta Tables

Changes to base tables are tracked in delta tables automatically. Delta table naming:
`delta_<base_table_name>`.

```sql
-- View delta table contents
SELECT * FROM delta_orders;

-- System metadata tables
SELECT * FROM _duckdb_ivm_views;          -- view definitions
SELECT * FROM _duckdb_ivm_delta_tables;   -- delta table mappings + timestamps
```

**Important**: Do NOT manually INSERT into delta tables in tests. Let OpenIVM handle
delta tracking. Direct manipulation breaks the IVM invariants.

### Configuration

```sql
SET ivm_refresh_mode = 'auto';         -- auto (default), incremental, or full
SET ivm_adaptive_refresh = true;               -- enable adaptive cost model
SET ivm_files_path = '/tmp/ivm/';      -- path for compiled query files
```

### Supported Query Patterns

| Pattern | IVM Type | Example |
|---|---|---|
| Projection | `SIMPLE_PROJECTION` | `SELECT a, b FROM t` |
| Filter | `SIMPLE_PROJECTION` | `SELECT a FROM t WHERE x > 5` |
| Grouped aggregate | `AGGREGATE_GROUP` | `SELECT a, SUM(b) FROM t GROUP BY a` |
| Ungrouped aggregate | `SIMPLE_AGGREGATE` | `SELECT COUNT(*) FROM t` |
| Inner join | Any | `SELECT ... FROM t1 JOIN t2 ON ...` |

### Limitations

- Only INNER joins (no outer/cross joins)
- Only SUM, COUNT aggregates fully supported (MIN/MAX partial)
- No `DROP MATERIALIZED VIEW` yet
- LPTS plan-to-SQL conversion is ongoing work

### Key source files

- `src/core/openivm_parser.cpp` — parser extension (intercepts CREATE MATERIALIZED VIEW)
- `src/core/openivm_metadata.cpp` — system table queries for view/delta metadata
- `src/upsert/openivm_upsert.cpp` — PRAGMA ivm() handler
- `src/upsert/openivm_compile_upsert.cpp` — generates upsert SQL (3 paths by view type)
- `src/upsert/openivm_cost_model.cpp` — IVM vs recompute cost estimation
