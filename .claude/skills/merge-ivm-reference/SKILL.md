---
name: merge-ivm-reference
description: Reference for SQL MERGE statement and its application to IVM upsert patterns. Auto-loaded when discussing upserts, delta application, MERGE, INSERT OR REPLACE, or the CompileAggregateGroups/CompileSimpleAggregates/CompileProjectionsFilters functions.
---

# SQL MERGE for IVM Upsert Operations -- Reference

Comprehensive reference on the SQL MERGE statement and its application to
Incremental View Maintenance (IVM) upsert patterns, written in the context of
the OpenIVM DuckDB extension.

---

## 1. The MERGE Statement (SQL Standard)

### 1.1 Overview

MERGE is a single DML statement that can perform INSERT, UPDATE, and DELETE
operations on a **target table** based on the results of a join with a
**source table** (or subquery). It was introduced in SQL:2003 and refined in
SQL:2008 and SQL:2016.

The key abstraction: the database joins source to target once, then for each
resulting row it decides which action to take based on declarative WHEN clauses.

### 1.2 Full Syntax (SQL Standard, as implemented across major databases)

```sql
[ WITH cte_name AS (...) [, ...] ]
MERGE INTO target_table [ [ AS ] target_alias ]
USING source_table_or_subquery [ [ AS ] source_alias ]
ON join_condition

-- Actions for rows that exist in BOTH source and target
{ WHEN MATCHED [ AND condition ] THEN
    { UPDATE SET col = expr [, ...]
    | DELETE
    | DO NOTHING }
} [ ... ]                          -- multiple WHEN MATCHED allowed

-- Actions for rows in the SOURCE that have no match in target
{ WHEN NOT MATCHED [ BY TARGET ] [ AND condition ] THEN
    { INSERT [ ( col [, ...] ) ] VALUES ( expr [, ...] )
    | INSERT DEFAULT VALUES
    | DO NOTHING }
} [ ... ]

-- Actions for rows in the TARGET that have no match in source (SQL Server / PG17+ / DuckDB)
{ WHEN NOT MATCHED BY SOURCE [ AND condition ] THEN
    { UPDATE SET col = expr [, ...]
    | DELETE
    | DO NOTHING }
} [ ... ]

[ RETURNING ... ]
```

### 1.3 Clause Semantics

| Clause | Meaning | Available Actions |
|--------|---------|-------------------|
| `WHEN MATCHED` | Row exists in both source and target (join succeeds) | UPDATE, DELETE, DO NOTHING |
| `WHEN NOT MATCHED [BY TARGET]` | Row in source has no target match | INSERT, DO NOTHING |
| `WHEN NOT MATCHED BY SOURCE` | Row in target has no source match | UPDATE, DELETE, DO NOTHING |

**Evaluation order:** WHEN clauses are evaluated top-to-bottom; the first
clause whose condition is satisfied fires. Only ONE action per row.

**Critical constraint:** Each target row must match **at most one** source row.
If multiple source rows match the same target row, the database raises an error
(this is mandated by the standard, though enforcement varies).

### 1.4 Conditional Actions

```sql
WHEN MATCHED AND target.status = 'active' THEN
    UPDATE SET balance = target.balance + source.amount
WHEN MATCHED AND target.status = 'closed' THEN
    DELETE
WHEN MATCHED THEN
    DO NOTHING
```

Multiple WHEN MATCHED clauses with different AND conditions allow branching
logic within a single statement.

### 1.5 The Source/Target Model

- **Target:** The table being modified (the materialized view in IVM context)
- **Source:** A table, subquery, or CTE providing the change data (the delta
  in IVM context)
- **ON clause:** Defines how source rows map to target rows (typically a
  primary/group key equality)

### 1.6 How MERGE Differs from Other Upsert Mechanisms

| Feature | INSERT OR REPLACE | INSERT ON CONFLICT | MERGE |
|---------|------------------|--------------------|-------|
| Requires PK/unique constraint | Yes | Yes | No (arbitrary ON condition) |
| Can UPDATE subset of columns | No (replaces entire row) | Yes | Yes |
| Can DELETE | No | No | Yes |
| Multiple conditional actions | No | No | Yes |
| Source can be a subquery | N/A | N/A | Yes |
| Single-statement insert+update+delete | No | No | Yes |
| NULL key handling | Breaks (NULLs don't conflict) | Breaks | Works (IS NOT DISTINCT FROM in ON) |
| Handle "not matched by source" | No | No | Yes |

---

## 2. DuckDB MERGE Support

### 2.1 Availability

MERGE INTO was introduced in **DuckDB v1.4.0** (September 2025, "Andium" LTS
release). OpenIVM currently builds against DuckDB main, which tracks ahead of
v1.4.0, so MERGE is available.

### 2.2 Supported Syntax

```sql
MERGE INTO target_table
USING source_table_or_subquery AS source_alias
ON (match_condition)               -- parentheses required around condition

-- Shorthand: USING (column_name) when column names match
MERGE INTO target USING source USING (id)

-- All WHEN clause types:
WHEN MATCHED [ AND condition ] THEN UPDATE [ SET col = expr, ... ]
WHEN MATCHED [ AND condition ] THEN DELETE
WHEN NOT MATCHED [ BY TARGET ] [ AND condition ] THEN INSERT [ (cols) VALUES (vals) ]
WHEN NOT MATCHED [ BY TARGET ] [ AND condition ] THEN INSERT BY NAME
WHEN NOT MATCHED BY SOURCE [ AND condition ] THEN UPDATE SET ...
WHEN NOT MATCHED BY SOURCE [ AND condition ] THEN DELETE

-- RETURNING clause:
RETURNING merge_action, *;
-- merge_action returns 'INSERT', 'UPDATE', or 'DELETE'
```

### 2.3 DuckDB-Specific Features

- **`INSERT BY NAME`**: Inserts using column name matching rather than
  positional. DuckDB extension, not in the SQL standard.
- **`USING (column_name)` shorthand**: When the join column has the same name
  in source and target, this shorthand avoids repeating `ON source.id = target.id`.
- **`RETURNING merge_action`**: Returns a string column indicating which
  action was taken for each affected row.
- **CTE support in USING**: CTEs defined with `WITH` can be used as the source
  in the `USING` clause.

### 2.4 Known Limitations (as of v1.4.x)

1. **MERGE cannot appear inside a CTE**: `WITH result AS (MERGE INTO ...) SELECT ...`
   produces a parser error. This is a known missing feature (GitHub Discussion #19451).
2. **RETURNING cannot reference source columns**: `RETURNING merge_action, src.*`
   fails with a binder error. Only target columns are accessible.
3. **No duplicate detection in source**: If the source contains duplicate rows
   matching the same target row, DuckDB does not raise an error (unlike the
   standard), which can produce undefined behavior.
4. **Memory usage**: Large MERGE operations can consume significant memory
   (GitHub Issue #19958).
5. **CTEs are materialized by default** (since v1.4.0): This is generally
   beneficial for correctness but may affect performance of complex CTEs used
   as MERGE sources.

### 2.5 DuckDB MERGE Examples

**Basic upsert:**
```sql
MERGE INTO people
USING (SELECT 3 AS id, 'Sarah' AS name, 95000.0 AS salary) AS upserts
ON (upserts.id = people.id)
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED THEN INSERT;
```

**Conditional delete:**
```sql
WITH deletes(item_id, delete_threshold) AS (VALUES (10, 3000))
MERGE INTO stock
USING deletes
USING (item_id)
WHEN MATCHED AND balance < delete_threshold THEN DELETE
RETURNING merge_action, *;
```

**Full sync (insert + update + delete):**
```sql
MERGE INTO target
USING (SELECT 1 AS id) source
USING (id)
WHEN MATCHED THEN UPDATE
WHEN NOT MATCHED BY SOURCE THEN DELETE
RETURNING merge_action, *;
```

---

## 3. MERGE for IVM Upsert Operations

### 3.1 Current OpenIVM Upsert Patterns

OpenIVM currently generates SQL strings to apply deltas to materialized views.
The patterns differ by view type. Source: `src/upsert/openivm_compile_upsert.cpp`.

#### 3.1.1 AGGREGATE_GROUP (Current Pattern)

Three separate statements executed sequentially:

```
Statement 1: UPDATE (add delta to existing groups)
  WITH ivm_cte AS (
    SELECT group_key,
           SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -agg ELSE agg END) AS agg
    FROM delta_mv WHERE <ts_filter>
    GROUP BY group_key
  )
  UPDATE mv SET agg = mv.agg + d.agg
  FROM ivm_cte d
  WHERE mv.group_key IS NOT DISTINCT FROM d.group_key;

Statement 2: INSERT (new groups not yet in MV)
  WITH ivm_cte AS ( ... same CTE ... )
  INSERT INTO mv SELECT d.group_key, d.agg
  FROM ivm_cte d
  WHERE NOT EXISTS (SELECT 1 FROM mv v WHERE v.group_key IS NOT DISTINCT FROM d.group_key);

Statement 3: DELETE (groups that zeroed out)
  DELETE FROM mv WHERE agg = 0;
```

**Problems with this pattern:**
- Three separate passes over the data
- The CTE is duplicated in UPDATE and INSERT (though DuckDB materializes it)
- The DELETE is a full table scan checking all rows, not just affected groups
- Race conditions possible between UPDATE and INSERT (though run in single TX)

#### 3.1.2 SIMPLE_AGGREGATE (Current Pattern)

Single UPDATE statement with correlated subqueries:

```sql
UPDATE mv SET
  col = COALESCE(col, 0)
    - COALESCE((SELECT col FROM delta_mv WHERE _duckdb_ivm_multiplicity = false AND <ts>), 0)
    + COALESCE((SELECT col FROM delta_mv WHERE _duckdb_ivm_multiplicity = true AND <ts>), 0);
```

**This pattern works well** since there is always exactly one row. MERGE would
add complexity without benefit here.

#### 3.1.3 SIMPLE_PROJECTION (Current Pattern)

Two statements using EXCEPT ALL for bag-arithmetic consolidation:

```sql
-- Delete net removals
WITH net_dels AS (
  SELECT cols FROM delta_mv WHERE multiplicity = false
  EXCEPT ALL
  SELECT cols FROM delta_mv WHERE multiplicity = true
)
DELETE FROM mv WHERE EXISTS (SELECT 1 FROM net_dels WHERE <match>);

-- Insert net additions
WITH net_ins AS (
  SELECT cols FROM delta_mv WHERE multiplicity = true
  EXCEPT ALL
  SELECT cols FROM delta_mv WHERE multiplicity = false
)
INSERT INTO mv SELECT cols FROM net_ins;
```

### 3.2 MERGE-Based Replacement for AGGREGATE_GROUP

This is the highest-value target for MERGE. The three-statement pattern
(UPDATE + INSERT + DELETE) collapses into a single MERGE:

```sql
WITH delta AS (
    SELECT group_key,
           SUM(CASE WHEN _duckdb_ivm_multiplicity = false
                    THEN -agg_col ELSE agg_col END) AS agg_col
    FROM delta_mv
    WHERE _duckdb_ivm_timestamp >= '<last_update>'
    GROUP BY group_key
)
MERGE INTO mv
USING delta AS d
ON (mv.group_key IS NOT DISTINCT FROM d.group_key)

WHEN MATCHED AND (mv.agg_col + d.agg_col) = 0 THEN
    DELETE

WHEN MATCHED THEN
    UPDATE SET agg_col = mv.agg_col + d.agg_col

WHEN NOT MATCHED BY TARGET THEN
    INSERT (group_key, agg_col)
    VALUES (d.group_key, d.agg_col);
```

**Advantages:**
- Single statement, single pass over both tables
- Consolidation (the CTE) is computed once
- DELETE of zeroed-out groups happens in the same pass (no full table scan)
- Correct ordering: DELETE check before UPDATE (clause evaluation is top-down)
- NULL group keys handled correctly via IS NOT DISTINCT FROM in ON clause

#### Multi-column example

For a view like:
```sql
CREATE MATERIALIZED VIEW mv AS
  SELECT group_key, SUM(value) AS total, COUNT(*) AS cnt
  FROM base_table GROUP BY group_key;
```

The MERGE becomes:

```sql
WITH delta AS (
    SELECT group_key,
           SUM(CASE WHEN _duckdb_ivm_multiplicity = false
                    THEN -total ELSE total END) AS total,
           SUM(CASE WHEN _duckdb_ivm_multiplicity = false
                    THEN -cnt ELSE cnt END) AS cnt
    FROM delta_mv
    WHERE _duckdb_ivm_timestamp >= '<last_update>'
    GROUP BY group_key
)
MERGE INTO mv
USING delta AS d
ON (mv.group_key IS NOT DISTINCT FROM d.group_key)

-- Delete groups where ALL aggregates reach zero
WHEN MATCHED AND (mv.total + d.total) = 0 AND (mv.cnt + d.cnt) = 0 THEN
    DELETE

-- Update existing groups
WHEN MATCHED THEN
    UPDATE SET total = mv.total + d.total,
               cnt   = mv.cnt   + d.cnt

-- Insert brand-new groups
WHEN NOT MATCHED BY TARGET THEN
    INSERT (group_key, total, cnt)
    VALUES (d.group_key, d.total, d.cnt);
```

**Deletion condition note:** For correctness, a group should be deleted when
`cnt` reaches 0 (COUNT(*) = 0 means no rows contribute to the group). The
`total = 0` check is not strictly necessary but acts as a safety check.
In practice, `cnt = 0` is the authoritative signal.

### 3.3 MERGE-Based Replacement for SIMPLE_PROJECTION

```sql
WITH net_delta AS (
    -- Consolidate: compute net inserts and net deletes via bag arithmetic
    SELECT cols, true AS _is_insert FROM (
        SELECT cols FROM delta_mv WHERE _duckdb_ivm_multiplicity = true
        EXCEPT ALL
        SELECT cols FROM delta_mv WHERE _duckdb_ivm_multiplicity = false
    )
    UNION ALL
    SELECT cols, false AS _is_insert FROM (
        SELECT cols FROM delta_mv WHERE _duckdb_ivm_multiplicity = false
        EXCEPT ALL
        SELECT cols FROM delta_mv WHERE _duckdb_ivm_multiplicity = true
    )
)
-- Problem: MERGE requires a join condition, but projections have no key.
-- This makes MERGE awkward for SIMPLE_PROJECTION.
```

**MERGE is NOT well-suited for SIMPLE_PROJECTION** because:
- Projection views have no natural key to join on
- Rows are bags (duplicates allowed), and MERGE requires at-most-one match
- The EXCEPT ALL consolidation pattern is already clean and correct
- Forcing a MERGE would require synthesizing row IDs, adding complexity

**Recommendation:** Keep the current EXCEPT ALL pattern for SIMPLE_PROJECTION.

### 3.4 MERGE-Based Replacement for SIMPLE_AGGREGATE

Not recommended. The current single-UPDATE pattern is already optimal:
- There is always exactly one row in the MV
- MERGE overhead (join logic, clause evaluation) is unnecessary
- The correlated subquery pattern is clear and efficient

### 3.5 Summary: Where MERGE Helps

| View Type | Current Pattern | MERGE Benefit | Recommendation |
|-----------|----------------|---------------|----------------|
| AGGREGATE_GROUP | UPDATE + INSERT + DELETE (3 stmts) | High: collapses to 1 stmt, single pass, no full-scan DELETE | **Adopt MERGE** |
| SIMPLE_AGGREGATE | Single UPDATE | None: already optimal | Keep current |
| SIMPLE_PROJECTION | EXCEPT ALL + DELETE + INSERT | Low: no natural key for MERGE | Keep current |

---

## 4. MERGE vs Alternative Upsert Strategies

### 4.1 INSERT OR REPLACE (SQLite / DuckDB)

```sql
INSERT OR REPLACE INTO mv SELECT ...
```

| Aspect | Assessment |
|--------|-----------|
| **Mechanism** | Deletes conflicting row, inserts new one |
| **Requires** | PRIMARY KEY or UNIQUE constraint |
| **Pros** | Simple syntax, single statement |
| **Cons** | Replaces entire row (can't do partial update like `mv.agg + delta.agg`); triggers DELETE+INSERT rather than UPDATE; NULL keys never conflict (silently inserts duplicates); loses row identity |
| **IVM suitability** | Poor. Cannot express "add delta to existing aggregate." Would need to pre-compute the final value in the source query, defeating the purpose of incremental maintenance. |

### 4.2 INSERT ON CONFLICT DO UPDATE (PostgreSQL UPSERT)

```sql
INSERT INTO mv (group_key, total, cnt)
VALUES (...)
ON CONFLICT (group_key) DO UPDATE SET
    total = mv.total + EXCLUDED.total,
    cnt   = mv.cnt   + EXCLUDED.cnt;
```

| Aspect | Assessment |
|--------|-----------|
| **Mechanism** | Attempts INSERT; on unique violation, runs UPDATE |
| **Requires** | UNIQUE constraint or unique index on conflict columns |
| **Pros** | Single statement for upsert; `EXCLUDED` pseudo-table is convenient; handles concurrent inserts well |
| **Cons** | Cannot DELETE in the same statement; NULL keys don't conflict (unique indexes allow multiple NULLs); DuckDB supports ON CONFLICT but it requires a PK/unique index; no conditional branching |
| **IVM suitability** | Moderate. Handles UPDATE+INSERT but needs a separate DELETE for zeroed-out groups. NULL group keys are problematic. OpenIVM previously used this but switched away due to NULL key issues. |

### 4.3 MERGE (SQL Standard)

```sql
MERGE INTO mv USING delta ON (mv.key IS NOT DISTINCT FROM delta.key)
WHEN MATCHED AND ... THEN DELETE
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

| Aspect | Assessment |
|--------|-----------|
| **Mechanism** | Joins source to target, executes declarative actions per match status |
| **Requires** | Nothing (arbitrary ON condition) |
| **Pros** | Single statement for INSERT+UPDATE+DELETE; arbitrary join conditions (IS NOT DISTINCT FROM for NULLs); multiple conditional clauses; no PK required; clean semantics |
| **Cons** | More complex SQL generation; source must not produce duplicate matches to same target row; some databases report MERGE as slightly slower than separate statements for simple cases |
| **IVM suitability** | **Excellent for AGGREGATE_GROUP.** Handles all three operations (update existing, insert new, delete zeroed) in one pass. IS NOT DISTINCT FROM solves the NULL key problem. |

### 4.4 Separate INSERT + UPDATE + DELETE

```sql
UPDATE mv SET agg = mv.agg + d.agg FROM delta d WHERE mv.key = d.key;
INSERT INTO mv SELECT ... FROM delta d WHERE NOT EXISTS (...);
DELETE FROM mv WHERE agg = 0;
```

| Aspect | Assessment |
|--------|-----------|
| **Mechanism** | Three independent statements |
| **Requires** | Nothing special |
| **Pros** | Each statement is simple and independently tunable; easy to debug; clear semantics |
| **Cons** | Three passes over the data; CTE duplication (for UPDATE and INSERT); DELETE scans full MV table; potential for subtle ordering bugs |
| **IVM suitability** | This is the **current OpenIVM approach** for AGGREGATE_GROUP. It works correctly but is suboptimal. |

### 4.5 DELETE then INSERT

```sql
DELETE FROM mv WHERE key IN (SELECT key FROM delta);
INSERT INTO mv SELECT * FROM (full_recompute_for_affected_keys);
```

| Aspect | Assessment |
|--------|-----------|
| **Mechanism** | Remove affected rows, recompute and reinsert |
| **Requires** | Access to the original view query for recomputation |
| **Pros** | Correct by construction; handles MIN/MAX (non-invertible aggregates) |
| **Cons** | Requires re-executing the original query (partially); loses and recreates rows; more I/O |
| **IVM suitability** | This is OpenIVM's **current approach for MIN/MAX aggregates** (`has_minmax` path). Appropriate for non-invertible aggregates but wasteful for SUM/COUNT. |

### 4.6 EXCEPT ALL Consolidation (OpenIVM Current for Projections)

```sql
-- Net deletes
WITH net_dels AS (dels EXCEPT ALL ins)
DELETE FROM mv WHERE EXISTS (SELECT 1 FROM net_dels WHERE ...);

-- Net inserts
WITH net_ins AS (ins EXCEPT ALL dels)
INSERT INTO mv SELECT * FROM net_ins;
```

| Aspect | Assessment |
|--------|-----------|
| **Mechanism** | Bag-arithmetic cancellation of matching insert/delete pairs |
| **Requires** | Nothing special |
| **Pros** | Correct for bag semantics (handles duplicates properly); essential when inclusion-exclusion join delta rules produce redundant pairs |
| **Cons** | Two passes; DELETE uses EXISTS which may not use indexes efficiently |
| **IVM suitability** | **Ideal for SIMPLE_PROJECTION** where there is no key and rows form a bag. MERGE cannot easily replace this. |

---

## 5. Detailed MERGE Pattern for Grouped Aggregates

### 5.1 Setup

```sql
-- Original view definition
CREATE MATERIALIZED VIEW mv AS
    SELECT group_key, SUM(value) AS total, COUNT(*) AS cnt
    FROM base_table
    GROUP BY group_key;

-- Delta table (populated by IVM triggers/rules)
-- delta_mv has columns: group_key, total, cnt, _duckdb_ivm_multiplicity, _duckdb_ivm_timestamp
```

### 5.2 The Complete MERGE Statement

```sql
WITH ivm_cte AS (
    -- Step 1: Consolidate delta rows per group.
    -- Multiplicity=true means INSERT (positive contribution).
    -- Multiplicity=false means DELETE (negative contribution).
    SELECT
        group_key,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -total ELSE total END) AS total,
        SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -cnt   ELSE cnt   END) AS cnt
    FROM delta_mv
    WHERE _duckdb_ivm_timestamp >= '<last_update_ts>'
    GROUP BY group_key
)
MERGE INTO mv
USING ivm_cte AS d
ON (mv.group_key IS NOT DISTINCT FROM d.group_key)

-- Case 1: Group exists and aggregates zero out -> delete the group
WHEN MATCHED AND (mv.cnt + d.cnt) = 0 THEN
    DELETE

-- Case 2: Group exists and has nonzero result -> update aggregates
WHEN MATCHED THEN
    UPDATE SET
        total = mv.total + d.total,
        cnt   = mv.cnt   + d.cnt

-- Case 3: New group not in MV -> insert
WHEN NOT MATCHED BY TARGET THEN
    INSERT (group_key, total, cnt)
    VALUES (d.group_key, d.total, d.cnt);
```

### 5.3 Multi-Column Group Keys

When the group key is composite (e.g., `region, product`), the ON clause uses
AND for each key column:

```sql
ON (mv.region IS NOT DISTINCT FROM d.region
    AND mv.product IS NOT DISTINCT FROM d.product)
```

### 5.4 Handling the Multiplicity Column

The `_duckdb_ivm_multiplicity` column is a BOOLEAN:
- `true` = this delta row represents an INSERT (positive contribution)
- `false` = this delta row represents a DELETE (negative contribution)

The CTE consolidation converts this to signed arithmetic:
```sql
SUM(CASE WHEN _duckdb_ivm_multiplicity = false THEN -agg ELSE agg END)
```

This is identical to the current OpenIVM approach and is preserved in the
MERGE pattern.

### 5.5 Deletion of Zeroed-Out Groups

**Yes, MERGE can handle this in one statement.** The key is clause ordering:

```sql
WHEN MATCHED AND (mv.cnt + d.cnt) = 0 THEN DELETE    -- checked first
WHEN MATCHED THEN UPDATE SET ...                       -- fallback
```

Because WHEN clauses are evaluated top-to-bottom, the DELETE condition is
checked before the UPDATE. If `cnt` reaches zero, the row is deleted; otherwise
it is updated. This eliminates the need for a separate `DELETE FROM mv WHERE cnt = 0`
that scans the entire table.

### 5.6 Comparison with Current OpenIVM Approach

| Aspect | Current (UPDATE + INSERT + DELETE) | MERGE |
|--------|-----------------------------------|-------|
| Number of statements | 3 | 1 |
| CTE duplication | CTE repeated in UPDATE and INSERT | CTE used once |
| DELETE scope | Full table scan (`WHERE agg = 0`) | Only matched rows |
| NULL key handling | IS NOT DISTINCT FROM (manual) | IS NOT DISTINCT FROM (in ON) |
| Passes over data | 3 | 1 |
| Code complexity | ~80 lines of C++ string building | ~40 lines (estimated) |
| Correctness risk | Ordering between UPDATE/INSERT matters | Single atomic operation |
| DuckDB version required | Any | >= 1.4.0 |

### 5.7 List Mode (PAC Counters)

For the PAC integration where aggregates are LIST<DOUBLE>, the MERGE pattern
adapts the SET clause:

```sql
WHEN MATCHED THEN
    UPDATE SET
        total = list_transform(
            list_zip(mv.total, d.total),
            lambda x: x[1] + x[2])
```

The CTE consolidation similarly uses list operations:

```sql
list_reduce(
    list(CASE WHEN _duckdb_ivm_multiplicity = false
         THEN list_transform(total, lambda x: -x)
         ELSE total END),
    lambda a, b: list_transform(
        list_zip(a, b), lambda x: x[1] + x[2])
) AS total
```

### 5.8 MIN/MAX Aggregates

MERGE does not help with MIN/MAX (non-invertible) aggregates. The current
DELETE+recompute strategy remains necessary. The `has_minmax` code path should
be kept as-is.

---

## 6. Performance Considerations

### 6.1 MERGE vs Separate Statements

**Theoretical advantage of MERGE:**
- Single pass: the database joins source (delta) to target (MV) once, then
  applies all actions in that single pass
- Single execution plan: the optimizer sees the full picture and can choose
  optimal join strategies
- Single transaction overhead: one statement = one implicit transaction

**Empirical findings (from SQL Server benchmarks, not DuckDB-specific):**
- SQL Server benchmarks show MERGE can be ~19-28% slower than separate
  INSERT/UPDATE for simple cases, due to the overhead of the more complex
  execution plan
- However, for IVM the workload is different: we need INSERT+UPDATE+DELETE
  together, and the separate approach requires 3 passes. MERGE's single-pass
  advantage likely dominates.
- DuckDB's columnar engine and vectorized execution may handle MERGE
  differently than row-store databases like SQL Server

**Expected IVM-specific benefits:**
- The delta CTE is computed exactly once (vs. twice in the current approach)
- The DELETE check is scoped to matched rows (vs. full table scan)
- Single plan means the join between delta and MV is done once
- For small deltas (common in IVM), the MERGE overhead is negligible

### 6.2 Index Usage During MERGE

- DuckDB uses ART indexes for primary keys and unique constraints
- The ON clause in MERGE can leverage indexes on the target table's group key
- OpenIVM already creates an index (`<view_name>_ivm_index`) on group keys
  for AGGREGATE_GROUP views -- this index will benefit MERGE's ON clause
- No index changes are needed when adopting MERGE

### 6.3 Batch MERGE (Many Deltas at Once)

MERGE naturally handles batch deltas. The CTE consolidation aggregates all
pending delta rows per group, and the MERGE applies them all in one pass.
This is superior to row-by-row approaches.

For chained materialized views (where delta_mv rows accumulate from multiple
refresh rounds), the timestamp filter in the CTE ensures only relevant deltas
are processed:

```sql
WHERE _duckdb_ivm_timestamp >= '<last_update_ts>'
```

### 6.4 Memory Considerations

DuckDB's MERGE implementation may consume significant memory for large delta
sets (GitHub Issue #19958). For IVM, delta sets are typically small relative
to the full table, so this is unlikely to be a practical concern. If memory
becomes an issue, the adaptive cost model (`ivm_adaptive_refresh` setting) can fall
back to full recompute.

---

## 7. Other Database MERGE Implementations

### 7.1 PostgreSQL MERGE (PG 15+)

**Introduced in PG 15** after years of development.

**Syntax (PG 18 current):**
```sql
[ WITH cte ... ]
MERGE INTO [ ONLY ] target [ * ] [ AS alias ]
USING source ON condition
WHEN MATCHED [ AND cond ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN NOT MATCHED BY SOURCE [ AND cond ] THEN { UPDATE SET ... | DELETE | DO NOTHING }
WHEN NOT MATCHED [ BY TARGET ] [ AND cond ] THEN { INSERT ... | DO NOTHING }
[ RETURNING [ WITH ( { OLD | NEW } AS alias ) ] ... ]
```

**PG-specific features:**
- `WHEN NOT MATCHED BY SOURCE` added in PG 17
- `RETURNING` clause added in PG 17, with `OLD`/`NEW` aliases
- `merge_action()` function in RETURNING
- `DO NOTHING` action (standard extension)
- Cannot target materialized views, foreign tables, or tables with rules
- CTE support in USING clause
- MERGE can appear inside CTEs (DuckDB cannot do this yet)

**pg_ivm (PostgreSQL IVM extension):**
pg_ivm uses AFTER triggers with transition tables. It does NOT use MERGE
internally; it uses separate INSERT/UPDATE/DELETE with transition table data.
This is partly because pg_ivm predates PG15 MERGE support.

### 7.2 Oracle MERGE

Oracle was the first major database to support MERGE (Oracle 9i, 2001).

**Unique Oracle feature -- DELETE clause within WHEN MATCHED:**
```sql
MERGE INTO target USING source ON (condition)
WHEN MATCHED THEN
    UPDATE SET col = val
    DELETE WHERE delete_condition;
```

The DELETE clause in Oracle is a sub-clause of UPDATE: it first updates the
row, then deletes it if the DELETE WHERE condition is met. This is different
from the standard where DELETE is a separate WHEN MATCHED action.

**Important Oracle limitation:** The DELETE clause only affects rows that were
updated by the MERGE. It cannot delete rows that were not matched and updated.

### 7.3 SQL Server MERGE

SQL Server has had MERGE since SQL Server 2008.

**SQL Server-specific features:**
- `WHEN NOT MATCHED BY SOURCE` (SQL Server was the first to support this)
- `OUTPUT` clause (similar to RETURNING)
- `$action` in OUTPUT (returns 'INSERT', 'UPDATE', 'DELETE')

**Known issues:**
- Multiple documented bugs and race conditions in concurrent scenarios
- Microsoft's own guidance has at times recommended against MERGE for
  high-concurrency workloads
- No guaranteed ordering of UPDATE vs INSERT operations

### 7.4 Snowflake MERGE

Snowflake uses MERGE extensively for its **Dynamic Tables** feature, which
is Snowflake's approach to incremental materialized views:

- Dynamic tables support three refresh modes: `AUTO`, `INCREMENTAL`, `FULL`
- In incremental mode, Snowflake creates lightweight streams (change data
  capture) on base tables, then uses MERGE to apply changes
- The system automatically decides between incremental and full refresh
  based on query complexity and cost estimation
- Dynamic tables in incremental mode cannot be downstream from full-refresh
  dynamic tables

**Relevance to OpenIVM:** Snowflake's Dynamic Tables validate the approach of
using MERGE for IVM delta application. Their architecture (streams for CDC +
MERGE for application) is analogous to OpenIVM's (delta tables for CDC +
upsert queries for application).

### 7.5 Cross-System Comparison for IVM

| Feature | DuckDB | PostgreSQL | Oracle | SQL Server | Snowflake |
|---------|--------|-----------|--------|------------|-----------|
| MERGE available | v1.4.0+ | PG15+ | 9i+ | 2008+ | Always |
| NOT MATCHED BY SOURCE | Yes | PG17+ | No | Yes | Yes |
| RETURNING/OUTPUT | Yes | PG17+ | No | Yes (OUTPUT) | No |
| CTE in USING | Yes | Yes | Yes | Yes | Yes |
| MERGE inside CTE | No | Yes | No | Yes (OUTPUT INTO) | No |
| DO NOTHING | Yes | Yes | No | No | No |
| IS NOT DISTINCT FROM in ON | Yes | Yes | No (use DECODE) | No (use ISNULL) | Yes (IS) |
| DELETE as separate WHEN action | Yes | Yes | Only sub-clause of UPDATE | Yes | Yes |

---

## 8. Implementation Roadmap for OpenIVM

### 8.1 Phase 1: AGGREGATE_GROUP Only

Replace `CompileAggregateGroups()` in `src/upsert/openivm_compile_upsert.cpp`
for the non-minmax, non-list-mode case.

**Changes needed:**
1. Generate a single MERGE statement instead of UPDATE + INSERT + DELETE
2. Keep the CTE consolidation logic (it maps directly to the MERGE USING clause)
3. The ON clause uses IS NOT DISTINCT FROM (already used in current code)
4. WHEN MATCHED AND ... = 0 THEN DELETE replaces the separate DELETE statement
5. Keep the `has_minmax` path unchanged (DELETE+recompute)

**Estimated code reduction:** The function shrinks from ~100 lines to ~50 lines.

### 8.2 Phase 2: List Mode

Adapt the MERGE SET clause for LIST<DOUBLE> element-wise operations.
This is a straightforward syntax change within the WHEN MATCHED THEN UPDATE SET.

### 8.3 Phase 3: Verify Companion Row Logic

The companion row insertion (for downstream AGGREGATE_GROUP views) runs
separately from the upsert. Verify that the MERGE-based upsert does not
interfere with companion row generation. The companion logic reads from
`delta_mv` and `mv`, and the MERGE modifies `mv`, so execution order matters.
The companion query should run BEFORE the MERGE (it currently runs before
the upsert in `openivm_upsert.cpp`).

### 8.4 DuckDB Version Gate

Since MERGE requires DuckDB >= 1.4.0, add a version check or feature flag.
If OpenIVM needs to support older DuckDB versions, keep the old code path
behind a conditional.

---

## 9. Concrete Code Sketch

Below is a sketch of how `CompileAggregateGroups()` would look with MERGE
(non-minmax, non-list-mode case):

```cpp
// CTE: consolidate deltas per group (unchanged from current)
string cte = "WITH ivm_cte AS (\n  SELECT ";
for (auto &key : keys) cte += key + ", ";
for (auto &agg : aggregates) {
    cte += "SUM(CASE WHEN " + MULTIPLICITY_COL + " = false THEN -" + agg
        + " ELSE " + agg + " END) AS " + agg + ", ";
}
cte.pop_back(); cte.pop_back(); // trim trailing ", "
cte += "\n  FROM " + delta_name;
if (!ts_filter.empty()) cte += " WHERE " + ts_filter;
cte += "\n  GROUP BY ";
for (auto &key : keys) cte += key + ", ";
cte.pop_back(); cte.pop_back();
cte += "\n)\n";

// MERGE
string merge = cte + "MERGE INTO " + view_name + "\n"
    + "USING ivm_cte AS d\n"
    + "ON (";
for (size_t i = 0; i < keys.size(); i++) {
    if (i > 0) merge += " AND ";
    merge += view_name + "." + keys[i] + " IS NOT DISTINCT FROM d." + keys[i];
}
merge += ")\n";

// WHEN MATCHED AND all aggregates zero -> DELETE
merge += "WHEN MATCHED AND ";
for (size_t i = 0; i < aggregates.size(); i++) {
    if (i > 0) merge += " AND ";
    merge += "(" + view_name + "." + aggregates[i] + " + d." + aggregates[i] + ") = 0";
}
merge += " THEN\n  DELETE\n";

// WHEN MATCHED -> UPDATE
merge += "WHEN MATCHED THEN\n  UPDATE SET ";
for (size_t i = 0; i < aggregates.size(); i++) {
    if (i > 0) merge += ", ";
    merge += aggregates[i] + " = " + view_name + "." + aggregates[i] + " + d." + aggregates[i];
}
merge += "\n";

// WHEN NOT MATCHED -> INSERT
merge += "WHEN NOT MATCHED BY TARGET THEN\n  INSERT (";
for (auto &key : keys) merge += key + ", ";
for (auto &agg : aggregates) merge += agg + ", ";
merge.pop_back(); merge.pop_back();
merge += ")\n  VALUES (";
for (auto &key : keys) merge += "d." + key + ", ";
for (auto &agg : aggregates) merge += "d." + agg + ", ";
merge.pop_back(); merge.pop_back();
merge += ");\n";

return merge;
```

---

## Sources

- [DuckDB MERGE INTO Statement Documentation](https://duckdb.org/docs/stable/sql/statements/merge_into)
- [DuckDB MERGE for SCD Type 2 Guide](https://duckdb.org/docs/stable/guides/sql_features/merge)
- [DuckDB v1.4.0 Announcement](https://duckdb.org/2025/09/16/announcing-duckdb-140)
- [DuckDB MERGE INTO Features Discussion (#19451)](https://github.com/duckdb/duckdb/discussions/19451)
- [DuckDB MERGE Statement Discussion (#4601)](https://github.com/duckdb/duckdb/discussions/4601)
- [DuckDB MERGE Memory Usage Issue (#19958)](https://github.com/duckdb/duckdb/issues/19958)
- [PostgreSQL MERGE Documentation (PG 18)](https://www.postgresql.org/docs/current/sql-merge.html)
- [PostgreSQL 15 MERGE Introduction](https://www.depesz.com/2022/03/31/waiting-for-postgresql-15-add-support-for-merge-sql-command/)
- [Oracle MERGE Documentation](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/MERGE.html)
- [Oracle MERGE DELETE Clause Details](https://oracle-base.com/articles/10g/merge-enhancements-10g)
- [SQL Server MERGE Performance vs INSERT/UPDATE/DELETE](https://www.mssqltips.com/sqlservertip/7590/sql-merge-performance-vs-insert-update-delete/)
- [SQL Server MERGE Performance Comparison](https://www.mssqltips.com/sqlservertip/2651/comparing-performance-for-the-merge-statement-to-select-insert-update-or-delete/)
- [PostgreSQL Upsert: MERGE vs ON CONFLICT](https://www.baeldung.com/sql/postgresql-upsert-merge-insert)
- [Snowflake Dynamic Tables Documentation](https://docs.snowflake.com/en/user-guide/dynamic-tables-about)
- [Snowflake Dynamic Tables Refresh](https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh)
- [Snowflake MERGE Optimization Techniques](https://www.chaosgenius.io/blog/snowflake-merge-statement/)
- [DBSP: Automatic IVM for Rich Query Languages (VLDB)](https://www.vldb.org/pvldb/vol16/p1601-budiu.pdf)
- [DBToaster: Higher-order Delta Processing](http://vldb.org/pvldb/vol5/p968_yanifahmad_vldb2012.pdf)
- [pg_ivm: PostgreSQL IVM Extension](https://github.com/sraoss/pg_ivm)
- [Incremental View Maintenance -- PostgreSQL Wiki](https://wiki.postgresql.org/wiki/Incremental_View_Maintenance)
- [SQL MERGE -- Wikipedia](https://en.wikipedia.org/wiki/Merge_(SQL))
