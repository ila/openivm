# Metadata columns

OpenIVM adds hidden columns to materialized views and delta tables. These columns are internal — the user never sees them directly. The physical MV table is named `_ivm_data_<view_name>`, and a SQL view named `<view_name>` is created on top using `EXCLUDE` to hide them.

## Delta table columns

Delta tables have two metadata columns appended to the base table schema. See [Delta Tables](delta-tables.md) for the full lifecycle.

| Column | Type | Description |
|---|---|---|
| `_duckdb_ivm_multiplicity` | `BOOLEAN` | `true` = inserted row, `false` = deleted row. |
| `_duckdb_ivm_timestamp` | `TIMESTAMP` | When the change was recorded. Defaults to `now()`. |

**Multiplicity** encodes the sign of each delta row. INSERT writes `true`, DELETE writes `false`, UPDATE writes two rows (old=`false`, new=`true`). During upsert, aggregates use `CASE WHEN mul = false THEN -value ELSE value END` to fold insertions and deletions into a net change. Projection views compute `SUM(CASE WHEN mul THEN 1 ELSE -1 END)` per distinct tuple.

**Timestamp** is set to `now()::timestamp` when the delta row is written. During refresh, only rows where `_duckdb_ivm_timestamp >= last_refresh_timestamp` are read. This lets multiple MVs share a single delta table and each consume only the changes it hasn't processed yet.

## MV hidden columns

These columns live on the physical data table (`_ivm_data_<view_name>`) and are hidden from the user via the view.

### `_ivm_left_key`

Added when the view contains a LEFT or RIGHT JOIN. Stores the preserved-side join key so the upsert can do partial recompute — delete all MV rows for affected keys and re-insert from the original query filtered to those keys. See [Left Join](../operators/left-join.md) for the upsert strategy.

```sql
-- Original
SELECT c.name, o.amount
FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;

-- Internal (stored in _ivm_data_customer_orders)
SELECT c.name, o.amount, c.id AS _ivm_left_key
FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;
```

The type matches the join key's type.

### `_ivm_sum_*` and `_ivm_count_*`

Added when the view uses `AVG()`. The parser decomposes each `AVG(x) AS alias` into two hidden columns that can be maintained incrementally:

| Hidden column | Source | Updated via |
|---|---|---|
| `_ivm_sum_<alias>` | `SUM(x)` | MERGE adds delta sum to existing value |
| `_ivm_count_<alias>` | `COUNT(x)` | MERGE adds delta count to existing value |

The user-visible `AVG` column is recomputed after each MERGE as `_ivm_sum / NULLIF(_ivm_count, 0)`.

```sql
-- Original
SELECT region, AVG(amount) AS avg_amount FROM sales GROUP BY region;

-- Internal (stored in _ivm_data_sales_summary)
SELECT region,
    SUM(amount) AS _ivm_sum_avg_amount,
    COUNT(amount) AS _ivm_count_avg_amount,
    AVG(amount) AS avg_amount
FROM sales GROUP BY region;
```

### `_ivm_distinct_count`

Added when the view uses `SELECT DISTINCT`, which the parser rewrites to `GROUP BY` + a hidden `COUNT(*)`. See [Distinct](../operators/distinct.md).

## The user-facing view

The physical table holding MV data is named `_ivm_data_<view_name>`. Users interact with a SQL view that hides internal columns:

```sql
-- No internal columns
CREATE VIEW mv_name AS SELECT * FROM _ivm_data_mv_name;

-- With internal columns (e.g. AVG decomposition + left join key)
CREATE VIEW mv_name AS
    SELECT * EXCLUDE (_ivm_sum_avg_amount, _ivm_count_avg_amount, _ivm_left_key)
    FROM _ivm_data_mv_name;
```

All IVM operations (upsert, delta tracking, MERGE) target the data table, never the view. The view is purely cosmetic.
