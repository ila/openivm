# Metadata columns

OpenIVM adds hidden columns to materialized views and delta tables. These columns are internal — the user never sees them directly. Any column whose name starts with `openivm_` is treated as internal (`IncrementalTableNames::IsInternalColumn`). The physical MV table is named `openivm_data_<internal_key>`; the user-facing SQL view `<view_name>` reads a published table that contains only the visible columns (see [The user-facing view](#the-user-facing-view)).

New views use an internal key derived from their catalog, schema, and name: `__openivm_mv_` followed by the hex-encoded lowercase bytes of each part, each terminated by `_` (`RefreshMetadata::AllocateViewKey`). The key is stored in `openivm_views.view_name`; the SQL name is in `view_sql_name`. Views created by earlier versions keep their stored key. The examples below write `<view_name>` for the key.

## Delta table columns

Delta tables have two metadata columns appended to the base table schema. See [Delta Tables](delta-tables.md) for the full lifecycle.

| Column | Type | Description |
|---|---|---|
| `openivm_multiplicity` | `INTEGER` | Signed Z-set weight: `+1` = inserted row, `-1` = deleted row. Multiplicity > 1 is encoded by repeated rows. |
| `openivm_timestamp` | `TIMESTAMP` | When the change was recorded, in UTC. |

**Multiplicity** encodes the sign and count of each delta row as a signed integer (Z-set weight). INSERT writes `+1`, DELETE writes `-1`, UPDATE writes two rows (old=`-1`, new=`+1`). During upsert, aggregates fold insertions and deletions into a net change with `SUM(openivm_multiplicity * value)`; projection views compute `SUM(openivm_multiplicity)` per distinct tuple to determine the net inserts/deletes. Joins multiply weights across leaves and apply a Möbius inclusion-exclusion sign — see [`inner-join.md`](../operators/inner-join.md).

**Timestamp** is set to the current UTC time when the delta row is written. During refresh, only rows where `openivm_timestamp >= last_update` are read (where `last_update` is the per-(view,table) cursor in `openivm_delta_tables` — see [delta-tables.md](delta-tables.md)). This lets multiple MVs share a single delta table and each consume only the changes it hasn't processed yet.

## MV hidden columns

These columns live on the physical data table (`openivm_data_<view_name>`) and are hidden from the user via the view.

### `openivm_left_key` and `openivm_right_key`

Added when a join refresh path needs key-based partial recompute. `openivm_left_key` stores the preserved-side key for LEFT/RIGHT JOINs. FULL OUTER JOIN projection views can also store `openivm_right_key` so either side's changed key can delete and reinsert the affected rows. See [Left Join](../operators/left-join.md) and [Full Outer Join](../operators/full-outer-join.md).

```sql
-- Original
SELECT c.name, o.amount
FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;

-- Internal (stored in openivm_data_customer_orders)
SELECT c.name, o.amount, c.id AS openivm_left_key
FROM customers c LEFT JOIN orders o ON c.id = o.customer_id;
```

The type matches the join key's type.

### Aggregate helper columns

Added when an aggregate needs decomposed state that can be maintained incrementally:

| Hidden column | Source | Updated via |
|---|---|---|
| `openivm_sum_<alias>` | `SUM(x)` for AVG/STDDEV/VARIANCE | MERGE adds delta sum to existing value |
| `openivm_count_<alias>` | `COUNT(x)` for AVG/STDDEV/VARIANCE | MERGE adds delta count to existing value |
| `openivm_sum_sq_<alias>` | `SUM(x*x)` for STDDEV sample | MERGE adds delta sum-of-squares |
| `openivm_var_sq_<alias>` | `SUM(x*x)` for VARIANCE sample | MERGE adds delta sum-of-squares |
| `openivm_sum_sqp_<alias>` | `SUM(x*x)` for STDDEV population | MERGE adds delta sum-of-squares |
| `openivm_var_sqp_<alias>` | `SUM(x*x)` for VARIANCE population | MERGE adds delta sum-of-squares |
| `openivm_count_star` | Hidden `COUNT(*)` for some grouped aggregates | Tracks group cardinality so empty groups can be deleted safely |
| `openivm_nonnull_sum_count_<n>` | `COUNT(x)` for a visible `SUM(x)` output at projection index `n` | Lets refresh return `NULL` when a group has no remaining non-NULL inputs |

User-visible AVG, STDDEV, and VARIANCE columns are recomputed from the merged helper columns.

```sql
-- Original
SELECT region, AVG(amount) AS avg_amount FROM sales GROUP BY region;

-- Internal (stored in openivm_data_sales_summary)
SELECT region,
    SUM(amount) AS openivm_sum_avg_amount,
    COUNT(amount) AS openivm_count_avg_amount,
    AVG(amount) AS avg_amount
FROM sales GROUP BY region;
```

### `openivm_distinct_count`

Added when the view uses `SELECT DISTINCT`, which the parser rewrites to `GROUP BY` + a hidden `COUNT(*)`. See [Distinct](../operators/distinct.md).

### Other hidden columns

| Hidden column | Added when |
|---|---|
| `openivm_group_key_<n>` | A GROUP BY key is not part of the visible output; kept so affected groups can still be identified. |
| `openivm_sort_value_<n>` | An `ORDER BY` expression of a top-k view is not a visible column. |
| `openivm_running_<n>`, `openivm_rows_position_<n>` | Running window aggregates need their input values and `ROWS` peer positions as maintenance state. |
| `openivm_ordinal` | Present only in the published table (`openivm_visible_<view_name>`); records the published row order. |

### `openivm_match_count` and `openivm_right_match_count`

Added by outer-join aggregate maintenance. These helper counts track whether a preserved-side row currently has matches, so MERGE refresh can handle NULL-padding transitions. FULL OUTER JOIN paths may keep counts for both sides.

## The user-facing view

The physical table holding MV data is named `openivm_data_<view_name>`. Its visible rows are published into a separate table, `openivm_visible_<view_name>`, which stores the *published query* (saved in `openivm_views.published_query`): the data table without internal columns, with the HAVING predicate and any `ORDER BY`/`LIMIT` applied, plus an `openivm_ordinal` column. The user-facing SQL view reads that table:

```sql
-- Published query (stored in openivm_views.published_query)
SELECT * EXCLUDE (openivm_sum_avg_amount, openivm_count_avg_amount, openivm_left_key),
       CAST(0 AS BIGINT) AS openivm_ordinal
FROM openivm_data_mv_name;

-- User-facing view
CREATE VIEW mv_name AS SELECT * EXCLUDE (openivm_ordinal) FROM openivm_visible_mv_name;
```

For top-k views, `openivm_ordinal` is a `ROW_NUMBER()` over the view's ordering and the user-facing view adds `ORDER BY openivm_ordinal`.

Upsert, delta tracking, and MERGE target the data table. After each refresh, OpenIVM recomputes the published query for the affected rows, applies only the changed rows to `openivm_visible_<view_name>`, and records them with signed multiplicities in `openivm_delta_openivm_visible_<view_name>` (`BuildPublishViewSQL` in `src/core/published_view.cpp`). Chained views read this visible boundary, so a parent's helper columns and HAVING/LIMIT threshold crossings are reflected correctly downstream.
