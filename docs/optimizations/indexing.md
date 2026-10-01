# Indexing

## Automatic ART Index (AGGREGATE_GROUP and AGGREGATE_HAVING Views)

At materialized view creation time, OpenIVM creates a `UNIQUE` ART index on the stored
`GROUP BY` columns of `AGGREGATE_GROUP` and `AGGREGATE_HAVING` data tables. The index
is named after the data table with the `openivm_index` suffix.

```sql
CREATE MATERIALIZED VIEW mv AS
    SELECT key, SUM(val) FROM t GROUP BY key;

-- OpenIVM automatically creates:
--   ART index on mv(key)
```

The index enforces uniqueness on the GROUP BY key combination. DuckDB's `MERGE INTO`
statement uses hash joins internally for key matching, so the ART index primarily serves
as a correctness guard rather than a lookup accelerator. Recompute paths on indexed data
tables use an upsert form instead of `DELETE` + re-`INSERT` of the same keys, because
DuckDB's unique index still reports keys deleted earlier in the same transaction.
No user action is required.

**Non-unique GROUP BY keys:** if creating the UNIQUE INDEX fails because the initial
MV data contains duplicate keys (for example, stored `group_columns` that are not unique
in the view output), OpenIVM prints a warning and creates the view without the index.
The MERGE path still works correctly — it just relies on hash matching rather than the
index.

**DuckLake tables:** ART index creation is skipped for DuckLake-backed views because
DuckLake does not support DuckDB-native index types. Group column identification falls
back to metadata stored in `openivm_views`.

**Qualified or non-default locations:** the index is also skipped when the MV's data
table is created with a catalog/schema prefix, i.e. when the MV name is qualified or the
current catalog/schema differs from the metadata catalog's default.

## Zone Maps on `openivm_timestamp`

Every delta table includes a `openivm_timestamp` column that records when each
delta row was produced. DuckDB's built-in zone maps (min/max metadata per row group)
enable efficient filtering on this column.

During refresh, each delta scan is filtered by the consuming view's watermark from
`openivm_delta_tables.last_update`:

```sql
WHERE openivm_timestamp >= last_update
```

After a refresh, `last_update` advances to the delta table's maximum `openivm_timestamp`
plus one microsecond (or the current UTC time when the delta table is empty).

Zone maps allow DuckDB to skip row groups whose timestamp range falls entirely outside
the filter window. This is especially effective when delta tables accumulate rows across
many refresh cycles.

## Summary

| Index type | Target | Created on | Purpose |
|---|---|---|---|
| ART index | GROUP BY columns | MV creation | Unique-key guard for MERGE/upsert |
| Zone maps | `openivm_timestamp` | Automatic (DuckDB) | Delta timestamp filtering |
