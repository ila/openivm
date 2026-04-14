# Indexing

## Automatic ART Index (AGGREGATE_GROUP Views)

At materialized view creation time, OpenIVM creates an ART index on the `GROUP BY`
columns of `AGGREGATE_GROUP` views.

```sql
CREATE MATERIALIZED VIEW mv AS
    SELECT key, SUM(val) FROM t GROUP BY key;

-- OpenIVM automatically creates:
--   ART index on mv(key)
```

The index enforces uniqueness on the GROUP BY key combination. DuckDB's `MERGE INTO`
statement uses hash joins internally for key matching, so the ART index primarily serves
as a correctness guard rather than a lookup accelerator. No user action is required.

**DuckLake tables:** ART index creation is skipped for DuckLake-backed views because
DuckLake does not support DuckDB-native index types. Group column identification falls
back to metadata stored in `_duckdb_ivm_views`.

## Zone Maps on `_duckdb_ivm_timestamp`

Every delta table includes a `_duckdb_ivm_timestamp` column that records when each
delta row was produced. DuckDB's built-in zone maps (min/max metadata per row group)
enable efficient filtering on this column.

During refresh, OpenIVM filters deltas by timestamp range:

```sql
WHERE _duckdb_ivm_timestamp > last_refresh_ts
  AND _duckdb_ivm_timestamp <= current_ts
```

Zone maps allow DuckDB to skip row groups whose timestamp range falls entirely outside
the filter window. This is especially effective when delta tables accumulate rows across
many refresh cycles.

## Summary

| Index type | Target | Created on | Purpose |
|---|---|---|---|
| ART index | GROUP BY columns | MV creation | Fast MERGE key lookup |
| Zone maps | `_duckdb_ivm_timestamp` | Automatic (DuckDB) | Delta timestamp filtering |
