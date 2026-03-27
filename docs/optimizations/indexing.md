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

The index is used by the `MERGE` (upsert) statement during refresh to perform fast
key lookups. This avoids a full table scan of the materialized view on every delta
application.

No user action is required. The index is created and maintained automatically.

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
