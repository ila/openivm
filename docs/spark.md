# Spark support

OpenIVM runs inside DuckDB. Spark support means three things: it can read Spark-dialect `CREATE MATERIALIZED VIEW` bodies, it can compile a view's refresh program as Spark SQL without executing it, and it registers a Spark-compatible `add_months` scalar. OpenIVM does not run Spark, connect to a Spark cluster, or apply the compiled statements for you.

## Input dialect

`openivm_input_dialect` (default `duckdb`) sets the dialect of incoming `CREATE MATERIALIZED VIEW` bodies. Valid values are `duckdb` and `spark`; any other value is rejected by `SET`.

With `spark`, the body is translated to DuckDB syntax (via LPTS `NormalizeInputSqlToDuckDB`) before parsing. This covers backtick identifiers and Spark time-travel clauses. See [parser](internals/parser.md#input-dialect).

```sql
SET openivm_input_dialect='spark';

CREATE MATERIALIZED VIEW sales_by_region AS
    SELECT region, SUM(amount) AS total
    FROM `sales` VERSION AS OF 366
    GROUP BY region;
```

### `VERSION AS OF` and `TIMESTAMP AS OF`

Both pins are accepted, each on its own relation, with or without an alias, in joins and in CTEs:

```sql
SELECT c.region, SUM(o.amount) AS total
FROM orders VERSION AS OF 366 o
JOIN customers VERSION AS OF 12 c ON o.c_id = c.c_id
GROUP BY c.region;

SELECT c_id, SUM(amount) FROM orders TIMESTAMP AS OF '2024-01-01 00:00:00' GROUP BY c_id;
```

(Tested in `test/sql/time_travel.test`.) Behaviour and limits:

- The pin is stored in the view SQL in DuckDB spelling (`orders AT (VERSION => 366)`), so it survives restarts. When output is compiled for Spark it is rendered back as `VERSION AS OF n`.
- Under the default `duckdb` input dialect, `VERSION AS OF` is a syntax error. It is not reinterpreted.
- A view that pins the same relation ambiguously is refused with `NotImplementedException` instead of guessing a snapshot. This covers the same relation at two different snapshots, and the same relation scanned both pinned and unpinned. Repeated scans that share one pin are fine.
- If a pin cannot be attached to the same relation and alias after translation, creation fails rather than reading a different snapshot.
- For a target dialect with no verified time-travel syntax, compilation raises `LPTS_UNSUPPORTED_TIME_TRAVEL`.
- Sources of a compiled view are plain in-memory stand-in tables, so the local copy holds no snapshots. The pin is carried in the compiled SQL for the target engine. Native DuckLake time travel is described in [DuckLake](ducklake.md).

Details are in [parser: time-travel pins](internals/parser.md#time-travel-pins).

## Compile-only output

`openivm_compile_with_facts(view_name, facts_json)` compiles a view's refresh program and returns it as rows without executing it. For Spark, set `target_dialect` to `spark`:

```sql
SELECT stmt_order, stmt_kind, sql
FROM openivm_compile_with_facts(
    'sales_by_region',
    '{"target_dialect":"spark","compile_only":true}'
)
ORDER BY stmt_order;
```

Result columns: `refresh_type` (INTEGER), `refresh_type_name` (VARCHAR), `stmt_order` (INTEGER), `stmt_kind` (VARCHAR), `sql` (VARCHAR). There is one row per top-level statement. `stmt_kind` is `meta_pre`, `data` or `meta_post`. The `data` rows hold the refresh statements. `refresh_type` and `refresh_type_name` repeat the view's refresh type on every row (for example `2` and `SIMPLE_PROJECTION`).

Facts JSON fields, as parsed in `src/compile_facts.cpp`:

| Field | Meaning |
|---|---|
| `target_dialect` | **Required.** `spark`, `duckdb` or `ducklake`; anything else is an error. |
| `schema_version` | Integer; the current version is 2. |
| `compile_only` | Boolean, default `false`. |
| `force_view_delta_cascade` | Boolean, default `false`. |
| `assume_insert_only` | Boolean, default `false`. Set only if the caller has proven the batch is append-only. With `compile_only` it re-enables the insert-only fast paths. |
| `delta_shape` | Object mapping source table to a shape string. |
| `running_window_incremental` | Boolean, default `false`. |
| `emit_spark_hints` | Boolean, default `false`; see below. |
| `fk_relations` | Array of `{child_table, child_columns, parent_table, parent_columns, rely}`. Only entries with `rely: true` and matching column counts are used. |

Unknown fields are ignored. The facts apply to that one call only.

Spark output observed in `test/sql/compile_spark_dialect_hardening.test`: no DuckDB `::` casts (`CAST(...)` instead), `current_timestamp()`, `<=>` for null-safe equality, backtick-quoted identifiers, and `DECIMAL(38,0)` where a provably bounded `HUGEINT` appears. Those tests check particular view shapes. They are not a promise that every view or SQL construct compiles to Spark, and the compiled SQL is not executed against Spark by OpenIVM's tests.

## `openivm_emit_spark_hints`

Spark optimizer hints are off by default. With hints off, Spark output is byte-identical to output with `"emit_spark_hints":false`.

Enable them for one compile call:

```sql
SELECT sql
FROM openivm_compile_with_facts(
    'sph_hint_mv',
    '{"target_dialect":"spark","compile_only":true,"emit_spark_hints":true}'
)
WHERE stmt_kind = 'data';
```

or for the session:

```sql
SET openivm_emit_spark_hints = true;
```

Semantics, from `src/upsert/refresh_sql.cpp`:

- Hints are emitted only when `target_dialect` is `spark`. They are ignored for `duckdb` and `ducklake`.
- Hints are emitted if either the `emit_spark_hints` fact or the `openivm_emit_spark_hints` setting is true.
- The only hint verified by tests is `/*+ BROADCAST(...) */`, for a two-table inner-join view whose refresh SQL reads a delta table (`openivm_delta_<table>`). The test checks that the hint and the delta table both appear. It does not pin down which relation is broadcast. Which other hints LPTS emits is decided inside LPTS and is not covered here or by an OpenIVM test, so do not rely on any others.
- Hints are optimizer advice for Spark. They do not change query results.

## `add_months(DATE, INTEGER)`

Loading the extension registers the scalar `add_months(DATE, INTEGER)`, implemented in LPTS (`spark_scalar_functions`). It lets Spark-style view bodies that call `add_months` resolve in DuckDB and be maintained incrementally.

```sql
SELECT add_months(DATE '2015-01-31', 1);   -- 2015-02-28
SELECT add_months(DATE '2015-02-28', 1);   -- 2015-03-28

CREATE MATERIALIZED VIEW am_mv AS
    SELECT id, d, n, add_months(d, n) AS shifted FROM am_item;
```

(Both literals are checked in `test/sql/spark_add_months.test`. That test also shows a view using `add_months` compiled as a `SIMPLE_PROJECTION` delta rather than demoted to a full refresh, and equal to a full recomputation after inserts, updates and deletes.)

Limits:

- Only the `(DATE, INTEGER)` signature is registered. No other overloads (for example `TIMESTAMP` or `VARCHAR` inputs) are documented or tested here.
- Month-end results are shown only by the examples above, and the same test also covers `2016-02-29 + 12` months = `2017-02-28` and `2015-04-30 + 1` = `2015-05-30`. This page does not describe other edge cases (such as `NULL` or negative months). Exhaustive scalar tests live in the LPTS repository, not in OpenIVM.
- The test above runs on DuckDB. It does not show how the call is rendered in Spark output.
