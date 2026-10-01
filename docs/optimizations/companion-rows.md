# Companion Rows

## Problem

When materialized views are chained (MV2 depends on MV1), downstream views need to see
old-to-new state transitions, not just raw deltas. Without companion rows, a downstream
`COUNT(*)` or `SUM()` produces incorrect results because it cannot distinguish between
"a new group appeared" and "an existing group changed value."

### Example:

Suppose MV1 is `SELECT region, SUM(amount) AS total FROM sales GROUP BY region`, and MV2 is `SELECT COUNT(*) AS num_regions FROM mv1`.

MV1 currently has `{(US, 100), (EU, 200)}`, so MV2 = `{num_regions: 2}`.

Now a new sale is inserted: `INSERT INTO sales VALUES ('US', 50)`. The IVM delta for MV1 is:

```
openivm_delta_mv1:  (region='US', total=50, mul=+1)
```

MV1 is updated to `(US, 150)`. But the downstream MV2 sees one new `+1` row and increments: `num_regions = 2 + 1 = 3`. **Wrong** — the US region already existed; the count should still be 2.

The problem: the delta says "here's a change for US" but doesn't say "US was already there." The downstream view has no way to know whether this is a new group or an update to an existing one. Companion rows solve this by emitting a canceling `-1` row for existing groups.

## When companion rows are emitted

Native chained MVs consume their parent's published boundary (`openivm_visible_<view>`), whose
signed delta is computed by diffing the parent's old and new visible rows (see
[Refresh pipelines](../refresh/pipelines.md)). Companion rows are written into the MV's own
delta table `openivm_delta_<view>` only when a consumer of that table is registered in
`openivm_delta_tables`, or when `openivm_compile_with_facts` is called with
`force_view_delta_cascade = true`. Otherwise the MV's delta rows are deleted at the end of the
refresh. `SIMPLE_PROJECTION` deltas already carry exact old-to-new changes and get no companion.

## Solution by View Type

### AGGREGATE_GROUP and AGGREGATE_HAVING Views

By default, OpenIVM snapshots the **affected groups** — the distinct group keys in the
incoming delta — before the MERGE upsert, then replaces the relative delta with signed
old/new rows for exactly those groups:

```sql
-- Before the upsert: affected keys and their old rows
CREATE TEMP TABLE openivm_old_affected_sales_summary AS
SELECT DISTINCT region FROM openivm_delta_sales_summary WHERE <ts filter>;
CREATE TEMP TABLE openivm_old_snapshot_sales_summary AS
SELECT openivm_data.* FROM <data table> openivm_data
WHERE EXISTS (SELECT 1 FROM openivm_old_affected_sales_summary openivm_aff
              WHERE openivm_aff.region IS NOT DISTINCT FROM openivm_data.region);

-- (... MERGE upsert runs here ...)

-- After the upsert: old rows as -1, new rows of the affected groups as +1
DELETE FROM openivm_delta_sales_summary WHERE <ts filter>;
INSERT INTO openivm_delta_sales_summary (...) SELECT ..., -1 FROM openivm_old_snapshot_sales_summary openivm_old;
INSERT INTO openivm_delta_sales_summary (...) SELECT ..., 1 FROM <data table> openivm_new
WHERE EXISTS (SELECT 1 FROM openivm_old_affected_sales_summary openivm_aff
              WHERE openivm_aff.region IS NOT DISTINCT FROM openivm_new.region);
DROP TABLE openivm_old_snapshot_sales_summary;
DROP TABLE openivm_old_affected_sales_summary;
```

A downstream `COUNT(*)` over region now sees `-1 (old US) + 1 (new US) = 0` for an existing
group, while a genuinely new key produces only `+1`.

#### Inline variant (`force_view_delta_cascade`)

Callers that execute the refresh program statement by statement cannot keep TEMP TABLEs
between statements. With `force_view_delta_cascade = true`, OpenIVM instead emits an inline
retraction row for each positive delta row whose group already exists in the MV: group keys
are kept, every other column is `NULL`, and the multiplicity is `-1`.

```sql
INSERT INTO openivm_delta_sales_summary (region, total, cnt, openivm_multiplicity)
SELECT d.region, NULL, NULL, -1
FROM openivm_delta_sales_summary d
WHERE d.openivm_multiplicity > 0 AND <ts filter>
  AND EXISTS (SELECT 1 FROM <data table> m WHERE m.region IS NOT DISTINCT FROM d.region);
```

When the MV has the hidden `openivm_count_star` column, a matching `+1` add-back row is also
emitted for negative delta rows whose group survives the refresh. Downstream consumers rely on
`SUM(NULL) = NULL` preservation for the `NULL` columns.

### SIMPLE_AGGREGATE and Recompute Views

`SIMPLE_AGGREGATE` views, and views whose refresh does not produce a relative delta
(`GROUP_RECOMPUTE`, `WINDOW_PARTITION`, `DISTINCT_INCREMENTAL`, `SEMI_ANTI_RECOMPUTE`,
`FULL_REFRESH`, and similar), use a **snapshot-based approach**: the MV is copied before the
upsert, and afterwards the relative delta is replaced by the bag difference between the old
and new state:

```sql
-- Step 1 (pre-companion): snapshot the current MV state before any changes
CREATE OR REPLACE TEMP TABLE openivm_old_emp_names AS SELECT * FROM <data table>;

-- Step 2: IVM query computes delta, upsert applies it
-- (... IVM + upsert runs here ...)

-- Step 3 (post-companion): replace the relative delta with the old/new difference
DELETE FROM openivm_delta_emp_names WHERE 1=1 AND <ts filter>;
INSERT INTO openivm_delta_emp_names (id, name, openivm_multiplicity)
SELECT id, name, -1 FROM (SELECT id, name FROM openivm_old_emp_names
                          EXCEPT ALL SELECT id, name FROM <data table>) openivm_diff
UNION ALL
SELECT id, name, 1 FROM (SELECT id, name FROM <data table>
                         EXCEPT ALL SELECT id, name FROM openivm_old_emp_names) openivm_diff;

DROP TABLE IF EXISTS openivm_old_emp_names;
```

Only rows that actually changed are emitted, so downstream views receive a correct
transition regardless of how many intermediate changes occurred. `GROUP_RECOMPUTE` and
`WINDOW_PARTITION` refreshes that emit their own signed old/new delta skip this step. For
`target_dialect = spark`, `FULL_REFRESH` uses a split-safe variant that emits every old row as
`-1` before the recompute and every new row as `+1` after it, without TEMP TABLEs.

When `openivm_skip_empty_deltas = true`, the MV delta retained for consumers is then compacted
to one net row per tuple, dropping tuples whose weights cancel, so an unchanged result gives
downstream views an empty delta.

## Effect on Downstream Views

| Downstream operation | Without companions | With companions |
|---|---|---|
| `COUNT(*)` over groups | Overcounts (treats value changes as new groups) | Correct (old group cancels) |
| `SUM(x)` over groups | Adds delta on top of stale base | Correct (subtracts old, adds new) |
| Projection chain | Missing deletes for changed rows | Correct (old row deleted, new row inserted) |
