# Companion Rows

## Problem

When materialized views are chained (MV2 depends on MV1), downstream views need to see
old-to-new state transitions, not just raw deltas. Without companion rows, a downstream
`COUNT(*)` or `SUM()` produces incorrect results because it cannot distinguish between
"a new group appeared" and "an existing group changed value."

## Solution by View Type

### AGGREGATE_GROUP Views

For each key that appears in the incoming delta, emit a **zero-valued false row**
representing the old state of that group.

```
delta contains:  (key=A, val=10, mul=true)   -- new value
companion adds:  (key=A, val=10, mul=false)   -- cancels old contribution
```

A downstream `COUNT(*)` over group keys now sees `+1 - 1 = 0` for existing keys
(no net change in group count), while genuinely new keys produce `+1` with no
companion cancellation.

The companion row's value matches the **pre-upsert** state of the group in the
materialized view. After the MERGE upsert, the view stores the absolute new value;
the companion row ensures the delta stream is self-consistent for downstream consumers.

### SIMPLE_AGGREGATE and PROJECTION Views

These views use a snapshot-based approach:

1. **Pre-upsert snapshot:** Before applying deltas, copy the current view state into
   a temp table.
2. **Apply deltas:** Execute the INSERT/DELETE/MERGE into the materialized view.
3. **Post-upsert replacement:** Emit the full old state as deletions and the full new
   state as insertions into the downstream delta table.

```sql
-- Step 1: snapshot
CREATE TEMP TABLE _snap AS SELECT * FROM mv;

-- Step 2: apply deltas to mv
...

-- Step 3: emit companion deltas
INSERT INTO mv_delta (col1, ..., _duckdb_ivm_multiplicity)
    SELECT col1, ..., false FROM _snap       -- old state (deletions)
    UNION ALL
    SELECT col1, ..., true  FROM mv;         -- new state (insertions)
```

This absolute-state replacement guarantees downstream views always receive a correct
transition regardless of how many intermediate changes occurred.

## Effect on Downstream Views

| Downstream operation | Without companions | With companions |
|---|---|---|
| `COUNT(*)` over groups | Overcounts (treats value changes as new groups) | Correct (old group cancels) |
| `SUM(x)` over groups | Adds delta on top of stale base | Correct (subtracts old, adds new) |
| Projection chain | Missing deletes for changed rows | Correct (old row deleted, new row inserted) |
