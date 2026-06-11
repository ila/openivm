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

## Solution by View Type

### Grouped-aggregate views

For each key that appears in the incoming delta, emit a **zero-valued retraction row**
representing the old state of that group. This tells downstream consumers "the old
contribution from this group is being cancelled."

A downstream `COUNT(*)` over group keys now sees `+1 - 1 = 0` for existing keys
(no net change in group count), while genuinely new keys produce `+1` with no
companion cancellation.

#### Worked example

Suppose `sales_summary` has `(US, total=100, cnt=5)` and the IVM delta adds 50 to the US group:

```sql
-- The IVM query inserts the delta into the delta view table
INSERT INTO openivm_delta_sales_summary VALUES ('US', 50, 2, 1);
```

The companion query inserts a retraction row (multiplicity `-1`) for the existing group:

```sql
-- Record that the 'US' group existed before this refresh.
-- Zero-valued retraction row: cancels old contribution for downstream consumers.
-- Only inserted for groups that already exist in the MV (not for new groups).
INSERT INTO openivm_delta_sales_summary (region, total, cnt, openivm_multiplicity)
SELECT d.region, 0, 0, -1
FROM openivm_delta_sales_summary d
WHERE d.openivm_multiplicity > 0
  AND EXISTS (SELECT 1 FROM sales_summary m
              WHERE m.region IS NOT DISTINCT FROM d.region);
```

After the MERGE upsert updates the MV to `(US, total=150, cnt=7)`, the delta view contains:

| region | total | cnt | mul |
|---|---|---|---|
| US | 50 | 2 | +1 |
| US | 0 | 0 | -1 |

A downstream `COUNT(*)` over region sees: `+1 + (-1) = 0` net change for 'US'. Correct — the group already existed.

### Ungrouped-aggregate and projection views

These views use a **snapshot-based approach** — replace the entire IVM delta with absolute old-to-new transitions:

```sql
-- Step 1 (pre-companion): snapshot the current MV state before any changes
CREATE TEMP TABLE openivm_old_emp_names AS SELECT * FROM emp_names;

-- Step 2: IVM query computes delta, upsert applies it to emp_names
-- (... IVM + upsert runs here ...)

-- Step 3 (post-companion): replace IVM delta with absolute old→new transition
-- Clear the relative IVM delta rows — they served their purpose for this MV
DELETE FROM openivm_delta_emp_names WHERE openivm_timestamp >= '{ts}';

-- Emit old state as deletions: each old row is "removed" for downstream consumers
INSERT INTO openivm_delta_emp_names (id, name, openivm_multiplicity)
SELECT id, name, -1 FROM openivm_old_emp_names;

-- Emit new state as insertions: each new row is "added" for downstream consumers
INSERT INTO openivm_delta_emp_names (id, name, openivm_multiplicity)
SELECT id, name, 1 FROM emp_names;

DROP TABLE openivm_old_emp_names;
```

This absolute-state replacement guarantees downstream views always receive a correct
transition regardless of how many intermediate changes occurred.

## Effect on Downstream Views

| Downstream operation | Without companions | With companions |
|---|---|---|
| `COUNT(*)` over groups | Overcounts (treats value changes as new groups) | Correct (old group cancels) |
| `SUM(x)` over groups | Adds delta on top of stale base | Correct (subtracts old, adds new) |
| Projection chain | Missing deletes for changed rows | Correct (old row deleted, new row inserted) |
