# List-valued aggregates

> Linearity: **NON_LINEAR** (group recompute — list elements aren't summable so deltas can't compose). ([what does this mean?](../internals/linearity.md))

## Example

```sql
CREATE TABLE measurements (sensor VARCHAR, readings FLOAT[]);
INSERT INTO measurements VALUES ('A', [1.0, 2.0, 3.0]), ('A', [4.0, 5.0, 6.0]);

CREATE MATERIALIZED VIEW sensor_readings AS
    SELECT sensor, LIST(readings) AS all_readings
    FROM measurements GROUP BY sensor;

INSERT INTO measurements VALUES ('A', [10.0, 20.0, 30.0]);
PRAGMA refresh('sensor_readings');
-- A | [[1.0, 2.0, 3.0], [4.0, 5.0, 6.0], [10.0, 20.0, 30.0]]
```

## How IVM handles it

`LIST` is not summable: the delta of a group's list cannot be combined with the stored list by
arithmetic. Any view containing a `LIST` aggregate — including `LIST(...) FILTER` and
expressions over `LIST`, such as `list_reduce(list(val), ...)` — is maintained with
**affected-group recompute**, for every delta shape (insert-only included):

1. Find the GROUP BY keys touched by the delta.
2. Recompute those groups from the stored view query.
3. Delete affected groups that no longer exist, and upsert the recomputed rows.

The classifier marks `LIST` views like MIN/MAX views (`has_minmax` metadata), and
`CompileAggregateGroups` (`src/upsert/refresh_compiler.cpp`) routes them to group recompute
because no real MIN/MAX is present.

For `LIST(...) FILTER`, DuckDB's list aggregate keeps NULL elements. Rewriting the filter to `LIST(CASE WHEN p THEN x ELSE NULL END)` would change results. OpenIVM keeps the original SQL for these views.

## Compiled SQL

### IVM query (delta propagation)

The delta query aggregates the delta rows per group as usual; the upsert only uses it to find
the affected group keys.

```sql
WITH scan_0 (...) AS (
    SELECT sensor, readings, openivm_multiplicity
    FROM openivm_delta_measurements
    WHERE openivm_timestamp >= '{ts}'::TIMESTAMP
),
aggregate_1 (...) AS (
    SELECT sensor, LIST(readings) AS all_readings, openivm_multiplicity
    FROM scan_0
    GROUP BY sensor, openivm_multiplicity
)
INSERT INTO openivm_delta_sensor_readings (sensor, all_readings, openivm_multiplicity)
SELECT sensor, all_readings, openivm_multiplicity FROM aggregate_1;
```

### Upsert (affected-group recompute)

```sql
-- Recompute only the groups touched by the delta
CREATE OR REPLACE TEMP TABLE openivm_recompute_sensor_readings AS
SELECT * FROM (
    SELECT sensor, list(readings) AS all_readings, count_star() AS openivm_count_star
    FROM measurements GROUP BY sensor
) openivm_recompute
WHERE EXISTS (
    SELECT 1 FROM (SELECT DISTINCT sensor FROM openivm_delta_sensor_readings
                   WHERE openivm_timestamp > '{ts}'::TIMESTAMP) AS openivm_aff
    WHERE openivm_aff.sensor IS NOT DISTINCT FROM openivm_recompute.sensor
);

-- Delete affected groups that disappeared (and NULL-keyed affected groups)
DELETE FROM openivm_data_sensor_readings AS openivm_tgt
WHERE EXISTS (SELECT 1 FROM (...affected keys...) AS openivm_aff
              WHERE openivm_aff.sensor IS NOT DISTINCT FROM openivm_tgt.sensor)
  AND ((openivm_tgt.sensor IS NULL)
    OR NOT EXISTS (SELECT 1 FROM openivm_recompute_sensor_readings AS openivm_keep
                   WHERE openivm_keep.sensor IS NOT DISTINCT FROM openivm_tgt.sensor));

-- Upsert the recomputed groups
INSERT OR REPLACE INTO openivm_data_sensor_readings
SELECT * FROM openivm_recompute_sensor_readings;

DROP TABLE IF EXISTS openivm_recompute_sensor_readings;
```

## Limitations
- Refresh cost is proportional to the size of the affected groups, not to the delta size.
- Filtered `LIST` aggregates keep the original SQL to preserve DuckDB's NULL-element semantics.
- Ungrouped views with a `LIST` aggregate recompute their single row.
