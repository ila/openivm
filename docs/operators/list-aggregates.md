# List aggregates

## Example

```sql
CREATE TABLE measurements (sensor VARCHAR, readings LIST(FLOAT));
INSERT INTO measurements VALUES ('A', [1.0, 2.0, 3.0]), ('A', [4.0, 5.0, 6.0]);

CREATE MATERIALIZED VIEW sensor_totals AS
    SELECT sensor, LIST(readings) AS all_readings
    FROM measurements GROUP BY sensor;

INSERT INTO measurements VALUES ('A', [10.0, 20.0, 30.0]);
PRAGMA ivm('sensor_totals');
```

## How IVM handles it

**Algebraic rule:**

```
new_MV[key].list = elementwise_add(old_MV[key].list, delta[key].list)
```

When LIST-typed aggregate columns are detected, OpenIVM switches to element-wise list operations. Instead of scalar addition, it uses `list_transform` + `list_zip` to add corresponding elements. Deletions negate each element with `list_transform(col, lambda x: -x)`.

## Compiled SQL

### IVM query (delta propagation)

```sql
-- Aggregate delta rows per group, preserving multiplicity
-- The LIST aggregate collects all readings for each sensor
WITH scan_0 (...) AS (
    SELECT sensor, readings, _duckdb_ivm_multiplicity
    FROM delta_measurements
    WHERE _duckdb_ivm_timestamp >= '{ts}'::TIMESTAMP
),
aggregate_1 (...) AS (
    SELECT sensor, LIST(readings) AS all_readings, _duckdb_ivm_multiplicity
    FROM scan_0
    GROUP BY sensor, _duckdb_ivm_multiplicity
)
INSERT INTO delta_sensor_totals (sensor, all_readings, _duckdb_ivm_multiplicity)
SELECT sensor, all_readings, _duckdb_ivm_multiplicity FROM aggregate_1;
```

### Upsert (CTE consolidation + MERGE)

```sql
-- Consolidate deltas: negate deleted lists element-wise, then reduce via element-wise addition
-- list_transform negates each element for deletions (multiplicity = false)
-- list_reduce folds multiple lists into one by adding corresponding elements
WITH ivm_cte AS (
    SELECT sensor,
        list_reduce(list(
            CASE WHEN _duckdb_ivm_multiplicity = false
                THEN list_transform(all_readings, lambda x: -x)
                ELSE all_readings
            END
        ), lambda a, b: list_transform(
            list_zip(a, b), lambda x: x[1] + x[2]
        )) AS all_readings
    FROM delta_sensor_totals
    GROUP BY sensor
)
-- MERGE: add corresponding elements of existing and delta lists
MERGE INTO sensor_totals v USING ivm_cte d
ON v.sensor IS NOT DISTINCT FROM d.sensor
-- Existing group: element-wise addition of old and delta lists
WHEN MATCHED THEN UPDATE SET
    all_readings = list_transform(
        list_zip(
            COALESCE(v.all_readings, [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]),
            d.all_readings),
        lambda x: x[1] + x[2])
-- New group: insert the delta as the initial value
WHEN NOT MATCHED THEN INSERT (sensor, all_readings) VALUES (d.sensor, d.all_readings);

-- Delete groups where all elements sum to zero (group no longer exists)
DELETE FROM sensor_totals
WHERE list_reduce(all_readings, lambda a, b: a + b) = 0.0;
```

## Limitations
- Only works with numeric list elements (element-wise negation and addition require arithmetic).
