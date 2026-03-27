# List aggregates

## Example

```sql
CREATE TABLE measurements (sensor VARCHAR, readings LIST(FLOAT));
INSERT INTO measurements VALUES ('A', [1.0, 2.0, 3.0]), ('A', [4.0, 5.0, 6.0]);

CREATE MATERIALIZED VIEW sensor_totals AS
    SELECT sensor, LIST(readings) AS all_readings
    FROM measurements GROUP BY sensor;
```

## How IVM handles it

When LIST-typed aggregate columns are detected, OpenIVM switches to element-wise list operations. Instead of scalar addition, it uses `list_transform(list_zip(v, d), lambda x: x[1] + x[2])` to add corresponding elements. Deletions negate each element with `list_transform(col, lambda x: -x)`.

## Compiled SQL patterns

### CTE consolidation (grouped)

```sql
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
```

### MERGE update

```sql
WHEN MATCHED THEN UPDATE SET
    all_readings = list_transform(
        list_zip(v.all_readings, d.all_readings),
        lambda x: x[1] + x[2])
```

A hardcoded 64-element zero list is used as the COALESCE default for NULL lists.
