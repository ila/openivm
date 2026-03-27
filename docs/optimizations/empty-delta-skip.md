# Empty Delta Skip

## Behavior

When **all** delta tables for a materialized view are empty, OpenIVM skips the entire
refresh cycle. No query planning, LPTS computation, or SQL generation is performed.

## Condition

This optimization applies only when `ivm_cascade_refresh = 'off'` (the default for
manual refresh).

```sql
SET ivm_cascade_refresh = 'off';

-- If no base table has changed since the last refresh, this is a no-op:
REFRESH MATERIALIZED VIEW my_mv;
```

During cascade mode (`ivm_cascade_refresh = 'on'`), the skip is disabled because an
upstream view's refresh may populate delta tables that were empty at check time.

## What Is Avoided

| Step | Skipped |
|---|---|
| Delta query planning | Yes |
| LPTS timestamp bookkeeping | Yes |
| SQL generation for INSERT/DELETE/MERGE | Yes |
| Downstream cascade trigger | Yes |

## When It Does Not Apply

- Cascade refresh is active (`ivm_cascade_refresh = 'on'`)
- At least one delta table contains rows for the current timestamp window
