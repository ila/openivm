# OpenIVM Refresh Hooks

Refresh hooks allow extensions and users to register custom SQL that runs on materialized view refresh. This enables post-processing, notifications, cache invalidation, or completely replacing the IVM refresh with custom logic.

## Hook Table

Hooks are stored in the `_duckdb_ivm_refresh_hooks` system table:

```sql
CREATE TABLE _duckdb_ivm_refresh_hooks(
    view_name VARCHAR PRIMARY KEY,   -- MV name
    hook_sql  VARCHAR NOT NULL,      -- SQL to execute
    mode      VARCHAR NOT NULL       -- 'before', 'after', or 'replace'
);
```

## Modes

| Mode | Behavior |
|------|----------|
| `before` | Execute `hook_sql` BEFORE `PRAGMA ivm()` runs |
| `after` | Execute `hook_sql` AFTER `PRAGMA ivm()` completes |
| `replace` | Execute `hook_sql` INSTEAD of `PRAGMA ivm()` — IVM is skipped |

## Examples

### After-hook: Log every refresh
```sql
CREATE TABLE refresh_log(view_name VARCHAR, refreshed_at TIMESTAMP);

INSERT INTO _duckdb_ivm_refresh_hooks VALUES(
    'my_view',
    'INSERT INTO refresh_log VALUES(''my_view'', now()::TIMESTAMP)',
    'after'
);
```

### Replace-hook: Custom flush logic (SIDRA)
```sql
-- Register SIDRA's flush as the refresh action for a centralized MV
INSERT INTO _duckdb_ivm_refresh_hooks VALUES(
    'daily_steps',
    'PRAGMA flush(''daily_steps'', ''duckdb'')',
    'replace'
);
```

### Before-hook: Validate data before refresh
```sql
INSERT INTO _duckdb_ivm_refresh_hooks VALUES(
    'my_view',
    'SELECT CASE WHEN COUNT(*) = 0 THEN error(''No delta data'') END FROM delta_my_table',
    'before'
);
```

### Building custom hooks with `execute=false`

You can use OpenIVM's `execute` setting to inspect the compiled IVM SQL, then paste it into a hook with modifications:

```sql
-- Step 1: See what IVM would execute
SET execute = false;
PRAGMA ivm('my_view');
-- This prints the compiled SQL without executing it

-- Step 2: Copy the SQL, modify as needed, register as hook
SET execute = true;
INSERT INTO _duckdb_ivm_refresh_hooks VALUES(
    'my_view',
    '<paste modified SQL here>',
    'replace'
);
```

## Removing a Hook

```sql
DELETE FROM _duckdb_ivm_refresh_hooks WHERE view_name = 'my_view';
```

After removal, `PRAGMA ivm()` reverts to default IVM behavior.

## Interaction with Refresh Daemon

Hooks are respected by both:
- **Manual refresh**: `PRAGMA ivm('view_name')`
- **Automatic refresh**: OpenIVM's background daemon (when `REFRESH EVERY` is configured)

The daemon checks for hooks before each scheduled refresh.

## Use Cases

- **SIDRA**: Register `PRAGMA flush()` as a replace-hook for centralized MVs
- **Audit logging**: Insert into a log table after each refresh
- **Cache invalidation**: Clear downstream caches when a view refreshes
- **Data export**: `COPY ... TO 's3://...'` after refresh
- **Alerting**: Check conditions and trigger notifications
- **Custom IVM**: Replace the default IVM with custom delta logic
