# OpenIVM Refresh Hooks

Refresh hooks allow extensions and users to register custom SQL that runs on materialized view refresh. This enables post-processing, notifications, cache invalidation, or completely replacing the IVM refresh with custom logic.

## Hook Table

Hooks are stored in the `openivm_refresh_hooks` system table:

```sql
CREATE TABLE openivm_refresh_hooks(
    view_name VARCHAR PRIMARY KEY,           -- internal MV key (openivm_views.view_name)
    hook_sql  VARCHAR NOT NULL,              -- SQL to execute
    mode      VARCHAR NOT NULL DEFAULT 'after' -- 'before', 'after', or 'replace'
);
```

The table is created with the first materialized view. `view_name` is the MV's internal
storage key, not its SQL name: `openivm_views.view_name` holds keys such as
`__openivm_mv_<hex catalog>_<hex schema>_<hex name>_`, and the SQL name is stored in
`openivm_views.view_sql_name`. Look the key up when registering a hook:

```sql
SELECT view_name FROM openivm_views WHERE view_sql_name = 'my_view';
```

The examples below register hooks by selecting the key this way. A hook row whose
`view_name` is the plain SQL name does not match any MV and is never run.

## Modes

| Mode | Behavior |
|------|----------|
| `before` | Execute `hook_sql` BEFORE the view's IVM refresh runs |
| `after` | Execute `hook_sql` AFTER the view's IVM refresh completes |
| `replace` | Execute `hook_sql` INSTEAD of the view's IVM refresh — IVM is skipped |

## Execution semantics

- **Empty deltas:** with `openivm_skip_empty_deltas = true` (the default), a hook-bearing
  view whose source deltas are all empty is skipped entirely — its hook does not run.
  Cascade traversal continues to dependent views.
- **Errors:** in `PRAGMA refresh`, a failing `before` or `replace` hook prints a warning and
  the refresh continues. In `PRAGMA refresh_pipeline`, any hook failure stops the run.
- **After-hooks:** when every write of the hook binds to the MV's own catalog, the hook runs in
  the refresh transaction; if it fails, the refresh rolls back. DuckLake MVs, hooks that write
  to another database, and scripts whose write targets cannot be resolved before execution run
  after the refresh commits. OpenIVM records `openivm_views.pending_after_hook` and retries the
  hook on the next refresh until it succeeds, even when no input changes remain. A crash after
  the hook commits but before the flag is cleared can repeat the hook, so use idempotent SQL.
- **Explicit transactions:** inside `BEGIN ... COMMIT`, hooks are emitted inline into the
  refresh program and commit or roll back with the caller's transaction.

## Examples

### After-hook: Log every refresh
```sql
CREATE TABLE refresh_log(view_name VARCHAR, refreshed_at TIMESTAMP);

INSERT INTO openivm_refresh_hooks
SELECT view_name,
       'INSERT INTO refresh_log VALUES(''my_view'', now()::TIMESTAMP)',
       'after'
FROM openivm_views WHERE view_sql_name = 'my_view';
```

### Replace-hook: Custom flush logic (SIDRA)
```sql
-- Register SIDRA's flush as the refresh action for a centralized MV
INSERT INTO openivm_refresh_hooks
SELECT view_name, 'PRAGMA flush(''daily_steps'', ''duckdb'')', 'replace'
FROM openivm_views WHERE view_sql_name = 'daily_steps';
```

### Before-hook: Validate data before refresh
```sql
INSERT INTO openivm_refresh_hooks
SELECT view_name,
       'SELECT CASE WHEN COUNT(*) = 0 THEN error(''No delta data'') END FROM openivm_delta_my_table',
       'before'
FROM openivm_views WHERE view_sql_name = 'my_view';
```

### Building custom hooks from the compiled refresh SQL

Set `openivm_files_path` to an existing writable directory to have OpenIVM write the
refresh program it executes to `openivm_upsert_queries_<view key>.sql` in that directory.
`PRAGMA openivm_files('my_view')` lists the paths. Copy the SQL, modify it, and register
it as a hook:

```sql
-- Step 1: Write the compiled refresh SQL to a file (the refresh still executes)
SET openivm_files_path = '/tmp/openivm';
PRAGMA refresh('my_view');
PRAGMA openivm_files('my_view');

-- Step 2: Copy the SQL, modify as needed, register as hook
INSERT INTO openivm_refresh_hooks
SELECT view_name, '<paste modified SQL here>', 'replace'
FROM openivm_views WHERE view_sql_name = 'my_view';
```

## Removing a Hook

```sql
DELETE FROM openivm_refresh_hooks
WHERE view_name = (SELECT view_name FROM openivm_views WHERE view_sql_name = 'my_view');
```

After removal, `PRAGMA refresh()` reverts to default IVM behavior. Dropping the MV also
removes its hook.

## Interaction with Refresh Daemon

Hooks are respected by:
- **Manual refresh**: `PRAGMA refresh('view_name')`
- **Pipelines**: every node selected by `PRAGMA refresh_pipeline(...)`
- **Automatic refresh**: OpenIVM's background daemon (when `REFRESH EVERY` is configured)

The daemon checks for hooks before each scheduled refresh.

In an autocommit `PRAGMA refresh`, only the named view runs its hook. Views refreshed
through `openivm_cascade_refresh` (ancestors or descendants) are refreshed without their
hooks; use `refresh_pipeline` when each node's hook must run. Inside an explicit
transaction, the hooks of every cascaded view are included.

## Use Cases

- **SIDRA**: Register `PRAGMA flush()` as a replace-hook for centralized MVs
- **Audit logging**: Insert into a log table after each refresh
- **Cache invalidation**: Clear downstream caches when a view refreshes
- **Data export**: `COPY ... TO 's3://...'` after refresh
- **Alerting**: Check conditions and trigger notifications
- **Custom IVM**: Replace the default IVM with custom delta logic
