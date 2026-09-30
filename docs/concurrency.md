# Concurrency and operations

What you can rely on when several connections read, write and refresh at the same time, and how to run OpenIVM
day to day. For the mechanics behind this (the mutation gate, cursor bookkeeping, UTC timestamps), see
[internals/concurrency.md](internals/concurrency.md).

## Guarantees at a glance

| Situation | What happens |
|---|---|
| Reading a native MV while it refreshes | You see the view before or after the refresh, never in between: a native refresh commits in one transaction. |
| Reading a DuckLake MV while it refreshes | Not atomic today: a reader can briefly see groups missing or half-published. The final state is correct ([#88](https://github.com/ila/openivm/issues/88)). |
| Two refreshes at once | Serialized: the second waits for the first to finish. |
| Writing to a tracked table while a refresh runs | Serialized with the refresh. |
| Writing to two different tracked tables at once | Serialized with each other (see [below](#one-gate-per-database)). |
| Writing to a table no MV depends on | Not affected by OpenIVM. |
| Two sessions creating the same MV | Exactly one succeeds and metadata stays consistent; the other fails with a write-write conflict naming an internal table ([#91](https://github.com/ila/openivm/issues/91)). |
| `DROP VIEW` or `ALTER TABLE` during a refresh | Waits for the refresh to finish, then runs. |
| Writers outside this DuckDB process (e.g. other DuckLake clients) | Not blocked by OpenIVM; their changes are picked up by the next refresh. |

A table is *tracked* when at least one materialized view reads from it.

## Known issues

- **Queries running during a refresh can crash the process.** A refresh temporarily changes DuckDB's database-wide
  `disabled_optimizers` setting without synchronization, and concurrent queries can read it mid-change. The refresh
  also leaves the setting overwritten afterwards ([#89](https://github.com/ila/openivm/issues/89)). Until this is
  fixed, avoid running heavy concurrent queries against a database while it refreshes.
- **DuckLake refreshes are not atomic for readers** ([#88](https://github.com/ila/openivm/issues/88)).
- **A long transaction blocks everything tracked** ([#90](https://github.com/ila/openivm/issues/90)); see below.

## One gate per database

OpenIVM serializes every change to its own state through one lock per database: delta capture on tracked tables,
refreshes, materialized view creation and drops, and `ALTER TABLE` on tracked tables. Ordinary reads never wait.

Two consequences matter in practice:

- **Tracked writers serialize with each other**, not only with refreshes. Two sessions inserting into two different
  tracked tables take turns. For write-heavy workloads, batch changes into fewer, larger transactions.
- **The wait has no timeout.** An explicit transaction holds the gate from its first write to a tracked table until
  it commits or rolls back. A transaction left open (for example an idle `BEGIN` in a notebook) blocks every other
  tracked write, every refresh and the refresh daemon on that database until it ends, and nothing reports who holds
  the gate ([#90](https://github.com/ila/openivm/issues/90)).

Keep transactions that touch tracked tables short, and commit before doing slow work in the same session.

## Scheduled refresh needs a running process

`REFRESH EVERY` is served by a background thread inside the process that has the database open with OpenIVM loaded.
Nothing refreshes while no such process is running: changes accumulate as deltas and are applied the next time a
refresh runs, manually or by the daemon of a process that opens the database again.

DuckDB allows one read-write process per database file, so this is also the only place the daemon can run. For
scheduled refresh, keep one long-lived process (a service, or a job runner that holds the connection) with the
database open. While it holds the file, other processes can't open it at all, not even with `-readonly`.

Only one refresh daemon exists per process. Opening a second database with OpenIVM in the same process moves the
daemon to it, and scheduled refresh stops on the first ([#34](https://github.com/ila/openivm/issues/34)). Run one
database per process if you rely on `REFRESH EVERY`.

The daemon reads settings through its own connection, so use `SET GLOBAL` for settings it should see. See
[automatic refresh](refresh/automatic-refresh.md).

## Batching statements

Don't send `CREATE MATERIALIZED VIEW` and a `PRAGMA` on that view in one query string (one `duckdb -c "..."` argument
or one multi-statement client call): DuckDB prepares the `PRAGMA` before the view exists, and the batch fails
([#35](https://github.com/ila/openivm/issues/35)). Send them as separate statements.

## Side effects of loading OpenIVM

- **`preserve_insertion_order` is turned off for the whole database**, and `PRAGMA refresh` resets it to `false` even
  if a session set it back to `true` ([#92](https://github.com/ila/openivm/issues/92)). Queries and exports that rely
  on insertion order without an `ORDER BY` can return rows in a different order: reading a 5M-row CSV returned every
  row out of file order. Use `ORDER BY` where order matters.
- **`disabled_optimizers` is overwritten by refresh** ([#89](https://github.com/ila/openivm/issues/89)). If you set it
  yourself, set it again after refreshing.

## Refresh hooks run arbitrary SQL

A row in `openivm_refresh_hooks` makes the matching refresh execute its `hook_sql` as part of the refresh. Treat
write access to that table like write access to a stored procedure: anyone who can insert there can make a refresh
run any statement. See [refresh hooks](refresh_hooks.md).

## Upgrading OpenIVM

- OpenIVM must run with the DuckDB version it was built for; see
  [databases created by other DuckDB versions](build/building.md#databases-created-by-other-duckdb-versions) for file
  compatibility.
- On first use, a newer OpenIVM adds any metadata columns it needs to the existing `openivm_*` tables; existing rows
  are kept.
- Compiled view definitions are not migrated. If a view misbehaves after an upgrade, or you move a chain of views to a
  version with a different internal representation, recreate it from its original SQL (`view_sql_name` and
  `sql_string` in `openivm_views`). See [chained views](refresh/pipelines.md).
- Keep a copy of the database file before upgrading until refreshes have been verified.

## Troubleshooting

| Symptom | What to check |
|---|---|
| Refresh, DML on a tracked table or the daemon hangs | Another session holds the gate: look for an open transaction that wrote to a tracked table, and commit or roll it back. |
| `Warning: recovering '<view>' from interrupted refresh via full recompute` | A previous refresh stopped mid-way; the full recompute restores the view. See [crash safety](refresh/automatic-refresh.md#crash-safety). |
| `OpenIVM refresh committed; external delta cleanup deferred` | Consumed deltas in another attached database couldn't be deleted after commit. The view is correct; the rows are retried on a later refresh. |
| `... does not exist in IVM metadata` right after `CREATE` | The statements were sent in one query string; see [batching statements](#batching-statements). |
| A scheduled view stopped updating | Check that a process with the database open is still running, then `PRAGMA refresh_status('<view>')` for the effective interval and last refresh. |
| A refresh is slower than recomputing | `PRAGMA refresh_cost('<view>')` compares the estimates; `SET openivm_profile_refresh = true` records per-step timings in `openivm_refresh_profile`. |
| Unsure what a refresh runs | `SET openivm_files_path = '<dir>'` writes the compiled SQL; see [inspect the generated SQL](build/building.md#inspect-the-generated-sql). |

For build and installation problems, see [building: troubleshooting](build/building.md#troubleshooting).
