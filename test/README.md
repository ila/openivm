# Testing OpenIVM

Most behavior is tested with SQLLogicTest fixtures in `test/sql/`:

```bash
make test
make test_debug
build/release/test/unittest 'test/sql/internal_schema.test'
```

Release tests also run the integration targets below. Build and configuration details
are in [the testing guide](../docs/build/testing.md).

| Target | Behavior that needs an integration runner |
| --- | --- |
| `make test_source_delta_publication` | Deterministic CREATE/DML/DROP races, shared ownership, lifecycle transactions, and reopening with a fresh configuration |
| `make test_scheduler_catalog` | Native metadata discovery and schema synchronization while an unrelated SQLite-backed DuckLake catalog is locked |
| `make test_benchmark_metadata` | Benchmark metadata resolution across source and target catalogs |
| `make test_regular_nterm_compiled` | Exported refresh SQL executed by a separate CLI process |
| `make test_remote_metadata` | PostgreSQL metadata, separate client processes, leases, crash recovery, and concurrent source changes |

Python runners provide temporary directories and process/lock orchestration. The
native lifecycle executable requires a fresh database path; its runner supplies and
removes one. Remote tests require `OPENIVM_POSTGRES_DSN` for a disposable PostgreSQL
database and are skipped when it is unset. CI requires them through
[RemoteMetadata.yml](../.github/workflows/RemoteMetadata.yml).

## Coverage ownership

Use shared scenarios for the three metadata/archival feature contracts. SQL fixtures
cover operator results, placement, SQL archival, migration, and backend-specific
transaction/recovery behavior. The native lifecycle executable owns the stronger
cross-connection and fresh-configuration checks:

- `TestMetadataSchemaSettingTransaction`: first-use initialization, new/existing
  registries, CREATE/ALTER/REPLACE/DROP, commit/rollback, and schema cleanup.
- `TestInternalSchemaReopen`: unset settings after a real reopen, metadata discovery,
  stored maintenance placement, source-delta reuse, replacement, and final cleanup.
- `TestMetadataSchemaMigration`: CREATE/refresh-triggered movement, late copy failure,
  atomic rollback and retry, movement back to `main`, hooks, and unchanged maintenance tables.
- Publication fixtures: conflicting batched DML, concurrent allocation/drop,
  and failed-create ownership.
- Cascade/replay fixtures: qualified identities, native versus staged cross-catalog
  cleanup, shared consumers, pending deltas, and commit/rollback.

Avoid duplicating these basic lifecycle checks in SQL fixtures. Keep SQL cases that
exercise additional operator state, identifier parsing, or DuckLake snapshot behavior.
Every successful refresh must compare the MV with the base query using `EXCEPT ALL`
in both directions. Stress cases batch conflicting INSERT/DELETE/UPDATE operations
before refreshing. Concurrency tests synchronize on actual admission/lock state;
timeouts bound failures rather than establish ordering.
