# Extension updating

OpenIVM targets **DuckDB v2.0.0**. The DuckDB release tag and the matching
extension tooling ref have different names:

| Component | Ref | Where it is selected |
|---|---|---|
| DuckDB | `v2.0.0` | `duckdb` gitlink; `duckdb_version` in both CI jobs; `OVERRIDE_GIT_DESCRIBE` in `Makefile` |
| Extension tooling | `v2.0-cyanoptera` | `extension-ci-tools` gitlink; its branch in `.gitmodules`; both reusable workflow refs and `ci_tools_version` inputs |
| LPTS | Compatible pinned commit | `third_party/lpts` gitlink |

Normal setup should restore the committed versions, including the tooling used
by the local Makefile:

```sh
git submodule update --init --recursive
```

When upgrading DuckDB:

1. Update the `duckdb` gitlink to the intended release tag. Keep the DuckDB source
   unmodified; extension compatibility changes belong in OpenIVM or LPTS.
2. Find the corresponding release branch in `duckdb/extension-ci-tools`. Update
   its gitlink and `.gitmodules` branch together. Do not assume that the tooling
   ref has the same name as the DuckDB tag, or use the unrelated `main` branch.
3. Update both jobs in `.github/workflows/MainDistributionPipeline.yml`: each
   `duckdb_version` must match the DuckDB release; each reusable workflow ref and
   `ci_tools_version` must match the tooling release branch.
4. Update `OVERRIDE_GIT_DESCRIBE` in `Makefile` and pin a compatible LPTS commit.
   LPTS compiles against DuckDB's planner API.
5. Update the version and build instructions in [building](build/building.md).
6. Build from the committed submodule pins and run the checks in
   [testing](build/testing.md). Verify the native and loadable extension paths,
   and wait for the CI platform matrix before merging.

Use `git ls-tree HEAD duckdb extension-ci-tools third_party/lpts` to inspect the
committed pins and `git submodule status` to detect local checkouts that differ.

# API changes
DuckDB extensions built with this extension template are built against the internal C++ API of DuckDB. This API is not guaranteed to be stable.
What this means for extension development is that when updating your extensions DuckDB target version using the above steps, you may run into the fact that your extension no longer builds properly.

Currently, DuckDB does not (yet) provide a specific change log for these API changes, but it is generally not too hard to figure out what has changed.

For figuring out how and why the C++ API changed, we recommend using the following resources:
- DuckDB's [Release Notes](https://github.com/duckdb/duckdb/releases)
- DuckDB's history of [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
- The git history of the relevant C++ Header file of the API that has changed
