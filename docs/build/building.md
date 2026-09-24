# Building and running OpenIVM

OpenIVM currently requires **DuckDB v1.5.4** and must be built from source. It is
not yet published as a community extension, so `LOAD 'openivm';` in a separately
installed DuckDB is not enough. The build below produces both a DuckDB command-line
program with OpenIVM built in and a loadable extension.

You do not need to replace your existing DuckDB installation. Use the executable
from this build for OpenIVM, and open an existing database with it as described below.

## 1. Install the build tools

You need Git, CMake (>= 3.5), Make, Python 3, and a C++ compiler. Ninja is recommended.
The commands below are for macOS and Linux; run shell commands in Terminal, and SQL
in the DuckDB prompt after launching it.

### macOS (including Apple Silicon)

Install Apple's command-line developer tools, then install the remaining tools
with [Homebrew](https://brew.sh/) if you use it:

```bash
xcode-select --install
brew install cmake ninja python
```

If the developer tools are already installed, continue to the next step. On Apple
Silicon, use a native ARM terminal and toolchain rather than mixing Rosetta and ARM
builds.

### Ubuntu / Debian

```bash
sudo apt-get update
sudo apt-get install build-essential git cmake ninja-build python3
```

## 2. Clone the source and its dependencies

```bash
git clone --recurse-submodules https://github.com/ila/openivm.git
cd openivm
```

For an existing clone that is missing dependencies:

```bash
git submodule update --init --recursive
```

Use the submodule revisions recorded in the repository. They include the matching
DuckDB source and LPTS SQL compiler; installing DuckDB separately does not provide
these build dependencies. Do not update the submodules to their latest branches
when following this guide.

## 3. Compile

From the `openivm` directory:

```bash
CMAKE_BUILD_PARALLEL_LEVEL=4 GEN=ninja make
```

This builds DuckDB as well as OpenIVM, so the first build can take a while. Four
build workers are a conservative starting point; lower the number if compilation
runs out of memory. You can rerun the same command after an interrupted build.

The main outputs are:

| File | Purpose |
|---|---|
| `build/release/duckdb` | DuckDB CLI with OpenIVM built in; use this for the steps below |
| `build/release/extension/openivm/openivm.duckdb_extension` | Loadable extension for a matching DuckDB build and platform |

Without Ninja, use `CMAKE_BUILD_PARALLEL_LEVEL=4 make` in a fresh checkout. Keep the
same generator when reusing a build directory.

## 4. Launch OpenIVM and check the version

From the repository root:

```bash
./build/release/duckdb openivm_demo.duckdb
```

This opens or creates a persistent database in the current directory. Calling just
`duckdb` may start a different installation. At the DuckDB prompt, run:

```sql
SELECT version(); -- expected: v1.5.4
SELECT extension_name, loaded
FROM duckdb_extensions()
WHERE extension_name = 'openivm'; -- expected: openivm | true
```

OpenIVM is already loaded in this executable; no `INSTALL` or `LOAD` is needed.
Use `.quit` to exit the CLI.

## 5. Try an incremental refresh

In a fresh demo database, paste the following SQL into the DuckDB prompt:

```sql
CREATE TABLE sales (region VARCHAR, product VARCHAR, amount INT);
INSERT INTO sales VALUES ('US', 'Widget', 100), ('EU', 'Gadget', 200);

CREATE MATERIALIZED VIEW regional_totals AS
    SELECT region, SUM(amount) AS total, COUNT(*) AS cnt
    FROM sales GROUP BY region;

-- Batch changes before refreshing once.
INSERT INTO sales VALUES ('US', 'Bolt', 50), ('JP', 'Gear', 300);
UPDATE sales SET amount = 110 WHERE product = 'Widget';
DELETE FROM sales WHERE product = 'Gadget';
PRAGMA refresh('regional_totals');

SELECT * FROM regional_totals ORDER BY region;
-- JP | 300 | 1
-- US | 160 | 2

-- Both checks must return zero rows, including when duplicates are present.
SELECT * FROM regional_totals
EXCEPT ALL
SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region;

SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region
EXCEPT ALL
SELECT * FROM regional_totals;
```

For scripts, save the SQL in `demo.sql` and run it against a fresh database:

```bash
./build/release/duckdb -bail openivm_script_demo.duckdb < demo.sql
```

Use the prompt or a SQL file rather than putting creation and refresh together in
one `-c` argument: refresh metadata can be resolved before the preceding view
creation has executed.

## 6. Use an existing DuckDB database

Start with a backup or test copy, and close other processes that have the database
open for writing. You can open the file directly:

```bash
./build/release/duckdb /absolute/path/to/analytics.duckdb
```

Or launch `./build/release/duckdb` and attach it at the SQL prompt:

```sql
ATTACH '/absolute/path/to/analytics.duckdb' AS analytics;
USE analytics;
SHOW TABLES;
```

Replace the path with an existing file: DuckDB otherwise creates a new database.
`USE analytics` makes it the default catalog for subsequent unqualified table and
view names. You can now create materialized views over its tables and refresh them
as in the example above. You do not have to export and reimport compatible files.

OpenIVM runs inside this DuckDB process; a separate metadata server is not required.
For this workflow, keep the source tables and materialized views in the same
writable database. OpenIVM creates its metadata, backing tables, and delta tables
there. A `READ_ONLY` attachment is useful for inspecting data, but cannot support
this in-place maintenance workflow.

For ordinary DuckDB tables, perform subsequent inserts, updates, and deletes through
connections with OpenIVM loaded so it can record the changes. Writes made through
another DuckDB installation without OpenIVM are not automatically captured.
DuckLake uses a different snapshot-based change-tracking path; see
[DuckLake integration](../ducklake.md).

### Databases created by other DuckDB versions

The **extension version requirement** and the **database file format** are separate:

- OpenIVM must run with its matching DuckDB v1.5.4 build. A locally compiled
  extension cannot be loaded into an arbitrary DuckDB version.
- DuckDB supports reading older database formats, with backward compatibility
  established from v0.10. A database does not need to have been created with v1.5.4
  to be usable here.
- Reading a file written by a newer DuckDB release depends on the storage features
  used; forward compatibility is not guaranteed. `ATTACH` does not convert an
  incompatible format.

See DuckDB's [storage compatibility documentation](https://duckdb.org/docs/stable/internals/storage).
As a concrete check, a file created with DuckDB v1.1.2 was successfully attached to
the v1.5.4 OpenIVM build, then used for MV creation and a mixed-DML refresh checked
against full recomputation in both directions. This is not a guarantee for every
file version, extension-defined type, or stored SQL definition.

If the file is incompatible, open it with a DuckDB version that can read it, export
the required tables to Parquet, and import them into a new database using the
OpenIVM build. Recreate views and other schema objects as needed. Keep the original
file until the imported data and queries have been verified.

## Loading the extension into another DuckDB client

The built CLI is the simplest starting point. For a separate compatible DuckDB
v1.5.4 CLI, enable unsigned local extensions when launching it:

```bash
duckdb -unsigned /absolute/path/to/test.duckdb
```

Then load the compiled file by its full path:

```sql
LOAD '/absolute/path/to/openivm/build/release/extension/openivm/openivm.duckdb_extension';
```

The DuckDB version, operating system, architecture, and build platform must match.
`-unsigned` allows loading your local build; it does not bypass version or platform
compatibility. Likewise, a Python client uses its own DuckDB library: building the
CLI does not update an installed Python package. Hosted services also have their
own extension-loading policies; a successful local build does not establish
MotherDuck deployment support.

## Troubleshooting

| Symptom | What to check |
|---|---|
| `LOAD 'openivm'` cannot find the extension | Use `./build/release/duckdb`, or load the compiled extension by its full path in a matching client. |
| Extension version/platform mismatch | Check `SELECT version()` and use the DuckDB executable produced by this build. |
| Unsigned extension error | Launch a compatible external CLI with `-unsigned` before loading the local extension. |
| Missing DuckDB/LPTS source or build files | Run `git submodule update --init --recursive` from the repository root. |
| Compiler killed / out of memory | Rerun with `CMAKE_BUILD_PARALLEL_LEVEL=2 GEN=ninja make`. |
| Database lock error | Close the other process using the file, or work with a separate test copy. |
| Incompatible database storage version | Follow the file compatibility guidance above; rebuilding OpenIVM does not convert the database. |

When reporting a problem, include your OS/architecture, `SELECT version()` output,
`git rev-parse HEAD`, the command you ran, and the complete error message.

## Development

For a debug build, use `GEN=ninja make debug`; its outputs are under `build/debug/`.
Run `make format-fix` after C++ edits. See [testing](testing.md) for the test suite,
[UPDATING.md](../UPDATING.md) for maintainer instructions on changing the pinned
DuckDB version, and [pipelines](../refresh/pipelines.md) for refresh orchestration.
