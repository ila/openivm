# Building OpenIVM

## Prerequisites

- **git**
- **cmake** (>= 3.5)
- **ninja** (optional but recommended — significantly faster builds)
- **C++ compiler** with C++14 support

## Clone

```bash
git clone --recurse-submodules https://github.com/ila/openivm.git
cd openivm
```

If you already cloned without `--recurse-submodules`, run:

```bash
git submodule update --init --recursive
```

## Build

```bash
GEN=ninja make        # release build (recommended)
make                  # release build without ninja
GEN=ninja make debug  # debug build
```

The extension is built into:

```
build/release/extension/openivm/openivm.duckdb_extension   # release
build/debug/extension/openivm/openivm.duckdb_extension     # debug
```

## Format Code

```bash
make format-fix
```

## Updating the DuckDB Version

The DuckDB version is pinned via the `duckdb` and `extension-ci-tools` submodules. To update to a new release:

```bash
cd duckdb && git fetch --tags && git checkout v1.X.0
cd ../extension-ci-tools && git fetch && git checkout origin/v1.X.0
cd ..
git add duckdb extension-ci-tools
git commit -m "Bump DuckDB to v1.X.0"
```

Replace `v1.X.0` with the target version tag. See [UPDATING.md](../UPDATING.md) for details on handling DuckDB API changes.
