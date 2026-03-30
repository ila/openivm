# Building OpenIVM

## Quick Start

```bash
GEN=ninja make      # Build (release)
make format-fix     # Auto-format code before committing
```

## Requirements

| Dependency | Version |
|---|---|
| CMake | 3.5+ |
| C++ compiler | C++17 support |
| DuckDB | v1.5+ (included as git submodule) |
| Ninja | Recommended (via `GEN=ninja`) |

## Build Output

```
build/release/extension/openivm/openivm.duckdb_extension
```

## Submodule Setup

DuckDB is pinned as a git submodule. On first clone:

```bash
git submodule update --init --recursive
```

## Debug Build

```bash
GEN=ninja make debug
```

Output: `build/debug/extension/openivm/openivm.duckdb_extension`
