#!/usr/bin/env python3
"""
strip_dbt_jinja.py — Fetch dbt SQL models from ivm-bench and convert to
plain SQL for use as benchmark/queries/tpcdi/ query files.

Usage:
    python3 benchmark/scripts/strip_dbt_jinja.py [--out benchmark/queries/tpcdi]

What it does:
  1. Fetches the DuckDB dbt project from:
       https://github.com/mdrakiburrahman/ivm-bench/tree/main/dbt-projects/duckdb/
  2. Replaces {{ ref('table_name') }} with table_name
  3. Replaces {{ dbt_utils.generate_surrogate_key([...]) }} with md5(concat(...))
  4. Strips other Jinja blocks ({{ config(...) }}, {# comments #}, etc.)
  5. Writes each model as a .sql file in the output directory with JSON metadata
     header matching the format used by rewriter_benchmark.cpp

Dependencies: requests (pip install requests)
"""

import json
import re
import os
import sys
import argparse

try:
    import requests
except ImportError:
    print("Error: requests not installed. Run: pip install requests")
    sys.exit(1)

GITHUB_API = "https://api.github.com"
REPO = "mdrakiburrahman/ivm-bench"
BASE_PATH = "src/containers/dbt-server/dbt-projects/duckdb/models"

# Best-effort complexity classification based on SQL content
def classify_query(sql: str) -> dict:
    sql_upper = sql.upper()
    has_window = bool(re.search(r'\bOVER\s*\(', sql_upper))
    has_between = "BETWEEN" in sql_upper
    has_last_value = "LAST_VALUE" in sql_upper or "FIRST_VALUE" in sql_upper
    has_cte = sql_upper.lstrip().startswith("WITH ")
    has_full_outer = "FULL OUTER" in sql_upper or "FULL JOIN" in sql_upper
    has_group_by = "GROUP BY" in sql_upper

    n_joins = len(re.findall(r'\bJOIN\b', sql_upper))
    complexity = "low" if n_joins == 0 else "medium" if n_joins <= 2 else "high"

    # A query is incremental if it has no window functions, no range joins (BETWEEN on
    # non-key columns), no FULL OUTER JOIN, and no forward-fill patterns.
    is_incremental = not (has_window or has_between or has_last_value or has_full_outer)

    operators = []
    if "JOIN" in sql_upper:
        if "FULL OUTER" in sql_upper or "FULL JOIN" in sql_upper:
            operators.append("FULL_OUTER_JOIN")
        elif "LEFT" in sql_upper:
            operators.append("OUTER_JOIN")
        else:
            operators.append("INNER_JOIN")
    if has_group_by:
        operators.append("AGGREGATE")
    if has_window:
        operators.append("WINDOW")
    if has_cte:
        operators.append("CTE")
    if "CASE" in sql_upper:
        operators.append("CASE")
    if not operators:
        operators.append("PROJECTION")

    return {
        "operators": ",".join(operators) if operators else "PROJECTION",
        "complexity": complexity,
        "is_incremental": is_incremental,
        "has_nulls": False,
        "has_cast": "CAST(" in sql_upper or "::" in sql,
        "has_case": "CASE" in sql_upper,
        "source": "ivm-bench/duckdb",
    }


def strip_jinja(sql: str) -> str:
    """Remove/replace Jinja2 constructs from a dbt SQL model."""
    # Strip {# comments #}
    sql = re.sub(r'\{#.*?#\}', '', sql, flags=re.DOTALL)

    # Strip {{ config(...) }} blocks (may span multiple lines)
    sql = re.sub(r'\{\{-?\s*config\s*\(.*?\)\s*-?\}\}', '', sql, flags=re.DOTALL)

    # Replace {{ ref('table_name') }} with table_name
    def replace_ref(m):
        inner = m.group(1).strip().strip("'\"")
        return inner
    sql = re.sub(r'\{\{\s*ref\s*\(\s*([^)]+)\s*\)\s*\}\}', replace_ref, sql)

    # Replace {{ source('schema', 'table') }} with table
    def replace_source(m):
        args = [a.strip().strip("'\"") for a in m.group(1).split(',')]
        return args[-1] if args else m.group(0)
    sql = re.sub(r'\{\{\s*source\s*\(\s*([^)]+)\s*\)\s*\}\}', replace_source, sql)

    # Replace {{ dbt_utils.generate_surrogate_key(['a', 'b', 'c']) }} with md5(concat(a, '|', b, '|', c))
    def replace_surrogate_key(m):
        # Extract list contents: ['col1', 'col2', ...]
        inner = m.group(1)
        cols = re.findall(r"['\"]([^'\"]+)['\"]", inner)
        if not cols:
            return "md5('unknown')"
        parts = " || '|' || ".join(cols)
        return f"md5({parts})"
    sql = re.sub(
        r'\{\{\s*dbt_utils\.generate_surrogate_key\s*\(\s*(\[.*?\])\s*\)\s*\}\}',
        replace_surrogate_key, sql, flags=re.DOTALL
    )

    # Replace any remaining {{ ... }} with empty string (other dbt macros)
    sql = re.sub(r'\{\{[^}]+\}\}', '', sql)

    # Replace {% ... %} blocks (if/endif, set, etc.) with empty
    sql = re.sub(r'\{%.*?%\}', '', sql, flags=re.DOTALL)

    # Collapse multiple blank lines
    sql = re.sub(r'\n{3,}', '\n\n', sql)

    return sql.strip()


def fetch_tree(path: str) -> list:
    """Fetch directory listing from GitHub API."""
    url = f"{GITHUB_API}/repos/{REPO}/contents/{path}"
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_file(download_url: str) -> str:
    resp = requests.get(download_url, timeout=30)
    resp.raise_for_status()
    return resp.text


def collect_sql_files(path: str) -> list[tuple[str, str]]:
    """Recursively collect (name, download_url) for all .sql files under path."""
    results = []
    try:
        entries = fetch_tree(path)
    except Exception as e:
        print(f"  Warning: could not fetch {path}: {e}")
        return results

    for entry in entries:
        if entry["type"] == "file" and entry["name"].endswith(".sql"):
            results.append((entry["name"], entry["download_url"]))
        elif entry["type"] == "dir":
            results.extend(collect_sql_files(entry["path"]))
    return results


def main():
    parser = argparse.ArgumentParser(description="Strip dbt Jinja from ivm-bench DuckDB models")
    parser.add_argument("--out", default="benchmark/queries/tpcdi",
                        help="Output directory for stripped .sql files")
    parser.add_argument("--layer", default="gold",
                        help="Which dbt layer to fetch: bronze, silver, gold, all (default: gold)")
    args = parser.parse_args()

    os.makedirs(args.out, exist_ok=True)

    print(f"Fetching dbt SQL models from https://github.com/{REPO}/...")

    if args.layer == "all":
        paths = [BASE_PATH]
    else:
        # Layer maps to directory names in the dbt project
        layer_dirs = {
            "bronze": ["staging", "bronze"],
            "silver": ["silver", "intermediate"],
            "gold": ["gold", "marts", "dimensions", "facts"],
        }
        dirs = layer_dirs.get(args.layer, [args.layer])
        # Try to find them under BASE_PATH
        paths = [f"{BASE_PATH}/{d}" for d in dirs]

    all_files: list[tuple[str, str]] = []
    for p in paths:
        files = collect_sql_files(p)
        all_files.extend(files)
        print(f"  Found {len(files)} .sql files in {p}")

    if not all_files:
        # Fall back to fetching the whole models tree
        print("  No files found in specified layer paths, trying full models tree...")
        all_files = collect_sql_files(BASE_PATH)

    if not all_files:
        print("Error: no SQL files found. Check network access and repo structure.")
        sys.exit(1)

    print(f"Processing {len(all_files)} SQL files...")
    written = 0
    for i, (fname, url) in enumerate(all_files, 1):
        try:
            raw_sql = fetch_file(url)
            stripped = strip_jinja(raw_sql)

            if not stripped or len(stripped) < 10:
                print(f"  [{i}/{len(all_files)}] Skipping {fname} (empty after strip)")
                continue

            # Classify the stripped SQL
            meta = classify_query(stripped)
            model_name = fname.replace(".sql", "")
            meta["tpcdi"] = model_name

            # Build metadata comment header (same format as TPC-C query files)
            header = f"-- {json.dumps(meta)}"
            out_sql = header + "\n" + stripped

            out_name = f"tpcdi_query{i:04d}_{model_name}.sql"
            out_path = os.path.join(args.out, out_name)
            with open(out_path, "w") as f:
                f.write(out_sql + "\n")
            written += 1
            print(f"  [{i}/{len(all_files)}] {fname} -> {out_name} "
                  f"({'INCREMENTAL' if meta['is_incremental'] else 'FULL_REFRESH'})")

        except Exception as e:
            print(f"  [{i}/{len(all_files)}] Error processing {fname}: {e}")

    print(f"\nDone. Wrote {written} query files to {args.out}/")
    print("Run the benchmark with:")
    print(f"  ./rewriter_benchmark --workload tpcdi --db rewriter_tpcdi.db --out results_tpcdi.csv")


if __name__ == "__main__":
    main()
