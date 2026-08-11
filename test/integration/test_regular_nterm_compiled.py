#!/usr/bin/env python3

import json
import subprocess
import sys
import tempfile
from pathlib import Path


def run_duckdb(binary: Path, database: Path, sql: str) -> str:
    result = subprocess.run(
        [str(binary), str(database), "-csv", "-noheader"],
        input=sql,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"DuckDB failed:\n{result.stdout}\n{result.stderr}")
    return result.stdout.strip()


def bag_equality_sql(view_name: str, base_query: str) -> str:
    return f"""
SELECT
    (SELECT COUNT(*) FROM (SELECT * FROM {view_name} EXCEPT ALL {base_query})) +
    (SELECT COUNT(*) FROM ({base_query} EXCEPT ALL SELECT * FROM {view_name}));
"""


def run_scenario(
    binary: Path,
    root: Path,
    name: str,
    setup_sql: str,
    view_name: str,
    facts: dict,
    base_query: str,
):
    scenario_dir = root / name
    scenario_dir.mkdir()
    database = scenario_dir / "test.db"
    output_dir = scenario_dir / "compiled"
    output_dir.mkdir()
    output_path = str(output_dir).replace("'", "''")
    facts_json = json.dumps(facts, separators=(",", ":")).replace("'", "''")

    compile_sql = f"""
SET openivm_files_path='{output_path}';
{setup_sql}
SELECT COUNT(*) FROM openivm_compile_with_facts('{view_name}', '{facts_json}');
"""
    run_duckdb(binary, database, compile_sql)

    program_path = output_dir / f"openivm_upsert_queries_{view_name}.sql"
    if not program_path.exists():
        raise RuntimeError(f"OpenIVM did not emit {program_path}")
    run_duckdb(binary, database, program_path.read_text())

    mismatch_count = run_duckdb(binary, database, bag_equality_sql(view_name, base_query))
    if mismatch_count != "0":
        raise AssertionError(f"{name}: compiled N-term refresh has {mismatch_count} bag mismatches")


def main():
    if len(sys.argv) != 2:
        raise SystemExit("usage: test_regular_nterm_compiled.py /path/to/duckdb")
    binary = Path(sys.argv[1]).resolve()
    if not binary.exists():
        raise SystemExit(f"DuckDB binary not found: {binary}")

    with tempfile.TemporaryDirectory(prefix="openivm_regular_nterm_") as temp_dir:
        root = Path(temp_dir)
        run_scenario(
            binary,
            root,
            "all_sources_changed",
            """
CREATE TABLE nterm_a(id INTEGER, b_id INTEGER);
CREATE TABLE nterm_b(id INTEGER, c_id INTEGER);
CREATE TABLE nterm_c(id INTEGER, label VARCHAR);
INSERT INTO nterm_a VALUES (1, 10), (2, 10);
INSERT INTO nterm_b VALUES (10, 100), (20, 200);
INSERT INTO nterm_c VALUES (100, 'old'), (200, 'new');
CREATE MATERIALIZED VIEW nterm_mv AS
SELECT a.id, c.label
FROM nterm_a a
JOIN nterm_b b ON a.b_id = b.id
JOIN nterm_c c ON b.c_id = c.id;
UPDATE nterm_a SET b_id = 20 WHERE id = 1;
DELETE FROM nterm_a WHERE id = 2;
INSERT INTO nterm_a VALUES (3, 10);
UPDATE nterm_b SET c_id = 200 WHERE id = 10;
UPDATE nterm_c SET label = 'new_x' WHERE id = 200;
""",
            "nterm_mv",
            {
                "target_dialect": "duckdb",
                "compile_only": True,
                "delta_shape": {
                    "nterm_a": "MIXED",
                    "nterm_b": "MIXED",
                    "nterm_c": "MIXED",
                },
            },
            """SELECT a.id, c.label
FROM nterm_a a
JOIN nterm_b b ON a.b_id = b.id
JOIN nterm_c c ON b.c_id = c.id""",
        )
        run_scenario(
            binary,
            root,
            "unchanged_sources",
            """
CREATE TABLE unchanged_a(id INTEGER, b_id INTEGER);
CREATE TABLE unchanged_b(id INTEGER, c_id INTEGER);
CREATE TABLE unchanged_c(id INTEGER, label VARCHAR);
INSERT INTO unchanged_a VALUES (1, 10);
INSERT INTO unchanged_b VALUES (10, 100);
INSERT INTO unchanged_c VALUES (100, 'old'), (101, 'new');
CREATE MATERIALIZED VIEW unchanged_mv AS
SELECT a.id, c.label
FROM unchanged_a a
JOIN unchanged_b b ON a.b_id = b.id
JOIN unchanged_c c ON b.c_id = c.id;
UPDATE unchanged_b SET c_id = 101 WHERE id = 10;
""",
            "unchanged_mv",
            {
                "target_dialect": "duckdb",
                "compile_only": True,
                "delta_shape": {
                    "unchanged_a": "UNCHANGED",
                    "unchanged_b": "MIXED",
                    "unchanged_c": "UNCHANGED",
                },
            },
            """SELECT a.id, c.label
FROM unchanged_a a
JOIN unchanged_b b ON a.b_id = b.id
JOIN unchanged_c c ON b.c_id = c.id""",
        )

    print("regular N-term compiled SQL integration tests passed")


if __name__ == "__main__":
    main()
