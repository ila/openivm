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
    (SELECT COUNT(*) FROM ((SELECT * FROM {view_name}) EXCEPT ALL ({base_query}))) +
    (SELECT COUNT(*) FROM (({base_query}) EXCEPT ALL (SELECT * FROM {view_name})));
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
SELECT DISTINCT 'openivm_refresh_type=' || refresh_type_name
FROM openivm_compile_with_facts('{view_name}', '{facts_json}')
WHERE stmt_kind = 'data';
"""
    compile_output = run_duckdb(binary, database, compile_sql)
    classifications = [line for line in compile_output.splitlines() if line.startswith("openivm_refresh_type=")]
    if classifications != ["openivm_refresh_type=SIMPLE_PROJECTION"]:
        raise AssertionError(f"{name}: compiled join must stay incremental, got {classifications!r}")

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
    binary = Path(sys.argv[1])
    if not binary.exists() and binary.suffix.lower() != ".exe":
        windows_binary = binary.with_name(f"{binary.name}.exe")
        if windows_binary.exists():
            binary = windows_binary
    binary = binary.resolve()
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
        run_scenario(
            binary,
            root,
            "projection_wrapped_self_join",
            """
CREATE TABLE self_event(event_id INTEGER, account_id INTEGER, event_ts INTEGER, label VARCHAR);
CREATE TABLE self_account(account_id INTEGER, account_name VARCHAR);
INSERT INTO self_event VALUES
    (1, 1, 10, 'a'),
    (2, 1, 20, 'b'),
    (3, 2, 10, 'c');
INSERT INTO self_account VALUES (1, 'one'), (2, 'two');
CREATE MATERIALIZED VIEW self_mv AS
SELECT cur.event_id AS current_id,
       prev.event_id AS previous_id,
       account.account_name,
       cur.label
FROM self_event cur
JOIN self_event prev
  ON cur.account_id = prev.account_id
 AND prev.event_ts < cur.event_ts
JOIN self_account account ON cur.account_id = account.account_id;
INSERT INTO self_event VALUES
    (4, 1, 30, 'd'),
    (5, 2, 20, 'e'),
    (6, 2, 30, 'temporary');
UPDATE self_event SET label = 'bx', event_ts = 25 WHERE event_id = 2;
UPDATE self_event SET label = 'temporary_x' WHERE event_id = 6;
DELETE FROM self_event WHERE event_id IN (1, 6);
UPDATE self_account SET account_name = 'ONE' WHERE account_id = 1;
""",
            "self_mv",
            {
                "target_dialect": "duckdb",
                "compile_only": True,
                "delta_shape": {
                    "self_event": "MIXED",
                    "self_account": "MIXED",
                },
            },
            """SELECT cur.event_id,
       prev.event_id,
       account.account_name,
       cur.label
FROM self_event cur
JOIN self_event prev
  ON cur.account_id = prev.account_id
 AND prev.event_ts < cur.event_ts
JOIN self_account account ON cur.account_id = account.account_id""",
        )
        run_scenario(
            binary,
            root,
            "left_join_passthrough_mixed",
            """
CREATE TABLE left_fact(id INTEGER, k1 INTEGER, k2 INTEGER);
CREATE TABLE left_one(k INTEGER, label VARCHAR);
CREATE TABLE left_two(k INTEGER, label VARCHAR);
INSERT INTO left_fact VALUES (1, 10, 20), (2, 11, 21), (3, 10, 21), (4, 12, 22);
INSERT INTO left_one VALUES (10, 'a'), (10, 'duplicate'), (12, 'c');
INSERT INTO left_two VALUES (20, 'old'), (21, 'removed'), (22, 'kept');
CREATE MATERIALIZED VIEW left_mv AS
WITH projected AS (
    SELECT f.id, f.k2, d1.label
    FROM left_fact f LEFT JOIN left_one d1 ON f.k1 = d1.k
)
SELECT p.id, d2.label
FROM projected p LEFT JOIN left_two d2 ON p.k2 = d2.k;
INSERT INTO left_one VALUES (11, 'late'), (11, 'late_duplicate');
DELETE FROM left_one WHERE label = 'duplicate';
UPDATE left_two SET label = 'changed' WHERE k = 20;
DELETE FROM left_two WHERE k = 21;
INSERT INTO left_two VALUES (23, 'new');
UPDATE left_fact SET k2 = 23 WHERE id = 1;
DELETE FROM left_fact WHERE id = 4;
INSERT INTO left_fact VALUES (5, 11, 20);
""",
            "left_mv",
            {
                "target_dialect": "duckdb",
                "compile_only": True,
                "force_view_delta_cascade": True,
                "delta_shape": {
                    "left_fact": "MIXED",
                    "left_one": "MIXED",
                    "left_two": "MIXED",
                },
            },
            """WITH projected AS (
    SELECT f.id, f.k2, d1.label
    FROM left_fact f LEFT JOIN left_one d1 ON f.k1 = d1.k
)
SELECT p.id, d2.label
FROM projected p LEFT JOIN left_two d2 ON p.k2 = d2.k""",
        )

    print("regular N-term compiled SQL integration tests passed")


if __name__ == "__main__":
    main()
