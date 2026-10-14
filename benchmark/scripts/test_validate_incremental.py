#!/usr/bin/env python3
"""
Deterministic tests for validate_incremental.py.

The OpenIVM extension is not needed: FakeOpenIVMConnection wraps a real in-memory
DuckDB connection, backs `CREATE MATERIALIZED VIEW` with a real `openivm_views`
table (same column names/types as src/core/parser_create_mv_helpers.cpp) and
scripts what the extension would store, so the metadata SQL is bound against a
real catalog.

Run: python3 -m unittest benchmark/scripts/test_validate_incremental.py
"""

import json
import re
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))

import validate_incremental as vi  # noqa: E402

AGGREGATE_GROUP = 0
SIMPLE_PROJECTION = 2
FULL_REFRESH = 3

CREATE_RE = re.compile(r"CREATE MATERIALIZED VIEW (\S+) AS ", re.IGNORECASE)
DROP_RE = re.compile(r"DROP VIEW IF EXISTS (\S+)", re.IGNORECASE)


def encoded_key(catalog: str, schema: str, name: str) -> str:
    """Internal metadata key, as produced by RefreshMetadata::AllocateViewKey."""
    key = "__openivm_mv_"
    for part in (catalog, schema, name):
        key += part.lower().encode().hex() + "_"
    return key


class FakeOpenIVMConnection:
    def __init__(self, metadata_schema="main", types=None, fail_create=(), legacy_columns=False, create_metadata=True):
        self.con = duckdb.connect(":memory:")
        self.types = dict(types or {})
        self.fail_create = set(fail_create)
        self.metadata_schema = metadata_schema
        self.legacy_columns = legacy_columns
        self.has_metadata = create_metadata
        self.created = []
        self.dropped = []
        self.created_keys = {}  # user-facing name -> internal keys written by this fake's CREATE
        if create_metadata:
            if metadata_schema != "main":
                self.con.execute(f"CREATE SCHEMA {metadata_schema}")
            self.con.execute(
                f"CREATE TABLE {metadata_schema}.openivm_views (view_name VARCHAR PRIMARY KEY, "
                "view_catalog VARCHAR DEFAULT NULL, view_schema VARCHAR DEFAULT NULL, "
                "view_sql_name VARCHAR DEFAULT NULL, sql_string VARCHAR, type TINYINT)"
            )

    def insert_view(self, key, sql_name, ordinal, catalog="memory", schema="main"):
        self.con.execute(
            f"INSERT INTO {self.metadata_schema}.openivm_views VALUES (?, ?, ?, ?, 'q', ?)",
            [key, catalog, schema, sql_name, ordinal],
        )

    def execute(self, sql, *args):
        create = CREATE_RE.match(sql)
        if create:
            name = create.group(1)
            self.created.append(name)
            if name in self.fail_create:
                raise duckdb.CatalogException(f"Catalog Error: cannot create {name}")
            if name in self.types and self.has_metadata:
                key = name if self.legacy_columns else encoded_key("memory", "main", name)
                self.insert_view(key, None if self.legacy_columns else name, self.types[name])
                self.created_keys.setdefault(name, []).append(key)
            return self.con.execute("SELECT 1")
        drop = DROP_RE.match(sql)
        if drop:
            name = drop.group(1)
            self.dropped.append(name)
            # Like the extension, DROP removes only this view's own metadata row; rows seeded by a
            # test for other identities (duplicates, other schemas) must survive.
            if self.has_metadata:
                for key in self.created_keys.pop(name, []):
                    self.con.execute(f"DELETE FROM {self.metadata_schema}.openivm_views WHERE view_name = ?", [key])
            return self.con.execute("SELECT 1")
        return self.con.execute(sql, *args)


class ClassifyQueryTest(unittest.TestCase):
    def test_incremental_view_uses_numeric_type_and_encoded_key(self):
        con = FakeOpenIVMConnection(types={"validate_mv": AGGREGATE_GROUP})
        result = vi.classify_query(con, "SELECT 1", "validate_mv")
        self.assertEqual(result, ("incremental", "AGGREGATE_GROUP"))
        # the internal key differs from the user-facing name and is still resolved
        self.assertNotEqual(encoded_key("memory", "main", "validate_mv"), "validate_mv")

    def test_simple_projection_is_incremental(self):
        con = FakeOpenIVMConnection(types={"validate_mv": SIMPLE_PROJECTION})
        self.assertEqual(vi.classify_query(con, "SELECT 1", "validate_mv"), ("incremental", "SIMPLE_PROJECTION"))

    def test_full_refresh_is_enum_ordinal_not_string(self):
        con = FakeOpenIVMConnection(types={"validate_mv": FULL_REFRESH})
        self.assertEqual(vi.classify_query(con, "SELECT 1", "validate_mv"), ("full_refresh", "FULL_REFRESH"))

    def test_view_is_dropped_after_classification(self):
        con = FakeOpenIVMConnection(types={"validate_mv": AGGREGATE_GROUP})
        vi.classify_query(con, "SELECT 1", "validate_mv")
        # once for the clean slate before CREATE, once for cleanup afterwards
        self.assertEqual(con.dropped, ["validate_mv", "validate_mv"])

    def test_view_name_matching_is_case_insensitive(self):
        con = FakeOpenIVMConnection(types={"Validate_MV": FULL_REFRESH})
        self.assertEqual(vi.classify_query(con, "SELECT 1", "Validate_MV")[0], "full_refresh")

    def test_same_name_in_other_schema_is_not_confused(self):
        con = FakeOpenIVMConnection(types={"validate_mv": AGGREGATE_GROUP})
        con.insert_view(encoded_key("memory", "other", "validate_mv2"), "validate_mv", FULL_REFRESH, schema="other")
        self.assertEqual(vi.classify_query(con, "SELECT 1", "validate_mv"), ("incremental", "AGGREGATE_GROUP"))

    def test_ambiguous_identity_is_a_metadata_failure(self):
        con = FakeOpenIVMConnection(types={"validate_mv": AGGREGATE_GROUP})
        con.insert_view("duplicate_key", "validate_mv", FULL_REFRESH)
        result, detail = vi.classify_query(con, "SELECT 1", "validate_mv")
        self.assertEqual(result, "metadata_failed")
        self.assertIn("ambiguous", detail)

    def test_legacy_metadata_without_sql_name(self):
        con = FakeOpenIVMConnection(types={"validate_mv": FULL_REFRESH}, legacy_columns=True)
        self.assertEqual(vi.classify_query(con, "SELECT 1", "validate_mv"), ("full_refresh", "FULL_REFRESH"))

    def test_metadata_in_configured_schema_is_located(self):
        con = FakeOpenIVMConnection(metadata_schema="ivm_meta", types={"validate_mv": FULL_REFRESH})
        self.assertEqual(vi.classify_query(con, "SELECT 1", "validate_mv"), ("full_refresh", "FULL_REFRESH"))
        self.assertEqual(vi.classify_query(con, "SELECT 1", "validate_mv", "ivm_meta")[0], "full_refresh")

    def test_explicit_missing_metadata_schema_is_a_metadata_failure(self):
        con = FakeOpenIVMConnection(types={"validate_mv": FULL_REFRESH})
        result, _ = vi.classify_query(con, "SELECT 1", "validate_mv", "no_such_schema")
        self.assertEqual(result, "metadata_failed")

    def test_create_failure_is_create_failed(self):
        con = FakeOpenIVMConnection(fail_create={"validate_mv"})
        result, detail = vi.classify_query(con, "SELECT 1", "validate_mv")
        self.assertEqual(result, "create_failed")
        self.assertIn("cannot create", detail)

    def test_missing_metadata_row_is_not_create_failed(self):
        con = FakeOpenIVMConnection()  # CREATE "succeeds" but stores nothing
        result, detail = vi.classify_query(con, "SELECT 1", "validate_mv")
        self.assertEqual(result, "metadata_failed")
        self.assertIn("no openivm_views row", detail)
        self.assertEqual(con.dropped, ["validate_mv", "validate_mv"])  # cleanup still happens

    def test_missing_metadata_table_is_metadata_failed(self):
        # CREATE succeeds but there is no openivm_views table anywhere
        con = FakeOpenIVMConnection(types={"validate_mv": AGGREGATE_GROUP}, create_metadata=False)
        result, _ = vi.classify_query(con, "SELECT 1", "validate_mv")
        self.assertEqual(result, "metadata_failed")

    def test_unreadable_metadata_column_is_metadata_failed(self):
        con = FakeOpenIVMConnection(types={"validate_mv": AGGREGATE_GROUP})
        con.con.execute("ALTER TABLE openivm_views RENAME COLUMN type TO refresh_type")
        result, _ = vi.classify_query(con, "SELECT 1", "validate_mv")
        self.assertEqual(result, "metadata_failed")

    def test_unknown_or_null_refresh_type_is_metadata_failed(self):
        for ordinal in (10, 99):
            con = FakeOpenIVMConnection(types={"validate_mv": ordinal})
            result, detail = vi.classify_query(con, "SELECT 1", "validate_mv")
            self.assertEqual(result, "metadata_failed")
            self.assertIn("unknown refresh type", detail)
        con = FakeOpenIVMConnection(types={"validate_mv": None})
        result, detail = vi.classify_query(con, "SELECT 1", "validate_mv")
        self.assertEqual(result, "metadata_failed")
        self.assertIn("NULL", detail)

    def test_ordinals_match_refresh_type_enum(self):
        header = (Path(__file__).resolve().parents[2] / "src/include/core/openivm_constants.hpp").read_text()
        body = header.split("enum class RefreshType", 1)[1].split("};", 1)[0]
        declared = {int(n): name for name, n in re.findall(r"^\s*([A-Z_]+)\s*=\s*(\d+)", body, re.MULTILINE)}
        self.assertEqual(declared, vi.REFRESH_TYPE_NAMES)


class MainTest(unittest.TestCase):
    def run_main(self, files, dry_run, **connection_args):
        """files: {name: (metadata, query)}. Returns (exit_code, {name: file text}, original texts)."""
        with tempfile.TemporaryDirectory() as tmp:
            queries = Path(tmp)
            for name, (metadata, query) in files.items():
                vi.write_query_file(queries / name, metadata, query)
            before = {p.name: p.read_text() for p in queries.glob("query_*.sql")}
            argv = ["--queries-dir", str(queries), "--progress-every", "1000"]
            if dry_run:
                argv.append("--dry-run")
            code = vi.main(argv, connect=lambda _path: FakeOpenIVMConnection(**connection_args))
            after = {p.name: p.read_text() for p in queries.glob("query_*.sql")}
            return code, before, after

    QUERY = "SELECT W_ID FROM WAREHOUSE"

    def test_write_mode_rewrites_wrong_annotations_from_numeric_types(self):
        files = {
            "query_a.sql": ({"is_incremental": True}, self.QUERY),  # actually FULL_REFRESH
            "query_b.sql": ({"is_incremental": False, "mv_create_error": "old"}, self.QUERY),  # incremental
        }
        code, before, after = self.run_main(
            files, dry_run=False, types={"_val_mv_1": FULL_REFRESH, "_val_mv_2": SIMPLE_PROJECTION}
        )
        self.assertEqual(code, 0)
        a = json.loads(after["query_a.sql"].splitlines()[0][3:])
        self.assertEqual(a, {"is_incremental": False, "mv_verified": True, "refresh_type": "FULL_REFRESH"})
        b = json.loads(after["query_b.sql"].splitlines()[0][3:])
        self.assertEqual(b, {"is_incremental": True, "mv_verified": True, "refresh_type": "SIMPLE_PROJECTION"})

    def test_dry_run_never_writes(self):
        files = {
            "query_a.sql": ({"is_incremental": True}, self.QUERY),
            "query_b.sql": ({"is_incremental": True}, self.QUERY),
        }
        code, before, after = self.run_main(
            files, dry_run=True, types={"_val_mv_1": FULL_REFRESH}, fail_create={"_val_mv_2"}
        )
        self.assertEqual(code, 0)
        self.assertEqual(before, after)

    def test_create_failure_is_recorded_in_write_mode(self):
        files = {"query_a.sql": ({"is_incremental": True}, self.QUERY)}
        code, before, after = self.run_main(files, dry_run=False, fail_create={"_val_mv_1"})
        self.assertEqual(code, 0)
        meta = json.loads(after["query_a.sql"].splitlines()[0][3:])
        self.assertFalse(meta["is_incremental"])
        self.assertTrue(meta["mv_verified"])
        self.assertIn("cannot create", meta["mv_create_error"])

    def test_metadata_failure_never_rewrites_annotations(self):
        files = {
            "query_a.sql": ({"is_incremental": True}, self.QUERY),
            "query_b.sql": ({"is_incremental": False}, self.QUERY),
        }
        # both CREATEs succeed but no metadata is ever stored
        code, before, after = self.run_main(files, dry_run=False)
        self.assertNotEqual(code, 0)
        self.assertEqual(before, after)
        for text in after.values():
            self.assertNotIn("mv_verified", text)
            self.assertNotIn("mv_create_error", text)

    def test_metadata_failure_does_not_block_other_files(self):
        files = {
            "query_a.sql": ({"is_incremental": True}, self.QUERY),  # no metadata -> untouched
            "query_b.sql": ({"is_incremental": True}, self.QUERY),  # FULL_REFRESH -> rewritten
        }
        code, before, after = self.run_main(files, dry_run=False, types={"_val_mv_2": FULL_REFRESH})
        self.assertNotEqual(code, 0)
        self.assertEqual(before["query_a.sql"], after["query_a.sql"])
        self.assertFalse(json.loads(after["query_b.sql"].splitlines()[0][3:])["is_incremental"])

    def test_configured_metadata_schema_through_main(self):
        files = {"query_a.sql": ({"is_incremental": True}, self.QUERY)}
        code, _, after = self.run_main(
            files, dry_run=False, metadata_schema="ivm_meta", types={"_val_mv_1": FULL_REFRESH}
        )
        self.assertEqual(code, 0)
        self.assertEqual(json.loads(after["query_a.sql"].splitlines()[0][3:])["refresh_type"], "FULL_REFRESH")


if __name__ == "__main__":
    unittest.main()
