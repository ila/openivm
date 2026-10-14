#!/usr/bin/env python3
"""
Validate is_incremental metadata by attempting to CREATE MATERIALIZED VIEW
with OpenIVM for each query and checking the resulting refresh_type.

For each query file in benchmark/queries/:
  - SUCCESS + refresh_type != 'FULL_REFRESH'  → is_incremental = True
  - SUCCESS + refresh_type == 'FULL_REFRESH'  → is_incremental = False
  - MV CREATE fails                       → is_incremental = False (record error)
"""

from __future__ import annotations

import os
import sys
import json
import argparse
import duckdb
from pathlib import Path
from datetime import datetime


def log(msg: str):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def create_tpcc_schema(con):
    """Create empty TPC-C schema for structural validation."""
    ddls = [
        """CREATE TABLE WAREHOUSE (
            W_ID INT, W_YTD DECIMAL(12, 2), W_TAX DECIMAL(4, 4),
            W_NAME VARCHAR(10), W_STREET_1 VARCHAR(20), W_STREET_2 VARCHAR(20),
            W_CITY VARCHAR(20), W_STATE CHAR(2), W_ZIP CHAR(9)
        )""",
        """CREATE TABLE DISTRICT (
            D_W_ID INT, D_ID INT, D_YTD DECIMAL(12, 2), D_TAX DECIMAL(4, 4),
            D_NEXT_O_ID INT, D_NAME VARCHAR(10), D_STREET_1 VARCHAR(20),
            D_STREET_2 VARCHAR(20), D_CITY VARCHAR(20), D_STATE CHAR(2), D_ZIP CHAR(9)
        )""",
        """CREATE TABLE CUSTOMER (
            C_W_ID INT, C_D_ID INT, C_ID INT, C_DISCOUNT DECIMAL(4, 4),
            C_CREDIT CHAR(2), C_LAST VARCHAR(16), C_FIRST VARCHAR(16),
            C_CREDIT_LIM DECIMAL(12, 2), C_BALANCE DECIMAL(12, 2),
            C_YTD_PAYMENT FLOAT, C_PAYMENT_CNT INT, C_DELIVERY_CNT INT,
            C_STREET_1 VARCHAR(20), C_STREET_2 VARCHAR(20),
            C_CITY VARCHAR(20), C_STATE CHAR(2), C_ZIP CHAR(9),
            C_PHONE CHAR(16), C_SINCE TIMESTAMP, C_MIDDLE CHAR(2), C_DATA VARCHAR(500)
        )""",
        """CREATE TABLE ITEM (
            I_ID INT, I_NAME VARCHAR(24), I_PRICE DECIMAL(5, 2),
            I_DATA VARCHAR(50), I_IM_ID INT
        )""",
        """CREATE TABLE STOCK (
            S_W_ID INT, S_I_ID INT, S_QUANTITY INT, S_YTD DECIMAL(8, 2),
            S_ORDER_CNT INT, S_REMOTE_CNT INT, S_DATA VARCHAR(50),
            S_DIST_01 CHAR(24), S_DIST_02 CHAR(24), S_DIST_03 CHAR(24),
            S_DIST_04 CHAR(24), S_DIST_05 CHAR(24), S_DIST_06 CHAR(24),
            S_DIST_07 CHAR(24), S_DIST_08 CHAR(24), S_DIST_09 CHAR(24), S_DIST_10 CHAR(24)
        )""",
        """CREATE TABLE OORDER (
            O_W_ID INT, O_D_ID INT, O_ID INT, O_C_ID INT, O_CARRIER_ID INT,
            O_OL_CNT INT, O_ALL_LOCAL INT, O_ENTRY_D TIMESTAMP
        )""",
        """CREATE TABLE NEW_ORDER (
            NO_W_ID INT, NO_D_ID INT, NO_O_ID INT
        )""",
        """CREATE TABLE ORDER_LINE (
            OL_W_ID INT, OL_D_ID INT, OL_O_ID INT, OL_NUMBER INT, OL_I_ID INT,
            OL_DELIVERY_D TIMESTAMP, OL_AMOUNT DECIMAL(6, 2), OL_SUPPLY_W_ID INT,
            OL_QUANTITY DECIMAL(6, 2), OL_DIST_INFO CHAR(24)
        )""",
        """CREATE TABLE HISTORY (
            H_C_ID INT, H_C_D_ID INT, H_C_W_ID INT, H_D_ID INT, H_W_ID INT,
            H_DATE TIMESTAMP, H_AMOUNT DECIMAL(6, 2), H_DATA VARCHAR(24)
        )""",
    ]
    for ddl in ddls:
        con.execute(ddl)


def read_query_file(path: Path) -> tuple[dict, str]:
    """Return (metadata, query) from a query file. Metadata is {} if absent."""
    lines = path.read_text().splitlines()
    metadata = {}
    query_lines = []
    for line in lines:
        if line.startswith("-- {") and not query_lines:
            try:
                metadata = json.loads(line[3:])
            except Exception:
                pass
        elif not line.startswith("--") and line.strip():
            query_lines.append(line)
    return metadata, " ".join(query_lines).strip()


def write_query_file(path: Path, metadata: dict, query: str):
    path.write_text("-- " + json.dumps(metadata) + "\n" + query + "\n")


METADATA_TABLE = "openivm_views"
DEFAULT_METADATA_SCHEMA = "main"

# Ordinals persisted in openivm_views.type (RefreshType in src/include/core/openivm_constants.hpp).
# Ordinal 10 is intentionally unused.
REFRESH_TYPE_NAMES = {
    0: "AGGREGATE_GROUP",
    1: "SIMPLE_AGGREGATE",
    2: "SIMPLE_PROJECTION",
    3: "FULL_REFRESH",
    4: "AGGREGATE_HAVING",
    5: "WINDOW_PARTITION",
    6: "GROUP_RECOMPUTE",
    7: "TOP_K",
    8: "DISTINCT_INCREMENTAL",
    9: "SEMI_ANTI_RECOMPUTE",
    11: "COUNT_DISTINCT_INCREMENTAL",
}
FULL_REFRESH_ORDINAL = 3

RESULT_INCREMENTAL = "incremental"
RESULT_FULL_REFRESH = "full_refresh"
RESULT_CREATE_FAILED = "create_failed"
RESULT_METADATA_FAILED = "metadata_failed"


class MetadataError(Exception):
    """The OpenIVM metadata could not be read or interpreted (the CREATE itself succeeded)."""


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def locate_views_table(con, metadata_schema: str | None = None) -> str:
    """
    Return the quoted, fully qualified name of the openivm_views table.

    OpenIVM stores metadata in the schema of the catalog that owns the view
    (normally main). The catalog is never assumed: it is looked up from the
    connection, and if the table is not in the default location the catalog is
    searched for it (or `metadata_schema` is used when given).
    """
    current_catalog = con.execute("SELECT current_database()").fetchone()[0]
    rows = con.execute(
        "SELECT database_name, schema_name FROM duckdb_tables() "
        f"WHERE table_name = {quote_literal(METADATA_TABLE)} AND NOT internal"
    ).fetchall()
    candidates = [(str(db), str(schema)) for db, schema in rows]
    if metadata_schema is not None:
        wanted = (current_catalog, metadata_schema)
        if wanted not in candidates:
            raise MetadataError(f"{METADATA_TABLE} not found in {current_catalog}.{metadata_schema}")
        chosen = wanted
    elif (current_catalog, DEFAULT_METADATA_SCHEMA) in candidates:
        chosen = (current_catalog, DEFAULT_METADATA_SCHEMA)
    else:
        local = [c for c in candidates if c[0] == current_catalog]
        pool = local or candidates
        if len(pool) != 1:
            found = ", ".join(f"{db}.{schema}" for db, schema in pool) or "none"
            raise MetadataError(f"cannot locate a unique {METADATA_TABLE} table (found: {found})")
        chosen = pool[0]
    return ".".join(quote_identifier(part) for part in (chosen[0], chosen[1], METADATA_TABLE))


def read_refresh_type(con, mv_name: str, metadata_schema: str | None = None) -> str:
    """
    Resolve the refresh type name of a user-facing materialized view from the
    OpenIVM metadata. The internal key (openivm_views.view_name) may be an
    encoded identifier, so the view is matched by view_sql_name plus its
    catalog and schema. Raises MetadataError when it cannot be determined.
    """
    try:
        table = locate_views_table(con, metadata_schema)
        catalog, schema = con.execute("SELECT current_database(), current_schema()").fetchone()
        name = quote_literal(mv_name.lower())

        def lookup(name_expression: str):
            return con.execute(
                f"SELECT type FROM {table} "
                f"WHERE lower({name_expression}) = {name} "
                f"AND lower(COALESCE(view_catalog, {quote_literal(catalog)})) = {quote_literal(catalog.lower())} "
                f"AND lower(COALESCE(view_schema, {quote_literal(DEFAULT_METADATA_SCHEMA)})) = "
                f"{quote_literal(schema.lower())}"
            ).fetchall()

        try:
            rows = lookup("COALESCE(view_sql_name, view_name)")
        except duckdb.Error:
            rows = lookup("view_name")  # metadata written before SQL names were stored separately
    except MetadataError:
        raise
    except Exception as e:
        raise MetadataError(str(e)[:200].replace("\n", " ")) from e

    if not rows:
        raise MetadataError(f"no {METADATA_TABLE} row for {catalog}.{schema}.{mv_name}")
    if len(rows) > 1:
        raise MetadataError(f"ambiguous {METADATA_TABLE} rows for {catalog}.{schema}.{mv_name}")
    ordinal = rows[0][0]
    if ordinal is None:
        raise MetadataError(f"NULL refresh type for {mv_name}")
    try:
        return REFRESH_TYPE_NAMES[int(ordinal)]
    except (KeyError, ValueError, TypeError):
        raise MetadataError(f"unknown refresh type {ordinal!r} for {mv_name}") from None


def classify_query(con, query: str, mv_name: str, metadata_schema: str | None = None) -> tuple[str, str]:
    """
    Try CREATE MATERIALIZED VIEW, read the numeric refresh type from the
    OpenIVM metadata, then DROP.
    Returns (result, detail_string) where result is one of
    "incremental", "full_refresh", "create_failed" (CREATE raised) or
    "metadata_failed" (CREATE succeeded but the metadata could not be read).
    """
    # Ensure a clean slate for this view name
    try:
        con.execute(f"DROP VIEW IF EXISTS {mv_name}")
    except Exception:
        pass
    try:
        con.execute(f"CREATE MATERIALIZED VIEW {mv_name} AS {query}")
    except Exception as e:
        msg = str(e)[:200].replace("\n", " ")
        return (RESULT_CREATE_FAILED, msg)
    try:
        refresh_type = read_refresh_type(con, mv_name, metadata_schema)
    except MetadataError as e:
        return (RESULT_METADATA_FAILED, f"metadata read failed: {e}")
    finally:
        try:
            con.execute(f"DROP VIEW IF EXISTS {mv_name}")
        except Exception:
            pass

    if refresh_type == REFRESH_TYPE_NAMES[FULL_REFRESH_ORDINAL]:
        return (RESULT_FULL_REFRESH, refresh_type)
    return (RESULT_INCREMENTAL, refresh_type)


def open_connection(extension_path: str):
    con = duckdb.connect(":memory:", config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{extension_path}'")
    return con


def main(argv=None, connect=open_connection):
    parser = argparse.ArgumentParser()
    parser.add_argument("--queries-dir", default="benchmark/queries")
    parser.add_argument("--extension-path", default="build/release/extension/openivm/openivm.duckdb_extension")
    parser.add_argument("--limit", type=int, default=0, help="Only process first N files (0 = all)")
    parser.add_argument("--dry-run", action="store_true", help="Don't update files, just report")
    parser.add_argument("--progress-every", type=int, default=50)
    parser.add_argument(
        "--metadata-schema", default=None, help="Schema holding openivm_views (default: auto-detect, normally main)"
    )
    args = parser.parse_args(argv)

    queries_dir = Path(args.queries_dir)
    files = sorted(queries_dir.glob("query_*.sql"))
    if args.limit:
        files = files[: args.limit]

    log(f"Validating {len(files)} query files...")
    log("Setting up in-memory DuckDB with openivm extension + TPC-C schema...")
    con = connect(args.extension_path)
    create_tpcc_schema(con)

    stats = {
        "incremental": 0,
        "full_refresh": 0,
        "create_failed": 0,
        "metadata_failed": 0,
        "was_correct": 0,
        "was_wrong": 0,
        "updated": 0,
    }
    mismatches = []
    create_fail_reasons = {}
    metadata_fail_examples = []
    full_refresh_examples = []

    for idx, f in enumerate(files, 1):
        metadata, query = read_query_file(f)
        if not query:
            continue

        # Strip trailing semicolon since we're wrapping with CREATE MV AS
        q = query.rstrip().rstrip(';').strip()
        mv_name = f"_val_mv_{idx}"

        result, detail = classify_query(con, q, mv_name, args.metadata_schema)
        stats[result] += 1

        if result == RESULT_METADATA_FAILED:
            # CREATE succeeded but the classification is unknown: never judge or rewrite the annotations.
            if len(metadata_fail_examples) < 10:
                metadata_fail_examples.append((f.name, detail))
            continue

        actual_incremental = result == "incremental"
        claimed_incremental = bool(metadata.get("is_incremental", False))

        if actual_incremental == claimed_incremental:
            stats["was_correct"] += 1
        else:
            stats["was_wrong"] += 1
            mismatches.append((f.name, claimed_incremental, actual_incremental, detail))

        # Track examples / reasons
        if result == "create_failed":
            reason_key = detail[:80]
            create_fail_reasons[reason_key] = create_fail_reasons.get(reason_key, 0) + 1
        if result == "full_refresh" and len(full_refresh_examples) < 10:
            full_refresh_examples.append((f.name, q[:80]))

        # Update metadata if wrong
        if actual_incremental != claimed_incremental and not args.dry_run:
            metadata["is_incremental"] = actual_incremental
            metadata["mv_verified"] = True
            if result == "create_failed":
                metadata["mv_create_error"] = detail[:150]
            elif result == "full_refresh":
                metadata.pop("mv_create_error", None)
                metadata["refresh_type"] = "FULL_REFRESH"
            else:
                metadata.pop("mv_create_error", None)
                metadata["refresh_type"] = detail
            write_query_file(f, metadata, query)
            stats["updated"] += 1

        if idx % args.progress_every == 0 or idx == len(files):
            log(
                f"  [{idx}/{len(files)}] incr={stats['incremental']} full={stats['full_refresh']} fail={stats['create_failed']} correct={stats['was_correct']} wrong={stats['was_wrong']}"
            )

    log("")
    log("=" * 60)
    log("Validation complete.")
    log(f"  Incremental:     {stats['incremental']} ({100*stats['incremental']/max(len(files),1):.1f}%)")
    log(f"  Full refresh:    {stats['full_refresh']} ({100*stats['full_refresh']/max(len(files),1):.1f}%)")
    log(f"  MV create fail:  {stats['create_failed']} ({100*stats['create_failed']/max(len(files),1):.1f}%)")
    log(f"  Metadata read fail: {stats['metadata_failed']} (not classified, files untouched)")
    log(f"  Metadata correct before: {stats['was_correct']}")
    log(f"  Metadata wrong before:   {stats['was_wrong']}")
    if not args.dry_run:
        log(f"  Files updated:   {stats['updated']}")
    log("")

    if create_fail_reasons:
        log("Top MV-create failure reasons:")
        for r, c in sorted(create_fail_reasons.items(), key=lambda x: -x[1])[:10]:
            log(f"  {c}x: {r}")

    if metadata_fail_examples:
        log("")
        log("Metadata read failures (query files NOT modified):")
        for name, detail in metadata_fail_examples:
            log(f"  {name}: {detail[:100]}")

    if full_refresh_examples:
        log("")
        log("Sample full-refresh queries:")
        for name, qsnip in full_refresh_examples[:5]:
            log(f"  {name}: {qsnip}...")

    if mismatches and args.dry_run:
        log("")
        log(f"First 10 mismatches (dry-run):")
        for name, claimed, actual, detail in mismatches[:10]:
            log(f"  {name}: claimed={claimed} actual={actual} ({detail[:60]})")

    # An unclassified query means the validation is incomplete.
    return 1 if stats["metadata_failed"] else 0


if __name__ == "__main__":
    sys.exit(main())
