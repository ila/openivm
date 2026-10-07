#!/usr/bin/env python3
"""OpenIVM metadata in a remote PostgreSQL catalog (#84).

Covers placement, reopen discovery, explicit-transaction rejection, crash recovery with an
abandoned refresh lease, DROP, and concurrent clients sharing one metadata schema while the
materialized view lives in a DuckLake (no persistent local frontend database).

Requires OPENIVM_POSTGRES_DSN, a libpq key/value connection string for a disposable local
PostgreSQL database (CI starts one as a service container). Never point it at a shared or
production server: each scenario creates and drops its own schema. Without the variable the
test is skipped, unless --require is passed.

Usage: test_remote_metadata.py <duckdb CLI with OpenIVM linked> [--require]
"""

import os
import subprocess
import sys
import tempfile
import threading
import uuid
from pathlib import Path

EXTENSIONS = "INSTALL postgres; LOAD postgres; INSTALL ducklake; LOAD ducklake; INSTALL sqlite; LOAD sqlite;\n"


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class Client:
    def __init__(self, binary: Path, dsn: str, schema: str, database: str, lake: Path = None, configure=True):
        self.binary = binary
        self.database = database
        preamble = EXTENSIONS + f"ATTACH {sql_literal(dsn)} AS control (TYPE postgres);\n"
        if lake is not None:
            preamble += (
                f"ATTACH {sql_literal('ducklake:sqlite:' + str(lake / 'lake.sqlite'))} AS lake "
                f"(DATA_PATH {sql_literal(str(lake / 'data'))});\n"
            )
        if configure:
            preamble += (
                "SET openivm_metadata_catalog = 'control';\n" f"SET openivm_metadata_schema = {sql_literal(schema)};\n"
            )
        self.preamble = preamble

    def run(self, sql: str, check: bool = True) -> subprocess.CompletedProcess:
        result = subprocess.run(
            [str(self.binary), self.database, "-csv", "-noheader", "-bail"],
            input=self.preamble + sql,
            text=True,
            capture_output=True,
            check=False,
        )
        if check and result.returncode != 0:
            raise AssertionError(f"DuckDB failed:\n{sql}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}")
        return result

    def value(self, sql: str) -> str:
        lines = self.run(sql).stdout.strip().splitlines()
        if not lines:
            raise AssertionError(f"No result for:\n{sql}")
        return lines[-1]

    def expect_error(self, sql: str, message: str):
        result = self.run(sql, check=False)
        if result.returncode == 0 or message not in result.stderr:
            raise AssertionError(f"Expected error containing {message!r}:\n{sql}\nstderr:\n{result.stderr}")


def bag_difference(view: str, query: str) -> str:
    return (
        f"SELECT (SELECT count(*) FROM (SELECT * FROM {view} EXCEPT ALL {query})) + "
        f"(SELECT count(*) FROM ({query} EXCEPT ALL SELECT * FROM {view}));\n"
    )


# Evaluated by PostgreSQL itself (through postgres_query/postgres_execute), never by DuckDB.
PG_UTC_NOW = "(clock_timestamp() AT TIME ZONE 'UTC')"


def pg_table(schema: str, table: str) -> str:
    return f'"{schema}".{table}'


def pg_query(sql: str) -> str:
    return f"SELECT * FROM postgres_query('control', {sql_literal(sql)});\n"


def set_server_lease(schema: str, offset: str) -> str:
    """Moves the lease deadline relative to the PostgreSQL server clock."""
    sql = f"UPDATE {pg_table(schema, 'openivm_refresh_leases')} SET lease_until = {PG_UTC_NOW} {offset}"
    return f"CALL postgres_execute('control', {sql_literal(sql)});\n"


def drop_schema(binary: Path, dsn: str, schema: str):
    Client(binary, dsn, schema, ":memory:", configure=False).run(
        f"CALL postgres_execute('control', {sql_literal('DROP SCHEMA IF EXISTS ' + chr(34) + schema + chr(34) + ' CASCADE')});\n"
    )


def native_host_scenario(binary: Path, dsn: str, root: Path):
    schema = "openivm_it_" + uuid.uuid4().hex[:12]
    host = str(root / "host.db")
    client = Client(binary, dsn, schema, host)
    base = "SELECT product, SUM(amount), COUNT(*) FROM orders GROUP BY product"
    try:
        client.run(
            "CREATE TABLE orders (id INTEGER, product VARCHAR, amount INTEGER);\n"
            "INSERT INTO orders VALUES (1, 'a', 10), (2, 'b', 20), (3, 'a', 30), (4, NULL, 4);\n"
            "CREATE MATERIALIZED VIEW sales AS SELECT product, SUM(amount) AS total, COUNT(*) AS cnt "
            "FROM orders GROUP BY product;\n"
            "INSERT INTO orders VALUES (5, 'c', 5), (6, 'a', 6);\n"
            "UPDATE orders SET amount = amount + 1 WHERE product = 'b';\n"
            "DELETE FROM orders WHERE id IN (1, 6);\n"
            "PRAGMA refresh('sales');\n"
        )
        assert client.value(bag_difference("sales", base)) == "0", "refresh through remote metadata diverged"
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_views;") == "1"
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_refresh_leases;") == "0"
        assert (
            client.value(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'host' AND table_name = 'openivm_views';"
            )
            == "0"
        ), "metadata leaked into the host database"
        assert (
            client.value(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'host' AND starts_with(table_name, 'openivm_data_');"
            )
            == "1"
        ), "backing table moved with the metadata"
        # The compiled-SQL archive is metadata too: committed CREATE and refresh programs
        # are stored in the remote schema, never in the host database.
        assert (
            client.value(
                f"SELECT count(DISTINCT operation) FROM control.{schema}.openivm_compiled_programs "
                "WHERE last_outcome = 'committed';"
            )
            == "2"
        ), "compiled programs were not archived in the remote metadata schema"
        assert (
            client.value(
                f"SELECT count(*) > 0 FROM control.{schema}.openivm_compiled_statements WHERE operation = 'refresh';"
            )
            == "true"
        )
        assert (
            client.value(
                "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'host' "
                "AND starts_with(table_name, 'openivm_compiled_');"
            )
            == "0"
        ), "compiled-SQL archive leaked into the host database"

        client.expect_error(
            "BEGIN;\nINSERT INTO orders VALUES (7, 'd', 7);\nPRAGMA refresh('sales');\n",
            "cannot run inside an explicit transaction",
        )

        # A view inside the remote metadata catalog would bypass the split protocol.
        client.expect_error(
            f"CREATE MATERIALIZED VIEW control.{schema}.misplaced AS SELECT product, COUNT(*) AS c FROM orders GROUP BY product;\n",
            "remote OpenIVM metadata catalog",
        )
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_views;") == "1"

        # RENAME COLUMN rewrites remote view metadata through a helper connection, so an
        # explicit transaction cannot roll it back and is refused...
        client.expect_error(
            "BEGIN;\nALTER TABLE orders RENAME COLUMN amount TO amt;\n",
            "cannot run inside an explicit transaction",
        )
        assert (
            client.value(f"SELECT count(*) FROM control.{schema}.openivm_views WHERE sql_string LIKE '%amt%';") == "0"
        )
        # ...and a rename that fails after the rewrite committed restores the remote rows
        # with DELETE + INSERT (PostgreSQL has no INSERT OR REPLACE).
        failed = client.run("ALTER TABLE orders RENAME COLUMN amount TO id;\n", check=False)
        assert failed.returncode != 0, "renaming onto an existing column must fail"
        assert "could not restore" not in failed.stdout + failed.stderr, failed.stdout + failed.stderr
        client.run(
            "INSERT INTO orders VALUES (20, 'a', 20), (21, 'r', 21);\nDELETE FROM orders WHERE id = 21;\nPRAGMA refresh('sales');\n"
        )
        assert client.value(bag_difference("sales", base)) == "0", "failed rename left rewritten remote metadata"
        renamed = "SELECT product, SUM(amt), COUNT(*) FROM orders GROUP BY product"
        client.run(
            "ALTER TABLE orders RENAME COLUMN amount TO amt;\nINSERT INTO orders VALUES (22, 'a', 22);\n"
            "UPDATE orders SET amt = amt + 1 WHERE id = 20;\nPRAGMA refresh('sales');\n"
        )
        assert client.value(bag_difference("sales", renamed)) == "0", "refresh after a remote rename diverged"
        client.run(
            "ALTER TABLE orders RENAME COLUMN amt TO amount;\nDELETE FROM orders WHERE id IN (20, 22);\nPRAGMA refresh('sales');\n"
        )
        assert client.value(bag_difference("sales", base)) == "0"

        # Another client takes the lease over just before the watermark commit. The fence
        # writes the owned lease row, finds it gone and refuses to advance the watermarks;
        # the view stays marked, and the next client recomputes it.
        client.expect_error(
            "INSERT INTO orders VALUES (23, 'a', 23), (24, 't', 24);\nDELETE FROM orders WHERE id = 4;\n"
            "UPDATE orders SET amount = amount + 2 WHERE product = 'b';\n"
            "SET openivm_test_fail_point = 'lease_lost_before_watermark';\nPRAGMA refresh('sales');\n",
            "lost the refresh lease",
        )
        assert client.value(f"SELECT refresh_in_progress FROM control.{schema}.openivm_views;") == "true"
        assert (
            client.value(f"SELECT owner FROM control.{schema}.openivm_refresh_leases;") == "openivm-simulated-takeover"
        )
        client.run("PRAGMA refresh('sales');\n")
        assert client.value(bag_difference("sales", base)) == "0", "a stale watermark commit lost or doubled changes"
        assert client.value(f"SELECT refresh_in_progress FROM control.{schema}.openivm_views;") == "false"
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_refresh_leases;") == "0"

        # Reopen without settings: the marker in the host database finds the remote schema.
        reopened = Client(binary, dsn, schema, host, configure=False)
        reopened.run(
            "INSERT INTO orders VALUES (8, 'e', 8);\nDELETE FROM orders WHERE id = 2;\nPRAGMA refresh('sales');\n"
        )
        assert reopened.value(bag_difference("sales", base)) == "0", "refresh after reopen diverged"

        # A client crashes after committing MV data; its lease stays behind.
        client.expect_error(
            "INSERT INTO orders VALUES (9, 'a', 9), (10, 'f', 10);\nUPDATE orders SET amount = 0 WHERE id = 3;\n"
            "SET openivm_test_fail_point = 'after_data_commit';\nPRAGMA refresh('sales');\n",
            "simulated a process crash",
        )
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_refresh_leases;") == "1"
        assert client.value(f"SELECT refresh_in_progress FROM control.{schema}.openivm_views;") == "true"
        # Lease times are PostgreSQL server times: the abandoned lease ends one lease
        # (default 600 s) after the server clock at acquisition, a few seconds ago.
        assert (
            client.value(
                pg_query(
                    f"SELECT lease_until BETWEEN {PG_UTC_NOW} + interval '540 seconds' AND {PG_UTC_NOW} + "
                    f"interval '600 seconds' FROM {pg_table(schema, 'openivm_refresh_leases')}"
                )
            )
            == "true"
        ), "lease_until was not computed from the PostgreSQL server clock"
        # Another client cannot refresh while the lease is live on the server clock...
        client.expect_error("PRAGMA refresh('sales');\n", "being refreshed by another OpenIVM client")
        client.expect_error(
            set_server_lease(schema, "+ interval '120 seconds'") + "PRAGMA refresh('sales');\n",
            "being refreshed by another OpenIVM client",
        )
        # ...and recovers once it expired on the server clock, recomputing instead of
        # replaying the deltas.
        client.run(set_server_lease(schema, "- interval '1 second'") + "PRAGMA refresh('sales');\n")
        assert client.value(bag_difference("sales", base)) == "0", "crash recovery double-applied or lost changes"
        assert client.value(f"SELECT refresh_in_progress FROM control.{schema}.openivm_views;") == "false"
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_refresh_leases;") == "0"

        # A crash inside the data transaction loses only the uncommitted data change.
        client.expect_error(
            "INSERT INTO orders VALUES (11, 'g', 11);\nSET openivm_test_fail_point = 'before_data_commit';\n"
            "PRAGMA refresh('sales');\n",
            "simulated a process crash",
        )
        client.run(
            f"UPDATE control.{schema}.openivm_refresh_leases SET lease_until = TIMESTAMP '2000-01-01';\n"
            "PRAGMA refresh('sales');\n"
        )
        assert client.value(bag_difference("sales", base)) == "0", "recovery after an uncommitted crash diverged"

        client.run("DROP VIEW sales;\n")
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_views;") == "0"
        assert client.value(f"SELECT count(*) FROM control.{schema}.openivm_delta_tables;") == "0"
    finally:
        drop_schema(binary, dsn, schema)


def ducklake_concurrency_scenario(binary: Path, dsn: str, root: Path):
    schema = "openivm_it_" + uuid.uuid4().hex[:12]
    lake = root / "lake"
    (lake / "data").mkdir(parents=True)
    base = "SELECT k, SUM(v), COUNT(*) FROM lake.main.events GROUP BY k"
    view = "lake.main.event_totals"

    def client():
        # No persistent local database: metadata in PostgreSQL, data in DuckLake.
        return Client(binary, dsn, schema, ":memory:", lake=lake)

    try:
        client().run(
            "CREATE TABLE lake.main.events (id INTEGER, k INTEGER, v INTEGER);\n"
            "INSERT INTO lake.main.events SELECT i, i % 5, i FROM range(100) t(i);\n"
            f"CREATE MATERIALIZED VIEW {view} AS SELECT k, SUM(v) AS s, COUNT(*) AS c "
            "FROM lake.main.events GROUP BY k;\n"
        )
        for round_index in range(4):
            start = 1000 + round_index * 100
            client().run(
                f"INSERT INTO lake.main.events SELECT i, i % 7, i FROM range({start}, {start + 50}) t(i);\n"
                f"DELETE FROM lake.main.events WHERE id % 11 = {round_index};\n"
                f"UPDATE lake.main.events SET v = v + 1 WHERE k = {round_index};\n"
            )
            failures = []

            def refresh():
                result = client().run(f"PRAGMA refresh('{view}');\n", check=False)
                if result.returncode != 0:
                    failures.append(result.stderr)

            workers = [threading.Thread(target=refresh) for _ in range(3)]
            for worker in workers:
                worker.start()
            for worker in workers:
                worker.join()
            for failure in failures:
                # Losing the lease race is the expected concurrent failure. SQLite-backed
                # DuckLake catalogs can also refuse a concurrent reader; either way the
                # client gave up, and the bag check below proves nothing was lost or doubled.
                if not any(
                    expected in failure
                    for expected in ("being refreshed by another OpenIVM client", "database is locked")
                ):
                    raise AssertionError(f"Unexpected concurrent refresh failure:\n{failure}")
            checker = client()
            checker.run(f"PRAGMA refresh('{view}');\n")
            assert checker.value(bag_difference(view, base)) == "0", f"round {round_index}: concurrent refresh diverged"
            assert checker.value(f"SELECT count(*) FROM control.{schema}.openivm_refresh_leases;") == "0"
            assert checker.value(f"SELECT bool_or(refresh_in_progress) FROM control.{schema}.openivm_views;") == "false"
        # Staged DuckLake CREATE and refreshes archive their programs in the remote schema.
        assert (
            client().value(
                f"SELECT count(DISTINCT operation) FROM control.{schema}.openivm_compiled_programs "
                "WHERE committed_count > 0;"
            )
            == "2"
        ), "DuckLake programs were not archived in the remote metadata schema"
    finally:
        drop_schema(binary, dsn, schema)


def ducklake_lease_scenario(binary: Path, dsn: str, root: Path):
    """A client whose lease expires or is taken over mid-refresh never writes DuckLake data
    after its local deadline and never commits stale watermarks."""
    schema = "openivm_it_" + uuid.uuid4().hex[:12]
    lake = root / "lease_lake"
    (lake / "data").mkdir(parents=True)
    base = "SELECT k, SUM(v), COUNT(*) FROM lake.main.events GROUP BY k"
    view = "lake.main.event_totals"

    def client():
        return Client(binary, dsn, schema, ":memory:", lake=lake)

    try:
        client().run(
            "CREATE TABLE lake.main.events (id INTEGER, k INTEGER, v INTEGER);\n"
            "INSERT INTO lake.main.events SELECT i, i % 5, i FROM range(100) t(i);\n"
            f"CREATE MATERIALIZED VIEW {view} AS SELECT k, SUM(v) AS s, COUNT(*) AS c "
            "FROM lake.main.events GROUP BY k;\n"
        )
        cases = [
            # The deadline passed before the data phase: no DuckLake statement runs.
            ("lease_lost_before_data", "stopped before writing more materialized view data"),
            # The deadline passed after the first DuckLake statement committed.
            ("lease_lost_mid_data", "stopped before writing more materialized view data"),
            # Taken over after the data committed, unnoticed locally: the fence refuses.
            ("lease_lost_before_watermark", "lost the refresh lease"),
        ]
        for index, (point, message) in enumerate(cases):
            start = 1000 + index * 100
            client().run(
                f"INSERT INTO lake.main.events SELECT i, i % 7, i FROM range({start}, {start + 40}) t(i);\n"
                f"DELETE FROM lake.main.events WHERE id % 13 = {index};\n"
                f"UPDATE lake.main.events SET v = v + 3 WHERE k = {index};\n"
                "CREATE OR REPLACE TABLE lake.main.before_refresh AS SELECT * FROM lake.main.event_totals;\n"
            )
            client().expect_error(f"SET openivm_test_fail_point = '{point}';\nPRAGMA refresh('{view}');\n", message)
            checker = client()
            assert checker.value(f"SELECT refresh_in_progress FROM control.{schema}.openivm_views;") == "true", point
            assert (
                checker.value(f"SELECT owner FROM control.{schema}.openivm_refresh_leases;")
                == "openivm-simulated-takeover"
            ), point
            if point == "lease_lost_before_data":
                assert (
                    checker.value(bag_difference(view, "SELECT * FROM lake.main.before_refresh")) == "0"
                ), "a client past its lease deadline wrote DuckLake data"
            # The takeover's lease has expired; the next client recovers by recomputing.
            checker.run(f"PRAGMA refresh('{view}');\n")
            assert checker.value(bag_difference(view, base)) == "0", f"{point}: recovery lost or doubled changes"
            assert checker.value(f"SELECT refresh_in_progress FROM control.{schema}.openivm_views;") == "false"
            assert checker.value(f"SELECT count(*) FROM control.{schema}.openivm_refresh_leases;") == "0"
    finally:
        drop_schema(binary, dsn, schema)


def main():
    if len(sys.argv) < 2:
        raise SystemExit("usage: test_remote_metadata.py <duckdb binary> [--require]")
    binary = Path(sys.argv[1]).resolve()
    dsn = os.environ.get("OPENIVM_POSTGRES_DSN", "")
    if not dsn:
        if "--require" in sys.argv[2:]:
            raise SystemExit("OPENIVM_POSTGRES_DSN is required but not set")
        print("SKIP: OPENIVM_POSTGRES_DSN is not set; remote metadata integration test not run")
        return
    with tempfile.TemporaryDirectory(prefix="openivm-remote-metadata-") as directory:
        root = Path(directory)
        native_host_scenario(binary, dsn, root)
        ducklake_concurrency_scenario(binary, dsn, root)
        ducklake_lease_scenario(binary, dsn, root)
    print("Remote PostgreSQL metadata: placement, reopen, crash recovery, lease takeover and concurrent clients passed")


if __name__ == "__main__":
    main()
