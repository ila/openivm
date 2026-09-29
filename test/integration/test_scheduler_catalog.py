#!/usr/bin/env python3
"""Native metadata discovery and ALTER must not access an unrelated, locked DuckLake catalog."""

import sqlite3
import subprocess
import sys
import tempfile
from contextlib import closing
from pathlib import Path

with tempfile.TemporaryDirectory(prefix="openivm-scheduler-") as directory:
    catalog = str(Path(directory) / "lake.sqlite")
    with subprocess.Popen(
        [str(Path(sys.argv[1]).resolve()), catalog],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        try:
            ready = process.stdout.readline().strip()
            if ready != "READY":
                raise AssertionError(f"Setup failed: {ready} {process.stderr.read()}")
            with closing(sqlite3.connect(catalog, timeout=1)) as lock:
                lock.execute("BEGIN EXCLUSIVE")
                try:
                    stdout, stderr = process.communicate("scan\n", timeout=15)
                finally:
                    lock.rollback()
            assert process.returncode == 0, stderr
            assert stdout.strip() == "PASS", (stdout, stderr)
        finally:
            if process.poll() is None:
                process.kill()
                process.communicate()
print("Native scheduling and delta schema synchronization work with locked DuckLake metadata")
