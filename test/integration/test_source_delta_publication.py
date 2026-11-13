#!/usr/bin/env python3
"""Run namespace lifecycle checks with an isolated database for real reopen coverage."""

import subprocess
import sys
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory(prefix="openivm-lifecycle-") as directory:
    subprocess.run(
        [str(Path(sys.argv[1]).resolve()), str(Path(directory) / "namespace.db")],
        check=True,
    )
