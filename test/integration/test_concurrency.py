#!/usr/bin/env python3
"""Run deterministic lock interleavings with a process deadline for deadlocks."""

import subprocess
import sys
import tempfile
from pathlib import Path

with tempfile.TemporaryDirectory(prefix="openivm-concurrency-") as directory:
    subprocess.run(
        [str(Path(sys.argv[1]).resolve()), directory],
        check=True,
        timeout=120,
    )
