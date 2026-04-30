#!/usr/bin/env python3
"""
POC 1: Goldilocks zone for IVM vs bypass on a single-table aggregate MV.

For varying delta fractions f, measures two strategies:
  A. IVM       — PRAGMA ivm('mv') + SELECT * FROM mv
  B. BYPASS    — run the original SELECT against the current base (no MV)

For single-table aggregate MVs, "rebuild" (full recompute of MV from base) is
the same cost as "bypass" (run the query directly), so we don't measure it
separately. Rebuild matters for POC 2 (MV-on-MV DAG, where rebuild means
rebuilding the chain).

Each (f, strategy) runs in a fresh DB to avoid state leakage. Reports CSV +
a textual crossover summary.

Usage:
    python3 goldilocks_poc.py [--base-rows N] [--reps R] [--out path.csv]
"""

from __future__ import annotations

import argparse
import csv
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DUCKDB = "/home/ila/Code/openivm/build/release/duckdb"
EXT = "/home/ila/Code/openivm/build/release/extension/openivm/openivm.duckdb_extension"

DEFAULT_BASE_ROWS = 200_000
DEFAULT_DELTAS = [0.001, 0.005, 0.01, 0.02, 0.05, 0.10, 0.20, 0.50, 1.00]
DEFAULT_REPS = 3


def run_sql(db_path: str, sql: str, timeout: int = 300) -> tuple[str, str, int]:
	"""Run SQL through the duckdb CLI. Returns (stdout, stderr, returncode)."""
	preamble = f"LOAD '{EXT}';\n"
	res = subprocess.run(
		[DUCKDB, db_path],
		input=preamble + sql,
		capture_output=True,
		text=True,
		timeout=timeout,
	)
	return res.stdout, res.stderr, res.returncode


def make_base_sql(n: int) -> str:
	"""SQL to create the orders table and populate with N deterministic rows."""
	return f"""
CREATE TABLE orders(
    o_id INTEGER,
    o_region VARCHAR,
    o_customer INTEGER,
    o_amount DOUBLE
);
INSERT INTO orders
SELECT
    i AS o_id,
    (['us-east','us-west','eu','asia','latam'])[(i % 5) + 1] AS o_region,
    (i % 10000) AS o_customer,
    ((i * 2654435761) % 1000000) / 100.0 AS o_amount
FROM range({n}) t(i);
"""


# The view definition — a single-table aggregate.
# NOTE: OpenIVM's parser extension for CREATE MATERIALIZED VIEW does not handle
# newlines between AS and SELECT (collapses them), so keep it on one line.
MV_DEFINITION = (
	"CREATE MATERIALIZED VIEW mv AS "
	"SELECT o_region, SUM(o_amount) AS total, COUNT(*) AS cnt "
	"FROM orders GROUP BY o_region;\n"
)

# Same query, run directly against the base — this is the "bypass" strategy.
BYPASS_QUERY = (
	"SELECT o_region, SUM(o_amount) AS total, COUNT(*) AS cnt "
	"FROM orders GROUP BY o_region;\n"
)


def insert_delta_sql(start_id: int, count: int) -> str:
	"""Append `count` fresh rows to orders starting at id=start_id."""
	return f"""
INSERT INTO orders
SELECT
    {start_id} + i AS o_id,
    (['us-east','us-west','eu','asia','latam'])[((({start_id} + i) * 7919) % 5) + 1] AS o_region,
    (({start_id} + i) % 10000) AS o_customer,
    (((({start_id} + i) * 2654435761) % 1000000)) / 100.0 AS o_amount
FROM range({count}) t(i);
"""


def time_strategy(db_path: str, strategy: str) -> float:
	"""
	Time the named strategy on the given DB. Returns wall seconds.
	Strategies:
	  ivm     — PRAGMA ivm('mv') + SELECT * FROM mv
	  bypass  — run the query directly against base
	  rebuild — DROP MATERIALIZED VIEW mv + CREATE MATERIALIZED VIEW mv + SELECT * FROM mv
	"""
	if strategy == "ivm":
		sql = "PRAGMA ivm('mv');\nSELECT * FROM mv;"
	elif strategy == "bypass":
		sql = BYPASS_QUERY
	else:
		raise ValueError(strategy)

	start = time.perf_counter()
	out, err, rc = run_sql(db_path, sql)
	elapsed = time.perf_counter() - start
	if rc != 0:
		raise RuntimeError(f"strategy={strategy} failed:\nSTDOUT:\n{out}\nSTDERR:\n{err}")
	return elapsed


def one_run(base_rows: int, delta_fraction: float, strategy: str) -> float:
	"""Single trial: fresh DB, populate, create MV, sync, insert delta, time strategy."""
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")

		# Setup: base table + MV + sync so delta_orders starts empty.
		setup_sql = make_base_sql(base_rows) + MV_DEFINITION + "PRAGMA ivm('mv');\n"
		out, err, rc = run_sql(db, setup_sql)
		if rc != 0:
			raise RuntimeError(f"setup failed: {err}")

		# Insert delta rows (they flow into delta_orders automatically).
		delta_rows = int(base_rows * delta_fraction)
		if delta_rows < 1:
			delta_rows = 1
		delta_sql = insert_delta_sql(start_id=base_rows, count=delta_rows)
		out, err, rc = run_sql(db, delta_sql)
		if rc != 0:
			raise RuntimeError(f"delta insert failed: {err}")

		return time_strategy(db, strategy)


def run_matrix(base_rows: int, deltas: list[float], reps: int) -> list[dict]:
	rows = []
	for f in deltas:
		for strategy in ("ivm", "bypass"):
			samples = []
			for rep in range(reps):
				try:
					t = one_run(base_rows, f, strategy)
					samples.append(t)
					print(
						f"  delta_frac={f:.4f} strategy={strategy:<7} "
						f"rep={rep+1}/{reps} t={t*1000:8.1f}ms",
						file=sys.stderr,
					)
				except Exception as e:
					print(f"  ERROR f={f} strategy={strategy} rep={rep}: {e}", file=sys.stderr)
			if not samples:
				continue
			rows.append({
				"base_rows": base_rows,
				"delta_fraction": f,
				"delta_rows": int(base_rows * f) if int(base_rows * f) else 1,
				"strategy": strategy,
				"reps": len(samples),
				"median_s": statistics.median(samples),
				"min_s": min(samples),
				"max_s": max(samples),
			})
	return rows


def summarize(rows: list[dict]) -> None:
	"""Print a simple crossover table per delta fraction."""
	print("\n=== Crossover summary ===")
	print(f"{'delta_frac':>10}  {'ivm(ms)':>10}  {'bypass(ms)':>12}  winner  ratio(bypass/ivm)")
	by_frac: dict[float, dict[str, float]] = {}
	for r in rows:
		by_frac.setdefault(r["delta_fraction"], {})[r["strategy"]] = r["median_s"] * 1000
	for f in sorted(by_frac):
		pair = by_frac[f]
		winner = min(pair, key=pair.get)
		ivm_ms = pair.get("ivm", float("nan"))
		bypass_ms = pair.get("bypass", float("nan"))
		ratio = bypass_ms / ivm_ms if ivm_ms else float("nan")
		print(
			f"{f:>10.4f}  {ivm_ms:>10.1f}  {bypass_ms:>12.1f}  "
			f"{winner:<8} {ratio:>10.2f}x"
		)


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--base-rows", type=int, default=DEFAULT_BASE_ROWS)
	ap.add_argument("--reps", type=int, default=DEFAULT_REPS)
	ap.add_argument(
		"--deltas",
		type=str,
		default=",".join(str(d) for d in DEFAULT_DELTAS),
		help="comma-separated delta fractions",
	)
	ap.add_argument("--out", type=str, default="poc1_results.csv")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]

	print(f"POC 1: base_rows={args.base_rows} reps={args.reps} deltas={deltas}", file=sys.stderr)
	print(f"DuckDB: {DUCKDB}", file=sys.stderr)
	print(f"Extension: {EXT}", file=sys.stderr)

	t0 = time.time()
	rows = run_matrix(args.base_rows, deltas, args.reps)
	print(f"[total wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	out_path = Path(args.out)
	with out_path.open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=[
				"base_rows",
				"delta_fraction",
				"delta_rows",
				"strategy",
				"reps",
				"median_s",
				"min_s",
				"max_s",
			],
			extrasaction="ignore",
		)
		w.writeheader()
		w.writerows(rows)
	print(f"wrote {out_path.resolve()}", file=sys.stderr)

	summarize(rows)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
