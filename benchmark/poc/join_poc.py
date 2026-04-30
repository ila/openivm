#!/usr/bin/env python3
"""
POC 2 (join version): Goldilocks zone for IVM vs bypass on a 2-way join MV.

Scenario: lineitem ⋈ orders, aggregated by region. MV defined over the join.
Bypass = run the join + aggregate against the base tables.
IVM    = PRAGMA ivm('mv') applies the delta via inclusion-exclusion, then scan.

For varying delta fractions on lineitem (the "hot" fact table), measures:
  A. IVM     — PRAGMA ivm('mv') + SELECT * FROM mv
  B. BYPASS  — run the join + aggregate on current base tables

Expectation: since bypass has to join (cost scales with |lineitem| × selectivity),
the Goldilocks zone should open up — IVM wins when delta is small relative to base.
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

DEFAULT_ORDERS = 100_000
DEFAULT_LINEITEM_PER_ORDER = 4  # avg
DEFAULT_DELTAS = [0.0005, 0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5]
DEFAULT_REPS = 3


def run_sql(db_path: str, sql: str, timeout: int = 600) -> tuple[str, str, int]:
	preamble = f"LOAD '{EXT}';\n"
	res = subprocess.run(
		[DUCKDB, db_path],
		input=preamble + sql,
		capture_output=True,
		text=True,
		timeout=timeout,
	)
	return res.stdout, res.stderr, res.returncode


def setup_sql(n_orders: int, avg_li: int) -> str:
	"""Build orders + lineitem with a deterministic FK join."""
	n_lineitem = n_orders * avg_li
	return f"""
CREATE TABLE orders(
    o_id INTEGER PRIMARY KEY,
    o_region VARCHAR,
    o_customer INTEGER,
    o_date DATE
);
CREATE TABLE lineitem(
    l_id INTEGER,
    l_order_id INTEGER,
    l_product INTEGER,
    l_qty INTEGER,
    l_price DOUBLE
);
INSERT INTO orders
SELECT
    i AS o_id,
    (['us-east','us-west','eu','asia','latam'])[(i % 5) + 1] AS o_region,
    (i % 10000) AS o_customer,
    DATE '2024-01-01' + INTERVAL (i % 365) DAY AS o_date
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT
    i AS l_id,
    (i % {n_orders}) AS l_order_id,
    (i % 500) AS l_product,
    1 + (i % 10) AS l_qty,
    ((i * 2654435761) % 1000000) / 100.0 AS l_price
FROM range({n_lineitem}) t(i);
"""


MV_DEFINITION = (
	"CREATE MATERIALIZED VIEW mv AS "
	"SELECT o.o_region, SUM(l.l_qty * l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id = o.o_id "
	"GROUP BY o.o_region;\n"
)

BYPASS_QUERY = (
	"SELECT o.o_region, SUM(l.l_qty * l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id = o.o_id "
	"GROUP BY o.o_region;\n"
)


def insert_delta_sql(start_li: int, count: int, n_orders: int) -> str:
	"""Append `count` lineitem rows (FK-valid) starting at l_id=start_li."""
	return f"""
INSERT INTO lineitem
SELECT
    {start_li} + i AS l_id,
    (({start_li} + i) % {n_orders}) AS l_order_id,
    (({start_li} + i) % 500) AS l_product,
    1 + (({start_li} + i) % 10) AS l_qty,
    ((({start_li} + i) * 2654435761) % 1000000) / 100.0 AS l_price
FROM range({count}) t(i);
"""


def time_strategy(db_path: str, strategy: str) -> float:
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
		raise RuntimeError(f"strategy={strategy} failed:\n{err}")
	return elapsed


def one_run(n_orders: int, avg_li: int, delta_fraction: float, strategy: str) -> float:
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		setup = setup_sql(n_orders, avg_li) + MV_DEFINITION + "PRAGMA ivm('mv');\n"
		out, err, rc = run_sql(db, setup)
		if rc != 0:
			raise RuntimeError(f"setup failed: {err}")
		delta_rows = max(1, int(n_lineitem * delta_fraction))
		delta = insert_delta_sql(start_li=n_lineitem, count=delta_rows, n_orders=n_orders)
		out, err, rc = run_sql(db, delta)
		if rc != 0:
			raise RuntimeError(f"delta failed: {err}")
		return time_strategy(db, strategy)


def run_matrix(n_orders: int, avg_li: int, deltas: list[float], reps: int) -> list[dict]:
	rows = []
	n_lineitem = n_orders * avg_li
	for f in deltas:
		for strategy in ("ivm", "bypass"):
			samples = []
			for rep in range(reps):
				try:
					t = one_run(n_orders, avg_li, f, strategy)
					samples.append(t)
					print(
						f"  f={f:.4f} strat={strategy:<7} "
						f"rep={rep+1}/{reps} t={t*1000:8.1f}ms",
						file=sys.stderr,
					)
				except Exception as e:
					print(f"  ERROR f={f} strat={strategy}: {e}", file=sys.stderr)
			if not samples:
				continue
			rows.append({
				"n_orders": n_orders,
				"n_lineitem": n_lineitem,
				"delta_fraction": f,
				"delta_rows": max(1, int(n_lineitem * f)),
				"strategy": strategy,
				"reps": len(samples),
				"median_s": statistics.median(samples),
				"min_s": min(samples),
				"max_s": max(samples),
			})
	return rows


def summarize(rows: list[dict]) -> None:
	print("\n=== Crossover summary (2-way join) ===")
	print(f"{'delta_frac':>10}  {'ivm(ms)':>10}  {'bypass(ms)':>12}  winner   speedup(bypass/ivm)")
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
	ap.add_argument("--n-orders", type=int, default=DEFAULT_ORDERS)
	ap.add_argument("--avg-li", type=int, default=DEFAULT_LINEITEM_PER_ORDER)
	ap.add_argument("--reps", type=int, default=DEFAULT_REPS)
	ap.add_argument(
		"--deltas",
		type=str,
		default=",".join(str(d) for d in DEFAULT_DELTAS),
	)
	ap.add_argument("--out", type=str, default="poc2_results.csv")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]
	n_lineitem = args.n_orders * args.avg_li
	print(
		f"POC 2 (2-way join): n_orders={args.n_orders} n_lineitem={n_lineitem} "
		f"reps={args.reps} deltas={deltas}",
		file=sys.stderr,
	)

	t0 = time.time()
	rows = run_matrix(args.n_orders, args.avg_li, deltas, args.reps)
	print(f"[total wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	out_path = Path(args.out)
	with out_path.open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=[
				"n_orders",
				"n_lineitem",
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
