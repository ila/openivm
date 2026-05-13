#!/usr/bin/env python3
"""
POC 7 — Amortization: one delta, N queries.

Real dashboards hit the same MV many times per refresh cycle. This POC sweeps
N ∈ {1, 2, 5, 10, 20} and measures total wallclock for four strategies:

  bypass_N          — run Q against base N times
  cascade_once_N    — refresh chain once, then scan MV N times (amortized)
  stale_residual_N  — run stale+residual N times (no amortization)
  hybrid_one_refresh — run stale+residual 1st query, cascade the rest (the
                      matcher's opportunistic strategy)

Hypothesis: Tier 2 (stale+residual) wins for small N; Tier 3 (cascade_once)
wins for large N; crossover depends on pipeline depth and delta size.

This is what makes DASHBOARD workloads the killer use case for cascade.
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


def run_sql(db: str, sql: str, timeout: int = 600) -> tuple[str, str, int]:
	res = subprocess.run(
		[DUCKDB, db],
		input=f"LOAD '{EXT}';\n{sql}",
		capture_output=True,
		text=True,
		timeout=timeout,
	)
	return res.stdout, res.stderr, res.returncode


def setup(n_orders: int, avg_li: int) -> str:
	n_lineitem = n_orders * avg_li
	return f"""
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_region VARCHAR, o_month INTEGER);
CREATE TABLE lineitem(l_id INTEGER, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);
INSERT INTO orders
SELECT i, (['us-east','us-west','eu','asia','latam'])[(i%5)+1], 1+(i%12)
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
CREATE MATERIALIZED VIEW mv_a AS SELECT o.o_region, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY o.o_region, l.l_product;
CREATE MATERIALIZED VIEW mv_b AS SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM mv_a GROUP BY o_region;
PRAGMA refresh('mv_a'); PRAGMA refresh('mv_b');
"""


BYPASS = (
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region;"
)
SCAN_MV = "SELECT * FROM mv_b;"
STALE_RES = (
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
	"    SELECT o_region, revenue, cnt FROM mv_b "
	"    UNION ALL "
	"    SELECT o.o_region, "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
	"                    ELSE -dl.l_qty*dl.l_price END), "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) "
	"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
	"    GROUP BY o.o_region "
	") x GROUP BY o_region;"
)
CASCADE = (
	"SET ivm_cascade_refresh='downstream';\n"
	"PRAGMA refresh('mv_a');\n"
)


def insert_delta_sql(start: int, count: int, n_orders: int) -> str:
	return (
		f"INSERT INTO lineitem "
		f"SELECT {start}+i, ({start}+i)%{n_orders}, ({start}+i)%500, "
		f"1+(({start}+i)%10), ((({start}+i)*2654435761)%1000000)/100.0 "
		f"FROM range({count}) t(i);"
	)


def scenario_bypass_N(db: str, n_queries: int) -> float:
	"""N bypass queries."""
	sql = BYPASS * n_queries  # concat same query N times
	start = time.perf_counter()
	out, err, rc = run_sql(db, sql)
	if rc != 0:
		raise RuntimeError(err)
	return time.perf_counter() - start


def scenario_cascade_once_N(db: str, n_queries: int) -> float:
	"""One cascade refresh + N scans of mv_b."""
	sql = CASCADE + (SCAN_MV * n_queries)
	start = time.perf_counter()
	out, err, rc = run_sql(db, sql)
	if rc != 0:
		raise RuntimeError(err)
	return time.perf_counter() - start


def scenario_stale_residual_N(db: str, n_queries: int) -> float:
	"""N stale+residual queries (no amortization)."""
	sql = STALE_RES * n_queries
	start = time.perf_counter()
	out, err, rc = run_sql(db, sql)
	if rc != 0:
		raise RuntimeError(err)
	return time.perf_counter() - start


def scenario_hybrid(db: str, n_queries: int) -> float:
	"""1st query via stale+residual, then cascade, then N-1 MV scans.
	Matcher's opportunistic: answer the first query fast with stale+residual,
	then refresh in the background (here inline) before subsequent queries."""
	if n_queries == 1:
		return scenario_stale_residual_N(db, 1)
	sql = STALE_RES + CASCADE + (SCAN_MV * (n_queries - 1))
	start = time.perf_counter()
	out, err, rc = run_sql(db, sql)
	if rc != 0:
		raise RuntimeError(err)
	return time.perf_counter() - start


def one_run(n_orders: int, avg_li: int, delta_fraction: float, n_queries: int, strategy: str) -> float:
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		out, err, rc = run_sql(db, setup(n_orders, avg_li))
		if rc != 0:
			raise RuntimeError(f"setup: {err}")
		delta_rows = max(1, int(n_lineitem * delta_fraction))
		run_sql(db, insert_delta_sql(n_lineitem, delta_rows, n_orders))
		if strategy == "bypass_N":
			return scenario_bypass_N(db, n_queries)
		elif strategy == "cascade_once_N":
			return scenario_cascade_once_N(db, n_queries)
		elif strategy == "stale_residual_N":
			return scenario_stale_residual_N(db, n_queries)
		elif strategy == "hybrid":
			return scenario_hybrid(db, n_queries)
		else:
			raise ValueError(strategy)


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--n-orders", type=int, default=1_500_000)
	ap.add_argument("--avg-li", type=int, default=4)
	ap.add_argument("--reps", type=int, default=2)
	ap.add_argument("--delta", type=float, default=0.01, help="single delta fraction to test")
	ap.add_argument("--n-queries", type=str, default="1,2,5,10,20")
	ap.add_argument("--out", type=str, default="poc7_amortization.csv")
	args = ap.parse_args()

	ns = [int(x) for x in args.n_queries.split(",")]
	strategies = ("bypass_N", "cascade_once_N", "stale_residual_N", "hybrid")
	print(
		f"POC 7 (amortization): n_orders={args.n_orders} avg_li={args.avg_li} "
		f"delta={args.delta} reps={args.reps} N_queries={ns}",
		file=sys.stderr,
	)

	rows = []
	t0 = time.time()
	for n in ns:
		for strategy in strategies:
			samples = []
			for rep in range(args.reps):
				try:
					t = one_run(args.n_orders, args.avg_li, args.delta, n, strategy)
					samples.append(t)
					print(
						f"  N={n:>3} strat={strategy:<20} "
						f"rep={rep+1}/{args.reps} t={t*1000:7.1f}ms "
						f"(per-query {t*1000/n:5.1f}ms)",
						file=sys.stderr,
					)
				except Exception as e:
					print(f"  N={n} strat={strategy}: ERROR {e}", file=sys.stderr)
			if samples:
				rows.append({
					"n_queries": n,
					"strategy": strategy,
					"reps": len(samples),
					"total_median_s": statistics.median(samples),
					"per_query_median_ms": statistics.median(samples) * 1000 / n,
				})
	print(f"[wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	with Path(args.out).open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=["n_queries", "strategy", "reps", "total_median_s", "per_query_median_ms"],
			extrasaction="ignore",
		)
		w.writeheader()
		w.writerows(rows)

	# Summary
	print(f"\n=== Amortization (delta={args.delta}) — total wall time in ms ===")
	print(
		f"{'N_queries':>10}  {'bypass':>9}  {'casc_once':>10}  "
		f"{'stl+res':>9}  {'hybrid':>8}  winner"
	)
	by = {}
	for r in rows:
		by.setdefault(r["n_queries"], {})[r["strategy"]] = r["total_median_s"] * 1000
	for n in sorted(by):
		d = by[n]
		winner = min(d, key=d.get)
		print(
			f"{n:>10}  "
			f"{d.get('bypass_N', float('nan')):>9.1f}  "
			f"{d.get('cascade_once_N', float('nan')):>10.1f}  "
			f"{d.get('stale_residual_N', float('nan')):>9.1f}  "
			f"{d.get('hybrid', float('nan')):>8.1f}  "
			f"{winner}"
		)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
