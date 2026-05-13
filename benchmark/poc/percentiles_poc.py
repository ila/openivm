#!/usr/bin/env python3
"""
POC 10 — Latency distributions (p50/p90/p95/p99 + min/max).

Real dashboards care about tail latency, not just median. Re-run the headline
chain3 + Tier 2 scenario at 6M with 10 reps per data point to get percentile
distributions and confirm the speedups are stable, not artifacts.

Three deltas: 0.1%, 1%, 10%.
Three strategies: bypass / cascade / stale_residual.
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
INSERT INTO orders SELECT i, (['us-east','us-west','eu','asia','latam'])[(i%5)+1], 1+(i%12) FROM range({n_orders}) t(i);
INSERT INTO lineitem SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0 FROM range({n_lineitem}) t(i);
CREATE MATERIALIZED VIEW mv_a AS SELECT o.o_region, o.o_month, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY o.o_region, o.o_month, l.l_product;
CREATE MATERIALIZED VIEW mv_b AS SELECT o_region, o_month, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM mv_a GROUP BY o_region, o_month;
CREATE MATERIALIZED VIEW mv_c AS SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM mv_b GROUP BY o_region;
PRAGMA refresh('mv_a'); PRAGMA refresh('mv_b'); PRAGMA refresh('mv_c');
"""


BYPASS = "SELECT o.o_region, SUM(l.l_qty*l.l_price), COUNT(*) FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY o.o_region;"
CASCADE = "SET ivm_cascade_refresh='downstream';\nPRAGMA ivm('mv_a');\nSELECT * FROM mv_c;"
STALE_RES = (
	"SELECT o_region, SUM(revenue), SUM(cnt) FROM ( "
	"  SELECT o_region, revenue, cnt FROM mv_c "
	"  UNION ALL "
	"  SELECT o.o_region, "
	"    SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price ELSE -dl.l_qty*dl.l_price END), "
	"    SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) "
	"  FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id GROUP BY o.o_region "
	") x GROUP BY o_region;"
)


def insert_delta(start: int, count: int, n_orders: int) -> str:
	return (
		f"INSERT INTO lineitem SELECT {start}+i, ({start}+i)%{n_orders}, "
		f"({start}+i)%500, 1+(({start}+i)%10), "
		f"((({start}+i)*2654435761)%1000000)/100.0 FROM range({count}) t(i);"
	)


def one_run(n_orders: int, avg_li: int, frac: float, strategy: str) -> float:
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		out, err, rc = run_sql(db, setup(n_orders, avg_li))
		if rc != 0:
			raise RuntimeError(err)
		run_sql(db, insert_delta(n_lineitem, max(1, int(n_lineitem * frac)), n_orders))
		sql = {"bypass": BYPASS, "cascade": CASCADE, "stale_residual": STALE_RES}[strategy]
		start = time.perf_counter()
		out, err, rc = run_sql(db, sql)
		t = time.perf_counter() - start
		if rc != 0:
			raise RuntimeError(err)
		return t


def percentile(samples: list[float], p: float) -> float:
	"""Linear-interpolation percentile."""
	xs = sorted(samples)
	if not xs:
		return float("nan")
	k = (len(xs) - 1) * p
	f, c = int(k), min(int(k) + 1, len(xs) - 1)
	if f == c:
		return xs[f]
	return xs[f] + (xs[c] - xs[f]) * (k - f)


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--n-orders", type=int, default=1_500_000)
	ap.add_argument("--avg-li", type=int, default=4)
	ap.add_argument("--reps", type=int, default=10)
	ap.add_argument("--deltas", type=str, default="0.001,0.01,0.1")
	ap.add_argument("--out", type=str, default="poc10_percentiles.csv")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]
	print(
		f"POC 10 (percentiles): n_orders={args.n_orders} reps={args.reps} deltas={deltas}",
		file=sys.stderr,
	)

	rows = []
	t0 = time.time()
	for f in deltas:
		for strategy in ("bypass", "cascade", "stale_residual"):
			samples = []
			for rep in range(args.reps):
				try:
					t = one_run(args.n_orders, args.avg_li, f, strategy)
					samples.append(t)
				except Exception as e:
					print(f"  ERROR f={f} strat={strategy} rep={rep}: {e}", file=sys.stderr)
			if not samples:
				continue
			rows.append({
				"delta_fraction": f,
				"strategy": strategy,
				"n_samples": len(samples),
				"min_ms": min(samples) * 1000,
				"p50_ms": percentile(samples, 0.5) * 1000,
				"p90_ms": percentile(samples, 0.9) * 1000,
				"p95_ms": percentile(samples, 0.95) * 1000,
				"p99_ms": percentile(samples, 0.99) * 1000,
				"max_ms": max(samples) * 1000,
				"mean_ms": statistics.mean(samples) * 1000,
				"stdev_ms": statistics.stdev(samples) * 1000 if len(samples) > 1 else 0.0,
			})
			print(
				f"  f={f:.3f} strat={strategy:<15} "
				f"p50={percentile(samples, 0.5)*1000:6.1f}ms "
				f"p95={percentile(samples, 0.95)*1000:6.1f}ms "
				f"max={max(samples)*1000:6.1f}ms",
				file=sys.stderr,
			)
	print(f"[wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	with Path(args.out).open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=[
				"delta_fraction", "strategy", "n_samples",
				"min_ms", "p50_ms", "p90_ms", "p95_ms", "p99_ms", "max_ms",
				"mean_ms", "stdev_ms",
			],
			extrasaction="ignore",
		)
		w.writeheader()
		w.writerows(rows)

	# Summary
	print("\n=== Latency distributions (chain3, 6M lineitem) ===")
	print(
		f"{'delta':>6}  {'strategy':<15}  {'p50':>6}  {'p90':>6}  "
		f"{'p95':>6}  {'p99':>6}  {'max':>6}  {'σ':>5}"
	)
	for r in rows:
		print(
			f"{r['delta_fraction']:>6.3f}  "
			f"{r['strategy']:<15}  "
			f"{r['p50_ms']:>6.1f}  "
			f"{r['p90_ms']:>6.1f}  "
			f"{r['p95_ms']:>6.1f}  "
			f"{r['p99_ms']:>6.1f}  "
			f"{r['max_ms']:>6.1f}  "
			f"{r['stdev_ms']:>5.1f}"
		)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
