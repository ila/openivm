#!/usr/bin/env python3
"""
POC 3 — Chained MV (MV-on-MV cascade). The real Tier 3 test.

Setup:
  base:   orders ⋈ lineitem
  mv_a:   orders ⋈ lineitem, grouped by (region, product)   [silver]
  mv_b:   further rolled up to region                        [gold, defined over mv_a]

Scenario: delta inserted into lineitem. mv_a is stale, mv_b is stale.
Compare four strategies for a query that matches mv_b's shape:

  A. BYPASS       — run the full base query (3-way logical: join + agg)
  B. STALE_B      — read mv_b as-is (wrong answer; baseline for zero-refresh cost)
  C. CASCADE      — PRAGMA refresh('mv_a'); PRAGMA refresh('mv_b'); SELECT * FROM mv_b
  D. CASCADE_AUTO — PRAGMA refresh('mv_b') (OpenIVM's cascade setting handles the chain)

Expectation: CASCADE (C) beats BYPASS (A) for moderate delta sizes because
mv_b's query is cheap once mv_a is refreshed. STALE_B (B) is the absolute
lower-bound — what you'd pay if you skipped correctness. CASCADE_AUTO (D)
should match C but with less orchestration overhead.

Usage:
    python3 chained_mv_poc.py --n-orders 500000 --reps 3
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

DEFAULT_DELTAS = [0.001, 0.005, 0.01, 0.05, 0.1, 0.2, 0.5]
STRATEGIES = ("bypass", "stale_b", "cascade", "cascade_auto", "stale_plus_residual")


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
	n_lineitem = n_orders * avg_li
	return f"""
CREATE TABLE orders(
    o_id INTEGER PRIMARY KEY, o_region VARCHAR, o_customer INTEGER
);
CREATE TABLE lineitem(
    l_id INTEGER, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE
);
INSERT INTO orders
SELECT i, (['us-east','us-west','eu','asia','latam'])[(i % 5) + 1],
       (i % 10000)
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, (i % {n_orders}), (i % 500), 1 + (i % 10),
       ((i * 2654435761) % 1000000) / 100.0
FROM range({n_lineitem}) t(i);
"""


# Silver: join + group by (region, product)
MV_A = (
	"CREATE MATERIALIZED VIEW mv_a AS "
	"SELECT o.o_region, l.l_product, SUM(l.l_qty * l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id = o.o_id "
	"GROUP BY o.o_region, l.l_product;\n"
)

# Gold: rollup of mv_a to region only
MV_B = (
	"CREATE MATERIALIZED VIEW mv_b AS "
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
	"FROM mv_a GROUP BY o_region;\n"
)

# "What mv_b represents" — bypass query is this against base tables
BYPASS_QUERY = (
	"SELECT o.o_region, SUM(l.l_qty * l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id = o.o_id "
	"GROUP BY o.o_region;\n"
)


def insert_delta_sql(start_li: int, count: int, n_orders: int) -> str:
	return f"""
INSERT INTO lineitem
SELECT {start_li} + i, (({start_li} + i) % {n_orders}),
       (({start_li} + i) % 500), 1 + (({start_li} + i) % 10),
       ((({start_li} + i) * 2654435761) % 1000000) / 100.0
FROM range({count}) t(i);
"""


def time_strategy(db_path: str, strategy: str) -> float:
	if strategy == "bypass":
		sql = BYPASS_QUERY
	elif strategy == "stale_b":
		# read mv_b without refreshing — intentionally stale
		sql = "SELECT * FROM mv_b;"
	elif strategy == "cascade":
		# explicit chain: refresh A first, then B
		sql = "PRAGMA refresh('mv_a');\nPRAGMA ivm('mv_b');\nSELECT * FROM mv_b;"
	elif strategy == "cascade_auto":
		# rely on OpenIVM's cascade setting (default 'downstream' — refreshing mv_a
		# propagates to mv_b). We still need to trigger the lowest level.
		sql = (
			"SET ivm_cascade_refresh='downstream';\n"
			"PRAGMA refresh('mv_a');\n"
			"SELECT * FROM mv_b;"
		)
	elif strategy == "stale_plus_residual":
		# The Tier 2 novelty: DO NOT refresh. Read stale mv_b + compute delta
		# contribution inline from delta_lineitem + orders, then aggregate.
		# Correct under bag semantics because mv_b = Q(T_old) and delta gives
		# the diff to Q(T_new), so re-aggregating their union yields Q(T_new).
		# Multiplicity = true means insert (+1), false means delete (-1).
		sql = (
			"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
			"FROM ( "
			"    SELECT o_region, revenue, cnt FROM mv_b "
			"    UNION ALL "
			"    SELECT o.o_region, "
			"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
			"                    ELSE -dl.l_qty*dl.l_price END) AS revenue, "
			"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS cnt "
			"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id = o.o_id "
			"    GROUP BY o.o_region "
			") x GROUP BY o_region;"
		)
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
		setup = (
			setup_sql(n_orders, avg_li)
			+ MV_A
			+ MV_B
			+ "PRAGMA refresh('mv_a');\n"
			+ "PRAGMA refresh('mv_b');\n"
		)
		out, err, rc = run_sql(db, setup)
		if rc != 0:
			raise RuntimeError(f"setup failed: {err}\n----\n{out}")
		delta_rows = max(1, int(n_lineitem * delta_fraction))
		out, err, rc = run_sql(
			db, insert_delta_sql(start_li=n_lineitem, count=delta_rows, n_orders=n_orders)
		)
		if rc != 0:
			raise RuntimeError(f"delta failed: {err}")
		return time_strategy(db, strategy)


def run_matrix(n_orders: int, avg_li: int, deltas: list[float], reps: int) -> list[dict]:
	rows = []
	n_lineitem = n_orders * avg_li
	for f in deltas:
		for strategy in STRATEGIES:
			samples = []
			for rep in range(reps):
				try:
					t = one_run(n_orders, avg_li, f, strategy)
					samples.append(t)
					print(
						f"  f={f:.4f} strat={strategy:<13} "
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
	print("\n=== Chained MV (mv_b on mv_a) crossover ===")
	header = (
		f"{'delta_frac':>10}  {'bypass':>8}  {'stale_b':>8}  "
		f"{'cascade':>8}  {'casc_auto':>10}  {'stl+res':>8}  winner        speedup"
	)
	print(header)
	by_frac: dict[float, dict[str, float]] = {}
	for r in rows:
		by_frac.setdefault(r["delta_fraction"], {})[r["strategy"]] = r["median_s"] * 1000
	for f in sorted(by_frac):
		d = by_frac[f]
		# stale_b is not a correct strategy (wrong answer); exclude from winner search.
		correct = {k: v for k, v in d.items() if k != "stale_b"}
		winner = min(correct, key=correct.get) if correct else "?"
		best_correct_ms = min(correct.values()) if correct else float("nan")
		bypass_ms = d.get("bypass", float("nan"))
		speedup = bypass_ms / best_correct_ms if best_correct_ms else float("nan")
		print(
			f"{f:>10.4f}  "
			f"{d.get('bypass', float('nan')):>8.1f}  "
			f"{d.get('stale_b', float('nan')):>8.1f}  "
			f"{d.get('cascade', float('nan')):>8.1f}  "
			f"{d.get('cascade_auto', float('nan')):>10.1f}  "
			f"{d.get('stale_plus_residual', float('nan')):>8.1f}  "
			f"{winner:<13} {speedup:>6.2f}x"
		)


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--n-orders", type=int, default=500_000)
	ap.add_argument("--avg-li", type=int, default=4)
	ap.add_argument("--reps", type=int, default=3)
	ap.add_argument("--deltas", type=str, default=",".join(str(d) for d in DEFAULT_DELTAS))
	ap.add_argument("--out", type=str, default="poc3_chained.csv")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]
	n_lineitem = args.n_orders * args.avg_li
	print(
		f"POC 3 (chained MV): n_orders={args.n_orders} n_lineitem={n_lineitem} "
		f"reps={args.reps} deltas={deltas}",
		file=sys.stderr,
	)
	t0 = time.time()
	rows = run_matrix(args.n_orders, args.avg_li, deltas, args.reps)
	print(f"[total wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	with Path(args.out).open("w", newline="") as fp:
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

	summarize(rows)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
