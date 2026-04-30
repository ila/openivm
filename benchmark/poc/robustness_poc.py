#!/usr/bin/env python3
"""
POC 9 — Robustness: multi-table deltas, heavy deletes, skewed distributions.

Three robustness tests on the chain3 pipeline (base → mv_a → mv_b → mv_c):

  multi_table_delta — insert delta on BOTH orders (small) and lineitem (large).
                      Realistic OLTP-sourced warehouse pattern.
  heavy_delete      — pure-delete workloads at 6M scale.
  skewed_data       — Zipfian region distribution + power-law product distribution
                      with delta inserts concentrated on hot keys.

Compare bypass / cascade_auto / stale_plus_residual across realistic deltas.
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


# Uniform setup
def base_setup(n_orders: int, avg_li: int) -> str:
	n_lineitem = n_orders * avg_li
	return f"""
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_region VARCHAR, o_month INTEGER);
CREATE TABLE lineitem(l_id INTEGER PRIMARY KEY, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);
INSERT INTO orders
SELECT i, (['us-east','us-west','eu','asia','latam'])[(i%5)+1], 1+(i%12)
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""


# Skewed setup: 60% of orders in 'us-east', 25% 'us-west', 10% 'eu', 4% 'asia', 1% 'latam'
# Products follow power law: top 50 products see ~80% of activity
def skewed_setup(n_orders: int, avg_li: int) -> str:
	n_lineitem = n_orders * avg_li
	return f"""
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_region VARCHAR, o_month INTEGER);
CREATE TABLE lineitem(l_id INTEGER PRIMARY KEY, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);
-- Skewed region distribution via probabilistic mapping on i mod 100
INSERT INTO orders
SELECT i,
       CASE
         WHEN (i*7919) % 100 < 60 THEN 'us-east'
         WHEN (i*7919) % 100 < 85 THEN 'us-west'
         WHEN (i*7919) % 100 < 95 THEN 'eu'
         WHEN (i*7919) % 100 < 99 THEN 'asia'
         ELSE 'latam' END,
       1+(i%12)
FROM range({n_orders}) t(i);
-- Skewed product: 80% of rows hit products 0..49, rest spread over 50..499
INSERT INTO lineitem
SELECT i, i%{n_orders},
       CASE WHEN (i*2654435761) % 10 < 8 THEN i % 50 ELSE 50 + (i % 450) END,
       1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""


MV_CHAIN = (
	"CREATE MATERIALIZED VIEW mv_a AS "
	"SELECT o.o_region, o.o_month, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region, o.o_month, l.l_product;\n"
	"CREATE MATERIALIZED VIEW mv_b AS "
	"SELECT o_region, o_month, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
	"FROM mv_a GROUP BY o_region, o_month;\n"
	"CREATE MATERIALIZED VIEW mv_c AS "
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
	"FROM mv_b GROUP BY o_region;\n"
)

REFRESH_ALL = "PRAGMA ivm('mv_a');\nPRAGMA ivm('mv_b');\nPRAGMA ivm('mv_c');\n"

BYPASS = (
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY o.o_region;"
)
CASCADE = (
	"SET ivm_cascade_refresh='downstream';\n"
	"PRAGMA ivm('mv_a');\n"
	"SELECT * FROM mv_c;"
)
STALE_RES = (
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
	"    SELECT o_region, revenue, cnt FROM mv_c "
	"    UNION ALL "
	"    SELECT o.o_region, "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
	"                    ELSE -dl.l_qty*dl.l_price END), "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) "
	"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
	"    GROUP BY o.o_region "
	") x GROUP BY o_region;"
)


def insert_lineitem(start: int, count: int, n_orders: int) -> str:
	return (
		f"INSERT INTO lineitem "
		f"SELECT {start}+i, ({start}+i)%{n_orders}, ({start}+i)%500, "
		f"1+(({start}+i)%10), ((({start}+i)*2654435761)%1000000)/100.0 "
		f"FROM range({count}) t(i);"
	)


def insert_orders(start_o: int, count: int) -> str:
	return (
		f"INSERT INTO orders "
		f"SELECT {start_o}+i, "
		f"(['us-east','us-west','eu','asia','latam'])[(({start_o}+i)%5)+1], "
		f"1+(({start_o}+i)%12) "
		f"FROM range({count}) t(i);"
	)


def insert_skewed_lineitem(start: int, count: int, n_orders: int) -> str:
	# Hot keys: 80% of inserts hit products 0..49 (matches skewed distribution)
	return (
		f"INSERT INTO lineitem "
		f"SELECT {start}+i, ({start}+i)%{n_orders}, "
		f"CASE WHEN (({start}+i)*2654435761)%10 < 8 THEN ({start}+i)%50 "
		f"     ELSE 50 + (({start}+i)%450) END, "
		f"1+(({start}+i)%10), ((({start}+i)*2654435761)%1000000)/100.0 "
		f"FROM range({count}) t(i);"
	)


def apply_workload(db: str, scenario: str, n_orders: int, n_lineitem: int, frac: float) -> None:
	delta_li = max(1, int(n_lineitem * frac))
	if scenario == "multi_table_delta":
		# Small delta on orders (10% of lineitem delta), bigger delta on lineitem
		delta_o = max(1, delta_li // 10)
		sql = insert_orders(n_orders, delta_o) + "\n" + insert_lineitem(n_lineitem, delta_li, n_orders + delta_o)
	elif scenario == "heavy_delete":
		# Delete delta_li lowest-id rows
		sql = f"DELETE FROM lineitem WHERE l_id < {delta_li};"
	elif scenario == "skewed_data":
		sql = insert_skewed_lineitem(n_lineitem, delta_li, n_orders)
	else:
		raise ValueError(scenario)
	out, err, rc = run_sql(db, sql)
	if rc != 0:
		raise RuntimeError(f"workload failed: {err}")


def one_run(scenario: str, n_orders: int, avg_li: int, frac: float, strategy: str) -> float:
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		setup_func = skewed_setup if scenario == "skewed_data" else base_setup
		out, err, rc = run_sql(db, setup_func(n_orders, avg_li) + MV_CHAIN + REFRESH_ALL)
		if rc != 0:
			raise RuntimeError(f"setup: {err}")
		apply_workload(db, scenario, n_orders, n_lineitem, frac)
		sql = {"bypass": BYPASS, "cascade": CASCADE, "stale_residual": STALE_RES}[strategy]
		start = time.perf_counter()
		out, err, rc = run_sql(db, sql)
		t = time.perf_counter() - start
		if rc != 0:
			raise RuntimeError(f"{strategy}: {err[:300]}")
		return t


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--n-orders", type=int, default=1_500_000)
	ap.add_argument("--avg-li", type=int, default=4)
	ap.add_argument("--reps", type=int, default=3)
	ap.add_argument("--deltas", type=str, default="0.001,0.01,0.05,0.1,0.2")
	ap.add_argument("--scenarios", type=str, default="multi_table_delta,heavy_delete,skewed_data")
	ap.add_argument("--out", type=str, default="poc9_robustness.csv")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]
	scenarios = args.scenarios.split(",")
	print(
		f"POC 9 (robustness): n_orders={args.n_orders} avg_li={args.avg_li} "
		f"reps={args.reps} deltas={deltas} scenarios={scenarios}",
		file=sys.stderr,
	)

	rows = []
	t0 = time.time()
	for scen in scenarios:
		for f in deltas:
			for strategy in ("bypass", "cascade", "stale_residual"):
				samples = []
				for rep in range(args.reps):
					try:
						t = one_run(scen, args.n_orders, args.avg_li, f, strategy)
						samples.append(t)
						print(
							f"  [{scen:<18}] f={f:.3f} strat={strategy:<15} "
							f"rep={rep+1}/{args.reps} t={t*1000:7.1f}ms",
							file=sys.stderr,
						)
					except Exception as e:
						print(f"  [{scen}] f={f} strat={strategy}: ERROR {e}", file=sys.stderr)
				if samples:
					rows.append({
						"scenario": scen,
						"delta_fraction": f,
						"strategy": strategy,
						"reps": len(samples),
						"median_s": statistics.median(samples),
						"min_s": min(samples),
						"max_s": max(samples),
					})
	print(f"[wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	with Path(args.out).open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=["scenario", "delta_fraction", "strategy", "reps", "median_s", "min_s", "max_s"],
			extrasaction="ignore",
		)
		w.writeheader()
		w.writerows(rows)

	for scen in scenarios:
		print(f"\n=== {scen} ===")
		print(f"{'delta':>8}  {'bypass':>8}  {'cascade':>8}  {'stl+res':>8}  winner        speedup")
		by_f: dict = {}
		for r in rows:
			if r["scenario"] != scen:
				continue
			by_f.setdefault(r["delta_fraction"], {})[r["strategy"]] = r["median_s"] * 1000
		for f in sorted(by_f):
			d = by_f[f]
			winner = min(d, key=d.get)
			best = d[winner]
			bp = d.get("bypass", float("nan"))
			speedup = bp / best if best else float("nan")
			print(
				f"{f:>8.3f}  "
				f"{d.get('bypass', float('nan')):>8.1f}  "
				f"{d.get('cascade', float('nan')):>8.1f}  "
				f"{d.get('stale_residual', float('nan')):>8.1f}  "
				f"{winner:<13} {speedup:>6.2f}x"
			)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
