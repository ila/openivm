#!/usr/bin/env python3
"""
POC 8 — Fan-out and diamond pipeline topologies.

Two non-linear topologies:

  FAN-OUT (one base, three MVs branching):
       base (lineitem + orders)
       /      |        \\
     mv1     mv2       mv3
  (region)  (product) (month)

  DIAMOND (V-shape: two MVs feed a top MV):
       base
       /  \\
     mv_a  mv_b       (different groupings)
       \\  /
      mv_top          (joins/unions mv_a and mv_b)

For each, measure bypass vs cascade-refresh vs stale+residual when a delta
hits the base table.

Why this matters: real dbt/medallion DAGs are not linear chains. Fan-out
tests whether refresh cost is per-branch (N branches = N × refresh cost) or
amortizable. Diamond tests matcher behavior when a query could be answered
from either mv_a, mv_b, or mv_top.
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


def base_setup(n_orders: int, avg_li: int) -> str:
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
"""


# =============== FAN-OUT ===============

FANOUT_MVS = (
	"CREATE MATERIALIZED VIEW mv_region AS "
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY o.o_region;\n"
	"CREATE MATERIALIZED VIEW mv_product AS "
	"SELECT l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY l.l_product;\n"
	"CREATE MATERIALIZED VIEW mv_month AS "
	"SELECT o.o_month, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY o.o_month;\n"
)

# Query is against the region dimension (one of the fan-out branches)
FANOUT_BYPASS = (
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY o.o_region;"
)
FANOUT_CASCADE_ALL = (
	"PRAGMA refresh('mv_region');\n"
	"PRAGMA refresh('mv_product');\n"
	"PRAGMA refresh('mv_month');\n"
	"SELECT * FROM mv_region;"
)
FANOUT_CASCADE_ONE = (
	"PRAGMA refresh('mv_region');\n"
	"SELECT * FROM mv_region;"
)
FANOUT_STALE_RES = (
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
	"    SELECT o_region, revenue, cnt FROM mv_region "
	"    UNION ALL "
	"    SELECT o.o_region, "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
	"                    ELSE -dl.l_qty*dl.l_price END), "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) "
	"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
	"    GROUP BY o.o_region "
	") x GROUP BY o_region;"
)


# =============== DIAMOND ===============

DIAMOND_MVS = (
	# mv_a: group by (region, product)
	"CREATE MATERIALIZED VIEW mv_a AS "
	"SELECT o.o_region, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region, l.l_product;\n"
	# mv_b: group by (month, product)
	"CREATE MATERIALIZED VIEW mv_b AS "
	"SELECT o.o_month, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_month, l.l_product;\n"
	# mv_top: union of (region, product) and (month, product) is meaningful only
	# if we aggregate to a common dimension. Simpler diamond: mv_top = rollup of mv_a
	# to region. But then mv_b is unused. True diamond: mv_top joins mv_a and mv_b
	# on product, which is a bit contrived. Use: mv_top groups by product from mv_a.
	"CREATE MATERIALIZED VIEW mv_top AS "
	"SELECT l_product, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
	"FROM mv_a GROUP BY l_product;\n"
)

DIAMOND_BYPASS = (
	"SELECT l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id GROUP BY l.l_product;"
)
# Cascade — must refresh mv_a before mv_top (dependency), but mv_b is orthogonal
DIAMOND_CASCADE_NEEDED = (
	"PRAGMA refresh('mv_a');\n"
	"PRAGMA refresh('mv_top');\n"
	"SELECT * FROM mv_top;"
)
# Cascade with ALL MVs refreshed (wasteful if we don't need mv_b)
DIAMOND_CASCADE_ALL = (
	"PRAGMA refresh('mv_a');\n"
	"PRAGMA refresh('mv_b');\n"
	"PRAGMA refresh('mv_top');\n"
	"SELECT * FROM mv_top;"
)
DIAMOND_STALE_RES = (
	"SELECT l_product, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
	"    SELECT l_product, revenue, cnt FROM mv_top "
	"    UNION ALL "
	"    SELECT dl.l_product, "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
	"                    ELSE -dl.l_qty*dl.l_price END), "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) "
	"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
	"    GROUP BY dl.l_product "
	") x GROUP BY l_product;"
)


TOPOLOGIES = {
	"fanout": {
		"mvs": FANOUT_MVS,
		"mv_names": ["mv_region", "mv_product", "mv_month"],
		"strategies": {
			"bypass": FANOUT_BYPASS,
			"cascade_all": FANOUT_CASCADE_ALL,
			"cascade_smart": FANOUT_CASCADE_ONE,  # only the MV we read
			"stale_residual": FANOUT_STALE_RES,
		},
	},
	"diamond": {
		"mvs": DIAMOND_MVS,
		"mv_names": ["mv_a", "mv_b", "mv_top"],
		"strategies": {
			"bypass": DIAMOND_BYPASS,
			"cascade_all": DIAMOND_CASCADE_ALL,
			"cascade_smart": DIAMOND_CASCADE_NEEDED,  # skip mv_b
			"stale_residual": DIAMOND_STALE_RES,
		},
	},
}


def insert_delta(start: int, count: int, n_orders: int) -> str:
	return (
		f"INSERT INTO lineitem "
		f"SELECT {start}+i, ({start}+i)%{n_orders}, ({start}+i)%500, "
		f"1+(({start}+i)%10), ((({start}+i)*2654435761)%1000000)/100.0 "
		f"FROM range({count}) t(i);"
	)


def one_run(topology: str, n_orders: int, avg_li: int, delta_fraction: float, strategy: str) -> float:
	cfg = TOPOLOGIES[topology]
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		setup = base_setup(n_orders, avg_li) + cfg["mvs"]
		for name in cfg["mv_names"]:
			setup += f"PRAGMA refresh('{name}');\n"
		out, err, rc = run_sql(db, setup)
		if rc != 0:
			raise RuntimeError(f"setup: {err}")
		delta_rows = max(1, int(n_lineitem * delta_fraction))
		run_sql(db, insert_delta(n_lineitem, delta_rows, n_orders))
		sql = cfg["strategies"][strategy]
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
	ap.add_argument("--reps", type=int, default=2)
	ap.add_argument("--deltas", type=str, default="0.001,0.01,0.05,0.1,0.2")
	ap.add_argument("--topologies", type=str, default="fanout,diamond")
	ap.add_argument("--out", type=str, default="poc8_topology.csv")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]
	tops = args.topologies.split(",")
	print(
		f"POC 8 (fanout/diamond): n_orders={args.n_orders} avg_li={args.avg_li} "
		f"reps={args.reps} deltas={deltas} topologies={tops}",
		file=sys.stderr,
	)

	rows = []
	t0 = time.time()
	for top in tops:
		cfg = TOPOLOGIES[top]
		for f in deltas:
			for strategy in cfg["strategies"]:
				samples = []
				for rep in range(args.reps):
					try:
						t = one_run(top, args.n_orders, args.avg_li, f, strategy)
						samples.append(t)
						print(
							f"  [{top:<8}] f={f:.3f} strat={strategy:<18} "
							f"rep={rep+1}/{args.reps} t={t*1000:7.1f}ms",
							file=sys.stderr,
						)
					except Exception as e:
						print(f"  [{top}] f={f} strat={strategy}: ERROR {e}", file=sys.stderr)
				if samples:
					rows.append({
						"topology": top,
						"delta_fraction": f,
						"strategy": strategy,
						"reps": len(samples),
						"median_s": statistics.median(samples),
					})
	print(f"[wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	with Path(args.out).open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=["topology", "delta_fraction", "strategy", "reps", "median_s"],
			extrasaction="ignore",
		)
		w.writeheader()
		w.writerows(rows)

	# Summary
	for top in tops:
		print(f"\n=== {top} ===")
		print(
			f"{'delta':>8}  {'bypass':>8}  {'casc_all':>9}  "
			f"{'casc_smart':>10}  {'stl+res':>8}  winner"
		)
		by_f = {}
		for r in rows:
			if r["topology"] != top:
				continue
			by_f.setdefault(r["delta_fraction"], {})[r["strategy"]] = r["median_s"] * 1000
		for f in sorted(by_f):
			d = by_f[f]
			winner = min(d, key=d.get)
			print(
				f"{f:>8.3f}  "
				f"{d.get('bypass', float('nan')):>8.1f}  "
				f"{d.get('cascade_all', float('nan')):>9.1f}  "
				f"{d.get('cascade_smart', float('nan')):>10.1f}  "
				f"{d.get('stale_residual', float('nan')):>8.1f}  "
				f"{winner}"
			)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
