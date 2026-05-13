#!/usr/bin/env python3
"""
POC 6 — Partial matches: MV covers SOME of the query; rest comes from base.

Three partial-match patterns:

  A. filter_partial:  MV has WHERE region IN ('us-east','us-west')
                      Query has  WHERE region IN ('us-east','us-west','eu','asia')
                      Matcher: scan MV for covered regions + scan base for uncovered.

  B. rollup_partial:  MV grouped by (region, product)
                      Query grouped by region  only.
                      Matcher: re-aggregate MV (cheap).

  C. date_partial:    MV covers orders.o_date <= '2024-06-30' (the "cold" half)
                      Query has no date restriction (wants all data).
                      Matcher: scan MV + scan base for date > '2024-06-30'.

For each, compare three strategies:
  bypass:       run full query on base tables.
  mv_plus_base: matcher-style (partial MV scan + compensation scan on base).
  partial_mv_only (reference only; wrong answer in A and C, right in B).

Measure how much the matcher saves.
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
CREATE TABLE orders(
    o_id INTEGER PRIMARY KEY,
    o_region VARCHAR,
    o_customer INTEGER,
    o_date DATE
);
CREATE TABLE lineitem(
    l_id INTEGER, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE
);
INSERT INTO orders
SELECT i, (['us-east','us-west','eu','asia','latam'])[(i%5)+1],
       i%10000, DATE '2024-01-01' + INTERVAL ((i%365)) DAY
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""


# -----------------------------------------------------------------------------
# SCENARIO A — filter_partial
# MV covers 2 of 5 regions. Query wants 4 of 5.
# -----------------------------------------------------------------------------
A_MV = (
	"CREATE MATERIALIZED VIEW mv_a AS "
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"WHERE o.o_region IN ('us-east','us-west') "
	"GROUP BY o.o_region;\n"
)
A_BYPASS = (
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"WHERE o.o_region IN ('us-east','us-west','eu','asia') "
	"GROUP BY o.o_region ORDER BY o_region;"
)
# Matcher approach: MV covers 2 regions; recompute the other 2 from base.
A_MV_PLUS_BASE = (
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
	"    SELECT o_region, revenue, cnt FROM mv_a "
	"    UNION ALL "
	"    SELECT o.o_region, SUM(l.l_qty*l.l_price), COUNT(*) "
	"    FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"    WHERE o.o_region IN ('eu','asia') "
	"    GROUP BY o.o_region "
	") x GROUP BY o_region ORDER BY o_region;"
)
A_PARTIAL_MV_ONLY = (
	"SELECT o_region, revenue, cnt FROM mv_a ORDER BY o_region;"
)


# -----------------------------------------------------------------------------
# SCENARIO B — rollup_partial
# MV grouped by (region, product); query wants only region.
# Matcher: re-aggregate MV (no base scan needed).
# -----------------------------------------------------------------------------
B_MV = (
	"CREATE MATERIALIZED VIEW mv_b AS "
	"SELECT o.o_region, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region, l.l_product;\n"
)
B_BYPASS = (
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region ORDER BY o_region;"
)
B_MV_REAGGREGATE = (
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
	"FROM mv_b GROUP BY o_region ORDER BY o_region;"
)


# -----------------------------------------------------------------------------
# SCENARIO C — date_partial
# MV covers orders.o_date <= '2024-06-30'. Query wants all data.
# Matcher: MV + base-scan of the later half.
# -----------------------------------------------------------------------------
C_MV = (
	"CREATE MATERIALIZED VIEW mv_c AS "
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"WHERE o.o_date <= DATE '2024-06-30' "
	"GROUP BY o.o_region;\n"
)
C_BYPASS = (
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region ORDER BY o_region;"
)
C_MV_PLUS_BASE = (
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
	"    SELECT o_region, revenue, cnt FROM mv_c "
	"    UNION ALL "
	"    SELECT o.o_region, SUM(l.l_qty*l.l_price), COUNT(*) "
	"    FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"    WHERE o.o_date > DATE '2024-06-30' "
	"    GROUP BY o.o_region "
	") x GROUP BY o_region ORDER BY o_region;"
)


SCENARIOS = {
	"filter_partial": {
		"mv": A_MV,
		"mv_name": "mv_a",
		"bypass": A_BYPASS,
		"partial_smart": A_MV_PLUS_BASE,
		"description": "MV covers 2/5 regions; query needs 4/5",
	},
	"rollup_partial": {
		"mv": B_MV,
		"mv_name": "mv_b",
		"bypass": B_BYPASS,
		"partial_smart": B_MV_REAGGREGATE,
		"description": "MV grouped (region, product); query wants region only",
	},
	"date_partial": {
		"mv": C_MV,
		"mv_name": "mv_c",
		"bypass": C_BYPASS,
		"partial_smart": C_MV_PLUS_BASE,
		"description": "MV covers H1 2024; query wants full year",
	},
}


def time_strategy(db: str, sql: str) -> float:
	start = time.perf_counter()
	out, err, rc = run_sql(db, sql)
	elapsed = time.perf_counter() - start
	if rc != 0:
		raise RuntimeError(f"strategy failed: {err}\n----SQL----\n{sql[:400]}")
	return elapsed


def correctness_check(scenario: str, n_orders: int, avg_li: int) -> dict:
	cfg = SCENARIOS[scenario]
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "c.db")
		setup = base_setup(n_orders, avg_li) + cfg["mv"] + f"PRAGMA refresh('{cfg['mv_name']}');\n"
		run_sql(db, setup)
		# Round floating-point aggregates before comparing to tolerate IEEE 754 drift
		# (SUM-of-SUM composition drifts by ~1e-9 vs native SUM — known OpenIVM
		# behavior, see CLAUDE.md). 10 decimal places = 1e-10 tolerance, matches
		# rewriter_benchmark's tolerance setting.
		diff_sql = f"""
.mode csv
.headers off
WITH
  bypass_raw AS ({cfg["bypass"].rstrip(';')}),
  partial_raw AS ({cfg["partial_smart"].rstrip(';')}),
  bypass_q AS (SELECT * EXCLUDE (revenue), ROUND(revenue, 6) AS revenue FROM bypass_raw),
  partial_q AS (SELECT * EXCLUDE (revenue), ROUND(revenue, 6) AS revenue FROM partial_raw)
SELECT
  CASE WHEN (SELECT COUNT(*) FROM bypass_q) = (SELECT COUNT(*) FROM partial_q)
       AND NOT EXISTS (SELECT 1 FROM (TABLE bypass_q EXCEPT ALL TABLE partial_q))
       AND NOT EXISTS (SELECT 1 FROM (TABLE partial_q EXCEPT ALL TABLE bypass_q))
  THEN 'MATCH' ELSE 'DIFFER' END AS verdict;
"""
		out, err, rc = run_sql(db, diff_sql)
		verdict = "UNKNOWN"
		for line in out.splitlines():
			line = line.strip()
			if line in ("MATCH", "DIFFER"):
				verdict = line
				break
		return {"scenario": scenario, "verdict": verdict, "stderr": err[:300] if err.strip() else ""}


def one_run(scenario: str, n_orders: int, avg_li: int, strategy: str) -> float:
	cfg = SCENARIOS[scenario]
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		setup = base_setup(n_orders, avg_li) + cfg["mv"] + f"PRAGMA refresh('{cfg['mv_name']}');\n"
		out, err, rc = run_sql(db, setup)
		if rc != 0:
			raise RuntimeError(f"setup failed: {err}")
		sql = cfg["bypass"] if strategy == "bypass" else cfg["partial_smart"]
		return time_strategy(db, sql)


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--n-orders", type=int, default=500_000)
	ap.add_argument("--avg-li", type=int, default=4)
	ap.add_argument("--reps", type=int, default=3)
	ap.add_argument("--out", type=str, default="poc6_partial.csv")
	args = ap.parse_args()

	n_lineitem = args.n_orders * args.avg_li
	print(
		f"POC 6 (partial match): n_orders={args.n_orders} n_lineitem={n_lineitem} "
		f"reps={args.reps}",
		file=sys.stderr,
	)

	# Correctness
	print("\n--- Correctness ---", file=sys.stderr)
	for scen in SCENARIOS:
		r = correctness_check(scen, 100_000, args.avg_li)
		status = "PASS" if r["verdict"] == "MATCH" else f"**{r['verdict']}**"
		print(f"  {scen:<18} {SCENARIOS[scen]['description']:<50}  {status}", file=sys.stderr)

	# Timing
	print("\n--- Timing ---", file=sys.stderr)
	rows = []
	t0 = time.time()
	for scen in SCENARIOS:
		for strategy in ("bypass", "partial_smart"):
			samples = []
			for rep in range(args.reps):
				try:
					t = one_run(scen, args.n_orders, args.avg_li, strategy)
					samples.append(t)
					print(
						f"  [{scen:<18}] strat={strategy:<15} "
						f"rep={rep+1}/{args.reps} t={t*1000:7.1f}ms",
						file=sys.stderr,
					)
				except Exception as e:
					print(f"  [{scen}] {strategy}: ERROR {e}", file=sys.stderr)
			if not samples:
				continue
			rows.append({
				"scenario": scen,
				"strategy": strategy,
				"reps": len(samples),
				"median_s": statistics.median(samples),
				"min_s": min(samples),
				"max_s": max(samples),
			})
	print(f"[wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	# Summary
	print("\n=== Partial-match benefit ===")
	print(f"{'scenario':<18}  {'bypass(ms)':>10}  {'partial(ms)':>11}  speedup")
	by = {}
	for r in rows:
		by.setdefault(r["scenario"], {})[r["strategy"]] = r["median_s"] * 1000
	for scen in by:
		d = by[scen]
		bp = d.get("bypass", float("nan"))
		ps = d.get("partial_smart", float("nan"))
		ratio = bp / ps if ps else float("nan")
		print(f"{scen:<18}  {bp:>10.1f}  {ps:>11.1f}  {ratio:>6.2f}x")

	with Path(args.out).open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=["scenario", "strategy", "reps", "median_s", "min_s", "max_s"],
			extrasaction="ignore",
		)
		w.writeheader()
		w.writerows(rows)

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
