#!/usr/bin/env python3
"""
POC 5 — Mixed DML: does Tier 2 still win under deletes and updates?

Tests four workload patterns on the same base (orders ⋈ lineitem → mv_a → mv_b):
  pure_insert     — N new rows added (what all prior POCs tested)
  pure_delete     — N existing rows deleted
  mixed_50_50     — N/2 inserts + N/2 deletes
  update_heavy    — N "updates" (OpenIVM models update as delete+insert)

For each: bypass, cascade_auto, stale_plus_residual.
Also: correctness checks — stale+residual MUST equal bypass & cascade exactly.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path

DUCKDB = "/home/ila/Code/openivm/build/release/duckdb"
EXT = "/home/ila/Code/openivm/build/release/extension/openivm/openivm.duckdb_extension"


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
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_region VARCHAR, o_customer INTEGER);
CREATE TABLE lineitem(l_id INTEGER PRIMARY KEY, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);
INSERT INTO orders
SELECT i, (['us-east','us-west','eu','asia','latam'])[(i%5)+1], i%10000
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""


MV_A = (
	"CREATE MATERIALIZED VIEW mv_a AS "
	"SELECT o.o_region, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region, l.l_product;\n"
)
MV_B = (
	"CREATE MATERIALIZED VIEW mv_b AS "
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
	"FROM mv_a GROUP BY o_region;\n"
)

BYPASS = (
	"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
	"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
	"GROUP BY o.o_region ORDER BY o_region;"
)
CASCADE = (
	"SET ivm_cascade_refresh='downstream';\n"
	"PRAGMA refresh('mv_a');\n"
	"SELECT o_region, revenue, cnt FROM mv_b ORDER BY o_region;"
)
STALE_RES = (
	"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
	"    SELECT o_region, revenue, cnt FROM mv_b "
	"    UNION ALL "
	"    SELECT o.o_region, "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
	"                    ELSE -dl.l_qty*dl.l_price END) AS revenue, "
	"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS cnt "
	"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
	"    GROUP BY o.o_region "
	") x GROUP BY o_region ORDER BY o_region;"
)


def apply_workload(db: str, n_orders: int, n_lineitem: int, workload: str, n_affected: int) -> None:
	"""Apply the workload to the DB. `n_affected` = number of delta rows."""
	if workload == "pure_insert":
		sql = (
			f"INSERT INTO lineitem "
			f"SELECT {n_lineitem}+i, ({n_lineitem}+i)%{n_orders}, "
			f"({n_lineitem}+i)%500, 1+(({n_lineitem}+i)%10), "
			f"((({n_lineitem}+i)*2654435761)%1000000)/100.0 "
			f"FROM range({n_affected}) t(i);"
		)
	elif workload == "pure_delete":
		# delete the first N rows by l_id
		sql = f"DELETE FROM lineitem WHERE l_id < {n_affected};"
	elif workload == "mixed_50_50":
		half = n_affected // 2
		sql = (
			f"DELETE FROM lineitem WHERE l_id < {half};\n"
			f"INSERT INTO lineitem "
			f"SELECT {n_lineitem}+i, ({n_lineitem}+i)%{n_orders}, "
			f"({n_lineitem}+i)%500, 1+(({n_lineitem}+i)%10), "
			f"((({n_lineitem}+i)*2654435761)%1000000)/100.0 "
			f"FROM range({half}) t(i);"
		)
	elif workload == "update_heavy":
		# "Update" = change l_price; OpenIVM models as delete+insert with changed row
		sql = (
			f"UPDATE lineitem SET l_price = l_price * 1.10 "
			f"WHERE l_id < {n_affected};"
		)
	else:
		raise ValueError(workload)
	out, err, rc = run_sql(db, sql)
	if rc != 0:
		raise RuntimeError(f"workload {workload} failed: {err}")


def collect_result(db: str, which: str) -> list[tuple]:
	"""Run a strategy and return its result rows (not the timing)."""
	if which == "bypass":
		sql = ".mode csv\n.headers off\n" + BYPASS
	elif which == "cascade":
		sql = ".mode csv\n.headers off\n" + CASCADE
	elif which == "stale_residual":
		sql = ".mode csv\n.headers off\n" + STALE_RES
	else:
		raise ValueError(which)
	out, err, rc = run_sql(db, sql)
	if rc != 0:
		raise RuntimeError(f"collect {which} failed: {err}\n---\n{out}")
	# Parse CSV lines
	rows = []
	for line in out.splitlines():
		line = line.strip()
		if not line or line.startswith("│") or line.startswith("├") or line.startswith("┌") or line.startswith("└"):
			continue
		parts = line.split(",")
		if len(parts) >= 3:
			try:
				rows.append((parts[0], round(float(parts[1]), 2), int(parts[2])))
			except ValueError:
				continue
	return sorted(rows)


def time_strategy(db: str, strategy: str) -> float:
	if strategy == "bypass":
		sql = BYPASS
	elif strategy == "cascade":
		sql = CASCADE
	elif strategy == "stale_residual":
		sql = STALE_RES
	else:
		raise ValueError(strategy)
	start = time.perf_counter()
	out, err, rc = run_sql(db, sql)
	elapsed = time.perf_counter() - start
	if rc != 0:
		raise RuntimeError(f"{strategy} failed: {err}")
	return elapsed


def one_run(n_orders: int, avg_li: int, workload: str, n_affected: int, strategy: str) -> float:
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		setup = setup_sql(n_orders, avg_li) + MV_A + MV_B
		setup += "PRAGMA refresh('mv_a');\nPRAGMA ivm('mv_b');\n"
		out, err, rc = run_sql(db, setup)
		if rc != 0:
			raise RuntimeError(f"setup failed: {err}")
		apply_workload(db, n_orders, n_lineitem, workload, n_affected)
		return time_strategy(db, strategy)


def correctness_check(n_orders: int, avg_li: int, workload: str, n_affected: int) -> dict:
	"""Verify bypass == cascade == stale_residual for this workload."""
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		setup = setup_sql(n_orders, avg_li) + MV_A + MV_B
		setup += "PRAGMA refresh('mv_a');\nPRAGMA ivm('mv_b');\n"
		run_sql(db, setup)
		apply_workload(db, n_orders, n_lineitem, workload, n_affected)
		bypass_res = collect_result(db, "bypass")
		stale_res_res = collect_result(db, "stale_residual")
		# Run cascade on a fresh copy (it mutates the MV)
	with tempfile.TemporaryDirectory() as tmp2:
		db2 = os.path.join(tmp2, "bench.db")
		setup = setup_sql(n_orders, avg_li) + MV_A + MV_B
		setup += "PRAGMA refresh('mv_a');\nPRAGMA ivm('mv_b');\n"
		run_sql(db2, setup)
		apply_workload(db2, n_orders, n_lineitem, workload, n_affected)
		cascade_res = collect_result(db2, "cascade")
	return {
		"workload": workload,
		"n_affected": n_affected,
		"bypass_rows": len(bypass_res),
		"stale_residual_matches_bypass": bypass_res == stale_res_res,
		"cascade_matches_bypass": bypass_res == cascade_res,
		"bypass_sample": bypass_res[:3],
		"stale_residual_sample": stale_res_res[:3],
		"cascade_sample": cascade_res[:3],
	}


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--n-orders", type=int, default=500_000)
	ap.add_argument("--avg-li", type=int, default=4)
	ap.add_argument("--reps", type=int, default=2)
	ap.add_argument("--deltas", type=str, default="0.001,0.01,0.05,0.1,0.2")
	ap.add_argument(
		"--workloads",
		type=str,
		default="pure_insert,pure_delete,mixed_50_50,update_heavy",
	)
	ap.add_argument("--out", type=str, default="poc5_dml.csv")
	ap.add_argument("--correctness-only", action="store_true")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]
	workloads = args.workloads.split(",")
	n_lineitem = args.n_orders * args.avg_li

	print(
		f"POC 5 (DML): n_lineitem={n_lineitem} reps={args.reps} "
		f"deltas={deltas} workloads={workloads}",
		file=sys.stderr,
	)

	# === Correctness pass ===
	print("\n--- Correctness checks (small scale) ---", file=sys.stderr)
	corr_results = []
	for w in workloads:
		for f in (0.01, 0.05):
			n_affected = max(2, int(n_lineitem * f))
			try:
				r = correctness_check(args.n_orders, args.avg_li, w, n_affected)
				corr_results.append(r)
				status_sr = "OK" if r["stale_residual_matches_bypass"] else "MISMATCH"
				status_c = "OK" if r["cascade_matches_bypass"] else "MISMATCH"
				print(
					f"  {w:<15} delta={f:.2f}  stale+res={status_sr}  cascade={status_c}",
					file=sys.stderr,
				)
				if not r["stale_residual_matches_bypass"]:
					print(f"    bypass:         {r['bypass_sample']}", file=sys.stderr)
					print(f"    stale+residual: {r['stale_residual_sample']}", file=sys.stderr)
			except Exception as e:
				print(f"  {w} f={f}: ERROR {e}", file=sys.stderr)
	if args.correctness_only:
		print(json.dumps(corr_results, indent=2, default=str))
		return 0

	# === Timing pass ===
	print("\n--- Timing ---", file=sys.stderr)
	rows = []
	t0 = time.time()
	for w in workloads:
		for f in deltas:
			n_affected = max(2, int(n_lineitem * f))
			for strategy in ("bypass", "cascade", "stale_residual"):
				samples = []
				for rep in range(args.reps):
					try:
						t = one_run(args.n_orders, args.avg_li, w, n_affected, strategy)
						samples.append(t)
						print(
							f"  [{w}] f={f:.3f} strat={strategy:<15} "
							f"rep={rep+1}/{args.reps} t={t*1000:7.1f}ms",
							file=sys.stderr,
						)
					except Exception as e:
						print(f"  [{w}] f={f} strat={strategy}: ERROR {e}", file=sys.stderr)
				if not samples:
					continue
				rows.append({
					"workload": w,
					"delta_fraction": f,
					"n_affected": n_affected,
					"strategy": strategy,
					"reps": len(samples),
					"median_s": statistics.median(samples),
					"min_s": min(samples),
					"max_s": max(samples),
				})
	print(f"[total wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	# === Write CSV ===
	with Path(args.out).open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=[
				"workload", "delta_fraction", "n_affected",
				"strategy", "reps", "median_s", "min_s", "max_s",
			],
			extrasaction="ignore",
		)
		w.writeheader()
		w.writerows(rows)

	# === Summary ===
	by: dict = {}
	for r in rows:
		by.setdefault(r["workload"], {}).setdefault(r["delta_fraction"], {})[r["strategy"]] = r["median_s"] * 1000
	for workload in sorted(by):
		print(f"\n=== {workload} ===")
		print(f"{'delta':>8}  {'bypass':>8}  {'cascade':>8}  {'stl+res':>8}  winner        speedup")
		for f in sorted(by[workload]):
			d = by[workload][f]
			winner = min(d, key=d.get)
			best = d[winner]
			bypass = d.get("bypass", float("nan"))
			speedup = bypass / best if best else float("nan")
			print(
				f"{f:>8.3f}  "
				f"{d.get('bypass', float('nan')):>8.1f}  "
				f"{d.get('cascade', float('nan')):>8.1f}  "
				f"{d.get('stale_residual', float('nan')):>8.1f}  "
				f"{winner:<13} {speedup:>6.2f}x"
			)

	print("\n--- Correctness summary ---")
	for r in corr_results:
		sr = "PASS" if r["stale_residual_matches_bypass"] else "**FAIL**"
		c = "PASS" if r["cascade_matches_bypass"] else "**FAIL**"
		print(f"  {r['workload']:<15} n={r['n_affected']}  stale+res {sr}  cascade {c}")

	return 0


if __name__ == "__main__":
	raise SystemExit(main())
