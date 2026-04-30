#!/usr/bin/env python3
"""
POC 4 — Diverse pipeline shapes at realistic delta ranges (0.1%–20%).

Four pipeline variants, each tested at the same delta sweep:

  1. chain3  — 3-level MV chain: base -> mv_a (join) -> mv_b (product rollup) -> mv_c (region rollup)
  2. star    — star-schema: fact ⋈ customer ⋈ product ⋈ region, single agg MV
  3. wide    — many-group aggregate: group by customer rather than region (5 groups -> 10K groups)
  4. per_customer_chain — 2-level chain with per-customer grouping (wider MV)

Strategies per variant: bypass, cascade_auto, stale_plus_residual.

The goal: prove (or disprove) that cascade-refresh beats bypass across a
realistic set of pipeline shapes, not just the friendly 2-level orders×lineitem
case.
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
from dataclasses import dataclass
from pathlib import Path

DUCKDB = "/home/ila/Code/openivm/build/release/duckdb"
EXT = "/home/ila/Code/openivm/build/release/extension/openivm/openivm.duckdb_extension"

DEFAULT_DELTAS = [0.001, 0.005, 0.01, 0.05, 0.1, 0.2]


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


@dataclass
class Scenario:
	name: str
	setup_sql: str
	mv_sqls: list[str]      # MV definitions in order of creation/refresh
	bypass_query: str       # query to run against base
	stale_residual_sql: str # stale_top + inline compensation SQL
	top_mv: str             # name of the top-of-chain MV


# -----------------------------------------------------------------------------
# Scenario 1: chain3 — 3-level chain
# -----------------------------------------------------------------------------

def chain3(n_orders: int, avg_li: int) -> Scenario:
	n_lineitem = n_orders * avg_li
	setup = f"""
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_region VARCHAR, o_month INTEGER);
CREATE TABLE lineitem(l_id INTEGER, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);
INSERT INTO orders
SELECT i, (['us-east','us-west','eu','asia','latam'])[(i%5)+1], 1+(i%12)
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""
	# mv_a: silver — full join + group by (region, month, product)
	mv_a = (
		"CREATE MATERIALIZED VIEW mv_a AS "
		"SELECT o.o_region, o.o_month, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
		"GROUP BY o.o_region, o.o_month, l.l_product;\n"
	)
	# mv_b: rollup to (region, month)
	mv_b = (
		"CREATE MATERIALIZED VIEW mv_b AS "
		"SELECT o_region, o_month, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
		"FROM mv_a GROUP BY o_region, o_month;\n"
	)
	# mv_c: top — rollup to region
	mv_c = (
		"CREATE MATERIALIZED VIEW mv_c AS "
		"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
		"FROM mv_b GROUP BY o_region;\n"
	)
	bypass = (
		"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
		"GROUP BY o.o_region;"
	)
	stl_res = (
		"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
		"    SELECT o_region, revenue, cnt FROM mv_c "
		"    UNION ALL "
		"    SELECT o.o_region, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
		"                    ELSE -dl.l_qty*dl.l_price END) AS revenue, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS cnt "
		"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
		"    GROUP BY o.o_region "
		") x GROUP BY o_region;"
	)
	return Scenario("chain3", setup, [mv_a, mv_b, mv_c], bypass, stl_res, "mv_c")


# -----------------------------------------------------------------------------
# Scenario 2: star — fact ⋈ 3 dims, single MV (no chain)
# -----------------------------------------------------------------------------

def star(n_orders: int, avg_li: int) -> Scenario:
	n_lineitem = n_orders * avg_li
	setup = f"""
CREATE TABLE customer(c_id INTEGER PRIMARY KEY, c_country VARCHAR, c_segment VARCHAR);
CREATE TABLE product(p_id INTEGER PRIMARY KEY, p_category VARCHAR, p_brand VARCHAR);
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_customer INTEGER, o_date DATE);
CREATE TABLE lineitem(l_id INTEGER, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);

INSERT INTO customer
SELECT i, (['US','DE','UK','FR','JP'])[(i%5)+1], (['retail','enterprise','smb'])[(i%3)+1]
FROM range(10000) t(i);
INSERT INTO product
SELECT i, (['electronics','apparel','food','tools'])[(i%4)+1], 'brand_'||(i%50)
FROM range(500) t(i);
INSERT INTO orders
SELECT i, i%10000, DATE '2024-01-01' + INTERVAL ((i%365)) DAY
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""
	mv = (
		"CREATE MATERIALIZED VIEW mv_star AS "
		"SELECT c.c_country, p.p_category, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l "
		"JOIN orders o ON l.l_order_id=o.o_id "
		"JOIN customer c ON o.o_customer=c.c_id "
		"JOIN product p ON l.l_product=p.p_id "
		"GROUP BY c.c_country, p.p_category;\n"
	)
	bypass = (
		"SELECT c.c_country, p.p_category, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l "
		"JOIN orders o ON l.l_order_id=o.o_id "
		"JOIN customer c ON o.o_customer=c.c_id "
		"JOIN product p ON l.l_product=p.p_id "
		"GROUP BY c.c_country, p.p_category;"
	)
	# Residual: stale mv + delta over 4-way join (only lineitem in delta)
	stl_res = (
		"SELECT c_country, p_category, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
		"    SELECT c_country, p_category, revenue, cnt FROM mv_star "
		"    UNION ALL "
		"    SELECT c.c_country, p.p_category, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
		"                    ELSE -dl.l_qty*dl.l_price END) AS revenue, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS cnt "
		"    FROM delta_lineitem dl "
		"    JOIN orders o ON dl.l_order_id=o.o_id "
		"    JOIN customer c ON o.o_customer=c.c_id "
		"    JOIN product p ON dl.l_product=p.p_id "
		"    GROUP BY c.c_country, p.p_category "
		") x GROUP BY c_country, p_category;"
	)
	return Scenario("star", setup, [mv], bypass, stl_res, "mv_star")


# -----------------------------------------------------------------------------
# Scenario 3: wide — same 2-level chain but grouped by customer (10K groups)
# -----------------------------------------------------------------------------

def wide(n_orders: int, avg_li: int) -> Scenario:
	n_lineitem = n_orders * avg_li
	setup = f"""
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_customer INTEGER, o_region VARCHAR);
CREATE TABLE lineitem(l_id INTEGER, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);
INSERT INTO orders
SELECT i, i%10000, (['us-east','us-west','eu','asia','latam'])[(i%5)+1]
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""
	mv_a = (
		"CREATE MATERIALIZED VIEW mv_a AS "
		"SELECT o.o_customer, l.l_product, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
		"GROUP BY o.o_customer, l.l_product;\n"
	)
	mv_b = (
		"CREATE MATERIALIZED VIEW mv_b AS "
		"SELECT o_customer, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
		"FROM mv_a GROUP BY o_customer;\n"
	)
	bypass = (
		"SELECT o.o_customer, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
		"GROUP BY o.o_customer;"
	)
	stl_res = (
		"SELECT o_customer, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
		"    SELECT o_customer, revenue, cnt FROM mv_b "
		"    UNION ALL "
		"    SELECT o.o_customer, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
		"                    ELSE -dl.l_qty*dl.l_price END) AS revenue, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS cnt "
		"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
		"    GROUP BY o.o_customer "
		") x GROUP BY o_customer;"
	)
	return Scenario("wide", setup, [mv_a, mv_b], bypass, stl_res, "mv_b")


# -----------------------------------------------------------------------------
# Scenario 4: chain4 — 4-level MV chain (base → a → b → c → d)
# -----------------------------------------------------------------------------

def chain4(n_orders: int, avg_li: int) -> Scenario:
	n_lineitem = n_orders * avg_li
	setup = f"""
CREATE TABLE orders(o_id INTEGER PRIMARY KEY, o_region VARCHAR, o_month INTEGER);
CREATE TABLE lineitem(l_id INTEGER, l_order_id INTEGER, l_product INTEGER, l_qty INTEGER, l_price DOUBLE);
INSERT INTO orders
SELECT i, (['us-east','us-west','eu','asia','latam'])[(i%5)+1], 1+(i%12)
FROM range({n_orders}) t(i);
INSERT INTO lineitem
SELECT i, i%{n_orders}, i%500, 1+(i%10), ((i*2654435761)%1000000)/100.0
FROM range({n_lineitem}) t(i);
"""
	# Level 1: join + group by (region, month, product, qty_bucket)
	mv_a = (
		"CREATE MATERIALIZED VIEW mv_a AS "
		"SELECT o.o_region, o.o_month, l.l_product, l.l_qty, "
		"       SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
		"GROUP BY o.o_region, o.o_month, l.l_product, l.l_qty;\n"
	)
	# Level 2: collapse qty
	mv_b = (
		"CREATE MATERIALIZED VIEW mv_b AS "
		"SELECT o_region, o_month, l_product, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
		"FROM mv_a GROUP BY o_region, o_month, l_product;\n"
	)
	# Level 3: collapse product
	mv_c = (
		"CREATE MATERIALIZED VIEW mv_c AS "
		"SELECT o_region, o_month, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
		"FROM mv_b GROUP BY o_region, o_month;\n"
	)
	# Level 4: collapse month
	mv_d = (
		"CREATE MATERIALIZED VIEW mv_d AS "
		"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt "
		"FROM mv_c GROUP BY o_region;\n"
	)
	bypass = (
		"SELECT o.o_region, SUM(l.l_qty*l.l_price) AS revenue, COUNT(*) AS cnt "
		"FROM lineitem l JOIN orders o ON l.l_order_id=o.o_id "
		"GROUP BY o.o_region;"
	)
	stl_res = (
		"SELECT o_region, SUM(revenue) AS revenue, SUM(cnt) AS cnt FROM ( "
		"    SELECT o_region, revenue, cnt FROM mv_d "
		"    UNION ALL "
		"    SELECT o.o_region, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN dl.l_qty*dl.l_price "
		"                    ELSE -dl.l_qty*dl.l_price END) AS revenue, "
		"           SUM(CASE WHEN dl._duckdb_ivm_multiplicity THEN 1 ELSE -1 END) AS cnt "
		"    FROM delta_lineitem dl JOIN orders o ON dl.l_order_id=o.o_id "
		"    GROUP BY o.o_region "
		") x GROUP BY o_region;"
	)
	return Scenario("chain4", setup, [mv_a, mv_b, mv_c, mv_d], bypass, stl_res, "mv_d")


def delta_sql_lineitem(start_li: int, count: int, n_orders: int) -> str:
	return (
		f"INSERT INTO lineitem "
		f"SELECT {start_li}+i, ({start_li}+i)%{n_orders}, "
		f"({start_li}+i)%500, 1+(({start_li}+i)%10), "
		f"((({start_li}+i)*2654435761)%1000000)/100.0 "
		f"FROM range({count}) t(i);"
	)


def time_strategy(db: str, s: Scenario, strategy: str) -> float:
	if strategy == "bypass":
		sql = s.bypass_query
	elif strategy == "cascade_auto":
		# refresh deepest MV; OpenIVM cascades downstream
		sql = (
			"SET ivm_cascade_refresh='downstream';\n"
			f"PRAGMA ivm('{s.mv_sqls[0].split()[3]}');\n"  # first MV's name (ugly but works)
			f"SELECT * FROM {s.top_mv};"
		)
		# Parse first mv name properly — the above is fragile; fix by extracting
		# directly from the SQL.
		first_mv_name = s.mv_sqls[0].split("CREATE MATERIALIZED VIEW ")[1].split(" ")[0]
		sql = (
			"SET ivm_cascade_refresh='downstream';\n"
			f"PRAGMA ivm('{first_mv_name}');\n"
			f"SELECT * FROM {s.top_mv};"
		)
	elif strategy == "stale_plus_residual":
		sql = s.stale_residual_sql
	else:
		raise ValueError(strategy)
	start = time.perf_counter()
	out, err, rc = run_sql(db, sql)
	elapsed = time.perf_counter() - start
	if rc != 0:
		raise RuntimeError(f"strategy={strategy} failed:\n{err}\n----SQL----\n{sql}")
	return elapsed


def one_run(scenario_factory, n_orders: int, avg_li: int, delta_fraction: float, strategy: str) -> float:
	s = scenario_factory(n_orders, avg_li)
	n_lineitem = n_orders * avg_li
	with tempfile.TemporaryDirectory() as tmp:
		db = os.path.join(tmp, "bench.db")
		# Build setup: base + all MVs + initial refresh
		setup = s.setup_sql
		for mv_sql in s.mv_sqls:
			setup += mv_sql
		for mv_sql in s.mv_sqls:
			first_mv_name = mv_sql.split("CREATE MATERIALIZED VIEW ")[1].split(" ")[0]
			setup += f"PRAGMA ivm('{first_mv_name}');\n"
		out, err, rc = run_sql(db, setup)
		if rc != 0:
			raise RuntimeError(f"setup failed: {err}\n----\n{out}")
		# Insert delta
		delta_rows = max(1, int(n_lineitem * delta_fraction))
		out, err, rc = run_sql(db, delta_sql_lineitem(n_lineitem, delta_rows, n_orders))
		if rc != 0:
			raise RuntimeError(f"delta failed: {err}")
		return time_strategy(db, s, strategy)


def run_scenario(name: str, factory, n_orders: int, avg_li: int, deltas: list[float], reps: int) -> list[dict]:
	rows = []
	n_lineitem = n_orders * avg_li
	for f in deltas:
		for strategy in ("bypass", "cascade_auto", "stale_plus_residual"):
			samples = []
			for rep in range(reps):
				try:
					t = one_run(factory, n_orders, avg_li, f, strategy)
					samples.append(t)
					print(
						f"  [{name}] f={f:.4f} strat={strategy:<20} "
						f"rep={rep+1}/{reps} t={t*1000:8.1f}ms",
						file=sys.stderr,
					)
				except Exception as e:
					print(f"  ERROR [{name}] f={f} strat={strategy}: {e}", file=sys.stderr)
			if not samples:
				continue
			rows.append({
				"scenario": name,
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


def summarize(all_rows: list[dict]) -> None:
	by = {}
	for r in all_rows:
		by.setdefault(r["scenario"], {}).setdefault(r["delta_fraction"], {})[r["strategy"]] = r["median_s"] * 1000

	for scenario_name in sorted(by):
		print(f"\n=== {scenario_name} ===")
		print(
			f"{'delta_frac':>10}  {'bypass':>8}  {'cascade':>8}  {'stl+res':>8}  "
			f"winner              speedup"
		)
		for f in sorted(by[scenario_name]):
			d = by[scenario_name][f]
			winner = min(d, key=d.get)
			best_ms = d[winner]
			bypass_ms = d.get("bypass", float("nan"))
			speedup = bypass_ms / best_ms if best_ms else float("nan")
			print(
				f"{f:>10.4f}  "
				f"{d.get('bypass', float('nan')):>8.1f}  "
				f"{d.get('cascade_auto', float('nan')):>8.1f}  "
				f"{d.get('stale_plus_residual', float('nan')):>8.1f}  "
				f"{winner:<19} {speedup:>6.2f}x"
			)


def main() -> int:
	ap = argparse.ArgumentParser()
	ap.add_argument("--n-orders", type=int, default=500_000)
	ap.add_argument("--avg-li", type=int, default=4)
	ap.add_argument("--reps", type=int, default=2)
	ap.add_argument("--deltas", type=str, default=",".join(str(d) for d in DEFAULT_DELTAS))
	ap.add_argument(
		"--scenarios",
		type=str,
		default="chain3,chain4,star,wide",
		help="comma-separated scenario names",
	)
	ap.add_argument("--out", type=str, default="poc4_diverse.csv")
	args = ap.parse_args()

	deltas = [float(x) for x in args.deltas.split(",") if x.strip()]
	wanted = args.scenarios.split(",")
	factories = {"chain3": chain3, "chain4": chain4, "star": star, "wide": wide}

	print(
		f"POC 4: scenarios={wanted} n_orders={args.n_orders} "
		f"n_lineitem={args.n_orders * args.avg_li} reps={args.reps} deltas={deltas}",
		file=sys.stderr,
	)

	all_rows = []
	t0 = time.time()
	for name in wanted:
		if name not in factories:
			print(f"unknown scenario: {name}", file=sys.stderr)
			continue
		all_rows.extend(run_scenario(name, factories[name], args.n_orders, args.avg_li, deltas, args.reps))
	print(f"[total wallclock: {time.time()-t0:.1f}s]", file=sys.stderr)

	with Path(args.out).open("w", newline="") as fp:
		w = csv.DictWriter(
			fp,
			fieldnames=[
				"scenario",
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
		w.writerows(all_rows)

	summarize(all_rows)
	return 0


if __name__ == "__main__":
	raise SystemExit(main())
