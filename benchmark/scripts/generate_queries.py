#!/usr/bin/env python3
"""
TPC-C Query Validator for OpenIVM Testing

Validates hand-written SQL queries against the TPC-C schema (and optionally a
DuckLake catalog for `ducklake_*.sql` files), extracts structural metadata,
and rewrites the leading `-- {JSON}` header used by the benchmark harness.

The Claude API calls that used to live here have been removed — new queries
are added by hand to benchmark/queries/ (or via the manual_queries() /
programmatic_queries() / composite_queries() builder helpers kept below for
reference).
"""

import os
import sys
import re
import json
import argparse
import duckdb
from pathlib import Path
from datetime import datetime

def log(msg: str):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}")

TPCC_SCHEMA = """
- WAREHOUSE(W_ID INT, W_YTD DECIMAL(12,2), W_TAX DECIMAL(4,4), W_NAME VARCHAR(10), W_STREET_1 VARCHAR(20), W_STREET_2 VARCHAR(20), W_CITY VARCHAR(20), W_STATE CHAR(2), W_ZIP CHAR(9))
- DISTRICT(D_W_ID INT, D_ID INT, D_YTD DECIMAL(12,2), D_TAX DECIMAL(4,4), D_NEXT_O_ID INT, D_NAME VARCHAR(10), D_STREET_1 VARCHAR(20), D_STREET_2 VARCHAR(20), D_CITY VARCHAR(20), D_STATE CHAR(2), D_ZIP CHAR(9))
- CUSTOMER(C_W_ID INT, C_D_ID INT, C_ID INT, C_DISCOUNT DECIMAL(4,4), C_CREDIT CHAR(2), C_LAST VARCHAR(16), C_FIRST VARCHAR(16), C_CREDIT_LIM DECIMAL(12,2), C_BALANCE DECIMAL(12,2), C_YTD_PAYMENT FLOAT, C_PAYMENT_CNT INT, C_DELIVERY_CNT INT, C_CITY VARCHAR(20), C_STATE CHAR(2), C_SINCE TIMESTAMP, C_MIDDLE CHAR(2), C_DATA VARCHAR(500))
- ITEM(I_ID INT, I_NAME VARCHAR(24), I_PRICE DECIMAL(5,2), I_DATA VARCHAR(50), I_IM_ID INT)
- STOCK(S_W_ID INT, S_I_ID INT, S_QUANTITY INT, S_YTD DECIMAL(8,2), S_ORDER_CNT INT, S_REMOTE_CNT INT, S_DATA VARCHAR(50))
- OORDER(O_W_ID INT, O_D_ID INT, O_ID INT, O_C_ID INT, O_CARRIER_ID INT, O_OL_CNT INT, O_ALL_LOCAL INT, O_ENTRY_D TIMESTAMP)
- NEW_ORDER(NO_W_ID INT, NO_D_ID INT, NO_O_ID INT)
- ORDER_LINE(OL_W_ID INT, OL_D_ID INT, OL_O_ID INT, OL_NUMBER INT, OL_I_ID INT, OL_DELIVERY_D TIMESTAMP, OL_AMOUNT DECIMAL(6,2), OL_SUPPLY_W_ID INT, OL_QUANTITY DECIMAL(6,2), OL_DIST_INFO CHAR(24))
- HISTORY(H_C_ID INT, H_C_D_ID INT, H_C_W_ID INT, H_D_ID INT, H_W_ID INT, H_DATE TIMESTAMP, H_AMOUNT DECIMAL(6,2), H_DATA VARCHAR(24))
"""

# Note: the Claude-API-driven TARGETED_PROMPTS / TARGETED_FEATURES blocks that
# used to sit here have been removed. New queries are added by hand.


def manual_queries() -> list[str]:
    """Hand-crafted queries covering all operators: window, CTE, all join types,
    table functions, correlated/uncorrelated subqueries, TPC-H style complex queries."""
    return [
        # --- Window functions ---
        "SELECT C_W_ID, C_ID, C_BALANCE, ROW_NUMBER() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS rn FROM CUSTOMER;",
        "SELECT C_W_ID, C_ID, C_BALANCE, RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS rnk FROM CUSTOMER;",
        "SELECT C_W_ID, C_ID, C_BALANCE, DENSE_RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS drnk FROM CUSTOMER;",
        "SELECT OL_W_ID, OL_O_ID, OL_AMOUNT, SUM(OL_AMOUNT) OVER (PARTITION BY OL_W_ID ORDER BY OL_O_ID ROWS UNBOUNDED PRECEDING) AS running_total FROM ORDER_LINE;",
        "SELECT OL_W_ID, OL_O_ID, OL_AMOUNT, AVG(OL_AMOUNT) OVER (PARTITION BY OL_W_ID ORDER BY OL_O_ID ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg FROM ORDER_LINE;",
        "SELECT W_ID, W_YTD, LAG(W_YTD, 1) OVER (ORDER BY W_ID) AS prev_ytd, LEAD(W_YTD, 1) OVER (ORDER BY W_ID) AS next_ytd FROM WAREHOUSE;",
        "SELECT C_W_ID, C_ID, C_BALANCE, NTILE(4) OVER (ORDER BY C_BALANCE) AS quartile FROM CUSTOMER;",
        "SELECT C_W_ID, C_ID, C_BALANCE, PERCENT_RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS pct_rank FROM CUSTOMER;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, FIRST_VALUE(S_QUANTITY) OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY) AS min_in_wh FROM STOCK;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, LAST_VALUE(S_QUANTITY) OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS max_in_wh FROM STOCK;",
        "SELECT OL_W_ID, OL_O_ID, OL_NUMBER, OL_AMOUNT, COUNT(*) OVER (PARTITION BY OL_O_ID, OL_W_ID) AS lines_per_order FROM ORDER_LINE;",
        "SELECT C_W_ID, C_ID, C_BALANCE, SUM(C_BALANCE) OVER (PARTITION BY C_W_ID) AS w_total FROM CUSTOMER;",
        "SELECT O_W_ID, O_D_ID, O_ID, O_OL_CNT, SUM(O_OL_CNT) OVER (PARTITION BY O_W_ID, O_D_ID ORDER BY O_ID ROWS UNBOUNDED PRECEDING) AS cumulative_lines FROM OORDER;",
        "SELECT C_W_ID, C_ID, C_BALANCE, MIN(C_BALANCE) OVER (PARTITION BY C_W_ID) AS min_bal, MAX(C_BALANCE) OVER (PARTITION BY C_W_ID) AS max_bal FROM CUSTOMER;",
        "SELECT OL_W_ID, OL_I_ID, OL_AMOUNT, CUME_DIST() OVER (PARTITION BY OL_W_ID ORDER BY OL_AMOUNT) AS cum_dist FROM ORDER_LINE;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, ROW_NUMBER() OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY ASC) AS qty_rank_asc, ROW_NUMBER() OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY DESC) AS qty_rank_desc FROM STOCK;",
        "SELECT H_W_ID, H_C_ID, H_AMOUNT, AVG(H_AMOUNT) OVER (PARTITION BY H_W_ID ORDER BY H_DATE ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_avg FROM HISTORY;",
        "SELECT I_ID, I_PRICE, ROW_NUMBER() OVER (ORDER BY I_PRICE DESC) AS price_rank, I_PRICE - AVG(I_PRICE) OVER () AS diff_from_avg FROM ITEM;",
        "SELECT D_W_ID, D_ID, D_YTD, SUM(D_YTD) OVER (PARTITION BY D_W_ID ORDER BY D_ID ROWS UNBOUNDED PRECEDING) AS cumulative_ytd FROM DISTRICT;",
        "SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, ROW_NUMBER() OVER (PARTITION BY C_W_ID, C_D_ID ORDER BY C_BALANCE DESC) AS rank_in_district FROM CUSTOMER;",
        "SELECT OL_W_ID, OL_O_ID, OL_AMOUNT, OL_AMOUNT - LAG(OL_AMOUNT) OVER (PARTITION BY OL_W_ID ORDER BY OL_O_ID, OL_NUMBER) AS diff_from_prev FROM ORDER_LINE;",
        "SELECT W_ID, W_YTD, W_TAX, W_YTD * W_TAX AS tax_amount, SUM(W_YTD * W_TAX) OVER () AS total_tax FROM WAREHOUSE;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_ORDER_CNT, CASE WHEN ROW_NUMBER() OVER (PARTITION BY S_W_ID ORDER BY S_ORDER_CNT DESC) <= 3 THEN 'top3' ELSE 'other' END AS category FROM STOCK;",
        "SELECT C_STATE, C_CREDIT, C_BALANCE, SUM(C_BALANCE) OVER (PARTITION BY C_STATE) AS state_total, COUNT(*) OVER (PARTITION BY C_STATE, C_CREDIT) AS credit_count FROM CUSTOMER;",
        "SELECT OL_W_ID, OL_I_ID, OL_AMOUNT, ROUND(OL_AMOUNT / SUM(OL_AMOUNT) OVER (PARTITION BY OL_W_ID), 4) AS pct_of_warehouse FROM ORDER_LINE;",
        # --- CTEs ---
        "WITH cust_avg AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER GROUP BY C_W_ID) SELECT c.C_W_ID, c.C_ID, c.C_BALANCE, ca.avg_bal FROM CUSTOMER c JOIN cust_avg ca ON c.C_W_ID = ca.C_W_ID WHERE c.C_BALANCE > ca.avg_bal;",
        "WITH high_stock AS (SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_QUANTITY > 100) SELECT i.I_ID, i.I_NAME, i.I_PRICE, hs.S_QUANTITY FROM ITEM i JOIN high_stock hs ON i.I_ID = hs.S_I_ID;",
        "WITH order_totals AS (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) SELECT o.O_W_ID, o.O_ID, ot.total FROM OORDER o JOIN order_totals ot ON o.O_ID = ot.OL_O_ID AND o.O_W_ID = ot.OL_W_ID WHERE ot.total > 500;",
        "WITH top_items AS (SELECT I_ID, I_PRICE FROM ITEM WHERE I_PRICE > 50), stocked AS (SELECT S_I_ID, SUM(S_QUANTITY) AS total_qty FROM STOCK WHERE S_I_ID IN (SELECT I_ID FROM top_items) GROUP BY S_I_ID) SELECT ti.I_ID, ti.I_PRICE, s.total_qty FROM top_items ti JOIN stocked s ON ti.I_ID = s.S_I_ID;",
        "WITH cust_payments AS (SELECT C_W_ID, C_D_ID, SUM(C_YTD_PAYMENT) AS total_payments FROM CUSTOMER GROUP BY C_W_ID, C_D_ID), district_tax AS (SELECT D_W_ID, D_ID, D_TAX FROM DISTRICT) SELECT cp.C_W_ID, cp.C_D_ID, cp.total_payments, dt.D_TAX, ROUND(cp.total_payments * dt.D_TAX, 2) AS tax_amount FROM cust_payments cp JOIN district_tax dt ON cp.C_W_ID = dt.D_W_ID AND cp.C_D_ID = dt.D_ID;",
        "WITH active_orders AS (SELECT O_W_ID, O_D_ID, O_C_ID, COUNT(*) AS order_cnt FROM OORDER GROUP BY O_W_ID, O_D_ID, O_C_ID HAVING COUNT(*) > 1) SELECT c.C_W_ID, c.C_ID, c.C_LAST, ao.order_cnt FROM CUSTOMER c JOIN active_orders ao ON c.C_W_ID = ao.O_W_ID AND c.C_D_ID = ao.O_D_ID AND c.C_ID = ao.O_C_ID;",
        "WITH warehouse_summary AS (SELECT W_ID, W_NAME, W_YTD, W_TAX FROM WAREHOUSE), district_summary AS (SELECT D_W_ID, COUNT(*) AS num_districts, AVG(D_YTD) AS avg_ytd FROM DISTRICT GROUP BY D_W_ID) SELECT ws.W_ID, ws.W_NAME, ws.W_YTD, COALESCE(ds.num_districts, 0) AS num_districts, ds.avg_ytd FROM warehouse_summary ws LEFT JOIN district_summary ds ON ws.W_ID = ds.D_W_ID;",
        "WITH recent_history AS (SELECT H_C_W_ID, H_C_ID, SUM(H_AMOUNT) AS total FROM HISTORY GROUP BY H_C_W_ID, H_C_ID HAVING SUM(H_AMOUNT) > 100) SELECT rh.H_C_W_ID, c.C_LAST, rh.total FROM recent_history rh JOIN CUSTOMER c ON rh.H_C_W_ID = c.C_W_ID AND rh.H_C_ID = c.C_ID;",
        "WITH new_order_summary AS (SELECT NO_W_ID, NO_D_ID, COUNT(*) AS cnt FROM NEW_ORDER GROUP BY NO_W_ID, NO_D_ID), district_info AS (SELECT D_W_ID, D_ID, D_NAME, D_NEXT_O_ID FROM DISTRICT) SELECT di.D_W_ID, di.D_NAME, COALESCE(nos.cnt, 0) AS pending_orders FROM district_info di LEFT JOIN new_order_summary nos ON di.D_W_ID = nos.NO_W_ID AND di.D_ID = nos.NO_D_ID;",
        "WITH customer_balance_tiers AS (SELECT C_W_ID, C_ID, C_BALANCE, CASE WHEN C_BALANCE >= 5000 THEN 'premium' WHEN C_BALANCE >= 1000 THEN 'standard' WHEN C_BALANCE >= 0 THEN 'basic' ELSE 'overdue' END AS tier FROM CUSTOMER) SELECT C_W_ID, tier, COUNT(*) AS cnt, AVG(C_BALANCE) AS avg_bal FROM customer_balance_tiers GROUP BY C_W_ID, tier;",
        "WITH item_orders AS (SELECT OL_I_ID, COUNT(*) AS order_cnt, SUM(OL_AMOUNT) AS revenue FROM ORDER_LINE GROUP BY OL_I_ID) SELECT i.I_ID, i.I_NAME, i.I_PRICE, COALESCE(io.order_cnt, 0) AS times_ordered, COALESCE(io.revenue, 0) AS revenue FROM ITEM i LEFT JOIN item_orders io ON i.I_ID = io.OL_I_ID;",
        "WITH bad_credit AS (SELECT C_W_ID, COUNT(*) AS bad_cnt FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID), good_credit AS (SELECT C_W_ID, COUNT(*) AS good_cnt FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID) SELECT COALESCE(b.C_W_ID, g.C_W_ID) AS w_id, COALESCE(b.bad_cnt, 0) AS bad, COALESCE(g.good_cnt, 0) AS good FROM bad_credit b FULL OUTER JOIN good_credit g ON b.C_W_ID = g.C_W_ID;",
        "WITH stock_value AS (SELECT S_W_ID, S_I_ID, S_QUANTITY, I_PRICE, S_QUANTITY * I_PRICE AS value FROM STOCK JOIN ITEM ON S_I_ID = I_ID) SELECT S_W_ID, SUM(value) AS total_value, COUNT(*) AS items, AVG(value) AS avg_value FROM stock_value GROUP BY S_W_ID;",
        "WITH order_line_summary AS (SELECT OL_W_ID, OL_O_ID, COUNT(*) AS line_cnt, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) SELECT o.O_W_ID, o.O_D_ID, o.O_OL_CNT, ols.line_cnt, ols.total FROM OORDER o LEFT JOIN order_line_summary ols ON o.O_W_ID = ols.OL_W_ID AND o.O_ID = ols.OL_O_ID WHERE o.O_OL_CNT != ols.line_cnt OR ols.line_cnt IS NULL;",
        "WITH per_warehouse AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal, STDDEV(C_BALANCE) AS std_bal, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID) SELECT C_W_ID, ROUND(avg_bal, 2) AS avg, ROUND(std_bal, 2) AS std, cnt, ROUND(avg_bal / NULLIF(std_bal, 0), 4) AS cv FROM per_warehouse;",
        "WITH delivery_stats AS (SELECT OL_W_ID, COUNT(*) AS total, SUM(CASE WHEN OL_DELIVERY_D IS NOT NULL THEN 1 ELSE 0 END) AS delivered FROM ORDER_LINE GROUP BY OL_W_ID) SELECT ds.OL_W_ID, ds.total, ds.delivered, ds.total - ds.delivered AS pending, ROUND(100.0 * ds.delivered / NULLIF(ds.total, 0), 2) AS delivery_rate FROM delivery_stats ds;",
        "WITH w_district AS (SELECT D_W_ID, COUNT(*) AS d_cnt FROM DISTRICT GROUP BY D_W_ID), w_customer AS (SELECT C_W_ID, COUNT(*) AS c_cnt, SUM(C_BALANCE) AS bal FROM CUSTOMER GROUP BY C_W_ID) SELECT w.W_ID, w.W_NAME, wd.d_cnt, wc.c_cnt, wc.bal FROM WAREHOUSE w LEFT JOIN w_district wd ON w.W_ID = wd.D_W_ID LEFT JOIN w_customer wc ON w.W_ID = wc.C_W_ID;",
        "WITH top_customers AS (SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > 4000) SELECT tc.C_W_ID, tc.C_ID, tc.C_BALANCE, COUNT(o.O_ID) AS order_count FROM top_customers tc LEFT JOIN OORDER o ON tc.C_ID = o.O_C_ID AND tc.C_W_ID = o.O_W_ID GROUP BY tc.C_W_ID, tc.C_ID, tc.C_BALANCE;",
        # --- FULL OUTER JOIN ---
        "SELECT COALESCE(w.W_ID, d.D_W_ID) AS wid, w.W_NAME, COUNT(d.D_ID) AS num_districts FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY COALESCE(w.W_ID, d.D_W_ID), w.W_NAME;",
        "SELECT COALESCE(i.I_ID, s.S_I_ID) AS item_id, i.I_NAME, i.I_PRICE, COALESCE(s.S_QUANTITY, 0) AS qty FROM ITEM i FULL OUTER JOIN STOCK s ON i.I_ID = s.S_I_ID;",
        "SELECT COALESCE(o.O_W_ID, ol.OL_W_ID) AS w_id, COALESCE(o.O_ID, ol.OL_O_ID) AS o_id, o.O_OL_CNT, COUNT(ol.OL_NUMBER) AS actual_lines FROM OORDER o FULL OUTER JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY COALESCE(o.O_W_ID, ol.OL_W_ID), COALESCE(o.O_ID, ol.OL_O_ID), o.O_OL_CNT;",
        "SELECT COALESCE(c.C_W_ID, h.H_C_W_ID) AS w_id, COUNT(DISTINCT c.C_ID) AS customers, COUNT(DISTINCT h.H_C_ID) AS customers_with_history FROM CUSTOMER c FULL OUTER JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY COALESCE(c.C_W_ID, h.H_C_W_ID);",
        "SELECT COALESCE(no.NO_W_ID, o.O_W_ID) AS w_id, COUNT(no.NO_O_ID) AS new_orders, COUNT(o.O_ID) AS all_orders FROM NEW_ORDER no FULL OUTER JOIN OORDER o ON no.NO_O_ID = o.O_ID AND no.NO_W_ID = o.O_W_ID GROUP BY COALESCE(no.NO_W_ID, o.O_W_ID);",
        "SELECT COALESCE(s.S_W_ID, ol.OL_SUPPLY_W_ID) AS w_id, COALESCE(s.S_I_ID, ol.OL_I_ID) AS i_id, s.S_QUANTITY, SUM(ol.OL_QUANTITY) AS ordered_qty FROM STOCK s FULL OUTER JOIN ORDER_LINE ol ON s.S_I_ID = ol.OL_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID GROUP BY COALESCE(s.S_W_ID, ol.OL_SUPPLY_W_ID), COALESCE(s.S_I_ID, ol.OL_I_ID), s.S_QUANTITY;",
        "SELECT COALESCE(d.D_W_ID, c.C_W_ID) AS w_id, COALESCE(d.D_ID, c.C_D_ID) AS d_id, d.D_NAME, COUNT(c.C_ID) AS cust_count FROM DISTRICT d FULL OUTER JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY COALESCE(d.D_W_ID, c.C_W_ID), COALESCE(d.D_ID, c.C_D_ID), d.D_NAME;",
        "SELECT COALESCE(w.W_ID, c.C_W_ID) AS w_id, w.W_YTD, SUM(c.C_YTD_PAYMENT) AS cust_ytd_payments FROM WAREHOUSE w FULL OUTER JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY COALESCE(w.W_ID, c.C_W_ID), w.W_YTD;",
        # --- RIGHT JOIN ---
        "SELECT w.W_ID, w.W_NAME, d.D_ID, d.D_NAME FROM WAREHOUSE w RIGHT JOIN DISTRICT d ON w.W_ID = d.D_W_ID;",
        "SELECT i.I_ID, i.I_NAME, s.S_W_ID, s.S_QUANTITY FROM ITEM i RIGHT JOIN STOCK s ON i.I_ID = s.S_I_ID;",
        "SELECT c.C_ID, c.C_LAST, o.O_ID, o.O_OL_CNT FROM CUSTOMER c RIGHT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID;",
        "SELECT no.NO_W_ID, no.NO_D_ID, no.NO_O_ID, o.O_ENTRY_D FROM NEW_ORDER no RIGHT JOIN OORDER o ON no.NO_O_ID = o.O_ID AND no.NO_W_ID = o.O_W_ID;",
        "SELECT i.I_ID, i.I_PRICE, ol.OL_W_ID, ol.OL_AMOUNT FROM ITEM i RIGHT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID;",
        "SELECT d.D_W_ID, d.D_ID, d.D_TAX, c.C_ID, c.C_BALANCE FROM DISTRICT d RIGHT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID;",
        "SELECT w.W_ID, w.W_YTD, h.H_C_ID, h.H_AMOUNT FROM WAREHOUSE w RIGHT JOIN HISTORY h ON w.W_ID = h.H_W_ID;",
        "SELECT o.O_W_ID, o.O_ID, ol.OL_NUMBER, ol.OL_AMOUNT FROM OORDER o RIGHT JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID;",
        # --- CROSS JOIN ---
        "SELECT w.W_ID, d.D_ID FROM WAREHOUSE w CROSS JOIN DISTRICT d WHERE d.D_W_ID = w.W_ID;",
        "SELECT W_ID, r.n AS slot FROM WAREHOUSE, range(1, 11) r(n);",
        "SELECT C_W_ID, COUNT(*) AS cnt FROM CUSTOMER WHERE C_W_ID IN (SELECT * FROM generate_series(1, 5)) GROUP BY C_W_ID;",
        "SELECT W_ID, W_NAME, s.i FROM WAREHOUSE CROSS JOIN generate_series(1, 5) s(i) WHERE W_ID = s.i;",
        "SELECT I_ID, I_NAME FROM ITEM CROSS JOIN (SELECT 'A' AS grade UNION ALL SELECT 'B' UNION ALL SELECT 'C') g WHERE I_PRICE > 50 AND g.grade = 'A';",
        # --- LATERAL joins ---
        "SELECT c.C_ID, c.C_W_ID, lat.total_payments FROM CUSTOMER c JOIN LATERAL (SELECT SUM(H_AMOUNT) AS total_payments FROM HISTORY WHERE H_C_ID = c.C_ID AND H_C_W_ID = c.C_W_ID) lat ON true;",
        "SELECT w.W_ID, w.W_NAME, d_stats.num_districts, d_stats.avg_tax FROM WAREHOUSE w JOIN LATERAL (SELECT COUNT(*) AS num_districts, AVG(D_TAX) AS avg_tax FROM DISTRICT WHERE D_W_ID = w.W_ID) d_stats ON true;",
        "SELECT o.O_W_ID, o.O_ID, lat.total_amount FROM OORDER o JOIN LATERAL (SELECT SUM(OL_AMOUNT) AS total_amount FROM ORDER_LINE WHERE OL_O_ID = o.O_ID AND OL_W_ID = o.O_W_ID) lat ON true;",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST, lat.num_orders FROM CUSTOMER c JOIN LATERAL (SELECT COUNT(*) AS num_orders FROM OORDER WHERE O_C_ID = c.C_ID AND O_W_ID = c.C_W_ID) lat ON true WHERE lat.num_orders > 0;",
        "SELECT w.W_ID, w.W_YTD, lat.total_orders FROM WAREHOUSE w, LATERAL (SELECT COUNT(*) AS total_orders FROM OORDER WHERE O_W_ID = w.W_ID) lat;",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, lat.times_ordered FROM STOCK s JOIN LATERAL (SELECT COUNT(*) AS times_ordered FROM ORDER_LINE WHERE OL_I_ID = s.S_I_ID AND OL_SUPPLY_W_ID = s.S_W_ID) lat ON true;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, lat.avg_balance FROM DISTRICT d JOIN LATERAL (SELECT AVG(C_BALANCE) AS avg_balance FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) lat ON true;",
        "SELECT w.W_ID, w.W_NAME, stock_stats.total_qty, stock_stats.item_count FROM WAREHOUSE w JOIN LATERAL (SELECT SUM(S_QUANTITY) AS total_qty, COUNT(*) AS item_count FROM STOCK WHERE S_W_ID = w.W_ID) stock_stats ON true;",
        # --- Table functions ---
        "SELECT n FROM generate_series(1, 10) t(n) WHERE n IN (SELECT DISTINCT C_W_ID FROM CUSTOMER);",
        "SELECT s AS warehouse_id, COUNT(c.C_ID) AS customer_count FROM generate_series(1, 5) t(s) LEFT JOIN CUSTOMER c ON c.C_W_ID = t.s GROUP BY s;",
        "SELECT unnest(['BC', 'GC']) AS credit_type;",
        "SELECT r.n, COUNT(s.S_W_ID) AS warehouses_with_stock FROM range(1, 101) r(n) LEFT JOIN STOCK s ON s.S_I_ID = r.n GROUP BY r.n;",
        "SELECT g.m AS month_num, COUNT(o.O_ID) AS orders FROM generate_series(1, 12) g(m) LEFT JOIN OORDER o ON EXTRACT(MONTH FROM o.O_ENTRY_D) = g.m GROUP BY g.m;",
        "SELECT r.n AS target_qty, COUNT(*) AS items_below FROM range(10, 110, 10) r(n) JOIN STOCK s ON s.S_QUANTITY < r.n GROUP BY r.n;",
        # --- Correlated subqueries ---
        "SELECT c.C_ID, c.C_W_ID, c.C_BALANCE FROM CUSTOMER c WHERE c.C_BALANCE > (SELECT AVG(c2.C_BALANCE) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID);",
        "SELECT o.O_ID, o.O_W_ID, o.O_OL_CNT FROM OORDER o WHERE EXISTS (SELECT 1 FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID AND ol.OL_AMOUNT > 100);",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY FROM STOCK s WHERE s.S_QUANTITY < (SELECT AVG(s2.S_QUANTITY) FROM STOCK s2 WHERE s2.S_W_ID = s.S_W_ID);",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST FROM CUSTOMER c WHERE NOT EXISTS (SELECT 1 FROM HISTORY h WHERE h.H_C_ID = c.C_ID AND h.H_C_W_ID = c.C_W_ID);",
        "SELECT w.W_ID, w.W_NAME FROM WAREHOUSE w WHERE (SELECT COUNT(*) FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID AND c.C_BALANCE < 0) > 0;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE FROM ITEM i WHERE i.I_PRICE > (SELECT AVG(ol.OL_AMOUNT) FROM ORDER_LINE ol WHERE ol.OL_I_ID = i.I_ID);",
        "SELECT o.O_W_ID, o.O_ID FROM OORDER o WHERE o.O_OL_CNT > (SELECT COUNT(*) FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID);",
        "SELECT c.C_W_ID, c.C_ID, c.C_BALANCE FROM CUSTOMER c WHERE c.C_CREDIT = 'BC' AND c.C_BALANCE = (SELECT MAX(c2.C_BALANCE) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID AND c2.C_CREDIT = 'BC');",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME FROM DISTRICT d WHERE (SELECT SUM(C_BALANCE) FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID) > 10000;",
        "SELECT s.S_W_ID, s.S_I_ID FROM STOCK s WHERE s.S_ORDER_CNT > (SELECT COUNT(*) FROM ORDER_LINE ol WHERE ol.OL_I_ID = s.S_I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID);",
        "SELECT c.C_W_ID, c.C_ID FROM CUSTOMER c WHERE EXISTS (SELECT 1 FROM OORDER o WHERE o.O_C_ID = c.C_ID AND o.O_W_ID = c.C_W_ID AND EXISTS (SELECT 1 FROM NEW_ORDER no WHERE no.NO_O_ID = o.O_ID AND no.NO_W_ID = o.O_W_ID));",
        # --- Uncorrelated subqueries ---
        "SELECT C_ID, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_TAX > 0.1);",
        "SELECT I_ID, I_NAME, I_PRICE FROM ITEM WHERE I_PRICE > (SELECT AVG(I_PRICE) FROM ITEM);",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_QUANTITY < (SELECT AVG(S_QUANTITY) FROM STOCK);",
        "SELECT O_W_ID, O_ID, O_C_ID FROM OORDER WHERE O_ID IN (SELECT DISTINCT OL_O_ID FROM ORDER_LINE WHERE OL_AMOUNT > 100);",
        "SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE = (SELECT MAX(C_BALANCE) FROM CUSTOMER);",
        "SELECT D_W_ID, D_ID, D_NAME FROM DISTRICT WHERE D_TAX IN (SELECT DISTINCT W_TAX FROM WAREHOUSE);",
        "SELECT W_ID, W_NAME, W_YTD FROM WAREHOUSE WHERE W_YTD > (SELECT AVG(D_YTD) FROM DISTRICT);",
        "SELECT OL_W_ID, OL_O_ID, OL_AMOUNT FROM ORDER_LINE WHERE OL_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE > 80);",
        "SELECT o.O_W_ID, o.O_ID, ol_totals.total FROM OORDER o JOIN (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) ol_totals ON o.O_W_ID = ol_totals.OL_W_ID AND o.O_ID = ol_totals.OL_O_ID WHERE ol_totals.total > 500;",
        "SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_BALANCE > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY C_BALANCE) FROM CUSTOMER);",
        "SELECT I_ID, I_NAME FROM ITEM WHERE I_ID NOT IN (SELECT DISTINCT OL_I_ID FROM ORDER_LINE);",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_STATE = 'CA') OR S_QUANTITY > (SELECT MAX(S_QUANTITY) FROM STOCK) * 0.9;",
        "SELECT C_W_ID, COUNT(*) AS cnt FROM CUSTOMER WHERE C_D_ID IN (SELECT D_ID FROM DISTRICT WHERE D_TAX < 0.05) GROUP BY C_W_ID;",
        # --- Multi-table JOINs (3-4 way) ---
        "SELECT w.W_ID, w.W_NAME, d.D_ID, d.D_NAME, COUNT(c.C_ID) AS customers FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY w.W_ID, w.W_NAME, d.D_ID, d.D_NAME;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, s.S_W_ID, s.S_QUANTITY, ol.OL_W_ID, SUM(ol.OL_AMOUNT) AS revenue FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID JOIN ORDER_LINE ol ON ol.OL_I_ID = i.I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE, s.S_W_ID, s.S_QUANTITY, ol.OL_W_ID;",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST, o.O_ID, SUM(ol.OL_AMOUNT) AS order_total FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY c.C_W_ID, c.C_ID, c.C_LAST, o.O_ID;",
        "SELECT w.W_ID, d.D_ID, o.O_ID, COUNT(no.NO_O_ID) AS is_new FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN OORDER o ON o.O_W_ID = d.D_W_ID AND o.O_D_ID = d.D_ID LEFT JOIN NEW_ORDER no ON no.NO_O_ID = o.O_ID AND no.NO_W_ID = o.O_W_ID GROUP BY w.W_ID, d.D_ID, o.O_ID;",
        "SELECT w.W_ID, w.W_NAME, COUNT(DISTINCT o.O_ID) AS total_orders, SUM(ol.OL_AMOUNT) AS total_revenue FROM WAREHOUSE w JOIN OORDER o ON w.W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY w.W_ID, w.W_NAME;",
        "SELECT c.C_STATE, COUNT(DISTINCT o.O_ID) AS orders, SUM(ol.OL_AMOUNT) AS revenue FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY c.C_STATE;",
        "SELECT i.I_ID, i.I_NAME, s.S_W_ID, s.S_QUANTITY, SUM(ol.OL_QUANTITY) AS qty_ordered FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID LEFT JOIN ORDER_LINE ol ON ol.OL_I_ID = i.I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID GROUP BY i.I_ID, i.I_NAME, s.S_W_ID, s.S_QUANTITY;",
        "SELECT c.C_W_ID, c.C_CREDIT, h.H_D_ID, COUNT(*) AS payments, SUM(h.H_AMOUNT) AS total FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_W_ID, c.C_CREDIT, h.H_D_ID;",
        # --- TPC-H style complex queries ---
        "SELECT c.C_W_ID, c.C_D_ID, COUNT(*) AS num_cust, SUM(c.C_BALANCE) AS total_bal, AVG(c.C_BALANCE) AS avg_bal, CASE WHEN AVG(c.C_BALANCE) > 1000 THEN 'high' WHEN AVG(c.C_BALANCE) > 0 THEN 'medium' ELSE 'low' END AS tier FROM CUSTOMER c GROUP BY c.C_W_ID, c.C_D_ID HAVING COUNT(*) > 5;",
        "WITH warehouse_revenue AS (SELECT ol.OL_W_ID, SUM(ol.OL_AMOUNT) AS revenue FROM ORDER_LINE ol GROUP BY ol.OL_W_ID) SELECT w.W_ID, w.W_NAME, ROUND(wr.revenue, 2) AS revenue, ROUND(wr.revenue * w.W_TAX, 2) AS tax FROM WAREHOUSE w JOIN warehouse_revenue wr ON w.W_ID = wr.OL_W_ID;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, COUNT(DISTINCT c.C_ID) AS num_customers, SUM(c.C_BALANCE) AS total_balance, AVG(c.C_BALANCE) AS avg_balance, MAX(c.C_BALANCE) AS max_balance FROM DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID, d.D_NAME HAVING AVG(c.C_BALANCE) > 100;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, SUM(ol.OL_QUANTITY) AS total_sold, SUM(ol.OL_AMOUNT) AS total_revenue, COUNT(DISTINCT ol.OL_W_ID) AS num_warehouses FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE HAVING SUM(ol.OL_QUANTITY) > 10;",
        "SELECT w.W_ID, w.W_NAME, COUNT(DISTINCT o.O_ID) AS total_orders, COUNT(DISTINCT no.NO_O_ID) AS new_orders FROM WAREHOUSE w LEFT JOIN OORDER o ON w.W_ID = o.O_W_ID LEFT JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID GROUP BY w.W_ID, w.W_NAME;",
        "SELECT EXTRACT(YEAR FROM o.O_ENTRY_D) AS yr, EXTRACT(MONTH FROM o.O_ENTRY_D) AS mo, COUNT(*) AS orders, SUM(ol.OL_AMOUNT) AS revenue FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY EXTRACT(YEAR FROM o.O_ENTRY_D), EXTRACT(MONTH FROM o.O_ENTRY_D);",
        "WITH customer_segments AS (SELECT C_W_ID, CASE WHEN C_CREDIT = 'GC' THEN 'good' ELSE 'bad' END AS credit_seg, AVG(C_BALANCE) AS avg_bal, STDDEV(C_BALANCE) AS std_bal, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, CASE WHEN C_CREDIT = 'GC' THEN 'good' ELSE 'bad' END) SELECT cs.C_W_ID, cs.credit_seg, cs.avg_bal, cs.std_bal, cs.cnt, w.W_TAX FROM customer_segments cs JOIN WAREHOUSE w ON cs.C_W_ID = w.W_ID;",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, i.I_PRICE, s.S_QUANTITY * i.I_PRICE AS stock_value, CASE WHEN s.S_QUANTITY < 20 THEN 'reorder' WHEN s.S_QUANTITY < 50 THEN 'low' ELSE 'adequate' END AS stock_status FROM STOCK s JOIN ITEM i ON s.S_I_ID = i.I_ID;",
        "SELECT c.C_W_ID, c.C_CREDIT, SUM(h.H_AMOUNT) AS total_payments, COUNT(*) AS payment_count, AVG(h.H_AMOUNT) AS avg_payment, MAX(h.H_AMOUNT) AS max_payment FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_W_ID, c.C_CREDIT HAVING COUNT(*) > 1;",
        "SELECT c.C_STATE, COUNT(*) AS cnt, SUM(c.C_BALANCE) AS total_bal, SUM(c.C_YTD_PAYMENT) AS total_payments, AVG(c.C_DISCOUNT) AS avg_discount, BOOL_OR(c.C_BALANCE < 0) AS any_negative FROM CUSTOMER c GROUP BY c.C_STATE;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, COALESCE(SUM(ol.OL_AMOUNT), 0) AS total_revenue, COALESCE(COUNT(ol.OL_NUMBER), 0) AS times_ordered FROM ITEM i LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE;",
        "SELECT w.W_ID, w.W_NAME, w.W_YTD, COUNT(DISTINCT d.D_ID) AS districts, COUNT(DISTINCT c.C_ID) AS customers, SUM(c.C_BALANCE) AS customer_balance FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY w.W_ID, w.W_NAME, w.W_YTD;",
        "SELECT OL_W_ID, OL_I_ID, DATE_TRUNC('month', OL_DELIVERY_D) AS delivery_month, COUNT(*) AS deliveries, SUM(OL_AMOUNT) AS total, AVG(OL_QUANTITY) AS avg_qty FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL GROUP BY OL_W_ID, OL_I_ID, DATE_TRUNC('month', OL_DELIVERY_D);",
        "SELECT o.O_W_ID, o.O_D_ID, BOOL_AND(o.O_ALL_LOCAL = 1) AS all_local, COUNT(*) AS orders, SUM(ol.OL_AMOUNT) AS total_amount FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_W_ID, o.O_D_ID HAVING BOOL_AND(o.O_ALL_LOCAL = 1);",
        "SELECT C_W_ID, COUNT(*) AS total, SUM(CASE WHEN C_CREDIT = 'BC' THEN 1 ELSE 0 END) AS bad_credit, SUM(CASE WHEN C_BALANCE < 0 THEN 1 ELSE 0 END) AS negative_bal, ROUND(100.0 * SUM(CASE WHEN C_CREDIT = 'BC' THEN 1 ELSE 0 END) / COUNT(*), 2) AS bad_pct FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT s.S_W_ID, COUNT(*) AS items, SUM(s.S_QUANTITY) AS total_qty, AVG(s.S_QUANTITY) AS avg_qty, STDDEV(s.S_QUANTITY) AS std_qty, MIN(s.S_QUANTITY) AS min_qty, MAX(s.S_QUANTITY) AS max_qty, BOOL_AND(s.S_QUANTITY > 0) AS all_in_stock FROM STOCK s GROUP BY s.S_W_ID;",
        "WITH order_lines_agg AS (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS total, COUNT(*) AS lines FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID), order_info AS (SELECT O_W_ID, O_ID, O_C_ID, O_OL_CNT FROM OORDER) SELECT oi.O_W_ID, oi.O_ID, oi.O_C_ID, oi.O_OL_CNT, ola.lines, ola.total FROM order_info oi LEFT JOIN order_lines_agg ola ON oi.O_W_ID = ola.OL_W_ID AND oi.O_ID = ola.OL_O_ID WHERE oi.O_OL_CNT != ola.lines OR ola.lines IS NULL;",
        "SELECT C_W_ID, C_D_ID, VARIANCE(C_BALANCE) AS var_bal, STDDEV(C_BALANCE) AS std_bal, STDDEV_POP(C_BALANCE) AS std_pop, VAR_POP(C_BALANCE) AS var_pop FROM CUSTOMER GROUP BY C_W_ID, C_D_ID;",
        "SELECT COALESCE(w.W_STATE, 'UNKNOWN') AS state, COUNT(DISTINCT w.W_ID) AS warehouses, COUNT(DISTINCT d.D_ID) AS districts, COUNT(DISTINCT c.C_ID) AS customers FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY COALESCE(w.W_STATE, 'UNKNOWN');",
        "SELECT c.C_W_ID, c.C_D_ID, c.C_STATE, SUM(c.C_BALANCE) AS balance, SUM(h.H_AMOUNT) AS history_payments FROM CUSTOMER c LEFT JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_W_ID, c.C_D_ID, c.C_STATE HAVING SUM(c.C_BALANCE) > SUM(COALESCE(h.H_AMOUNT, 0));",
        "SELECT i.I_NAME, COUNT(DISTINCT ol.OL_W_ID) AS warehouses_sold, COUNT(*) AS total_lines, SUM(ol.OL_AMOUNT) AS revenue, ROUND(AVG(ol.OL_AMOUNT), 2) AS avg_line_amount FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_NAME HAVING COUNT(*) > 1;",
        # --- CASE WHEN, CAST, date arithmetic ---
        "SELECT C_W_ID, C_ID, CAST(C_BALANCE AS BIGINT) AS balance_int, CAST(C_DISCOUNT AS DECIMAL(6,2)) AS discount_pct, CASE WHEN C_BALANCE > 5000 THEN 'VIP' WHEN C_BALANCE > 0 THEN 'ACTIVE' ELSE 'INACTIVE' END AS status FROM CUSTOMER;",
        "SELECT I_ID, I_NAME, I_PRICE, CAST(I_PRICE AS VARCHAR) AS price_str, CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 60 THEN 'mid' ELSE 'expensive' END AS price_tier FROM ITEM;",
        "SELECT O_W_ID, O_ID, O_ENTRY_D, EXTRACT(YEAR FROM O_ENTRY_D) AS yr, EXTRACT(MONTH FROM O_ENTRY_D) AS mo, EXTRACT(DOW FROM O_ENTRY_D) AS dow, DATE_TRUNC('week', O_ENTRY_D) AS week_start FROM OORDER WHERE O_ENTRY_D IS NOT NULL;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, ABS(S_QUANTITY - 50) AS dist_from_50, ROUND(CAST(S_QUANTITY AS DECIMAL) / 100.0, 4) AS qty_pct FROM STOCK;",
        "SELECT H_W_ID, H_C_ID, H_AMOUNT, H_DATE, CAST(H_DATE AS DATE) AS payment_date, ROUND(H_AMOUNT, 0) AS rounded_amt FROM HISTORY WHERE H_AMOUNT IS NOT NULL;",
        "SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, CASE WHEN OL_DELIVERY_D IS NULL THEN 'undelivered' ELSE 'delivered' END AS status, COALESCE(CAST(OL_DELIVERY_D AS VARCHAR), 'N/A') AS delivery_str FROM ORDER_LINE;",
        "SELECT C_W_ID, C_D_ID, CASE WHEN C_STATE IN ('CA', 'NY', 'TX') THEN 'major' ELSE 'other' END AS state_group, COUNT(*) AS cnt, SUM(C_BALANCE) AS total FROM CUSTOMER GROUP BY C_W_ID, C_D_ID, CASE WHEN C_STATE IN ('CA', 'NY', 'TX') THEN 'major' ELSE 'other' END;",
        "SELECT W_ID, W_NAME, W_YTD, W_TAX, ROUND(W_YTD * (1 - W_TAX), 2) AS net_ytd, CASE WHEN W_TAX > 0.1 THEN 'high_tax' ELSE 'low_tax' END AS tax_band FROM WAREHOUSE;",
        "SELECT D_W_ID, D_ID, D_NAME, D_YTD, CAST(D_YTD AS BIGINT) AS ytd_int, D_TAX, CASE WHEN D_NEXT_O_ID > 1000 THEN 'busy' ELSE 'quiet' END AS activity FROM DISTRICT;",
        # --- HAVING and DISTINCT ---
        "SELECT C_W_ID, C_STATE, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, C_STATE HAVING COUNT(*) > 5;",
        "SELECT O_W_ID, O_D_ID, COUNT(*) AS order_cnt, AVG(O_OL_CNT) AS avg_lines FROM OORDER GROUP BY O_W_ID, O_D_ID HAVING AVG(O_OL_CNT) > 5;",
        "SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS revenue FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID HAVING SUM(OL_AMOUNT) > 1000 AND COUNT(*) > 3;",
        "SELECT S_W_ID, COUNT(*) AS items, SUM(S_QUANTITY) AS total FROM STOCK GROUP BY S_W_ID HAVING STDDEV(S_QUANTITY) > 10;",
        "SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal, STDDEV(C_BALANCE) AS std_bal FROM CUSTOMER GROUP BY C_W_ID HAVING STDDEV(C_BALANCE) > 100 AND AVG(C_BALANCE) > 0;",
        "SELECT DISTINCT C_STATE FROM CUSTOMER ORDER BY C_STATE;",
        "SELECT DISTINCT C_CREDIT, C_W_ID FROM CUSTOMER;",
        "SELECT DISTINCT OL_W_ID, OL_I_ID FROM ORDER_LINE WHERE OL_AMOUNT > 50;",
        "SELECT DISTINCT W_STATE FROM WAREHOUSE;",
        "SELECT DISTINCT O_CARRIER_ID FROM OORDER WHERE O_CARRIER_ID IS NOT NULL;",
        # --- UNION ---
        "SELECT C_W_ID AS w_id, 'customer' AS type, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID UNION ALL SELECT O_W_ID, 'order', COUNT(*) FROM OORDER GROUP BY O_W_ID;",
        "SELECT W_ID AS id, W_NAME AS name, 'warehouse' AS type FROM WAREHOUSE UNION ALL SELECT D_W_ID * 100 + D_ID, D_NAME, 'district' FROM DISTRICT;",
        "SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) AS total FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID, C_D_ID UNION ALL SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID, C_D_ID;",
        "SELECT OL_W_ID AS w_id, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID UNION ALL SELECT H_W_ID, SUM(H_AMOUNT) FROM HISTORY GROUP BY H_W_ID;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_QUANTITY < 10 UNION ALL SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_ORDER_CNT > 100;",
        # --- Additional aggregate functions ---
        "SELECT C_W_ID, BOOL_AND(C_BALANCE > 0) AS all_positive, BOOL_OR(C_CREDIT = 'BC') AS has_bad_credit FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT S_W_ID, STDDEV(S_QUANTITY) AS std, VARIANCE(S_QUANTITY) AS var, STDDEV_POP(S_QUANTITY) AS std_pop, VAR_POP(S_QUANTITY) AS var_pop FROM STOCK GROUP BY S_W_ID;",
        "SELECT OL_W_ID, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY OL_AMOUNT) AS median_amount FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT C_W_ID, COUNT(*) AS total, COUNT(DISTINCT C_STATE) AS unique_states, COUNT(DISTINCT C_CITY) AS unique_cities FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT W_ID, W_NAME, W_YTD, W_TAX, ABS(W_YTD - AVG(W_YTD) OVER ()) AS deviation FROM WAREHOUSE;",
        "SELECT OL_W_ID, APPROX_COUNT_DISTINCT(OL_I_ID) AS approx_unique_items, COUNT(DISTINCT OL_I_ID) AS exact_unique_items FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT C_W_ID, STRING_AGG(DISTINCT C_CREDIT, ',' ORDER BY C_CREDIT) AS credit_types, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID;",
        # --- NULL handling ---
        "SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, COALESCE(OL_DELIVERY_D::VARCHAR, 'PENDING') AS delivery_status FROM ORDER_LINE;",
        "SELECT C_ID, COALESCE(C_MIDDLE, 'N/A') AS middle, COALESCE(C_DATA, '') AS data FROM CUSTOMER;",
        "SELECT O_ID, O_W_ID, COALESCE(O_CARRIER_ID, -1) AS carrier_id, CASE WHEN O_CARRIER_ID IS NULL THEN 'unassigned' ELSE CAST(O_CARRIER_ID AS VARCHAR) END AS carrier_str FROM OORDER;",
        "SELECT S_W_ID, S_I_ID, NULLIF(S_QUANTITY, 0) AS safe_qty, NULLIF(S_ORDER_CNT, 0) AS safe_orders FROM STOCK;",
        "SELECT C_W_ID, C_ID, C_BALANCE, CASE WHEN C_BALANCE IS NULL THEN 0.0 ELSE C_BALANCE END AS safe_balance FROM CUSTOMER;",
        # --- Date/timestamp arithmetic ---
        "SELECT O_W_ID, O_ID, O_ENTRY_D, O_ENTRY_D + INTERVAL '30 days' AS due_date FROM OORDER;",
        "SELECT H_W_ID, H_C_ID, H_DATE, H_AMOUNT, EXTRACT(EPOCH FROM H_DATE) AS epoch_seconds FROM HISTORY WHERE H_DATE IS NOT NULL;",
        "SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, OL_DELIVERY_D - INTERVAL '7 days' AS pickup_date FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;",
        "SELECT O_W_ID, DATE_TRUNC('month', O_ENTRY_D) AS month, COUNT(*) AS orders, SUM(O_OL_CNT) AS total_lines FROM OORDER GROUP BY O_W_ID, DATE_TRUNC('month', O_ENTRY_D);",
        "SELECT C_W_ID, DATE_TRUNC('year', C_SINCE) AS join_year, COUNT(*) AS new_customers FROM CUSTOMER WHERE C_SINCE IS NOT NULL GROUP BY C_W_ID, DATE_TRUNC('year', C_SINCE);",
    ]


# Table metadata used by programmatic query generation.
# (table_name, numeric_agg_cols, group_by_cols, filter_col+literal_sample)
TABLE_AGG_INFO = [
    # (table, numeric_cols, group_cols, (filter_col, filter_literal))
    ("CUSTOMER",   ["C_BALANCE", "C_YTD_PAYMENT", "C_PAYMENT_CNT", "C_DELIVERY_CNT", "C_DISCOUNT", "C_CREDIT_LIM"],
                   ["C_W_ID", "C_D_ID", "C_STATE", "C_CREDIT"], ("C_BALANCE", "0")),
    ("STOCK",      ["S_QUANTITY", "S_YTD", "S_ORDER_CNT", "S_REMOTE_CNT"],
                   ["S_W_ID"],                                    ("S_QUANTITY", "50")),
    ("ORDER_LINE", ["OL_AMOUNT", "OL_QUANTITY", "OL_NUMBER"],
                   ["OL_W_ID", "OL_I_ID", "OL_D_ID", "OL_O_ID"],  ("OL_AMOUNT", "10")),
    ("OORDER",     ["O_OL_CNT", "O_ALL_LOCAL", "O_CARRIER_ID"],
                   ["O_W_ID", "O_D_ID", "O_C_ID"],                ("O_OL_CNT", "5")),
    ("DISTRICT",   ["D_YTD", "D_TAX", "D_NEXT_O_ID"],
                   ["D_W_ID"],                                    ("D_YTD", "0")),
    ("WAREHOUSE",  ["W_YTD", "W_TAX"],
                   ["W_STATE"],                                   ("W_YTD", "0")),
    ("HISTORY",    ["H_AMOUNT"],
                   ["H_W_ID", "H_D_ID", "H_C_W_ID"],              ("H_AMOUNT", "10")),
    ("ITEM",       ["I_PRICE"],
                   ["I_IM_ID"],                                   ("I_PRICE", "50")),
    ("NEW_ORDER",  ["NO_O_ID"],
                   ["NO_W_ID", "NO_D_ID"],                        ("NO_O_ID", "0")),
]

# FK pairs: (left_table, right_table, join_condition, left_cols_sample, right_cols_sample)
FK_JOIN_PAIRS = [
    ("WAREHOUSE", "DISTRICT", "w.W_ID = d.D_W_ID",
     ["W_ID", "W_NAME", "W_YTD", "W_TAX"], ["D_ID", "D_NAME", "D_YTD"]),
    ("WAREHOUSE", "CUSTOMER", "w.W_ID = c.C_W_ID",
     ["W_ID", "W_NAME"], ["C_ID", "C_LAST", "C_BALANCE"]),
    ("WAREHOUSE", "OORDER", "w.W_ID = o.O_W_ID",
     ["W_ID", "W_NAME"], ["O_ID", "O_OL_CNT"]),
    ("DISTRICT", "CUSTOMER", "d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID",
     ["D_W_ID", "D_ID", "D_NAME"], ["C_ID", "C_BALANCE"]),
    ("DISTRICT", "OORDER", "d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID",
     ["D_W_ID", "D_ID"], ["O_ID", "O_OL_CNT"]),
    ("CUSTOMER", "OORDER", "c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID",
     ["C_ID", "C_LAST", "C_BALANCE"], ["O_ID", "O_OL_CNT"]),
    ("CUSTOMER", "HISTORY", "c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID",
     ["C_ID", "C_LAST"], ["H_AMOUNT", "H_DATE"]),
    ("OORDER", "ORDER_LINE", "o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID",
     ["O_ID", "O_W_ID", "O_OL_CNT"], ["OL_NUMBER", "OL_AMOUNT"]),
    ("ITEM", "STOCK", "i.I_ID = s.S_I_ID",
     ["I_ID", "I_NAME", "I_PRICE"], ["S_W_ID", "S_QUANTITY"]),
    ("ITEM", "ORDER_LINE", "i.I_ID = ol.OL_I_ID",
     ["I_ID", "I_NAME", "I_PRICE"], ["OL_W_ID", "OL_AMOUNT"]),
    ("STOCK", "ORDER_LINE", "s.S_I_ID = ol.OL_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID",
     ["S_W_ID", "S_I_ID", "S_QUANTITY"], ["OL_O_ID", "OL_AMOUNT"]),
    ("OORDER", "NEW_ORDER", "o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID",
     ["O_ID", "O_W_ID"], ["NO_O_ID"]),
]

TABLE_ALIAS = {
    "WAREHOUSE": "w", "DISTRICT": "d", "CUSTOMER": "c", "ITEM": "i",
    "STOCK": "s", "OORDER": "o", "NEW_ORDER": "no", "ORDER_LINE": "ol", "HISTORY": "h",
}


def programmatic_queries() -> list[str]:
    """Systematically construct many valid DuckDB queries from templates."""
    queries = []

    # ===== 1. Simple aggregate queries (per-table variations) =====
    agg_fns = [
        ("COUNT", "COUNT(*)", "cnt"),
        ("SUM", "SUM({col})", "total"),
        ("AVG", "AVG({col})", "avg_val"),
        ("MIN", "MIN({col})", "min_val"),
        ("MAX", "MAX({col})", "max_val"),
        ("STDDEV", "STDDEV({col})", "std_val"),
        ("VARIANCE", "VARIANCE({col})", "var_val"),
        ("STDDEV_POP", "STDDEV_POP({col})", "std_pop"),
        ("VAR_POP", "VAR_POP({col})", "var_pop"),
        ("COUNT_DISTINCT", "COUNT(DISTINCT {col})", "unique_vals"),
    ]
    for table, num_cols, group_cols, (fcol, flit) in TABLE_AGG_INFO:
        for gcol in group_cols:
            # Single aggregate per query
            for fn_name, fn_tmpl, alias in agg_fns:
                col = num_cols[0]
                agg_expr = fn_tmpl.format(col=col) if "{col}" in fn_tmpl else fn_tmpl
                queries.append(f"SELECT {gcol}, {agg_expr} AS {alias} FROM {table} GROUP BY {gcol};")
            # Multi-agg per query (SUM + COUNT + AVG + MIN + MAX)
            col = num_cols[0]
            queries.append(
                f"SELECT {gcol}, COUNT(*) AS cnt, SUM({col}) AS total, AVG({col}) AS avg_val, "
                f"MIN({col}) AS min_val, MAX({col}) AS max_val FROM {table} GROUP BY {gcol};"
            )
            # SUM and COUNT with filter
            queries.append(
                f"SELECT {gcol}, COUNT(*) AS cnt, SUM({col}) AS total FROM {table} "
                f"WHERE {fcol} > {flit} GROUP BY {gcol};"
            )
            # HAVING variant
            queries.append(
                f"SELECT {gcol}, COUNT(*) AS cnt, AVG({col}) AS avg_val FROM {table} "
                f"GROUP BY {gcol} HAVING COUNT(*) > 0 AND AVG({col}) > 0;"
            )
        # Simple aggregate without GROUP BY
        col = num_cols[0]
        queries.append(f"SELECT COUNT(*) AS cnt FROM {table};")
        queries.append(f"SELECT COUNT(*) AS cnt, SUM({col}) AS total, AVG({col}) AS avg_val FROM {table};")
        queries.append(f"SELECT MIN({col}) AS min_val, MAX({col}) AS max_val FROM {table};")
        queries.append(f"SELECT STDDEV({col}) AS std, VARIANCE({col}) AS var FROM {table};")
        # Multi-column group by
        if len(group_cols) >= 2:
            g1, g2 = group_cols[0], group_cols[1]
            queries.append(f"SELECT {g1}, {g2}, COUNT(*) AS cnt, SUM({col}) AS total FROM {table} GROUP BY {g1}, {g2};")
            queries.append(f"SELECT {g1}, {g2}, AVG({col}) AS avg_val FROM {table} GROUP BY {g1}, {g2} HAVING AVG({col}) > 0;")

    # ===== 2. Simple SELECT / WHERE =====
    for table, num_cols, group_cols, (fcol, flit) in TABLE_AGG_INFO:
        for gcol in group_cols[:2]:
            queries.append(f"SELECT {gcol}, {num_cols[0]} FROM {table} WHERE {fcol} > {flit};")
            queries.append(f"SELECT {gcol}, {num_cols[0]} FROM {table} WHERE {fcol} >= {flit} AND {num_cols[0]} IS NOT NULL;")
            queries.append(f"SELECT DISTINCT {gcol} FROM {table};")
            queries.append(f"SELECT {gcol}, {num_cols[0]} FROM {table} WHERE {fcol} BETWEEN {flit} AND 100000;")

    # ===== 3. Two-table JOINs (INNER, LEFT, RIGHT) =====
    for left, right, cond, lcols, rcols in FK_JOIN_PAIRS:
        la = TABLE_ALIAS[left]
        ra = TABLE_ALIAS[right]
        lproj = ", ".join(f"{la}.{c}" for c in lcols)
        rproj = ", ".join(f"{ra}.{c}" for c in rcols)
        # INNER JOIN
        queries.append(f"SELECT {lproj}, {rproj} FROM {left} {la} JOIN {right} {ra} ON {cond};")
        # LEFT JOIN
        queries.append(f"SELECT {lproj}, {rproj} FROM {left} {la} LEFT JOIN {right} {ra} ON {cond};")
        # RIGHT JOIN
        queries.append(f"SELECT {lproj}, {rproj} FROM {left} {la} RIGHT JOIN {right} {ra} ON {cond};")
        # FULL OUTER JOIN
        queries.append(f"SELECT {lproj}, {rproj} FROM {left} {la} FULL OUTER JOIN {right} {ra} ON {cond};")
        # INNER JOIN with aggregate
        first_left = lcols[0]
        if rcols and any(c for c in rcols if c not in ["O_ENTRY_D", "H_DATE", "OL_DELIVERY_D", "C_SINCE"]):
            num_right = next((c for c in rcols if not c.endswith("_D") and "DATE" not in c and "SINCE" not in c), rcols[0])
            queries.append(
                f"SELECT {la}.{first_left}, COUNT(*) AS cnt FROM {left} {la} JOIN {right} {ra} ON {cond} GROUP BY {la}.{first_left};"
            )
            queries.append(
                f"SELECT {la}.{first_left}, COUNT(*) AS cnt, COUNT(DISTINCT {ra}.{num_right}) AS uniq FROM {left} {la} LEFT JOIN {right} {ra} ON {cond} GROUP BY {la}.{first_left};"
            )
            queries.append(
                f"SELECT {la}.{first_left}, COUNT({ra}.{num_right}) AS cnt FROM {left} {la} LEFT JOIN {right} {ra} ON {cond} GROUP BY {la}.{first_left} HAVING COUNT({ra}.{num_right}) > 0;"
            )
        # WHERE variant on INNER JOIN
        queries.append(f"SELECT {lproj}, {rproj} FROM {left} {la} JOIN {right} {ra} ON {cond} WHERE {la}.{lcols[0]} > 0;")

    # ===== 4. Three-table JOINs =====
    three_way_joins = [
        # WAREHOUSE -> DISTRICT -> CUSTOMER
        ("WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID",
         "w.W_ID, w.W_NAME, d.D_ID, d.D_NAME, c.C_ID, c.C_BALANCE",
         "w.W_ID, d.D_ID"),
        # CUSTOMER -> OORDER -> ORDER_LINE
        ("CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID",
         "c.C_ID, c.C_LAST, o.O_ID, ol.OL_NUMBER, ol.OL_AMOUNT",
         "c.C_W_ID, c.C_ID"),
        # ITEM -> STOCK -> ORDER_LINE
        ("ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID JOIN ORDER_LINE ol ON ol.OL_I_ID = i.I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID",
         "i.I_ID, i.I_NAME, s.S_W_ID, s.S_QUANTITY, ol.OL_AMOUNT",
         "i.I_ID, s.S_W_ID"),
        # WAREHOUSE -> OORDER -> ORDER_LINE
        ("WAREHOUSE w JOIN OORDER o ON w.W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID",
         "w.W_ID, w.W_NAME, o.O_ID, ol.OL_AMOUNT",
         "w.W_ID"),
        # DISTRICT -> CUSTOMER -> HISTORY
        ("DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID",
         "d.D_W_ID, d.D_ID, c.C_ID, h.H_AMOUNT",
         "d.D_W_ID, d.D_ID"),
        # OORDER -> ORDER_LINE -> ITEM
        ("OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID JOIN ITEM i ON ol.OL_I_ID = i.I_ID",
         "o.O_ID, ol.OL_AMOUNT, i.I_NAME, i.I_PRICE",
         "o.O_W_ID, o.O_ID"),
        # OORDER -> NEW_ORDER -> DISTRICT
        ("OORDER o JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID JOIN DISTRICT d ON o.O_W_ID = d.D_W_ID AND o.O_D_ID = d.D_ID",
         "o.O_ID, o.O_W_ID, d.D_NAME, no.NO_O_ID",
         "d.D_W_ID, d.D_ID"),
    ]
    for join_expr, proj, group_cols in three_way_joins:
        queries.append(f"SELECT {proj} FROM {join_expr};")
        queries.append(f"SELECT {group_cols}, COUNT(*) AS cnt FROM {join_expr} GROUP BY {group_cols};")
        queries.append(f"SELECT {group_cols}, COUNT(*) AS cnt FROM {join_expr} GROUP BY {group_cols} HAVING COUNT(*) > 1;")

    # ===== 5. DISTINCT variations =====
    distinct_cols = [
        ("CUSTOMER", "C_STATE"), ("CUSTOMER", "C_CREDIT"), ("CUSTOMER", "C_MIDDLE"),
        ("WAREHOUSE", "W_STATE"), ("WAREHOUSE", "W_CITY"),
        ("DISTRICT", "D_W_ID"), ("DISTRICT", "D_NAME"),
        ("ITEM", "I_IM_ID"),
        ("OORDER", "O_CARRIER_ID"), ("OORDER", "O_ALL_LOCAL"),
        ("ORDER_LINE", "OL_SUPPLY_W_ID"), ("ORDER_LINE", "OL_I_ID"),
        ("STOCK", "S_W_ID"),
        ("HISTORY", "H_W_ID"),
    ]
    for table, col in distinct_cols:
        queries.append(f"SELECT DISTINCT {col} FROM {table};")
        queries.append(f"SELECT COUNT(DISTINCT {col}) AS uniq FROM {table};")

    # Pairs
    distinct_pairs = [
        ("CUSTOMER", "C_W_ID", "C_STATE"),
        ("CUSTOMER", "C_W_ID", "C_CREDIT"),
        ("STOCK", "S_W_ID", "S_I_ID"),
        ("ORDER_LINE", "OL_W_ID", "OL_I_ID"),
        ("ORDER_LINE", "OL_W_ID", "OL_O_ID"),
        ("OORDER", "O_W_ID", "O_D_ID"),
        ("HISTORY", "H_W_ID", "H_D_ID"),
    ]
    for table, c1, c2 in distinct_pairs:
        queries.append(f"SELECT DISTINCT {c1}, {c2} FROM {table};")
        queries.append(f"SELECT {c1}, COUNT(DISTINCT {c2}) AS uniq FROM {table} GROUP BY {c1};")

    # ===== 6. UNION ALL combinations =====
    unions = [
        ("SELECT C_W_ID AS w_id, 'customer' AS type, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID",
         "SELECT O_W_ID, 'order', COUNT(*) FROM OORDER GROUP BY O_W_ID"),
        ("SELECT C_W_ID AS w_id, SUM(C_BALANCE) AS total FROM CUSTOMER GROUP BY C_W_ID",
         "SELECT H_W_ID, SUM(H_AMOUNT) FROM HISTORY GROUP BY H_W_ID"),
        ("SELECT S_W_ID AS w_id, S_I_ID AS id FROM STOCK WHERE S_QUANTITY < 10",
         "SELECT OL_W_ID, OL_I_ID FROM ORDER_LINE WHERE OL_AMOUNT > 100"),
        ("SELECT OL_W_ID AS w_id, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID",
         "SELECT H_W_ID, SUM(H_AMOUNT) FROM HISTORY GROUP BY H_W_ID"),
        ("SELECT C_W_ID AS w_id, COUNT(*) AS cnt FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID",
         "SELECT C_W_ID, COUNT(*) FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID"),
        ("SELECT W_ID AS id, W_YTD AS amount FROM WAREHOUSE",
         "SELECT D_W_ID * 100 + D_ID, D_YTD FROM DISTRICT"),
        ("SELECT W_ID AS id, 'warehouse' AS type FROM WAREHOUSE",
         "SELECT C_W_ID, 'customer' FROM CUSTOMER"),
    ]
    for q1, q2 in unions:
        queries.append(f"{q1} UNION ALL {q2};")
        queries.append(f"{q1} UNION {q2};")

    # ===== 7. CASE WHEN variations =====
    case_templates = [
        "SELECT C_W_ID, C_ID, CASE WHEN C_BALANCE > 5000 THEN 'vip' WHEN C_BALANCE > 1000 THEN 'standard' ELSE 'basic' END AS tier FROM CUSTOMER;",
        "SELECT C_W_ID, COUNT(CASE WHEN C_CREDIT = 'GC' THEN 1 END) AS good_cnt, COUNT(CASE WHEN C_CREDIT = 'BC' THEN 1 END) AS bad_cnt FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT W_ID, CASE WHEN W_TAX > 0.1 THEN 'high' ELSE 'low' END AS tax_band, W_YTD FROM WAREHOUSE;",
        "SELECT S_W_ID, SUM(CASE WHEN S_QUANTITY < 20 THEN 1 ELSE 0 END) AS low_stock, SUM(CASE WHEN S_QUANTITY >= 20 THEN 1 ELSE 0 END) AS ok_stock FROM STOCK GROUP BY S_W_ID;",
        "SELECT O_W_ID, O_D_ID, COUNT(CASE WHEN O_ALL_LOCAL = 1 THEN 1 END) AS local_cnt, COUNT(CASE WHEN O_ALL_LOCAL = 0 THEN 1 END) AS remote_cnt FROM OORDER GROUP BY O_W_ID, O_D_ID;",
        "SELECT OL_W_ID, SUM(CASE WHEN OL_DELIVERY_D IS NOT NULL THEN OL_AMOUNT ELSE 0 END) AS delivered_total FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT I_ID, I_NAME, CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 60 THEN 'mid' ELSE 'premium' END AS category FROM ITEM;",
        "SELECT D_W_ID, SUM(CASE WHEN D_NEXT_O_ID > 1000 THEN 1 ELSE 0 END) AS busy_districts FROM DISTRICT GROUP BY D_W_ID;",
        "SELECT H_W_ID, SUM(CASE WHEN H_AMOUNT > 100 THEN H_AMOUNT ELSE 0 END) AS big_payments, COUNT(CASE WHEN H_AMOUNT > 100 THEN 1 END) AS big_cnt FROM HISTORY GROUP BY H_W_ID;",
        "SELECT C_W_ID, C_STATE, CASE WHEN AVG(C_BALANCE) > 1000 THEN 'high_avg' ELSE 'low_avg' END AS avg_tier, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, C_STATE;",
        "SELECT OL_W_ID, OL_I_ID, SUM(OL_QUANTITY) AS total_qty, CASE WHEN SUM(OL_QUANTITY) > 100 THEN 'popular' ELSE 'regular' END AS label FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID;",
    ]
    queries.extend(case_templates)

    # ===== 8. CAST / date variations =====
    cast_templates = [
        "SELECT C_W_ID, C_ID, CAST(C_BALANCE AS BIGINT) AS bal_int, CAST(C_DISCOUNT AS DECIMAL(6,2)) AS disc_pct FROM CUSTOMER;",
        "SELECT I_ID, CAST(I_PRICE AS VARCHAR) AS price_str, CAST(I_IM_ID AS DECIMAL) AS im_dec FROM ITEM;",
        "SELECT W_ID, CAST(W_YTD AS BIGINT) AS ytd_bigint, CAST(W_TAX AS DOUBLE) AS tax_double FROM WAREHOUSE;",
        "SELECT S_W_ID, S_I_ID, CAST(S_QUANTITY AS DECIMAL(10,2)) AS qty_dec FROM STOCK;",
        "SELECT O_W_ID, O_ID, O_ENTRY_D, CAST(O_ENTRY_D AS DATE) AS entry_date FROM OORDER WHERE O_ENTRY_D IS NOT NULL;",
        "SELECT H_W_ID, H_C_ID, H_DATE, EXTRACT(YEAR FROM H_DATE) AS yr, EXTRACT(MONTH FROM H_DATE) AS mo FROM HISTORY WHERE H_DATE IS NOT NULL;",
        "SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, DATE_TRUNC('day', OL_DELIVERY_D) AS delivery_day FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;",
        "SELECT C_W_ID, DATE_TRUNC('month', C_SINCE) AS join_month, COUNT(*) AS new_cust FROM CUSTOMER WHERE C_SINCE IS NOT NULL GROUP BY C_W_ID, DATE_TRUNC('month', C_SINCE);",
        "SELECT O_W_ID, EXTRACT(DOW FROM O_ENTRY_D) AS day_of_week, COUNT(*) AS orders FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY O_W_ID, EXTRACT(DOW FROM O_ENTRY_D);",
        "SELECT H_W_ID, DATE_TRUNC('week', H_DATE) AS week, SUM(H_AMOUNT) AS total FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY H_W_ID, DATE_TRUNC('week', H_DATE);",
        "SELECT O_W_ID, O_ID, O_ENTRY_D + INTERVAL '7 days' AS week_later FROM OORDER WHERE O_ENTRY_D IS NOT NULL;",
        "SELECT C_W_ID, ABS(SUM(C_BALANCE)) AS abs_total, ROUND(AVG(C_BALANCE), 2) AS rnd_avg, FLOOR(MIN(C_BALANCE)) AS flr_min, CEIL(MAX(C_BALANCE)) AS cel_max FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT I_ID, POWER(I_PRICE, 2) AS sq_price, SQRT(I_PRICE) AS sqrt_price, LN(I_PRICE + 1) AS ln_price FROM ITEM WHERE I_PRICE > 0;",
    ]
    queries.extend(cast_templates)

    # ===== 9. NULL handling =====
    null_templates = [
        "SELECT O_ID, O_W_ID, O_CARRIER_ID, COALESCE(O_CARRIER_ID, -1) AS safe_carrier FROM OORDER;",
        "SELECT C_ID, C_MIDDLE, COALESCE(C_MIDDLE, 'XX') AS mid FROM CUSTOMER;",
        "SELECT OL_W_ID, OL_O_ID, OL_DELIVERY_D, CASE WHEN OL_DELIVERY_D IS NULL THEN 'pending' ELSE 'delivered' END AS status FROM ORDER_LINE;",
        "SELECT C_W_ID, COUNT(*) AS total, COUNT(C_MIDDLE) AS with_mid, COUNT(*) - COUNT(C_MIDDLE) AS without_mid FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT OL_W_ID, COUNT(OL_DELIVERY_D) AS delivered, COUNT(*) - COUNT(OL_DELIVERY_D) AS pending FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT O_W_ID, O_D_ID, COUNT(*) AS total, SUM(CASE WHEN O_CARRIER_ID IS NULL THEN 1 ELSE 0 END) AS unassigned FROM OORDER GROUP BY O_W_ID, O_D_ID;",
        "SELECT C_W_ID, C_ID, NULLIF(C_BALANCE, 0) AS non_zero_bal FROM CUSTOMER;",
        "SELECT S_W_ID, S_I_ID, NULLIF(S_QUANTITY, 0) AS safe_qty FROM STOCK;",
    ]
    queries.extend(null_templates)

    # ===== 10. Window function variations =====
    window_templates = []
    for table, num_cols, group_cols, _ in TABLE_AGG_INFO:
        if not num_cols or not group_cols:
            continue
        col = num_cols[0]
        gcol = group_cols[0]
        window_templates.append(
            f"SELECT {gcol}, {col}, ROW_NUMBER() OVER (PARTITION BY {gcol} ORDER BY {col} DESC) AS rn FROM {table};"
        )
        window_templates.append(
            f"SELECT {gcol}, {col}, RANK() OVER (PARTITION BY {gcol} ORDER BY {col}) AS rnk FROM {table};"
        )
        window_templates.append(
            f"SELECT {gcol}, {col}, SUM({col}) OVER (PARTITION BY {gcol}) AS part_sum FROM {table};"
        )
        window_templates.append(
            f"SELECT {gcol}, {col}, AVG({col}) OVER (PARTITION BY {gcol}) AS part_avg FROM {table};"
        )
        window_templates.append(
            f"SELECT {gcol}, {col}, {col} - AVG({col}) OVER () AS diff_from_avg FROM {table};"
        )
        window_templates.append(
            f"SELECT {gcol}, {col}, LAG({col}, 1) OVER (PARTITION BY {gcol} ORDER BY {col}) AS prev FROM {table};"
        )
        window_templates.append(
            f"SELECT {gcol}, {col}, LEAD({col}, 1) OVER (PARTITION BY {gcol} ORDER BY {col}) AS nxt FROM {table};"
        )
    queries.extend(window_templates)

    # ===== 11. CTE variations =====
    cte_templates = [
        "WITH w_agg AS (SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY C_W_ID) SELECT w.W_ID, w.W_NAME, COALESCE(wa.tot, 0) AS cust_total FROM WAREHOUSE w LEFT JOIN w_agg wa ON w.W_ID = wa.C_W_ID;",
        "WITH s_low AS (SELECT S_W_ID, S_I_ID FROM STOCK WHERE S_QUANTITY < 20) SELECT i.I_ID, i.I_NAME, COUNT(sl.S_I_ID) AS low_warehouses FROM ITEM i LEFT JOIN s_low sl ON i.I_ID = sl.S_I_ID GROUP BY i.I_ID, i.I_NAME;",
        "WITH o_cnt AS (SELECT O_W_ID, O_D_ID, COUNT(*) AS cnt FROM OORDER GROUP BY O_W_ID, O_D_ID) SELECT d.D_W_ID, d.D_ID, d.D_NAME, COALESCE(oc.cnt, 0) AS orders FROM DISTRICT d LEFT JOIN o_cnt oc ON d.D_W_ID = oc.O_W_ID AND d.D_ID = oc.O_D_ID;",
        "WITH per_cust AS (SELECT C_W_ID, C_ID, C_LAST, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > 0), orders_c AS (SELECT O_C_ID, O_W_ID, COUNT(*) AS n FROM OORDER GROUP BY O_C_ID, O_W_ID) SELECT pc.*, oc.n FROM per_cust pc LEFT JOIN orders_c oc ON pc.C_ID = oc.O_C_ID AND pc.C_W_ID = oc.O_W_ID;",
        "WITH stats AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_b, STDDEV(C_BALANCE) AS std_b FROM CUSTOMER GROUP BY C_W_ID) SELECT C_W_ID, avg_b, std_b, ROUND(avg_b / NULLIF(std_b, 0), 4) AS cv FROM stats;",
        "WITH dist_info AS (SELECT D_W_ID, D_ID, D_NAME, D_TAX FROM DISTRICT) SELECT di.D_W_ID, di.D_ID, di.D_NAME, COUNT(c.C_ID) AS cust_count FROM dist_info di LEFT JOIN CUSTOMER c ON di.D_W_ID = c.C_W_ID AND di.D_ID = c.C_D_ID GROUP BY di.D_W_ID, di.D_ID, di.D_NAME;",
        "WITH item_revenue AS (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_I_ID HAVING SUM(OL_AMOUNT) > 100) SELECT i.I_ID, i.I_NAME, i.I_PRICE, ir.rev FROM ITEM i JOIN item_revenue ir ON i.I_ID = ir.OL_I_ID;",
        "WITH stock_value AS (SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY * i.I_PRICE AS val FROM STOCK s JOIN ITEM i ON s.S_I_ID = i.I_ID) SELECT S_W_ID, SUM(val) AS total_val, COUNT(*) AS items, AVG(val) AS avg_val FROM stock_value GROUP BY S_W_ID;",
        "WITH cust_payments AS (SELECT H_C_W_ID, H_C_ID, SUM(H_AMOUNT) AS pmt FROM HISTORY GROUP BY H_C_W_ID, H_C_ID) SELECT c.C_W_ID, c.C_ID, c.C_LAST, COALESCE(cp.pmt, 0) AS total_paid FROM CUSTOMER c LEFT JOIN cust_payments cp ON c.C_W_ID = cp.H_C_W_ID AND c.C_ID = cp.H_C_ID;",
        "WITH order_totals AS (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS tot, COUNT(*) AS lines FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID), order_info AS (SELECT O_W_ID, O_ID, O_OL_CNT, O_ENTRY_D FROM OORDER) SELECT oi.*, ot.tot, ot.lines FROM order_info oi LEFT JOIN order_totals ot ON oi.O_W_ID = ot.OL_W_ID AND oi.O_ID = ot.OL_O_ID;",
        "WITH bad AS (SELECT C_W_ID, COUNT(*) AS n FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID), good AS (SELECT C_W_ID, COUNT(*) AS n FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID) SELECT COALESCE(b.C_W_ID, g.C_W_ID) AS w_id, COALESCE(b.n, 0) AS bad_cnt, COALESCE(g.n, 0) AS good_cnt FROM bad b FULL OUTER JOIN good g ON b.C_W_ID = g.C_W_ID;",
        "WITH per_wh AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_b FROM CUSTOMER GROUP BY C_W_ID) SELECT c.C_W_ID, c.C_ID, c.C_BALANCE, pw.avg_b FROM CUSTOMER c JOIN per_wh pw ON c.C_W_ID = pw.C_W_ID WHERE c.C_BALANCE > pw.avg_b;",
        "WITH active AS (SELECT DISTINCT O_C_ID, O_W_ID FROM OORDER) SELECT c.C_W_ID, c.C_ID, c.C_LAST, CASE WHEN a.O_C_ID IS NULL THEN 'inactive' ELSE 'active' END AS status FROM CUSTOMER c LEFT JOIN active a ON c.C_ID = a.O_C_ID AND c.C_W_ID = a.O_W_ID;",
        "WITH state_balance AS (SELECT C_STATE, AVG(C_BALANCE) AS avg_b, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_STATE) SELECT sb.C_STATE, sb.avg_b, sb.cnt, sb.cnt * sb.avg_b AS total FROM state_balance sb;",
        "WITH h_per AS (SELECT H_W_ID, H_D_ID, DATE_TRUNC('month', H_DATE) AS mo, SUM(H_AMOUNT) AS tot FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY H_W_ID, H_D_ID, DATE_TRUNC('month', H_DATE)) SELECT H_W_ID, mo, SUM(tot) AS total FROM h_per GROUP BY H_W_ID, mo;",
        "WITH warehouse_cust AS (SELECT C_W_ID, COUNT(*) AS cust FROM CUSTOMER GROUP BY C_W_ID), warehouse_ord AS (SELECT O_W_ID, COUNT(*) AS ord FROM OORDER GROUP BY O_W_ID) SELECT w.W_ID, w.W_NAME, COALESCE(wc.cust, 0) AS customers, COALESCE(wo.ord, 0) AS orders FROM WAREHOUSE w LEFT JOIN warehouse_cust wc ON w.W_ID = wc.C_W_ID LEFT JOIN warehouse_ord wo ON w.W_ID = wo.O_W_ID;",
    ]
    queries.extend(cte_templates)

    # ===== 12. Correlated subqueries =====
    correlated_templates = [
        "SELECT c.C_W_ID, c.C_ID, c.C_BALANCE FROM CUSTOMER c WHERE c.C_BALANCE > (SELECT AVG(c2.C_BALANCE) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID);",
        "SELECT c.C_W_ID, c.C_ID FROM CUSTOMER c WHERE c.C_PAYMENT_CNT < (SELECT AVG(c2.C_PAYMENT_CNT) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID);",
        "SELECT o.O_ID, o.O_W_ID FROM OORDER o WHERE EXISTS (SELECT 1 FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID AND ol.OL_AMOUNT > 100);",
        "SELECT o.O_ID, o.O_W_ID FROM OORDER o WHERE NOT EXISTS (SELECT 1 FROM NEW_ORDER no WHERE no.NO_O_ID = o.O_ID AND no.NO_W_ID = o.O_W_ID);",
        "SELECT s.S_W_ID, s.S_I_ID FROM STOCK s WHERE s.S_QUANTITY < (SELECT AVG(s2.S_QUANTITY) FROM STOCK s2 WHERE s2.S_W_ID = s.S_W_ID);",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY FROM STOCK s WHERE s.S_QUANTITY > (SELECT MAX(s2.S_QUANTITY) FROM STOCK s2 WHERE s2.S_W_ID = s.S_W_ID) * 0.8;",
        "SELECT c.C_W_ID, c.C_ID FROM CUSTOMER c WHERE NOT EXISTS (SELECT 1 FROM HISTORY h WHERE h.H_C_ID = c.C_ID AND h.H_C_W_ID = c.C_W_ID);",
        "SELECT d.D_W_ID, d.D_ID FROM DISTRICT d WHERE (SELECT SUM(c.C_BALANCE) FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID) > 1000;",
        "SELECT w.W_ID, w.W_NAME FROM WAREHOUSE w WHERE (SELECT COUNT(*) FROM DISTRICT d WHERE d.D_W_ID = w.W_ID) > 5;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE FROM ITEM i WHERE i.I_PRICE > (SELECT AVG(i2.I_PRICE) FROM ITEM i2 WHERE i2.I_IM_ID = i.I_IM_ID);",
        "SELECT ol.OL_W_ID, ol.OL_O_ID FROM ORDER_LINE ol WHERE ol.OL_AMOUNT > (SELECT AVG(ol2.OL_AMOUNT) FROM ORDER_LINE ol2 WHERE ol2.OL_W_ID = ol.OL_W_ID);",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST FROM CUSTOMER c WHERE c.C_PAYMENT_CNT = (SELECT MAX(c2.C_PAYMENT_CNT) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID);",
        "SELECT o.O_W_ID, o.O_ID FROM OORDER o WHERE o.O_OL_CNT >= (SELECT AVG(o2.O_OL_CNT) FROM OORDER o2 WHERE o2.O_W_ID = o.O_W_ID AND o2.O_D_ID = o.O_D_ID);",
        "SELECT h.H_W_ID, h.H_C_ID FROM HISTORY h WHERE h.H_AMOUNT > (SELECT AVG(h2.H_AMOUNT) * 2 FROM HISTORY h2 WHERE h2.H_W_ID = h.H_W_ID);",
        "SELECT c.C_W_ID, c.C_ID FROM CUSTOMER c WHERE c.C_ID IN (SELECT o.O_C_ID FROM OORDER o WHERE o.O_W_ID = c.C_W_ID AND o.O_OL_CNT > 10);",
    ]
    queries.extend(correlated_templates)

    # ===== 13. Uncorrelated subquery variations =====
    uncorr_templates = [
        "SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > (SELECT AVG(C_BALANCE) FROM CUSTOMER);",
        "SELECT I_ID, I_NAME, I_PRICE FROM ITEM WHERE I_PRICE = (SELECT MAX(I_PRICE) FROM ITEM);",
        "SELECT I_ID, I_NAME, I_PRICE FROM ITEM WHERE I_PRICE = (SELECT MIN(I_PRICE) FROM ITEM);",
        "SELECT S_W_ID, S_I_ID FROM STOCK WHERE S_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE > 80);",
        "SELECT W_ID, W_NAME FROM WAREHOUSE WHERE W_ID IN (SELECT C_W_ID FROM CUSTOMER WHERE C_BALANCE > 4000);",
        "SELECT D_W_ID, D_ID, D_NAME FROM DISTRICT WHERE D_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_TAX > 0.08);",
        "SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_ID IN (SELECT DISTINCT O_C_ID FROM OORDER WHERE O_OL_CNT > 10);",
        "SELECT OL_W_ID, OL_O_ID FROM ORDER_LINE WHERE OL_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE > 50);",
        "SELECT O_W_ID, O_ID FROM OORDER WHERE O_ID IN (SELECT NO_O_ID FROM NEW_ORDER);",
        "SELECT H_W_ID, H_C_ID FROM HISTORY WHERE H_AMOUNT > (SELECT AVG(H_AMOUNT) FROM HISTORY);",
        "SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_W_ID IN (SELECT W_ID FROM WAREHOUSE) AND C_BALANCE > 0;",
        "SELECT I_ID, I_NAME FROM ITEM WHERE I_ID NOT IN (SELECT DISTINCT S_I_ID FROM STOCK WHERE S_QUANTITY = 0);",
        "SELECT W_ID, W_NAME, (SELECT COUNT(*) FROM DISTRICT WHERE D_W_ID = W_ID) AS district_count FROM WAREHOUSE;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) AS cust_count FROM DISTRICT d;",
        "SELECT I_ID, I_NAME, I_PRICE, (SELECT AVG(I_PRICE) FROM ITEM) AS overall_avg FROM ITEM;",
        "SELECT o.O_W_ID, o.O_ID, ot.total FROM OORDER o JOIN (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) ot ON o.O_W_ID = ot.OL_W_ID AND o.O_ID = ot.OL_O_ID;",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, iv.I_NAME FROM STOCK s JOIN (SELECT I_ID, I_NAME FROM ITEM WHERE I_PRICE > 50) iv ON s.S_I_ID = iv.I_ID;",
    ]
    queries.extend(uncorr_templates)

    # ===== 14. FULL OUTER JOIN + COALESCE =====
    fojs = [
        "SELECT COALESCE(w.W_ID, d.D_W_ID) AS w_id, w.W_NAME, COUNT(d.D_ID) AS num_districts FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY COALESCE(w.W_ID, d.D_W_ID), w.W_NAME;",
        "SELECT COALESCE(i.I_ID, s.S_I_ID) AS item_id, i.I_NAME, SUM(s.S_QUANTITY) AS total_stock FROM ITEM i FULL OUTER JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY COALESCE(i.I_ID, s.S_I_ID), i.I_NAME;",
        "SELECT COALESCE(c.C_W_ID, h.H_C_W_ID) AS w_id, COALESCE(c.C_ID, h.H_C_ID) AS c_id, c.C_BALANCE, SUM(h.H_AMOUNT) AS hist FROM CUSTOMER c FULL OUTER JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY COALESCE(c.C_W_ID, h.H_C_W_ID), COALESCE(c.C_ID, h.H_C_ID), c.C_BALANCE;",
        "SELECT COALESCE(o.O_W_ID, no.NO_W_ID) AS w_id, COALESCE(o.O_D_ID, no.NO_D_ID) AS d_id, COUNT(o.O_ID) AS orders, COUNT(no.NO_O_ID) AS pending FROM OORDER o FULL OUTER JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID AND o.O_D_ID = no.NO_D_ID GROUP BY COALESCE(o.O_W_ID, no.NO_W_ID), COALESCE(o.O_D_ID, no.NO_D_ID);",
        "SELECT COALESCE(s.S_I_ID, ol.OL_I_ID) AS i_id, COALESCE(s.S_W_ID, ol.OL_SUPPLY_W_ID) AS w_id, SUM(s.S_QUANTITY) AS stock, SUM(ol.OL_QUANTITY) AS ordered FROM STOCK s FULL OUTER JOIN ORDER_LINE ol ON s.S_I_ID = ol.OL_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID GROUP BY COALESCE(s.S_I_ID, ol.OL_I_ID), COALESCE(s.S_W_ID, ol.OL_SUPPLY_W_ID);",
        "SELECT COALESCE(o.O_ID, ol.OL_O_ID) AS o_id, COALESCE(o.O_W_ID, ol.OL_W_ID) AS w_id, o.O_OL_CNT, COUNT(ol.OL_NUMBER) AS lines FROM OORDER o FULL OUTER JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY COALESCE(o.O_ID, ol.OL_O_ID), COALESCE(o.O_W_ID, ol.OL_W_ID), o.O_OL_CNT;",
        "SELECT COALESCE(c.C_W_ID, d.D_W_ID) AS w_id, COALESCE(c.C_D_ID, d.D_ID) AS d_id, COUNT(c.C_ID) AS cust, d.D_NAME FROM CUSTOMER c FULL OUTER JOIN DISTRICT d ON c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID GROUP BY COALESCE(c.C_W_ID, d.D_W_ID), COALESCE(c.C_D_ID, d.D_ID), d.D_NAME;",
    ]
    queries.extend(fojs)

    # ===== 15. Table functions & LATERAL =====
    tf_templates = [
        "SELECT n FROM generate_series(1, 20) t(n);",
        "SELECT n FROM range(1, 20) t(n);",
        "SELECT n, n * 2 AS doubled FROM range(1, 30) t(n) WHERE n IN (SELECT DISTINCT C_W_ID FROM CUSTOMER);",
        "SELECT n, COUNT(c.C_ID) AS cust_count FROM generate_series(1, 10) r(n) LEFT JOIN CUSTOMER c ON c.C_W_ID = r.n GROUP BY n;",
        "SELECT r.n AS target_qty, COUNT(*) AS below_cnt FROM range(10, 100, 10) r(n) JOIN STOCK s ON s.S_QUANTITY < r.n GROUP BY r.n;",
        "SELECT g.m AS month_num, COUNT(DISTINCT o.O_ID) AS orders FROM generate_series(1, 12) g(m) LEFT JOIN OORDER o ON EXTRACT(MONTH FROM o.O_ENTRY_D) = g.m GROUP BY g.m;",
        "SELECT t.t AS tier, COUNT(*) FROM (SELECT unnest(['premium', 'standard', 'basic']) AS t) t CROSS JOIN CUSTOMER c GROUP BY t.t;",
        "SELECT c.C_ID, c.C_W_ID, lat.pay_total FROM CUSTOMER c JOIN LATERAL (SELECT SUM(H_AMOUNT) AS pay_total FROM HISTORY WHERE H_C_ID = c.C_ID AND H_C_W_ID = c.C_W_ID) lat ON TRUE;",
        "SELECT w.W_ID, w.W_NAME, lat.num FROM WAREHOUSE w JOIN LATERAL (SELECT COUNT(*) AS num FROM DISTRICT WHERE D_W_ID = w.W_ID) lat ON TRUE;",
        "SELECT d.D_W_ID, d.D_ID, lat.avg_bal FROM DISTRICT d JOIN LATERAL (SELECT AVG(C_BALANCE) AS avg_bal FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) lat ON TRUE;",
        "SELECT o.O_W_ID, o.O_ID, lat.total_amount FROM OORDER o JOIN LATERAL (SELECT SUM(OL_AMOUNT) AS total_amount FROM ORDER_LINE WHERE OL_O_ID = o.O_ID AND OL_W_ID = o.O_W_ID) lat ON TRUE;",
        "SELECT i.I_ID, i.I_NAME, lat.times_ordered, lat.total_revenue FROM ITEM i JOIN LATERAL (SELECT COUNT(*) AS times_ordered, SUM(OL_AMOUNT) AS total_revenue FROM ORDER_LINE WHERE OL_I_ID = i.I_ID) lat ON TRUE;",
        "SELECT w.W_ID, lat.d_cnt, lat.c_cnt FROM WAREHOUSE w, LATERAL (SELECT (SELECT COUNT(*) FROM DISTRICT WHERE D_W_ID = w.W_ID) AS d_cnt, (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = w.W_ID) AS c_cnt) lat;",
    ]
    queries.extend(tf_templates)

    # ===== 16. Complex TPC-H-style multi-operator queries =====
    tpch_templates = [
        "SELECT w.W_ID, w.W_NAME, d.D_ID, COUNT(DISTINCT c.C_ID) AS cust, SUM(c.C_BALANCE) AS tot_bal, AVG(c.C_BALANCE) AS avg_bal, MIN(c.C_BALANCE) AS min_bal, MAX(c.C_BALANCE) AS max_bal FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY w.W_ID, w.W_NAME, d.D_ID HAVING COUNT(DISTINCT c.C_ID) > 1;",
        "SELECT c.C_W_ID, c.C_D_ID, c.C_CREDIT, COUNT(*) AS cust, SUM(c.C_BALANCE) AS bal, CASE WHEN SUM(c.C_BALANCE) > 10000 THEN 'profitable' WHEN SUM(c.C_BALANCE) > 0 THEN 'neutral' ELSE 'loss' END AS health FROM CUSTOMER c GROUP BY c.C_W_ID, c.C_D_ID, c.C_CREDIT HAVING COUNT(*) > 1;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, COUNT(ol.OL_NUMBER) AS times_sold, SUM(ol.OL_QUANTITY) AS total_qty, SUM(ol.OL_AMOUNT) AS revenue, ROUND(AVG(ol.OL_AMOUNT), 2) AS avg_price FROM ITEM i LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE;",
        "SELECT s.S_W_ID, COUNT(*) AS items, AVG(s.S_QUANTITY) AS avg_qty, STDDEV(s.S_QUANTITY) AS std_qty, SUM(s.S_QUANTITY * i.I_PRICE) AS total_value, COUNT(CASE WHEN s.S_QUANTITY < 20 THEN 1 END) AS low_stock_items FROM STOCK s JOIN ITEM i ON s.S_I_ID = i.I_ID GROUP BY s.S_W_ID;",
        "SELECT w.W_STATE, COUNT(DISTINCT w.W_ID) AS warehouses, COUNT(DISTINCT c.C_ID) AS customers, SUM(c.C_BALANCE) AS total_bal, AVG(c.C_DISCOUNT) AS avg_discount FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE;",
        "SELECT o.O_W_ID, o.O_D_ID, COUNT(DISTINCT o.O_ID) AS orders, SUM(ol.OL_AMOUNT) AS revenue, AVG(ol.OL_AMOUNT) AS avg_line, COUNT(DISTINCT ol.OL_I_ID) AS unique_items FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_W_ID, o.O_D_ID HAVING SUM(ol.OL_AMOUNT) > 100;",
        "SELECT c.C_W_ID, c.C_STATE, COUNT(*) AS cust, COUNT(DISTINCT o.O_ID) AS orders, COALESCE(SUM(ol.OL_AMOUNT), 0) AS revenue FROM CUSTOMER c LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID LEFT JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY c.C_W_ID, c.C_STATE;",
        "SELECT DATE_TRUNC('month', o.O_ENTRY_D) AS mo, COUNT(DISTINCT o.O_ID) AS orders, SUM(ol.OL_AMOUNT) AS revenue, COUNT(DISTINCT ol.OL_I_ID) AS unique_items, AVG(o.O_OL_CNT) AS avg_lines_per_order FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID WHERE o.O_ENTRY_D IS NOT NULL GROUP BY DATE_TRUNC('month', o.O_ENTRY_D);",
        "WITH top AS (SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > 1000), orders AS (SELECT O_C_ID, O_W_ID, COUNT(*) AS n, SUM(O_OL_CNT) AS tot_lines FROM OORDER GROUP BY O_C_ID, O_W_ID) SELECT t.C_W_ID, t.C_ID, t.C_BALANCE, COALESCE(o.n, 0) AS orders, COALESCE(o.tot_lines, 0) AS lines FROM top t LEFT JOIN orders o ON t.C_ID = o.O_C_ID AND t.C_W_ID = o.O_W_ID;",
        "WITH cust_summary AS (SELECT C_W_ID, C_D_ID, COUNT(*) AS cust_cnt, SUM(C_BALANCE) AS bal, SUM(C_YTD_PAYMENT) AS pay FROM CUSTOMER GROUP BY C_W_ID, C_D_ID), dist_info AS (SELECT D_W_ID, D_ID, D_NAME, D_TAX FROM DISTRICT) SELECT di.D_NAME, cs.cust_cnt, cs.bal, cs.pay, ROUND(cs.pay * di.D_TAX, 2) AS tax_est FROM cust_summary cs JOIN dist_info di ON cs.C_W_ID = di.D_W_ID AND cs.C_D_ID = di.D_ID;",
        "SELECT c.C_W_ID, c.C_CREDIT, COUNT(*) AS n, AVG(c.C_BALANCE) AS avg_b, STDDEV(c.C_BALANCE) AS std_b, SUM(CASE WHEN c.C_BALANCE < 0 THEN 1 ELSE 0 END) AS negatives, BOOL_OR(c.C_BALANCE > 5000) AS has_vip FROM CUSTOMER c GROUP BY c.C_W_ID, c.C_CREDIT;",
        "SELECT ol.OL_W_ID, ol.OL_I_ID, COUNT(*) AS lines, SUM(ol.OL_QUANTITY) AS qty, SUM(ol.OL_AMOUNT) AS revenue, SUM(CASE WHEN ol.OL_DELIVERY_D IS NULL THEN 1 ELSE 0 END) AS pending, SUM(CASE WHEN ol.OL_DELIVERY_D IS NOT NULL THEN 1 ELSE 0 END) AS delivered FROM ORDER_LINE ol GROUP BY ol.OL_W_ID, ol.OL_I_ID;",
        "SELECT s.S_W_ID, i.I_ID, i.I_NAME, s.S_QUANTITY, i.I_PRICE, s.S_QUANTITY * i.I_PRICE AS val, CASE WHEN s.S_QUANTITY = 0 THEN 'out' WHEN s.S_QUANTITY < 20 THEN 'low' WHEN s.S_QUANTITY < 50 THEN 'med' ELSE 'ok' END AS status FROM STOCK s JOIN ITEM i ON s.S_I_ID = i.I_ID;",
        "WITH per_w AS (SELECT C_W_ID, C_STATE, COUNT(*) AS cnt, AVG(C_BALANCE) AS avg_b FROM CUSTOMER GROUP BY C_W_ID, C_STATE) SELECT p.C_W_ID, p.C_STATE, p.cnt, p.avg_b, RANK() OVER (PARTITION BY p.C_W_ID ORDER BY p.cnt DESC) AS state_rank FROM per_w p;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, COUNT(c.C_ID) AS cust, SUM(c.C_BALANCE) AS bal, SUM(c.C_YTD_PAYMENT) AS pay, SUM(c.C_PAYMENT_CNT) AS pay_cnt, BOOL_AND(c.C_BALANCE >= 0) AS all_positive FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID, d.D_NAME;",
        "WITH tp AS (SELECT i.I_ID, i.I_NAME, i.I_PRICE, SUM(ol.OL_QUANTITY) AS qty FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE HAVING SUM(ol.OL_QUANTITY) > 0) SELECT tp.I_NAME, tp.I_PRICE, tp.qty, tp.qty * tp.I_PRICE AS revenue FROM tp;",
    ]
    queries.extend(tpch_templates)

    # ===== 17. ORDER BY / LIMIT (intentionally non-incrementable) =====
    order_limit = [
        "SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER ORDER BY C_BALANCE DESC LIMIT 100;",
        "SELECT C_W_ID, C_ID, C_LAST FROM CUSTOMER ORDER BY C_W_ID, C_ID LIMIT 50;",
        "SELECT I_ID, I_NAME, I_PRICE FROM ITEM ORDER BY I_PRICE DESC LIMIT 20;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK ORDER BY S_QUANTITY LIMIT 100;",
        "SELECT O_W_ID, O_ID, O_ENTRY_D FROM OORDER ORDER BY O_ENTRY_D DESC LIMIT 50;",
        "SELECT C_W_ID, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID ORDER BY cnt DESC LIMIT 10;",
        "SELECT I_ID, I_NAME, I_PRICE FROM ITEM ORDER BY I_PRICE LIMIT 10 OFFSET 10;",
        "SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID ORDER BY rev DESC LIMIT 20;",
        "SELECT C_W_ID, C_STATE, AVG(C_BALANCE) AS avg_b FROM CUSTOMER GROUP BY C_W_ID, C_STATE ORDER BY avg_b DESC LIMIT 15;",
        "SELECT W_ID, W_NAME, W_YTD FROM WAREHOUSE ORDER BY W_YTD DESC LIMIT 5;",
    ]
    queries.extend(order_limit)

    return queries


def composite_queries() -> list[str]:
    """Composite operator queries: aggregates on top of joins, windows on top of joins,
    joins on top of aggregates, HAVING on top of joins, + data type / function coverage."""
    queries = []

    # ===== A. 4-way JOINs (high complexity) =====
    four_way_joins = [
        ("WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID",
         "w.W_ID, d.D_ID, c.C_ID, o.O_ID, c.C_BALANCE, o.O_OL_CNT",
         "w.W_ID, d.D_ID"),
        ("WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID",
         "w.W_ID, w.W_NAME, d.D_ID, o.O_ID, ol.OL_AMOUNT",
         "w.W_ID, d.D_ID"),
        ("CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID JOIN ITEM i ON ol.OL_I_ID = i.I_ID",
         "c.C_ID, c.C_LAST, o.O_ID, ol.OL_AMOUNT, i.I_NAME, i.I_PRICE",
         "c.C_W_ID, c.C_ID"),
        ("ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID JOIN ORDER_LINE ol ON ol.OL_I_ID = i.I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID JOIN OORDER o ON ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID",
         "i.I_ID, i.I_NAME, s.S_W_ID, s.S_QUANTITY, ol.OL_AMOUNT, o.O_OL_CNT",
         "i.I_ID"),
        ("WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID",
         "w.W_ID, w.W_NAME, c.C_ID, o.O_ID, ol.OL_AMOUNT",
         "w.W_ID"),
        ("OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID JOIN ITEM i ON ol.OL_I_ID = i.I_ID JOIN STOCK s ON i.I_ID = s.S_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID",
         "o.O_ID, ol.OL_AMOUNT, i.I_NAME, i.I_PRICE, s.S_QUANTITY",
         "o.O_W_ID, o.O_ID"),
        ("DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID",
         "d.D_NAME, c.C_ID, o.O_ID, ol.OL_AMOUNT",
         "d.D_W_ID, d.D_ID"),
        ("WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID",
         "w.W_ID, d.D_ID, c.C_ID, h.H_AMOUNT",
         "w.W_ID, d.D_ID"),
    ]
    for join_expr, proj, gkey in four_way_joins:
        queries.append(f"SELECT {proj} FROM {join_expr};")
        queries.append(f"SELECT {gkey}, COUNT(*) AS cnt FROM {join_expr} GROUP BY {gkey};")
        queries.append(f"SELECT {gkey}, COUNT(*) AS cnt FROM {join_expr} GROUP BY {gkey} HAVING COUNT(*) > 1;")
        queries.append(f"SELECT {gkey}, COUNT(DISTINCT CAST(({gkey.split(', ')[0]}) AS VARCHAR)) AS uniq, COUNT(*) AS cnt FROM {join_expr} GROUP BY {gkey};")

    # ===== 19. Multi-CTE complex queries =====
    multi_cte = [
        "WITH cust_by_w AS (SELECT C_W_ID, COUNT(*) AS c_cnt, SUM(C_BALANCE) AS c_bal FROM CUSTOMER GROUP BY C_W_ID), dist_by_w AS (SELECT D_W_ID, COUNT(*) AS d_cnt, SUM(D_YTD) AS d_ytd FROM DISTRICT GROUP BY D_W_ID), ord_by_w AS (SELECT O_W_ID, COUNT(*) AS o_cnt FROM OORDER GROUP BY O_W_ID) SELECT w.W_ID, w.W_NAME, COALESCE(cbw.c_cnt, 0) AS customers, COALESCE(dbw.d_cnt, 0) AS districts, COALESCE(obw.o_cnt, 0) AS orders FROM WAREHOUSE w LEFT JOIN cust_by_w cbw ON w.W_ID = cbw.C_W_ID LEFT JOIN dist_by_w dbw ON w.W_ID = dbw.D_W_ID LEFT JOIN ord_by_w obw ON w.W_ID = obw.O_W_ID;",
        "WITH high_value_items AS (SELECT I_ID, I_NAME, I_PRICE FROM ITEM WHERE I_PRICE > 50), stock_of_high AS (SELECT S_W_ID, S_I_ID, S_QUANTITY FROM STOCK WHERE S_I_ID IN (SELECT I_ID FROM high_value_items)), orders_of_high AS (SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE WHERE OL_I_ID IN (SELECT I_ID FROM high_value_items) GROUP BY OL_W_ID, OL_I_ID) SELECT hvi.I_ID, hvi.I_NAME, hvi.I_PRICE, sh.S_W_ID, sh.S_QUANTITY, COALESCE(oh.rev, 0) AS revenue FROM high_value_items hvi JOIN stock_of_high sh ON hvi.I_ID = sh.S_I_ID LEFT JOIN orders_of_high oh ON hvi.I_ID = oh.OL_I_ID AND sh.S_W_ID = oh.OL_W_ID;",
        "WITH pay_totals AS (SELECT H_C_W_ID, H_C_ID, SUM(H_AMOUNT) AS pay FROM HISTORY GROUP BY H_C_W_ID, H_C_ID), ord_totals AS (SELECT O_W_ID, O_C_ID, COUNT(*) AS orders, SUM(O_OL_CNT) AS lines FROM OORDER GROUP BY O_W_ID, O_C_ID) SELECT c.C_W_ID, c.C_ID, c.C_LAST, c.C_BALANCE, COALESCE(pt.pay, 0) AS paid, COALESCE(ot.orders, 0) AS orders, COALESCE(ot.lines, 0) AS lines FROM CUSTOMER c LEFT JOIN pay_totals pt ON c.C_W_ID = pt.H_C_W_ID AND c.C_ID = pt.H_C_ID LEFT JOIN ord_totals ot ON c.C_W_ID = ot.O_W_ID AND c.C_ID = ot.O_C_ID;",
        "WITH delivered AS (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS tot FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL GROUP BY OL_W_ID, OL_O_ID), pending AS (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS tot FROM ORDER_LINE WHERE OL_DELIVERY_D IS NULL GROUP BY OL_W_ID, OL_O_ID) SELECT COALESCE(d.OL_W_ID, p.OL_W_ID) AS w_id, COALESCE(d.OL_O_ID, p.OL_O_ID) AS o_id, COALESCE(d.tot, 0) AS delivered_amt, COALESCE(p.tot, 0) AS pending_amt FROM delivered d FULL OUTER JOIN pending p ON d.OL_W_ID = p.OL_W_ID AND d.OL_O_ID = p.OL_O_ID;",
        "WITH good AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_b, COUNT(*) AS cnt FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID), bad AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_b, COUNT(*) AS cnt FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID) SELECT COALESCE(g.C_W_ID, b.C_W_ID) AS w_id, g.avg_b AS good_avg, g.cnt AS good_cnt, b.avg_b AS bad_avg, b.cnt AS bad_cnt FROM good g FULL OUTER JOIN bad b ON g.C_W_ID = b.C_W_ID;",
        "WITH revenue_by_item AS (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev, COUNT(*) AS sales FROM ORDER_LINE GROUP BY OL_I_ID), stock_by_item AS (SELECT S_I_ID, SUM(S_QUANTITY) AS stock FROM STOCK GROUP BY S_I_ID) SELECT i.I_ID, i.I_NAME, i.I_PRICE, COALESCE(rbi.rev, 0) AS revenue, COALESCE(rbi.sales, 0) AS sales, COALESCE(sbi.stock, 0) AS stock FROM ITEM i LEFT JOIN revenue_by_item rbi ON i.I_ID = rbi.OL_I_ID LEFT JOIN stock_by_item sbi ON i.I_ID = sbi.S_I_ID;",
        "WITH wh_stats AS (SELECT W_ID, W_YTD, W_TAX FROM WAREHOUSE), cust_stats AS (SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal, STDDEV(C_BALANCE) AS std_bal FROM CUSTOMER GROUP BY C_W_ID), ord_stats AS (SELECT O_W_ID, COUNT(*) AS orders, AVG(O_OL_CNT) AS avg_lines FROM OORDER GROUP BY O_W_ID) SELECT ws.W_ID, ws.W_YTD, cs.avg_bal, cs.std_bal, os.orders, os.avg_lines FROM wh_stats ws LEFT JOIN cust_stats cs ON ws.W_ID = cs.C_W_ID LEFT JOIN ord_stats os ON ws.W_ID = os.O_W_ID;",
        "WITH active_items AS (SELECT DISTINCT OL_I_ID FROM ORDER_LINE), stocked_items AS (SELECT DISTINCT S_I_ID FROM STOCK WHERE S_QUANTITY > 0) SELECT i.I_ID, i.I_NAME, CASE WHEN a.OL_I_ID IS NULL THEN 'never_ordered' WHEN s.S_I_ID IS NULL THEN 'out_of_stock' ELSE 'active' END AS status FROM ITEM i LEFT JOIN active_items a ON i.I_ID = a.OL_I_ID LEFT JOIN stocked_items s ON i.I_ID = s.S_I_ID;",
    ]
    queries.extend(multi_cte)

    # ===== 20. Window functions + aggregates combined =====
    window_combined = [
        "SELECT C_W_ID, C_D_ID, AVG(C_BALANCE) AS avg_bal, RANK() OVER (PARTITION BY C_W_ID ORDER BY AVG(C_BALANCE) DESC) AS rnk FROM CUSTOMER GROUP BY C_W_ID, C_D_ID;",
        "SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS rev, ROW_NUMBER() OVER (PARTITION BY OL_W_ID ORDER BY SUM(OL_AMOUNT) DESC) AS top_rank FROM ORDER_LINE GROUP BY OL_W_ID, OL_I_ID;",
        "SELECT S_W_ID, COUNT(*) AS items, PERCENT_RANK() OVER (ORDER BY COUNT(*)) AS pr FROM STOCK GROUP BY S_W_ID;",
        "SELECT D_W_ID, D_ID, D_YTD, D_YTD / SUM(D_YTD) OVER (PARTITION BY D_W_ID) AS pct FROM DISTRICT;",
        "SELECT H_W_ID, H_D_ID, SUM(H_AMOUNT) AS tot, LAG(SUM(H_AMOUNT)) OVER (PARTITION BY H_W_ID ORDER BY H_D_ID) AS prev FROM HISTORY GROUP BY H_W_ID, H_D_ID;",
        "SELECT C_W_ID, C_STATE, COUNT(*) AS cnt, NTILE(4) OVER (ORDER BY COUNT(*)) AS quartile FROM CUSTOMER GROUP BY C_W_ID, C_STATE;",
        "SELECT O_W_ID, O_D_ID, COUNT(*) AS orders, SUM(COUNT(*)) OVER (PARTITION BY O_W_ID) AS w_total FROM OORDER GROUP BY O_W_ID, O_D_ID;",
        "SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS tot, SUM(SUM(OL_AMOUNT)) OVER (PARTITION BY OL_W_ID ORDER BY OL_O_ID ROWS UNBOUNDED PRECEDING) AS cumul FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID;",
    ]
    queries.extend(window_combined)

    # ===== 21. Correlated scalar subqueries in SELECT =====
    correlated_select = [
        "SELECT w.W_ID, w.W_NAME, (SELECT COUNT(*) FROM DISTRICT d WHERE d.D_W_ID = w.W_ID) AS d_cnt, (SELECT COUNT(*) FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID) AS c_cnt FROM WAREHOUSE w;",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST, (SELECT COUNT(*) FROM OORDER o WHERE o.O_C_ID = c.C_ID AND o.O_W_ID = c.C_W_ID) AS order_count, (SELECT SUM(h.H_AMOUNT) FROM HISTORY h WHERE h.H_C_ID = c.C_ID AND h.H_C_W_ID = c.C_W_ID) AS total_payments FROM CUSTOMER c;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, (SELECT COUNT(*) FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID) AS cust, (SELECT AVG(c.C_BALANCE) FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID) AS avg_bal FROM DISTRICT d;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, (SELECT SUM(S_QUANTITY) FROM STOCK WHERE S_I_ID = i.I_ID) AS total_stock, (SELECT COUNT(*) FROM ORDER_LINE WHERE OL_I_ID = i.I_ID) AS times_ordered FROM ITEM i;",
        "SELECT o.O_W_ID, o.O_ID, o.O_OL_CNT, (SELECT COUNT(*) FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID) AS actual_lines, (SELECT SUM(ol.OL_AMOUNT) FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID) AS order_total FROM OORDER o;",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, (SELECT COUNT(*) FROM ORDER_LINE WHERE OL_I_ID = s.S_I_ID AND OL_SUPPLY_W_ID = s.S_W_ID) AS sales_count FROM STOCK s;",
    ]
    queries.extend(correlated_select)

    # ===== 22. Nested subqueries =====
    nested = [
        "SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_YTD > (SELECT AVG(W_YTD) FROM WAREHOUSE));",
        "SELECT I_ID, I_NAME FROM ITEM WHERE I_ID IN (SELECT OL_I_ID FROM ORDER_LINE WHERE OL_AMOUNT > (SELECT AVG(OL_AMOUNT) FROM ORDER_LINE));",
        "SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_ID IN (SELECT O_C_ID FROM OORDER WHERE O_W_ID IN (SELECT W_ID FROM WAREHOUSE WHERE W_TAX > 0.05));",
        "SELECT O_W_ID, O_ID FROM OORDER WHERE O_ID IN (SELECT OL_O_ID FROM ORDER_LINE WHERE OL_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE > 80));",
        "SELECT S_W_ID, S_I_ID FROM STOCK WHERE S_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE IN (SELECT MAX(I_PRICE) FROM ITEM UNION SELECT MIN(I_PRICE) FROM ITEM));",
    ]
    queries.extend(nested)

    # ===== 23. More CROSS JOIN queries =====
    cross_joins = [
        "SELECT w.W_ID, d.D_ID FROM WAREHOUSE w CROSS JOIN DISTRICT d WHERE d.D_W_ID = w.W_ID;",
        "SELECT w.W_ID, w.W_NAME, t.tier FROM WAREHOUSE w CROSS JOIN (SELECT unnest(['A', 'B', 'C']) AS tier) t;",
        "SELECT d.D_W_ID, d.D_ID, m.mo FROM DISTRICT d CROSS JOIN generate_series(1, 12) m(mo);",
        "SELECT w.W_ID, w.W_NAME, COUNT(DISTINCT g.n) AS slots FROM WAREHOUSE w CROSS JOIN generate_series(1, 10) g(n) WHERE g.n <= w.W_ID GROUP BY w.W_ID, w.W_NAME;",
        "SELECT c.C_CREDIT, t.tier, COUNT(*) AS cnt FROM CUSTOMER c CROSS JOIN (SELECT unnest(['low', 'med', 'high']) AS tier) t WHERE (t.tier = 'high' AND c.C_BALANCE > 4000) OR (t.tier = 'med' AND c.C_BALANCE BETWEEN 1000 AND 4000) OR (t.tier = 'low' AND c.C_BALANCE < 1000) GROUP BY c.C_CREDIT, t.tier;",
    ]
    queries.extend(cross_joins)

    # ===== 24. More LATERAL queries =====
    more_lateral = [
        "SELECT c.C_ID, c.C_W_ID, c.C_LAST, lat.max_pay FROM CUSTOMER c JOIN LATERAL (SELECT MAX(h.H_AMOUNT) AS max_pay FROM HISTORY h WHERE h.H_C_ID = c.C_ID AND h.H_C_W_ID = c.C_W_ID) lat ON TRUE WHERE lat.max_pay > 100;",
        "SELECT d.D_W_ID, d.D_ID, lat.top_cust_bal FROM DISTRICT d JOIN LATERAL (SELECT MAX(C_BALANCE) AS top_cust_bal FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) lat ON TRUE;",
        "SELECT w.W_ID, w.W_NAME, lat.tot_rev FROM WAREHOUSE w JOIN LATERAL (SELECT SUM(ol.OL_AMOUNT) AS tot_rev FROM ORDER_LINE ol WHERE ol.OL_W_ID = w.W_ID) lat ON TRUE;",
        "SELECT i.I_ID, i.I_NAME, lat.stock_total FROM ITEM i JOIN LATERAL (SELECT SUM(S_QUANTITY) AS stock_total FROM STOCK WHERE S_I_ID = i.I_ID) lat ON TRUE WHERE lat.stock_total > 0;",
        "SELECT o.O_W_ID, o.O_ID, o.O_OL_CNT, lat.lines, lat.tot FROM OORDER o JOIN LATERAL (SELECT COUNT(*) AS lines, SUM(OL_AMOUNT) AS tot FROM ORDER_LINE WHERE OL_O_ID = o.O_ID AND OL_W_ID = o.O_W_ID) lat ON TRUE;",
        "SELECT w.W_ID, stats.d_cnt, stats.c_cnt, stats.o_cnt FROM WAREHOUSE w, LATERAL (SELECT (SELECT COUNT(*) FROM DISTRICT WHERE D_W_ID = w.W_ID) AS d_cnt, (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = w.W_ID) AS c_cnt, (SELECT COUNT(*) FROM OORDER WHERE O_W_ID = w.W_ID) AS o_cnt) stats;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, lat.cust_count FROM DISTRICT d, LATERAL (SELECT COUNT(*) AS cust_count FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) lat;",
    ]
    queries.extend(more_lateral)

    # ===== 25. More table function queries =====
    more_tf = [
        "SELECT r AS warehouse_id, (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = r) AS cust_count FROM generate_series(1, 10) t(r);",
        "SELECT n, (SELECT W_NAME FROM WAREHOUSE WHERE W_ID = n) AS w_name FROM range(1, 11) t(n);",
        "SELECT c.C_W_ID, u.v AS creditview FROM CUSTOMER c CROSS JOIN (SELECT unnest(['BC', 'GC']) AS v) u WHERE c.C_CREDIT = u.v;",
        "SELECT s.i AS scale, COUNT(c.C_ID) AS customers FROM generate_series(1, 5) s(i) LEFT JOIN CUSTOMER c ON c.C_W_ID <= s.i GROUP BY s.i;",
        "SELECT w.W_ID, s.s AS slot FROM WAREHOUSE w, generate_series(1, 3) s(s) WHERE s.s <= w.W_TAX * 100;",
        "SELECT r.n AS benchmark, COUNT(s.S_W_ID) AS low_stock_items FROM range(5, 55, 5) r(n) LEFT JOIN STOCK s ON s.S_QUANTITY < r.n GROUP BY r.n;",
    ]
    queries.extend(more_tf)

    # ===== 26. GROUP BY ROLLUP / GROUPING SETS (advanced aggregation) =====
    rollup_templates = [
        "SELECT C_W_ID, C_D_ID, COUNT(*) AS cnt FROM CUSTOMER GROUP BY ROLLUP(C_W_ID, C_D_ID);",
        "SELECT C_W_ID, C_STATE, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY ROLLUP(C_W_ID, C_STATE);",
        "SELECT O_W_ID, O_D_ID, COUNT(*) AS orders FROM OORDER GROUP BY CUBE(O_W_ID, O_D_ID);",
        "SELECT OL_W_ID, OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY GROUPING SETS ((OL_W_ID), (OL_I_ID), (OL_W_ID, OL_I_ID));",
        "SELECT S_W_ID, COUNT(*) AS items, SUM(S_QUANTITY) AS stock FROM STOCK GROUP BY ROLLUP(S_W_ID);",
    ]
    queries.extend(rollup_templates)

    # ===== 27. EXCEPT / INTERSECT =====
    set_ops = [
        "SELECT DISTINCT C_W_ID FROM CUSTOMER EXCEPT SELECT DISTINCT W_ID FROM WAREHOUSE;",
        "SELECT DISTINCT W_ID FROM WAREHOUSE EXCEPT SELECT DISTINCT C_W_ID FROM CUSTOMER;",
        "SELECT DISTINCT OL_I_ID FROM ORDER_LINE INTERSECT SELECT DISTINCT S_I_ID FROM STOCK;",
        "SELECT DISTINCT C_W_ID FROM CUSTOMER INTERSECT SELECT DISTINCT H_C_W_ID FROM HISTORY;",
        "SELECT I_ID FROM ITEM EXCEPT SELECT S_I_ID FROM STOCK;",
    ]
    queries.extend(set_ops)

    # ===== 28. String operations =====
    string_ops = [
        "SELECT C_W_ID, C_LAST, LENGTH(C_LAST) AS name_len FROM CUSTOMER WHERE LENGTH(C_LAST) > 5;",
        "SELECT I_ID, UPPER(I_NAME) AS upper_name, LOWER(I_DATA) AS lower_data FROM ITEM;",
        "SELECT W_ID, W_NAME, W_STATE, CONCAT(W_NAME, '-', W_STATE) AS full FROM WAREHOUSE;",
        "SELECT C_ID, C_LAST, C_FIRST, SUBSTRING(C_LAST FROM 1 FOR 3) AS last3 FROM CUSTOMER;",
        "SELECT D_W_ID, D_ID, TRIM(D_NAME) AS clean_name FROM DISTRICT;",
        "SELECT C_W_ID, COUNT(DISTINCT SUBSTRING(C_LAST FROM 1 FOR 1)) AS first_letters FROM CUSTOMER GROUP BY C_W_ID;",
    ]
    queries.extend(string_ops)

    # ===== B. Aggregates on top of JOINs (every join type) =====
    # INNER JOIN + aggregate
    agg_on_inner = [
        "SELECT w.W_STATE, COUNT(DISTINCT c.C_ID) AS cust, SUM(c.C_BALANCE) AS bal, AVG(c.C_DISCOUNT) AS avg_disc FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE;",
        "SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS cust_cnt, SUM(c.C_BALANCE) AS bal_tot, AVG(c.C_BALANCE) AS bal_avg, STDDEV(c.C_BALANCE) AS bal_std FROM DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, SUM(s.S_QUANTITY) AS stock, COUNT(DISTINCT s.S_W_ID) AS in_wh FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE;",
        "SELECT c.C_W_ID, c.C_CREDIT, COUNT(o.O_ID) AS orders, SUM(o.O_OL_CNT) AS total_lines FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_W_ID, c.C_CREDIT;",
        "SELECT o.O_W_ID, o.O_D_ID, COUNT(ol.OL_NUMBER) AS lines, SUM(ol.OL_AMOUNT) AS total, AVG(ol.OL_QUANTITY) AS avg_qty FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_W_ID, o.O_D_ID;",
        "SELECT i.I_IM_ID, COUNT(*) AS sales, SUM(ol.OL_AMOUNT) AS revenue, MIN(ol.OL_AMOUNT) AS min_sale, MAX(ol.OL_AMOUNT) AS max_sale FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_IM_ID;",
        "SELECT c.C_W_ID, COUNT(*) AS payments, SUM(h.H_AMOUNT) AS total, AVG(h.H_AMOUNT) AS avg_pay, MAX(h.H_AMOUNT) AS biggest FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_W_ID;",
    ]
    queries.extend(agg_on_inner)

    # LEFT JOIN + aggregate
    agg_on_left = [
        "SELECT w.W_ID, w.W_NAME, COUNT(d.D_ID) AS districts, SUM(d.D_YTD) AS total_ytd, AVG(d.D_TAX) AS avg_tax FROM WAREHOUSE w LEFT JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID, w.W_NAME;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, COUNT(c.C_ID) AS customers, COALESCE(SUM(c.C_BALANCE), 0) AS total_bal FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID, d.D_NAME;",
        "SELECT i.I_ID, i.I_NAME, COUNT(ol.OL_NUMBER) AS times_ordered, COALESCE(SUM(ol.OL_AMOUNT), 0) AS revenue FROM ITEM i LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME;",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST, COUNT(o.O_ID) AS orders FROM CUSTOMER c LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_W_ID, c.C_ID, c.C_LAST;",
        "SELECT o.O_W_ID, o.O_ID, o.O_OL_CNT, COUNT(ol.OL_NUMBER) AS actual_lines, SUM(COALESCE(ol.OL_AMOUNT, 0)) AS total FROM OORDER o LEFT JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_W_ID, o.O_ID, o.O_OL_CNT;",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, COUNT(ol.OL_NUMBER) AS sales, COALESCE(SUM(ol.OL_QUANTITY), 0) AS ordered FROM STOCK s LEFT JOIN ORDER_LINE ol ON s.S_I_ID = ol.OL_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID GROUP BY s.S_W_ID, s.S_I_ID, s.S_QUANTITY;",
    ]
    queries.extend(agg_on_left)

    # RIGHT JOIN + aggregate
    agg_on_right = [
        "SELECT d.D_W_ID, d.D_NAME, COUNT(w.W_ID) AS warehouses FROM WAREHOUSE w RIGHT JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY d.D_W_ID, d.D_NAME;",
        "SELECT s.S_W_ID, COUNT(i.I_ID) AS items, SUM(s.S_QUANTITY) AS total_qty FROM ITEM i RIGHT JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY s.S_W_ID;",
        "SELECT o.O_W_ID, o.O_D_ID, COUNT(c.C_ID) AS cust_matched, COUNT(*) AS total_orders FROM CUSTOMER c RIGHT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY o.O_W_ID, o.O_D_ID;",
        "SELECT ol.OL_W_ID, ol.OL_I_ID, COUNT(i.I_ID) AS item_matched, SUM(ol.OL_AMOUNT) AS revenue FROM ITEM i RIGHT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY ol.OL_W_ID, ol.OL_I_ID;",
    ]
    queries.extend(agg_on_right)

    # FULL OUTER JOIN + aggregate
    agg_on_full = [
        "SELECT COALESCE(w.W_ID, d.D_W_ID) AS w_id, COUNT(w.W_ID) AS has_wh, COUNT(d.D_ID) AS districts, SUM(d.D_YTD) AS total_ytd FROM WAREHOUSE w FULL OUTER JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY COALESCE(w.W_ID, d.D_W_ID);",
        "SELECT COALESCE(i.I_ID, s.S_I_ID) AS item_id, COUNT(i.I_ID) AS has_item, COUNT(s.S_I_ID) AS has_stock, SUM(COALESCE(s.S_QUANTITY, 0)) AS total_qty FROM ITEM i FULL OUTER JOIN STOCK s ON i.I_ID = s.S_I_ID GROUP BY COALESCE(i.I_ID, s.S_I_ID);",
        "SELECT COALESCE(c.C_W_ID, h.H_C_W_ID) AS w_id, COUNT(DISTINCT c.C_ID) AS customers, COUNT(h.H_AMOUNT) AS payments, SUM(h.H_AMOUNT) AS total_paid FROM CUSTOMER c FULL OUTER JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY COALESCE(c.C_W_ID, h.H_C_W_ID);",
        "SELECT COALESCE(o.O_W_ID, ol.OL_W_ID) AS w_id, COUNT(DISTINCT o.O_ID) AS orders, COUNT(ol.OL_NUMBER) AS lines, SUM(COALESCE(ol.OL_AMOUNT, 0)) AS revenue FROM OORDER o FULL OUTER JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY COALESCE(o.O_W_ID, ol.OL_W_ID);",
    ]
    queries.extend(agg_on_full)

    # ===== C. HAVING on top of JOINs =====
    having_on_joins = [
        "SELECT w.W_ID, w.W_NAME, COUNT(d.D_ID) AS d_cnt FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID, w.W_NAME HAVING COUNT(d.D_ID) >= 5;",
        "SELECT c.C_W_ID, c.C_D_ID, COUNT(o.O_ID) AS orders FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_W_ID, c.C_D_ID HAVING COUNT(o.O_ID) > 1;",
        "SELECT i.I_ID, i.I_NAME, SUM(ol.OL_QUANTITY) AS qty FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME HAVING SUM(ol.OL_QUANTITY) > 5 AND COUNT(*) > 2;",
        "SELECT d.D_W_ID, d.D_ID, AVG(c.C_BALANCE) AS avg_bal FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID HAVING AVG(c.C_BALANCE) > 100 OR AVG(c.C_BALANCE) IS NULL;",
        "SELECT w.W_STATE, COUNT(DISTINCT c.C_ID) AS cust_cnt FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE HAVING COUNT(DISTINCT c.C_ID) > 10;",
        "SELECT o.O_W_ID, o.O_D_ID, SUM(ol.OL_AMOUNT) AS rev FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_W_ID, o.O_D_ID HAVING SUM(ol.OL_AMOUNT) > 100 AND COUNT(*) > 3;",
        "SELECT c.C_STATE, c.C_CREDIT, COUNT(*) AS n FROM CUSTOMER c JOIN WAREHOUSE w ON c.C_W_ID = w.W_ID GROUP BY c.C_STATE, c.C_CREDIT HAVING COUNT(*) > 5 AND SUM(c.C_BALANCE) > 0;",
        "SELECT COALESCE(w.W_ID, c.C_W_ID) AS w_id, COUNT(c.C_ID) AS cust FROM WAREHOUSE w FULL OUTER JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY COALESCE(w.W_ID, c.C_W_ID) HAVING COUNT(c.C_ID) > 0;",
        "SELECT d.D_NAME, COUNT(c.C_ID) AS cust FROM DISTRICT d RIGHT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_NAME HAVING COUNT(*) > 10;",
    ]
    queries.extend(having_on_joins)

    # ===== D. Window functions on top of JOINs =====
    wf_on_joins = [
        "SELECT w.W_ID, d.D_ID, d.D_YTD, RANK() OVER (PARTITION BY w.W_ID ORDER BY d.D_YTD DESC) AS rnk FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID;",
        "SELECT c.C_W_ID, c.C_ID, c.C_BALANCE, ROW_NUMBER() OVER (PARTITION BY c.C_W_ID ORDER BY c.C_BALANCE DESC) AS rn, o.O_ID FROM CUSTOMER c LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, s.S_W_ID, s.S_QUANTITY, DENSE_RANK() OVER (PARTITION BY s.S_W_ID ORDER BY s.S_QUANTITY DESC) AS stock_rank FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;",
        "SELECT o.O_W_ID, o.O_ID, ol.OL_NUMBER, ol.OL_AMOUNT, SUM(ol.OL_AMOUNT) OVER (PARTITION BY o.O_W_ID, o.O_ID) AS order_total FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID;",
        "SELECT c.C_W_ID, c.C_ID, c.C_BALANCE, h.H_AMOUNT, AVG(h.H_AMOUNT) OVER (PARTITION BY c.C_W_ID) AS w_avg_pay FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID;",
        "SELECT d.D_W_ID, d.D_ID, d.D_YTD, c.C_BALANCE, c.C_BALANCE / NULLIF(SUM(c.C_BALANCE) OVER (PARTITION BY d.D_W_ID, d.D_ID), 0) AS pct FROM DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID;",
        "SELECT w.W_ID, d.D_ID, d.D_YTD, d.D_YTD - LAG(d.D_YTD) OVER (PARTITION BY w.W_ID ORDER BY d.D_ID) AS diff_prev FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID;",
        "SELECT i.I_ID, i.I_NAME, ol.OL_W_ID, ol.OL_AMOUNT, NTILE(4) OVER (PARTITION BY i.I_ID ORDER BY ol.OL_AMOUNT) AS amt_quartile FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID;",
        "SELECT o.O_W_ID, o.O_ID, ol.OL_NUMBER, ol.OL_AMOUNT, PERCENT_RANK() OVER (PARTITION BY o.O_W_ID ORDER BY ol.OL_AMOUNT) AS prank FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID;",
        "SELECT c.C_W_ID, c.C_ID, o.O_ID, o.O_ENTRY_D, ROW_NUMBER() OVER (PARTITION BY c.C_W_ID, c.C_ID ORDER BY o.O_ENTRY_D DESC) AS order_rn FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID;",
        "SELECT w.W_ID, w.W_NAME, c.C_ID, c.C_BALANCE, FIRST_VALUE(c.C_ID) OVER (PARTITION BY w.W_ID ORDER BY c.C_BALANCE DESC) AS top_cust FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID;",
        "SELECT COALESCE(o.O_W_ID, no.NO_W_ID) AS w_id, o.O_ID, ROW_NUMBER() OVER (PARTITION BY COALESCE(o.O_W_ID, no.NO_W_ID) ORDER BY o.O_ID) AS rn FROM OORDER o FULL OUTER JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID;",
    ]
    queries.extend(wf_on_joins)

    # ===== E. JOINs on top of AGGREGATES (subquery or CTE provides aggregate) =====
    join_on_agg = [
        "SELECT w.W_ID, w.W_NAME, cs.avg_bal FROM WAREHOUSE w JOIN (SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER GROUP BY C_W_ID) cs ON w.W_ID = cs.C_W_ID;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, COALESCE(cs.cnt, 0) AS cust_cnt FROM DISTRICT d LEFT JOIN (SELECT C_W_ID, C_D_ID, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, C_D_ID) cs ON d.D_W_ID = cs.C_W_ID AND d.D_ID = cs.C_D_ID;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, COALESCE(ir.rev, 0) AS revenue FROM ITEM i LEFT JOIN (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_I_ID) ir ON i.I_ID = ir.OL_I_ID;",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST, COALESCE(pc.orders, 0) AS orders FROM CUSTOMER c LEFT JOIN (SELECT O_C_ID, O_W_ID, COUNT(*) AS orders FROM OORDER GROUP BY O_C_ID, O_W_ID) pc ON c.C_ID = pc.O_C_ID AND c.C_W_ID = pc.O_W_ID;",
        "SELECT o.O_W_ID, o.O_ID, o.O_OL_CNT, ot.lines, ot.total FROM OORDER o JOIN (SELECT OL_W_ID, OL_O_ID, COUNT(*) AS lines, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) ot ON o.O_W_ID = ot.OL_W_ID AND o.O_ID = ot.OL_O_ID;",
        "SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY, COALESCE(sales.qty, 0) AS sold_qty FROM STOCK s LEFT JOIN (SELECT OL_SUPPLY_W_ID, OL_I_ID, SUM(OL_QUANTITY) AS qty FROM ORDER_LINE GROUP BY OL_SUPPLY_W_ID, OL_I_ID) sales ON s.S_W_ID = sales.OL_SUPPLY_W_ID AND s.S_I_ID = sales.OL_I_ID;",
        "WITH cust_agg AS (SELECT C_W_ID, C_D_ID, COUNT(*) AS cnt, SUM(C_BALANCE) AS bal FROM CUSTOMER GROUP BY C_W_ID, C_D_ID), order_agg AS (SELECT O_W_ID, O_D_ID, COUNT(*) AS orders FROM OORDER GROUP BY O_W_ID, O_D_ID) SELECT d.D_W_ID, d.D_ID, d.D_NAME, ca.cnt, ca.bal, oa.orders FROM DISTRICT d LEFT JOIN cust_agg ca ON d.D_W_ID = ca.C_W_ID AND d.D_ID = ca.C_D_ID LEFT JOIN order_agg oa ON d.D_W_ID = oa.O_W_ID AND d.D_ID = oa.O_D_ID;",
        "SELECT w.W_ID, w.W_NAME, stats.total_rev FROM WAREHOUSE w JOIN (SELECT ol.OL_W_ID, SUM(ol.OL_AMOUNT) AS total_rev FROM ORDER_LINE ol GROUP BY ol.OL_W_ID HAVING SUM(ol.OL_AMOUNT) > 100) stats ON w.W_ID = stats.OL_W_ID;",
        "WITH top_items AS (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_I_ID ORDER BY rev DESC LIMIT 20) SELECT i.I_ID, i.I_NAME, i.I_PRICE, ti.rev FROM ITEM i JOIN top_items ti ON i.I_ID = ti.OL_I_ID;",
    ]
    queries.extend(join_on_agg)

    # ===== F. HAVING on top of JOIN + aggregate (4 operators stacked) =====
    stacked_4 = [
        "SELECT w.W_STATE, c.C_CREDIT, COUNT(*) AS cnt, AVG(c.C_BALANCE) AS avg_bal FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE, c.C_CREDIT HAVING COUNT(*) > 10 AND AVG(c.C_BALANCE) > 0;",
        "SELECT d.D_W_ID, d.D_ID, COUNT(o.O_ID) AS orders, SUM(o.O_OL_CNT) AS lines FROM DISTRICT d JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID GROUP BY d.D_W_ID, d.D_ID HAVING COUNT(o.O_ID) > 1 AND SUM(o.O_OL_CNT) > 5;",
        "SELECT i.I_IM_ID, COUNT(DISTINCT i.I_ID) AS items, SUM(ol.OL_AMOUNT) AS rev FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_IM_ID HAVING COUNT(DISTINCT i.I_ID) > 2 AND SUM(ol.OL_AMOUNT) > 50;",
        "SELECT c.C_STATE, COUNT(*) AS payments, SUM(h.H_AMOUNT) AS total FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_STATE HAVING SUM(h.H_AMOUNT) > 100 OR COUNT(*) > 3;",
    ]
    queries.extend(stacked_4)

    # ===== G. WINDOW on top of AGGREGATE (CTE or subquery with aggregate, then window) =====
    wf_on_agg = [
        "WITH w_totals AS (SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY C_W_ID) SELECT C_W_ID, tot, RANK() OVER (ORDER BY tot DESC) AS rnk, tot / SUM(tot) OVER () AS pct FROM w_totals;",
        "WITH d_agg AS (SELECT D_W_ID, D_ID, AVG(D_YTD) AS avg_ytd, COUNT(*) AS n FROM DISTRICT GROUP BY D_W_ID, D_ID) SELECT D_W_ID, D_ID, avg_ytd, ROW_NUMBER() OVER (PARTITION BY D_W_ID ORDER BY avg_ytd DESC) AS rn FROM d_agg;",
        "WITH item_rev AS (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev, COUNT(*) AS sales FROM ORDER_LINE GROUP BY OL_I_ID) SELECT OL_I_ID, rev, sales, NTILE(10) OVER (ORDER BY rev) AS decile FROM item_rev;",
        "WITH per_state AS (SELECT C_STATE, COUNT(*) AS cnt, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER GROUP BY C_STATE) SELECT C_STATE, cnt, avg_bal, DENSE_RANK() OVER (ORDER BY cnt DESC) AS state_rank, LAG(cnt) OVER (ORDER BY cnt DESC) AS prev_cnt FROM per_state;",
        "SELECT w_id, total, total / SUM(total) OVER () AS pct_of_all FROM (SELECT OL_W_ID AS w_id, SUM(OL_AMOUNT) AS total FROM ORDER_LINE GROUP BY OL_W_ID) sub;",
    ]
    queries.extend(wf_on_agg)

    # ===== H. Deeply nested (JOIN + AGG + WINDOW + HAVING) =====
    deep = [
        "WITH agg AS (SELECT w.W_ID, COUNT(c.C_ID) AS c_cnt, SUM(c.C_BALANCE) AS bal FROM WAREHOUSE w LEFT JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_ID HAVING COUNT(c.C_ID) > 0) SELECT W_ID, c_cnt, bal, ROW_NUMBER() OVER (ORDER BY bal DESC) AS rn FROM agg;",
        "WITH joined AS (SELECT o.O_W_ID, o.O_D_ID, o.O_ID, SUM(ol.OL_AMOUNT) AS tot FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_W_ID, o.O_D_ID, o.O_ID HAVING SUM(ol.OL_AMOUNT) > 10) SELECT O_W_ID, O_D_ID, O_ID, tot, RANK() OVER (PARTITION BY O_W_ID, O_D_ID ORDER BY tot DESC) AS rnk FROM joined;",
        "WITH cust_ord AS (SELECT c.C_W_ID, c.C_ID, c.C_LAST, COUNT(o.O_ID) AS n_ord, COALESCE(SUM(o.O_OL_CNT), 0) AS lines FROM CUSTOMER c LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_W_ID, c.C_ID, c.C_LAST) SELECT C_W_ID, C_ID, C_LAST, n_ord, lines, PERCENT_RANK() OVER (PARTITION BY C_W_ID ORDER BY lines) AS pr FROM cust_ord;",
        "WITH item_stats AS (SELECT i.I_IM_ID, COUNT(DISTINCT i.I_ID) AS items, SUM(ol.OL_AMOUNT) AS rev FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_IM_ID HAVING SUM(ol.OL_AMOUNT) > 0) SELECT I_IM_ID, items, rev, rev / SUM(rev) OVER () AS pct FROM item_stats;",
        "WITH t AS (SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS n, AVG(c.C_BALANCE) AS avg_b FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID HAVING COUNT(c.C_ID) > 0) SELECT D_W_ID, D_ID, n, avg_b, LAG(avg_b) OVER (PARTITION BY D_W_ID ORDER BY D_ID) AS prev_avg FROM t;",
    ]
    queries.extend(deep)

    # ===== I. DATA TYPES - VARCHAR/CHAR operations =====
    varchar_ops = [
        "SELECT C_W_ID, C_LAST FROM CUSTOMER WHERE C_LAST LIKE 'A%';",
        "SELECT C_W_ID, C_LAST FROM CUSTOMER WHERE C_LAST ILIKE '%son%';",
        "SELECT C_ID, UPPER(C_LAST) || ', ' || UPPER(C_FIRST) AS full_name FROM CUSTOMER;",
        "SELECT I_ID, LENGTH(I_NAME) AS name_len, LENGTH(I_DATA) AS data_len FROM ITEM;",
        "SELECT W_ID, W_NAME, LPAD(W_NAME, 20, '*') AS padded, RPAD(W_NAME, 20, '-') AS rpadded FROM WAREHOUSE;",
        "SELECT C_ID, REPLACE(C_LAST, 'a', '@') AS replaced, REVERSE(C_LAST) AS reversed FROM CUSTOMER;",
        "SELECT D_W_ID, D_ID, POSITION('a' IN D_NAME) AS pos_a FROM DISTRICT;",
        "SELECT I_ID, LEFT(I_NAME, 5) AS first5, RIGHT(I_NAME, 5) AS last5 FROM ITEM;",
        "SELECT C_W_ID, STARTS_WITH(C_LAST, 'B') AS starts_b, ENDS_WITH(C_FIRST, 'a') AS ends_a FROM CUSTOMER WHERE C_LAST IS NOT NULL AND C_FIRST IS NOT NULL;",
        "SELECT W_ID, TRIM(W_STREET_1) AS addr, LENGTH(TRIM(W_STREET_1)) AS addr_len FROM WAREHOUSE;",
        "SELECT C_W_ID, C_STATE, UPPER(C_STATE) AS state_upper, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, C_STATE;",
        "SELECT I_ID, I_NAME, REGEXP_MATCHES(I_NAME, '[A-Z]+') AS matches FROM ITEM LIMIT 50;",
        "SELECT C_W_ID, STRING_AGG(C_LAST, ',' ORDER BY C_LAST) AS all_names FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT I_ID, I_NAME, CONCAT_WS('|', I_NAME, I_DATA) AS joined FROM ITEM;",
    ]
    queries.extend(varchar_ops)

    # ===== J. DATA TYPES - TIMESTAMP/DATE operations =====
    date_ops = [
        "SELECT O_ID, O_W_ID, O_ENTRY_D, O_ENTRY_D + INTERVAL '1 day' AS plus_day, O_ENTRY_D - INTERVAL '1 hour' AS minus_hour FROM OORDER;",
        "SELECT H_W_ID, H_C_ID, H_DATE, EXTRACT(EPOCH FROM H_DATE) AS epoch_sec, EXTRACT(QUARTER FROM H_DATE) AS quarter FROM HISTORY WHERE H_DATE IS NOT NULL;",
        "SELECT O_W_ID, O_ID, DATE_PART('year', O_ENTRY_D) AS yr, DATE_PART('month', O_ENTRY_D) AS mo, DATE_PART('day', O_ENTRY_D) AS day FROM OORDER WHERE O_ENTRY_D IS NOT NULL;",
        "SELECT OL_W_ID, OL_O_ID, AGE(OL_DELIVERY_D, CAST('2020-01-01' AS TIMESTAMP)) AS age_since FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;",
        "SELECT O_W_ID, COUNT(*) FROM OORDER WHERE O_ENTRY_D >= CAST('2020-01-01' AS TIMESTAMP) GROUP BY O_W_ID;",
        "SELECT H_W_ID, DATE_TRUNC('quarter', H_DATE) AS quarter, SUM(H_AMOUNT) AS total FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY H_W_ID, DATE_TRUNC('quarter', H_DATE);",
        "SELECT DATE_TRUNC('hour', O_ENTRY_D) AS hour_bucket, COUNT(*) AS orders FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY DATE_TRUNC('hour', O_ENTRY_D);",
        "SELECT C_W_ID, C_ID, C_SINCE, DATE_DIFF('day', C_SINCE, CURRENT_TIMESTAMP) AS days_since_join FROM CUSTOMER WHERE C_SINCE IS NOT NULL;",
        "SELECT H_W_ID, EXTRACT(HOUR FROM H_DATE) AS hour_of_day, COUNT(*) AS cnt FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY H_W_ID, EXTRACT(HOUR FROM H_DATE);",
        "SELECT OL_W_ID, MIN(OL_DELIVERY_D) AS first_delivery, MAX(OL_DELIVERY_D) AS last_delivery, COUNT(OL_DELIVERY_D) AS delivered FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT O_W_ID, DAYOFWEEK(O_ENTRY_D) AS dow, COUNT(*) AS orders FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY O_W_ID, DAYOFWEEK(O_ENTRY_D);",
    ]
    queries.extend(date_ops)

    # ===== K. DATA TYPES - Numeric (DECIMAL/INT/FLOAT) =====
    numeric_ops = [
        "SELECT W_ID, W_YTD, ABS(W_YTD) AS abs_ytd, SIGN(W_YTD) AS sgn, ROUND(W_YTD, 2) AS rnd FROM WAREHOUSE;",
        "SELECT I_ID, I_PRICE, POWER(I_PRICE, 2) AS squared, SQRT(I_PRICE) AS sqrt_price, LN(I_PRICE + 1) AS ln_price, EXP(0.1) AS exp_val FROM ITEM WHERE I_PRICE > 0;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, S_QUANTITY % 10 AS mod_10, S_QUANTITY / 2 AS halved, S_QUANTITY * S_QUANTITY AS squared_qty FROM STOCK;",
        "SELECT C_W_ID, C_ID, C_BALANCE, FLOOR(C_BALANCE) AS floor_b, CEIL(C_BALANCE) AS ceil_b, TRUNC(C_BALANCE, 0) AS trunc_b FROM CUSTOMER;",
        "SELECT OL_W_ID, OL_O_ID, OL_AMOUNT, ROUND(OL_AMOUNT, 0) AS rnd_0, ROUND(OL_AMOUNT, 1) AS rnd_1, ROUND(OL_AMOUNT, 3) AS rnd_3 FROM ORDER_LINE;",
        "SELECT D_W_ID, D_ID, D_TAX, D_TAX * 100 AS tax_pct, ROUND(D_TAX * 100, 2) AS tax_pct_rnd FROM DISTRICT;",
        "SELECT H_W_ID, H_AMOUNT, GREATEST(H_AMOUNT, 0) AS non_neg, LEAST(H_AMOUNT, 1000) AS capped FROM HISTORY;",
        "SELECT W_ID, W_YTD, W_TAX, W_YTD * W_TAX AS tax_amt, W_YTD / NULLIF(W_TAX, 0) AS inv_tax FROM WAREHOUSE;",
        "SELECT C_W_ID, SUM(C_BALANCE) AS tot_bal, AVG(C_BALANCE) AS avg_bal, MEDIAN(C_BALANCE) AS med_bal FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT OL_W_ID, MIN(OL_AMOUNT) AS min_amt, MAX(OL_AMOUNT) AS max_amt, MAX(OL_AMOUNT) - MIN(OL_AMOUNT) AS range_amt FROM ORDER_LINE GROUP BY OL_W_ID;",
    ]
    queries.extend(numeric_ops)

    # ===== L. More table function variety =====
    more_tf = [
        "SELECT * FROM (VALUES (1, 'one'), (2, 'two'), (3, 'three')) AS t(n, label);",
        "SELECT t.n, t.label, COUNT(c.C_ID) AS cust FROM (VALUES (1, 'small'), (2, 'medium'), (3, 'large')) AS t(n, label) LEFT JOIN CUSTOMER c ON c.C_W_ID = t.n GROUP BY t.n, t.label;",
        "SELECT unnest(ARRAY[10, 20, 30, 40, 50]) AS threshold;",
        "SELECT threshold, COUNT(*) AS above_cnt FROM (SELECT unnest([10, 50, 100, 500, 1000]) AS threshold) t JOIN STOCK s ON s.S_QUANTITY >= t.threshold GROUP BY threshold;",
        "SELECT generate_series AS n, n * n AS n_squared FROM generate_series(1, 10);",
        "SELECT range AS n, n::VARCHAR AS n_str FROM range(100, 110);",
        "SELECT w_id, slot FROM WAREHOUSE w CROSS JOIN LATERAL (SELECT generate_series AS slot FROM generate_series(1, w.W_ID)) t(slot);",
        "SELECT COUNT(*) AS n FROM range(1, 10000) WHERE range % 7 = 0;",
        "SELECT s.S_W_ID, s.S_I_ID, dist.n AS district_slot FROM STOCK s, generate_series(1, 10) dist(n) WHERE s.S_W_ID * 10 + dist.n <= 100;",
        "SELECT unnest(list_value(1, 2, 3, 4, 5)) AS n;",
        "WITH numbers AS (SELECT generate_series AS n FROM generate_series(1, 20)) SELECT n, (SELECT W_NAME FROM WAREHOUSE WHERE W_ID = n) AS wh_name FROM numbers;",
    ]
    queries.extend(more_tf)

    # ===== M. IF/IFNULL/GREATEST/LEAST =====
    cond_fns = [
        "SELECT C_W_ID, C_ID, IFNULL(C_MIDDLE, 'NA') AS mid FROM CUSTOMER;",
        "SELECT O_ID, O_W_ID, IFNULL(O_CARRIER_ID, 0) AS carrier FROM OORDER;",
        "SELECT W_ID, GREATEST(W_YTD, 0) AS non_neg_ytd, LEAST(W_YTD, 1000000) AS capped_ytd FROM WAREHOUSE;",
        "SELECT C_W_ID, C_ID, IF(C_BALANCE > 0, 'positive', 'negative') AS sign_cat FROM CUSTOMER;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, IF(S_QUANTITY > 50, S_QUANTITY, NULL) AS high_only FROM STOCK;",
        "SELECT I_ID, I_PRICE, GREATEST(I_PRICE, 10.0) AS floor_10, LEAST(I_PRICE, 100.0) AS ceil_100 FROM ITEM;",
        "SELECT H_W_ID, H_C_ID, H_AMOUNT, CASE WHEN H_AMOUNT IS NULL THEN 0 ELSE H_AMOUNT END AS safe_amt FROM HISTORY;",
        "SELECT D_W_ID, D_ID, COALESCE(D_NAME, 'UNKNOWN'), COALESCE(D_STREET_1, D_STREET_2, 'NO ADDR') AS addr FROM DISTRICT;",
    ]
    queries.extend(cond_fns)

    # ===== N. BOOLEAN aggregations on top of joins =====
    bool_on_joins = [
        "SELECT w.W_ID, BOOL_AND(d.D_YTD >= 0) AS all_non_neg, BOOL_OR(d.D_TAX > 0.1) AS any_high_tax FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID;",
        "SELECT c.C_W_ID, BOOL_AND(o.O_ALL_LOCAL = 1) AS all_local, BOOL_OR(o.O_OL_CNT > 10) AS any_big FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID GROUP BY c.C_W_ID;",
        "SELECT s.S_W_ID, BOOL_AND(ol.OL_AMOUNT > 0) AS all_paid FROM STOCK s JOIN ORDER_LINE ol ON s.S_I_ID = ol.OL_I_ID AND s.S_W_ID = ol.OL_SUPPLY_W_ID GROUP BY s.S_W_ID;",
    ]
    queries.extend(bool_on_joins)

    # ===== O. Multiple aggregates + HAVING + JOIN =====
    multi_metric = [
        "SELECT w.W_STATE, COUNT(c.C_ID) AS n, MIN(c.C_BALANCE) AS min_b, MAX(c.C_BALANCE) AS max_b, AVG(c.C_BALANCE) AS avg_b, STDDEV(c.C_BALANCE) AS std_b FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE HAVING COUNT(c.C_ID) >= 3;",
        "SELECT d.D_W_ID, d.D_ID, COUNT(*) AS orders, SUM(o.O_OL_CNT) AS tot_lines, AVG(o.O_OL_CNT) AS avg_lines, MAX(o.O_OL_CNT) AS max_lines FROM DISTRICT d JOIN OORDER o ON d.D_W_ID = o.O_W_ID AND d.D_ID = o.O_D_ID GROUP BY d.D_W_ID, d.D_ID HAVING AVG(o.O_OL_CNT) > 0;",
        "SELECT i.I_IM_ID, COUNT(DISTINCT i.I_ID) AS unique_items, SUM(ol.OL_QUANTITY) AS tot_qty, AVG(ol.OL_AMOUNT) AS avg_amt, VARIANCE(ol.OL_AMOUNT) AS amt_var FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_IM_ID;",
        "SELECT c.C_CREDIT, c.C_W_ID, COUNT(*) AS n, SUM(h.H_AMOUNT) AS tot_paid, AVG(h.H_AMOUNT) AS avg_paid, MAX(h.H_AMOUNT) AS max_paid FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID GROUP BY c.C_CREDIT, c.C_W_ID HAVING COUNT(*) > 1;",
    ]
    queries.extend(multi_metric)

    # ===== P. UNION on top of aggregates =====
    union_agg = [
        "SELECT 'GC' AS credit, C_W_ID, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID UNION ALL SELECT 'BC', C_W_ID, AVG(C_BALANCE) FROM CUSTOMER WHERE C_CREDIT = 'BC' GROUP BY C_W_ID;",
        "SELECT 'in_stock' AS status, S_W_ID, COUNT(*) AS cnt FROM STOCK WHERE S_QUANTITY > 0 GROUP BY S_W_ID UNION ALL SELECT 'out_of_stock', S_W_ID, COUNT(*) FROM STOCK WHERE S_QUANTITY = 0 GROUP BY S_W_ID;",
        "SELECT 'delivered' AS status, OL_W_ID, SUM(OL_AMOUNT) AS tot FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL GROUP BY OL_W_ID UNION ALL SELECT 'pending', OL_W_ID, SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_DELIVERY_D IS NULL GROUP BY OL_W_ID;",
    ]
    queries.extend(union_agg)

    # ===== Q. DISTINCT on top of joins =====
    distinct_on_joins = [
        "SELECT DISTINCT w.W_STATE, d.D_NAME FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID;",
        "SELECT DISTINCT c.C_CREDIT, o.O_ALL_LOCAL FROM CUSTOMER c JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID;",
        "SELECT DISTINCT i.I_IM_ID, s.S_W_ID FROM ITEM i JOIN STOCK s ON i.I_ID = s.S_I_ID;",
        "SELECT COUNT(DISTINCT c.C_STATE) AS states, COUNT(DISTINCT c.C_W_ID) AS warehouses FROM CUSTOMER c JOIN WAREHOUSE w ON c.C_W_ID = w.W_ID;",
        "SELECT w.W_ID, COUNT(DISTINCT d.D_NAME) AS unique_names FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID GROUP BY w.W_ID;",
    ]
    queries.extend(distinct_on_joins)

    # ===== R. Subqueries in FROM clause (derived tables) composed with JOIN =====
    derived_table_joins = [
        "SELECT w.W_ID, w.W_NAME, sub.avg_bal FROM WAREHOUSE w JOIN (SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER WHERE C_CREDIT = 'GC' GROUP BY C_W_ID) sub ON w.W_ID = sub.C_W_ID;",
        "SELECT i.I_ID, i.I_NAME, hot.rev FROM ITEM i JOIN (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_I_ID HAVING SUM(OL_AMOUNT) > 50) hot ON i.I_ID = hot.OL_I_ID;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, cust_count.cnt FROM DISTRICT d JOIN (SELECT C_W_ID, C_D_ID, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_W_ID, C_D_ID) cust_count ON d.D_W_ID = cust_count.C_W_ID AND d.D_ID = cust_count.C_D_ID;",
        "SELECT o.O_W_ID, o.O_ID, o.O_OL_CNT, line_tot.tot FROM OORDER o JOIN (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS tot FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) line_tot ON o.O_W_ID = line_tot.OL_W_ID AND o.O_ID = line_tot.OL_O_ID WHERE line_tot.tot > 0;",
        "SELECT top5.c_id, top5.bal, ord.cnt FROM (SELECT C_ID AS c_id, C_BALANCE AS bal, C_W_ID FROM CUSTOMER WHERE C_BALANCE > 3000) top5 LEFT JOIN (SELECT O_C_ID, O_W_ID, COUNT(*) AS cnt FROM OORDER GROUP BY O_C_ID, O_W_ID) ord ON top5.c_id = ord.O_C_ID AND top5.C_W_ID = ord.O_W_ID;",
    ]
    queries.extend(derived_table_joins)

    # ===== S. More LATERAL with aggregates =====
    lateral_agg = [
        "SELECT w.W_ID, w.W_NAME, lat.n, lat.total FROM WAREHOUSE w JOIN LATERAL (SELECT COUNT(*) AS n, SUM(c.C_BALANCE) AS total FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID) lat ON TRUE;",
        "SELECT d.D_W_ID, d.D_ID, lat.avg_b, lat.std_b, lat.max_b FROM DISTRICT d JOIN LATERAL (SELECT AVG(C_BALANCE) AS avg_b, STDDEV(C_BALANCE) AS std_b, MAX(C_BALANCE) AS max_b FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) lat ON TRUE;",
        "SELECT c.C_W_ID, c.C_ID, lat.order_count, lat.line_sum FROM CUSTOMER c JOIN LATERAL (SELECT COUNT(DISTINCT o.O_ID) AS order_count, SUM(ol.OL_AMOUNT) AS line_sum FROM OORDER o LEFT JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID WHERE o.O_C_ID = c.C_ID AND o.O_W_ID = c.C_W_ID) lat ON TRUE WHERE lat.order_count > 0;",
        "SELECT w.W_ID, lat.cust_count, lat.dist_count, lat.order_count FROM WAREHOUSE w, LATERAL (SELECT (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = w.W_ID) AS cust_count, (SELECT COUNT(*) FROM DISTRICT WHERE D_W_ID = w.W_ID) AS dist_count, (SELECT COUNT(*) FROM OORDER WHERE O_W_ID = w.W_ID) AS order_count) lat;",
    ]
    queries.extend(lateral_agg)

    return queries


def composite_extra_queries() -> list[str]:
    """Additional systematic composite-operator patterns: agg-on-join, wf-on-join,
    join-on-agg across every FK pair, plus more data-type/function variety."""
    queries = []

    # ===== Aggregates on every FK join pair × every join type =====
    for left, right, cond, lcols, rcols in FK_JOIN_PAIRS:
        la = TABLE_ALIAS[left]
        ra = TABLE_ALIAS[right]
        gcol = f"{la}.{lcols[0]}"
        right_num = None
        for c in rcols:
            if not c.endswith("_D") and "DATE" not in c and "SINCE" not in c and c not in ("O_ID", "NO_O_ID"):
                right_num = f"{ra}.{c}"
                break
        if right_num is None:
            right_num = "1"
        for jtype in ["JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN"]:
            if jtype == "FULL OUTER JOIN":
                gexpr = f"COALESCE({la}.{lcols[0]}, {ra}.{rcols[0]})"
                queries.append(
                    f"SELECT {gexpr} AS gk, COUNT(*) AS n, SUM(COALESCE({right_num}, 0)) AS tot FROM {left} {la} {jtype} {right} {ra} ON {cond} GROUP BY {gexpr};"
                )
            else:
                queries.append(
                    f"SELECT {gcol}, COUNT(*) AS n, SUM(COALESCE({right_num}, 0)) AS tot, AVG({right_num}) AS avg_val FROM {left} {la} {jtype} {right} {ra} ON {cond} GROUP BY {gcol};"
                )
                queries.append(
                    f"SELECT {gcol}, MIN({right_num}) AS mn, MAX({right_num}) AS mx, COUNT(DISTINCT {right_num}) AS uniq FROM {left} {la} {jtype} {right} {ra} ON {cond} GROUP BY {gcol};"
                )
                queries.append(
                    f"SELECT {gcol}, COUNT(*) AS n FROM {left} {la} {jtype} {right} {ra} ON {cond} GROUP BY {gcol} HAVING COUNT(*) > 0;"
                )

    # ===== Window functions on every FK join pair =====
    for left, right, cond, lcols, rcols in FK_JOIN_PAIRS:
        la = TABLE_ALIAS[left]
        ra = TABLE_ALIAS[right]
        pcol = f"{la}.{lcols[0]}"
        right_num = None
        for c in rcols:
            if not c.endswith("_D") and "DATE" not in c and "SINCE" not in c and c not in ("O_ID", "NO_O_ID"):
                right_num = f"{ra}.{c}"
                break
        if right_num is None:
            continue
        queries.append(
            f"SELECT {pcol}, {right_num}, ROW_NUMBER() OVER (PARTITION BY {pcol} ORDER BY {right_num} DESC) AS rn FROM {left} {la} JOIN {right} {ra} ON {cond};"
        )
        queries.append(
            f"SELECT {pcol}, {right_num}, RANK() OVER (PARTITION BY {pcol} ORDER BY {right_num}) AS rnk FROM {left} {la} JOIN {right} {ra} ON {cond};"
        )
        queries.append(
            f"SELECT {pcol}, {right_num}, SUM({right_num}) OVER (PARTITION BY {pcol}) AS part_total FROM {left} {la} JOIN {right} {ra} ON {cond};"
        )
        queries.append(
            f"SELECT {pcol}, {right_num}, AVG({right_num}) OVER (PARTITION BY {pcol}) AS part_avg, {right_num} - AVG({right_num}) OVER (PARTITION BY {pcol}) AS diff FROM {left} {la} JOIN {right} {ra} ON {cond};"
        )
        queries.append(
            f"SELECT {pcol}, {right_num}, DENSE_RANK() OVER (PARTITION BY {pcol} ORDER BY {right_num} DESC) AS dr, NTILE(4) OVER (PARTITION BY {pcol} ORDER BY {right_num}) AS q FROM {left} {la} JOIN {right} {ra} ON {cond};"
        )
        queries.append(
            f"SELECT {pcol}, {right_num}, LAG({right_num}) OVER (PARTITION BY {pcol} ORDER BY {right_num}) AS prv, LEAD({right_num}) OVER (PARTITION BY {pcol} ORDER BY {right_num}) AS nxt FROM {left} {la} JOIN {right} {ra} ON {cond};"
        )

    # ===== JOIN on top of aggregate (derived table) =====
    for left, right, cond, lcols, rcols in FK_JOIN_PAIRS:
        la = TABLE_ALIAS[left]
        ra = TABLE_ALIAS[right]
        right_num = None
        for c in rcols:
            if not c.endswith("_D") and "DATE" not in c and "SINCE" not in c:
                right_num = c
                break
        if right_num is None:
            continue
        left_key = lcols[0]
        match = re.findall(rf'{ra}\.(\w+)', cond)
        if match:
            right_key = match[0]
            queries.append(
                f"SELECT {la}.{left_key}, COALESCE(agg.tot, 0) AS tot FROM {left} {la} LEFT JOIN (SELECT {right_key}, SUM({right_num}) AS tot FROM {right} GROUP BY {right_key}) agg ON {la}.{left_key} = agg.{right_key};"
            )
            queries.append(
                f"SELECT {la}.{left_key}, COALESCE(agg.n, 0) AS n FROM {left} {la} LEFT JOIN (SELECT {right_key}, COUNT(*) AS n FROM {right} GROUP BY {right_key} HAVING COUNT(*) > 0) agg ON {la}.{left_key} = agg.{right_key};"
            )
            queries.append(
                f"SELECT {la}.{left_key}, agg.avg_val FROM {left} {la} JOIN (SELECT {right_key}, AVG({right_num}) AS avg_val FROM {right} GROUP BY {right_key}) agg ON {la}.{left_key} = agg.{right_key};"
            )

    # ===== More diverse data type / column function queries =====
    extra_funcs = [
        # String
        "SELECT C_W_ID, CHAR_LENGTH(C_LAST) AS clen, CHARACTER_LENGTH(C_FIRST) AS flen FROM CUSTOMER WHERE C_LAST IS NOT NULL;",
        "SELECT I_ID, I_NAME, MD5(I_NAME) AS hash, HASH(I_ID) AS id_hash FROM ITEM;",
        "SELECT C_W_ID, REGEXP_REPLACE(C_LAST, '[^a-zA-Z]', '', 'g') AS alpha_only FROM CUSTOMER;",
        "SELECT I_ID, SPLIT_PART(I_NAME, ' ', 1) AS first_word FROM ITEM;",
        "SELECT C_W_ID, C_ID, FORMAT('Customer {} in warehouse {}', C_ID, C_W_ID) AS label FROM CUSTOMER;",
        "SELECT I_ID, I_NAME, REPEAT('*', LEAST(CAST(I_PRICE AS INTEGER), 10)) AS bars FROM ITEM;",
        # Date/Time
        "SELECT O_W_ID, MIN(O_ENTRY_D) AS earliest, MAX(O_ENTRY_D) AS latest, DATE_DIFF('day', MIN(O_ENTRY_D), MAX(O_ENTRY_D)) AS span_days FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY O_W_ID;",
        "SELECT H_W_ID, EXTRACT(ISODOW FROM H_DATE) AS iso_dow, COUNT(*) AS n FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY H_W_ID, EXTRACT(ISODOW FROM H_DATE);",
        "SELECT O_W_ID, O_ID, CAST(O_ENTRY_D AS DATE) AS entry_date, CAST(O_ENTRY_D AS TIMESTAMP) AS entry_ts FROM OORDER WHERE O_ENTRY_D IS NOT NULL;",
        "SELECT OL_W_ID, OL_O_ID, STRFTIME(OL_DELIVERY_D, '%Y-%m') AS month_str FROM ORDER_LINE WHERE OL_DELIVERY_D IS NOT NULL;",
        "SELECT O_W_ID, COUNT(*) FROM OORDER WHERE O_ENTRY_D < CAST('2025-01-01' AS TIMESTAMP) GROUP BY O_W_ID;",
        # Numeric
        "SELECT W_ID, LN(CAST(W_YTD AS DOUBLE) + 1) AS ln_ytd, LOG10(CAST(W_YTD AS DOUBLE) + 1) AS log10_ytd FROM WAREHOUSE WHERE W_YTD >= 0;",
        "SELECT C_W_ID, CORR(C_BALANCE, CAST(C_YTD_PAYMENT AS DOUBLE)) AS corr_bal_pay FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT C_W_ID, COVAR_POP(C_BALANCE, CAST(C_YTD_PAYMENT AS DOUBLE)) AS cov, COVAR_SAMP(C_BALANCE, CAST(C_YTD_PAYMENT AS DOUBLE)) AS cov_samp FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT OL_W_ID, SUM(OL_AMOUNT * OL_QUANTITY) AS weighted_sum, ROUND(SUM(OL_AMOUNT * OL_QUANTITY) / NULLIF(SUM(OL_QUANTITY), 0), 2) AS weighted_avg FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT I_ID, I_PRICE, CAST(I_PRICE AS TINYINT) AS tiny, CAST(I_PRICE AS SMALLINT) AS small, CAST(I_PRICE AS BIGINT) AS big, CAST(I_PRICE AS HUGEINT) AS huge FROM ITEM;",
        "SELECT W_ID, BIT_COUNT(W_ID) AS bits FROM WAREHOUSE;",
        # Aggregates
        "SELECT C_W_ID, APPROX_QUANTILE(C_BALANCE, 0.5) AS median_approx, APPROX_QUANTILE(C_BALANCE, 0.9) AS p90 FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT C_W_ID, MODE() WITHIN GROUP (ORDER BY C_CREDIT) AS mode_credit FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT OL_W_ID, HISTOGRAM(OL_I_ID) AS item_histogram FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT C_W_ID, LIST(C_ID ORDER BY C_BALANCE DESC) AS cust_list FROM CUSTOMER GROUP BY C_W_ID;",
        # Types: LIST / STRUCT
        "SELECT C_W_ID, LIST(C_LAST ORDER BY C_BALANCE DESC) AS top_names FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT I_ID, I_NAME, STRUCT_PACK(name := I_NAME, price := I_PRICE) AS info FROM ITEM;",
        "SELECT unnest([10, 20, 30, 40]) AS threshold, COUNT(*) AS above FROM STOCK CROSS JOIN (SELECT 1) z WHERE S_QUANTITY >= (SELECT unnest([10, 20, 30, 40])) GROUP BY threshold;",
    ]
    queries.extend(extra_funcs)

    # ===== Complex multi-level queries =====
    complex_levels = [
        "WITH inner_agg AS (SELECT C_W_ID, C_D_ID, COUNT(*) AS n, AVG(C_BALANCE) AS avg_b FROM CUSTOMER GROUP BY C_W_ID, C_D_ID) SELECT ia.C_W_ID, COUNT(*) AS n_districts, SUM(ia.n) AS total_cust, AVG(ia.avg_b) AS grand_avg FROM inner_agg ia GROUP BY ia.C_W_ID HAVING COUNT(*) > 1;",
        "WITH ord_lines AS (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS ord_total FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID) SELECT o.O_W_ID, o.O_D_ID, COUNT(*) AS orders, AVG(ol_a.ord_total) AS avg_order_val, MAX(ol_a.ord_total) AS max_order FROM OORDER o JOIN ord_lines ol_a ON o.O_W_ID = ol_a.OL_W_ID AND o.O_ID = ol_a.OL_O_ID GROUP BY o.O_W_ID, o.O_D_ID HAVING COUNT(*) > 2;",
        "SELECT top.w_id, top.tot, top.rn FROM (SELECT C_W_ID AS w_id, SUM(C_BALANCE) AS tot, ROW_NUMBER() OVER (ORDER BY SUM(C_BALANCE) DESC) AS rn FROM CUSTOMER GROUP BY C_W_ID) top WHERE top.rn <= 5;",
        "WITH cust_per_state AS (SELECT C_STATE, COUNT(*) AS n FROM CUSTOMER GROUP BY C_STATE), wh_per_state AS (SELECT W_STATE, COUNT(*) AS n FROM WAREHOUSE GROUP BY W_STATE) SELECT COALESCE(c.C_STATE, w.W_STATE) AS state, COALESCE(c.n, 0) AS cust, COALESCE(w.n, 0) AS wh FROM cust_per_state c FULL OUTER JOIN wh_per_state w ON c.C_STATE = w.W_STATE;",
        "SELECT gk, cnt, cnt * 1.0 / SUM(cnt) OVER () AS share FROM (SELECT C_CREDIT AS gk, COUNT(*) AS cnt FROM CUSTOMER GROUP BY C_CREDIT) sub;",
        "WITH ranked AS (SELECT i.I_ID, i.I_NAME, SUM(ol.OL_AMOUNT) AS rev, RANK() OVER (ORDER BY SUM(ol.OL_AMOUNT) DESC) AS rnk FROM ITEM i LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME) SELECT I_ID, I_NAME, rev, rnk FROM ranked WHERE rnk <= 20;",
        "SELECT state, credit, cust_count, AVG(cust_count) OVER (PARTITION BY state) AS state_avg FROM (SELECT C_STATE AS state, C_CREDIT AS credit, COUNT(*) AS cust_count FROM CUSTOMER GROUP BY C_STATE, C_CREDIT) sub;",
        "WITH o_info AS (SELECT O_W_ID, O_D_ID, COUNT(*) AS n FROM OORDER GROUP BY O_W_ID, O_D_ID), no_info AS (SELECT NO_W_ID, NO_D_ID, COUNT(*) AS n FROM NEW_ORDER GROUP BY NO_W_ID, NO_D_ID) SELECT COALESCE(o.O_W_ID, n.NO_W_ID) AS w, COALESCE(o.O_D_ID, n.NO_D_ID) AS d, COALESCE(o.n, 0) AS orders, COALESCE(n.n, 0) AS new FROM o_info o FULL OUTER JOIN no_info n ON o.O_W_ID = n.NO_W_ID AND o.O_D_ID = n.NO_D_ID;",
        "WITH pay AS (SELECT H_W_ID, SUM(H_AMOUNT) AS tot FROM HISTORY GROUP BY H_W_ID), bal AS (SELECT C_W_ID, SUM(C_BALANCE) AS tot FROM CUSTOMER GROUP BY C_W_ID) SELECT w.W_ID, w.W_NAME, COALESCE(p.tot, 0) AS payments, COALESCE(b.tot, 0) AS balance FROM WAREHOUSE w LEFT JOIN pay p ON w.W_ID = p.H_W_ID LEFT JOIN bal b ON w.W_ID = b.C_W_ID;",
        "SELECT w.W_ID, w.W_NAME, (SELECT AVG(c.C_BALANCE) FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID) AS w_avg_bal, (SELECT AVG(C_BALANCE) FROM CUSTOMER) AS overall_avg_bal, (SELECT AVG(c.C_BALANCE) FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID) - (SELECT AVG(C_BALANCE) FROM CUSTOMER) AS diff FROM WAREHOUSE w;",
    ]
    queries.extend(complex_levels)

    # ===== More varied CTEs with multiple levels =====
    multi_cte_deeper = [
        "WITH cust AS (SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER), orders AS (SELECT O_W_ID, O_C_ID, COUNT(*) AS n FROM OORDER GROUP BY O_W_ID, O_C_ID), joined AS (SELECT c.C_W_ID, c.C_ID, c.C_BALANCE, COALESCE(o.n, 0) AS orders FROM cust c LEFT JOIN orders o ON c.C_W_ID = o.O_W_ID AND c.C_ID = o.O_C_ID) SELECT C_W_ID, AVG(C_BALANCE) AS avg_bal, AVG(orders) AS avg_ord, CORR(C_BALANCE, orders) AS corr FROM joined GROUP BY C_W_ID;",
        "WITH line_agg AS (SELECT OL_W_ID, OL_O_ID, SUM(OL_AMOUNT) AS tot, SUM(OL_QUANTITY) AS qty FROM ORDER_LINE GROUP BY OL_W_ID, OL_O_ID), ord_with_totals AS (SELECT o.O_W_ID, o.O_ID, o.O_C_ID, o.O_OL_CNT, la.tot, la.qty FROM OORDER o JOIN line_agg la ON o.O_W_ID = la.OL_W_ID AND o.O_ID = la.OL_O_ID) SELECT owt.O_W_ID, COUNT(*) AS orders, SUM(owt.tot) AS revenue, AVG(owt.tot / NULLIF(owt.qty, 0)) AS avg_unit_price FROM ord_with_totals owt GROUP BY owt.O_W_ID;",
        "WITH cust AS (SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, C_PAYMENT_CNT FROM CUSTOMER), ranked AS (SELECT cust.*, ROW_NUMBER() OVER (PARTITION BY C_W_ID, C_D_ID ORDER BY C_BALANCE DESC) AS bal_rn FROM cust) SELECT C_W_ID, C_D_ID, AVG(C_BALANCE) AS avg_top, COUNT(*) AS top3_cnt FROM ranked WHERE bal_rn <= 3 GROUP BY C_W_ID, C_D_ID;",
        "WITH stock_val AS (SELECT s.S_W_ID, s.S_I_ID, s.S_QUANTITY * i.I_PRICE AS val FROM STOCK s JOIN ITEM i ON s.S_I_ID = i.I_ID), per_wh AS (SELECT S_W_ID, SUM(val) AS tot_val, AVG(val) AS avg_val FROM stock_val GROUP BY S_W_ID) SELECT pw.S_W_ID, pw.tot_val, pw.avg_val, RANK() OVER (ORDER BY pw.tot_val DESC) AS rnk FROM per_wh pw;",
        "WITH t1 AS (SELECT C_W_ID, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID), t2 AS (SELECT t1.C_W_ID, t1.n, CASE WHEN t1.n > 100 THEN 'big' WHEN t1.n > 10 THEN 'med' ELSE 'small' END AS size FROM t1), t3 AS (SELECT t2.size, COUNT(*) AS wh_count, SUM(t2.n) AS total_cust FROM t2 GROUP BY t2.size) SELECT * FROM t3;",
    ]
    queries.extend(multi_cte_deeper)

    return queries


def composite_final_queries() -> list[str]:
    """Final batch targeting under-represented patterns: more correlated subqueries,
    more LATERAL, more CROSS JOIN, deeper CTEs, and deeper TPC-H style queries."""
    queries = []

    # ===== More correlated subqueries (EXISTS/NOT EXISTS/IN/scalar) across tables =====
    more_correlated = [
        "SELECT w.W_ID, w.W_NAME FROM WAREHOUSE w WHERE EXISTS (SELECT 1 FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID AND c.C_BALANCE > 1000);",
        "SELECT w.W_ID FROM WAREHOUSE w WHERE NOT EXISTS (SELECT 1 FROM DISTRICT d WHERE d.D_W_ID = w.W_ID AND d.D_TAX > 0.15);",
        "SELECT d.D_W_ID, d.D_ID FROM DISTRICT d WHERE EXISTS (SELECT 1 FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID AND c.C_CREDIT = 'GC');",
        "SELECT c.C_W_ID, c.C_ID FROM CUSTOMER c WHERE NOT EXISTS (SELECT 1 FROM OORDER o WHERE o.O_C_ID = c.C_ID AND o.O_W_ID = c.C_W_ID);",
        "SELECT o.O_W_ID, o.O_ID FROM OORDER o WHERE o.O_ID IN (SELECT no.NO_O_ID FROM NEW_ORDER no WHERE no.NO_W_ID = o.O_W_ID AND no.NO_D_ID = o.O_D_ID);",
        "SELECT i.I_ID, i.I_NAME FROM ITEM i WHERE EXISTS (SELECT 1 FROM STOCK s WHERE s.S_I_ID = i.I_ID AND s.S_QUANTITY > 50);",
        "SELECT s.S_W_ID, s.S_I_ID FROM STOCK s WHERE NOT EXISTS (SELECT 1 FROM ORDER_LINE ol WHERE ol.OL_I_ID = s.S_I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID);",
        "SELECT h.H_W_ID, h.H_C_ID FROM HISTORY h WHERE h.H_AMOUNT = (SELECT MAX(h2.H_AMOUNT) FROM HISTORY h2 WHERE h2.H_W_ID = h.H_W_ID);",
        "SELECT d.D_W_ID, d.D_ID FROM DISTRICT d WHERE d.D_YTD > (SELECT AVG(d2.D_YTD) FROM DISTRICT d2 WHERE d2.D_W_ID = d.D_W_ID);",
        "SELECT c.C_W_ID, c.C_ID FROM CUSTOMER c WHERE c.C_ID NOT IN (SELECT o.O_C_ID FROM OORDER o WHERE o.O_W_ID = c.C_W_ID);",
        "SELECT ol.OL_W_ID, ol.OL_O_ID, ol.OL_NUMBER FROM ORDER_LINE ol WHERE ol.OL_AMOUNT > (SELECT 2 * AVG(ol2.OL_AMOUNT) FROM ORDER_LINE ol2 WHERE ol2.OL_W_ID = ol.OL_W_ID);",
        "SELECT o.O_W_ID, o.O_ID, o.O_OL_CNT FROM OORDER o WHERE (SELECT COUNT(*) FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID AND ol.OL_DELIVERY_D IS NOT NULL) = o.O_OL_CNT;",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST FROM CUSTOMER c WHERE EXISTS (SELECT 1 FROM HISTORY h WHERE h.H_C_ID = c.C_ID AND h.H_C_W_ID = c.C_W_ID AND h.H_AMOUNT > c.C_BALANCE);",
        "SELECT i.I_ID, i.I_NAME FROM ITEM i WHERE (SELECT SUM(s.S_QUANTITY) FROM STOCK s WHERE s.S_I_ID = i.I_ID) > 100;",
        "SELECT d.D_W_ID, d.D_ID FROM DISTRICT d WHERE (SELECT COUNT(DISTINCT c.C_ID) FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID AND c.C_CREDIT = 'GC') > (SELECT COUNT(DISTINCT c.C_ID) FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID AND c.C_CREDIT = 'BC');",
    ]
    queries.extend(more_correlated)

    # ===== More LATERAL joins (varied patterns) =====
    more_lateral = [
        "SELECT w.W_ID, w.W_NAME, stats.cust_cnt, stats.dist_cnt FROM WAREHOUSE w JOIN LATERAL (SELECT (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = w.W_ID) AS cust_cnt, (SELECT COUNT(*) FROM DISTRICT WHERE D_W_ID = w.W_ID) AS dist_cnt) stats ON TRUE;",
        "SELECT c.C_W_ID, c.C_ID, top_pay.amt FROM CUSTOMER c JOIN LATERAL (SELECT MAX(H_AMOUNT) AS amt FROM HISTORY WHERE H_C_ID = c.C_ID AND H_C_W_ID = c.C_W_ID) top_pay ON TRUE WHERE top_pay.amt IS NOT NULL;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, cust.top_bal, cust.top_cust_id FROM DISTRICT d JOIN LATERAL (SELECT MAX(C_BALANCE) AS top_bal, MAX(C_ID) AS top_cust_id FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID) cust ON TRUE;",
        "SELECT w.W_ID, stats.* FROM WAREHOUSE w, LATERAL (SELECT COUNT(*) AS n_cust, SUM(C_BALANCE) AS total_bal, AVG(C_BALANCE) AS avg_bal FROM CUSTOMER WHERE C_W_ID = w.W_ID) stats;",
        "SELECT o.O_W_ID, o.O_ID, lines.cnt, lines.total FROM OORDER o CROSS JOIN LATERAL (SELECT COUNT(*) AS cnt, SUM(OL_AMOUNT) AS total FROM ORDER_LINE WHERE OL_O_ID = o.O_ID AND OL_W_ID = o.O_W_ID) lines;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, revenue.tot, revenue.avg FROM ITEM i, LATERAL (SELECT COALESCE(SUM(OL_AMOUNT), 0) AS tot, COALESCE(AVG(OL_AMOUNT), 0) AS avg FROM ORDER_LINE WHERE OL_I_ID = i.I_ID) revenue;",
        "SELECT c.C_W_ID, c.C_STATE, agg.cust_cnt FROM (SELECT DISTINCT C_W_ID, C_STATE FROM CUSTOMER) c JOIN LATERAL (SELECT COUNT(*) AS cust_cnt FROM CUSTOMER WHERE C_W_ID = c.C_W_ID AND C_STATE = c.C_STATE) agg ON TRUE;",
        "SELECT d.D_W_ID, d.D_ID, first_cust.c_id, first_cust.c_last FROM DISTRICT d JOIN LATERAL (SELECT C_ID AS c_id, C_LAST AS c_last FROM CUSTOMER WHERE C_W_ID = d.D_W_ID AND C_D_ID = d.D_ID LIMIT 1) first_cust ON TRUE;",
    ]
    queries.extend(more_lateral)

    # ===== More CROSS JOIN queries =====
    more_cross = [
        "SELECT s.label, w.W_ID, w.W_NAME FROM WAREHOUSE w CROSS JOIN (SELECT unnest(['small', 'medium', 'large']) AS label) s;",
        "SELECT c.C_W_ID, r.n FROM CUSTOMER c CROSS JOIN range(1, 4) r(n) WHERE c.C_BALANCE > r.n * 1000;",
        "SELECT w.W_ID, d.D_ID FROM WAREHOUSE w CROSS JOIN DISTRICT d;",
        "SELECT i.I_ID, s.slot FROM ITEM i CROSS JOIN generate_series(1, 3) s(slot) WHERE i.I_PRICE > s.slot * 20;",
        "SELECT t.tier, COUNT(c.C_ID) AS cust_cnt FROM (VALUES ('vip'), ('gold'), ('silver'), ('bronze')) t(tier) LEFT JOIN CUSTOMER c ON (t.tier = 'vip' AND c.C_BALANCE > 5000) OR (t.tier = 'gold' AND c.C_BALANCE BETWEEN 1000 AND 5000) OR (t.tier = 'silver' AND c.C_BALANCE BETWEEN 100 AND 1000) OR (t.tier = 'bronze' AND c.C_BALANCE < 100) GROUP BY t.tier;",
        "SELECT dim.lvl, COUNT(*) AS n FROM (SELECT unnest(['low', 'mid', 'high']) AS lvl) dim CROSS JOIN STOCK s WHERE (dim.lvl = 'low' AND s.S_QUANTITY < 20) OR (dim.lvl = 'mid' AND s.S_QUANTITY BETWEEN 20 AND 80) OR (dim.lvl = 'high' AND s.S_QUANTITY > 80) GROUP BY dim.lvl;",
    ]
    queries.extend(more_cross)

    # ===== More CTEs with different patterns =====
    more_cte = [
        "WITH tax_bins AS (SELECT D_W_ID, D_ID, CASE WHEN D_TAX < 0.05 THEN 'low' WHEN D_TAX < 0.10 THEN 'med' ELSE 'high' END AS bin FROM DISTRICT) SELECT bin, COUNT(*) AS n, AVG(D_YTD) FROM tax_bins tb JOIN DISTRICT d ON tb.D_W_ID = d.D_W_ID AND tb.D_ID = d.D_ID GROUP BY bin;",
        "WITH orders_per_cust AS (SELECT O_C_ID, O_W_ID, COUNT(*) AS n FROM OORDER GROUP BY O_C_ID, O_W_ID), order_buckets AS (SELECT O_C_ID, O_W_ID, n, NTILE(4) OVER (ORDER BY n) AS bucket FROM orders_per_cust) SELECT bucket, COUNT(*) AS cust_cnt, AVG(n) AS avg_orders FROM order_buckets GROUP BY bucket;",
        "WITH item_popularity AS (SELECT OL_I_ID, COUNT(*) AS sales, RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk FROM ORDER_LINE GROUP BY OL_I_ID) SELECT i.I_ID, i.I_NAME, i.I_PRICE, ip.sales, ip.rnk FROM ITEM i LEFT JOIN item_popularity ip ON i.I_ID = ip.OL_I_ID;",
        "WITH mat AS (SELECT C_W_ID, C_D_ID, SUM(C_BALANCE) AS tot, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID, C_D_ID) SELECT C_W_ID, SUM(tot) AS w_tot, SUM(n) AS w_cnt, COUNT(*) AS dists FROM mat GROUP BY C_W_ID;",
        "WITH stock_low AS (SELECT S_W_ID, COUNT(*) AS low_cnt FROM STOCK WHERE S_QUANTITY < 20 GROUP BY S_W_ID), stock_high AS (SELECT S_W_ID, COUNT(*) AS high_cnt FROM STOCK WHERE S_QUANTITY >= 80 GROUP BY S_W_ID) SELECT COALESCE(l.S_W_ID, h.S_W_ID) AS w_id, COALESCE(l.low_cnt, 0) AS low_stock, COALESCE(h.high_cnt, 0) AS high_stock FROM stock_low l FULL OUTER JOIN stock_high h ON l.S_W_ID = h.S_W_ID;",
        "WITH hist_by_type AS (SELECT H_W_ID, CASE WHEN H_AMOUNT > 100 THEN 'big' ELSE 'small' END AS kind, COUNT(*) AS n FROM HISTORY GROUP BY H_W_ID, CASE WHEN H_AMOUNT > 100 THEN 'big' ELSE 'small' END) SELECT H_W_ID, SUM(CASE WHEN kind = 'big' THEN n ELSE 0 END) AS big_n, SUM(CASE WHEN kind = 'small' THEN n ELSE 0 END) AS small_n FROM hist_by_type GROUP BY H_W_ID;",
        "WITH w_rev AS (SELECT OL_W_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_W_ID), w_cust AS (SELECT C_W_ID, COUNT(*) AS cust FROM CUSTOMER GROUP BY C_W_ID), w_stock AS (SELECT S_W_ID, SUM(S_QUANTITY) AS stock FROM STOCK GROUP BY S_W_ID) SELECT w.W_ID, w.W_NAME, COALESCE(r.rev, 0) AS revenue, COALESCE(c.cust, 0) AS customers, COALESCE(s.stock, 0) AS stock_units FROM WAREHOUSE w LEFT JOIN w_rev r ON w.W_ID = r.OL_W_ID LEFT JOIN w_cust c ON w.W_ID = c.C_W_ID LEFT JOIN w_stock s ON w.W_ID = s.S_W_ID;",
        "WITH state_cust AS (SELECT C_STATE, COUNT(*) AS cnt, AVG(C_BALANCE) AS avg_b FROM CUSTOMER GROUP BY C_STATE), state_rank AS (SELECT *, RANK() OVER (ORDER BY cnt DESC) AS cust_rank, RANK() OVER (ORDER BY avg_b DESC) AS wealth_rank FROM state_cust) SELECT C_STATE, cnt, avg_b, cust_rank, wealth_rank FROM state_rank WHERE cust_rank <= 10 OR wealth_rank <= 10;",
        "WITH paying_cust AS (SELECT DISTINCT H_C_ID, H_C_W_ID FROM HISTORY), no_pay AS (SELECT C_W_ID, C_ID, C_LAST, C_BALANCE FROM CUSTOMER WHERE (C_ID, C_W_ID) NOT IN (SELECT H_C_ID, H_C_W_ID FROM paying_cust)) SELECT COUNT(*) AS non_paying, AVG(C_BALANCE) AS avg_bal FROM no_pay;",
        "WITH multi_warehouse AS (SELECT OL_I_ID, COUNT(DISTINCT OL_SUPPLY_W_ID) AS w_cnt FROM ORDER_LINE GROUP BY OL_I_ID HAVING COUNT(DISTINCT OL_SUPPLY_W_ID) > 1) SELECT i.I_ID, i.I_NAME, mw.w_cnt FROM ITEM i JOIN multi_warehouse mw ON i.I_ID = mw.OL_I_ID;",
    ]
    queries.extend(more_cte)

    # ===== Deep TPC-H style queries =====
    deep_tpch = [
        "SELECT w.W_STATE, c.C_CREDIT, COUNT(DISTINCT c.C_ID) AS n_cust, SUM(c.C_BALANCE) AS bal, SUM(c.C_YTD_PAYMENT) AS ytd_pay, AVG(c.C_DISCOUNT) AS avg_disc, STDDEV(c.C_BALANCE) AS std_bal FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE, c.C_CREDIT HAVING COUNT(DISTINCT c.C_ID) > 5;",
        "WITH per_item AS (SELECT OL_I_ID, COUNT(*) AS cnt, SUM(OL_AMOUNT) AS rev, SUM(OL_QUANTITY) AS qty FROM ORDER_LINE GROUP BY OL_I_ID) SELECT i.I_NAME, i.I_PRICE, pi.cnt, pi.rev, pi.qty, ROUND(pi.rev / NULLIF(pi.qty, 0), 2) AS unit_price, RANK() OVER (ORDER BY pi.rev DESC) AS rev_rank FROM ITEM i JOIN per_item pi ON i.I_ID = pi.OL_I_ID;",
        "SELECT EXTRACT(YEAR FROM o.O_ENTRY_D) AS yr, EXTRACT(QUARTER FROM o.O_ENTRY_D) AS q, o.O_W_ID, COUNT(*) AS orders, SUM(ol.OL_AMOUNT) AS rev, COUNT(DISTINCT ol.OL_I_ID) AS unique_items, AVG(o.O_OL_CNT) AS avg_lines FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID WHERE o.O_ENTRY_D IS NOT NULL GROUP BY EXTRACT(YEAR FROM o.O_ENTRY_D), EXTRACT(QUARTER FROM o.O_ENTRY_D), o.O_W_ID;",
        "WITH cust_spend AS (SELECT C_W_ID, C_ID, C_LAST, SUM(H_AMOUNT) AS total_paid FROM CUSTOMER JOIN HISTORY ON C_ID = H_C_ID AND C_W_ID = H_C_W_ID GROUP BY C_W_ID, C_ID, C_LAST), cust_orders AS (SELECT O_W_ID, O_C_ID, COUNT(*) AS n_ord FROM OORDER GROUP BY O_W_ID, O_C_ID) SELECT cs.C_W_ID, cs.C_ID, cs.C_LAST, cs.total_paid, COALESCE(co.n_ord, 0) AS orders, ROUND(cs.total_paid / NULLIF(co.n_ord, 0), 2) AS avg_order_pay FROM cust_spend cs LEFT JOIN cust_orders co ON cs.C_W_ID = co.O_W_ID AND cs.C_ID = co.O_C_ID;",
        "SELECT d.D_W_ID, d.D_ID, d.D_NAME, d.D_TAX, COUNT(DISTINCT c.C_ID) AS cust, SUM(c.C_BALANCE) AS bal, SUM(c.C_YTD_PAYMENT) AS pay, ROUND(SUM(c.C_YTD_PAYMENT) * d.D_TAX, 2) AS tax_est, CASE WHEN AVG(c.C_BALANCE) > 2000 THEN 'A' WHEN AVG(c.C_BALANCE) > 500 THEN 'B' ELSE 'C' END AS grade FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID, d.D_NAME, d.D_TAX;",
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, SUM(s.S_QUANTITY) AS stock, SUM(ol.OL_QUANTITY) AS ordered, SUM(s.S_QUANTITY) - COALESCE(SUM(ol.OL_QUANTITY), 0) AS net, CASE WHEN SUM(s.S_QUANTITY) < 50 THEN 'reorder' ELSE 'ok' END AS action FROM ITEM i LEFT JOIN STOCK s ON i.I_ID = s.S_I_ID LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_ID, i.I_NAME, i.I_PRICE;",
        "SELECT s.S_W_ID, COUNT(*) AS items, SUM(s.S_QUANTITY * i.I_PRICE) AS inventory_value, AVG(s.S_QUANTITY * i.I_PRICE) AS avg_item_value, MAX(s.S_QUANTITY * i.I_PRICE) AS max_item_value, STDDEV(s.S_QUANTITY * i.I_PRICE) AS val_stddev FROM STOCK s JOIN ITEM i ON s.S_I_ID = i.I_ID GROUP BY s.S_W_ID;",
        "WITH top_20_items AS (SELECT OL_I_ID, SUM(OL_AMOUNT) AS rev FROM ORDER_LINE GROUP BY OL_I_ID ORDER BY rev DESC LIMIT 20), top_cust AS (SELECT C_ID, C_W_ID, SUM(C_YTD_PAYMENT) AS pay FROM CUSTOMER GROUP BY C_ID, C_W_ID ORDER BY pay DESC LIMIT 50) SELECT i.I_NAME, ti.rev, COUNT(DISTINCT ol.OL_W_ID) AS n_warehouses FROM ITEM i JOIN top_20_items ti ON i.I_ID = ti.OL_I_ID LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID GROUP BY i.I_NAME, ti.rev;",
        "SELECT c.C_W_ID, c.C_D_ID, COUNT(DISTINCT c.C_ID) AS cust, COUNT(DISTINCT o.O_ID) AS orders, COUNT(DISTINCT ol.OL_O_ID) AS ordered_with_lines, ROUND(COUNT(DISTINCT o.O_ID) * 1.0 / NULLIF(COUNT(DISTINCT c.C_ID), 0), 2) AS orders_per_cust FROM CUSTOMER c LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID LEFT JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY c.C_W_ID, c.C_D_ID;",
    ]
    queries.extend(deep_tpch)

    return queries


def filler_queries() -> list[str]:
    """Short filler batch to push total over 2000 — extra high/medium complexity."""
    return [
        # More window-function variations
        "SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, SUM(C_BALANCE) OVER (PARTITION BY C_W_ID ORDER BY C_D_ID, C_ID ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS centered FROM CUSTOMER;",
        "SELECT OL_W_ID, OL_O_ID, OL_NUMBER, OL_AMOUNT, FIRST_VALUE(OL_AMOUNT) OVER (PARTITION BY OL_W_ID, OL_O_ID ORDER BY OL_NUMBER) AS first_amt, LAST_VALUE(OL_AMOUNT) OVER (PARTITION BY OL_W_ID, OL_O_ID ORDER BY OL_NUMBER ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_amt FROM ORDER_LINE;",
        "SELECT S_W_ID, S_I_ID, S_QUANTITY, NTH_VALUE(S_QUANTITY, 2) OVER (PARTITION BY S_W_ID ORDER BY S_QUANTITY DESC) AS second_largest FROM STOCK;",
        "SELECT H_W_ID, H_DATE, H_AMOUNT, COUNT(*) OVER (PARTITION BY H_W_ID ORDER BY H_DATE RANGE BETWEEN INTERVAL '30 days' PRECEDING AND CURRENT ROW) AS rolling_30d FROM HISTORY WHERE H_DATE IS NOT NULL;",
        "SELECT C_W_ID, C_BALANCE, CUME_DIST() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS dist, PERCENT_RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS pct FROM CUSTOMER;",
        # Deeper joins with filtering
        "SELECT w.W_STATE, COUNT(DISTINCT c.C_ID) AS cust, SUM(ol.OL_AMOUNT) AS rev FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID JOIN OORDER o ON c.C_ID = o.O_C_ID AND c.C_W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID WHERE c.C_CREDIT = 'GC' GROUP BY w.W_STATE;",
        "SELECT d.D_NAME, COUNT(DISTINCT c.C_ID) AS cust, SUM(h.H_AMOUNT) AS paid FROM DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID WHERE c.C_BALANCE > 0 GROUP BY d.D_NAME HAVING SUM(h.H_AMOUNT) > 100;",
        "SELECT i.I_IM_ID, COUNT(DISTINCT i.I_ID) AS unique_items, SUM(ol.OL_QUANTITY) AS qty, SUM(ol.OL_AMOUNT) AS rev, AVG(i.I_PRICE) AS avg_price FROM ITEM i JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID JOIN OORDER o ON ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID WHERE o.O_ALL_LOCAL = 1 GROUP BY i.I_IM_ID;",
        # Subquery in SELECT list with correlated count
        "SELECT o.O_W_ID, o.O_ID, (SELECT COUNT(*) FROM ORDER_LINE WHERE OL_O_ID = o.O_ID AND OL_W_ID = o.O_W_ID AND OL_DELIVERY_D IS NULL) AS pending_lines FROM OORDER o WHERE o.O_CARRIER_ID IS NULL;",
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST, (SELECT MIN(H_DATE) FROM HISTORY WHERE H_C_ID = c.C_ID AND H_C_W_ID = c.C_W_ID) AS first_payment, (SELECT MAX(H_DATE) FROM HISTORY WHERE H_C_ID = c.C_ID AND H_C_W_ID = c.C_W_ID) AS last_payment FROM CUSTOMER c WHERE c.C_BALANCE > 500;",
        # UNION with aggregates
        "SELECT 'warehouse_revenue' AS source, W_ID AS key, CAST(W_YTD AS DECIMAL(12,2)) AS value FROM WAREHOUSE UNION ALL SELECT 'district_revenue', D_W_ID * 100 + D_ID, CAST(D_YTD AS DECIMAL(12,2)) FROM DISTRICT UNION ALL SELECT 'customer_balance', C_W_ID * 10000 + C_ID, CAST(SUM(C_BALANCE) AS DECIMAL(12,2)) FROM CUSTOMER GROUP BY C_W_ID, C_ID;",
        # CASE in GROUP BY
        "SELECT CASE WHEN C_BALANCE < 0 THEN 'overdue' WHEN C_BALANCE < 1000 THEN 'low' WHEN C_BALANCE < 5000 THEN 'mid' ELSE 'high' END AS tier, COUNT(*) AS cnt, AVG(C_YTD_PAYMENT) AS avg_pay FROM CUSTOMER GROUP BY CASE WHEN C_BALANCE < 0 THEN 'overdue' WHEN C_BALANCE < 1000 THEN 'low' WHEN C_BALANCE < 5000 THEN 'mid' ELSE 'high' END;",
        "SELECT CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 50 THEN 'mid' ELSE 'expensive' END AS tier, COUNT(*) AS items, AVG(I_PRICE) AS avg_p FROM ITEM GROUP BY CASE WHEN I_PRICE < 20 THEN 'cheap' WHEN I_PRICE < 50 THEN 'mid' ELSE 'expensive' END;",
        # FILTER clause on aggregates
        "SELECT C_W_ID, COUNT(*) FILTER (WHERE C_CREDIT = 'GC') AS good_cnt, COUNT(*) FILTER (WHERE C_CREDIT = 'BC') AS bad_cnt, SUM(C_BALANCE) FILTER (WHERE C_BALANCE > 0) AS positive_bal FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT OL_W_ID, SUM(OL_AMOUNT) FILTER (WHERE OL_DELIVERY_D IS NOT NULL) AS delivered_amt, SUM(OL_AMOUNT) FILTER (WHERE OL_DELIVERY_D IS NULL) AS pending_amt, COUNT(*) FILTER (WHERE OL_AMOUNT > 100) AS big_lines FROM ORDER_LINE GROUP BY OL_W_ID;",
        "SELECT S_W_ID, COUNT(*) FILTER (WHERE S_QUANTITY < 20) AS low, COUNT(*) FILTER (WHERE S_QUANTITY BETWEEN 20 AND 80) AS mid, COUNT(*) FILTER (WHERE S_QUANTITY > 80) AS high FROM STOCK GROUP BY S_W_ID;",
        # ARRAY / LIST operations
        "SELECT C_W_ID, LIST(C_ID) AS all_ids, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT OL_O_ID, OL_W_ID, LIST(OL_I_ID ORDER BY OL_NUMBER) AS item_sequence, SUM(OL_AMOUNT) AS tot FROM ORDER_LINE GROUP BY OL_O_ID, OL_W_ID;",
        "SELECT S_W_ID, LIST(S_I_ID ORDER BY S_QUANTITY DESC) FILTER (WHERE S_QUANTITY > 50) AS well_stocked FROM STOCK GROUP BY S_W_ID;",
        # Multi-key joins with complex predicates
        "SELECT w.W_ID, d.D_ID, COUNT(DISTINCT c.C_ID) AS cust, COUNT(DISTINCT o.O_ID) AS ord FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID LEFT JOIN OORDER o ON o.O_W_ID = d.D_W_ID AND o.O_D_ID = d.D_ID AND o.O_C_ID = c.C_ID GROUP BY w.W_ID, d.D_ID;",
        # EXCEPT/INTERSECT in nested queries
        "SELECT C_ID, C_W_ID FROM CUSTOMER WHERE C_W_ID IN (SELECT DISTINCT OL_W_ID FROM ORDER_LINE INTERSECT SELECT DISTINCT H_W_ID FROM HISTORY);",
        "SELECT I_ID FROM ITEM WHERE I_ID NOT IN (SELECT DISTINCT S_I_ID FROM STOCK EXCEPT SELECT DISTINCT OL_I_ID FROM ORDER_LINE);",
        # LIKE / ILIKE patterns with aggregates
        "SELECT C_W_ID, COUNT(*) FILTER (WHERE C_LAST LIKE 'A%') AS starts_a, COUNT(*) FILTER (WHERE C_LAST LIKE '%son') AS ends_son, COUNT(*) FILTER (WHERE UPPER(C_LAST) SIMILAR TO '[B-D].*') AS starts_bd FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT I_IM_ID, COUNT(*) AS total, COUNT(*) FILTER (WHERE I_NAME LIKE '%SPECIAL%') AS specials FROM ITEM GROUP BY I_IM_ID;",
        # Numeric aggregations with cast
        "SELECT C_W_ID, SUM(CAST(C_PAYMENT_CNT AS BIGINT)) AS total_payments, SUM(CAST(C_DELIVERY_CNT AS BIGINT)) AS total_deliveries FROM CUSTOMER GROUP BY C_W_ID;",
        "SELECT I_IM_ID, ROUND(AVG(I_PRICE::DOUBLE), 4) AS avg_price, ROUND(STDDEV(I_PRICE::DOUBLE), 4) AS std_price FROM ITEM GROUP BY I_IM_ID;",
        # ANY / ALL operator
        "SELECT C_W_ID, C_ID, C_BALANCE FROM CUSTOMER WHERE C_BALANCE > ALL (SELECT AVG(H_AMOUNT) FROM HISTORY GROUP BY H_W_ID);",
        "SELECT O_W_ID, O_ID FROM OORDER WHERE O_OL_CNT >= ANY (SELECT O_OL_CNT FROM OORDER WHERE O_ALL_LOCAL = 0);",
        "SELECT I_ID, I_NAME FROM ITEM WHERE I_PRICE > ALL (SELECT OL_AMOUNT / NULLIF(OL_QUANTITY, 0) FROM ORDER_LINE WHERE OL_I_ID = ITEM.I_ID);",
        # Window function on aggregated subquery output
        "SELECT yr, mo, orders, SUM(orders) OVER (ORDER BY yr, mo ROWS UNBOUNDED PRECEDING) AS cumulative FROM (SELECT EXTRACT(YEAR FROM O_ENTRY_D) AS yr, EXTRACT(MONTH FROM O_ENTRY_D) AS mo, COUNT(*) AS orders FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY EXTRACT(YEAR FROM O_ENTRY_D), EXTRACT(MONTH FROM O_ENTRY_D)) t;",
        "SELECT C_STATE, cust, rnk FROM (SELECT C_STATE, COUNT(*) AS cust, RANK() OVER (ORDER BY COUNT(*) DESC) AS rnk FROM CUSTOMER GROUP BY C_STATE) t WHERE rnk <= 5;",
        # Self-joins
        "SELECT c1.C_W_ID, c1.C_ID AS c1_id, c2.C_ID AS c2_id, c1.C_BALANCE AS b1, c2.C_BALANCE AS b2 FROM CUSTOMER c1 JOIN CUSTOMER c2 ON c1.C_W_ID = c2.C_W_ID AND c1.C_ID < c2.C_ID AND ABS(c1.C_BALANCE - c2.C_BALANCE) < 10;",
        "SELECT w1.W_ID AS w1, w2.W_ID AS w2 FROM WAREHOUSE w1 JOIN WAREHOUSE w2 ON w1.W_STATE = w2.W_STATE AND w1.W_ID < w2.W_ID;",
        "SELECT i1.I_ID, i2.I_ID, i1.I_IM_ID FROM ITEM i1 JOIN ITEM i2 ON i1.I_IM_ID = i2.I_IM_ID AND i1.I_ID < i2.I_ID AND ABS(i1.I_PRICE - i2.I_PRICE) < 5;",
        # Triple CTE with different aggregates
        "WITH cust_stats AS (SELECT C_W_ID, COUNT(*) AS cn, AVG(C_BALANCE) AS cb FROM CUSTOMER GROUP BY C_W_ID), dist_stats AS (SELECT D_W_ID, COUNT(*) AS dn, AVG(D_YTD) AS dy FROM DISTRICT GROUP BY D_W_ID), stock_stats AS (SELECT S_W_ID, COUNT(*) AS sn, AVG(S_QUANTITY) AS sq FROM STOCK GROUP BY S_W_ID) SELECT w.W_ID, w.W_NAME, cs.cn, cs.cb, ds.dn, ds.dy, ss.sn, ss.sq FROM WAREHOUSE w LEFT JOIN cust_stats cs ON w.W_ID = cs.C_W_ID LEFT JOIN dist_stats ds ON w.W_ID = ds.D_W_ID LEFT JOIN stock_stats ss ON w.W_ID = ss.S_W_ID;",
        # HAVING with DISTINCT COUNT on joined table
        "SELECT c.C_W_ID, COUNT(DISTINCT c.C_STATE) AS states FROM CUSTOMER c GROUP BY c.C_W_ID HAVING COUNT(DISTINCT c.C_STATE) > 5;",
        "SELECT w.W_STATE, COUNT(DISTINCT c.C_CREDIT) AS credit_types FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID GROUP BY w.W_STATE HAVING COUNT(DISTINCT c.C_CREDIT) > 1;",
        # Correlated subquery in SELECT
        "SELECT i.I_ID, i.I_NAME, i.I_PRICE, (SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_I_ID = i.I_ID) AS revenue, (SELECT COUNT(DISTINCT OL_W_ID) FROM ORDER_LINE WHERE OL_I_ID = i.I_ID) AS warehouses, (SELECT AVG(S_QUANTITY) FROM STOCK WHERE S_I_ID = i.I_ID) AS avg_stock FROM ITEM i;",
        # GREATEST / LEAST with joins
        "SELECT w.W_ID, GREATEST(w.W_YTD, COALESCE((SELECT SUM(D_YTD) FROM DISTRICT WHERE D_W_ID = w.W_ID), 0)) AS max_ytd FROM WAREHOUSE w;",
        "SELECT c.C_W_ID, c.C_ID, LEAST(c.C_BALANCE, c.C_CREDIT_LIM) AS effective_limit FROM CUSTOMER c;",
        # Complex CASE WHEN in WHERE
        "SELECT C_W_ID, C_ID, C_BALANCE, C_CREDIT FROM CUSTOMER WHERE (CASE WHEN C_CREDIT = 'GC' THEN C_BALANCE > 0 WHEN C_CREDIT = 'BC' THEN C_BALANCE > 1000 ELSE FALSE END);",
        # CTE referencing itself via UNION
        "WITH RECURSIVE wh_tree AS (SELECT W_ID, W_NAME, 0 AS lvl FROM WAREHOUSE WHERE W_ID = 1 UNION ALL SELECT w.W_ID, w.W_NAME, lvl + 1 FROM WAREHOUSE w, wh_tree wt WHERE w.W_ID = wt.W_ID + 1 AND lvl < 3) SELECT * FROM wh_tree;",
        # Division / ratio calculations
        "SELECT d.D_W_ID, d.D_ID, COUNT(c.C_ID) AS cust, ROUND(SUM(c.C_BALANCE) / NULLIF(COUNT(c.C_ID), 0), 2) AS avg_bal, ROUND(SUM(c.C_YTD_PAYMENT) / NULLIF(SUM(c.C_PAYMENT_CNT), 0), 2) AS avg_pay_per_payment FROM DISTRICT d LEFT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID GROUP BY d.D_W_ID, d.D_ID;",
        # Nested CTE + correlated subquery
        "WITH ord_agg AS (SELECT O_W_ID, O_D_ID, O_C_ID, COUNT(*) AS n, SUM(O_OL_CNT) AS lines FROM OORDER GROUP BY O_W_ID, O_D_ID, O_C_ID) SELECT oa.O_W_ID, oa.O_D_ID, oa.O_C_ID, oa.n, (SELECT C_LAST FROM CUSTOMER WHERE C_ID = oa.O_C_ID AND C_W_ID = oa.O_W_ID) AS c_last FROM ord_agg oa WHERE oa.n > 1;",
        # LIKE in JOIN predicate
        "SELECT c.C_W_ID, c.C_ID, c.C_LAST, i.I_NAME FROM CUSTOMER c CROSS JOIN ITEM i WHERE c.C_LAST LIKE SUBSTRING(i.I_NAME FROM 1 FOR 1) || '%' LIMIT 100;",
        # Multiple UNIONs
        "SELECT 'active' AS type, COUNT(*) AS n FROM OORDER WHERE O_CARRIER_ID IS NULL UNION ALL SELECT 'delivered', COUNT(*) FROM OORDER WHERE O_CARRIER_ID IS NOT NULL UNION ALL SELECT 'new', COUNT(*) FROM NEW_ORDER;",
        "SELECT 'customers' AS entity, C_W_ID AS w_id, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID UNION ALL SELECT 'orders', O_W_ID, COUNT(*) FROM OORDER GROUP BY O_W_ID UNION ALL SELECT 'order_lines', OL_W_ID, COUNT(*) FROM ORDER_LINE GROUP BY OL_W_ID;",
        # COUNT conditionals
        "SELECT C_W_ID, COUNT(*) AS total, COUNT(C_MIDDLE) AS with_middle, COUNT(DISTINCT C_CREDIT) AS credit_types, COUNT(DISTINCT C_STATE) AS states FROM CUSTOMER GROUP BY C_W_ID;",
        # Time-series pattern
        "SELECT DATE_TRUNC('day', H_DATE) AS day, H_W_ID, SUM(H_AMOUNT) AS daily_total, COUNT(*) AS payments, AVG(H_AMOUNT) AS avg_payment FROM HISTORY WHERE H_DATE IS NOT NULL GROUP BY DATE_TRUNC('day', H_DATE), H_W_ID;",
        "SELECT DATE_TRUNC('hour', O_ENTRY_D) AS hr, COUNT(*) AS orders, SUM(O_OL_CNT) AS total_lines FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY DATE_TRUNC('hour', O_ENTRY_D);",
        # Aggregates over LATERAL
        "SELECT w.W_ID, lat.cust_count, lat.order_count, lat.revenue FROM WAREHOUSE w, LATERAL (SELECT (SELECT COUNT(*) FROM CUSTOMER WHERE C_W_ID = w.W_ID) AS cust_count, (SELECT COUNT(*) FROM OORDER WHERE O_W_ID = w.W_ID) AS order_count, (SELECT SUM(OL_AMOUNT) FROM ORDER_LINE WHERE OL_W_ID = w.W_ID) AS revenue) lat;",
        # Deep deep analytics
        "WITH monthly AS (SELECT O_W_ID, DATE_TRUNC('month', O_ENTRY_D) AS mo, COUNT(*) AS orders, SUM(O_OL_CNT) AS lines FROM OORDER WHERE O_ENTRY_D IS NOT NULL GROUP BY O_W_ID, DATE_TRUNC('month', O_ENTRY_D)), with_diff AS (SELECT O_W_ID, mo, orders, lines, LAG(orders) OVER (PARTITION BY O_W_ID ORDER BY mo) AS prev_orders, orders - LAG(orders) OVER (PARTITION BY O_W_ID ORDER BY mo) AS delta FROM monthly) SELECT O_W_ID, mo, orders, prev_orders, delta FROM with_diff WHERE delta IS NOT NULL;",
        "WITH cust_rank AS (SELECT C_W_ID, C_ID, C_LAST, C_BALANCE, RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS rnk FROM CUSTOMER) SELECT cr.C_W_ID, cr.C_ID, cr.C_LAST, cr.C_BALANCE, cr.rnk, w.W_NAME FROM cust_rank cr JOIN WAREHOUSE w ON cr.C_W_ID = w.W_ID WHERE cr.rnk <= 5;",
        "WITH pop_items AS (SELECT OL_I_ID, SUM(OL_QUANTITY) AS qty FROM ORDER_LINE GROUP BY OL_I_ID HAVING SUM(OL_QUANTITY) > 10), multi_wh AS (SELECT OL_I_ID, COUNT(DISTINCT OL_SUPPLY_W_ID) AS wh_cnt FROM ORDER_LINE GROUP BY OL_I_ID HAVING COUNT(DISTINCT OL_SUPPLY_W_ID) > 1) SELECT i.I_ID, i.I_NAME, pi.qty, mw.wh_cnt FROM ITEM i JOIN pop_items pi ON i.I_ID = pi.OL_I_ID JOIN multi_wh mw ON i.I_ID = mw.OL_I_ID;",
        # Multiple window functions in single query
        "SELECT C_W_ID, C_ID, C_BALANCE, ROW_NUMBER() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS rn, RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS rnk, DENSE_RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS drnk, CUME_DIST() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS cd, PERCENT_RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS pr, NTILE(10) OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS decile FROM CUSTOMER;",
        # Summary
        "SELECT 'WAREHOUSE' AS tbl, COUNT(*) FROM WAREHOUSE UNION ALL SELECT 'DISTRICT', COUNT(*) FROM DISTRICT UNION ALL SELECT 'CUSTOMER', COUNT(*) FROM CUSTOMER UNION ALL SELECT 'ITEM', COUNT(*) FROM ITEM UNION ALL SELECT 'STOCK', COUNT(*) FROM STOCK UNION ALL SELECT 'OORDER', COUNT(*) FROM OORDER UNION ALL SELECT 'NEW_ORDER', COUNT(*) FROM NEW_ORDER UNION ALL SELECT 'ORDER_LINE', COUNT(*) FROM ORDER_LINE UNION ALL SELECT 'HISTORY', COUNT(*) FROM HISTORY;",
        # VALUES / generated data
        "SELECT label, low, high, COUNT(s.S_W_ID) AS items FROM (VALUES ('empty', 0, 1), ('low', 1, 20), ('med', 20, 80), ('high', 80, 10000)) t(label, low, high) LEFT JOIN STOCK s ON s.S_QUANTITY >= t.low AND s.S_QUANTITY < t.high GROUP BY label, low, high;",
        "SELECT * FROM (VALUES (1, 'GC', 'good'), (2, 'BC', 'bad')) t(n, code, label);",
        # Window on FULL OUTER JOIN
        "SELECT COALESCE(o.O_W_ID, no.NO_W_ID) AS w_id, COALESCE(o.O_ID, no.NO_O_ID) AS o_id, ROW_NUMBER() OVER (PARTITION BY COALESCE(o.O_W_ID, no.NO_W_ID) ORDER BY COALESCE(o.O_ID, no.NO_O_ID)) AS rn FROM OORDER o FULL OUTER JOIN NEW_ORDER no ON o.O_ID = no.NO_O_ID AND o.O_W_ID = no.NO_W_ID AND o.O_D_ID = no.NO_D_ID;",
        # Aggregate over derived window
        "SELECT C_W_ID, AVG(rn) AS avg_row_num FROM (SELECT C_W_ID, C_ID, ROW_NUMBER() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS rn FROM CUSTOMER) sub GROUP BY C_W_ID;",
        # More ORDER BY variations
        "SELECT D_NAME, D_YTD FROM DISTRICT ORDER BY D_YTD DESC NULLS LAST LIMIT 30;",
        "SELECT I_ID, I_NAME, I_PRICE FROM ITEM ORDER BY I_PRICE DESC, I_NAME ASC LIMIT 40;",
        "SELECT C_W_ID, C_STATE, COUNT(*) AS n FROM CUSTOMER GROUP BY C_W_ID, C_STATE ORDER BY n DESC LIMIT 25;",
    ]


class QueryGenerator:
    def __init__(self, output_dir: str = "benchmark/queries", ducklake: bool = False):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.generated = 0
        self.valid = 0
        self.invalid = 0
        self.rejection_reasons = {}
        self.ducklake_enabled = ducklake

        self.db = duckdb.connect(":memory:")
        self._create_tpcc_schema()
        if ducklake:
            self._attach_ducklake()

    def _attach_ducklake(self):
        """Attach a DuckLake catalog and mirror TPC-C tables into it.
        Needed to validate `ducklake_*.sql` queries that reference `dl.<table>`."""
        try:
            self.db.execute("INSTALL ducklake")
            self.db.execute("LOAD ducklake")
            dl_path = "/tmp/openivm_validator_ducklake.db"
            self.db.execute(f"ATTACH IF NOT EXISTS '{dl_path}' AS dl (TYPE ducklake)")
            # Recreate schema + empty tables each run. Validation only needs the
            # schema to resolve columns; no data is required.
            have = self.db.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_catalog = 'dl' AND table_name = 'warehouse'"
            ).fetchone()
            if not have or have[0] == 0:
                self.db.execute("USE dl.main")
                self._create_tpcc_schema(skip_log=True)
                self.db.execute("USE memory.main")
        except Exception as e:
            log(f"WARNING: could not attach DuckLake catalog: {e}")
            self.ducklake_enabled = False

    def _create_tpcc_schema(self, skip_log: bool = False):
        if not skip_log:
            log("Creating TPC-C schema in DuckDB for validation...")

        self.db.execute("""
            CREATE TABLE WAREHOUSE (
                W_ID INT, W_YTD DECIMAL(12, 2), W_TAX DECIMAL(4, 4),
                W_NAME VARCHAR(10), W_STREET_1 VARCHAR(20), W_STREET_2 VARCHAR(20),
                W_CITY VARCHAR(20), W_STATE CHAR(2), W_ZIP CHAR(9)
            )
        """)
        self.db.execute("""
            CREATE TABLE DISTRICT (
                D_W_ID INT, D_ID INT, D_YTD DECIMAL(12, 2), D_TAX DECIMAL(4, 4),
                D_NEXT_O_ID INT, D_NAME VARCHAR(10), D_STREET_1 VARCHAR(20),
                D_STREET_2 VARCHAR(20), D_CITY VARCHAR(20), D_STATE CHAR(2), D_ZIP CHAR(9)
            )
        """)
        self.db.execute("""
            CREATE TABLE CUSTOMER (
                C_W_ID INT, C_D_ID INT, C_ID INT, C_DISCOUNT DECIMAL(4, 4),
                C_CREDIT CHAR(2), C_LAST VARCHAR(16), C_FIRST VARCHAR(16),
                C_CREDIT_LIM DECIMAL(12, 2), C_BALANCE DECIMAL(12, 2),
                C_YTD_PAYMENT FLOAT, C_PAYMENT_CNT INT, C_DELIVERY_CNT INT,
                C_STREET_1 VARCHAR(20), C_STREET_2 VARCHAR(20),
                C_CITY VARCHAR(20), C_STATE CHAR(2), C_ZIP CHAR(9),
                C_PHONE CHAR(16), C_SINCE TIMESTAMP, C_MIDDLE CHAR(2), C_DATA VARCHAR(500)
            )
        """)
        self.db.execute("""
            CREATE TABLE ITEM (
                I_ID INT, I_NAME VARCHAR(24), I_PRICE DECIMAL(5, 2),
                I_DATA VARCHAR(50), I_IM_ID INT
            )
        """)
        self.db.execute("""
            CREATE TABLE STOCK (
                S_W_ID INT, S_I_ID INT, S_QUANTITY INT, S_YTD DECIMAL(8, 2),
                S_ORDER_CNT INT, S_REMOTE_CNT INT, S_DATA VARCHAR(50),
                S_DIST_01 CHAR(24), S_DIST_02 CHAR(24), S_DIST_03 CHAR(24),
                S_DIST_04 CHAR(24), S_DIST_05 CHAR(24), S_DIST_06 CHAR(24),
                S_DIST_07 CHAR(24), S_DIST_08 CHAR(24), S_DIST_09 CHAR(24), S_DIST_10 CHAR(24)
            )
        """)
        self.db.execute("""
            CREATE TABLE OORDER (
                O_W_ID INT, O_D_ID INT, O_ID INT, O_C_ID INT, O_CARRIER_ID INT,
                O_OL_CNT INT, O_ALL_LOCAL INT, O_ENTRY_D TIMESTAMP
            )
        """)
        self.db.execute("""
            CREATE TABLE NEW_ORDER (
                NO_W_ID INT, NO_D_ID INT, NO_O_ID INT
            )
        """)
        self.db.execute("""
            CREATE TABLE ORDER_LINE (
                OL_W_ID INT, OL_D_ID INT, OL_O_ID INT, OL_NUMBER INT, OL_I_ID INT,
                OL_DELIVERY_D TIMESTAMP, OL_AMOUNT DECIMAL(6, 2), OL_SUPPLY_W_ID INT,
                OL_QUANTITY DECIMAL(6, 2), OL_DIST_INFO CHAR(24)
            )
        """)
        self.db.execute("""
            CREATE TABLE HISTORY (
                H_C_ID INT, H_C_D_ID INT, H_C_W_ID INT, H_D_ID INT, H_W_ID INT,
                H_DATE TIMESTAMP, H_AMOUNT DECIMAL(6, 2), H_DATA VARCHAR(24)
            )
        """)

        log("TPC-C schema created for validation.")

    def _validate_query(self, query: str) -> tuple[bool, str]:
        """Returns (is_valid, reason)."""
        try:
            self.db.execute(query).fetchall()
            return True, "ok"
        except Exception as e:
            return False, str(e)[:120]

    def _extract_metadata(self, query: str) -> dict:
        """Extract metadata from query string."""
        query_upper = query.upper()

        operators = []

        # Join detection — order matters (check FULL OUTER before LEFT/RIGHT)
        if "JOIN" in query_upper:
            if "FULL OUTER JOIN" in query_upper or "FULL JOIN" in query_upper:
                operators.append("FULL_OUTER_JOIN")
            if "LEFT JOIN" in query_upper or "RIGHT JOIN" in query_upper:
                operators.append("OUTER_JOIN")
            if re.search(r'\bJOIN\b', query_upper) and "INNER JOIN" in query_upper or (
                    "JOIN" in query_upper and "LEFT JOIN" not in query_upper and
                    "RIGHT JOIN" not in query_upper and "FULL" not in query_upper and
                    "CROSS JOIN" not in query_upper):
                # plain JOIN = INNER JOIN
                if "INNER_JOIN" not in operators and "OUTER_JOIN" not in operators and "FULL_OUTER_JOIN" not in operators:
                    operators.append("INNER_JOIN")
        if "CROSS JOIN" in query_upper:
            operators.append("CROSS_JOIN")
        if "LATERAL" in query_upper:
            operators.append("LATERAL")

        # AGGREGATE: GROUP BY or aggregate function
        agg_fns = ["COUNT(", "SUM(", "AVG(", "MIN(", "MAX(", "STDDEV(", "VARIANCE(",
                   "VAR_POP(", "STDDEV_POP(", "VAR_SAMP(", "STDDEV_SAMP(",
                   "BOOL_AND(", "BOOL_OR(", "STRING_AGG(", "ARRAY_AGG(",
                   "BIT_AND(", "BIT_OR(", "KURTOSIS(", "SKEWNESS(", "ENTROPY(",
                   "MEDIAN(", "QUANTILE(", "APPROX_COUNT_DISTINCT(", "HISTOGRAM("]
        has_agg_fn = any(fn in query_upper for fn in agg_fns)
        if "GROUP BY" in query_upper or has_agg_fn:
            operators.append("AGGREGATE")

        if "WHERE" in query_upper:
            operators.append("FILTER")
        if "ORDER BY" in query_upper:
            operators.append("ORDER")
        if re.search(r'\bLIMIT\b', query_upper):
            operators.append("LIMIT")
        if "HAVING" in query_upper:
            operators.append("HAVING")
        if "UNION" in query_upper:
            operators.append("UNION")
        if "DISTINCT" in query_upper:
            operators.append("DISTINCT")
        if "OVER (" in query_upper or "OVER(" in query_upper:
            operators.append("WINDOW")
        if "GENERATE_SERIES" in query_upper or "RANGE(" in query_upper or "UNNEST(" in query_upper:
            operators.append("TABLE_FUNCTION")

        # CTE: starts with WITH keyword before the main SELECT
        stripped = query_upper.lstrip()
        if stripped.startswith("WITH ") and "SELECT" in stripped:
            operators.append("CTE")

        # Subqueries — distinguish by position and correlation, because OpenIVM
        # only handles subqueries that appear in FROM/JOIN position (derived tables).
        # Benchmark-verified patterns:
        #   - FROM (SELECT …), JOIN (SELECT …) → incremental
        #   - WHERE col IN (SELECT …)          → FULL_REFRESH
        #   - WHERE col [<>=] (SELECT scalar)  → FULL_REFRESH (scalar after op)
        #   - WHERE (SELECT scalar) [<>=] col  → FULL_REFRESH (scalar before op)
        #   - SELECT …, (SELECT scalar) AS …   → FULL_REFRESH (comma-separated)
        #   - SELECT func((SELECT scalar), …)  → FULL_REFRESH (scalar inside call)
        #   - Correlated (outer-alias reference in subquery) → FULL_REFRESH
        if re.search(r'\(\s*SELECT\s', query_upper):
            is_correlated = False
            # Correlated detection: look for outer-alias dot-references inside the subquery,
            # but skip when the query starts with WITH — CTE scopes don't count as outer.
            if not query_upper.lstrip().startswith("WITH "):
                subq_match = re.search(r'\(\s*SELECT\s.+?\)', query_upper, re.DOTALL)
                if subq_match:
                    outer_aliases = re.findall(r'\bFROM\s+\w+\s+(\w+)\b', query_upper)
                    for alias in outer_aliases:
                        if alias and re.search(rf'\b{alias}\.\w+',
                                               query_upper[subq_match.start():subq_match.end()]):
                            is_correlated = True
                            break
            if is_correlated:
                operators.append("CORRELATED_SUBQUERY")
            else:
                # Classify by position — OpenIVM only handles FROM/JOIN subqueries
                subq_where_in = bool(re.search(r'WHERE[^;]*?\s(?:NOT\s+)?IN\s*\(\s*SELECT', query_upper))
                subq_where_cmp_after = bool(re.search(r'WHERE[^;]*?[<>=!]\s*\(\s*SELECT', query_upper))
                # scalar subquery BEFORE comparison: WHERE (SELECT ...) op val
                subq_where_cmp_before = bool(re.search(r'WHERE\s*\(\s*SELECT[^;]*?\)\s*[<>=!]', query_upper))
                subq_in_select = bool(re.search(r'SELECT[^;]*?,\s*\(\s*SELECT', query_upper))
                # scalar subquery inside a function call: e.g. COALESCE((SELECT ...), …)
                # — any "(SELECT" that appears inside the SELECT-list expression tree but
                #   wasn't already matched as a top-level comma-separated item.
                # Heuristic: "(SELECT" appears BEFORE the FROM clause (i.e. in SELECT list).
                from_pos = query_upper.find(" FROM ")
                subq_in_func = False
                if from_pos > 0:
                    select_list = query_upper[:from_pos]
                    if re.search(r'\w+\s*\([^)]*\(\s*SELECT', select_list):
                        subq_in_func = True
                subq_in_from = bool(re.search(r'FROM\s*\(\s*SELECT', query_upper)) or \
                               bool(re.search(r'JOIN\s*\(\s*SELECT', query_upper))
                if subq_where_in or subq_where_cmp_after or subq_where_cmp_before or \
                        subq_in_select or subq_in_func:
                    operators.append("SUBQUERY_FILTER")
                elif subq_in_from:
                    operators.append("SUBQUERY")
                else:
                    operators.append("SUBQUERY")

        # No-base-table queries (VALUES-only, unnest-only) → FULL_REFRESH.
        # If the query's top-level FROM references only VALUES or a table function,
        # there's no base table to delta — OpenIVM falls back to full refresh.
        tpcc_tables_in_from = bool(re.search(
            r'\b(?:FROM|JOIN)\b\s+(?:WAREHOUSE|DISTRICT|CUSTOMER|ITEM|STOCK|OORDER|NEW_ORDER|ORDER_LINE|HISTORY)\b',
            query_upper))
        if not tpcc_tables_in_from:
            if re.search(r'\bFROM\s*\(\s*VALUES\b', query_upper) or \
                    re.search(r'\bFROM\s+(?:RANGE|GENERATE_SERIES|UNNEST)\s*\(', query_upper) or \
                    re.search(r'^\s*SELECT\s+UNNEST\s*\(', query_upper):
                operators.append("VALUES_ONLY")
        else:
            # Has a TPC-C base table in FROM, but VALUES/unnest appears as an extra
            # CROSS/LATERAL join with a VALUES/unnest source — still problematic for
            # OpenIVM because the VALUES/unnest source has no deltas.
            if re.search(r'(?:CROSS\s+JOIN|LEFT\s+JOIN|,)\s*\(\s*VALUES\b', query_upper) or \
                    re.search(r'(?:CROSS\s+JOIN|LEFT\s+JOIN|,)\s*\(\s*SELECT\s+UNNEST\s*\(', query_upper):
                operators.append("VALUES_ONLY")

        if not operators:
            operators.append("SCAN")

        # Complexity scoring
        join_count = query_upper.count("JOIN")
        has_join = any(op in operators for op in ("INNER_JOIN", "OUTER_JOIN", "FULL_OUTER_JOIN", "CROSS_JOIN", "LATERAL"))
        complex_ops = {"WINDOW", "CTE", "CORRELATED_SUBQUERY", "TABLE_FUNCTION", "LATERAL"}
        has_complex = bool(set(operators) & complex_ops)

        complexity = "low"
        if len(operators) >= 3 or (has_join and len(operators) >= 2) or has_complex:
            complexity = "medium"
        if len(operators) >= 5 or join_count >= 2 or has_complex and has_join or (
            "SUBQUERY" in operators and has_join) or (
            "CTE" in operators and ("AGGREGATE" in operators or has_join)):
            complexity = "high"

        has_nulls = "NULL" in query_upper or "COALESCE" in query_upper or "IS NULL" in query_upper
        has_cast = "CAST(" in query_upper or "::" in query
        has_case = "CASE WHEN" in query_upper

        # Incrementability — classification based on what OpenIVM actually supports,
        # verified by running the benchmark on 2000+ queries and reading the
        # `type` column of `_duckdb_ivm_views` after CREATE MATERIALIZED VIEW.
        #
        # Verified incrementalizable (OpenIVM reports non-FULL_REFRESH):
        #   - SCAN, FILTER, AGGREGATE (SUM/COUNT/AVG/MIN/MAX), GROUP BY, HAVING
        #   - DISTINCT, UNION / UNION ALL
        #   - INNER / LEFT / RIGHT / FULL OUTER JOIN (up to 16 tables; self-joins OK)
        #   - CTEs (inlined by DuckDB optimizer)
        #   - WINDOW functions — OpenIVM treats them as incrementalizable at the
        #     optimizer level (surprising, but confirmed by the run)
        #   - STDDEV / VARIANCE / STDDEV_POP / VAR_POP — OpenIVM handles these
        #     via SUM/COUNT decomposition
        #   - TABLE_FUNCTION (range, generate_series, unnest) — handled as long
        #     as not combined with LATERAL
        #   - FILTER (...) clause on aggregates — handled (FILTER → CASE-WHEN)
        #   - LIST / ARRAY_AGG — handled
        #   - ROLLUP / CUBE / GROUPING SETS — handled
        #   - CASE WHEN / CAST / COALESCE / NULLIF and scalar functions
        #   - Uncorrelated subqueries (flattened during optimization)
        #
        # Verified NOT supported (OpenIVM reports FULL_REFRESH):
        #   - ORDER BY, LIMIT (top-level ordering)
        #   - RANDOM / NOW / CURRENT_TIMESTAMP — non-deterministic
        #   - LATERAL joins
        #   - CORRELATED subqueries (EXISTS / NOT EXISTS / correlated IN / scalar)
        #   - EXCEPT / INTERSECT
        #   - ANY / ALL subquery quantifiers
        #   - RECURSIVE CTEs
        #   - Rare holistic aggregates (CORR, COVAR_*, KURTOSIS, SKEWNESS, MEDIAN,
        #     PERCENTILE_*, APPROX_*, ENTROPY, HISTOGRAM, MODE)
        #   - STRING_AGG (order-sensitive)
        #   - BIT_AND / BIT_OR, BOOL_AND / BOOL_OR (no inverse on delete)
        #   - FIRST / LAST / ANY_VALUE / ARBITRARY
        non_incremental_ops = {
            "ORDER", "LIMIT", "LATERAL", "CORRELATED_SUBQUERY",
            "SUBQUERY_FILTER",  # IN/scalar-compare/scalar-in-SELECT — not flattened by OpenIVM
            "VALUES_ONLY",      # FROM (VALUES …) — no base table to delta
        }
        non_incremental_fns = [
            "RANDOM(", "NOW(", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME",
            "CORR(", "COVAR_POP(", "COVAR_SAMP(",
            "KURTOSIS(", "SKEWNESS(",
            "MEDIAN(", "QUANTILE(", "APPROX_QUANTILE(", "APPROX_COUNT_DISTINCT(",
            "PERCENTILE_CONT(", "PERCENTILE_DISC(",
            "ENTROPY(", "HISTOGRAM(", "MODE(",
            "STRING_AGG(",
            "BIT_AND(", "BIT_OR(",
            "BOOL_AND(", "BOOL_OR(",
            "FIRST(", "LAST(", "ANY_VALUE(", "ARBITRARY(",
        ]
        non_incremental_keywords = [
            " EXCEPT ", " INTERSECT ",
            " RECURSIVE ",
            " ANY (", " ALL (",
        ]

        is_incremental = True
        reason = None
        if set(operators) & non_incremental_ops:
            is_incremental = False
            reason = "op:" + ",".join(sorted(set(operators) & non_incremental_ops))
        for fn in non_incremental_fns:
            if fn in query_upper:
                is_incremental = False
                reason = f"fn:{fn.rstrip('(')}"
                break
        if is_incremental:
            for kw in non_incremental_keywords:
                if kw in query_upper:
                    is_incremental = False
                    reason = f"kw:{kw.strip()}"
                    break

        tables = []
        for table in ["WAREHOUSE", "DISTRICT", "CUSTOMER", "ITEM", "STOCK",
                      "OORDER", "NEW_ORDER", "ORDER_LINE", "HISTORY"]:
            if re.search(rf'\b{table}\b', query_upper):
                tables.append(table)

        result = {
            "operators": ",".join(operators),
            "complexity": complexity,
            "is_incremental": is_incremental,
            "has_nulls": has_nulls,
            "has_cast": has_cast,
            "has_case": has_case,
            "tables": ",".join(tables) if tables else "UNKNOWN",
        }
        if not is_incremental and reason:
            result["non_incr_reason"] = reason
        return result

    def retag_existing(self, include_ducklake: bool = True):
        """Re-extract metadata for all existing query files and rewrite their first line.
        Preserves benchmark-verified flags (openivm_verified, openivm_lies) and the
        `ducklake` tag from the previous metadata — if a query has been empirically
        verified, don't overwrite is_incremental based on the heuristic alone."""
        files = sorted(self.output_dir.glob("query_*.sql"))
        if include_ducklake:
            files += sorted(self.output_dir.glob("ducklake_*.sql"))
        log(f"Retagging {len(files)} existing query files...")

        updated = 0
        errors = 0
        for f in files:
            try:
                lines = f.read_text().splitlines()
                if not lines:
                    continue

                # Preserve benchmark-verified flags and the ducklake tag.
                prev_verified = False
                prev_lies = False
                prev_is_incremental = None
                prev_ducklake = False
                if lines[0].startswith("-- {"):
                    try:
                        prev_meta = json.loads(lines[0][3:])
                        prev_verified = prev_meta.get("openivm_verified", False)
                        prev_lies = prev_meta.get("openivm_lies", False)
                        prev_is_incremental = prev_meta.get("is_incremental", None)
                        prev_ducklake = prev_meta.get("ducklake", False)
                    except Exception:
                        pass

                # Find the SQL query (skip comment lines)
                query_lines = [l for l in lines if not l.startswith("--")]
                query = " ".join(query_lines).strip()
                if not query:
                    continue

                metadata = self._extract_metadata(query)

                # If we have benchmark-verified truth, keep that value
                if prev_verified or prev_lies:
                    metadata["is_incremental"] = bool(prev_is_incremental)
                    if prev_verified:
                        metadata["openivm_verified"] = True
                    if prev_lies:
                        metadata["openivm_lies"] = True
                    metadata.pop("non_incr_reason", None)
                if prev_ducklake or f.name.startswith("ducklake_"):
                    metadata["ducklake"] = True
                new_content = "-- " + json.dumps(metadata) + "\n" + query + "\n"
                f.write_text(new_content)
                updated += 1
            except Exception as e:
                log(f"  Error retagging {f.name}: {e}")
                errors += 1

        log(f"  Retagged {updated} files ({errors} errors)")

    def prune_low_complexity(self, keep_max: int = 300):
        """Remove low-complexity queries beyond keep_max, keeping most recently numbered ones."""
        files = sorted(self.output_dir.glob("query_*.sql"))
        low_files = []
        for f in files:
            try:
                first_line = f.read_text().split('\n')[0]
                if first_line.startswith("-- "):
                    m = json.loads(first_line[3:])
                    if m.get("complexity") == "low":
                        low_files.append(f)
            except Exception:
                pass

        to_remove = len(low_files) - keep_max
        if to_remove <= 0:
            log(f"  Only {len(low_files)} low-complexity queries — no pruning needed")
            return 0

        # Remove the earliest-numbered low-complexity queries
        low_files.sort()
        removed = 0
        for f in low_files[:to_remove]:
            f.unlink()
            removed += 1

        log(f"  Pruned {removed} low-complexity queries (kept {keep_max})")
        return removed

    def renumber_queries(self, prefix: str = "query"):
        """Renumber `<prefix>_*.sql` files to be sequential starting from 0001.
        `ducklake_*.sql` files are renumbered independently so native and
        DuckLake suites don't collide."""
        files = sorted(self.output_dir.glob(f"{prefix}_*.sql"))
        log(f"Renumbering {len(files)} {prefix}_*.sql files...")
        if not files:
            return

        # Write to temp names first to avoid conflicts
        tmp_tag = f"_tmp_{prefix}_"
        for i, f in enumerate(files, 1):
            tmp = f.parent / f"{tmp_tag}{i:04d}.sql"
            f.rename(tmp)

        for i, tmp in enumerate(sorted(files[0].parent.glob(f"{tmp_tag}*.sql")), 1):
            target = tmp.parent / f"{prefix}_{i:04d}.sql"
            tmp.rename(target)

        log(f"  Renumbered {len(files)} files")

    def validate_all(self, prefix: str = None):
        """Run every existing query through the DuckDB validator and report
        which fail to parse/bind. `prefix` filters (e.g. 'ducklake')."""
        patterns = [f"{prefix}_*.sql"] if prefix else ["query_*.sql", "ducklake_*.sql"]
        files = []
        for p in patterns:
            files += sorted(self.output_dir.glob(p))
        log(f"Validating {len(files)} files...")

        ok = 0
        bad = []
        for f in files:
            try:
                lines = f.read_text().splitlines()
                query_lines = [l for l in lines if not l.startswith("--")]
                query = " ".join(query_lines).strip().rstrip(";")
                if not query:
                    continue
                is_valid, reason = self._validate_query(query)
                if is_valid:
                    ok += 1
                else:
                    bad.append((f.name, reason))
            except Exception as e:
                bad.append((f.name, f"exception: {e}"))

        log(f"  {ok} OK, {len(bad)} failed")
        for name, reason in bad[:20]:
            log(f"    {name}: {reason[:120]}")
        if len(bad) > 20:
            log(f"    ... and {len(bad) - 20} more")
        return len(bad) == 0

    def _next_query_id(self, prefix: str = "query") -> int:
        """Find the next available id for files named `<prefix>_NNNN.sql`."""
        files = list(self.output_dir.glob(f"{prefix}_*.sql"))
        if not files:
            return 1
        nums = []
        pat = re.compile(rf'{re.escape(prefix)}_(\d+)\.sql')
        for f in files:
            m = pat.match(f.name)
            if m:
                nums.append(int(m.group(1)))
        return max(nums) + 1 if nums else 1

    def process_query(self, query: str, query_id: int, prefix: str = "query") -> bool:
        """Validate, extract metadata, and save a single query as `<prefix>_NNNN.sql`."""
        is_valid, reason = self._validate_query(query)
        if not is_valid:
            self.invalid += 1
            self.rejection_reasons[reason] = self.rejection_reasons.get(reason, 0) + 1
            return False

        metadata = self._extract_metadata(query)
        if prefix == "ducklake":
            metadata["ducklake"] = True

        query_file = self.output_dir / f"{prefix}_{query_id:04d}.sql"
        with open(query_file, 'w') as f:
            f.write("-- " + json.dumps(metadata) + "\n")
            f.write(query + "\n")

        self.valid += 1
        return True

    def print_stats(self):
        """Print stats about the current query set (native + DuckLake)."""
        import collections
        native = sorted(self.output_dir.glob("query_*.sql"))
        ducklake = sorted(self.output_dir.glob("ducklake_*.sql"))
        complexity = collections.Counter()
        operators = collections.Counter()
        is_incr = collections.Counter()

        for f in native + ducklake:
            try:
                first_line = f.read_text().split('\n')[0]
                if first_line.startswith("-- "):
                    m = json.loads(first_line[3:])
                    complexity[m.get("complexity", "?")] += 1
                    for op in m.get("operators", "").split(","):
                        operators[op.strip()] += 1
                    is_incr[str(m.get("is_incremental", "?"))] += 1
            except Exception:
                pass

        log(f"Total queries: {len(native) + len(ducklake)} "
            f"({len(native)} native + {len(ducklake)} ducklake)")
        log(f"Complexity: {dict(complexity)}")
        log(f"Incremental: {dict(is_incr)}")
        log("Operators (top 20):")
        for op, cnt in operators.most_common(20):
            log(f"  {op}: {cnt}")




def main():
    parser = argparse.ArgumentParser(
        description="Validate TPC-C queries and maintain their metadata headers. "
                    "Query generation via LLM has been removed — all queries are hand-written "
                    "in benchmark/queries/ (named query_NNNN.sql or ducklake_NNNN.sql)."
    )
    parser.add_argument("--output", default="benchmark/queries",
                        help="Queries directory (default: benchmark/queries)")
    parser.add_argument("--ducklake", action="store_true",
                        help="Attach a DuckLake catalog for validating ducklake_*.sql files")

    # Modes
    parser.add_argument("--retag", action="store_true",
                        help="Re-extract metadata for existing queries (including ducklake_*.sql)")
    parser.add_argument("--renumber", action="store_true",
                        help="Renumber query_*.sql files sequentially starting at 0001")
    parser.add_argument("--renumber-ducklake", action="store_true",
                        help="Renumber ducklake_*.sql files sequentially starting at 0001")
    parser.add_argument("--prune-low", type=int, metavar="N",
                        help="Remove low-complexity queries beyond N, then exit")
    parser.add_argument("--validate", action="store_true",
                        help="Validate every query parses/binds against TPC-C (requires --ducklake to include ducklake_*.sql)")
    parser.add_argument("--validate-ducklake", action="store_true",
                        help="Validate only ducklake_*.sql (implies --ducklake)")
    parser.add_argument("--stats", action="store_true",
                        help="Print query-set statistics")

    # Bulk-add modes (run the hand-written builder helpers in this file)
    parser.add_argument("--add-manual", action="store_true",
                        help="Run manual_queries() and write each to query_NNNN.sql")
    parser.add_argument("--programmatic", action="store_true",
                        help="Run programmatic_queries()")
    parser.add_argument("--composite", action="store_true", help="Run composite_queries()")
    parser.add_argument("--composite-extra", action="store_true", help="Run composite_extra_queries()")
    parser.add_argument("--composite-final", action="store_true", help="Run composite_final_queries()")
    parser.add_argument("--filler", action="store_true", help="Run filler_queries()")
    parser.add_argument("--prefix", default="query",
                        help="Filename prefix for bulk-add modes (default: query; use 'ducklake' for DuckLake queries)")

    args = parser.parse_args()

    # Enable DuckLake attach if the user asked for a DuckLake-aware mode.
    need_ducklake = args.ducklake or args.validate_ducklake or args.prefix == "ducklake"
    generator = QueryGenerator(output_dir=args.output, ducklake=need_ducklake)

    if args.stats:
        generator.print_stats()
        return 0
    if args.retag:
        generator.retag_existing()
        return 0
    if args.renumber:
        generator.renumber_queries(prefix="query")
        return 0
    if args.renumber_ducklake:
        generator.renumber_queries(prefix="ducklake")
        return 0
    if args.prune_low is not None:
        generator.prune_low_complexity(keep_max=args.prune_low)
        return 0
    if args.validate_ducklake:
        ok = generator.validate_all(prefix="ducklake")
        return 0 if ok else 1
    if args.validate:
        ok = generator.validate_all()
        return 0 if ok else 1

    bulk_modes = [
        ("add_manual", manual_queries),
        ("programmatic", programmatic_queries),
        ("composite", composite_queries),
        ("composite_extra", composite_extra_queries),
        ("composite_final", composite_final_queries),
        ("filler", filler_queries),
    ]
    for attr, fn in bulk_modes:
        if getattr(args, attr, False):
            queries = fn()
            log(f"Running {fn.__name__} — {len(queries)} queries")
            qid = generator._next_query_id(prefix=args.prefix)
            saved = 0
            for q in queries:
                if generator.process_query(q, qid, prefix=args.prefix):
                    qid += 1
                    saved += 1
            log(f"Added {saved}/{len(queries)} {args.prefix}_*.sql queries")
            if generator.rejection_reasons:
                log("Top rejection reasons:")
                for reason, count in sorted(generator.rejection_reasons.items(), key=lambda x: -x[1])[:5]:
                    log(f"    {count}x: {reason}")
            return 0

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
