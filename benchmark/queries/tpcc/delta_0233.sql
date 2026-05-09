-- {"operators": "CROSS_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,ORDER_LINE", "delta": true}
SELECT w.W_ID, t.tot FROM d_WAREHOUSE w CROSS JOIN (SELECT SUM(OL_AMOUNT) AS tot FROM d_ORDER_LINE) t;
