-- {"operators": "CROSS_JOIN,AGGREGATE,SUBQUERY", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,ORDER_LINE", "ducklake": true}
SELECT w.W_ID, t.tot FROM dl.WAREHOUSE w CROSS JOIN (SELECT SUM(OL_AMOUNT) AS tot FROM dl.ORDER_LINE) t;
