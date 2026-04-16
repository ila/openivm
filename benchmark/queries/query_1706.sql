-- {"operators": "OUTER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_ID, COUNT(*) AS n, SUM(COALESCE(ol.OL_NUMBER, 0)) AS tot, AVG(ol.OL_NUMBER) AS avg_val FROM OORDER o LEFT JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_ID;
