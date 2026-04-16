-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,OORDER,ORDER_LINE"}
SELECT w.W_ID, COUNT(*) AS cnt FROM WAREHOUSE w JOIN OORDER o ON w.W_ID = o.O_W_ID JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY w.W_ID HAVING COUNT(*) > 1;
