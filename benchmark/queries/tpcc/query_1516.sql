-- {"operators": "INNER_JOIN,AGGREGATE,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, o.O_D_ID, SUM(ol.OL_AMOUNT) AS rev FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID AND o.O_W_ID = ol.OL_W_ID GROUP BY o.O_W_ID, o.O_D_ID HAVING SUM(ol.OL_AMOUNT) > 100 AND COUNT(*) > 3;
