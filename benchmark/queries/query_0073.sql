-- {"operators": "INNER_JOIN,AGGREGATE,FILTER,HAVING", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, SUM(ol.OL_AMOUNT) as total FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID WHERE ol.OL_AMOUNT > 50 GROUP BY o.O_W_ID HAVING SUM(ol.OL_AMOUNT) > 100;
