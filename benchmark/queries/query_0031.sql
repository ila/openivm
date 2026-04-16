-- {"operators": "INNER_JOIN,AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, o.O_D_ID, SUM(ol.OL_AMOUNT) FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID WHERE o.O_D_ID = 3 GROUP BY o.O_W_ID, o.O_D_ID;
