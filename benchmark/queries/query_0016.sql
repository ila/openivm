-- {"operators": "INNER_JOIN,AGGREGATE,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, SUM(ol.OL_AMOUNT) FROM OORDER o JOIN ORDER_LINE ol ON o.O_ID = ol.OL_O_ID WHERE ol.OL_DELIVERY_D IS NOT NULL GROUP BY o.O_W_ID;
