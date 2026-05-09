-- {"operators": "INNER_JOIN,AGGREGATE", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE", "delta": true}
SELECT o.O_W_ID, SUM(ol.OL_AMOUNT) FROM d_OORDER o JOIN d_ORDER_LINE ol ON o.O_ID = ol.OL_O_ID GROUP BY o.O_W_ID;
