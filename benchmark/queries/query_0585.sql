-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, o.O_ID FROM OORDER o WHERE o.O_OL_CNT > (SELECT COUNT(*) FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID);
