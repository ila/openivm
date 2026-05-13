-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_W_ID, o.O_ID, o.O_OL_CNT FROM OORDER o WHERE (SELECT COUNT(*) FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID AND ol.OL_DELIVERY_D IS NOT NULL) = o.O_OL_CNT;
