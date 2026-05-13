-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE"}
SELECT o.O_ID, o.O_W_ID FROM OORDER o WHERE EXISTS (SELECT 1 FROM ORDER_LINE ol WHERE ol.OL_O_ID = o.O_ID AND ol.OL_W_ID = o.O_W_ID AND ol.OL_AMOUNT > 100);
