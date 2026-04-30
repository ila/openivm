-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ORDER_LINE", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT s.S_W_ID, s.S_I_ID FROM STOCK s WHERE NOT EXISTS (SELECT 1 FROM ORDER_LINE ol WHERE ol.OL_I_ID = s.S_I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID);
