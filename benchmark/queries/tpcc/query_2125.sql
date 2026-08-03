-- {"operators": "POSITIONAL_JOIN,LEFT_JOIN", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "STOCK,ITEM,ORDER_LINE", "non_incr_reason": "join:POSITIONAL"}
SELECT s.S_W_ID, s.S_I_ID, i.I_NAME, ol.OL_AMOUNT FROM STOCK s POSITIONAL JOIN ITEM i LEFT JOIN ORDER_LINE ol ON ol.OL_I_ID = i.I_ID AND ol.OL_SUPPLY_W_ID = s.S_W_ID;
