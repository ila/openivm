-- {"operators": "FULL_OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT i.I_ID, i.I_NAME, i.I_PRICE, ol.OL_W_ID, ol.OL_AMOUNT FROM ITEM i FULL OUTER JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID;
