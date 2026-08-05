-- {"operators": "LEFT_JOIN,FILTER", "complexity": "medium", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE", "openivm_verified": true}
SELECT i.I_ID, i.I_NAME, ol.OL_W_ID, ol.OL_O_ID FROM ITEM i LEFT JOIN ORDER_LINE ol ON i.I_ID = ol.OL_I_ID WHERE ol.OL_I_ID IS NULL OR ol.OL_AMOUNT > 0;
