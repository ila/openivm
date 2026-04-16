-- {"operators": "FILTER,SUBQUERY", "complexity": "low", "is_incremental": false, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE", "non_incr_reason": "kw:ALL ("}
SELECT I_ID, I_NAME FROM ITEM WHERE I_PRICE > ALL (SELECT OL_AMOUNT / NULLIF(OL_QUANTITY, 0) FROM ORDER_LINE WHERE OL_I_ID = ITEM.I_ID);
