-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE"}
SELECT OL_W_ID, OL_O_ID, OL_AMOUNT FROM ORDER_LINE WHERE OL_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE > 80);
