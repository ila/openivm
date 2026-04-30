-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,OORDER,ORDER_LINE", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT O_W_ID, O_ID FROM OORDER WHERE O_ID IN (SELECT OL_O_ID FROM ORDER_LINE WHERE OL_I_ID IN (SELECT I_ID FROM ITEM WHERE I_PRICE > 80));
