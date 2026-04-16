-- {"operators": "AGGREGATE,FILTER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "ITEM,ORDER_LINE", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT I_ID, I_NAME FROM ITEM WHERE I_ID IN (SELECT OL_I_ID FROM ORDER_LINE WHERE OL_AMOUNT > (SELECT AVG(OL_AMOUNT) FROM ORDER_LINE));
