-- {"operators": "FILTER,DISTINCT,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,ORDER_LINE,HISTORY", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT C_ID, C_W_ID FROM CUSTOMER WHERE C_W_ID IN (SELECT DISTINCT OL_W_ID FROM ORDER_LINE INTERSECT SELECT DISTINCT H_W_ID FROM HISTORY);
