-- {"operators": "FILTER,DISTINCT,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "OORDER,ORDER_LINE", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT O_W_ID, O_ID, O_C_ID FROM OORDER WHERE O_ID IN (SELECT DISTINCT OL_O_ID FROM ORDER_LINE WHERE OL_AMOUNT > 100);
