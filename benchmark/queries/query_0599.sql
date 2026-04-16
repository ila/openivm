-- {"operators": "FILTER,ORDER,SUBQUERY_FILTER", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "fn:PERCENTILE_CONT"}
SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_BALANCE > (SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY C_BALANCE) FROM CUSTOMER);
