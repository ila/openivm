-- {"operators": "FILTER,SUBQUERY_FILTER", "complexity": "low", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "non_incr_reason": "op:SUBQUERY_FILTER"}
SELECT C_W_ID, C_ID FROM CUSTOMER WHERE C_W_ID IN (SELECT W_ID FROM WAREHOUSE) AND C_BALANCE > 0;
