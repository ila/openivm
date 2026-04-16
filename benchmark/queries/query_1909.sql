-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT c.C_W_ID, c.C_ID, c.C_LAST FROM CUSTOMER c WHERE EXISTS (SELECT 1 FROM HISTORY h WHERE h.H_C_ID = c.C_ID AND h.H_C_W_ID = c.C_W_ID AND h.H_AMOUNT > c.C_BALANCE);
