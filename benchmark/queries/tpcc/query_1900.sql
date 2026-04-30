-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,OORDER", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT c.C_W_ID, c.C_ID FROM CUSTOMER c WHERE NOT EXISTS (SELECT 1 FROM OORDER o WHERE o.O_C_ID = c.C_ID AND o.O_W_ID = c.C_W_ID);
