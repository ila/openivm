-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT d.D_W_ID, d.D_ID FROM DISTRICT d WHERE EXISTS (SELECT 1 FROM CUSTOMER c WHERE c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID AND c.C_CREDIT = 'GC');
