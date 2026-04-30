-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "non_incr_reason": "op:CORRELATED_SUBQUERY"}
SELECT w.W_ID, w.W_NAME FROM WAREHOUSE w WHERE EXISTS (SELECT 1 FROM CUSTOMER c WHERE c.C_W_ID = w.W_ID AND c.C_BALANCE > 1000);
