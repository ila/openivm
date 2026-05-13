-- {"operators": "FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY"}
SELECT c.C_W_ID, c.C_ID, c.C_LAST FROM CUSTOMER c WHERE NOT EXISTS (SELECT 1 FROM HISTORY h WHERE h.H_C_ID = c.C_ID AND h.H_C_W_ID = c.C_W_ID);
