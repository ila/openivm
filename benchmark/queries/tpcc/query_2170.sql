-- {"operators": "FILTER,SCALAR_SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT c.C_W_ID, c.C_D_ID, c.C_ID, c.C_BALANCE FROM CUSTOMER c WHERE c.C_BALANCE >= (SELECT AVG(c2.C_BALANCE) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID AND c2.C_D_ID = c.C_D_ID);
