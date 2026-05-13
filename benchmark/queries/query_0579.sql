-- {"operators": "AGGREGATE,FILTER,CORRELATED_SUBQUERY", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT c.C_ID, c.C_W_ID, c.C_BALANCE FROM CUSTOMER c WHERE c.C_BALANCE > (SELECT AVG(c2.C_BALANCE) FROM CUSTOMER c2 WHERE c2.C_W_ID = c.C_W_ID);
