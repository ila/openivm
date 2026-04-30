-- {"operators": "OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER"}
SELECT w.W_ID, w.W_NAME, c.C_ID, c.C_LAST, c.C_BALANCE FROM WAREHOUSE w RIGHT JOIN CUSTOMER c ON w.W_ID = c.C_W_ID;
