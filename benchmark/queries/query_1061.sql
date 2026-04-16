-- {"operators": "OUTER_JOIN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER"}
SELECT d.D_W_ID, d.D_ID, d.D_NAME, c.C_ID, c.C_BALANCE FROM DISTRICT d RIGHT JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID;
