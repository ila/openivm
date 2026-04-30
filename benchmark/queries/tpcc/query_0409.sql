-- {"operators": "INNER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER"}
SELECT * FROM WAREHOUSE w INNER JOIN DISTRICT d ON w.W_ID = d.D_W_ID INNER JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID;
