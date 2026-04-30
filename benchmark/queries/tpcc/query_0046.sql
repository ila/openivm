-- {"operators": "OUTER_JOIN", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,DISTRICT,CUSTOMER,OORDER"}
SELECT w.W_ID, d.D_ID, c.C_ID, o.O_ID FROM WAREHOUSE w JOIN DISTRICT d ON w.W_ID = d.D_W_ID JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID LEFT JOIN OORDER o ON c.C_ID = o.O_C_ID;
