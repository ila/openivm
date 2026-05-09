-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "delta": true}
SELECT c.C_W_ID, c.C_D_ID, c.C_ID, c.C_BALANCE, NTILE(4) OVER (PARTITION BY c.C_W_ID ORDER BY c.C_BALANCE) AS quartile FROM d_CUSTOMER c JOIN d_DISTRICT d ON c.C_W_ID = d.D_W_ID AND c.C_D_ID = d.D_ID;
