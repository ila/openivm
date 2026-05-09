-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "delta": true}
SELECT w.W_ID, w.W_NAME, c.C_ID, c.C_BALANCE, RANK() OVER (PARTITION BY w.W_ID ORDER BY c.C_BALANCE DESC) AS rk FROM d_WAREHOUSE w JOIN d_CUSTOMER c ON w.W_ID = c.C_W_ID;
