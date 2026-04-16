-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "openivm_verified": true}
SELECT w.W_ID, w.W_NAME, c.C_ID, c.C_BALANCE, FIRST_VALUE(c.C_ID) OVER (PARTITION BY w.W_ID ORDER BY c.C_BALANCE DESC) AS top_cust FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID;
