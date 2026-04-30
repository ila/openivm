-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "openivm_verified": true}
SELECT w.W_ID, c.C_ID, ROW_NUMBER() OVER (PARTITION BY w.W_ID ORDER BY c.C_ID DESC) AS rn FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID;
