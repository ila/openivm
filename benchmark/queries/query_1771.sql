-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "openivm_verified": true}
SELECT d.D_W_ID, c.C_ID, ROW_NUMBER() OVER (PARTITION BY d.D_W_ID ORDER BY c.C_ID DESC) AS rn FROM DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID;
