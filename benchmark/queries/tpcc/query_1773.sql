-- {"operators": "INNER_JOIN,AGGREGATE,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "DISTRICT,CUSTOMER", "openivm_verified": true}
SELECT d.D_W_ID, c.C_ID, SUM(c.C_ID) OVER (PARTITION BY d.D_W_ID) AS part_total FROM DISTRICT d JOIN CUSTOMER c ON d.D_W_ID = c.C_W_ID AND d.D_ID = c.C_D_ID;
