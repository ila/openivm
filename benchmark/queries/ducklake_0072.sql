-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "WAREHOUSE,CUSTOMER", "non_incr_reason": "op:ORDER", "ducklake": true}
SELECT w.W_ID, w.W_NAME, c.C_ID, c.C_BALANCE, RANK() OVER (PARTITION BY w.W_ID ORDER BY c.C_BALANCE DESC) AS rk FROM WAREHOUSE w JOIN CUSTOMER c ON w.W_ID = c.C_W_ID;
