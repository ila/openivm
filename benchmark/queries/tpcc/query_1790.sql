-- {"operators": "INNER_JOIN,ORDER,WINDOW", "complexity": "high", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER,HISTORY", "openivm_verified": true}
SELECT c.C_ID, h.H_AMOUNT, RANK() OVER (PARTITION BY c.C_ID ORDER BY h.H_AMOUNT) AS rnk FROM CUSTOMER c JOIN HISTORY h ON c.C_ID = h.H_C_ID AND c.C_W_ID = h.H_C_W_ID;
