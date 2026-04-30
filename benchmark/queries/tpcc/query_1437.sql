-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_D_ID, AVG(C_BALANCE) AS avg_bal, RANK() OVER (PARTITION BY C_W_ID ORDER BY AVG(C_BALANCE) DESC) AS rnk FROM CUSTOMER GROUP BY C_W_ID, C_D_ID;
