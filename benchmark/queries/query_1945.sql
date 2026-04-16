-- {"operators": "AGGREGATE,ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_D_ID, C_ID, C_BALANCE, SUM(C_BALANCE) OVER (PARTITION BY C_W_ID ORDER BY C_D_ID, C_ID ROWS BETWEEN 5 PRECEDING AND 5 FOLLOWING) AS centered FROM CUSTOMER;
