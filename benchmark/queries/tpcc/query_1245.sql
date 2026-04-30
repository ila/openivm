-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_BALANCE, LAG(C_BALANCE, 1) OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS prev FROM CUSTOMER;
