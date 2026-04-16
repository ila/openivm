-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_ID, C_BALANCE, MIN(C_BALANCE) OVER (PARTITION BY C_W_ID) AS min_bal, MAX(C_BALANCE) OVER (PARTITION BY C_W_ID) AS max_bal FROM CUSTOMER;
