-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_BALANCE, AVG(C_BALANCE) OVER (PARTITION BY C_W_ID) AS part_avg FROM CUSTOMER;
