-- {"operators": "AGGREGATE,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_STATE, C_CREDIT, C_BALANCE, SUM(C_BALANCE) OVER (PARTITION BY C_STATE) AS state_total, COUNT(*) OVER (PARTITION BY C_STATE, C_CREDIT) AS credit_count FROM CUSTOMER;
