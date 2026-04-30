-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "openivm_verified": true}
SELECT C_W_ID, C_BALANCE, CUME_DIST() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS dist, PERCENT_RANK() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE) AS pct FROM CUSTOMER;
