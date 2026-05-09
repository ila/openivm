-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "delta": true}
SELECT C_W_ID, C_ID, C_BALANCE, ROW_NUMBER() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS rn FROM d_CUSTOMER;
