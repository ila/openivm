-- {"operators": "ORDER,WINDOW", "complexity": "medium", "is_incremental": false, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER", "non_incr_reason": "op:ORDER", "ducklake": true}
SELECT C_W_ID, C_ID, C_BALANCE, ROW_NUMBER() OVER (PARTITION BY C_W_ID ORDER BY C_BALANCE DESC) AS rn FROM dl.CUSTOMER;
