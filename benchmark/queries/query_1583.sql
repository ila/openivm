-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, C_BALANCE, FLOOR(C_BALANCE) AS floor_b, CEIL(C_BALANCE) AS ceil_b, TRUNC(C_BALANCE, 0) AS trunc_b FROM CUSTOMER;
