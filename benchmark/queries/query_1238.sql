-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": true, "has_cast": false, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, NULLIF(C_BALANCE, 0) AS non_zero_bal FROM CUSTOMER;
