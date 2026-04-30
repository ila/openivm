-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": false, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, CAST(C_BALANCE AS BIGINT) AS bal_int, CAST(C_DISCOUNT AS DECIMAL(6,2)) AS disc_pct FROM CUSTOMER;
