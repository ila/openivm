-- {"operators": "SCAN", "complexity": "low", "is_incremental": true, "has_nulls": false, "has_cast": true, "has_case": true, "tables": "CUSTOMER"}
SELECT C_W_ID, C_ID, CAST(C_BALANCE AS BIGINT) AS balance_int, CAST(C_DISCOUNT AS DECIMAL(6,2)) AS discount_pct, CASE WHEN C_BALANCE > 5000 THEN 'VIP' WHEN C_BALANCE > 0 THEN 'ACTIVE' ELSE 'INACTIVE' END AS status FROM CUSTOMER;
